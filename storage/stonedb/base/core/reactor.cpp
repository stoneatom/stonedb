/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2014 Cloudius Systems
 * Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
 */

#include "reactor.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/statfs.h>
#include <sys/syscall.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <atomic>
#include <boost/algorithm/clamp.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/numeric.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/version.hpp>

#include <cassert>
#include <cinttypes>
#include <exception>
#include <regex>

#include "base/core/file_impl.h"
#include "base/core/future_util.h"
#include "base/core/posix.h"
#include "base/core/prefetch.h"
#include "base/core/print.h"
#include "base/core/report_exception.h"
#include "base/core/resource.h"
#include "base/core/scollectd_impl.h"
#include "base/core/systemwide_memory_barrier.h"
#include "base/core/task.h"
#include "base/core/thread.h"
#include "base/net/packet.h"
#include "base/net/posix_stack.h"
#include "base/net/stack.h"
#include "base/util/conversions.h"
#include "base/util/log.h"

#ifdef __GNUC__
#include <cxxabi.h>
#include <iostream>
#include <system_error>
#endif

#include <linux/falloc.h>
#include <linux/magic.h>
#include <sys/mman.h>
#include <sys/utsname.h>
#include "base/util/print_safe.h"
#include "base/util/spinlock.h"

#if defined(__x86_64__) || defined(__i386__)
#include <xmmintrin.h>
#endif

#include "base/core/exception_hacks.h"
#include "base/core/execution_stage.h"
#include "base/core/metrics.h"
#include "base/util/defer.h"

namespace stonedb {
namespace base {

using namespace std::chrono_literals;
using namespace net;

base::logger seastar_logger("seastar");
base::logger sched_logger("scheduler");

std::atomic<lowres_clock_impl::steady_rep> lowres_clock_impl::counters::_steady_now;
std::atomic<lowres_clock_impl::system_rep> lowres_clock_impl::counters::_system_now;
std::atomic<manual_clock::rep> manual_clock::_now;
constexpr std::chrono::milliseconds lowres_clock_impl::_granularity;

static bool sched_debug() { return false; }

template <typename... Args>
void sched_print(const char *fmt, Args &&...args) {
  if (sched_debug()) {
    sched_logger.trace(fmt, std::forward<Args>(args)...);
  }
}

timespec to_timespec(steady_clock_type::time_point t) {
  using ns = std::chrono::nanoseconds;
  auto n = std::chrono::duration_cast<ns>(t.time_since_epoch()).count();
  return {n / 1'000'000'000, n % 1'000'000'000};
}

lowres_clock_impl::lowres_clock_impl() {
  update();
  _timer.set_callback(&lowres_clock_impl::update);
  _timer.arm_periodic(_granularity);
}

void lowres_clock_impl::update() {
  auto const steady_count =
      std::chrono::duration_cast<steady_duration>(base_steady_clock::now().time_since_epoch()).count();

  auto const system_count =
      std::chrono::duration_cast<system_duration>(base_system_clock::now().time_since_epoch()).count();

  counters::_steady_now.store(steady_count, std::memory_order_relaxed);
  counters::_system_now.store(system_count, std::memory_order_relaxed);
}

reactor_backend_epoll::reactor_backend_epoll() : _epollfd(file_desc::epoll_create(EPOLL_CLOEXEC)) {}

reactor::signals::signals() : _pending_signals(0) {}

reactor::signals::~signals() {
  sigset_t mask;
  sigfillset(&mask);
  ::pthread_sigmask(SIG_BLOCK, &mask, NULL);
}

reactor::signals::signal_handler::signal_handler(int signo, std::function<void()> &&handler)
    : _handler(std::move(handler)) {
  struct sigaction sa;
  sa.sa_sigaction = action;
  sa.sa_mask = make_empty_sigset_mask();
  sa.sa_flags = SA_SIGINFO | SA_RESTART;
  auto r = ::sigaction(signo, &sa, nullptr);
  throw_system_error_on(r == -1);
  auto mask = make_sigset_mask(signo);
  r = ::pthread_sigmask(SIG_UNBLOCK, &mask, NULL);
  throw_pthread_error(r);
}

void reactor::signals::handle_signal(int signo, std::function<void()> &&handler) {
  _signal_handlers.emplace(std::piecewise_construct, std::make_tuple(signo),
                           std::make_tuple(signo, std::move(handler)));
}

void reactor::signals::handle_signal_once(int signo, std::function<void()> &&handler) {
  return handle_signal(signo, [fired = false, handler = std::move(handler)]() mutable {
    if (!fired) {
      fired = true;
      handler();
    }
  });
}

bool reactor::signals::poll_signal() {
  auto signals = _pending_signals.load(std::memory_order_relaxed);
  if (signals) {
    _pending_signals.fetch_and(~signals, std::memory_order_relaxed);
    for (size_t i = 0; i < sizeof(signals) * 8; i++) {
      if (signals & (1ull << i)) {
        _signal_handlers.at(i)._handler();
      }
    }
  }
  return signals;
}

bool reactor::signals::pure_poll_signal() const { return _pending_signals.load(std::memory_order_relaxed); }

void reactor::signals::action(int signo, [[maybe_unused]] siginfo_t *siginfo, [[maybe_unused]] void *ignore) {
  g_need_preempt = true;
  engine()._signals._pending_signals.fetch_or(1ull << signo, std::memory_order_relaxed);
}

static void print_with_backtrace([[maybe_unused]] const char *cause) noexcept {}

inline int alarm_signal() {
  // We don't want to use SIGALRM, because the boost unit test library
  // also plays with it.
  return SIGALRM;
}

inline int block_notifier_signal() { return SIGRTMIN + 1; }

// Installs signal handler stack for current thread.
// The stack remains installed as long as the returned object is kept alive.
// When it goes out of scope the previous handler is restored.
static decltype(auto) install_signal_handler_stack() {
  size_t size = SIGSTKSZ;
  auto mem = std::make_unique<char[]>(size);
  stack_t stack;
  stack_t prev_stack;
  stack.ss_sp = mem.get();
  stack.ss_flags = 0;
  stack.ss_size = size;
  auto r = sigaltstack(&stack, &prev_stack);
  throw_system_error_on(r == -1);
  return defer([mem = std::move(mem), prev_stack]() mutable {
    try {
      auto r = sigaltstack(&prev_stack, NULL);
      throw_system_error_on(r == -1);
    } catch (...) {
      mem.release();  // We failed to restore previous stack, must leak it.
      seastar_logger.error("Failed to restore signal stack: {}", std::current_exception());
    }
  });
}

reactor::task_queue::task_queue(unsigned id, sstring name, float shares)
    : _shares(std::max(shares, 1.0f)),
      _reciprocal_shares_times_2_power_32((uint64_t(1) << 32) / _shares),
      _id(id),
      _name(name) {
  namespace sm = base::metrics;
  static auto group = sm::label("group");
  auto group_label = group(_name);
  _metrics.add_group(
      "scheduler",
      {
          sm::make_counter("runtime_ms",
                           [this] { return std::chrono::duration_cast<std::chrono::milliseconds>(_runtime).count(); },
                           sm::description("Accumulated runtime of this task queue; an "
                                           "increment rate of 1000ms per "
                                           "second indicates full utilization"),
                           {group_label}),
          sm::make_counter("tasks_processed", _tasks_processed,
                           sm::description("Count of tasks executing on this queue; "
                                           "indicates together with "
                                           "runtime_ms indicates length of tasks"),
                           {group_label}),
          sm::make_gauge("queue_length", [this] { return _q.size(); },
                         sm::description("Size of backlog on this queue, in tasks; indicates whether "
                                         "the queue is busy and/or contended"),
                         {group_label}),
          sm::make_gauge("shares", [this] { return _shares; }, sm::description("Shares allocated to this queue"),
                         {group_label}),
      });
}

inline int64_t reactor::task_queue::to_vruntime(sched_clock::duration runtime) const {
  auto scaled = (runtime.count() * _reciprocal_shares_times_2_power_32) >> 32;
  // Prevent overflow from returning ridiculous values
  return std::max<int64_t>(scaled, 0);
}

void reactor::task_queue::set_shares(float shares) {
  _shares = std::max(shares, 1.0f);
  _reciprocal_shares_times_2_power_32 = (uint64_t(1) << 32) / _shares;
}

void reactor::account_runtime(task_queue &tq, sched_clock::duration runtime) {
  tq._vruntime += tq.to_vruntime(runtime);
  tq._runtime += runtime;
}

void reactor::account_idle([[maybe_unused]] sched_clock::duration runtime) {
  // anything to do here?
}

struct reactor::task_queue::indirect_compare {
  bool operator()(const task_queue *tq1, const task_queue *tq2) const { return tq1->_vruntime < tq2->_vruntime; }
};

reactor::reactor(unsigned id)
    : _backend(),
      _id(id),
      _cpu_started(0)
#if defined(ENABLE_AIO)
      ,
      _io_context(0),
      _io_context_available(max_aio)
#endif
      ,
      _reuseport(posix_reuseport_detect()) {
  _task_queues.push_back(std::make_unique<task_queue>(0, "main", 1000));
  _task_queues.push_back(std::make_unique<task_queue>(1, "atexit", 1000));
  _at_destroy_tasks = _task_queues.back().get();
  base::thread_impl::init();
#if defined(ENABLE_AIO)
  auto r = ::io_setup(max_aio, &_io_context);
  assert(r >= 0);
#else
  auto r = 0;
#endif
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, alarm_signal());
  r = ::pthread_sigmask(SIG_BLOCK, &mask, NULL);
  assert(r == 0);
  struct sigevent sev;
  sev.sigev_notify = SIGEV_THREAD_ID;
  sev._sigev_un._tid = syscall(SYS_gettid);
  sev.sigev_signo = alarm_signal();
  r = timer_create(CLOCK_MONOTONIC, &sev, &_steady_clock_timer);
  assert(r >= 0);
  (void)r;
}

reactor::~reactor() {
  _dying.store(true, std::memory_order_relaxed);
  timer_delete(_steady_clock_timer);
  auto eraser = [](auto &list) {
    while (!list.empty()) {
      auto &timer = *list.begin();
      timer.cancel();
    }
  };
  eraser(_expired_timers);
  eraser(_expired_lowres_timers);
  eraser(_expired_manual_timers);
}

// Add to an atomic integral non-atomically and returns the previous value
template <typename Integral>
inline Integral add_nonatomically(std::atomic<Integral> &value, Integral inc) {
  auto tmp = value.load(std::memory_order_relaxed);
  value.store(tmp + inc, std::memory_order_relaxed);
  return tmp;
}

// Increments an atomic integral non-atomically and returns the previous value
// Akin to value++;
template <typename Integral>
inline Integral increment_nonatomically(std::atomic<Integral> &value) {
  return add_nonatomically(value, Integral(1));
}

template <typename T, typename E, typename EnableFunc>
void reactor::complete_timers(T &timers, E &expired_timers, EnableFunc &&enable_fn) {
  expired_timers = timers.expire(timers.now());
  for (auto &t : expired_timers) {
    t._expired = true;
  }
  while (!expired_timers.empty()) {
    auto t = &*expired_timers.begin();
    expired_timers.pop_front();
    t->_queued = false;
    if (t->_armed) {
      t->_armed = false;
      if (t->_period) {
        t->readd_periodic();
      }
      try {
        t->_callback();
      } catch (...) {
        seastar_logger.error("Timer callback failed: {}", std::current_exception());
      }
    }
  }
  enable_fn();
}

class network_stack_registry {
 private:
  static std::unordered_map<sstring, std::function<future<std::unique_ptr<network_stack>>()>> &_map() {
    static std::unordered_map<sstring, std::function<future<std::unique_ptr<network_stack>>()>> map;
    return map;
  }
  static sstring &_default() {
    static sstring def;
    return def;
  }

 public:
  static void register_stack(sstring name, std::function<future<std::unique_ptr<network_stack>>()> create,
                             bool make_default = false);
  static sstring default_stack();
  static std::vector<sstring> list();
  static future<std::unique_ptr<network_stack>> create();
  static future<std::unique_ptr<network_stack>> create(sstring name);
};

void reactor::configure(const options &opt) {
  auto network_stack_ready = !opt.network_stack.empty() ? network_stack_registry::create(sstring(opt.network_stack))
                                                        : network_stack_registry::create();
  network_stack_ready.then(
      [this](std::unique_ptr<network_stack> stack) { _network_stack_ready_promise.set_value(std::move(stack)); });

  _handle_sigint = !opt.no_handle_interrupt;
  auto task_quota = opt.task_quota_ms * 1ms;
  _task_quota = std::chrono::duration_cast<sched_clock::duration>(task_quota);

  auto blocked_time = opt.blocked_reactor_notify_ms * 1ms;
  _tasks_processed_report_threshold = unsigned(blocked_time / task_quota);
  _stall_detector_reports_per_minute = opt.blocked_reactor_reports_per_minute;

  _max_task_backlog = opt.max_task_backlog;
  _max_poll_time = opt.idle_poll_time_us * 1us;
  if (opt.poll_mode) {
    _max_poll_time = std::chrono::nanoseconds::max();
  }
  if (opt.overprovisioned && (opt.idle_poll_time_us == 0) && !opt.poll_mode) {
    _max_poll_time = 0us;
  }
  set_strict_dma(!opt.relaxed_dma);
  if (!opt.poll_aio || (opt.poll_aio && opt.overprovisioned)) {
    _aio_eventfd = pollable_fd(file_desc::eventfd(0, 0));
  }
  set_bypass_fsync(opt.unsafe_bypass_fsync);
}

future<> reactor_backend_epoll::get_epoll_future(pollable_fd_state &pfd, promise<> pollable_fd_state::*pr, int event) {
  if (pfd.events_known & event) {
    pfd.events_known &= ~event;
    return make_ready_future();
  }
  pfd.events_requested |= event;
  if (!(pfd.events_epoll & event)) {
    auto ctl = pfd.events_epoll ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    pfd.events_epoll |= event;
    ::epoll_event eevt;
    eevt.events = pfd.events_epoll;
    eevt.data.ptr = &pfd;
    int r = ::epoll_ctl(_epollfd.get(), ctl, pfd.fd.get(), &eevt);
    assert(r == 0);
    engine().start_epoll();
  }
  pfd.*pr = promise<>();
  return (pfd.*pr).get_future();
}

void reactor_backend_epoll::abort_fd(pollable_fd_state &pfd, std::exception_ptr ex, promise<> pollable_fd_state::*pr,
                                     int event) {
  if (pfd.events_epoll & event) {
    pfd.events_epoll &= ~event;
    auto ctl = pfd.events_epoll ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
    ::epoll_event eevt;
    eevt.events = pfd.events_epoll;
    eevt.data.ptr = &pfd;
    int r = ::epoll_ctl(_epollfd.get(), ctl, pfd.fd.get(), &eevt);
    assert(r == 0);
  }
  if (pfd.events_requested & event) {
    pfd.events_requested &= ~event;
    (pfd.*pr).set_exception(std::move(ex));
  }
  pfd.events_known &= ~event;
}

future<> reactor_backend_epoll::readable(pollable_fd_state &fd) {
  return get_epoll_future(fd, &pollable_fd_state::pollin, EPOLLIN);
}

future<> reactor_backend_epoll::writeable(pollable_fd_state &fd) {
  return get_epoll_future(fd, &pollable_fd_state::pollout, EPOLLOUT);
}

void reactor_backend_epoll::abort_reader(pollable_fd_state &fd, std::exception_ptr ex) {
  abort_fd(fd, std::move(ex), &pollable_fd_state::pollin, EPOLLIN);
}

void reactor_backend_epoll::abort_writer(pollable_fd_state &fd, std::exception_ptr ex) {
  abort_fd(fd, std::move(ex), &pollable_fd_state::pollout, EPOLLOUT);
}

void reactor_backend_epoll::forget(pollable_fd_state &fd) {
  if (fd.events_epoll) {
    ::epoll_ctl(_epollfd.get(), EPOLL_CTL_DEL, fd.fd.get(), nullptr);
  }
}

future<> reactor_backend_epoll::notified([[maybe_unused]] reactor_notifier *n) {
  // Currently reactor_backend_epoll doesn't need to support notifiers,
  // because we add to it file descriptors instead. But this can be fixed
  // later.
  std::cout << "reactor_backend_epoll does not yet support notifiers!\n";
  abort();
}

pollable_fd reactor::posix_listen(socket_address sa, listen_options opts) {
  file_desc fd = file_desc::socket(sa.u.sa.sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, int(opts.proto));
  if (opts.reuse_address) {
    fd.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1);
  }
  if (_reuseport) fd.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1);

  fd.bind(sa.u.sa, sizeof(sa.u.sas));
  fd.listen(100);
  return pollable_fd(std::move(fd));
}

bool reactor::posix_reuseport_detect() {
  return false;  // FIXME: reuseport currently leads to heavy load imbalance.
                 // Until we fix that, just disable it unconditionally.
  try {
    file_desc fd = file_desc::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    fd.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1);
    return true;
  } catch (std::system_error &e) {
    return false;
  }
}

lw_shared_ptr<pollable_fd> reactor::make_pollable_fd(socket_address sa, transport proto) {
  file_desc fd = file_desc::socket(sa.u.sa.sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, int(proto));
  return make_lw_shared<pollable_fd>(pollable_fd(std::move(fd)));
}

future<> reactor::posix_connect(lw_shared_ptr<pollable_fd> pfd, socket_address sa, socket_address local) {
  pfd->get_file_desc().bind(local.u.sa, sizeof(sa.u.sas));
  pfd->get_file_desc().connect(sa.u.sa, sizeof(sa.u.sas));
  return pfd->writeable().then([pfd]() mutable {
    auto err = pfd->get_file_desc().getsockopt<int>(SOL_SOCKET, SO_ERROR);
    if (err != 0) {
      throw std::system_error(err, std::system_category());
    }
    return make_ready_future<>();
  });
}

server_socket reactor::listen(socket_address sa, listen_options opt) {
  return server_socket(_network_stack->listen(sa, opt));
}

future<connected_socket> reactor::connect(socket_address sa) { return _network_stack->connect(sa); }

future<connected_socket> reactor::connect(socket_address sa, socket_address local, transport proto) {
  return _network_stack->connect(sa, local, proto);
}

void reactor_backend_epoll::complete_epoll_event(pollable_fd_state &pfd, promise<> pollable_fd_state::*pr, int events,
                                                 int event) {
  if (pfd.events_requested & events & event) {
    pfd.events_requested &= ~event;
    pfd.events_known &= ~event;
    (pfd.*pr).set_value();
    pfd.*pr = promise<>();
  }
}

const io_priority_class &default_priority_class() {
  static thread_local io_priority_class shard_default_class;
  return shard_default_class;
}

#if defined(ENABLE_AIO)
template <typename Func>
future<io_event> reactor::submit_io(Func prepare_io) {
  return _io_context_available.wait(1).then([this, prepare_io = std::move(prepare_io)]() mutable {
    auto pr = std::make_unique<promise<io_event>>();
    iocb io;
    prepare_io(io);
    if (_aio_eventfd) {
      io_set_eventfd(&io, _aio_eventfd->get_fd());
    }
    auto f = pr->get_future();
    io.data = pr.get();
    _pending_aio.push_back(io);
    pr.release();
    if ((_io_queue->queued_requests() > 0) ||
        (_pending_aio.size() >= std::min(max_aio / 4, _io_queue->_capacity / 2))) {
      flush_pending_aio();
    }
    return f;
  });
}

bool reactor::flush_pending_aio() {
  bool did_work = false;
  while (!_pending_aio.empty()) {
    auto nr = _pending_aio.size();
    struct iocb *iocbs[max_aio];
    for (size_t i = 0; i < nr; ++i) {
      iocbs[i] = &_pending_aio[i];
    }
    auto r = ::io_submit(_io_context, nr, iocbs);
    size_t nr_consumed;
    if (r < 0) {
      auto ec = -r;
      switch (ec) {
        case EAGAIN:
          return did_work;
        case EBADF: {
          auto pr = reinterpret_cast<promise<io_event> *>(iocbs[0]->data);
          try {
            throw_kernel_error(r);
          } catch (...) {
            pr->set_exception(std::current_exception());
          }
          delete pr;
          _io_context_available.signal(1);
          // if EBADF, it means that the first request has a bad fd, so
          // we will only remove it from _pending_aio and try again.
          nr_consumed = 1;
          break;
        }
        default:
          throw_kernel_error(r);
          abort();
      }
    } else {
      nr_consumed = size_t(r);
    }

    did_work = true;
    if (nr_consumed == nr) {
      _pending_aio.clear();
    } else {
      _pending_aio.erase(_pending_aio.begin(), _pending_aio.begin() + nr_consumed);
    }
  }
  return did_work;
}

template <typename Func>
future<io_event> reactor::submit_io_read(const io_priority_class &pc, size_t len, Func prepare_io) {
  ++_io_stats.aio_reads;
  _io_stats.aio_read_bytes += len;
  return io_queue::queue_request(_io_coordinator, pc, len, std::move(prepare_io));
}

template <typename Func>
future<io_event> reactor::submit_io_write(const io_priority_class &pc, size_t len, Func prepare_io) {
  ++_io_stats.aio_writes;
  _io_stats.aio_write_bytes += len;
  return io_queue::queue_request(_io_coordinator, pc, len, std::move(prepare_io));
}

bool reactor::process_io() {
  io_event ev[max_aio];
  struct timespec timeout = {0, 0};
  auto n = ::io_getevents(_io_context, 1, max_aio, ev, &timeout);
  assert(n >= 0);
  for (size_t i = 0; i < size_t(n); ++i) {
    auto pr = reinterpret_cast<promise<io_event> *>(ev[i].data);
    pr->set_value(ev[i]);
    delete pr;
  }
  _io_context_available.signal(n);
  return n;
}

io_queue::io_queue(shard_id coordinator, size_t capacity, std::vector<shard_id> topology)
    : _coordinator(coordinator),
      _capacity(capacity),
      _io_topology(std::move(topology)),
      _priority_classes(),
      _fq(capacity) {}

io_queue::~io_queue() {
  // It is illegal to stop the I/O queue with pending requests.
  // Technically we would use a gate to guarantee that. But here, it is not
  // needed since this is expected to be destroyed only after the reactor is
  // destroyed.
  //
  // And that will happen only when there are no more fibers to run. If we ever
  // change that, then this has to change.
  for (auto &&pclasses : _priority_classes) {
    _fq.unregister_priority_class(pclasses.second->ptr);
  }
}

std::array<std::atomic<uint32_t>, io_queue::_max_classes> io_queue::_registered_shares;
// We could very well just add the name to the io_priority_class. However,
// because that structure is passed along all the time - and sometimes we can't
// help but copy it, better keep it lean. The name won't really be used for
// anything other than monitoring.
std::array<sstring, io_queue::_max_classes> io_queue::_registered_names;

void io_queue::fill_shares_array() {
  for (unsigned i = 0; i < _max_classes; ++i) {
    _registered_shares[i].store(0);
  }
}

io_priority_class io_queue::register_one_priority_class(sstring name, uint32_t shares) {
  for (unsigned i = 0; i < _max_classes; ++i) {
    uint32_t unused = 0;
    auto s = _registered_shares[i].compare_exchange_strong(unused, shares, std::memory_order_acq_rel);
    if (s) {
      io_priority_class p;
      _registered_names[i] = name;
      p.val = i;
      return p;
    };
  }
  throw std::runtime_error("No more room for new I/O priority classes");
}

base::metrics::label io_queue_shard("ioshard");

io_queue::priority_class_data::priority_class_data(sstring name, priority_class_ptr ptr, shard_id owner)
    : ptr(ptr), bytes(0), ops(0), nr_queued(0), queue_time(1s) {
  namespace sm = base::metrics;
  auto shard = sm::impl::shard();
  _metric_groups.add_group(
      "io_queue",
      {sm::make_derive(name + sstring("_total_bytes"), bytes, sm::description("Total bytes passed in the queue"),
                       {io_queue_shard(shard), sm::shard_label(owner)}),
       sm::make_derive(name + sstring("_total_operations"), ops, sm::description("Total bytes passed in the queue"),
                       {io_queue_shard(shard), sm::shard_label(owner)}),
       // Note: The counter below is not the same as reactor's
       // queued-io-requests queued-io-requests shows us how many requests in
       // total exist in this I/O Queue.
       //
       // This counter lives in the priority class, so it will count only queued
       // requests that belong to that class.
       //
       // In other words: the new counter tells you how busy a class is, and the
       // old counter tells you how busy the system is.

       sm::make_queue_length(name + sstring("_queue_length"), nr_queued,
                             sm::description("Number of requests in the queue"),
                             {io_queue_shard(shard), sm::shard_label(owner)}),
       sm::make_gauge(name + sstring("_delay"), [this] { return queue_time.count(); },
                      sm::description("total delay time in the queue"),
                      {io_queue_shard(shard), sm::shard_label(owner)})});
}

io_queue::priority_class_data &io_queue::find_or_create_class(const io_priority_class &pc, shard_id owner) {
  auto it_pclass = _priority_classes.find(pc.id());
  if (it_pclass == _priority_classes.end()) {
    auto shares = _registered_shares.at(pc.id()).load(std::memory_order_acquire);
    auto name = _registered_names.at(pc.id());
    // A note on naming:
    //
    // We could just add the owner as the instance id and have something like:
    //  io_queue-<class_owner>-<counter>-<class_name>
    //
    // However, when there are more than one shard per I/O queue, it is very
    // useful to know which shards are being served by the same queue.
    // Therefore, a better name scheme is:
    //
    //  io_queue-<queue_owner>-<counter>-<class_name>, shard=<class_owner>
    //  using the shard label to hold the owner number
    //
    // This conveys all the information we need and allows one to easily group
    // all classes from the same I/O queue (by filtering by shard)

    auto ret = _priority_classes.emplace(
        pc.id(), make_lw_shared<priority_class_data>(name, _fq.register_priority_class(shares), owner));
    it_pclass = ret.first;
  }
  return *(it_pclass->second);
}

template <typename Func>
future<io_event> io_queue::queue_request(shard_id coordinator, const io_priority_class &pc, size_t len,
                                         Func prepare_io) {
  auto start = std::chrono::steady_clock::now();
  return smp::submit_to(coordinator, [start, &pc, len, prepare_io = std::move(prepare_io), owner = engine().cpu_id()] {
    auto &queue = *(engine()._io_queue);
    unsigned weight = 1 + len / (16 << 10);
    // First time will hit here, and then we create the class. It is
    // important that we create the shared pointer in the same shard it will
    // be used at later.
    auto &pclass = queue.find_or_create_class(pc, owner);
    pclass.bytes += len;
    pclass.ops++;
    pclass.nr_queued++;
    return queue._fq.queue(pclass.ptr, weight, [&pclass, start, prepare_io = std::move(prepare_io)] {
      pclass.nr_queued--;
      pclass.queue_time =
          std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - start);
      return engine().submit_io(std::move(prepare_io));
    });
  });
}
#endif

void reactor::enable_timer(steady_clock_type::time_point when) {
  itimerspec its;
  its.it_interval = to_timespec(when);
  its.it_value = to_timespec(when);
  auto ret = timer_settime(_steady_clock_timer, TIMER_ABSTIME, &its, NULL);
  throw_system_error_on(ret == -1);
}

void reactor::add_timer(timer<steady_clock_type> *tmr) {
  if (queue_timer(tmr)) {
    enable_timer(_timers.get_next_timeout());
  }
}

bool reactor::queue_timer(timer<steady_clock_type> *tmr) { return _timers.insert(*tmr); }

void reactor::del_timer(timer<steady_clock_type> *tmr) {
  if (tmr->_expired) {
    _expired_timers.erase(_expired_timers.iterator_to(*tmr));
    tmr->_expired = false;
  } else {
    _timers.remove(*tmr);
  }
}

void reactor::add_timer(timer<lowres_clock> *tmr) {
  if (queue_timer(tmr)) {
    _lowres_next_timeout = _lowres_timers.get_next_timeout();
  }
}

bool reactor::queue_timer(timer<lowres_clock> *tmr) { return _lowres_timers.insert(*tmr); }

void reactor::del_timer(timer<lowres_clock> *tmr) {
  if (tmr->_expired) {
    _expired_lowres_timers.erase(_expired_lowres_timers.iterator_to(*tmr));
    tmr->_expired = false;
  } else {
    _lowres_timers.remove(*tmr);
  }
}

void reactor::add_timer(timer<manual_clock> *tmr) { queue_timer(tmr); }

bool reactor::queue_timer(timer<manual_clock> *tmr) { return _manual_timers.insert(*tmr); }

void reactor::del_timer(timer<manual_clock> *tmr) {
  if (tmr->_expired) {
    _expired_manual_timers.erase(_expired_manual_timers.iterator_to(*tmr));
    tmr->_expired = false;
  } else {
    _manual_timers.remove(*tmr);
  }
}

void reactor::at_exit(std::function<future<>()> func) {
  assert(!_stopping);
  _exit_funcs.push_back(std::move(func));
}

future<> reactor::run_exit_tasks() {
  _stop_requested.broadcast();
  _stopping = true;
#if defined(ENABLE_AIO)
  stop_aio_eventfd_loop();
#endif

  // The do_for_each will cause demangler-warning when gdb debugging, so we
  // replace it with repeat_until_value.
#if 0
    return do_for_each(_exit_funcs.rbegin(), _exit_funcs.rend(), [] (auto& func) {
        return func();
    });
#else
  auto functor = [this]() -> future<std::experimental::optional<bool>> {
    if (_exit_funcs.empty()) return make_ready_future<std::experimental::optional<bool>>(true);
    auto &func = _exit_funcs.back();
    func();
    _exit_funcs.pop_back();
    return make_ready_future<std::experimental::optional<bool>>(std::experimental::nullopt);
  };
  return repeat_until_value(std::move(functor)).then_wrapped([]([[maybe_unused]] auto &&f) {});
#endif
}

void reactor::stop() {
  assert(engine()._id == 0);
  smp::cleanup_cpu();
  if (!_stopping) {
    run_exit_tasks().then([this] {
      do_with(semaphore(0), [this](semaphore &sem) {
        for (unsigned i = 1; i < smp::count; i++) {
          smp::submit_to<>(i, []() {
            smp::cleanup_cpu();
            return engine().run_exit_tasks().then([] { engine()._stopped = true; });
          }).then([&sem]() { sem.signal(); });
        }
        return sem.wait(smp::count - 1).then([this] { _stopped = true; });
      });
    });
  }
}

void reactor::exit(int ret) {
  smp::submit_to(0, [this, ret] {
    _return = ret;
    stop();
  });
}

uint64_t reactor::pending_task_count() const {
  uint64_t ret = 0;
  for (auto &&tq : _task_queues) {
    ret += tq->_q.size();
  }
  return ret;
}

uint64_t reactor::tasks_processed() const {
  uint64_t ret = 0;
  for (auto &&tq : _task_queues) {
    ret += tq->_tasks_processed;
  }
  return ret;
}

void reactor::register_metrics() {
  namespace sm = base::metrics;

  _metric_groups.add_group("reactor", {
    sm::make_gauge("tasks_pending", std::bind(&reactor::pending_task_count, this),
                   sm::description("Number of pending tasks in the queue")),
        // total_operations value:DERIVE:0:U
        sm::make_derive("tasks_processed", std::bind(&reactor::tasks_processed, this),
                        sm::description("Total tasks processed")),
        sm::make_derive(
            "polls", [this] { return _polls.load(std::memory_order_relaxed); },
            sm::description("Number of times pollers were executed")),
        sm::make_derive("timers_pending", std::bind(&decltype(_timers)::size, &_timers),
                        sm::description("Number of tasks in the timer-pending queue")),
        sm::make_gauge(
            "utilization", [this] { return (1 - _load) * 100; }, sm::description("CPU utilization")),
        sm::make_derive(
            "cpu_busy_ns",
            [this]() -> int64_t {
              return std::chrono::duration_cast<std::chrono::nanoseconds>(total_busy_time()).count();
            },
            sm::description("Total cpu busy time in nanoseconds")),
#if defined(ENABLE_AIO)
        // total_operations value:DERIVE:0:U
        sm::make_derive("aio_reads", _io_stats.aio_reads, sm::description("Total aio-reads operations")),

        sm::make_total_bytes("aio_bytes_read", _io_stats.aio_read_bytes, sm::description("Total aio-reads bytes")),
        // total_operations value:DERIVE:0:U
        sm::make_derive("aio_writes", _io_stats.aio_writes, sm::description("Total aio-writes operations")),
        sm::make_total_bytes("aio_bytes_write", _io_stats.aio_write_bytes, sm::description("Total aio-writes bytes")),
#endif
        // total_operations value:DERIVE:0:U
        sm::make_derive("fsyncs", _fsyncs, sm::description("Total number of fsync operations")),
  });

  _metric_groups.add_group(
      "reactor",
      {
          sm::make_derive(
              "logging_failures", [] { return logging_failures; }, sm::description("Total number of logging failures")),
          // total_operations value:DERIVE:0:U
          sm::make_derive("cpp_exceptions", _cxx_exceptions, sm::description("Total number of C++ exceptions")),
      });

#if defined(ENABLE_AIO)
  if (my_io_queue) {
    _metric_groups.add_group("reactor", {
                                            sm::make_gauge(
                                                "io_queue_requests", [this] { return my_io_queue->queued_requests(); },
                                                sm::description("Number of requests in the io queue")),
                                        });
  }

  using namespace stonedb::base::metrics;
  _metric_groups.add_group(
      "reactor", {
                     make_counter("fstream_reads", _io_stats.fstream_reads,
                                  description("Counts reads from disk file streams.  A "
                                              "high rate indicates high disk activity."
                                              " Contrast with other fstream_read* "
                                              "counters to locate bottlenecks.")),
                     make_derive("fstream_read_bytes", _io_stats.fstream_read_bytes,
                                 description("Counts bytes read from disk file streams.  A high rate "
                                             "indicates high disk activity."
                                             " Divide by fstream_reads to determine average read size.")),
                     make_counter("fstream_reads_blocked", _io_stats.fstream_reads_blocked,
                                  description("Counts the number of times a disk read "
                                              "could not be satisfied from read-ahead "
                                              "buffers, and had to block."
                                              " Indicates short streams, or incorrect "
                                              "read ahead configuration.")),
                     make_derive("fstream_read_bytes_blocked", _io_stats.fstream_read_bytes_blocked,
                                 description("Counts the number of bytes read from disk "
                                             "that could not be satisfied from "
                                             "read-ahead buffers, and had to block."
                                             " Indicates short streams, or incorrect read "
                                             "ahead configuration.")),
                     make_counter("fstream_reads_aheads_discarded", _io_stats.fstream_read_aheads_discarded,
                                  description("Counts the number of times a buffer that was read ahead of "
                                              "time and was "
                                              "discarded because it was not needed, wasting disk bandwidth."
                                              " Indicates over-eager read ahead configuration.")),
                     make_derive("fstream_reads_ahead_bytes_discarded", _io_stats.fstream_read_ahead_discarded_bytes,
                                 description("Counts the number of buffered bytes that were read "
                                             "ahead of time and were "
                                             "discarded because they were not needed, wasting "
                                             "disk bandwidth."
                                             " Indicates over-eager read ahead configuration.")),
                 });
#endif
}

void reactor::run_tasks(task_queue &tq) {
  // Make sure new tasks will inherit our scheduling group
  *internal::current_scheduling_group_ptr() = scheduling_group(tq._id);
  auto &tasks = tq._q;
  while (!tasks.empty()) {
    auto tsk = std::move(tasks.front());
    tasks.pop_front();
    tsk->run();
    tsk.reset();
    ++tq._tasks_processed;
    // check at end of loop, to allow at least one task to run
    if (need_preempt() && tasks.size() <= _max_task_backlog) {
      break;
    }
  }
}

void reactor::force_poll() { g_need_preempt = true; }

bool reactor::flush_tcp_batches() {
  bool work = _flush_batching.size();
  while (!_flush_batching.empty()) {
    auto os = std::move(_flush_batching.front());
    _flush_batching.pop_front();
    os->poll_flush();
  }
  return work;
}

bool reactor::do_expire_lowres_timers() {
  if (_lowres_next_timeout == lowres_clock::time_point()) {
    return false;
  }
  auto now = lowres_clock::now();
  if (now > _lowres_next_timeout) {
    complete_timers(_lowres_timers, _expired_lowres_timers, [this] {
      if (!_lowres_timers.empty()) {
        _lowres_next_timeout = _lowres_timers.get_next_timeout();
      } else {
        _lowres_next_timeout = lowres_clock::time_point();
      }
    });
    return true;
  }
  return false;
}

void reactor::expire_manual_timers() {
  complete_timers(_manual_timers, _expired_manual_timers, [] {});
}

void manual_clock::expire_timers() { local_engine->expire_manual_timers(); }

void manual_clock::advance(manual_clock::duration d) {
  _now.fetch_add(d.count());
  if (local_engine) {
    schedule_urgent(make_task(default_scheduling_group(), &manual_clock::expire_timers));
    smp::invoke_on_all(&manual_clock::expire_timers);
  }
}

bool reactor::do_check_lowres_timers() const {
  if (_lowres_next_timeout == lowres_clock::time_point()) {
    return false;
  }
  return lowres_clock::now() > _lowres_next_timeout;
}
#if defined(ENABLE_AIO)
class reactor::io_pollfn final : public reactor::pollfn {
  reactor &_r;

 public:
  io_pollfn(reactor &r) : _r(r) {}
  virtual bool poll() override final { return _r.process_io(); }
  virtual bool pure_poll() override final {
    return poll();  // actually performs work, but triggers no user
                    // continuations, so okay
  }
  virtual bool try_enter_interrupt_mode() override {
    // aio cannot generate events if there are no inflight aios;
    // but if we enabled _aio_eventfd, we can always enter
    return _r._io_context_available.current() == reactor::max_aio || _r._aio_eventfd;
  }
  virtual void exit_interrupt_mode() override {
    // nothing to do
  }
};
#endif

class reactor::signal_pollfn final : public reactor::pollfn {
  reactor &_r;

 public:
  signal_pollfn(reactor &r) : _r(r) {}
  virtual bool poll() final override { return _r._signals.poll_signal(); }
  virtual bool pure_poll() override final { return _r._signals.pure_poll_signal(); }
  virtual bool try_enter_interrupt_mode() override {
    // Signals will interrupt our epoll_pwait() call, but
    // disable them now to avoid a signal between this point
    // and epoll_pwait()
    sigset_t block_all;
    sigfillset(&block_all);
    ::pthread_sigmask(SIG_SETMASK, &block_all, &_r._active_sigmask);
    if (poll()) {
      // raced already, and lost
      exit_interrupt_mode();
      return false;
    }
    return true;
  }
  virtual void exit_interrupt_mode() override final { ::pthread_sigmask(SIG_SETMASK, &_r._active_sigmask, nullptr); }
};

class reactor::batch_flush_pollfn final : public reactor::pollfn {
  reactor &_r;

 public:
  batch_flush_pollfn(reactor &r) : _r(r) {}
  virtual bool poll() final override { return _r.flush_tcp_batches(); }
  virtual bool pure_poll() override final {
    return poll();  // actually performs work, but triggers no user
                    // continuations, so okay
  }
  virtual bool try_enter_interrupt_mode() override {
    // This is a passive poller, so if a previous poll
    // returned false (idle), there's no more work to do.
    return true;
  }
  virtual void exit_interrupt_mode() override final {}
};

#if defined(ENABLE_AIO)
class reactor::aio_batch_submit_pollfn final : public reactor::pollfn {
  reactor &_r;

 public:
  aio_batch_submit_pollfn(reactor &r) : _r(r) {}
  virtual bool poll() final override { return _r.flush_pending_aio(); }
  virtual bool pure_poll() override final {
    return poll();  // actually performs work, but triggers no user
                    // continuations, so okay
  }
  virtual bool try_enter_interrupt_mode() override {
    // This is a passive poller, so if a previous poll
    // returned false (idle), there's no more work to do.
    return true;
  }
  virtual void exit_interrupt_mode() override final {}
};
#endif

class reactor::lowres_timer_pollfn final : public reactor::pollfn {
  reactor &_r;
  // A highres timer is implemented as a waking  signal; so
  // we arm one when we have a lowres timer during sleep, so
  // it can wake us up.
  timer<> _nearest_wakeup{[this] { _armed = false; }};
  bool _armed = false;

 public:
  lowres_timer_pollfn(reactor &r) : _r(r) {}
  virtual bool poll() final override { return _r.do_expire_lowres_timers(); }
  virtual bool pure_poll() final override { return _r.do_check_lowres_timers(); }
  virtual bool try_enter_interrupt_mode() override {
    // arm our highres timer so a signal will wake us up
    auto next = _r._lowres_next_timeout;
    if (next == lowres_clock::time_point()) {
      // no pending timers
      return true;
    }
    auto now = lowres_clock::now();
    if (next <= now) {
      // whoops, go back
      return false;
    }
    _nearest_wakeup.arm(next - now);
    _armed = true;
    return true;
  }
  virtual void exit_interrupt_mode() override final {
    if (_armed) {
      _nearest_wakeup.cancel();
      _armed = false;
    }
  }
};

class reactor::smp_pollfn final : public reactor::pollfn {
  reactor &_r;
  struct aligned_flag {
    std::atomic<bool> flag;
    char pad[cache_line_size - sizeof(flag)];
    bool try_lock() { return !flag.exchange(true, std::memory_order_relaxed); }
    void unlock() { flag.store(false, std::memory_order_relaxed); }
  };
  static aligned_flag _membarrier_lock;

 public:
  smp_pollfn(reactor &r) : _r(r) {}
  virtual bool poll() final override { return smp::poll_queues(); }
  virtual bool pure_poll() final override { return smp::pure_poll_queues(); }
  virtual bool try_enter_interrupt_mode() override {
    // systemwide_memory_barrier() is very slow if run concurrently,
    // so don't go to sleep if it is running now.
    if (!_membarrier_lock.try_lock()) {
      return false;
    }
    _r._sleeping.store(true, std::memory_order_relaxed);
    systemwide_memory_barrier();
    _membarrier_lock.unlock();
    if (poll()) {
      // raced
      _r._sleeping.store(false, std::memory_order_relaxed);
      return false;
    }
    return true;
  }
  virtual void exit_interrupt_mode() override final { _r._sleeping.store(false, std::memory_order_relaxed); }
};

class reactor::execution_stage_pollfn final : public reactor::pollfn {
  internal::execution_stage_manager &_esm;

 public:
  execution_stage_pollfn() : _esm(internal::execution_stage_manager::get()) {}

  virtual bool poll() override { return _esm.flush(); }
  virtual bool pure_poll() override { return _esm.poll(); }
  virtual bool try_enter_interrupt_mode() override {
    // This is a passive poller, so if a previous poll
    // returned false (idle), there's no more work to do.
    return true;
  }
  virtual void exit_interrupt_mode() override {}
};

alignas(cache_line_size) reactor::smp_pollfn::aligned_flag reactor::smp_pollfn::_membarrier_lock;

class reactor::epoll_pollfn final : public reactor::pollfn {
  reactor &_r;

 public:
  epoll_pollfn(reactor &r) : _r(r) {}
  virtual bool poll() final override { return _r.wait_and_process(); }
  virtual bool pure_poll() override final {
    return poll();  // actually performs work, but triggers no user
                    // continuations, so okay
  }
  virtual bool try_enter_interrupt_mode() override {
    // Since we'll be sleeping in epoll, no need to do anything
    // for interrupt mode.
    return true;
  }
  virtual void exit_interrupt_mode() override final {}
};

void reactor::wakeup() { pthread_kill(_thread_id, alarm_signal()); }

#if defined(ENABLE_AIO)
void reactor::start_aio_eventfd_loop() {
  if (!_aio_eventfd) {
    return;
  }
  future<> loop_done = repeat([this] {
    return _aio_eventfd->readable().then([this] {
      char garbage[8];
      ::read(_aio_eventfd->get_fd(), garbage, 8);  // totally uninteresting
      return _stopping ? stop_iteration::yes : stop_iteration::no;
    });
  });
  // must use make_lw_shared, because at_exit expects a copyable function
  at_exit([loop_done = make_lw_shared(std::move(loop_done))] { return std::move(*loop_done); });
}

void reactor::stop_aio_eventfd_loop() {
  if (!_aio_eventfd) {
    return;
  }
  uint64_t one = 1;
  ::write(_aio_eventfd->get_fd(), &one, 8);
}
#endif

inline bool reactor::have_more_tasks() const { return _active_task_queues.size() + _activating_task_queues.size(); }

void reactor::insert_active_task_queue(task_queue *tq) {
  tq->_active = true;
  auto &atq = _active_task_queues;
  auto less = task_queue::indirect_compare();
  if (atq.empty() || less(atq.back(), tq)) {
    // Common case: idle->working
    // Common case: CPU intensive task queue going to the back
    atq.push_back(tq);
  } else {
    // Common case: newly activated queue preempting everything else
    atq.push_front(tq);
    // Less common case: newly activated queue behind something already active
    size_t i = 0;
    while (i + 1 != atq.size() && !less(atq[i], atq[i + 1])) {
      std::swap(atq[i], atq[i + 1]);
      ++i;
    }
  }
}

void reactor::insert_activating_task_queues() {
  // Quadratic, but since we expect the common cases in
  // insert_active_task_queue() to dominate, faster
  for (auto &&tq : _activating_task_queues) {
    insert_active_task_queue(tq);
  }
  _activating_task_queues.clear();
}

void reactor::run_some_tasks(sched_clock::time_point &t_run_completed) {
  if (!have_more_tasks()) {
    return;
  }
  sched_print("run_some_tasks: start");
  g_need_preempt = true;
  do {
    auto t_run_started = t_run_completed;
    insert_activating_task_queues();
    auto tq = _active_task_queues.front();
    _active_task_queues.pop_front();
    sched_print("running tq {} {}", (void *)tq, tq->_name);
    tq->_current = true;
    run_tasks(*tq);
    tq->_current = false;
    t_run_completed = std::chrono::steady_clock::now();
    auto delta = t_run_completed - t_run_started;
    account_runtime(*tq, delta);
    _last_vruntime = std::max(tq->_vruntime, _last_vruntime);
    sched_print(
        "run complete ({} {}); time consumed {} usec; final vruntime {} empty "
        "{}",
        (void *)tq, tq->_name, delta / 1us, tq->_vruntime, tq->_q.empty());
    if (!tq->_q.empty()) {
      insert_active_task_queue(tq);
    } else {
      tq->_active = false;
    }
  } while (have_more_tasks() && !need_preempt());
  *internal::current_scheduling_group_ptr() = default_scheduling_group();  // Prevent inheritance from last group run
  sched_print("run_some_tasks: end");
}

void reactor::activate(task_queue &tq) {
  if (tq._active) {
    return;
  }
  sched_print("activating {} {}", (void *)&tq, tq._name);
  // If activate() was called, the task queue is likely network-bound or I/O
  // bound, not CPU-bound. As such its vruntime will be low, and it will have a
  // large advantage over other task queues. Limit the advantage so it doesn't
  // dominate scheduling for a long time, in case it _does_ become CPU bound
  // later.
  //
  // FIXME: different scheduling groups have different sensitivity to jitter,
  // take advantage
  auto advantage = tq.to_vruntime(_task_quota);
  if (_last_vruntime - advantage > tq._vruntime) {
    sched_print("tq {} {} losing vruntime {} due to sleep", (void *)&tq, tq._name,
                _last_vruntime - advantage - tq._vruntime);
  }
  tq._vruntime = std::max(_last_vruntime - advantage, tq._vruntime);
  _activating_task_queues.push_back(&tq);
}

int reactor::run() {
  auto signal_stack = install_signal_handler_stack();

  register_metrics();
#if defined(ENABLE_AIO)
  poller io_poller(std::make_unique<io_pollfn>(*this));
  poller aio_poller(std::make_unique<aio_batch_submit_pollfn>(*this));
#endif
  poller sig_poller(std::make_unique<signal_pollfn>(*this));
  poller batch_flush_poller(std::make_unique<batch_flush_pollfn>(*this));
  poller execution_stage_poller(std::make_unique<execution_stage_pollfn>());

#if defined(ENABLE_AIO)
  start_aio_eventfd_loop();
#endif

  if (_id == 0) {
    if (_handle_sigint) {
      _signals.handle_signal_once(SIGINT, [this] { stop(); });
    }
    _signals.handle_signal_once(SIGTERM, [this] { stop(); });
  }

  _cpu_started.wait(smp::count).then([this] {
    _network_stack->initialize().then([this] { _start_promise.set_value(); });
  });
  _network_stack_ready_promise.get_future().then([this](std::unique_ptr<network_stack> stack) {
    _network_stack = std::move(stack);
    for (unsigned c = 0; c < smp::count; c++) {
      smp::submit_to(c, [] { engine()._cpu_started.signal(); });
    }
  });

  // Register smp queues poller
  std::experimental::optional<poller> smp_poller;
  if (smp::count > 1) {
    smp_poller = poller(std::make_unique<smp_pollfn>(*this));
  }

  _signals.handle_signal(alarm_signal(), [this] {
    complete_timers(_timers, _expired_timers, [this] {
      if (!_timers.empty()) {
        enable_timer(_timers.get_next_timeout());
      }
    });
  });

  poller expire_lowres_timers(std::make_unique<lowres_timer_pollfn>(*this));

  using namespace std::chrono_literals;
  timer<lowres_clock> load_timer;
  auto last_idle = _total_idle;
  auto idle_start = sched_clock::now(), idle_end = idle_start;
  load_timer.set_callback([this, &last_idle, &idle_start, &idle_end]() mutable {
    _total_idle += idle_end - idle_start;
    auto load = double((_total_idle - last_idle).count()) /
                double(std::chrono::duration_cast<sched_clock::duration>(1s).count());
    last_idle = _total_idle;
    load = std::min(load, 1.0);
    idle_start = idle_end;
    _loads.push_front(load);
    if (_loads.size() > 5) {
      auto drop = _loads.back();
      _loads.pop_back();
      _load -= (drop / 5);
    }
    _load += (load / 5);
  });
  load_timer.arm_periodic(1s);

  bool idle = false;

  std::function<bool()> check_for_work = [this]() {
    return poll_once() || have_more_tasks() || base::thread::try_run_one_yielded_thread();
  };
  std::function<bool()> pure_check_for_work = [this]() {
    return pure_poll_once() || have_more_tasks() || base::thread::try_run_one_yielded_thread();
  };
  auto t_run_completed = idle_end;
  while (true) {
    run_some_tasks(t_run_completed);
    if (_stopped) {
      load_timer.cancel();
      // Final tasks may include sending the last response to cpu 0, so run them
      while (have_more_tasks()) {
        run_some_tasks(t_run_completed);
      }
      while (!_at_destroy_tasks->_q.empty()) {
        run_tasks(*_at_destroy_tasks);
      }
      smp::arrive_at_event_loop_end();
      if (_id == 0) {
        smp::join_all();
      }
      break;
    }

    increment_nonatomically(_polls);

    if (check_for_work()) {
      idle_end = t_run_completed;
      if (idle) {
        _total_idle += idle_end - idle_start;
        account_idle(idle_end - idle_start);
        idle_start = idle_end;
        idle = false;
      }
    } else {
      idle_end = sched_clock::now();
      if (!idle) {
        idle_start = idle_end;
        idle = true;
      }
      bool go_to_sleep = true;
      try {
        // we can't run check_for_work(), because that can run tasks in the
        // context of the idle handler which change its state, without the idle
        // handler expecting it.  So run pure_check_for_work() instead.
        auto handler_result = _idle_cpu_handler(pure_check_for_work);
        go_to_sleep = handler_result == idle_cpu_handler_result::no_more_work;
      } catch (...) {
        report_exception("Exception while running idle cpu handler", std::current_exception());
      }
      if (go_to_sleep) {
#if defined(__x86_64__) || defined(__i386__)
        _mm_pause();
#endif
        if (idle_end - idle_start > _max_poll_time) {
          // Turn off the task quota timer to avoid spurious wakeups
          auto start_sleep = sched_clock::now();
          sleep();
          // We may have slept for a while, so freshen idle_end
          idle_end = sched_clock::now();
          add_nonatomically(_stall_detector_missed_ticks, uint64_t((start_sleep - idle_end) / _task_quota));
        }
      } else {
        // We previously ran pure_check_for_work(), might not actually have
        // performed any work.
        check_for_work();
      }
    }
    t_run_completed = idle_end;
  }
#if defined(ENABLE_AIO)
  // To prevent ordering issues from rising, destroy the I/O queue explicitly at
  // this point. This is needed because the reactor is destroyed from the
  // thread_local destructors. If the I/O queue happens to use any other
  // infrastructure that is also kept this way (for instance, collectd), we will
  // not have any way to guarantee who is destroyed first.
  my_io_queue.reset(nullptr);
#endif
  return _return;
}

void reactor::sleep() {
  for (auto i = _pollers.begin(); i != _pollers.end(); ++i) {
    auto ok = (*i)->try_enter_interrupt_mode();
    if (!ok) {
      while (i != _pollers.begin()) {
        (*--i)->exit_interrupt_mode();
      }
      return;
    }
  }
  wait_and_process(-1, &_active_sigmask);
  for (auto i = _pollers.rbegin(); i != _pollers.rend(); ++i) {
    (*i)->exit_interrupt_mode();
  }
}

void reactor::maybe_wakeup() {
  std::atomic_signal_fence(std::memory_order_seq_cst);
  wakeup();
#if 0
    if (_sleeping.load(std::memory_order_relaxed)) {
        wakeup();
    }
#endif
}

void reactor::start_epoll() {
  if (!_epoll_poller) {
    _epoll_poller = poller(std::make_unique<epoll_pollfn>(*this));
  }
}

bool reactor::poll_once() {
  bool work = false;
  for (auto c : _pollers) {
    work |= c->poll();
  }

  return work;
}

bool reactor::pure_poll_once() {
  for (auto c : _pollers) {
    if (c->pure_poll()) {
      return true;
    }
  }
  return false;
}

class reactor::poller::registration_task : public task {
 private:
  poller *_p;

 public:
  explicit registration_task(poller *p) : _p(p) {}
  virtual void run() noexcept override {
    if (_p) {
      engine().register_poller(_p->_pollfn.get());
      _p->_registration_task = nullptr;
    }
  }
  void cancel() { _p = nullptr; }
  void moved(poller *p) { _p = p; }
};

class reactor::poller::deregistration_task : public task {
 private:
  std::unique_ptr<pollfn> _p;

 public:
  explicit deregistration_task(std::unique_ptr<pollfn> &&p) : _p(std::move(p)) {}
  virtual void run() noexcept override { engine().unregister_poller(_p.get()); }
};

void reactor::register_poller(pollfn *p) { _pollers.push_back(p); }

void reactor::unregister_poller(pollfn *p) { _pollers.erase(std::find(_pollers.begin(), _pollers.end(), p)); }

void reactor::replace_poller(pollfn *old, pollfn *neww) { std::replace(_pollers.begin(), _pollers.end(), old, neww); }

reactor::poller::poller(poller &&x) : _pollfn(std::move(x._pollfn)), _registration_task(x._registration_task) {
  if (_pollfn && _registration_task) {
    _registration_task->moved(this);
  }
}

reactor::poller &reactor::poller::operator=(poller &&x) {
  if (this != &x) {
    this->~poller();
    new (this) poller(std::move(x));
  }
  return *this;
}

void reactor::poller::do_register() {
  // We can't just insert a poller into reactor::_pollers, because we
  // may be running inside a poller ourselves, and so in the middle of
  // iterating reactor::_pollers itself.  So we schedule a task to add
  // the poller instead.
  auto task = std::make_unique<registration_task>(this);
  auto tmp = task.get();
  engine().add_task(std::move(task));
  _registration_task = tmp;
}

reactor::poller::~poller() {
  // We can't just remove the poller from reactor::_pollers, because we
  // may be running inside a poller ourselves, and so in the middle of
  // iterating reactor::_pollers itself.  So we schedule a task to remove
  // the poller instead.
  //
  // Since we don't want to call the poller after we exit the destructor,
  // we replace it atomically with another one, and schedule a task to
  // delete the replacement.
  if (_pollfn) {
    if (_registration_task) {
      // not added yet, so don't do it at all.
      _registration_task->cancel();
    } else {
      auto dummy = make_pollfn([] { return false; });
      auto dummy_p = dummy.get();
      auto task = std::make_unique<deregistration_task>(std::move(dummy));
      engine().add_task(std::move(task));
      engine().replace_poller(_pollfn.get(), dummy_p);
    }
  }
}

bool reactor_backend_epoll::wait_and_process(int timeout, const sigset_t *active_sigmask) {
  std::array<epoll_event, 128> eevt;
  int nr = ::epoll_pwait(_epollfd.get(), eevt.data(), eevt.size(), timeout, active_sigmask);
  if (nr == -1 && errno == EINTR) {
    return false;  // gdb can cause this
  }
  assert(nr != -1);
  for (int i = 0; i < nr; ++i) {
    auto &evt = eevt[i];
    auto pfd = reinterpret_cast<pollable_fd_state *>(evt.data.ptr);
    auto events = evt.events & (EPOLLIN | EPOLLOUT);
    auto events_to_remove = events & ~pfd->events_requested;
    complete_epoll_event(*pfd, &pollable_fd_state::pollin, events, EPOLLIN);
    complete_epoll_event(*pfd, &pollable_fd_state::pollout, events, EPOLLOUT);
    if (events_to_remove) {
      pfd->events_epoll &= ~events_to_remove;
      evt.events = pfd->events_epoll;
      auto op = evt.events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
      ::epoll_ctl(_epollfd.get(), op, pfd->fd.get(), &evt);
    }
  }
  return nr;
}

smp_message_queue::smp_message_queue(reactor *from, reactor *to) : _pending(to), _completed(from) {}

void smp_message_queue::stop() { _metrics.clear(); }
void smp_message_queue::move_pending() {
  auto begin = _tx.a.pending_fifo.cbegin();
  auto end = _tx.a.pending_fifo.cend();
  end = _pending.push(begin, end);
  if (begin == end) {
    return;
  }
  auto nr = end - begin;
  _pending.maybe_wakeup();
  _tx.a.pending_fifo.erase(begin, end);
  _current_queue_length += nr;
  _last_snt_batch = nr;
  _sent += nr;
}

bool smp_message_queue::pure_poll_tx() const {
  // can't use read_available(), not available on older boost
  // empty() is not const, so need const_cast.
  return !const_cast<lf_queue &>(_completed).empty();
}

void smp_message_queue::submit_item(std::unique_ptr<smp_message_queue::work_item> item) {
  _tx.a.pending_fifo.push_back(item.get());
  item.release();
  if (_tx.a.pending_fifo.size() >= batch_size) {
    move_pending();
  }
}

void smp_message_queue::respond(work_item *item) {
  _completed_fifo.push_back(item);
  if (_completed_fifo.size() >= batch_size || engine()._stopped) {
    flush_response_batch();
  }
}

void smp_message_queue::flush_response_batch() {
  if (!_completed_fifo.empty()) {
    auto begin = _completed_fifo.cbegin();
    auto end = _completed_fifo.cend();
    end = _completed.push(begin, end);
    if (begin == end) {
      return;
    }
    _completed.maybe_wakeup();
    _completed_fifo.erase(begin, end);
  }
}

bool smp_message_queue::has_unflushed_responses() const { return !_completed_fifo.empty(); }

bool smp_message_queue::pure_poll_rx() const {
  // can't use read_available(), not available on older boost
  // empty() is not const, so need const_cast.
  return !const_cast<lf_queue &>(_pending).empty();
}

void smp_message_queue::lf_queue::maybe_wakeup() {
  // Called after lf_queue_base::push().
  //
  // This is read-after-write, which wants memory_order_seq_cst,
  // but we insert that barrier using systemwide_memory_barrier()
  // because seq_cst is so expensive.
  //
  // However, we do need a compiler barrier:
  std::atomic_signal_fence(std::memory_order_seq_cst);
  if (remote->_sleeping.load(std::memory_order_relaxed)) {
    // We are free to clear it, because we're sending a signal now
    remote->_sleeping.store(false, std::memory_order_relaxed);
    remote->wakeup();
  }
}

template <size_t PrefetchCnt, typename Func>
size_t smp_message_queue::process_queue(lf_queue &q, Func process) {
  // copy batch to local memory in order to minimize
  // time in which cross-cpu data is accessed
  work_item *items[queue_length + PrefetchCnt];
  work_item *wi;
  if (!q.pop(wi)) return 0;
  // start prefecthing first item before popping the rest to overlap memory
  // access with potential cache miss the second pop may cause
  prefetch<2>(wi);
  auto nr = q.pop(items);
  std::fill(std::begin(items) + nr, std::begin(items) + nr + PrefetchCnt, nr ? items[nr - 1] : wi);
  unsigned i = 0;
  do {
    prefetch_n<2>(std::begin(items) + i, std::begin(items) + i + PrefetchCnt);
    process(wi);
    wi = items[i++];
  } while (i <= nr);

  return nr + 1;
}

size_t smp_message_queue::process_completions() {
  auto nr = process_queue<prefetch_cnt * 2>(_completed, [](work_item *wi) {
    wi->complete();
    delete wi;
  });
  _current_queue_length -= nr;
  _compl += nr;
  _last_cmpl_batch = nr;

  return nr;
}

void smp_message_queue::flush_request_batch() {
  if (!_tx.a.pending_fifo.empty()) {
    move_pending();
  }
}

size_t smp_message_queue::process_incoming() {
  auto nr =
      process_queue<prefetch_cnt>(_pending, [this](work_item *wi) { wi->process().then([this, wi] { respond(wi); }); });
  _received += nr;
  _last_rcv_batch = nr;
  return nr;
}

void smp_message_queue::start(unsigned cpuid) {
  _tx.init();
  namespace sm = base::metrics;
  char instance[10];
  std::snprintf(instance, sizeof(instance), "%u-%u", engine().cpu_id(), cpuid);
  _metrics.add_group(
      "smp",
      {// queue_length     value:GAUGE:0:U
       // Absolute value of num packets in last tx batch.
       sm::make_queue_length("send_batch_queue_length", _last_snt_batch,
                             sm::description("Current send batch queue length"),
                             {sm::shard_label(instance)})(sm::metric_disabled),
       sm::make_queue_length("receive_batch_queue_length", _last_rcv_batch,
                             sm::description("Current receive batch queue length"),
                             {sm::shard_label(instance)})(sm::metric_disabled),
       sm::make_queue_length("complete_batch_queue_length", _last_cmpl_batch,
                             sm::description("Current complete batch queue length"),
                             {sm::shard_label(instance)})(sm::metric_disabled),
       sm::make_queue_length("send_queue_length", _current_queue_length, sm::description("Current send queue length"),
                             {sm::shard_label(instance)})(sm::metric_disabled),
       // total_operations value:DERIVE:0:U
       sm::make_derive("total_received_messages", _received, sm::description("Total number of received messages"),
                       {sm::shard_label(instance)})(sm::metric_disabled),
       // total_operations value:DERIVE:0:U
       sm::make_derive("total_sent_messages", _sent, sm::description("Total number of sent messages"),
                       {sm::shard_label(instance)})(sm::metric_disabled),
       // total_operations value:DERIVE:0:U
       sm::make_derive("total_completed_messages", _compl, sm::description("Total number of messages completed"),
                       {sm::shard_label(instance)})(sm::metric_disabled)});
}

readable_eventfd writeable_eventfd::read_side() { return readable_eventfd(_fd.dup()); }

file_desc writeable_eventfd::try_create_eventfd(size_t initial) {
  assert(size_t(int(initial)) == initial);
  return file_desc::eventfd(initial, EFD_CLOEXEC);
}

void writeable_eventfd::signal(size_t count) {
  uint64_t c = count;
  auto r = _fd.write(&c, sizeof(c));
  assert(r == sizeof(c));
}

writeable_eventfd readable_eventfd::write_side() { return writeable_eventfd(_fd.get_file_desc().dup()); }

file_desc readable_eventfd::try_create_eventfd(size_t initial) {
  assert(size_t(int(initial)) == initial);
  return file_desc::eventfd(initial, EFD_CLOEXEC | EFD_NONBLOCK);
}

future<size_t> readable_eventfd::wait() {
  return engine().readable(*_fd._s).then([this] {
    uint64_t count;
    int r = ::read(_fd.get_fd(), &count, sizeof(count));
    assert(r == sizeof(count));
    return make_ready_future<size_t>(count);
  });
}

void schedule(std::unique_ptr<task> t) { engine().add_task(std::move(t)); }

void schedule_urgent(std::unique_ptr<task> t) { engine().add_urgent_task(std::move(t)); }

}  // namespace base
}  // namespace stonedb

bool operator==(const ::sockaddr_in a, const ::sockaddr_in b) {
  return (a.sin_addr.s_addr == b.sin_addr.s_addr) && (a.sin_port == b.sin_port);
}

namespace stonedb {
namespace base {

void network_stack_registry::register_stack(sstring name,
                                            std::function<future<std::unique_ptr<network_stack>>()> create,
                                            bool make_default) {
  _map()[name] = std::move(create);
  if (make_default) {
    _default() = name;
  }
}

sstring network_stack_registry::default_stack() { return _default(); }

std::vector<sstring> network_stack_registry::list() {
  std::vector<sstring> ret;
  for (auto &&ns : _map()) {
    ret.push_back(ns.first);
  }
  return ret;
}

future<std::unique_ptr<network_stack>> network_stack_registry::create() { return create(_default()); }

future<std::unique_ptr<network_stack>> network_stack_registry::create(sstring name) {
  if (!_map().count(name)) {
    throw std::runtime_error(sprint("network stack %s not registered", name));
  }
  return _map()[name]();
}

network_stack_registrator::network_stack_registrator(sstring name,
                                                     std::function<future<std::unique_ptr<network_stack>>()> factory,
                                                     bool make_default) {
  network_stack_registry::register_stack(name, factory, make_default);
}

options::options() { idle_poll_time_us = reactor::calculate_poll_time() / 1us; }

thread_local scollectd::impl scollectd_impl;

scollectd::impl &scollectd::get_impl() { return scollectd_impl; }

struct reactor_deleter {
  void operator()(reactor *p) {
    p->~reactor();
    free(p);
  }
};

thread_local std::unique_ptr<reactor, reactor_deleter> reactor_holder;

std::vector<posix_thread> smp::_threads;
std::experimental::optional<boost::barrier> smp::_all_event_loops_done;
std::vector<reactor *> smp::_reactors;
smp_message_queue **smp::_qs;
std::thread::id smp::_tmain;
unsigned smp::count = 1;

void smp::start_all_queues() {
  for (unsigned c = 0; c < count; c++) {
    if (c != engine().cpu_id()) {
      _qs[c][engine().cpu_id()].start(c);
    }
  }
}

void smp::join_all() {
  for (auto &&t : smp::_threads) {
    t.join();
  }
}

void smp::pin(unsigned cpu_id) { pin_this_thread(cpu_id); }

void smp::arrive_at_event_loop_end() {
  if (_all_event_loops_done) {
    _all_event_loops_done->wait();
  }
}

void smp::allocate_reactor(unsigned id) {
  assert(!reactor_holder);

  // we cannot just write "local_engin = new reactor" since reactor's
  // constructor uses local_engine
  void *buf;
  int r = posix_memalign(&buf, cache_line_size, sizeof(reactor));
  assert(r == 0);
  local_engine = reinterpret_cast<reactor *>(buf);
  new (buf) reactor(id);
  reactor_holder.reset(local_engine);
}

void smp::cleanup() { smp::_threads = std::vector<posix_thread>(); }

void smp::cleanup_cpu() {
  size_t cpuid = engine().cpu_id();

  if (_qs) {
    for (unsigned i = 0; i < smp::count; i++) {
      _qs[i][cpuid].stop();
    }
  }
}

void smp::create_thread(std::function<void()> thread_loop) { _threads.emplace_back(std::move(thread_loop)); }

// Installs handler for Signal which ensures that Func is invoked only once
// in the whole program and that after it is invoked the default handler is
// restored.
template <int Signal, void (*Func)()>
void install_oneshot_signal_handler() {
  static bool handled = false;
  static util::spinlock lock;

  struct sigaction sa;
  sa.sa_sigaction = [](int sig, [[maybe_unused]] siginfo_t *info, [[maybe_unused]] void *p) {
    std::lock_guard<util::spinlock> g(lock);
    if (!handled) {
      handled = true;
      Func();
      signal(sig, SIG_DFL);
    }
  };
  sigfillset(&sa.sa_mask);
  sa.sa_flags = SA_SIGINFO | SA_RESTART;
  if (Signal == SIGSEGV) {
    sa.sa_flags |= SA_ONSTACK;
  }
  auto r = ::sigaction(Signal, &sa, nullptr);
  throw_system_error_on(r == -1);
}

static void sigsegv_action() noexcept { print_with_backtrace("Segmentation fault"); }

static void sigabrt_action() noexcept { print_with_backtrace("Aborting"); }

void smp::configure(const options &opt) {
#ifndef NO_EXCEPTION_HACK
  if (opt.enable_glibc_exception_scaling_workaround) {
    init_phdr_cache();
  }
#endif

  // Mask most, to prevent threads (esp. dpdk helper threads)
  // from servicing a signal.  Individual reactors will unmask signals
  // as they become prepared to handle them.
  //
  // We leave some signals unmasked since we don't handle them ourself.
  sigset_t sigs;
  sigfillset(&sigs);
  for (auto sig :
       {SIGHUP, SIGQUIT, SIGILL, SIGABRT, SIGFPE, SIGSEGV, SIGALRM, SIGCONT, SIGSTOP, SIGTSTP, SIGTTIN, SIGTTOU}) {
    sigdelset(&sigs, sig);
  }
  pthread_sigmask(SIG_BLOCK, &sigs, nullptr);

  install_oneshot_signal_handler<SIGSEGV, sigsegv_action>();
  install_oneshot_signal_handler<SIGABRT, sigabrt_action>();

  auto thread_affinity = opt.thread_affinity;
  if (opt.overprovisioned) {
    thread_affinity = false;
  }

  auto mbind = opt.mbind;
  if (!thread_affinity) {
    mbind = false;
  }

  smp::count = 1;
  smp::_tmain = std::this_thread::get_id();
  auto nr_cpus = resource::nr_processing_units();
  resource::cpuset cpu_set;

  std::copy(boost::counting_iterator<unsigned>(0), boost::counting_iterator<unsigned>(nr_cpus),
            std::inserter(cpu_set, cpu_set.end()));
  if (opt.smp) {
    nr_cpus = *opt.smp;
  } else {
    nr_cpus = cpu_set.size();
  }
  smp::count = nr_cpus;
  _reactors.resize(nr_cpus);
  resource::configuration rc;
  if (opt.memory) rc.total_memory = parse_memory_size(*opt.memory);
  if (opt.reserve_memory) rc.reserve_memory = parse_memory_size(*opt.reserve_memory);

  auto mlock = opt.lock_memory;
  if (mlock) {
    auto r = mlockall(MCL_CURRENT | MCL_FUTURE);
    if (r) {
      // Don't hard fail for now, it's hard to get the configuration right
      print("warning: failed to mlockall: %s\n", strerror(errno));
    }
  }

  rc.cpus = smp::count;
  rc.cpu_set = std::move(cpu_set);
  rc.max_io_requests = opt.max_io_requests;
  rc.io_queues = opt.num_io_queues;

  auto resources = resource::allocate(rc);
  std::vector<resource::cpu> allocations = std::move(resources.cpus);
  if (thread_affinity) {
    smp::pin(allocations[0].cpu_id);
  }

  // Better to put it into the smp class, but at smp construction time
  // correct smp::count is not known.
  std::shared_ptr<boost::barrier> reactors_registered = std::make_shared<boost::barrier>(smp::count);
  std::shared_ptr<boost::barrier> smp_queues_constructed = std::make_shared<boost::barrier>(smp::count);
  std::shared_ptr<boost::barrier> inited = std::make_shared<boost::barrier>(smp::count);

#if defined(ENABLE_AIO)
  auto io_info = std::move(resources.io_queues);

  std::vector<io_queue *> all_io_queues;
  all_io_queues.resize(io_info.coordinators.size());
  io_queue::fill_shares_array();

  auto alloc_io_queue = [io_info, &all_io_queues](unsigned shard) {
    auto cid = io_info.shard_to_coordinator[shard];
    int vec_idx = 0;
    for (auto &coordinator : io_info.coordinators) {
      if (coordinator.id != cid) {
        vec_idx++;
        continue;
      }
      if (shard == cid) {
        all_io_queues[vec_idx] = new io_queue(coordinator.id, coordinator.capacity, io_info.shard_to_coordinator);
      }
      return vec_idx;
    }
    assert(0);  // Impossible
  };

  auto assign_io_queue = [&all_io_queues](shard_id id, int queue_idx) {
    if (all_io_queues[queue_idx]->coordinator() == id) {
      engine().my_io_queue.reset(all_io_queues[queue_idx]);
    }
    engine()._io_queue = all_io_queues[queue_idx];
    engine()._io_coordinator = all_io_queues[queue_idx]->coordinator();
  };
#endif

  _all_event_loops_done.emplace(smp::count);

  unsigned i;
  for (i = 1; i < smp::count; i++) {
    auto allocation = allocations[i];
#if defined(ENABLE_AIO)
    create_thread([opt, i, allocation, assign_io_queue, alloc_io_queue, thread_affinity, mbind, reactors_registered,
                   smp_queues_constructed, inited] {
#else
    create_thread([opt, i, allocation, thread_affinity, mbind, reactors_registered, smp_queues_constructed, inited] {
#endif
      auto thread_name = base::format("reactor-{}", i);
      pthread_setname_np(pthread_self(), thread_name.c_str());
      if (thread_affinity) {
        smp::pin(allocation.cpu_id);
      }

      sigset_t mask;
      sigfillset(&mask);
      for (auto sig : {SIGSEGV}) {
        sigdelset(&mask, sig);
      }
      auto r = ::pthread_sigmask(SIG_BLOCK, &mask, NULL);
      throw_pthread_error(r);
      allocate_reactor(i);
      _reactors[i] = &engine();
#if defined(ENABLE_AIO)
      auto queue_idx = alloc_io_queue(i);
#endif
      reactors_registered->wait();
      smp_queues_constructed->wait();
      start_all_queues();
#if defined(ENABLE_AIO)
      assign_io_queue(i, queue_idx);
#endif
      inited->wait();
      engine().configure(opt);
      engine().run();
    });
  }

  allocate_reactor(0);
  _reactors[0] = &engine();
#if defined(ENABLE_AIO)
  auto queue_idx = alloc_io_queue(0);
#endif

  reactors_registered->wait();
  smp::_qs = new smp_message_queue *[smp::count];
  for (unsigned i = 0; i < smp::count; i++) {
    smp::_qs[i] = reinterpret_cast<smp_message_queue *>(operator new[](sizeof(smp_message_queue) * smp::count));
    for (unsigned j = 0; j < smp::count; ++j) {
      new (&smp::_qs[i][j]) smp_message_queue(_reactors[j], _reactors[i]);
    }
  }
  smp_queues_constructed->wait();
  start_all_queues();
#if defined(ENABLE_AIO)
  assign_io_queue(0, queue_idx);
#endif
  inited->wait();

  engine().configure(opt);
  // The raw `new` is necessary because of the private constructor of
  // `lowres_clock_impl`. engine()._lowres_clock_impl =
  // std::unique_ptr<lowres_clock_impl>(new lowres_clock_impl);
}

bool smp::poll_queues() {
  size_t got = 0;
  for (unsigned i = 0; i < count; i++) {
    if (engine().cpu_id() != i) {
      auto &rxq = _qs[engine().cpu_id()][i];
      rxq.flush_response_batch();
      got += rxq.has_unflushed_responses();
      got += rxq.process_incoming();
      auto &txq = _qs[i][engine()._id];
      txq.flush_request_batch();
      got += txq.process_completions();
    }
  }
  return got != 0;
}

bool smp::pure_poll_queues() {
  for (unsigned i = 0; i < count; i++) {
    if (engine().cpu_id() != i) {
      auto &rxq = _qs[engine().cpu_id()][i];
      rxq.flush_response_batch();
      auto &txq = _qs[i][engine()._id];
      txq.flush_request_batch();
      if (rxq.pure_poll_rx() || txq.pure_poll_tx() || rxq.has_unflushed_responses()) {
        return true;
      }
    }
  }
  return false;
}

__thread bool g_need_preempt;

__thread reactor *local_engine;

class reactor_notifier_epoll : public reactor_notifier {
  writeable_eventfd _write;
  readable_eventfd _read;

 public:
  reactor_notifier_epoll() : _write(), _read(_write.read_side()) {}
  virtual future<> wait() override {
    // convert _read.wait(), a future<size_t>, to a future<>:
    return _read.wait().then([]([[maybe_unused]] size_t ignore) { return make_ready_future<>(); });
  }
  virtual void signal() override { _write.signal(1); }
};

std::unique_ptr<reactor_notifier> reactor_backend_epoll::make_reactor_notifier() {
  return std::make_unique<reactor_notifier_epoll>();
}

void report_exception(std::experimental::string_view message, std::exception_ptr eptr) noexcept {
  seastar_logger.error("{}: {}", message, eptr);
}

/**
 * engine_exit() exits the reactor. It should be given a pointer to the
 * exception which prompted this exit - or a null pointer if the exit
 * request was not caused by any exception.
 */
void engine_exit(std::exception_ptr eptr) {
  if (!eptr) {
    engine().exit(0);
    return;
  }
  report_exception("Exiting on unhandled exception", eptr);
  engine().exit(1);
}

void report_failed_future([[maybe_unused]] std::exception_ptr eptr) {
  // seastar_logger.warn("Exceptional future ignored: {}, backtrace: {}", eptr,
  // current_backtrace());
}

server_socket listen(socket_address sa) { return engine().listen(sa); }

server_socket listen(socket_address sa, listen_options opts) { return engine().listen(sa, opts); }

future<connected_socket> connect(socket_address sa) { return engine().connect(sa); }

future<connected_socket> connect(socket_address sa, socket_address local, transport proto = transport::TCP) {
  return engine().connect(sa, local, proto);
}

void reactor::add_high_priority_task(std::unique_ptr<task> &&t) {
  add_urgent_task(std::move(t));
  // break .then() chains
  g_need_preempt = true;
}

static bool virtualized() {
  return false;
  // return boost::filesystem::exists("/sys/hypervisor/type");
}

std::chrono::nanoseconds reactor::calculate_poll_time() {
  // In a non-virtualized environment, select a poll time
  // that is competitive with halt/unhalt.
  //
  // In a virutalized environment, IPIs are slow and dominate
  // sleep/wake (mprotect/tgkill), so increase poll time to reduce
  // so we don't sleep in a request/reply workload
  return virtualized() ? 2000us : 200us;
}

future<> later() {
  promise<> p;
  auto f = p.get_future();
  engine().force_poll();
  schedule(make_task(default_scheduling_group(), [p = std::move(p)]() mutable { p.set_value(); }));
  return f;
}

void add_to_flush_poller(output_stream<char> *os) { engine()._flush_batching.emplace_back(os); }

network_stack_registrator nsr_posix{
    "posix", []() { return smp::main_thread() ? posix_network_stack::create() : posix_ap_network_stack::create(); },
    true};

#ifndef NO_EXCEPTION_INTERCEPT
//}
// namespace stonedb::base {
#endif

reactor::sched_clock::duration reactor::total_idle_time() { return _total_idle; }

reactor::sched_clock::duration reactor::total_busy_time() { return sched_clock::now() - _start_time - _total_idle; }

void reactor::init_scheduling_group(base::scheduling_group sg, sstring name, float shares) {
  _task_queues.resize(std::max<size_t>(_task_queues.size(), sg._id + 1));
  _task_queues[sg._id] = std::make_unique<task_queue>(sg._id, name, shares);
}

const sstring &scheduling_group::name() const { return engine()._task_queues[_id]->_name; }

void scheduling_group::set_shares(float shares) { engine()._task_queues[_id]->set_shares(shares); }

future<scheduling_group> create_scheduling_group(sstring name, float shares) {
  static std::atomic<unsigned> last{2};  // 0=main, 1=atexit
  auto id = last.fetch_add(1);
  assert(id < max_scheduling_groups());
  auto sg = scheduling_group(id);
  return smp::invoke_on_all([sg, name, shares] { engine().init_scheduling_group(sg, name, shares); }).then([sg] {
    return make_ready_future<scheduling_group>(sg);
  });
}

}  // namespace base
}  // namespace stonedb
