/* Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
   Use is subject to license terms

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335 USA
*/

#include <boost/lockfree/queue.hpp>

#include "core/app_template.h"
#include "core/future.h"
#include "core/task_executor.h"

#ifndef DISABLE_USED_FOR_STONEDB
#include "core/engine.h"
#include "system/rc_system.h"
#endif

namespace stonedb {
namespace core {
class AppTemplate {
  static constexpr size_t queue_length = 128;

 public:
  explicit AppTemplate(base::reactor &main_reactor)
      : _main_reactor(main_reactor),
        _tasks_queue(queue_length),
        _tasks_poller(base::reactor::poller::simple([this] { return PollTask(); }, true)) {}

  ~AppTemplate() {
    _tasks_queue.consume_all([](TaskExecutor::Task *task) { delete task; });
  }

  void AddTask(std::unique_ptr<TaskExecutor::Task> task) {
    _tasks_queue.push(task.release());
    _main_reactor.maybe_wakeup();
  }

  void Exit() { _stopped.store(true); }

 private:
  bool PollTask() {
    bool pollabled = false;
    TaskExecutor::Task *task = nullptr;
    while (_tasks_queue.pop(task)) {
      task->Run();
      delete task;
      pollabled = true;
    }

    if (_stopped.load(std::memory_order_relaxed)) base::engine().exit(0);

    return pollabled;
  }

  base::reactor &_main_reactor;
  boost::lockfree::queue<TaskExecutor::Task *> _tasks_queue;
  std::atomic<bool> _stopped = {false};
  base::reactor::poller _tasks_poller;
};

TaskExecutor::TaskExecutor() {}
TaskExecutor::~TaskExecutor() {}

std::future<void> TaskExecutor::Init(unsigned threads) {
  std::shared_ptr<std::promise<void>> promise = std::make_shared<std::promise<void>>();
  std::future<void> ready_future = promise->get_future();

  _main_thread = std::thread([this, threads, promise]() {
    base::app_template app(base::create_default_options(threads));
    app.run_deprecated([this, promise] {
      ::pthread_setname_np(::pthread_self(), "reactor-main");
      _app_template = std::make_unique<AppTemplate>(base::engine());
      base::engine().at_destroy([this]() { _app_template.reset(); });
      promise->set_value();
    });
  });

  return ready_future;
}

void TaskExecutor::Exit() {
  _app_template->Exit();
  _main_thread.join();
}

void TaskExecutor::AddTask(std::unique_ptr<Task> task) { _app_template->AddTask(std::move(task)); }

#ifndef DISABLE_USED_FOR_STONEDB
TaskExecutor *GetTaskExecutor() { return rceng->GetTaskExecutor(); }
#endif

}  // namespace core
}  // namespace stonedb
