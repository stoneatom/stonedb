/*
 * Copyright 2016 ScyllaDB
 * Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
 */
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

#include "fsqual.h"

#if defined(ENABLE_AIO)
#include <libaio.h>
#endif

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
#include <cstdlib>
#include <iostream>
#include <type_traits>

#include "base/core/posix.h"
#include "base/util/defer.h"

namespace stonedb {
namespace base {

// Runs func(), and also adds the number of context switches
// that happened during func() to counter.
template <typename Counter, typename Func>
typename std::result_of<Func()>::type with_ctxsw_counting(Counter &counter, Func &&func) {
  struct count_guard {
    Counter &counter;
    count_guard(Counter &counter) : counter(counter) { counter -= nvcsw(); }
    ~count_guard() { counter += nvcsw(); }
    static Counter nvcsw() {
      struct rusage usage;
      getrusage(RUSAGE_THREAD, &usage);
      return usage.ru_nvcsw;
    }
  };
  count_guard g(counter);
  return func();
}

bool filesystem_has_good_aio_support([[maybe_unused]] sstring directory, [[maybe_unused]] bool verbose) {
#if defined(ENABLE_AIO)
  io_context_t ioctx = {};
  auto r = io_setup(1, &ioctx);
  throw_kernel_error(r);

  auto cleanup = defer([&] { io_destroy(ioctx); });
  auto fname = directory + "/fsqual.tmp";
  auto fd = file_desc::open(fname, O_CREAT | O_EXCL | O_RDWR | O_DIRECT, 0600);
  unlink(fname.c_str());
  auto nr = 1000;
  fd.truncate(nr * 4096);
  auto bufsize = 4096;
  auto ctxsw = 0;
  auto buf = aligned_alloc(4096, 4096);

  for (int i = 0; i < nr; ++i) {
    struct iocb cmd;
    io_prep_pwrite(&cmd, fd.get(), buf, bufsize, bufsize * i);
    struct iocb *cmds[1] = {&cmd};
    with_ctxsw_counting(ctxsw, [&] {
      auto r = io_submit(ioctx, 1, cmds);
      throw_kernel_error(r);
      assert(r == 1);
    });

    struct io_event ioev;
    auto n = io_getevents(ioctx, 1, 1, &ioev, nullptr);
    throw_kernel_error(n);
    assert(n == 1);
    throw_kernel_error(long(ioev.res));
    assert(long(ioev.res) == bufsize);
  }

  auto rate = float(ctxsw) / nr;
  bool ok = rate < 0.1;
  if (verbose) {
    auto verdict = ok ? "GOOD" : "BAD";
    std::cout << "context switch per appending io: " << rate << " (" << verdict << ")\n";
  }
  return ok;
#else
  return false;
#endif
}

}  // namespace base
}  // namespace stonedb
