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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 * Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
 */

#include "app_template.h"

#include <boost/make_shared.hpp>
#include <cstdlib>
#include <fstream>

#include "base/core/metrics_api.h"
#include "base/core/print.h"
#include "base/core/reactor.h"
#include "base/core/scollectd.h"
#include "base/util/log.h"
#include "base/util/log_cli.h"

namespace stonedb {
namespace base {

using namespace std::chrono_literals;

options create_default_options(unsigned threads, const std::chrono::duration<double> &default_task_quota) {
  options opt;
  opt.smp = threads;
  opt.task_quota_ms = default_task_quota / 1ms;
  return opt;
}

app_template::app_template(options opt) : _opt(std::move(opt)) {}

int app_template::run(std::function<future<int>()> &&func) {
  return run_deprecated([func = std::move(func)]() mutable {
    auto func_done = make_lw_shared<promise<>>();
    engine().at_exit([func_done] { return func_done->get_future(); });
    futurize_apply(func)
        .finally([func_done] { func_done->set_value(); })
        .then([](int exit_code) { return engine().exit(exit_code); })
        .or_terminate();
  });
}

int app_template::run(std::function<future<>()> &&func) {
  return run([func = std::move(func)] { return func().then([]() { return 0; }); });
}

int app_template::run_deprecated(std::function<void()> &&func) {
#ifdef DEBUG
  print("WARNING: debug mode. Not for benchmarking or production\n");
#endif
  // Needs to be before `smp::configure()`.
  try {
    apply_logging_settings(log_cli::create_default_logging_settings());
  } catch (const std::runtime_error &exn) {
    std::cout << "logging configuration error: " << exn.what() << '\n';
    return 1;
  }

  smp::configure(_opt);

  engine()
      .when_started()
      .then([this] {
        base::metrics::configure().then([this] {
          // set scollectd use the metrics configuration, so the later
          // need to be set first
          scollectd::configure();
        });
      })
      .then(std::move(func))
      .then_wrapped([](auto &&f) {
        try {
          f.get();
        } catch (std::exception &ex) {
          std::cout << "program failed with uncaught exception: " << ex.what() << "\n";
          engine().exit(1);
        }
      });
  auto exit_code = engine().run();
  smp::cleanup();
  return exit_code;
}

}  // namespace base
}  // namespace stonedb
