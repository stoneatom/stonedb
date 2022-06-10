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
#ifndef STONEDB_BASE_APP_TEMPLATE_H_
#define STONEDB_BASE_APP_TEMPLATE_H_
#pragma once

#include <chrono>
#include <functional>

#include <core/future.h>
#include <core/reactor.h>
#include <core/sstring.h>
#include <boost/optional.hpp>

namespace stonedb {
namespace base {

options create_default_options(unsigned threads,
                               const std::chrono::duration<double> &default_task_quota = std::chrono::milliseconds(2));

class app_template {
 private:
  options _opt;

 public:
  explicit app_template(options opt = options());

  int run_deprecated(std::function<void()> &&func);

  // Runs given function and terminates the application when the future it
  // returns resolves. The value with which the future resolves will be
  // returned by this function.
  int run(std::function<future<int>()> &&func);

  // Like run_sync() which takes std::function<future<int>()>, but returns
  // with exit code 0 when the future returned by func resolves
  // successfully.
  int run(std::function<future<>()> &&func);
};

}  // namespace base
}  // namespace stonedb

#endif  // STONEDB_BASE_ALIGNED_BUFFER_H__
