/*
 * Copyright 2017 ScyllaDB
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
#ifndef STONEDB_BASE_FSQUAL_H_
#define STONEDB_BASE_FSQUAL_H_
#pragma once

#include "base/core/sstring.h"

namespace stonedb {
namespace base {

bool filesystem_has_good_aio_support(sstring directory, bool verbose = false);

}  // namespace base
}  // namespace stonedb

#endif  // STONEDB_BASE_FSQUAL_H_
