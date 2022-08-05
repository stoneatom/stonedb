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
 * Copyright (C) 2017 IBM.
 * Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
 */
#ifndef TIANMU_BASE_CACHELINE_H_
#define TIANMU_BASE_CACHELINE_H_
#pragma once

#include <cstddef>

namespace Tianmu {
namespace base {

// Platform-dependent cache line size for alignment and padding purposes.
static constexpr size_t cache_line_size =
#if defined(__x86_64__) || defined(__i386__)
    64;
#elif defined(__s390x__) || defined(__zarch__)
    256;
#elif defined(__ppc64__)
    128;
#else
#error "cache_line_size not defined for this architecture"
#endif

}  // namespace base
}  // namespace Tianmu

#endif  // TIANMU_BASE_CACHELINE_H_
