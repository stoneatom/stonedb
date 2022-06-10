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
#ifndef STONEDB_COMMON_ASSERT_H_
#define STONEDB_COMMON_ASSERT_H_
#pragma once

#include <cassert>

#include "common/exception.h"

namespace stonedb {

#define ASSERT_1(condition)                                              \
  do {                                                                   \
    if (!(condition)) {                                                  \
      throw common::AssertException(#condition, __FILE__, __LINE__, ""); \
    }                                                                    \
  } while (0)

#define ASSERT_2(condition, msg)                                            \
  do {                                                                      \
    if (!(condition)) {                                                     \
      throw common::AssertException(#condition, __FILE__, __LINE__, (msg)); \
    }                                                                       \
  } while (0)

#define GET_MACRO(_1, _2, NAME, ...) NAME
#define ASSERT(...) GET_MACRO(__VA_ARGS__, ASSERT_2, ASSERT_1)(__VA_ARGS__)
#define STONEDB_ERROR(message) ASSERT(false, message)

#ifdef DBUG_OFF
#define DEBUG_ASSERT(condition) \
  do {                          \
  } while (false)
#else
#define DEBUG_ASSERT(...) assert(__VA_ARGS__)
#endif

}  // namespace stonedb
#endif  // STONEDB_COMMON_ASSERT_H_
