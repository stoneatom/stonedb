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
#ifndef STONEDB_CORE_ROUGH_VALUE_H_
#define STONEDB_CORE_ROUGH_VALUE_H_
#pragma once

#include "common/common_definitions.h"

namespace stonedb {
namespace core {
class RoughValue {
  int64_t min;
  int64_t max;

 public:
  RoughValue(int64_t min = common::MINUS_INF_64, int64_t max = common::PLUS_INF_64);
  int64_t GetMin() const { return min; }
  int64_t GetMax() const { return max; }
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_ROUGH_VALUE_H_
