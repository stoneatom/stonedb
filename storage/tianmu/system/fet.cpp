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

#include "fet.h"

#include <mutex>

namespace Tianmu {
namespace system {
#ifdef FUNCTIONS_EXECUTION_TIMES

void NotifyDataPackLoad(const core::PackCoordinate &coord) {
  static std::mutex mtx;
  std::scoped_lock guard(mtx);
  count_distinct_dp_loads.insert(coord);
}

void NotifyDataPackDecompression(const core::PackCoordinate &coord) {
  static std::mutex mtx;
  std::scoped_lock guard(mtx);
  count_distinct_dp_decompressions.insert(coord);
}

#endif
}  // namespace system
}  // namespace Tianmu
