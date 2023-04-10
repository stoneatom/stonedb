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
#ifndef TIANMU_CORE_CTASK_H_
#define TIANMU_CORE_CTASK_H_
#pragma once

#include <unordered_map>

namespace Tianmu {
namespace core {
struct CTask {
  int dwTaskId;     // task id
  int dwPackNum;    // previous task +  this task Actual process pack num
  int dwEndPackno;  // the last packno of this task
  int dwTuple;      // previous task + this task Actual  process tuple
  int dwStartPackno;
  int dwStartTuple;
  int dwEndTuple;
  std::unordered_map<int, int> *dwPack2cur;
  CTask()
      : dwTaskId(0),
        dwPackNum(0),
        dwEndPackno(0),
        dwTuple(0),
        dwStartPackno(0),
        dwStartTuple(0),
        dwEndTuple(0),
        dwPack2cur(nullptr) {}
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_CTASK_H_
