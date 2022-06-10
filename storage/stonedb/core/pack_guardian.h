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
#ifndef STONEDB_CORE_PACK_GUARDIAN_H_
#define STONEDB_CORE_PACK_GUARDIAN_H_
#pragma once

#include <mutex>
#include <vector>

namespace stonedb {
namespace vcolumn {
class VirtualColumn;
}  // namespace vcolumn
namespace core {
class MIIterator;

class VCPackGuardian final {
 public:
  VCPackGuardian(vcolumn::VirtualColumn *vc) : my_vc(*vc) {}
  ~VCPackGuardian() = default;

  void LockPackrow(const MIIterator &mit);
  void UnlockAll();

 private:
  void Initialize(int no_th);
  void ResizeLastPack(int taskNum);  // used only when Initialize is done

  vcolumn::VirtualColumn &my_vc;
  bool initialized{false};  // false if the object was not initialized yet.
  // To be done at the first use, because it is too risky to init in constructor
  // (multiindex may not be valid yet)

  // Structures used for LOCK_ONE
  std::vector<std::vector<int>> last_pack;

  int threads{1};  // number of parallel threads using the guardian
  std::mutex mx_thread;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_PACK_GUARDIAN_H_
