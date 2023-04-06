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
#ifndef TIANMU_CORE_PACK_GUARDIAN_H_
#define TIANMU_CORE_PACK_GUARDIAN_H_
#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>

namespace Tianmu {
namespace vcolumn {
class VirtualColumn;
}  // namespace vcolumn
namespace core {
class MIIterator;

class VCPackGuardian final {
 public:
  enum class GUARDIAN_LOCK_STRATEGY { LOCK_ONE, LOCK_ONE_THREAD };

 public:
  VCPackGuardian(vcolumn::VirtualColumn *vc) : my_vc_(*vc) {}
  ~VCPackGuardian() = default;

  void LockPackrow(const MIIterator &mit);
  void LockPackrowOnLockOne(const MIIterator &mit);
  void LockPackrowOnLockOneByThread(const MIIterator &mit);
  void UnlockAll();
  void UnlockAllOnLockOne();
  void UnlockAllOnLockOneByThread();

 private:
  void Initialize(int no_th);
  void ResizeLastPack(int taskNum);  // used only when Initialize is done

  vcolumn::VirtualColumn &my_vc_;
  bool initialized_{false};  // false if the object was not initialized yet.
  // To be done at the first use, because it is too risky to init in constructor
  // (multiindex may not be valid yet)

  // Structures used for LOCK_ONE
  std::vector<std::vector<int>> last_pack_;

  // 1. Use the pthread thread identifier of glibc as a thread unique identifier.
  //    Internal encapsulation is no longer used
  // 2. A single worker thread holds only one pack of vc of a particular dim
  // 3. During each LockPackrow, hold packs are identified by the pthread ID.
  //    If the read pack is different from the held pack, the corresponding pack is released
  using TypeLockOne = std::unordered_map<uint64_t, std::unordered_map<int, std::unordered_map<int, int>>>;
  TypeLockOne last_pack_thread_;

  int guardian_threads_{1};  // number of parallel threads using the guardian
  std::mutex mx_thread_;

  GUARDIAN_LOCK_STRATEGY current_strategy_ = GUARDIAN_LOCK_STRATEGY::LOCK_ONE_THREAD;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_PACK_GUARDIAN_H_
