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
  enum class GUARDIAN_LOCK_STRATEGY { LOCK_ONE, LOCK_ONE_THREAD, LOCK_ALL, LOCK_LRU };

 public:
  VCPackGuardian(vcolumn::VirtualColumn *vc) : my_vc_(*vc) {}
  ~VCPackGuardian() = default;

  void LockPackrow(const MIIterator &mit);
  void LockPackrowOnLockOne(const MIIterator &mit);
  void LockPackrowOnLockOneByThread(const MIIterator &mit);
  void LockPackrowOnLockAll(const MIIterator &mit);
  void LockPackrowOnLockLRU(const MIIterator &mit);
  void UnlockAll();
  void UnlockAllOnLockOne();
  void UnlockAllOnLockOneByThread();
  void UnlockAllOnLockAll();
  void UnlockAllOnLockLRU();

 private:
  void Initialize(int no_th);
  void ResizeLastPack(int taskNum);  // used only when Initialize is done

  vcolumn::VirtualColumn &my_vc_;
  bool initialized_{false};  // false if the object was not initialized yet.
  // To be done at the first use, because it is too risky to init in constructor
  // (multiindex may not be valid yet)

  // Structures used for LOCK_ONE
  std::vector<std::vector<int>> last_pack_;

  // thread_id::cur_dim::col_index -> pack
  // TODO: A parallel hash table implementation with multithreading security
  // https://greg7mdp.github.io/parallel-hashmap/
  std::unordered_map<uint64_t, std::unordered_map<int, std::unordered_map<int, int>>> last_pack_thread_;

  int guardian_threads_{1};  // number of parallel threads using the guardian
  std::mutex mx_thread_;

  GUARDIAN_LOCK_STRATEGY current_strategy_ = GUARDIAN_LOCK_STRATEGY::LOCK_ONE_THREAD;
  std::unordered_map<int, std::unordered_map<int, char>> lock_packs_;
  std::unordered_map<int, std::vector<int>> lru_packs_;

 private:
  constexpr static int LOCK_LRU_LIMIT = 10000;
  constexpr static int LOCK_LRU_ONCE_ERASE = 10;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_PACK_GUARDIAN_H_
