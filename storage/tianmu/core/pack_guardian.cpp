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

#include <pthread.h>

#include "pack_guardian.h"

#include "core/just_a_table.h"
#include "core/mi_iterator.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
void VCPackGuardian::Initialize(int no_th) {
  UnlockAll();
  last_pack_.clear();

  guardian_threads_ = no_th;

  int no_dims = -1;
  for (auto const &iter : my_vc_.GetVarMap())
    if (iter.dim > no_dims)
      no_dims = iter.dim;  // find the maximal number of dimension used
  no_dims++;
  if (no_dims > 0) {  // else constant
    last_pack_.reserve(no_dims);
    for (int i = 0; i < no_dims; ++i) last_pack_.emplace_back(guardian_threads_, common::NULL_VALUE_32);
  }
  initialized_ = true;
}

/*
 * ResizeLastPack handles last_pack_ overflow under multi-threads group by
 */
void VCPackGuardian::ResizeLastPack(int taskNum) {
  if (!initialized_)
    return;

  for (auto &v : last_pack_) v.resize(taskNum, common::NULL_VALUE_32);

  guardian_threads_ = taskNum;
}

void VCPackGuardian::LockPackrow(const MIIterator &mit) {
  switch (current_strategy_) {
    case GUARDIAN_LOCK_STRATEGY::LOCK_ONE:
      return LockPackrowOnLockOne(mit);
    case GUARDIAN_LOCK_STRATEGY::LOCK_ONE_THREAD:
      return LockPackrowOnLockOneByThread(mit);
    default:
      TIANMU_LOG(LogCtl_Level::ERROR, "LockPackrow fail, unkown current_strategy_: %d",
                 static_cast<int>(current_strategy_));
      ASSERT(0, "VCPackGuardian::LockPackrow fail, unkown current_strategy_: " + static_cast<int>(current_strategy_));
  }
}

// 1. Use the pthread thread identifier of glibc as a thread unique identifier.
//    Internal encapsulation is no longer used
// 2. A single worker thread holds only one pack of vc of a particular dim
// 3. During each LockPackrow, hold packs are identified by the pthread ID.
//    If the read pack is different from the held pack, the corresponding pack is released
void VCPackGuardian::LockPackrowOnLockOneByThread(const MIIterator &mit) {
  const auto &var_map = my_vc_.GetVarMap();
  if (var_map.empty()) {
    return;
  }

  uint64_t thread_id = pthread_self();

  TypeLockOne::iterator iter_thread;

  {
    std::scoped_lock lock(mx_thread_);
    iter_thread = last_pack_thread_.find(thread_id);
  }

  bool has_myself_thread = last_pack_thread_.end() != iter_thread;

  for (auto iter = var_map.cbegin(); iter != var_map.cend(); iter++) {
    int cur_dim = iter->dim;
    int col_index = iter->col_ndx;
    int cur_pack = mit.GetCurPackrow(cur_dim);

    if (!iter->GetTabPtr()) {
      continue;
    }

    JustATable *tab = iter->GetTabPtr().get();

    if (has_myself_thread) {
      auto iter_dim = iter_thread->second.find(cur_dim);
      if (iter_thread->second.end() != iter_dim) {
        auto iter_index = iter_dim->second.find(col_index);
        if (iter_dim->second.end() != iter_index) {
          int last_pack = iter_index->second;
          if (last_pack == cur_pack) {
            continue;
          }

          iter_dim->second.erase(col_index);
          tab->UnlockPackFromUse(col_index, last_pack);
        }
      }
    }

    try {
      tab->LockPackForUse(col_index, cur_pack);
    } catch (...) {
      TIANMU_LOG(LogCtl_Level::ERROR,
                 "LockPackrowOnLockOneByThread LockPackForUse fail, cur_dim: %d col_index: %d cur_pack: %d", cur_dim,
                 col_index, cur_pack);

      if (has_myself_thread) {
        auto it = var_map.begin();
        for (; it != iter; ++it) {
          int cur_dim = iter->dim;
          int col_index = iter->col_ndx;
          auto iter_dim = iter_thread->second.find(cur_dim);
          if (iter_thread->second.end() == iter_dim) {
            continue;
          }

          int it_cur_pack = mit.GetCurPackrow(it->dim);
          auto it_iter_pack = iter_dim->second.find(col_index);
          if (iter_dim->second.end() == it_iter_pack) {
            continue;
          }

          tab->UnlockPackFromUse(it->col_ndx, it_cur_pack);
        }
      }

      UnlockAll();
      throw;
    }
  }

  for (auto iter = var_map.cbegin(); iter != var_map.cend(); iter++) {
    if (!iter->GetTabPtr()) {
      continue;
    }

    int cur_dim = iter->dim;
    int col_index = iter->col_ndx;
    int cur_pack = mit.GetCurPackrow(cur_dim);

    {
      if (has_myself_thread) {
        auto iter_dim = iter_thread->second.find(cur_dim);
        if (iter_thread->second.end() != iter_dim) {
          auto iter_index = iter_dim->second.find(col_index);
          if (iter_dim->second.end() != iter_index) {
            int last_pack = iter_index->second;
            if (last_pack == cur_pack) {
              continue;
            }
          }
        }
      }
    }

    if (!has_myself_thread) {
      std::unordered_map<int, std::unordered_map<int, int>> pack_value;
      auto &lock_dim = pack_value[cur_dim];
      lock_dim[col_index] = cur_pack;

      {
        std::scoped_lock lock(mx_thread_);
        last_pack_thread_[thread_id] = std::move(pack_value);
      }
    } else {
      std::scoped_lock lock(mx_thread_);
      auto &lock_thread = last_pack_thread_[thread_id];
      auto &lock_dim = lock_thread[cur_dim];
      lock_dim[col_index] = cur_pack;
    }
  }
}

void VCPackGuardian::LockPackrowOnLockOne(const MIIterator &mit) {
  int threadId = mit.GetTaskId();
  int taskNum = mit.GetTaskNum();
  {
    std::scoped_lock g(mx_thread_);
    if (!initialized_) {
      Initialize(taskNum);
    }
    if (initialized_ && (taskNum > guardian_threads_)) {
      // recheck to make sure last_pack_ is not overflow
      ResizeLastPack(taskNum);
    }
  }
  for (auto iter = my_vc_.GetVarMap().cbegin(); iter != my_vc_.GetVarMap().cend(); iter++) {
    int cur_dim = iter->dim;
    if (last_pack_[cur_dim][threadId] != mit.GetCurPackrow(cur_dim)) {
      JustATable *tab = iter->GetTabPtr().get();
      if (last_pack_[cur_dim][threadId] != common::NULL_VALUE_32)
        tab->UnlockPackFromUse(iter->col_ndx, last_pack_[cur_dim][threadId]);
      try {
        tab->LockPackForUse(iter->col_ndx, mit.GetCurPackrow(cur_dim));
      } catch (...) {
        TIANMU_LOG(LogCtl_Level::ERROR,
                   "LockPackrowOnLockOne LockPackForUse fail, cur_dim: %d threadId: %d cur_pack: %d", cur_dim, threadId,
                   mit.GetCurPackrow(cur_dim));
        // unlock packs which are partially locked for this packrow
        auto it = my_vc_.GetVarMap().begin();
        for (; it != iter; ++it) {
          int cur_dim = it->dim;
          if (last_pack_[cur_dim][threadId] != mit.GetCurPackrow(cur_dim) &&
              last_pack_[cur_dim][threadId] != common::NULL_VALUE_32)
            it->GetTabPtr()->UnlockPackFromUse(it->col_ndx, mit.GetCurPackrow(cur_dim));
        }

        for (++iter; iter != my_vc_.GetVarMap().end(); ++iter) {
          int cur_dim = iter->dim;
          if (last_pack_[cur_dim][threadId] != mit.GetCurPackrow(cur_dim) &&
              last_pack_[cur_dim][threadId] != common::NULL_VALUE_32)
            iter->GetTabPtr()->UnlockPackFromUse(iter->col_ndx, last_pack_[cur_dim][threadId]);
        }

        for (auto const &iter : my_vc_.GetVarMap()) last_pack_[iter.dim][threadId] = common::NULL_VALUE_32;
        throw;
      }
    }
  }
  for (auto const &iter : my_vc_.GetVarMap())
    last_pack_[iter.dim][threadId] = mit.GetCurPackrow(iter.dim);  // must be in a separate loop, otherwise
                                                                   // for "a + b" will not lock b
}

void VCPackGuardian::UnlockAll() {
  switch (current_strategy_) {
    case GUARDIAN_LOCK_STRATEGY::LOCK_ONE:
      return UnlockAllOnLockOne();
    case GUARDIAN_LOCK_STRATEGY::LOCK_ONE_THREAD:
      return UnlockAllOnLockOneByThread();
    default:
      TIANMU_LOG(LogCtl_Level::ERROR, "UnlockAll fail, unkown current_strategy_: %d",
                 static_cast<int>(current_strategy_));
      ASSERT(0, "VCPackGuardian::UnlockAll fail, unkown current_strategy_: " + static_cast<int>(current_strategy_));
  }
}

void VCPackGuardian::UnlockAllOnLockOne() {
  if (!initialized_)
    return;
  for (auto const &iter : my_vc_.GetVarMap()) {
    for (int i = 0; i < guardian_threads_; ++i)
      if (last_pack_[iter.dim][i] != common::NULL_VALUE_32 && iter.GetTabPtr())
        iter.GetTabPtr()->UnlockPackFromUse(iter.col_ndx, last_pack_[iter.dim][i]);
  }
  for (auto const &iter : my_vc_.GetVarMap()) {
    for (int i = 0; i < guardian_threads_; ++i)
      last_pack_[iter.dim][i] = common::NULL_VALUE_32;  // must be in a separate loop, otherwise
                                                        // for "a + b" will not unlock b
  }
}

void VCPackGuardian::UnlockAllOnLockOneByThread() {
  if (last_pack_thread_.empty()) {
    return;
  }

  const auto &var_map = my_vc_.GetVarMap();
  if (var_map.empty()) {
    return;
  }

  uint64_t thread_id = pthread_self();
  auto iter_thread = last_pack_thread_.find(thread_id);
  if (last_pack_thread_.end() == iter_thread) {
    return;
  }

  for (auto const &iter : var_map) {
    if (!iter.GetTabPtr()) {
      continue;
    }

    int cur_dim = iter.dim;
    int col_index = iter.col_ndx;

    auto iter_pack = iter_thread->second.find(cur_dim);
    if (iter_pack == iter_thread->second.end()) {
      continue;
    }

    auto iter_val = iter_pack->second.find(col_index);
    if (iter_val == iter_pack->second.end()) {
      continue;
    }

    int cur_pack = iter_val->second;
    iter_pack->second.erase(col_index);
    iter.GetTabPtr()->UnlockPackFromUse(col_index, cur_pack);
  }

  {
    std::scoped_lock lock(mx_thread_);
    last_pack_thread_.erase(thread_id);
  }
}

}  // namespace core
}  // namespace Tianmu
