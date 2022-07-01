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

#include "pack_guardian.h"

#include "core/just_a_table.h"
#include "core/mi_iterator.h"
#include "core/rc_attr.h"
#include "vc/virtual_column.h"

namespace stonedb {
namespace core {
void VCPackGuardian::Initialize(int no_th) {
  UnlockAll();
  last_pack.clear();

  threads = no_th;

  int no_dims = -1;
  for (auto const &iter : my_vc.GetVarMap())
    if (iter.dimension_ > no_dims) no_dims = iter.dimension_;  // find the maximal number of dimension used
  no_dims++;
  if (no_dims > 0) {  // else constant
    last_pack.reserve(no_dims);
    for (int i = 0; i < no_dims; ++i) last_pack.emplace_back(threads, common::NULL_VALUE_32);
  }
  initialized = true;
}

/*
 * ResizeLastPack handles last_pack overflow under multi-threads group by
 */
void VCPackGuardian::ResizeLastPack(int taskNum) {
  if (!initialized) return;

  for (auto &v : last_pack) v.resize(taskNum, common::NULL_VALUE_32);

  threads = taskNum;
}

void VCPackGuardian::LockPackrow(const MIIterator &mit) {
  int threadId = mit.GetTaskId();
  int taskNum = mit.GetTaskNum();
  {
    std::scoped_lock g(mx_thread);
    if (!initialized) {
      Initialize(taskNum);
    }
    if (initialized && (taskNum > threads)) {
      // recheck to make sure last_pack is not overflow
      ResizeLastPack(taskNum);
    }
  }
  for (auto iter = my_vc.GetVarMap().cbegin(); iter != my_vc.GetVarMap().cend(); iter++) {
    int cur_dim = iter->dimension_;
    if (last_pack[cur_dim][threadId] != mit.GetCurPackrow(cur_dim)) {
      JustATable *tab = iter->GetTabPtr().get();
      if (last_pack[cur_dim][threadId] != common::NULL_VALUE_32)
        tab->UnlockPackFromUse(iter->col_idx_, last_pack[cur_dim][threadId]);
      try {
        tab->LockPackForUse(iter->col_idx_, mit.GetCurPackrow(cur_dim));
      } catch (...) {
        // unlock packs which are partially locked for this packrow
        auto it = my_vc.GetVarMap().begin();
        for (; it != iter; ++it) {
          int cur_dim = it->dimension_;
          if (last_pack[cur_dim][threadId] != mit.GetCurPackrow(cur_dim) &&
              last_pack[cur_dim][threadId] != common::NULL_VALUE_32)
            it->GetTabPtr()->UnlockPackFromUse(it->col_idx_, mit.GetCurPackrow(cur_dim));
        }

        for (++iter; iter != my_vc.GetVarMap().end(); ++iter) {
          int cur_dim = iter->dimension_;
          if (last_pack[cur_dim][threadId] != mit.GetCurPackrow(cur_dim) &&
              last_pack[cur_dim][threadId] != common::NULL_VALUE_32)
            iter->GetTabPtr()->UnlockPackFromUse(iter->col_idx_, last_pack[cur_dim][threadId]);
        }

        for (auto const &iter : my_vc.GetVarMap()) last_pack[iter.dimension_][threadId] = common::NULL_VALUE_32;
        throw;
      }
    }
  }
  for (auto const &iter : my_vc.GetVarMap())
    last_pack[iter.dimension_][threadId] = mit.GetCurPackrow(iter.dimension_);  // must be in a separate loop, otherwise
                                                                  // for "a + b" will not lock b
}

void VCPackGuardian::UnlockAll() {
  if (!initialized) return;
  for (auto const &iter : my_vc.GetVarMap()) {
    for (int i = 0; i < threads; ++i)
      if (last_pack[iter.dimension_][i] != common::NULL_VALUE_32 && iter.GetTabPtr())
        iter.GetTabPtr()->UnlockPackFromUse(iter.col_idx_, last_pack[iter.dimension_][i]);
  }
  for (auto const &iter : my_vc.GetVarMap()) {
    for (int i = 0; i < threads; ++i)
      last_pack[iter.dimension_][i] = common::NULL_VALUE_32;  // must be in a separate loop, otherwise
                                                       // for "a + b" will not unlock b
  }
}
}  // namespace core
}  // namespace stonedb
