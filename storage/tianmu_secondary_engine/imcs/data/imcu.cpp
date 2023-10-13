/* Copyright (c) 2018, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "imcu.h"

#include <cstring>

#include "../filter/table_filter.h"

namespace Tianmu {
namespace IMCS {
int Imcu::insert(uint32 idx, std::vector<Field *> &field_lst, uint64 trx_id) {
  TianmuSecondaryShare *share = meta_.share_;
  int ret = RET_SUCCESS;
  for (auto &field : field_lst) {
    const char *field_name = field->field_name;
    auto it = share->field_to_col_idx_.find(field_name);
    if (it == share->field_to_col_idx_.end()) {
      return RET_FIELD_NOT_MATCH;
    }
    uint32 col_idx = it->second;
    if (RET_FAIL(chunks_[col_idx].insert(idx, field, trx_id, rows_ + idx))) {
      // TODO: rollback
      return ret;
    }
  }
  rows_[idx].set_insert_id(trx_id);
  return RET_SUCCESS;
}

int Imcu::remove(uint32 idx, uint64 trx_id) {
  int ret = RET_SUCCESS;
  for (uint i = 0; i < n_chunks_; ++i) {
    if (RET_FAIL(chunks_[i].remove(idx, trx_id))) {
      return ret;
    }
  }
  rows_[idx].set_delete(trx_id);
  return ret;
}

int Imcu::scan(Imcs_context &context) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (context.get_cur_offset_in_imcu() == 0 &&
      context.filter_set.get() != nullptr) {  // check every chunk's zone map
    for (auto &[cid, filter] : context.filter_set->filters) {
      Zone_map &zone_map = chunk(cid)->meta().zone_map();
      if (zone_map.can_skip(context.read_view)) {
        return RET_NOT_FOUND;
      }
      if (!zone_map.can_use(context.read_view)) {
        break;
      }
      Filter_eval_result result =
          filter->check_zone_map(chunk(cid)->meta().zone_map());
      if (result == Filter_eval_result::FILTER_ALWAYS_FALSE) {
        return RET_NOT_FOUND;
      }
    }
  }
  assert(n_chunks_ > 0);
  uint32 n_buckets = chunk(0)->n_buckets();
#ifdef DEBUG
  for (uint32 i = 1; i < n_chunks_; ++i) {
    assert(chunk(i).n_buckets() == chunk(0).n_buckets());
  }
#endif
  while (context.get_cur_bucket_idx() < n_buckets) {
    int ret = RET_SUCCESS;

    uint32 bucket_idx = context.get_cur_bucket_idx();
    uint32 offset = context.get_cur_offset_in_bucket();

    if (offset == 0 && context.filter_set.get() != nullptr) {  // check zone map
      bool skip = true;
      for (auto &[cid, filter] : context.filter_set->filters) {
        Zone_map &zone_map =
            chunk(cid)->buckets()[bucket_idx].meta().zone_map();
        if (zone_map.can_skip(context.read_view)) {
          break;
        }
        if (!zone_map.can_use(context.read_view)) {
          skip = false;
          break;
        }
        Filter_eval_result result = filter->check_zone_map(zone_map);
        if (result != Filter_eval_result::FILTER_ALWAYS_FALSE) {
          skip = false;
          break;
        }
      }
      if (skip) {
        context.cur_row_id += context.share->n_cells_;
        continue;
      }
    }

    assert(n_chunks_ > 0);
    if (offset >= chunk(0)->buckets()[bucket_idx].n_cells()) {
      return RET_NOT_FOUND;
    }

    // mvcc
    uint32 imcu_offset = context.get_cur_offset_in_imcu();
    if (!rows_[imcu_offset].visible(context.read_view)) {
      (*context.cur_row_id)++;
      continue;
    }

    for (uint32 i = 0; i < n_chunks_; ++i) {
      auto it = context.cid_to_offset.find(i);
      assert(it != context.cid_to_offset.end());

      if (RET_FAIL(chunk(i)->fill_rec(bucket_idx, context,
                                      context.rec + it->second, offset))) {
        return ret;
      }
    }
    if (context.item == nullptr || context.item->val_bool()) {
      (*context.cur_row_id)++;
      return RET_SUCCESS;
    }
    (*context.cur_row_id)++;
  }
  return RET_NOT_FOUND;
}
}  // namespace IMCS

}  // namespace Tianmu