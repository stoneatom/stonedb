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

/**
 The fundmental code for imcs. The chunk is used to store the data which
 transfer from row-based format to column-based format. Created 2/2/2023 Hom.LEE
*/

#include "chunk.h"

namespace Tianmu {
namespace IMCS {
int Chunk::insert(uint32 idx, Field *field, uint64 trx_id, Row *row) {
  int ret = RET_SUCCESS;
  uint32 bucket_idx = idx / meta_.share_->n_cells_;
  uint32 in_bucket_idx = idx % meta_.share_->n_cells_;
  if (bucket_idx == n_buckets_) {
    n_buckets_++;
  } else if (bucket_idx + 1 != n_buckets_) {
    return RET_IMCS_INNER_ERROR;
  }
  Cell *ins_cell = nullptr;
  if (RET_FAIL(buckets_[bucket_idx].insert(in_bucket_idx, field, ins_cell,
                                           trx_id, row))) {
    return ret;
  }
  if (ins_cell != nullptr) {
    meta_.zone_map_.update_with_not_null(meta_.share_, type(), ins_cell,
                                         trx_id);
  } else {
    // handle null value
    meta_.zone_map_.update_with_null(trx_id);
  }
  return RET_SUCCESS;
}

int Chunk::remove(uint32 idx, uint64 trx_id) {
  int ret = RET_SUCCESS;
  uint32 bucket_idx = idx / meta_.share_->n_cells_;
  uint32 in_bucket_idx = idx % meta_.share_->n_cells_;
  if (bucket_idx >= n_buckets_) {
    return RET_INDEX_OUT_OF_BOUND;
  }
  bool is_null = false;
  Cell *cell;
  if (RET_FAIL(
          buckets_[bucket_idx].remove(in_bucket_idx, trx_id, is_null, cell))) {
    if (ret == RET_NEED_REORGANIZE_ZONE_MAP) {
      if (is_null) {
        meta_.zone_map_.update_delete_with_null(trx_id);
      } else {
        int ret = meta_.zone_map_.update_delete_with_not_null(
            meta_.share_, type(), cell, trx_id);
        if (ret == RET_NEED_REORGANIZE_ZONE_MAP) {
          reorganize_zone_map();
        }
      }
      return RET_SUCCESS;
    }
    return ret;
  }
  return RET_SUCCESS;
}

int Chunk::fill_rec(uint32 bucket_idx, Imcs_context &context, uchar *rec,
                    uint32 offset) {
  return buckets_[bucket_idx].fill_rec(context, rec, offset);
}

void Chunk::reorganize_zone_map() {
  meta_.zone_map_.clear();
  for (uint32 i = 0; i < n_buckets_; ++i) {
    Zone_map &buc_zone_map = buckets_[i].zone_map();
    meta_.zone_map_.update_with_zone_map(buc_zone_map, type());
  }
}

}  // namespace IMCS
}  // namespace Tianmu
