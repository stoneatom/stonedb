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

#include "bucket.h"

int Tianmu::IMCS::Bucket::insert(uint32 idx, Field *field, Cell *&ins_cell,
                                 uint64 trx_id, Row *row) {
  if (idx != n_cells_) {
    return RET_CELL_INDEX_NOT_MATCH;
  }

  int ret = RET_SUCCESS;

  if (field->is_null()) {
    bitmap_set_bit(&null_bits_, n_cells_);
    meta_.zone_map_.update_with_null(trx_id);
  } else {
    if (RET_FAIL(cells_[n_cells_].set_val(meta_.share_, field))) {
      return ret;
    }
    // TODO: handle null
    ins_cell = cells_ + n_cells_;
    meta_.zone_map_.update_with_not_null(
        meta_.share_, meta_.share_->fields_[meta_.cid_].type, ins_cell, trx_id);
  }
  rows_[n_cells_] = row;
  n_cells_++;
  return RET_SUCCESS;
}

int Tianmu::IMCS::Bucket::remove(uint32 idx, uint64 trx_id, bool &is_null,
                                 Cell *&cell) {
  if (idx >= n_cells_) {
    return RET_INDEX_OUT_OF_BOUND;
  }
  bitmap_set_bit(&delete_bits_, idx);
  if (bitmap_is_set(&null_bits_, idx)) {
    meta_.zone_map_.update_delete_with_null(trx_id);
    is_null = true;
  } else {
    is_null = false;
    cell = cells_ + idx;
    int ret = meta_.zone_map_.update_delete_with_not_null(
        meta_.share_, meta_.share_->fields_[meta_.cid_].type, cells_ + idx,
        trx_id);
    if (ret == RET_NEED_REORGANIZE_ZONE_MAP) {
      reorgnize_zone_map();
      return RET_NEED_REORGANIZE_ZONE_MAP;
    }
  }

  return RET_SUCCESS;
}

int Tianmu::IMCS::Bucket::fill_rec(Imcs_context &context, uchar *rec,
                                   uint32 offset) {
  if (bitmap_is_set(&null_bits_, offset)) {
    set_null_bit(context.null_bytes);
    return RET_SUCCESS;
  } else {
    int ret = RET_SUCCESS;
    if (RET_FAIL(cells_[offset].fill_rec(context.share, meta_.type(), rec))) {
      return ret;
    }
    return ret;
  }
}

void Tianmu::IMCS::Bucket::set_null_bit(uchar *null_bytes) {
  null_bytes[meta_.cid_ / 8] |= (1 << (meta_.cid_ % 8));
}

void Tianmu::IMCS::Bucket::reorgnize_zone_map() {
  meta_.zone_map_.clear();
  for (uint32 i = 0; i < n_cells_; ++i) {
    if (!bitmap_is_set(&delete_bits_, i)) {
      continue;
    }
    if (bitmap_is_set(&null_bits_, i)) {
      meta_.zone_map_.update_with_null(rows_[i]->insert_id());
    } else {
      meta_.zone_map_.update_with_not_null(
          meta_.share_, meta_.share_->fields_[meta_.cid_].type, cells_ + i,
          rows_[i]->insert_id());
    }
  }
}
