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
  The bucket is a baisc element for a chunk. A bucket has some tiles, which
  means some tiles was oraginzed as a bucket logically. and bukets creates
  chunks. it's logial object.

  Created 2/2/2023 Hom.LEE
 */

#ifndef __IMCS_BUCKET_H__
#define __IMCS_BUCKET_H__

#include <mutex>

#include "../common/arena.h"
#include "../common/common_def.h"
#include "../common/context.h"
#include "../common/error.h"
#include "../imcs_base.h"
#include "../statistics/camp.h"
#include "../statistics/histogram.h"
#include "../statistics/statistics.h"
#include "../statistics/zone_map.h"
#include "cell.h"
#include "field.h"        //for mysql field.
#include "field_types.h"  //for mysql field type.
#include "my_bitmap.h"
#include "row.h"

namespace Tianmu {
namespace IMCS {

class Bucket_meta {
  friend class Bucket;

 public:
  Bucket_meta(uint32 id, ColumnID cid, TianmuSecondaryShare *share)
      : id_(id), cid_(cid), share_(share) {}

  virtual ~Bucket_meta() = default;

  Zone_map &zone_map() { return zone_map_; }

  enum_field_types type() { return share_->fields_[cid_].type; }

 private:
  uint32 id_;

  // column oid.
  ColumnID cid_;

  // the zone map of this bucket.
  Zone_map zone_map_;

  // the meta data of the table which the chunk is corresponding to.
  TianmuSecondaryShare *share_;
};

// To store the tiles into a bucket. some buckets be a chunk. and chuncks be a
// IMCS.
class Bucket {
 public:
  Bucket(uint32 id, ColumnID cid, TianmuSecondaryShare *share)
      : meta_(id, cid, share), n_cells_(0), cells_(nullptr) {
    // TODO: the memory allocated by bitmap is not in arena
    bitmap_init(&null_bits_, nullptr, meta_.share_->n_cells_);
    bitmap_init(&delete_bits_, nullptr, meta_.share_->n_cells_);
  }
  virtual ~Bucket() {
    bitmap_free(&null_bits_);
    bitmap_free(&delete_bits_);
  }

  // adds a cell into buckets.
  int insert(uint32 idx, Field *field, Cell *&ins_cell, uint64 trx_id,
             Row *row);

  // removes a tile from bucket.
  int remove(uint32 idx, uint64 trx_id, bool &is_null, Cell *&cell);

  uint32 n_cells() { return n_cells_; }

  Cell *cells() { return cells_; }

  Cell *cell(uint32 idx) {
    assert(0 <= idx && idx < n_cells_);
    return &cells_[idx];
  }

  void set_cells(Cell *cells) { cells_ = cells; }

  void set_rows(Row **rows) { rows_ = rows; }

  Bucket_meta &meta() { return meta_; }

  int fill_rec(Imcs_context &context, uchar *rec, uint32 offset);

  void reorgnize_zone_map();

  Zone_map &zone_map() { return meta_.zone_map_; }

 private:
  void set_null_bit(uchar *null_bytes);

 private:
  // the meta info for this bucket.
  Bucket_meta meta_;

  // number of cell in this bucket(the same as bucket_rows in
  // TianmuSecondaryShare).
  uint32 n_cells_;

  // null bitmap
  MY_BITMAP null_bits_;

  // delete bitmap
  MY_BITMAP delete_bits_;

  // TODO: deprecated? thread safety is garanteed by imcs
  std::mutex mutex_;

  // the column values in the bucket.
  Cell *cells_;

  Row **rows_;
};

}  // namespace IMCS
}  // namespace Tianmu

#endif  //__IMCS_BUCKET_H__
