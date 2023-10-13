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
   the data in imcs was divided into several parts in hierachy. the first level
   is column, of course, all the data was orginized as column-based format. and
   then a column was split into some small part, which we call: chunk. and chunk
   was created by some verctors. some rows creates a vector(bucket).

   Data type we supported, now, we only support number(int, double, etc.) and
   string two types.

    > create table foo(col1 int, col2 varchar(5) col3 int not secondary,
                       col4 numeric) engine=innodb,
   seondary_engine=tianmu_rapid; > alter table foo secondary_load;

     foo table in IMCS memory pool layout:
   +-----------------------------------------------------------+
      chunck1                  chunk2               chunck3
     |         |            |          |         |          |
     | bucket1 |            | bucket1  |         | bucket1  |
   ...............................................................
   . | bucket2 |            | bucket2  |         | bucket2  |    .
   . |  ...    |            | ...      |         | ...      |    .  IMCU 2
   ...............................................................
     | bucketN |            | bucketN  |         | bucketN  |
     |         |            |          |         |          |
       foo.col1               foo.col2             foo.col4
   +------------------------------------------------------------+


    the bucket layout and tile layout in memory pool.
   +--------------------      ----------------------------------+
     |        |                   |        |
     |  tile1 |                   |  row1  |
     |  ...   |                   |  ...   |
     |  tileN |                   |  rowN  |

       bucket                        tile
   +---------------------     ----------------------------------+

   Created 2/2/2023 Hom.LEE
*/

#ifndef __IMCS_CHUNK_H__
#define __IMCS_CHUNK_H__

#include <algorithm>
#include <iostream>
#include <mutex>
#include <string>

#include "../../../include/field_types.h"  //column types for mysql
#include "../../../sql/item.h"             //for item_filed in sql
#include "../common/arena.h"
#include "../common/common_def.h"
#include "../common/error.h"
#include "../imcs_base.h"  //for the basic data type
#include "../statistics/zone_map.h"
#include "bucket.h"
#include "row.h"

namespace Tianmu {
namespace IMCS {

// the meta data for chunk, every chunk has only one meta info instance.
class Chunk_meta {
  friend class Chunk;

 public:
  Chunk_meta(ColumnID cid, TianmuSecondaryShare *share)
      : cid_(cid), share_(share) {}

  virtual ~Chunk_meta() = default;

  Zone_map &zone_map() { return zone_map_; }

 private:
  // column id
  ColumnID cid_;

  // some basic statistics.
  Zone_map zone_map_;

  // the meta data of the table which the chunk is corresponding to.
  TianmuSecondaryShare *share_;
};

// To store the data in chunk in column format.
class Chunk {
 public:
  Chunk(ColumnID cid, TianmuSecondaryShare *share)
      : n_buckets_(0), meta_(cid, share), buckets_(nullptr) {}
  virtual ~Chunk() = default;

  // adds a cell into chunk.
  int insert(uint32 idx, Field *field, uint64 trx_id, Row *row);

  // removes a tile from chunk.
  int remove(uint32 idx, uint64 trx_id);

  uint32 n_buckets() { return n_buckets_; }

  Bucket *buckets() { return buckets_; }

  Bucket *bucket(uint32 idx) { return buckets_ + idx; }

  void set_buckets(Bucket *buckets) { buckets_ = buckets; }

  Chunk_meta &meta() { return meta_; }

  int fill_rec(uint32 bucket_idx, Imcs_context &context, uchar *rec,
               uint32 offset);

 private:
  void reorganize_zone_map();

  enum_field_types type() { meta_.share_->fields_[meta_.cid_].type; }

 private:
  Chunk(const Chunk &) = delete;
  Chunk &operator=(const Chunk &) = delete;

  // the meta information for this chunk.
  Chunk_meta meta_;

  // number of buckets in this chunk.
  uint32 n_buckets_;

  // TODO: deprecated? thread safety is garanteed by imcs
  std::mutex mutex_;

  // the buckets in the Chunk
  Bucket *buckets_;
};

}  // namespace IMCS
}  // namespace Tianmu

#endif  //__IMCS_CHUNK_H__
