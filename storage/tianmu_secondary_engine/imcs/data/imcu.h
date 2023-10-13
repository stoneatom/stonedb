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
  In memory compressed unit. the detail info, pls refer to chunck.h.
  An In-Memory Compression Unit (IMCU) is a compressed, storage
  unit that contains data for one or more columns.

  Created 5/2/2023 Hom.LEE
 */

#ifndef __IMCU_H__
#define __IMCU_H__

#include <mutex>

#include "../common/arena.h"
#include "../common/common_def.h"
#include "../common/context.h"
#include "../common/error.h"
#include "../imcs_base.h"
#include "chunk.h"
#include "row.h"

namespace Tianmu {
namespace IMCS {

// meta info about this imcu.
class Imcu_meta {
  friend class Imcu;

 public:
  Imcu_meta(uint64 id, TianmuSecondaryShare *share) : id_(id), share_(share) {}
  virtual ~Imcu_meta() = default;

  uint64 id() { return id_; }

  TianmuSecondaryShare *share() { return share_; }

 private:
  // the id of this imcu.
  uint64 id_;

  // the meta data of the table which the imcu is corresponding to.
  TianmuSecondaryShare *share_;
};

// for detail, pls refer to chunck.h.
class Imcu {
 public:
  Imcu(Arena *arena, uint64 id, TianmuSecondaryShare *share)
      : arena_(arena),
        meta_(id, share),
        chunks_(nullptr),
        n_chunks_(share->fields_.size()) {}
  virtual ~Imcu() = default;

  // adds a row into imcu.
  int insert(uint32 idx, std::vector<Field *> &field_lst, uint64 trx_id);

  // delete a row in imcu
  int remove(uint32 idx, uint64 trx_id);

  int scan(Imcs_context &context);

  Chunk *chunk(uint32 idx) {
    assert(0 <= idx && idx < n_chunks_);
    return chunks_ + idx;
  }

  void set_chunks(Chunk *chunks) { chunks_ = chunks; }

  void set_rows(Row *rows) { rows_ = rows; }

  Arena *arena() { return arena_; }

 private:
  // the meta inf for this imcu. and there is a global imcu_meta hash table for
  // for fast access the whole data info.
  Imcu_meta meta_;

  // numf of chunks (aka column)
  int32 n_chunks_;

  Arena *arena_;

  // TODO: deprecated? thread safety is garanteed by imcs
  std::mutex mutex_;

  // row info in the imcu
  Row *rows_;

  // the chuncks in this IMCU.
  Chunk *chunks_;
};

}  // namespace IMCS
}  // namespace Tianmu

#endif  //__IMCU_H__
