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

#ifndef IMCS_COMMON_CONTEXT_H
#define IMCS_COMMON_CONTEXT_H

#include <memory>

#include "../filter/table_filter.h"
#include "common_def.h"

class ReadView;

namespace Tianmu {
namespace IMCS {
class Imcs_context {
 public:
  Imcs_context(std::shared_ptr<Table_filter_set> fs, TianmuSecondaryShare *s,
               uchar *buf, uint64 *start_idx, Item *item, uchar *null_bytes,
               ReadView *read_view)
      : filter_set(fs),
        share(s),
        rec(buf),
        cur_row_id(start_idx),
        found_row_id(0),
        found_cell_offset(0),
        found_bucket_offset(0),
        found_imcu_offset(0),
        item(item),
        null_bytes(null_bytes),
        read_view(read_view) {}

  uint32 get_cur_imcu_idx() { return *cur_row_id / share->imcu_rows_; }

  uint32 get_cur_offset_in_imcu() { return *cur_row_id % share->imcu_rows_; }

  uint32 get_cur_bucket_idx() {
    return get_cur_offset_in_imcu() / share->n_cells_;
  }

  uint32 get_cur_offset_in_bucket() {
    return get_cur_offset_in_imcu() % share->n_cells_;
  }

 public:
  uchar *null_bytes;

  std::unordered_map<uint32, uint32> cid_to_offset;

  std::shared_ptr<Table_filter_set> filter_set;

  TianmuSecondaryShare *share;

  uint64 *cur_row_id;

  uint64 found_row_id;

  uint32 found_cell_offset;

  uint32 found_bucket_offset;

  uint32 found_imcu_offset;

  uchar *rec;

  Item *item;

  ReadView *read_view;
};
}  // namespace IMCS

}  // namespace Tianmu

#endif