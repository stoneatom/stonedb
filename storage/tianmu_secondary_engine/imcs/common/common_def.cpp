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

#include "common_def.h"

#include "../data/bucket.h"
#include "../data/cell.h"
#include "../data/chunk.h"
#include "../data/imcu.h"
#include "../data/row.h"
#include "sql_base.h"

extern thread_local THD *current_thd;

namespace Tianmu {
namespace IMCS {

TianmuSecondaryShare::TianmuSecondaryShare(const TABLE &table) {
  thr_lock_init(&lock);
  row_manager_ = new Mem_row_manager();
  uint32 field_count = table.s->fields;

  TABLE_CATEGORY orig_category = table.s->table_category;
  table.s->table_category = TABLE_CATEGORY_SYSTEM;
  if (open_table_from_share(current_thd, table.s, "", 0,
                            (uint)READ_ALL | SKIP_NEW_HANDLER, 0, &table_,
                            false, nullptr)) {
    assert(0);
  }
  table.s->table_category = orig_category;
  primary_key_idx_ = table.s->primary_key;
  KEY *key = table.key_info + primary_key_idx_;
  primary_key_name_ = key->key_part->field->field_name;

  Field *field = nullptr;
  uint32 col_idx = 0;
  for (uint32 idx = 0; idx < field_count; idx++) {
    field = *(table.s->field + idx);
    if (field->is_flag_set(NOT_SECONDARY_FLAG)) continue;
    uint32 len;
    int ret = RET_SUCCESS;
    if (RET_FAIL(Cell::get_cell_len(field, &len))) {
      assert(1);
    }
    fields_.push_back({field->type(), std::string(field->field_name),
                       field->pack_length(), len});
    const char *field_name = field->field_name;
    field_to_col_idx_.insert({field_name, col_idx++});
  }

  // fix field name
  for (int32 idx = 0; idx < field_count; idx++) {
    Field *fix_field = *(table_.field + idx);
    fix_field->field_name = fields_[idx].field_name.c_str();
  }

  imcu_size_ = DEFAULT_IMCU_SIZE;
  int ret = RET_SUCCESS;
  if (RET_FAIL(get_n_rows_in_imcu(DEFAULT_IMCU_SIZE, &n_buckets_, &n_cells_))) {
    // handle error
    assert(1);
  }
  imcu_rows_ = n_buckets_ * n_cells_;
}

/*
sizeof(Imcu) + \sum_{i=0}^{n_fields-1} {sizeof(Chunk) + n_buckets *
(sizeof(Bucket) + n_cells * sizeof(Row*) + n_cells * (sizeof(Cell) +
data_size_{i} + sizeof(Row)))} <= imcu_size

=>

n_cells
<= (imcu_size - sizeof(Imcu) - n_fields * sizeof(Chunk) - n_fields * n_buckets *
sizeof(Bucket)) / (n_fields * n_buckets * (sizeof(Cell) + sizeof(Row*) +
sizeof(Row))+ n_buckets *
(\sum_{i=0}^{n_fields-1} * data_size_{i}))
*/
int TianmuSecondaryShare::get_n_rows_in_imcu(uint32 imcu_size,
                                             uint32 *n_buckets,
                                             uint32 *n_cells) {
  int ret = RET_SUCCESS;
  uint32 n_fields = fields_.size();
  *n_buckets = CHUNK_SIZE;
  uint32 numerator = imcu_size - sizeof(Imcu);
  // to avoid over precision
  if ((uint64)n_fields * sizeof(Chunk) > numerator) {
    return RET_IMCU_SIZE_TOO_SMALL;
  }
  // n_fields * sizeof(Chunk) isn't over uint32 because numerator is uint32
  numerator -= n_fields * sizeof(Chunk);
  if ((uint64)n_fields * *n_buckets > numerator ||
      (uint64)n_fields * *n_buckets * sizeof(Bucket) > numerator) {
    return RET_IMCU_SIZE_TOO_SMALL;
  }
  numerator -= n_fields * *n_buckets * sizeof(Bucket);
#ifdef DEBUG
  assert(numerator >= 0);
#endif
  if ((uint64)n_fields * *n_buckets > numerator ||
      (uint64)n_fields * *n_buckets * sizeof(Cell) > numerator) {
    return RET_IMCU_SIZE_TOO_SMALL;
  }
  uint32 denominator =
      n_fields * *n_buckets * (sizeof(Cell) + sizeof(Row *) + sizeof(Row));
  uint64 field_len_sum = 0ll;
  for (auto &field : fields_) {
    field_len_sum += field.rpd_length;
  }
  if (field_len_sum > numerator || *n_buckets * field_len_sum > numerator ||
      numerator - *n_buckets * field_len_sum < denominator) {
    return RET_IMCU_SIZE_TOO_SMALL;
  }
  denominator += *n_buckets * field_len_sum;
#ifdef DEBUG
  assert(denominator > 0);
#endif
  *n_cells = numerator / denominator;
  if (*n_cells < MIN_BUCKET_SIZE) {
    return RET_IMCU_SIZE_TOO_SMALL;
  }

// check
#ifdef DEBUG
  uint32 real_size = 0;
  real_size += sizeof(Imcu);
  for (uint32 i = 0; i < n_fields; ++i) {
    real_size += sizeof(Chunk);
    uint32 len = fields_[i].rpd_length;
    uint32 inner_size = sizeof(Bucket) + *n_cells * (sizeof(Cell) + len);
    real_size += *n_buckets * inner_size;
  }
  assert(real_size <= imcu_size);
#endif

  return RET_SUCCESS;
}

const char *get_ret_message(int code) {
  switch (code) {
    case RET_INSERT_FAILED_NO_EXTRA_MEM:
      return RET_INSERT_FAILED_NO_EXTRA_MEM_MSG;
    case RET_CELL_INDEX_NOT_MATCH:
      return RET_CELL_INDEX_NOT_MATCH_MSG;
    case RET_CELL_LEN_NOT_MATCH:
      return RET_CELL_LEN_NOT_MATCH_MSG;
    case RET_IMCU_SIZE_TOO_SMALL:
      return RET_IMCU_SIZE_TOO_SMALL_MSG;
    case RET_MYSQL_TYPE_NOT_SUPPORTED:
      return RET_MYSQL_TYPE_NOT_SUPPORTED_MSG;
    case RET_PK_ROW_ID_NOT_EXIST:
      return RET_PK_ROW_ID_NOT_EXIST_MSG;
    case RET_PK_ROW_ID_ALREADY_EXIST:
      return RET_PK_ROW_ID_ALREADY_EXIST_MSG;
    case RET_IMCS_INNER_ERROR:
      return RET_IMCS_INNER_ERROR_MSG;
    case RET_INDEX_OUT_OF_BOUND:
      return RET_INDEX_OUT_OF_BOUND_MSG;
    case RET_FIELD_NOT_MATCH:
      return RET_FIELD_NOT_MATCH_MSG;
    case RET_CREATE_IMCU_ERR:
      return RET_CREATE_IMCU_ERR_MSG;
    default:
      return nullptr;
  }
}

}  // namespace IMCS
}  // namespace Tianmu