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

#include "apply_redo.h"
#include "dict0mem.h"
#include "row0mysql.h"
#include "../common/common_def.h"
#include <string>
#include "sql_plugin.h"
#include "mysql/plugin.h"
#include "sql_class.h"
#include "sql_base.h"
#include "../imcs.h"

extern LoadedTables *loaded_tables;

extern thread_local THD *current_thd;

namespace Tianmu {
namespace IMCS {

static void innodb_field_convert_to_mysql_field(byte *dest, const byte *data,
                                                ulint len, dict_col_t *col,
                                                Field *field) {
  byte *ptr;
  switch (col->mtype) {
    case DATA_INT:
      ptr = dest + len;

      for (;;) {
        ptr--;
        *ptr = *data;
        if (ptr == dest) {
          break;
        }
        data++;
      }

      if (!(col->prtype & DATA_UNSIGNED)) {
        dest[len - 1] = (byte)(dest[len - 1] ^ 128);
      }

      break;

    case DATA_VARCHAR:
    case DATA_VARMYSQL:
    case DATA_BINARY: {
      ulint lenlen = len > 255 ? 2 : 1;
      dest = row_mysql_store_true_var_len(dest, len, lenlen);
      memcpy(dest, data, len);
      break;
    }
    case DATA_BLOB:
    case DATA_POINT:
    case DATA_VAR_POINT:
    case DATA_GEOMETRY:
      // TODO: handle more type
      assert(0);

    case DATA_MYSQL:
      memcpy(dest, data, len);
      if (col->get_mbminlen() == 1 && col->get_mbmaxlen() != 1) {
        // add mysql col len
        memset(dest + len, 0x20, field->pack_length() - len);
      }
    case DATA_SYS:
      if (field->type() == MYSQL_SYS_TYPE_TRX_ID) {
        trx_id_t id = mach_read_from_6(data);
        memcpy(dest, &id, sizeof(trx_id_t));
      }
      break;
    default:
      memcpy(dest, data, len);
  }
}

static uint32 find_col_in_index(const dict_index_t *index,
                                const dict_col_t *col) {
  for (uint32 i = 0; i < index->n_def; ++i) {
    const dict_field_t *field = index->fields + i;
    if (field->col == col) {
      return i;
    }
  }
  // TODO: handle error
  assert(0);
}

static ulint rec_get_nth_field_offs(const ulint *offsets, ulint n, ulint *len) {
  ulint offs;
  ulint length;
  ut_ad(n < rec_offs_n_fields(offsets));
  ut_ad(len);

  if (n == 0) {
    offs = 0;
  } else {
    offs = rec_offs_base(offsets)[n] & REC_OFFS_MASK;
  }

  length = rec_offs_base(offsets)[1 + n];

  if (length & REC_OFFS_SQL_NULL) {
    length = UNIV_SQL_NULL;
  } else if (length & REC_OFFS_DEFAULT) {
    length = UNIV_SQL_ADD_COL_DEFAULT;
  } else if (length & REC_OFFS_DROP) {
    length = UNIV_SQL_INSTANT_DROP_COL;
  } else {
    length &= REC_OFFS_MASK;
    length -= offs;
  }

  *len = length;
  return (offs);
}

static const byte *rec_get_nth_field(const rec_t *rec, const ulint *offsets,
                                     ulint n, ulint *len) {
  ulint off = rec_get_nth_field_offs(offsets, n, len);
  return (rec + off);
}

static void innodb_rec_convert_to_mysql_rec(const rec_t *rec,
                                            const dict_index_t *rec_index,
                                            const ulint *offsets, TABLE *table,
                                            uint32 n_fields) {
  dict_table_t *dict_table = rec_index->table;
  uint32 n_cols = dict_table->n_cols;

  uchar *buf = table->record[0];

  assert(rec_index->n_def == offsets[1]);

  for (uint32 i = 0; i < n_fields; ++i) {
    dict_col_t *col = dict_table->cols + i;
    uint32 index_col = find_col_in_index(rec_index, col);
    ulint len;
    const byte *data = rec_get_nth_field(rec, offsets, index_col, &len);
    Field *field = *(table->field + i);
    if (len == UNIV_SQL_NULL) {
      field->set_null();
    }
    uint32 offset = field->offset(buf);
    innodb_field_convert_to_mysql_field(buf + offset, data, len, col, field);
  }
}

static uint64 get_trx_id(const rec_t *rec, const dict_index_t *rec_index,
                         const ulint *offsets) {
  ulint len;
  const byte *data = rec_get_nth_field(rec, offsets, rec_index->n_uniq, &len);
  assert(len == 6);
  trx_id_t trx_id = mach_read_from_6(data);
  return trx_id;
}

int apply_insert(const byte *rec, dict_index_t *index, ulint *offsets,
                 const dict_index_t *real_index) {
  std::string db_name, table_name;
  real_index->table->get_table_name(db_name, table_name);
  THD *thd = current_thd;

  // get field length from rapid
  TianmuSecondaryShare *share = loaded_tables->get(db_name, table_name);

  if (share == nullptr) {  // skip
    return RET_SUCCESS;
  }

  // MEM_ROOT *root =
  // uchar *record = new uchar[share->rec_buff_length + share->null_bytes];
  // Field **field_ptr = new Field *[share->fields + 1 + 1];
  // ptrdiff_t move_offset = record - share->default_values;
  // for (i = 0; i < share->fields; i++, field_ptr++) {

  // }

  // TABLE_SHARE *table_share = share->share_;
  // TABLE table;
  // if (open_table_from_share(current_thd, table_share, "", 0,
  //                           (uint)READ_ALL | SKIP_NEW_HANDLER, 0, &table,
  //                           false, nullptr)) {
  //   // TODO: handle error
  //   assert(0);
  // }

  TABLE *table = &share->table_;

  uint32 n_fields = share->fields_.size();

  // convert
  innodb_rec_convert_to_mysql_rec(rec, real_index, offsets, table, n_fields);

  // apply
  const char *primary_key_name = share->primary_key_name_.c_str();
  std::vector<Field *> field_lst;
  uint32 primary_key_idx = n_fields;
  Field *field = nullptr;
  for (uint32 i = 0; i < n_fields; ++i) {
    field = *(table->field + i);
    // Skip columns marked as NOT SECONDARY.
    if (field->is_flag_set(NOT_SECONDARY_FLAG)) continue;

    field_lst.push_back(field);

    if (field->is_null())
      ;

    const char *field_name = field->field_name;

    if (!strcmp(field_name, primary_key_name)) {
      primary_key_idx = (int)field_lst.size() - 1;
    }
  }

  // get trx id
  uint64 trx_id = get_trx_id(rec, real_index, offsets);

  Tianmu::IMCS::Imcs *imcs =
      Tianmu::IMCS::IMCS_Instance::GetInstance("Tianmu_rapid");

  uint64 row_id;
  int ret;
  if (RET_FAIL(
          imcs->insert(share, field_lst, primary_key_idx, &row_id, trx_id))) {
    // TODO: handle error
    assert(0);
  }
  return RET_SUCCESS;
}

int apply_delete(const byte *rec, const dict_index_t *index, ulint *offsets) {
  std::string db_name, table_name;
  index->table->get_table_name(db_name, table_name);
  THD *thd = current_thd;

  // get field length from rapid
  TianmuSecondaryShare *share = loaded_tables->get(db_name, table_name);

  if (share == nullptr) {  // skip
    return RET_SUCCESS;
  }

  TABLE *table = &share->table_;

  uint32 n_fields = share->fields_.size();

  // convert
  innodb_rec_convert_to_mysql_rec(rec, index, offsets, table, n_fields);

  // apply
  const char *primary_key_name = share->primary_key_name_.c_str();
  Field *primary_key_field = nullptr;
  Field *field = nullptr;
  for (uint32 i = 0; i < n_fields; ++i) {
    field = *(table->field + i);
    // Skip columns marked as NOT SECONDARY.
    if (field->is_flag_set(NOT_SECONDARY_FLAG)) continue;

    if (field->is_null())
      ;

    const char *field_name = field->field_name;

    if (!strcmp(field_name, primary_key_name)) {
      primary_key_field = field;
    }
  }

  uint64 trx_id = get_trx_id(rec, index, offsets);

  Tianmu::IMCS::Imcs *imcs =
      Tianmu::IMCS::IMCS_Instance::GetInstance("Tianmu_rapid");

  int ret = RET_SUCCESS;

  if (RET_FAIL(imcs->remove(share, primary_key_field, trx_id))) {
    // TODO: handle error
    assert(0);
  }

  return ret;
}

}  // namespace IMCS
}  // namespace Tianmu