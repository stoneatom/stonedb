/* Copyright (c) 2008, 2022, Oracle and/or its affiliates.

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
  @file storage/perfschema/table_rpd_column_id.cc
  TABLE table_rpd_column_id.
*/

#include "storage/perfschema/table_rpd_column_id.h"

#include <assert.h>
#include <stddef.h>

#include <mysql/components/component_implementation.h>
#include <mysql/components/my_service.h>
#include <mysql/components/services/registry.h>
#include <mysql/service_plugin_registry.h>

#include "sql/current_thd.h"
#include "sql/field.h"
#include "sql/plugin_table.h"
#include "sql/sql_table.h"
#include "sql/table.h"

constexpr auto RPD_COLUMN_NAME_LENGTH = 64;
constexpr auto RPD_TALE_NAME_LENGTH = 64;

THR_LOCK table_rpd_column_id::s_table_lock;

namespace {
/** symbolic names for field offsets, keep in sync with field_types */
enum keys_field_offsets {
  FO_KEY_ID,
  FO_KEY_TABLE_ID,
  FO_KEY_TABLE_NAME,
  FO_KEY_COLUMN_NAME
};
}  // namespace

Plugin_table table_rpd_column_id::s_table_def(
    /* Schema name */
    "performance_schema",
    /* Name */
    "rpd_column_id",
    /* Definition */
    "  ID int unsigned NOT NULL,\n"
    "  TABLE_ID int unsigned NOT NULL,\n"
    "  TABLE_NAME VARCHAR(64) NOT NULL,\n"  //? we should be complied with
                                            // heatwave?
    "  COLUMN_NAME VARCHAR(64) NOT NULL\n",
    /* Options */
    " ENGINE=PERFORMANCE_SCHEMA CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
    /* Tablespace */
    nullptr);

PFS_engine_table_share table_rpd_column_id::m_share = {
    &pfs_readonly_acl,
    table_rpd_column_id::create,
    nullptr,                  /* write_row */
    nullptr,                  /* delete_all_rows */
    table_rpd_column_id::get_row_count,
    sizeof(PFS_simple_index), /* ref length */
    &s_table_lock,
    &s_table_def,
    false, /* perpetual */
    PFS_engine_table_proxy(),
    {0},
    false /* m_in_purgatory */
};

PFS_engine_table *table_rpd_column_id::create(PFS_engine_table_share *) {
  return new table_rpd_column_id();
}

table_rpd_column_id::table_rpd_column_id()
    : PFS_engine_table(&m_share, &m_pos), m_pos(0), m_next_pos(0) {
  /*
    Make a safe copy of the keys from the key that will not change while parsing
    it. The following part used as demo. When create/alter table with secondary
    engine, all the cols are included in secondary engine, which should be added
    into this meta-table. and should be removed when it droped or
    alter...removed.
  */
}

ha_rows table_rpd_column_id::get_row_count(void) {
  /*
  The real number of keys in the rpd_column_id does not matter,
  we only need to hint the optimizer,
  (which is a number of bytes, not keys)
  */
  return sizeof(row_rpd_column_id);
}

void table_rpd_column_id::reset_position(void) {
  m_pos.m_index = 0;
  m_next_pos.m_index = 0;
}

int table_rpd_column_id::rnd_pos(const void *pos) {
  set_position(pos);
  assert(m_pos.m_index < meta_rpd_columns.size());
  // m_row->m_id = meta_rpd_columns[m_pos.m_index].id;
  // m_row = &meta_rpd_columns[m_pos.m_index];
  return 0;
}

int table_rpd_column_id::rnd_init(bool scan) {
  m_pos.m_index = 0;
  m_next_pos.m_index = 0;
  return 0;
}

int table_rpd_column_id::rnd_next(void) {
  m_pos.set_at(&m_next_pos);

  if (m_pos.m_index < meta_rpd_columns.size()) {
    make_row(m_pos.m_index);
    m_next_pos.set_after(&m_pos);
    return 0;
  } else {
    return HA_ERR_END_OF_FILE;
  }
}

int table_rpd_column_id::make_row(uint index) {
  if (index >= meta_rpd_columns.size()) {
    return HA_ERR_END_OF_FILE;
  } else {
    m_row.m_id = meta_rpd_columns[index].id;
    m_row.m_table_id = meta_rpd_columns[index].table_id;

    strncpy(m_row.m_table_name, meta_rpd_columns[index].table_name,
            strlen(meta_rpd_columns[index].table_name));
    strncpy(m_row.m_column_name, meta_rpd_columns[index].column_name,
            strlen(meta_rpd_columns[index].column_name));
  }

  return 0;
}

int table_rpd_column_id::delete_row(uint colid, uint tableid) { return 0; }
int table_rpd_column_id::delete_all_rows() { return 0; }

int table_rpd_column_id::read_row_values(TABLE *table, unsigned char *buf,
                                         Field **fields, bool read_all) {
  Field *f;

  assert(&m_row);

  /* Set the null bits */
  assert(table->s->null_bytes == 0);
  buf[0] = 0;
  int index = 0;
  for (f = *fields; index < table->s->fields;
       index++, (f = *(fields + index))) {
    if (read_all || bitmap_is_set(table->read_set, f->field_index())) {
      switch (f->field_index()) {
        case FO_KEY_ID:
          set_field_ulong(f, m_row.m_id);
          break;
        case FO_KEY_TABLE_ID:
          set_field_ulong(f, m_row.m_table_id);
          break;
        case FO_KEY_TABLE_NAME:
          set_field_varchar_utf8mb4(f, m_row.m_table_name,
                                    strlen(m_row.m_table_name));
          break;
        case FO_KEY_COLUMN_NAME:
          set_field_varchar_utf8mb4(f, m_row.m_column_name,
                                    strlen(m_row.m_column_name));
          break;
        default:
          assert(false);
      }
    }
  }
  return 0;
}
