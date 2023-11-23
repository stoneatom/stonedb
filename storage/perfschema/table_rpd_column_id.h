/* Copyright (c) 2012, 2022, Oracle and/or its affiliates.

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

#ifndef TABLE_RPD_COLUMN_ID_H
#define TABLE_RPD_COLUMN_ID_H

/**
  @file storage/perfschema/table_rpd_column_id.h
  TABLE RPD COLUMN ID.
*/

#include <sys/types.h>
#include "storage/perfschema/table_helper.h"

#include "sql/sql_table.h"
/**
  A row of PERFORMANCE_SCHEMA.RPD_COLUMN_ID table.
*/
struct row_rpd_column_id {
  /** Column ID.*/
  uint m_id;
  /** table id.*/
  uint m_table_id;
  /** table name. In UTF8MB4 */
  char m_table_name[MAX_RPD_NAME_STRING_LEN] = {0};
  /** column name. In UTF8MB4 */
  char m_column_name[MAX_RPD_NAME_STRING_LEN] = {0};
};

/**
  @addtogroup performance_schema_tables
  @{
*/

using rpd_column_id_container_t = std::vector<row_rpd_column_id>;

/** Table PERFORMANCE_SCHEMA.RPD_COLUMN_ID. */
class table_rpd_column_id : public PFS_engine_table {
 public:
  /** Table share */
  static PFS_engine_table_share m_share;
  /** Table builder */
  static PFS_engine_table *create(PFS_engine_table_share *);
  static ha_rows get_row_count();

  void reset_position(void) override;
  int rnd_init(bool scan);
  int rnd_next() override;
  int rnd_pos(const void *pos) override;
  int make_row(const uint index);

  int delete_row(uint colid, uint tableid);
  static int delete_all_rows();

 private:
  table_rpd_column_id();
  int read_row_values(TABLE *table, unsigned char *buf, Field **fields,
                      bool read_all) override;

 private:
  // for data lock.
  mysql_mutex_t data_lock;
  /** Current row. */
  row_rpd_column_id m_row;
  /** Current position. */
  PFS_simple_index m_pos;
  /** Next position. */
  PFS_simple_index m_next_pos;

  /** Table share lock. */
  static THR_LOCK s_table_lock;
  /** Table definition. */
  static Plugin_table s_table_def;
  /**Rows container*/
};
/** @} */

#endif
