/* Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
   Use is subject to license terms

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335 USA
*/
#ifndef HA_MY_TIANMU_H_
#define HA_MY_TIANMU_H_
#pragma once

// mysql <--> tianmu interface functions
namespace Tianmu {
namespace DBHandler {

enum class QueryRouteTo {
  kToMySQL = 0,
  kToTianmu,
};

// processing the queries which routed to Tianmu engine.
QueryRouteTo ha_my_tianmu_query(THD *thd, LEX *lex, Query_result *&result_output, ulong setup_tables_done_option,
                                int &res, int &is_optimize_after_tianmu, int &tianmu_free_join,
                                int with_insert = false);

// update the comment for a column.
void ha_my_tianmu_update_and_store_col_comment(TABLE *table, int field_id, Field *source_field, int source_field_id,
                                               CHARSET_INFO *cs);

// used in stonedb 5.7, deleted in stonedb 8.0
bool ha_my_tianmu_set_statement_allowed(THD *thd, LEX *lex);

// processing the load operation.
bool ha_my_tianmu_load(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg);

}  // namespace DBHandler
}  //  namespace Tianmu
#endif  // TIANMU_HANDLER_HA_RCENGINE_H_
