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

#include "sql/sql_class.h"
#include "sql/sql_exchange.h"  // sql_exchange

// mysql <--> tianmu interface functions
namespace Tianmu {
namespace handler {

enum class QueryRouteTo : unsigned int { TO_MYSQL = 0, TO_TIANMU = 1 };

// processing the queries which routed to Tianmu Engine.
QueryRouteTo ha_my_tianmu_query(THD *thd, Query_expression *qe, Query_result *&result_output,
                                ulong setup_tables_done_option, int &res, int &optimize_after_tianmu,
                                int &tianmu_free_join, int with_insert = false);

// update the comment for a column.
void ha_my_tianmu_update_and_store_col_comment(TABLE *table, int field_id, Field *source_field, int source_field_id,
                                               CHARSET_INFO *cs);
// processing the load operation.
bool ha_my_tianmu_load(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg);

bool ha_my_tianmu_get_insert_delayed_flag([[maybe_unused]] THD *thd);

}  // namespace handler
}  //  namespace Tianmu

#endif  // HA_MY_TIANMU_H_
