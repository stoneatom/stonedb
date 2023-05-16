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

#include "common/mysql_gate.h"
#include "core/compilation_tools.h"
#include "core/engine.h"
#include "sql/sql_lex.h"
#include "system/configuration.h"
#include "util/log_ctl.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace handler {

enum class TianmuEngineReturnValues : int { LD_SUCCEED = 100, LD_FAILED = 101, LD_CONTINUE = 102 };

static bool AtLeastOneTianmuTableInvolved(Query_expression *qe) {
  for (Query_expression *expr = qe; expr; expr = expr->next_query_expression()) {
    for (Query_block *sl = expr->first_query_block(); sl; sl = sl->next_query_block()) {
      TABLE_LIST *tb = sl->get_table_list();
      for (; tb; tb = tb->next_global) {
        TABLE *table = tb->table;
        if (core::Engine::IsTianmuTable(table))
          return true;
      }
    }
  }

  return false;
}

static bool ForbiddenMySQLQueryPath([[maybe_unused]] Query_expression *qe) {
  // 0: not allowed route to mysql
  // 1: allowed route to mysql if tianmu engine not support
  return 0 == tianmu_sysvar_allowmysqlquerypath;
}

void ha_my_tianmu_update_and_store_col_comment(TABLE *table, int field_id, Field *source_field, int source_field_id,
                                               CHARSET_INFO *cs) {
  try {
    ha_rcengine_->UpdateAndStoreColumnComment(table, field_id, source_field, source_field_id, cs);
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
}

QueryRouteTo ha_my_tianmu_query(THD *thd, Query_expression *qe, Query_result *&result, ulong setup_tables_done_option,
                                int &res, int &optimize_after_tianmu, int &tianmu_free_join, bool with_insert) {
  QueryRouteTo ret = QueryRouteTo::TO_TIANMU;
  try {
    // ret is introduced here because in case of some exceptions
    // (e.g. thrown from ForbiddenMySQLQueryPath) we want to return

    // is explain, route to MySQL
    if (thd->lex->is_explain())
      return QueryRouteTo::TO_MYSQL;

    // is query, route to Tianmu
    ret = ha_rcengine_->Handle_Query(thd, qe, result, setup_tables_done_option, res, optimize_after_tianmu,
                                     tianmu_free_join, with_insert);

    if (ret == handler::QueryRouteTo::TO_MYSQL && AtLeastOneTianmuTableInvolved(qe) && ForbiddenMySQLQueryPath(qe)) {
      my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR),
                 "The query includes syntax that is not supported by the storage engine. "
                 "Either restructure the query with supported syntax, or enable the MySQL core::Query Path "
                 "in config file to execute the query with reduced performance.",
                 MYF(0));
      ret = QueryRouteTo::TO_TIANMU;
    }
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "Handle_Query Error: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  return ret;
}

static int Tianmu_LoadData(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg, char *errmsg, int len,
                           int &errcode) {
  common::TianmuError tianmu_error;
  int ret = static_cast<int>(TianmuEngineReturnValues::LD_FAILED);

  if (!core::Engine::IsTianmuTable(table_list->table))
    return static_cast<int>(TianmuEngineReturnValues::LD_CONTINUE);

  try {
    tianmu_error = ha_rcengine_->RunLoader(thd, ex, table_list, arg);

    ret = (tianmu_error.GetErrorCode() == common::ErrorCode::SUCCESS)
              ? static_cast<int>(TianmuEngineReturnValues::LD_SUCCEED)
              : (TIANMU_LOG(LogCtl_Level::ERROR, "RunLoader Error: %s", tianmu_error.Message().c_str()),
                 static_cast<int>(TianmuEngineReturnValues::LD_FAILED));

  } catch (std::exception &e) {
    tianmu_error = common::TianmuError(common::ErrorCode::UNKNOWN_ERROR, e.what());
    TIANMU_LOG(LogCtl_Level::ERROR, "RunLoader Error: %s", tianmu_error.Message().c_str());
  } catch (...) {
    tianmu_error = common::TianmuError(common::ErrorCode::UNKNOWN_ERROR, "An unknown system exception error caught.");
    TIANMU_LOG(LogCtl_Level::ERROR, "RunLoader Error: %s", tianmu_error.Message().c_str());
  }

  std::strncpy(errmsg, tianmu_error.Message().c_str(), len - 1);
  errmsg[len - 1] = '\0';
  errcode = static_cast<int>(tianmu_error.GetErrorCode());

  return ret;
}

// returning true means 'to continue'
bool ha_my_tianmu_load(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg) {
  char tianmu_msg[256] = {0};
  int tianmu_errcode{0};

  switch (static_cast<TianmuEngineReturnValues>(
      Tianmu_LoadData(thd, ex, table_list, arg, tianmu_msg, 256, tianmu_errcode))) {
    case TianmuEngineReturnValues::LD_CONTINUE:
      return true;
    case TianmuEngineReturnValues::LD_FAILED:
      my_message(tianmu_errcode, tianmu_msg, MYF(0));
      [[fallthrough]];
    case TianmuEngineReturnValues::LD_SUCCEED:
      return false;
    default:
      my_message(tianmu_errcode, tianmu_msg, MYF(0));
      break;
  }

  return false;
}

bool ha_my_tianmu_get_insert_delayed_flag([[maybe_unused]] THD *thd) { return tianmu_sysvar_insert_delayed; }
}  // namespace handler
}  // namespace Tianmu
