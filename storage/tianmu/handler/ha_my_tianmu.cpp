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
#include "system/configuration.h"
#include "util/log_ctl.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace DBHandler {

enum class TianmuEngineReturnValues {
  kLoadSuccessed = 0,
  kLoadFailed,
  kLoadContinue,
};

bool AtLeastOneTianmuTableInvolved(LEX *lex) {
  for (TABLE_LIST *table_list = lex->query_tables; table_list; table_list = table_list->next_global) {
    TABLE *table = table_list->table;
    if (core::Engine::IsTianmuTable(table))
      return true;
  }
  return false;
}

bool ForbiddenMySQLQueryPath(LEX *lex [[maybe_unused]]) {
  // 0: not allowed route to mysql
  // 1: allowed route to mysql if tianmu engine not support
  return 0 == tianmu_sysvar_allowmysqlquerypath;
}

void ha_my_tianmu_update_and_store_col_comment(TABLE *table, int field_id, Field *source_field, int source_field_id,
                                               CHARSET_INFO *cs) {
  try {
    ha_tianmu_engine_->UpdateAndStoreColumnComment(table, field_id, source_field, source_field_id, cs);
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
}

// used in stonedb 5.7, deleted in stonedb 8.0
bool ha_my_tianmu_set_statement_allowed(THD *thd, LEX *lex) {
  if (AtLeastOneTianmuTableInvolved(lex)) {
    if (ForbiddenMySQLQueryPath(lex)) {
      my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR),
                 "Queries inside SET statements are not supported. "
                 "Enable the MySQL core::Query Path in my.cnf to execute the query "
                 "with reduced "
                 "performance.",
                 MYF(0));
      return false;
    } else
      push_warning(thd, Sql_condition::SL_NOTE, ER_UNKNOWN_ERROR,
                   "SET statement not supported by the Tianmu Optimizer. The "
                   "query executed "
                   "by MySQL engine.");
  }
  return true;
}

QueryRouteTo ha_my_tianmu_query(THD *thd, LEX *lex, Query_result *&result_output, ulong setup_tables_done_option,
                                int &res, int &is_optimize_after_tianmu, int &tianmu_free_join, int with_insert) {
  QueryRouteTo ret = QueryRouteTo::kToTianmu;
  try {
    // handle_select_ret is introduced here because in case of some exceptions
    // (e.g. thrown from ForbiddenMySQLQueryPath) we want to return
    QueryRouteTo handle_select_ret =
        ha_tianmu_engine_->HandleSelect(thd, lex, result_output, setup_tables_done_option, res,
                                        is_optimize_after_tianmu, tianmu_free_join, with_insert);
    if (handle_select_ret == QueryRouteTo::kToMySQL && AtLeastOneTianmuTableInvolved(lex) &&
        ForbiddenMySQLQueryPath(lex)) {
      my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR),
                 "The query includes syntax that is not supported by the storage engine. \
Either restructure the query with supported syntax, or enable the MySQL core::Query Path in config file to execute the query with reduced performance.",
                 MYF(0));
      handle_select_ret = QueryRouteTo::kToTianmu;
    }
    ret = handle_select_ret;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "HandleSelect Error: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
  return ret;
}

int tianmu_load_impl(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg, char *errmsg, int len,
                     int &errcode) {
  common::TianmuError tianmu_error;
  int ret = static_cast<int>(TianmuEngineReturnValues::kLoadFailed);

  if (!core::Engine::IsTianmuTable(table_list->table))
    return static_cast<int>(TianmuEngineReturnValues::kLoadContinue);

  try {
    tianmu_error = ha_tianmu_engine_->RunLoader(thd, ex, table_list, arg);
    if (tianmu_error.GetErrorCode() != common::ErrorCode::SUCCESS) {
      TIANMU_LOG(LogCtl_Level::ERROR, "RunLoader Error: %s", tianmu_error.Message().c_str());
    } else {
      ret = static_cast<int>(TianmuEngineReturnValues::kLoadSuccessed);
    }
  } catch (std::exception &e) {
    tianmu_error = common::TianmuError(common::ErrorCode::UNKNOWN_ERROR, e.what());
    TIANMU_LOG(LogCtl_Level::ERROR, "RunLoader Error: %s", tianmu_error.Message().c_str());
  } catch (...) {
    tianmu_error = common::TianmuError(common::ErrorCode::UNKNOWN_ERROR, "An unknown system exception error caught.");
    TIANMU_LOG(LogCtl_Level::ERROR, "RunLoader Error: %s", tianmu_error.Message().c_str());
  }
  std::strncpy(errmsg, tianmu_error.Message().c_str(), len - 1);
  errmsg[len - 1] = '\0';
  errcode = (int)tianmu_error.GetErrorCode();
  return ret;
}

const int kMaxErrMsgLen = 256;
// returning true means 'to continue'
bool ha_my_tianmu_load(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg) {
  char tianmu_msg[256] = {0};
  int tianmu_errcode = 0;
  switch (static_cast<TianmuEngineReturnValues>(
      tianmu_load_impl(thd, ex, table_list, arg, tianmu_msg, kMaxErrMsgLen, tianmu_errcode))) {
    case TianmuEngineReturnValues::kLoadContinue:
      return true;
    case TianmuEngineReturnValues::kLoadFailed:
      my_message(tianmu_errcode, tianmu_msg, MYF(0));
      [[fallthrough]];
    case TianmuEngineReturnValues::kLoadSuccessed:
      return false;
    default:
      my_message(tianmu_errcode, tianmu_msg, MYF(0));
      break;
  }
  return false;
}

}  // namespace DBHandler
}  // namespace Tianmu
