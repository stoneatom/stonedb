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
namespace handler {

enum class TIANMUEngineReturnValues { LD_Successed = 100, LD_Failed = 101, LD_Continue = 102 };

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

namespace {
bool AtLeastOneTIANMUTableInvolved(LEX *lex) {
  for (TABLE_LIST *table_list = lex->query_tables; table_list; table_list = table_list->next_global) {
    TABLE *table = table_list->table;
    if (core::Engine::IsTianmuTable(table))
      return TRUE;
  }
  return FALSE;
}

bool ForbiddenMySQLQueryPath([[maybe_unused]] LEX *lex) { return (tianmu_sysvar_allowmysqlquerypath == 0); }
}  // namespace

bool ha_my_tianmu_set_statement_allowed(THD *thd, LEX *lex) {
  if (AtLeastOneTIANMUTableInvolved(lex)) {
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
                                int &res, int &optimize_after_tianmu, int &tianmu_free_join, int with_insert) {
  QueryRouteTo ret = QueryRouteTo::kToTianmu;
  try {
    // handle_select_ret is introduced here because in case of some exceptions
    // (e.g. thrown from ForbiddenMySQLQueryPath) we want to return
    QueryRouteTo handle_select_ret = ha_rcengine_->HandleSelect(thd, lex, result_output, setup_tables_done_option, res,
                                                                optimize_after_tianmu, tianmu_free_join, with_insert);
    if (handle_select_ret == QueryRouteTo::kToMySQL && AtLeastOneTIANMUTableInvolved(lex) &&
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

int ha_my_tianmu_load(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg, char *errmsg, int len,
                      int &errcode) {
  common::TianmuError tianmu_error;
  int ret = static_cast<int>(TIANMUEngineReturnValues::LD_Failed);

  if (!core::Engine::IsTianmuTable(table_list->table))
    return static_cast<int>(TIANMUEngineReturnValues::LD_Continue);

  try {
    tianmu_error = ha_rcengine_->RunLoader(thd, ex, table_list, arg);
    if (tianmu_error.GetErrorCode() != common::ErrorCode::SUCCESS) {
      TIANMU_LOG(LogCtl_Level::ERROR, "RunLoader Error: %s", tianmu_error.Message().c_str());
    } else {
      ret = static_cast<int>(TIANMUEngineReturnValues::LD_Successed);
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

// returning true means 'to continue'
bool tianmu_load(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg) {
  char tianmu_msg[256] = {0};
  int tianmu_errcode = 0;
  switch (static_cast<TIANMUEngineReturnValues>(
      ha_my_tianmu_load(thd, ex, table_list, arg, tianmu_msg, 256, tianmu_errcode))) {
    case TIANMUEngineReturnValues::LD_Continue:
      return true;
    case TIANMUEngineReturnValues::LD_Failed:
      my_message(tianmu_errcode, tianmu_msg, MYF(0));
      [[fallthrough]];
    case TIANMUEngineReturnValues::LD_Successed:
      return false;
    default:
      my_message(tianmu_errcode, tianmu_msg, MYF(0));
      break;
  }
  return false;
}

}  // namespace handler
}  // namespace Tianmu
