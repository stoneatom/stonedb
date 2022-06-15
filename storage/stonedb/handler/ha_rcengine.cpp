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

namespace stonedb {
namespace dbhandler {

enum class SDBEngineReturnValues { LD_Successed = 100, LD_Failed = 101, LD_Continue = 102 };

void SDB_UpdateAndStoreColumnComment(TABLE *table, int field_id, Field *source_field, int source_field_id,
                                     CHARSET_INFO *cs) {
  try {
    rceng->UpdateAndStoreColumnComment(table, field_id, source_field, source_field_id, cs);
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
}

namespace {
bool AtLeastOneSDBTableInvolved(LEX *lex) {
  for (TABLE_LIST *table_list = lex->query_tables; table_list; table_list = table_list->next_global) {
    TABLE *table = table_list->table;
    if (core::Engine::IsSDBTable(table)) return TRUE;
  }
  return FALSE;
}

bool ForbiddenMySQLQueryPath([[maybe_unused]] LEX *lex) { return (stonedb_sysvar_allowmysqlquerypath == 0); }
}  // namespace

bool SDB_SetStatementAllowed(THD *thd, LEX *lex) {
  if (AtLeastOneSDBTableInvolved(lex)) {
    if (ForbiddenMySQLQueryPath(lex)) {
      my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR),
                 "Queries inside SET statements are not supported. "
                 "Enable the MySQL core::Query Path in my.cnf to execute the query "
                 "with reduced "
                 "performance.",
                 MYF(0));
      return false;
    } else
      push_warning(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                   "SET statement not supported by the StoneDB Optimizer. The "
                   "query executed "
                   "by MySQL engine.");
  }
  return true;
}

int SDB_HandleSelect(THD *thd, LEX *lex, select_result *&result, ulong setup_tables_done_option, int &res,
                     int &optimize_after_sdb, int &sdb_free_join, int with_insert) {
  int ret = RCBASE_QUERY_ROUTE;
  try {
    // handle_select_ret is introduced here because in case of some exceptions
    // (e.g. thrown from ForbiddenMySQLQueryPath) we want to return
    // RCBASE_QUERY_ROUTE
    int handle_select_ret = rceng->HandleSelect(thd, lex, result, setup_tables_done_option, res, optimize_after_sdb,
                                                sdb_free_join, with_insert);
    if (handle_select_ret == RETURN_QUERY_TO_MYSQL_ROUTE && AtLeastOneSDBTableInvolved(lex) &&
        ForbiddenMySQLQueryPath(lex)) {
      my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR),
                 "The query includes syntax that is not supported by the storage engine. \
Either restructure the query with supported syntax, or enable the MySQL core::Query Path in config file to execute the query with reduced performance.",
                 MYF(0));
      handle_select_ret = RCBASE_QUERY_ROUTE;
    }
    ret = handle_select_ret;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "HandleSelect Error: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
  return ret;
}

int SDB_LoadData(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg, char *errmsg, int len, int &errcode) {
  common::SDBError sdb_error;
  int ret = static_cast<int>(SDBEngineReturnValues::LD_Failed);

  if (!core::Engine::IsSDBTable(table_list->table)) return static_cast<int>(SDBEngineReturnValues::LD_Continue);

  try {
    sdb_error = rceng->RunLoader(thd, ex, table_list, arg);
    if (sdb_error.GetErrorCode() != common::ErrorCode::SUCCESS) {
      STONEDB_LOG(LogCtl_Level::ERROR, "RunLoader Error: %s", sdb_error.Message().c_str());
    } else {
      ret = static_cast<int>(SDBEngineReturnValues::LD_Successed);
    }
  } catch (std::exception &e) {
    sdb_error = common::SDBError(common::ErrorCode::UNKNOWN_ERROR, e.what());
    STONEDB_LOG(LogCtl_Level::ERROR, "RunLoader Error: %s", sdb_error.Message().c_str());
  } catch (...) {
    sdb_error = common::SDBError(common::ErrorCode::UNKNOWN_ERROR, "An unknown system exception error caught.");
    STONEDB_LOG(LogCtl_Level::ERROR, "RunLoader Error: %s", sdb_error.Message().c_str());
  }
  std::strncpy(errmsg, sdb_error.Message().c_str(), len - 1);
  errmsg[len - 1] = '\0';
  errcode = (int)sdb_error.GetErrorCode();
  return ret;
}

// returning true means 'to continue'
bool stonedb_load(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg) {
  char sdbmsg[256];
  int sdberrcode;
  switch (static_cast<SDBEngineReturnValues>(SDB_LoadData(thd, ex, table_list, arg, sdbmsg, 256, sdberrcode))) {
    case SDBEngineReturnValues::LD_Continue:
      return true;
    case SDBEngineReturnValues::LD_Failed:
      my_message(sdberrcode, sdbmsg, MYF(0));
      [[fallthrough]];
    case SDBEngineReturnValues::LD_Successed:
      return false;
    default:
      my_message(sdberrcode, sdbmsg, MYF(0));
      break;
  }
  return false;
}

}  // namespace dbhandler
}  // namespace stonedb
