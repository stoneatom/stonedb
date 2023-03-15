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

#include <signal.h>
#include <sys/syscall.h>
#include <time.h>

#include "core/compilation_tools.h"
#include "core/compiled_query.h"
#include "core/engine.h"
#include "core/query.h"
#include "core/transaction.h"
#include "exporter/export2file.h"
#include "util/log_ctl.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {

int optimize_select(THD *thd, ulong select_options, Query_result *result, SELECT_LEX *select_lex,
                    int &is_optimize_after_tianmu, int &tianmu_free_join);

class KillTimer {
 public:
  KillTimer(THD *thd, long secs) {
    if (secs == 0)
      return;
    struct sigevent sev;
    sev.sigev_notify = SIGEV_THREAD_ID;
    sev.sigev_signo = SIGRTMIN;
    sev._sigev_un._tid = syscall(SYS_gettid);
    sev.sigev_value.sival_ptr = thd;
    if (timer_create(CLOCK_MONOTONIC, &sev, &id)) {
      TIANMU_LOG(LogCtl_Level::INFO, "Failed to create timer. error =%d[%s]", errno, std::strerror(errno));
      return;
    }

    struct itimerspec interval;
    std::memset(&interval, 0, sizeof(interval));
    interval.it_value.tv_sec = secs;
    if (timer_settime(id, 0, &interval, nullptr)) {
      TIANMU_LOG(LogCtl_Level::INFO, "Failed to set up timer. error =%d[%s]", errno, std::strerror(errno));
      return;
    }
    armed = true;
  }
  KillTimer() = delete;
  ~KillTimer() {
    if (armed)
      timer_delete(id);
  }

 private:
  timer_t id;
  bool armed = false;
};

/*
Handles a single query
If an error appears during query preparation/optimization
query structures are cleaned up and the function returns information about the
error through res'. If the query can not be compiled by Tianmu engine
QueryRouteTo::kToMySQL is returned and MySQL engine continues query
execution.
*/
QueryRouteTo Engine::HandleSelect(THD *thd, LEX *lex, Query_result *&result, ulong setup_tables_done_option, int &res,
                                  int &is_optimize_after_tianmu, int &tianmu_free_join, int with_insert) {
  KillTimer timer(thd, tianmu_sysvar_max_execution_time);

  int in_case_of_failure_can_go_to_mysql;

  is_optimize_after_tianmu = FALSE;
  tianmu_free_join = 0;

  SELECT_LEX_UNIT *unit = nullptr;
  SELECT_LEX *select_lex = nullptr;
  Query_result_export *se = nullptr;

  if (tianmu_sysvar_pushdown)
    thd->variables.optimizer_switch |= OPTIMIZER_SWITCH_ENGINE_CONDITION_PUSHDOWN;
  if (!IsTIANMURoute(thd, lex->query_tables, lex->select_lex, in_case_of_failure_can_go_to_mysql, with_insert)) {
    return QueryRouteTo::kToMySQL;
  }

  if (lock_tables(thd, thd->lex->query_tables, thd->lex->table_count, 0)) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to lock tables for query '%s'", thd->query().str);
    return QueryRouteTo::kToTianmu;
  }
  /*
    Only register query in cache if it tables were locked above.
    Tables must be locked before storing the query in the query cache.
  */
  query_cache.store_query(thd, thd->lex->query_tables);

  tianmu_stat.select++;

  // at this point all tables are in RCBase engine, so we can proceed with the
  // query and we know that if the result goes to the file, the TIANMU_DATAFORMAT is
  // one of TIANMU formats
  QueryRouteTo route = QueryRouteTo::kToTianmu;
  SELECT_LEX *save_current_select = lex->current_select();
  List<st_select_lex_unit> derived_optimized;  // collection to remember derived
                                               // tables that are optimized
  if (thd->fill_derived_tables() && lex->derived_tables) {
    // Derived tables are processed completely in the function
    // open_and_lock_tables(...). To avoid execution of derived tables in
    // open_and_lock_tables(...) the function mysql_derived_filling(..)
    // optimizing and executing derived tables is passed over, then optimization
    // of derived tables must go here.
    res = FALSE;
    int tianmu_free_join = FALSE;
    lex->thd->derived_tables_processing = TRUE;
    for (SELECT_LEX *sl = lex->all_selects_list; sl; sl = sl->next_select_in_list())        // for all selects
      for (TABLE_LIST *cursor = sl->get_table_list(); cursor; cursor = cursor->next_local)  // for all tables
        if (cursor->table && cursor->is_view_or_derived()) {  // data source (view or FROM subselect)
          // optimize derived table
          SELECT_LEX *first_select = cursor->derived_unit()->first_select();
          if (first_select->next_select() && first_select->next_select()->linkage == UNION_TYPE) {  //?? only if union
            if (lex->is_explain() || cursor->derived_unit()->item) {  //??called for explain
              // OR there is subselect(?)
              route = QueryRouteTo::kToMySQL;
              goto ret_derived;
            }
            if (!cursor->derived_unit()->is_executed() ||
                cursor->derived_unit()->uncacheable) {  //??not already executed (not
                                                        // materialized?)
              // OR not cacheable (meaning not yet in cache, i.e. not
              // materialized it seems to boil down to NOT MATERIALIZED(?)
              res = cursor->derived_unit()->optimize_for_tianmu();  //===exec()
              derived_optimized.push_back(cursor->derived_unit());
            }
          } else {  //??not union
            cursor->derived_unit()->set_limit(first_select);
            if (cursor->derived_unit()->select_limit_cnt == HA_POS_ERROR)
              first_select->remove_base_options(OPTION_FOUND_ROWS);
            lex->set_current_select(first_select);
            int optimize_derived_after_tianmu = FALSE;
            res = optimize_select(
                thd, ulong(first_select->active_options() | thd->variables.option_bits | SELECT_NO_UNLOCK),
                (Query_result *)cursor->derived_result, first_select, optimize_derived_after_tianmu, tianmu_free_join);
            if (optimize_derived_after_tianmu)
              derived_optimized.push_back(cursor->derived_unit());
          }
          lex->set_current_select(save_current_select);
          if (!res && tianmu_free_join)  // no error &
            route = QueryRouteTo::kToMySQL;
          if (res || route == QueryRouteTo::kToMySQL)
            goto ret_derived;
        }
    lex->thd->derived_tables_processing = FALSE;
  }

  se = dynamic_cast<Query_result_export *>(result);
  if (se != nullptr)
    result = new exporter::select_tianmu_export(se);
  // prepare, optimize and execute the main query
  select_lex = lex->select_lex;
  unit = lex->unit;
  if (select_lex->next_select()) {  // it is union
    if (!(res = unit->prepare(thd, result, (ulong)(SELECT_NO_UNLOCK | setup_tables_done_option), 0))) {
      // similar to mysql_union(...) from sql_union.cpp

      /* FIXME: create_table is private in mysql5.6
         select_create* sc = dynamic_cast<select_create*>(result);
         if (sc && sc->create_table->table && sc->create_table->table->db_stat
         != 0) { my_error(ER_TABLE_EXISTS_ERROR, MYF(0),
         sc->create_table->table_name); res = 1; } else
       */
      if (lex->is_explain() || unit->item)  // explain or sth was already computed - go to mysql
        route = QueryRouteTo::kToMySQL;
      else {
        int old_executed = unit->is_executed();
        res = unit->optimize_for_tianmu();  //====exec()
        is_optimize_after_tianmu = TRUE;
        if (!res) {
          try {
            route = ha_tianmu_engine_->Execute(unit->thd, unit->thd->lex, result, unit);
            if (route == QueryRouteTo::kToMySQL) {
              if (in_case_of_failure_can_go_to_mysql)
                if (old_executed)
                  unit->set_executed();
                else
                  unit->reset_executed();

              else {
                const char *err_msg =
                    "Error: Query syntax not implemented in Tianmu, can "
                    "export "
                    "only to MySQL format (set TIANMU_DATAFORMAT to 'MYSQL').";
                TIANMU_LOG(LogCtl_Level::ERROR, err_msg);
                my_message(ER_SYNTAX_ERROR, err_msg, MYF(0));
                throw ReturnMeToMySQLWithError();
              }
            }
          } catch (ReturnMeToMySQLWithError &) {
            route = QueryRouteTo::kToTianmu;
            res = TRUE;
          }
        }
      }
    }
    if (res || route == QueryRouteTo::kToTianmu) {
      res |= (int)unit->cleanup(0);
      is_optimize_after_tianmu = FALSE;
    }
  } else {
    unit->set_limit(unit->global_parameters());  // the fragment of original
                                                 // handle_select(...)
    //(until the first part of optimization)
    // used for non-union select

    //'options' of mysql_select will be set in JOIN, as far as JOIN for
    // every PS/SP execution new, we will not need reset this flag if
    // setup_tables_done_option changed for next rexecution

    int err;
    err = optimize_select(thd,
                          ulong(select_lex->active_options() | thd->variables.option_bits | setup_tables_done_option),
                          result, select_lex, is_optimize_after_tianmu, tianmu_free_join);

    // RCBase query engine entry point
    if (!err) {
      try {
        route = Execute(thd, lex, result);
        if (route == QueryRouteTo::kToMySQL && !in_case_of_failure_can_go_to_mysql) {
          TIANMU_LOG(LogCtl_Level::ERROR,
                     "Error: Query syntax not implemented in Tianmu, can export "
                     "only to MySQL format (set TIANMU_DATAFORMAT to 'MYSQL').");
          my_message(ER_SYNTAX_ERROR,
                     "Query syntax not implemented in Tianmu, can export only "
                     "to MySQL "
                     "format (set TIANMU_DATAFORMAT to 'MYSQL').",
                     MYF(0));
          throw ReturnMeToMySQLWithError();
        }
      } catch (ReturnMeToMySQLWithError &) {
        route = QueryRouteTo::kToTianmu;
        err = TRUE;
      }
    }
    if (tianmu_free_join) {  // there was a join created in an upper function
      // so an upper function will do the cleanup
      if (err || route == QueryRouteTo::kToTianmu) {
        thd->proc_info = "end";
        err |= (int)select_lex->cleanup(0);
        is_optimize_after_tianmu = FALSE;
        tianmu_free_join = 0;
      }
      res = (err || thd->is_error());
    } else
      res = select_lex->join->error;
  }
  if (select_lex->join && Query::IsLOJ(select_lex->join_list))
    is_optimize_after_tianmu = TRUE;  // optimize partially (phase=Doneoptimization), since part of LOJ
                                      // optimization was already done
  res |= (int)thd->is_error();        // the ending of original handle_select(...) */
  if (unlikely(res)) {
    // If we had a another error reported earlier then this will be ignored //
    result->send_error(ER_UNKNOWN_ERROR, ER(ER_UNKNOWN_ERROR));
    result->abort_result_set();
  }
  if (se != nullptr) {
    // free the tianmu export object,
    // restore the original mysql export object
    // and prepare if it is expected to be prepared
    if (!select_lex->next_select() && select_lex->join != 0 && select_lex->query_result() == result) {
      select_lex->set_query_result(se);
      if (((exporter::select_tianmu_export *)result)->IsPrepared())
        se->prepare(select_lex->join->fields_list, unit);
    }

    delete result;
    result = se;
  }
ret_derived:
  // if the query is redirected to MySQL engine
  // optimization of derived tables must be completed
  // and derived tables must be filled
  if (route == QueryRouteTo::kToMySQL) {
    for (SELECT_LEX *sl = lex->all_selects_list; sl; sl = sl->next_select_in_list())
      for (TABLE_LIST *cursor = sl->get_table_list(); cursor; cursor = cursor->next_local)
        if (cursor->table && cursor->is_derived()) {
          lex->thd->derived_tables_processing = TRUE;
          cursor->derived_unit()->optimize_after_tianmu();
        }
    lex->set_current_select(save_current_select);
  }
  lex->thd->derived_tables_processing = FALSE;

  return route;
}

/*
Prepares and optimizes a single select for Tianmu engine
*/
int optimize_select(THD *thd, ulong select_options, Query_result *result, SELECT_LEX *select_lex,
                    int &is_optimize_after_tianmu, int &tianmu_free_join) {
  // copied from sql_select.cpp from the beginning of mysql_select(...)
  int err = 0;
  tianmu_free_join = 1;
  select_lex->context.resolve_in_select_list = TRUE;
  JOIN *join;
  if (select_lex->join != 0) {
    join = select_lex->join;
    // here is EXPLAIN of subselect or derived table
    if (select_lex->linkage != DERIVED_TABLE_TYPE || (select_options & (1ULL << 2))) {
      if (select_lex->linkage != GLOBAL_OPTIONS_TYPE) {
        if (result->prepare(select_lex->join->fields_list, select_lex->master_unit()) || result->prepare2()) {
          return TRUE;
        }
      } else {
        if ((err = select_lex->prepare(thd))) {
          return err;
        }
      }
    }
    tianmu_free_join = 0;
    join->select_options = select_options;
  } else {
    thd_proc_info(thd, "init");

    if ((err = select_lex->prepare(thd))) {
      return err;
    }
    if (result->prepare(select_lex->fields_list, select_lex->master_unit()) || result->prepare2()) {
      return TRUE;
    }
    if (!(join = new JOIN(thd, select_lex)))
      return TRUE; /* purecov: inspected */
    select_lex->set_join(join);
  }
  join->best_rowcount = 2;
  is_optimize_after_tianmu = TRUE;
  if ((err = join->optimize(OptimizePhase::Before_LOJ_Transform)))
    return err;
  return FALSE;
}

QueryRouteTo handle_exceptions(THD *, Transaction *, bool with_error = false);

QueryRouteTo Engine::Execute(THD *thd, LEX *lex, Query_result *result_output, SELECT_LEX_UNIT *unit_for_union) {
  DEBUG_ASSERT(thd->lex == lex);
  SELECT_LEX *selects_list = lex->select_lex;
  SELECT_LEX *last_distinct = nullptr;
  if (unit_for_union != nullptr)
    last_distinct = unit_for_union->union_distinct;

  int is_dumpfile = 0;
  const char *export_file_name = GetFilename(selects_list, is_dumpfile);
  if (is_dumpfile) {
    push_warning(thd, Sql_condition::SL_NOTE, ER_UNKNOWN_ERROR,
                 "Dumpfile not implemented in Tianmu, executed by MySQL engine.");
    return QueryRouteTo::kToMySQL;
  }

  auto join_exec = ([&selects_list, &result_output] {
    selects_list->set_query_result(result_output);
    ASSERT(selects_list->join);
    selects_list->join->exec();
    return QueryRouteTo::kToTianmu;
  });

  // has sp fields but no from ...
  if ((!selects_list->table_list.elements) && (selects_list->fields_list.elements)) {
    List_iterator_fast<Item> li(selects_list->fields_list);
    for (Item *item = li++; item; item = li++) {
      if ((item->type() == Item::Type::FUNC_ITEM) &&
          (down_cast<Item_func *>(item)->functype() == Item_func::Functype::FUNC_SP)) {
        return join_exec();
      }
    }
  }

  // 1. all tables are derived and derived tables has no from table
  // 2. fields of derived tables has sp
  bool exec_direct = true;
  if ((selects_list->table_list.elements)) {
    TABLE_LIST *tables =
        selects_list->leaf_tables ? selects_list->leaf_tables : (TABLE_LIST *)selects_list->table_list.first;
    for (TABLE_LIST *table_ptr = tables; table_ptr; table_ptr = table_ptr->next_leaf) {
      // check derived table
      if (!table_ptr->is_derived()) {
        exec_direct = false;
        break;
      }

      // has from ...
      SELECT_LEX *first_select = table_ptr->derived_unit()->first_select();
      if (first_select->table_list.elements) {
        exec_direct = false;
        break;
      }

      // check fields has sp
      exec_direct = false;
      List_iterator_fast<Item> li(first_select->fields_list);
      for (Item *item = li++; item; item = li++) {
        if ((item->type() == Item::Type::FUNC_ITEM) &&
            (down_cast<Item_func *>(item)->functype() == Item_func::Functype::FUNC_SP)) {
          exec_direct = true;
          break;
        }
      }

      if (!exec_direct) {
        break;
      }
    }
  }

  for (SELECT_LEX *sl = selects_list; sl; sl = sl->next_select()) {
    if (sl->join->m_select_limit == 0) {
      exec_direct = true;
      break;
    }
  }

  if (exec_direct) {
    return join_exec();
  }

  Query query(current_txn_);
  CompiledQuery cqu;

  current_txn_->ResetDisplay();  // switch display on
  query.SetRoughQuery(selects_list->active_options() & SELECT_ROUGHLY);

  try {
    if (QueryRouteTo::kToMySQL == query.Compile(&cqu, selects_list, last_distinct)) {
      push_warning(thd, Sql_condition::SL_NOTE, ER_UNKNOWN_ERROR,
                   "Query syntax not implemented in Tianmu, executed by MySQL engine.");
      return QueryRouteTo::kToMySQL;
    }
  } catch (common::Exception const &x) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Query Compile Error: %s", x.what());
    my_message(ER_UNKNOWN_ERROR, (std::string("Tianmu compile specific error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  }

  std::unique_ptr<ResultSender> sender;

  FunctionExecutor lock_and_unlock_pack_info(std::bind(&Query::LockPackInfoForUse, &query),
                                             std::bind(&Query::UnlockPackInfoFromUse, &query));

  try {
    std::shared_ptr<TianmuTable> rct;
    if (lex->sql_command == SQLCOM_INSERT_SELECT &&
        Engine::IsTianmuTable(((Query_tables_list *)lex)->query_tables->table)) {
      std::string table_path = Engine::GetTablePath(((Query_tables_list *)lex)->query_tables->table);
      rct = current_txn_->GetTableByPathIfExists(table_path);
    }
    if ((unit_for_union != nullptr) && (lex->sql_command != SQLCOM_CREATE_TABLE)) {  //  for exclude CTAS
      int res = result_output->prepare(unit_for_union->item_list, unit_for_union);
      if (res) {
        TIANMU_LOG(LogCtl_Level::ERROR, "Error: Unsupported UNION");
        my_message(ER_UNKNOWN_ERROR, "Tianmu: unsupported UNION", MYF(0));
        throw ReturnMeToMySQLWithError();
      }
      if (export_file_name)
        sender.reset(new ResultExportSender(unit_for_union->thd, result_output, unit_for_union->item_list));
      else
        sender.reset(new ResultSender(unit_for_union->thd, result_output, unit_for_union->item_list));
    } else {
      if (export_file_name)
        sender.reset(new ResultExportSender(selects_list->master_unit()->thd, result_output, selects_list->item_list));
      else
        sender.reset(new ResultSender(selects_list->master_unit()->thd, result_output, selects_list->item_list));
    }

    TempTable *result = query.Preexecute(cqu, sender.get());
    ASSERT(result != nullptr, "Query execution returned no result object");
    if (query.IsRoughQuery())
      result->RoughMaterialize(false, sender.get());
    else
      result->Materialize(false, sender.get());

    sender->Finalize(result);

    if (rct) {
      // in this case if this is an insert to TianmuTable from select based on the
      // same table TianmuTable object for this table can't be deleted in TempTable
      // destructor It will be deleted in RefreshTables method that will be
      // called on commit
      result->RemoveFromManagedList(rct.get());
      query.RemoveFromManagedList(rct);
      rct.reset();
    }
    sender.reset();
  } catch (...) {
    bool with_error = false;
    if (sender) {
      if (sender->SentRows() > 0) {
        sender->CleanUp();
        with_error = true;
      } else
        sender->CleanUp();
    }
    return (handle_exceptions(thd, current_txn_, with_error));
  }
  return QueryRouteTo::kToTianmu;
}

QueryRouteTo handle_exceptions(THD *thd, Transaction *cur_connection, bool with_error) {
  try {
    std::string msg = "Query terminated with exception: ";
    msg += thd->query().str;
    TIANMU_LOG(LogCtl_Level::INFO, msg);
    throw;
  } catch (common::NotImplementedException const &x) {
    tianmu_control_.lock(cur_connection->GetThreadID()) << "Switched to MySQL: " << x.what() << system::unlock;
    my_message(ER_UNKNOWN_ERROR,
               (std::string("The query includes syntax that is not supported "
                            "by the storage engine. Tianmu: ") +
                x.what())
                   .c_str(),
               MYF(0));
    if (with_error) {
      std::string msg(x.what());
      msg.append(" Can't switch to MySQL execution path");
      throw common::InternalException(msg);
    }
    return QueryRouteTo::kToMySQL;
  } catch (common::OutOfMemoryException const &x) {
    tianmu_control_.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(static_cast<int>(common::ErrorCode::OUT_OF_MEMORY),
               (std::string("Tianmu out of resources error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (common::DataTypeConversionException const &x) {
    tianmu_control_.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(static_cast<int>(common::ErrorCode::DATACONVERSION),
               (std::string("Tianmu specific error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (common::DBObjectException const &x) {  // the subselect had more than one row in a comparison
                                                  // without ANY or ALL
    tianmu_control_.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(ER_SYNTAX_ERROR, (std::string("Tianmu specific error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (common::KilledException const &) {
    tianmu_control_.lock(cur_connection->GetThreadID()) << "Stopped by user. " << system::unlock;
    my_message(ER_UNKNOWN_ERROR, (std::string("Stopped by user.")).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (common::FileException const &e) {
    tianmu_control_.lock(cur_connection->GetThreadID()) << "Error: " << e.what() << system::unlock;
    my_message(static_cast<int>(common::ErrorCode::CANNOT_OPEN_FILE_OR_PIPE),
               (std::string("Tianmu specific error: ") + e.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (common::Exception const &x) {
    tianmu_control_.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(ER_UNKNOWN_ERROR, x.getExceptionMsg().data(), MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (std::bad_alloc const &) {
    tianmu_control_.lock(cur_connection->GetThreadID()) << "Error: std::bad_alloc caught" << system::unlock;
    my_message(ER_UNKNOWN_ERROR, (std::string("Tianmu out of memory error")).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  }
  return QueryRouteTo::kToMySQL;
}
}  // namespace core
}  // namespace Tianmu

int st_select_lex_unit::optimize_for_tianmu() {
  // copied from sql_union.cpp from the beginning of st_select_lex_unit::exec()
  SELECT_LEX *lex_select_save = thd->lex->current_select();
  SELECT_LEX *select_cursor = first_select();

  if (is_executed() && !uncacheable && !thd->lex->is_explain())
    return FALSE;

  set_executed();

  if (uncacheable || !item || !item->assigned() || thd->lex->is_explain()) {
    if (item)
      item->reset_value_registration();
    if (is_optimized() && item) {
      if (item->assigned()) {
        item->assigned(0);  // We will reinit & rexecute unit
        item->reset();
        table->file->ha_delete_all_rows();
      }
      // re-enabling indexes for next subselect iteration
      if (union_distinct && table->file->ha_enable_indexes(HA_KEY_SWITCH_ALL))
        DEBUG_ASSERT(0);
    }
    for (SELECT_LEX *sl = select_cursor; sl; sl = sl->next_select()) {
      thd->lex->set_current_select(sl);
      sl->add_active_options(SELECT_NO_UNLOCK);
      /*
        setup_tables_done_option should be set only for very first SELECT,
        because it protect from secont setup_tables call for select-like non
        select commands (DELETE/INSERT/...) and they use only very first
        SELECT (for union it can be only INSERT ... SELECT).
      */
      if (!sl->join) {
        JOIN *join = new JOIN(thd, sl);
        if (!join) {
          thd->lex->set_current_select(lex_select_save);
          cleanup(0);
          return TRUE;
        }
        sl->set_join(join);
      }
      if (is_optimized())
        sl->join->reset();
      else {
        set_limit(sl);
        if (sl == global_parameters() || thd->lex->is_explain()) {
          offset_limit_cnt = 0;
          // We can't use LIMIT at this stage if we are using ORDER BY for the
          // whole query
          if (sl->order_list.first || thd->lex->is_explain())
            select_limit_cnt = HA_POS_ERROR;
        }

        // When using braces, SQL_CALC_FOUND_ROWS affects the whole query:
        // we don't calculate found_rows() per union part.
        // Otherwise, SQL_CALC_FOUND_ROWS should be done on all sub parts.
        sl->join->select_options = (select_limit_cnt == HA_POS_ERROR || sl->braces)
                                       ? sl->active_options() & ~OPTION_FOUND_ROWS
                                       : sl->active_options() | found_rows_for_union;
        saved_error = sl->join->optimize(OptimizePhase::Before_LOJ_Transform);
      }

      // HERE ends the code from bool st_select_lex_unit::exec()
      if (saved_error) {
        thd->lex->set_current_select(lex_select_save);
        return saved_error;
      }
    }
  }
  /* code from st_select_lex_unit::exec*/
  if (!saved_error && !thd->is_fatal_error) {
    /* Send result to 'result' */
    saved_error = true;
    set_limit(global_parameters());
    if (fake_select_lex != nullptr) {
      thd->lex->set_current_select(fake_select_lex);
      if (!is_prepared()) {
        if (prepare_fake_select_lex(thd))
          return saved_error;
      }
      JOIN *join;
      if (fake_select_lex->join)
        join = fake_select_lex->join;
      else {
        if (!(join = new JOIN(thd, fake_select_lex)))
          DEBUG_ASSERT(0);
        // fake_select_lex->set_join(join);
      }

      if (!join->is_optimized()) {
        //    saved_error = join->prepare(fake_select_lex->table_list.first, 0, 0,
        //                                global_parameters->order_list.elements,
        //                                global_parameters->order_list.first, nullptr, nullptr, fake_select_lex,
        //                                this); //STONEDB UPGRADE
        if (!is_prepared()) {
          if (fake_select_lex->prepare(thd))
            return saved_error;
        }
      } else {
        join->examined_rows = 0;
        join->reset();
      }

      fake_select_lex->table_list.empty();
    }
  }

  set_optimized();
  thd->lex->set_current_select(lex_select_save);
  return FALSE;
}

int st_select_lex_unit::optimize_after_tianmu() {
  SELECT_LEX *lex_select_save = thd->lex->current_select();
  for (SELECT_LEX *sl = first_select(); sl; sl = sl->next_select()) {
    thd->lex->set_current_select(sl);
    if (!sl->join) {
      JOIN *join = new JOIN(thd, sl);
      if (!join) {
        thd->lex->set_current_select(lex_select_save);
        cleanup(0);
        return TRUE;
      }
      sl->set_join(join);
    }
    int res = sl->join->optimize(OptimizePhase::After_LOJ_Transform);
    if (res) {
      thd->lex->set_current_select(lex_select_save);
      return res;
    }
  }
  if (fake_select_lex && fake_select_lex->join) {
    // fake_select_lex->join must be cleaned up before returning to
    // MySQL route, otherwise sub select + union would coredump.
    thd->lex->set_current_select(fake_select_lex);
    fake_select_lex->cleanup(0);
  }
  executed = 0;
  thd->lex->set_current_select(lex_select_save);
  return FALSE;
}

/**
  Optimize a query block and all inner query expressions for tianmu

  @param thd    thread handler
  @returns false if success, true if error
*/

bool SELECT_LEX::optimize_select_for_tianmu(THD *thd) {
  DBUG_ENTER("SELECT_LEX::optimize_select_for_tianmu");

  JOIN *const join = new JOIN(thd, this);
  if (!join)
    DBUG_RETURN(true);

  set_join(join);

  if (join->optimize(OptimizePhase::Before_LOJ_Transform))
    DBUG_RETURN(true);

  for (SELECT_LEX_UNIT *unit = first_inner_unit(); unit; unit = unit->next_unit()) {
    if (!unit->is_optimized())
      DBUG_RETURN(true);

    SELECT_LEX *save_select = thd->lex->current_select();

    for (SELECT_LEX *sl = unit->first_select(); sl; sl = sl->next_select()) {
      thd->lex->set_current_select(sl);
      unit->set_limit(sl);

      if (sl->optimize_select_for_tianmu(thd))
        DBUG_RETURN(true);

      if (unit->query_result())
        unit->query_result()->estimated_rowcount +=
            sl->is_implicitly_grouped() || sl->join->group_optimized_away ? 2 : sl->join->best_rowcount;
    }
    SELECT_LEX *unit_fake_select_lex = unit->fake_select_lex;

    if (unit_fake_select_lex) {
      thd->lex->set_current_select(unit_fake_select_lex);
      unit->set_limit(unit_fake_select_lex);

      if (unit_fake_select_lex->optimize_select_for_tianmu(thd))
        DBUG_RETURN(true);
    }

    unit->set_optimized();

    thd->lex->set_current_select(save_select);
  }

  DBUG_RETURN(false);
}

/**
  Optimize the query expression representing a derived table/view for tianmu table.

  @note
  If optimizer finds out that the derived table/view is of the type
  "SELECT a_constant" this functions also materializes it.

  @param thd thread handle

  @returns false if success, true if error.
*/

bool TABLE_LIST::optimize_derived_for_tianmu(THD *thd) {
  DBUG_ENTER("TABLE_LIST::optimize_derived_for_tianmu");

  SELECT_LEX_UNIT *const unit = derived_unit();

  assert(unit && unit->is_prepared() && !unit->is_optimized());

  if (unit && unit->is_prepared() && !unit->is_optimized()) {
    SELECT_LEX *save_select = thd->lex->current_select();

    for (SELECT_LEX *sl = unit->first_select(); sl; sl = sl->next_select()) {
      thd->lex->set_current_select(sl);

      // LIMIT is required for optimization
      unit->set_limit(sl);

      if (sl->optimize_select_for_tianmu(thd))
        DBUG_RETURN(true);

      if (unit->query_result())
        unit->query_result()->estimated_rowcount +=
            sl->is_implicitly_grouped() || sl->join->group_optimized_away ? 2 : sl->join->best_rowcount;
    }
    st_select_lex *unit_fake_select_lex = unit->fake_select_lex;
    if (unit_fake_select_lex) {
      thd->lex->set_current_select(unit_fake_select_lex);

      unit->set_limit(unit_fake_select_lex);

      if (unit_fake_select_lex->optimize_select_for_tianmu(thd))
        DBUG_RETURN(true);
    }

    unit->set_optimized();

    thd->lex->set_current_select(save_select);
  }

  if (thd->is_error())
    DBUG_RETURN(true);

  if (materializable_is_const() && (create_derived(thd) || materialize_derived(thd)))
    DBUG_RETURN(true);

  DBUG_RETURN(false);
}
