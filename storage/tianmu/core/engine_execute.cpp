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

#include "handler/ha_my_tianmu.h"

namespace Tianmu {
namespace core {

using Tianmu::DBHandler::Query_Route_To;

int optimize_select(THD *thd, ulong select_options, Query_result *result, Query_block *select_lex,
                    int &optimize_after_tianmu, int &free_join);

class KillTimer {
 public:
  KillTimer(THD *thd, long secs) {
    if (secs == 0) return;
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
    if (timer_settime(id, 0, &interval, NULL)) {
      TIANMU_LOG(LogCtl_Level::INFO, "Failed to set up timer. error =%d[%s]", errno, std::strerror(errno));
      return;
    }
    armed = true;
  }
  KillTimer() = delete;
  ~KillTimer() {
    if (armed) timer_delete(id);
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
Query_Route_To::TO_MYSQL is returned and MySQL engine continues query
execution.
*/
Query_Route_To Engine::Handle_Query(THD *thd, LEX *lex, Query_result *&result, ulong setup_tables_done_option, int &res,
                                    int &optimize_after_tianmu, int &tianmu_free_join, int with_insert) {
  KillTimer timer(thd, tianmu_sysvar_max_execution_time);

  int in_case_of_failure_can_go_to_mysql;

  optimize_after_tianmu = false;
  tianmu_free_join = 0;

  Query_expression *unit = NULL;
  Query_block *select_lex = NULL;
  Query_result_export *se = NULL;

  if (tianmu_sysvar_pushdown) thd->variables.optimizer_switch |= OPTIMIZER_SWITCH_ENGINE_CONDITION_PUSHDOWN;
  if (!IsTIANMURoute(thd, lex->query_tables, lex->query_block, in_case_of_failure_can_go_to_mysql, with_insert)) {
    return Query_Route_To::TO_MYSQL;
  }

  if (lock_tables(thd, thd->lex->query_tables, thd->lex->table_count, 0)) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to lock tables for query '%s'", thd->query().str);
    return Query_Route_To::TO_TIANMU;
  }
  /*
    Only register query in cache if it tables were locked above.
    Tables must be locked before storing the query in the query cache.
  */
  // query_cache.store_query(thd, thd->lex->query_tables); // stonedb8

  tianmu_stat.select++;

  // at this point all tables are in RCBase engine, so we can proceed with the
  // query and we know that if the result goes to the file, the TIANMU_DATAFORMAT is
  // one of TIANMU formats
  Query_Route_To route = Query_Route_To::TO_TIANMU;
  Query_block *save_current_select = lex->current_query_block();
  List<Query_expression> derived_optimized;  // collection to remember derived
                                             // tables that are optimized
  if (lex->unit->derived_table) {
    // Derived tables are processed completely in the function
    // open_and_lock_tables(...). To avoid execution of derived tables in
    // open_and_lock_tables(...) the function mysql_derived_filling(..)
    // optimizing and executing derived tables is passed over, then optimization
    // of derived tables must go here.
    res = false;
    int free_join = false;
    lex->thd->derived_tables_processing = true;
    for (Query_block *sl = lex->all_query_blocks_list; sl; sl = sl->next_select_in_list())  // for all selects
      for (TABLE_LIST *cursor = sl->get_table_list(); cursor; cursor = cursor->next_local)  // for all tables
        if (cursor->table && cursor->is_view_or_derived()) {  // data source (view or FROM subselect)
          // optimize derived table
          Query_block *first_select = cursor->derived_query_expression()->first_query_block();
          if (first_select->next_query_block() &&
              first_select->next_query_block()->linkage == UNION_TYPE) {          //?? only if union
            if (lex->is_explain() || cursor->derived_query_expression()->item) {  //??called for explain
              // OR there is subselect(?)
              route = Query_Route_To::TO_MYSQL;
              goto ret_derived;
            }
            if (!cursor->derived_query_expression()->is_executed() ||
                cursor->derived_query_expression()->uncacheable) {  //??not already executed (not
                                                                    // materialized?)
              // OR not cacheable (meaning not yet in cache, i.e. not
              // materialized it seems to boil down to NOT MATERIALIZED(?)
              res = cursor->derived_query_expression()->optimize_for_tianmu(thd);  //===exec()
              derived_optimized.push_back(cursor->derived_query_expression());
            }
          } else {                                                             //??not union
            cursor->derived_query_expression()->set_limit(thd, first_select);  // stonedb8
            if (cursor->derived_query_expression()->select_limit_cnt == HA_POS_ERROR)
              first_select->remove_base_options(OPTION_FOUND_ROWS);
            lex->set_current_query_block(first_select);
            int optimize_derived_after_tianmu = false;
            res = optimize_select(
                thd, ulong(first_select->active_options() | thd->variables.option_bits | SELECT_NO_UNLOCK),
                (Query_result *)cursor->derived_result, first_select, optimize_derived_after_tianmu, free_join);
            if (optimize_derived_after_tianmu) derived_optimized.push_back(cursor->derived_query_expression());
          }
          lex->set_current_query_block(save_current_select);
          if (!res && free_join)  // no error &
            route = Query_Route_To::TO_MYSQL;
          if (res || route == Query_Route_To::TO_MYSQL) goto ret_derived;
        }
    lex->thd->derived_tables_processing = false;
  }

  se = dynamic_cast<Query_result_export *>(result);
  if (se != NULL) result = new exporter::select_tianmu_export(se);
  // prepare, optimize and execute the main query
  select_lex = lex->query_block;
  unit = lex->unit;
  if (select_lex->next_query_block()) {  // it is union
    if (!(res = unit->prepare(thd, result, nullptr, (ulong)(SELECT_NO_UNLOCK | setup_tables_done_option),
                              0))) {  // stonedb8
      // similar to mysql_union(...) from sql_union.cpp

      /* FIXME: create_table is private in mysql5.6
         select_create* sc = dynamic_cast<select_create*>(result);
         if (sc && sc->create_table->table && sc->create_table->table->db_stat
         != 0) { my_error(ER_TABLE_EXISTS_ERROR, MYF(0),
         sc->create_table->table_name); res = 1; } else
       */
      if (lex->is_explain() || unit->item)  // explain or sth was already computed - go to mysql
        route = Query_Route_To::TO_MYSQL;
      else {
        int old_executed = unit->is_executed();
        res = unit->optimize_for_tianmu(thd);  //====exec()
        optimize_after_tianmu = true;
        if (!res) {
          try {
            route = ha_rcengine_->Execute(thd, thd->lex, result, unit);  // stonedb8
            if (route == Query_Route_To::TO_MYSQL) {
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
            route = Query_Route_To::TO_TIANMU;
            res = true;
          }
        }
      }
    }
    if (res || route == Query_Route_To::TO_TIANMU) {
      // stonedb8 start
      // res |= (int)unit->cleanup(thd, 0);
      unit->cleanup(thd, 0);
      // stonedb8 end

      optimize_after_tianmu = false;
    }
  } else {
    unit->set_limit(thd, unit->global_parameters());  // the fragment of original  // stonedb8
                                                      // handle_select(...)
    //(until the first part of optimization)
    // used for non-union select

    //'options' of mysql_select will be set in JOIN, as far as JOIN for
    // every PS/SP execution new, we will not need reset this flag if
    // setup_tables_done_option changed for next rexecution

    int err;
    err = optimize_select(thd,
                          ulong(select_lex->active_options() | thd->variables.option_bits | setup_tables_done_option),
                          result, select_lex, optimize_after_tianmu, tianmu_free_join);

    // RCBase query engine entry point
    if (!err) {
      try {
        route = Execute(thd, lex, result);
        if (route == Query_Route_To::TO_MYSQL && !in_case_of_failure_can_go_to_mysql) {
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
        route = Query_Route_To::TO_TIANMU;
        err = true;
      }
    }
    if (tianmu_free_join) {  // there was a join created in an upper function
      // so an upper function will do the cleanup
      if (err || route == Query_Route_To::TO_TIANMU) {
        thd->set_proc_info("end");  // stonedb8

        // stonedb8 start
        // err |= (int)select_lex->cleanup(0);
        select_lex->cleanup(thd, 0);
        // stonedb8 end

        optimize_after_tianmu = false;
        tianmu_free_join = 0;
      }
      res = (err || thd->is_error());
    } else
      res = select_lex->join->error;
  }
  if (select_lex->join && Query::IsLOJNew(select_lex->join_list))
    optimize_after_tianmu = 2;  // optimize partially (part=4), since part of LOJ
                                // optimization was already done
  res |= (int)thd->is_error();  // the ending of original handle_select(...) */
  if (unlikely(res)) {
    // If we had a another error reported earlier then this will be ignored //

    // stonedb8 start
    // result->send_error(ER_UNKNOWN_ERROR, ER(ER_UNKNOWN_ERROR));
    my_message(ER_UNKNOWN_ERROR, ER(ER_UNKNOWN_ERROR), MYF(0));
    result->abort_result_set(thd);
    // stonedb8 end
  }
  if (se != NULL) {
    // free the tianmu export object,
    // restore the original mysql export object
    // and prepare if it is expected to be prepared
    if (!select_lex->next_query_block() && select_lex->join != nullptr && select_lex->query_result() == result) {
      select_lex->set_query_result(se);
      if (((exporter::select_tianmu_export *)result)->IsPrepared())
        se->prepare(thd, *(select_lex->join->fields), unit);  // stonedb8
    }

    delete result;
    result = se;
  }
ret_derived:
  // if the query is redirected to MySQL engine
  // optimization of derived tables must be completed
  // and derived tables must be filled
  if (route == Query_Route_To::TO_MYSQL) {
    for (Query_block *sl = lex->all_query_blocks_list; sl; sl = sl->next_select_in_list())
      for (TABLE_LIST *cursor = sl->get_table_list(); cursor; cursor = cursor->next_local)
        if (cursor->table && cursor->is_derived()) {
          lex->thd->derived_tables_processing = true;
          cursor->derived_query_expression()->optimize_after_tianmu(thd);
        }
    lex->set_current_query_block(save_current_select);
  }
  lex->thd->derived_tables_processing = false;

  return route;
}

/*
Prepares and optimizes a single select for Tianmu engine
*/
int optimize_select(THD *thd, ulong select_options, Query_result *result, Query_block *select_lex,
                    int &optimize_after_tianmu, int &free_join) {
  // copied from sql_select.cpp from the beginning of mysql_select(...)
  int err = 0;
  free_join = 1;
  select_lex->context.resolve_in_select_list = true;
  JOIN *join = nullptr;
  if (select_lex->join != nullptr) {
    join = select_lex->join;
    // here is EXPLAIN of subselect or derived table
    if (select_lex->linkage != DERIVED_TABLE_TYPE || (select_options & (1ULL << 2))) {
      // global_options_type means a global option, such as limit order by, etc., of a union query. refer to
      // Query_expression::add_fake_query_block
      if (select_lex->linkage != GLOBAL_OPTIONS_TYPE) {
        if (result->prepare(thd, *select_lex->join->fields, select_lex->master_query_expression())) {
          return true;
        }
      } else {  // it is a global_opton query block, such as limit, order by, etc.
        if ((err = select_lex->prepare(thd, nullptr)))  // stonedb8
        {
          return err;
        }
      }
    }
    free_join = 0;
    join->select_options = select_options;
  } else {
    thd_proc_info(thd, "init");

    if ((err = select_lex->prepare(thd, nullptr)))  // stonedb8
    {
      return err;
    }
    if (result->prepare(thd, select_lex->fields, select_lex->master_query_expression())) {
      return true;
    }
    if (!(join = new JOIN(thd, select_lex))) return true; /* purecov: inspected */
    select_lex->set_join(thd, join);
  }

  join->best_rowcount = 2;
  optimize_after_tianmu = true;
  // all the preparation operations are done, therefore, we set the `part` to 1, then pass it to JOIN::optimize.
  // 1 means we have done the preparation.
  if ((err = join->optimize(1))) return err;
  return false;
}

Query_Route_To handle_exceptions(THD *, Transaction *, bool with_error = false);

Query_Route_To Engine::Execute(THD *thd, LEX *lex, Query_result *result_output, Query_expression *unit_for_union) {
  DEBUG_ASSERT(thd->lex == lex);
  Query_block *selects_list = lex->query_block;
  Query_block *last_distinct = NULL;
  if (unit_for_union != NULL) last_distinct = unit_for_union->union_distinct;

  int is_dumpfile = 0;
  const char *export_file_name = GetFilename(selects_list, is_dumpfile);
  if (is_dumpfile) {
    push_warning(thd, Sql_condition::SL_NOTE, ER_UNKNOWN_ERROR,
                 "Dumpfile not implemented in Tianmu, executed by MySQL engine.");
    return Query_Route_To::TO_MYSQL;
  }

  Query query(current_txn_);
  CompiledQuery cqu;

  if (result_output->start_execution(thd)) return Query_Route_To::TO_MYSQL;

  current_txn_->ResetDisplay();  // switch display on
  query.SetRoughQuery(selects_list->active_options() & SELECT_ROUGHLY);

  try {
    if (query.Compile(&cqu, selects_list, last_distinct) == Query_Route_To::TO_MYSQL) {
      push_warning(thd, Sql_condition::SL_NOTE, ER_UNKNOWN_ERROR,
                   "Query syntax not implemented in Tianmu, executed by MySQL engine.");
      return Query_Route_To::TO_MYSQL;
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
    std::shared_ptr<RCTable> rct;
    if (lex->sql_command == SQLCOM_INSERT_SELECT &&
        Engine::IsTIANMUTable(((Query_tables_list *)lex)->query_tables->table)) {
      std::string table_path = Engine::GetTablePath(((Query_tables_list *)lex)->query_tables->table);
      rct = current_txn_->GetTableByPathIfExists(table_path);
    }
    if (unit_for_union != NULL && !unit_for_union->is_prepared()) {
      int res = result_output->prepare(thd, unit_for_union->item_list, unit_for_union);  // stonedb8 add thd
      if (res) {
        TIANMU_LOG(LogCtl_Level::ERROR, "Error: Unsupported UNION");
        my_message(ER_UNKNOWN_ERROR, "Tianmu: unsupported UNION", MYF(0));
        throw ReturnMeToMySQLWithError();
      }
      // stonedb8 start
      if (export_file_name)
        sender.reset(new ResultExportSender(thd, result_output, *unit_for_union->get_field_list()));
      else
        sender.reset(new ResultSender(thd, result_output, *unit_for_union->get_field_list()));
    } else {
      if (export_file_name)
        sender.reset(new ResultExportSender(thd, result_output, *selects_list->get_fields_list()));
      else
        sender.reset(new ResultSender(thd, result_output, *selects_list->get_fields_list()));
      // stonedb8 end
    }

    TempTable *result = query.Preexecute(cqu, sender.get());
    ASSERT(result != NULL, "Query execution returned no result object");
    if (query.IsRoughQuery())
      result->RoughMaterialize(false, sender.get());
    else
      result->Materialize(false, sender.get());

    sender->Finalize(result);

    if (rct) {
      // in this case if this is an insert to RCTable from select based on the
      // same table RCTable object for this table can't be deleted in TempTable
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
  return Query_Route_To::TO_TIANMU;
}

Query_Route_To handle_exceptions(THD *thd, Transaction *cur_connection, bool with_error) {
  try {
    std::string msg = "Query terminated with exception: ";
    msg += thd->query().str;
    TIANMU_LOG(LogCtl_Level::INFO, msg);
    throw;
  } catch (common::NotImplementedException const &x) {
    rc_control_.lock(cur_connection->GetThreadID()) << "Switched to MySQL: " << x.what() << system::unlock;
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
    return Query_Route_To::TO_MYSQL;

  } catch (common::OutOfMemoryException const &x) {
    rc_control_.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(static_cast<int>(common::ErrorCode::OUT_OF_MEMORY),
               (std::string("Tianmu out of resources error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();

  } catch (common::DataTypeConversionException const &x) {
    rc_control_.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(static_cast<int>(common::ErrorCode::DATACONVERSION),
               (std::string("Tianmu specific error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();

  } catch (common::DBObjectException const &x) {  // the subselect had more than one row in a comparison
                                                  // without ANY or ALL
    rc_control_.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(ER_SYNTAX_ERROR, (std::string("Tianmu specific error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();

  } catch (common::KilledException const &) {
    rc_control_.lock(cur_connection->GetThreadID()) << "Stopped by user. " << system::unlock;
    my_message(ER_UNKNOWN_ERROR, (std::string("Stopped by user.")).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();

  } catch (common::FileException const &e) {
    rc_control_.lock(cur_connection->GetThreadID()) << "Error: " << e.what() << system::unlock;
    my_message(static_cast<int>(common::ErrorCode::CANNOT_OPEN_FILE_OR_PIPE),
               (std::string("Tianmu specific error: ") + e.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();

  } catch (common::Exception const &x) {
    rc_control_.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(ER_UNKNOWN_ERROR, "Tianmu other specific error", MYF(0));
    throw ReturnMeToMySQLWithError();

  } catch (std::bad_alloc const &) {
    rc_control_.lock(cur_connection->GetThreadID()) << "Error: std::bad_alloc caught" << system::unlock;
    my_message(ER_UNKNOWN_ERROR, (std::string("Tianmu out of memory error")).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  }

  return Query_Route_To::TO_MYSQL;
}

}  // namespace core
}  // namespace Tianmu

int Query_expression::optimize_for_tianmu(THD *thd) {
  // copied from sql_union.cpp from the beginning of st_select_lex_unit::exec()
  Query_block *lex_select_save = thd->lex->current_query_block();
  Query_block *select_cursor = first_query_block();

  if (is_executed() && !uncacheable && !thd->lex->is_explain()) return false;
  executed = 1;

  if (uncacheable || !item || !item->assigned() || thd->lex->is_explain()) {
    if (item) item->reset_value_registration();
    if (is_optimized() && item) {
      if (item->assigned()) {
        item->assigned(0);  // We will reinit & rexecute unit
        item->reset();
        table->file->ha_delete_all_rows();
      }
      // re-enabling indexes for next subselect iteration
      if (union_distinct && table->file->ha_enable_indexes(HA_KEY_SWITCH_ALL)) DEBUG_ASSERT(0);
    }
    for (Query_block *sl = select_cursor; sl; sl = sl->next_query_block()) {
      thd->lex->set_current_query_block(sl);
      sl->add_active_options(SELECT_NO_UNLOCK);
      /*
        setup_tables_done_option should be set only for very first SELECT,
        because it protect from secont setup_tables call for select-like non
        select commands (DELETE/INSERT/...) and they use only very first
        SELECT (for union it can be only INSERT ... SELECT).
      */
      if (sl->join == nullptr) {
        JOIN *join = new JOIN(thd, sl);
        if (!join) {
          thd->lex->set_current_query_block(lex_select_save);
          cleanup(thd, 0);  // stonedb8
          return true;
        }
        sl->set_join(thd, join);
      }
      if (is_optimized())
        sl->join->reset();
      else {
        set_limit(thd, sl);  // stonedb8
        if (sl == global_parameters() || thd->lex->is_explain()) {
          offset_limit_cnt = 0;
          // We can't use LIMIT at this stage if we are using ORDER BY for the
          // whole query
          if (sl->order_list.first || thd->lex->is_explain()) select_limit_cnt = HA_POS_ERROR;
        }

        // When using braces, SQL_CALC_FOUND_ROWS affects the whole query:
        // we don't calculate found_rows() per union part.
        // Otherwise, SQL_CALC_FOUND_ROWS should be done on all sub parts.
        sl->join->select_options = (select_limit_cnt == HA_POS_ERROR || thd->lex->unit->is_union())
                                       ? sl->active_options() & ~OPTION_FOUND_ROWS
                                       : sl->active_options() | found_rows_for_union;
        saved_error = sl->join->optimize(1);
      }

      // HERE ends the code from bool st_select_lex_unit::exec()
      if (saved_error) {
        thd->lex->set_current_query_block(lex_select_save);
        return saved_error;
      }
    }
  }
  /* code from st_select_lex_unit::exec*/
  if (!saved_error && !thd->is_fatal_error()) {
    /* Send result to 'result' */
    saved_error = true;
    set_limit(thd, global_parameters());  // stonedb8 add thd
    if (fake_query_block != NULL) {
      thd->lex->set_current_query_block(fake_query_block);
      if (!is_prepared()) {
        if (prepare_fake_query_block(thd)) return saved_error;
      }
      JOIN *join = nullptr;
      if (fake_query_block->join != nullptr)
        join = fake_query_block->join;
      else {
        if (!(join = new JOIN(thd, fake_query_block))) DEBUG_ASSERT(0);
        // fake_query_block->set_join(join);
      }

      if (!join->is_optimized()) {
        //    saved_error = join->prepare(fake_query_block->table_list.first, 0, 0,
        //                                global_parameters->order_list.elements,
        //                                global_parameters->order_list.first, NULL, NULL, fake_query_block,
        //                                this); //STONEDB UPGRADE
        if (!is_prepared()) {
          if (fake_query_block->prepare(thd, nullptr))  // stonedb8 add nullptr from m8
            return saved_error;
        }
      } else {
        join->examined_rows = 0;
        join->reset();
      }

      fake_query_block->table_list.clear();
    }
  }

  optimized = 1;
  thd->lex->set_current_query_block(lex_select_save);
  return false;
}

int Query_expression::optimize_after_tianmu(THD *thd) {
  Query_block *lex_select_save = thd->lex->current_query_block();
  for (Query_block *sl = first_query_block(); sl; sl = sl->next_query_block()) {
    thd->lex->set_current_query_block(sl);
    if (!sl->join) {
      JOIN *join = new JOIN(thd, sl);
      if (!join) {
        thd->lex->set_current_query_block(lex_select_save);
        cleanup(thd, 0);  // stonedb8
        return true;
      }
      sl->set_join(thd, join);
    }

    // int res = sl->join->optimize(2);
    if (int res = sl->join->optimize(2)) {
      thd->lex->set_current_query_block(lex_select_save);
      return res;
    }
  }

  if (fake_query_block && fake_query_block->join) {
    // fake_query_block->join must be cleaned up before returning to
    // MySQL route, otherwise sub select + union would coredump.
    thd->lex->set_current_query_block(fake_query_block);
    fake_query_block->cleanup(thd, 0);  // stonedb8
  }

  executed = 0;
  thd->lex->set_current_query_block(lex_select_save);

  return false;
}
