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

namespace stonedb {
namespace core {

int optimize_select(THD *thd, TABLE_LIST *tables, uint wild_num, List<Item> &fields, Item *conds, uint og_num,
                    ORDER *order, ORDER *group, Item *having, ulong select_options, select_result *result,
                    SELECT_LEX_UNIT *unit, SELECT_LEX *select_lex, int &optimize_after_sdb, int &free_join);

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
      STONEDB_LOG(LogCtl_Level::INFO, "Failed to create timer. error =%d[%s]", errno, std::strerror(errno));
      return;
    }

    struct itimerspec interval;
    std::memset(&interval, 0, sizeof(interval));
    interval.it_value.tv_sec = secs;
    if (timer_settime(id, 0, &interval, NULL)) {
      STONEDB_LOG(LogCtl_Level::INFO, "Failed to set up timer. error =%d[%s]", errno, std::strerror(errno));
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
error through res'. If the query can not be compiled by StoneDB engine
RETURN_QUERY_TO_MYSQL_ROUTE is returned and MySQL engine continues query
execution.
*/
int Engine::HandleSelect(THD *thd, LEX *lex, select_result *&result, ulong setup_tables_done_option, int &res,
                         int &optimize_after_sdb, int &sdb_free_join, int with_insert) {
  KillTimer timer(thd, stonedb_sysvar_max_execution_time);

  int in_case_of_failure_can_go_to_mysql;

  optimize_after_sdb = FALSE;
  sdb_free_join = 0;

  SELECT_LEX_UNIT *unit = NULL;
  SELECT_LEX *select_lex = NULL;
  select_export *se = NULL;

  thd->variables.engine_condition_pushdown = stonedb_sysvar_pushdown;
  if (!IsSDBRoute(thd, lex->query_tables, &lex->select_lex, in_case_of_failure_can_go_to_mysql, with_insert)) {
    return RETURN_QUERY_TO_MYSQL_ROUTE;
  }

  if (lock_tables(thd, thd->lex->query_tables, thd->lex->table_count, 0)) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Failed to lock tables for query '%s'", thd_query_string(thd)->str);
    return RCBASE_QUERY_ROUTE;
  }
  /*
    Only register query in cache if it tables were locked above.
    Tables must be locked before storing the query in the query cache.
  */
  query_cache_store_query(thd, thd->lex->query_tables);

  stonedb_stat.select++;

  // at this point all tables are in RCBase engine, so we can proceed with the
  // query and we know that if the result goes to the file, the SDB_DATAFORMAT is
  // one of SDB formats
  int route = RCBASE_QUERY_ROUTE;
  SELECT_LEX *save_current_select = lex->current_select;
  List<st_select_lex_unit> derived_optimized;  // collection to remember derived
                                               // tables that are optimized
  if (thd->fill_derived_tables() && lex->derived_tables) {
    // Derived tables are processed completely in the function
    // open_and_lock_tables(...). To avoid execution of derived tables in
    // open_and_lock_tables(...) the function mysql_derived_filling(..)
    // optimizing and executing derived tables is passed over, then optimization
    // of derived tables must go here.
    res = FALSE;
    int free_join = FALSE;
    lex->thd->derived_tables_processing = TRUE;
    for (SELECT_LEX *sl = lex->all_selects_list; sl; sl = sl->next_select_in_list())        // for all selects
      for (TABLE_LIST *cursor = sl->get_table_list(); cursor; cursor = cursor->next_local)  // for all tables
        if (cursor->table && cursor->derived) {  // data source (view or FROM subselect)
          // optimize derived table
          SELECT_LEX *first_select = cursor->derived->first_select();
          if (first_select->next_select() && first_select->next_select()->linkage == UNION_TYPE) {  //?? only if union
            if (cursor->derived->describe || cursor->derived->item) {  //??called for explain
              // OR there is subselect(?)
              route = RETURN_QUERY_TO_MYSQL_ROUTE;
              goto ret_derived;
            }
            if (!cursor->derived->executed || cursor->derived->uncacheable) {  //??not already executed (not
                                                                               // materialized?)
              // OR not cacheable (meaning not yet in cache, i.e. not
              // materialized it seems to boil down to NOT MATERIALIZED(?)
              res = cursor->derived->optimize_for_stonedb();  //===exec()
              derived_optimized.push_back(cursor->derived);
            }
          } else {  //??not union
            cursor->derived->set_limit(first_select);
            if (cursor->derived->select_limit_cnt == HA_POS_ERROR) first_select->options &= ~OPTION_FOUND_ROWS;
            lex->current_select = first_select;
            int optimize_derived_after_sdb = FALSE;
            res = optimize_select(
                thd, (TABLE_LIST *)first_select->table_list.first, first_select->with_wild, first_select->item_list,
                first_select->where, (first_select->order_list.elements + first_select->group_list.elements),
                (ORDER *)first_select->order_list.first, (ORDER *)first_select->group_list.first, first_select->having,
                ulong(first_select->options | thd->variables.option_bits | SELECT_NO_UNLOCK), cursor->derived_result,
                cursor->derived, first_select, optimize_derived_after_sdb, free_join);
            if (optimize_derived_after_sdb) derived_optimized.push_back(cursor->derived);
          }
          lex->current_select = save_current_select;
          if (!res && free_join)  // no error &
            route = RETURN_QUERY_TO_MYSQL_ROUTE;
          if (res || route == RETURN_QUERY_TO_MYSQL_ROUTE) goto ret_derived;
        }
    lex->thd->derived_tables_processing = FALSE;
  }

  se = dynamic_cast<select_export *>(result);
  if (se != NULL) result = new exporter::select_sdb_export(se);
  // prepare, optimize and execute the main query
  select_lex = &lex->select_lex;
  unit = &lex->unit;
  if (select_lex->next_select()) {  // it is union
    if (!(res = unit->prepare(thd, result, SELECT_NO_UNLOCK | setup_tables_done_option))) {
      // similar to mysql_union(...) from sql_union.cpp

      /* FIXME: create_table is private in mysql5.6
         select_create* sc = dynamic_cast<select_create*>(result);
         if (sc && sc->create_table->table && sc->create_table->table->db_stat
         != 0) { my_error(ER_TABLE_EXISTS_ERROR, MYF(0),
         sc->create_table->table_name); res = 1; } else
       */
      if (unit->describe || unit->item)  // explain or sth was already computed - go to mysql
        route = RETURN_QUERY_TO_MYSQL_ROUTE;
      else {
        int old_executed = unit->executed;
        res = unit->optimize_for_stonedb();  //====exec()
        optimize_after_sdb = TRUE;
        if (!res) {
          try {
            route = rceng->Execute(unit->thd, unit->thd->lex, result, unit);
            if (route == RETURN_QUERY_TO_MYSQL_ROUTE) {
              if (in_case_of_failure_can_go_to_mysql)
                unit->executed = old_executed;
              else {
                const char *err_msg =
                    "Error: Query syntax not implemented in StoneDB, can "
                    "export "
                    "only to MySQL format (set SDB_DATAFORMAT to 'MYSQL').";
                STONEDB_LOG(LogCtl_Level::ERROR, err_msg);
                my_message(ER_SYNTAX_ERROR, err_msg, MYF(0));
                throw ReturnMeToMySQLWithError();
              }
            }
          } catch (ReturnMeToMySQLWithError &) {
            route = RCBASE_QUERY_ROUTE;
            res = TRUE;
          }
        }
      }
    }
    if (res || route == RCBASE_QUERY_ROUTE) {
      res |= (int)unit->cleanup();
      optimize_after_sdb = FALSE;
    }
  } else {
    unit->set_limit(unit->global_parameters);  // the fragment of original
                                               // handle_select(...)
    //(until the first part of optimization)
    // used for non-union select

    //'options' of mysql_select will be set in JOIN, as far as JOIN for
    // every PS/SP execution new, we will not need reset this flag if
    // setup_tables_done_option changed for next rexecution

    int err;
    err = optimize_select(thd, (TABLE_LIST *)select_lex->table_list.first, select_lex->with_wild, select_lex->item_list,
                          select_lex->where, select_lex->order_list.elements + select_lex->group_list.elements,
                          (ORDER *)select_lex->order_list.first, (ORDER *)select_lex->group_list.first,
                          select_lex->having,
                          ulong(select_lex->options | thd->variables.option_bits | setup_tables_done_option), result,
                          unit, select_lex, optimize_after_sdb, sdb_free_join);
    // RCBase query engine entry point
    if (!err) {
      try {
        route = Execute(thd, lex, result);
        if (route == RETURN_QUERY_TO_MYSQL_ROUTE && !in_case_of_failure_can_go_to_mysql) {
          STONEDB_LOG(LogCtl_Level::ERROR,
                      "Error: Query syntax not implemented in StoneDB, can export "
                      "only to MySQL format (set SDB_DATAFORMAT to 'MYSQL').");
          my_message(ER_SYNTAX_ERROR,
                     "Query syntax not implemented in StoneDB, can export only "
                     "to MySQL "
                     "format (set SDB_DATAFORMAT to 'MYSQL').",
                     MYF(0));
          throw ReturnMeToMySQLWithError();
        }
      } catch (ReturnMeToMySQLWithError &) {
        route = RCBASE_QUERY_ROUTE;
        err = TRUE;
      }
    }
    if (sdb_free_join) {  // there was a join created in an upper function
      // so an upper function will do the cleanup
      if (err || route == RCBASE_QUERY_ROUTE) {
        thd->proc_info = "end";
        err |= (int)select_lex->cleanup();
        optimize_after_sdb = FALSE;
        sdb_free_join = 0;
      }
      res = (err || thd->is_error());
    } else
      res = select_lex->join->error;
  }
  if (select_lex->join && Query::IsLOJ(select_lex->join->join_list))
    optimize_after_sdb = 2;     // optimize partially (part=4), since part of LOJ
                                // optimization was already done
  res |= (int)thd->is_error();  // the ending of original handle_select(...) */
  if (unlikely(res)) {
    // If we had a another error reported earlier then this will be ignored //
    result->send_error(ER_UNKNOWN_ERROR, ER(ER_UNKNOWN_ERROR));
    result->abort_result_set();
  }
  if (se != NULL) {
    // free the sdb export object,
    // restore the original mysql export object
    // and prepare if it is expected to be prepared
    if (!select_lex->next_select() && select_lex->join != 0 && select_lex->join->result == result) {
      select_lex->join->result = se;
      if (((exporter::select_sdb_export *)result)->IsPrepared()) se->prepare(select_lex->join->fields_list, unit);
    }
    delete result;
    result = se;
  }
ret_derived:
  // if the query is redirected to MySQL engine
  // optimization of derived tables must be completed
  // and derived tables must be filled
  if (route == RETURN_QUERY_TO_MYSQL_ROUTE) {
    for (SELECT_LEX *sl = lex->all_selects_list; sl; sl = sl->next_select_in_list())
      for (TABLE_LIST *cursor = sl->get_table_list(); cursor; cursor = cursor->next_local)
        if (cursor->table && cursor->derived) {
          lex->thd->derived_tables_processing = TRUE;
          cursor->derived->optimize_after_stonedb();
        }
    lex->current_select = save_current_select;
  }
  lex->thd->derived_tables_processing = FALSE;

  return route;
}

/*
Prepares and optimizes a single select for StoneDB engine
*/
int optimize_select(THD *thd, TABLE_LIST *tables, uint wild_num, List<Item> &fields, Item *conds, uint og_num,
                    ORDER *order, ORDER *group, Item *having, ulong select_options, select_result *result,
                    SELECT_LEX_UNIT *unit, SELECT_LEX *select_lex, int &optimize_after_sdb, int &free_join) {
  // copied from sql_select.cpp from the beginning of mysql_select(...)
  int err;
  free_join = 1;

  select_lex->context.resolve_in_select_list = TRUE;
  JOIN *join;
  if (select_lex->join != 0) {
    join = select_lex->join;
    // is it single SELECT in derived table, called in derived table
    // creation
    if (select_lex->linkage != DERIVED_TABLE_TYPE || (select_options & SELECT_DESCRIBE)) {
      if (select_lex->linkage != GLOBAL_OPTIONS_TYPE) {
        // here is EXPLAIN of subselect or derived table
        if (join->change_result(result)) return TRUE;
      } else {
        err = join->prepare(tables, wild_num, conds, og_num, order, group, having, select_lex, unit);
        if (err) {
          return err;
        }
      }
    }
    free_join = 0;
    join->select_options = select_options;
  } else {
    if (!(join = new JOIN(thd, fields, select_options, result))) return TRUE;
    thd_proc_info(thd, "init");
    // thd->used_tables = 0; // Updated by setup_fields... // gone in mysql 5.6
    err = join->prepare(tables, wild_num, conds, og_num, order, group, having, select_lex, unit);
    if (err) {
      return err;
    }
  }
  join->best_rowcount = 2;
  optimize_after_sdb = TRUE;
  if ((err = join->optimize(1))) return err;  // 1
  // until HERE this was the copy of thebeginning of mysql_select(...)
  return FALSE;
}

int handle_exceptions(THD *, Transaction *, bool with_error = false);

int Engine::Execute(THD *thd, LEX *lex, select_result *result_output, SELECT_LEX_UNIT *unit_for_union) {
  DEBUG_ASSERT(thd->lex == lex);
  SELECT_LEX *selects_list = &lex->select_lex;
  SELECT_LEX *last_distinct = NULL;
  if (unit_for_union != NULL) last_distinct = unit_for_union->union_distinct;

  int is_dumpfile = 0;
  const char *export_file_name = GetFilename(selects_list, is_dumpfile);
  if (is_dumpfile) {
    push_warning(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                 "Dumpfile not implemented in StoneDB, executed by MySQL engine.");
    return RETURN_QUERY_TO_MYSQL_ROUTE;
  }

  Query query(current_tx);
  CompiledQuery cqu;

  current_tx->ResetDisplay();  // switch display on
  query.SetRoughQuery(selects_list->options & SELECT_ROUGHLY);

  try {
    if (!query.Compile(&cqu, selects_list, last_distinct)) {
      push_warning(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                   "Query syntax not implemented in StoneDB, executed by MySQL engine.");
      return RETURN_QUERY_TO_MYSQL_ROUTE;
    }
  } catch (common::Exception const &x) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Query Compile Error: %s", x.what());
    my_message(ER_UNKNOWN_ERROR, (std::string("StoneDB compile specific error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  }

  std::unique_ptr<ResultSender> sender;

  FunctionExecutor lock_and_unlock_pack_info(std::bind(&Query::LockPackInfoForUse, &query),
                                             std::bind(&Query::UnlockPackInfoFromUse, &query));

  try {
    std::shared_ptr<RCTable> rct;
    if (lex->sql_command == SQLCOM_INSERT_SELECT &&
        Engine::IsSDBTable(((Query_tables_list *)lex)->query_tables->table)) {
      std::string table_path = Engine::GetTablePath(((Query_tables_list *)lex)->query_tables->table);
      rct = current_tx->GetTableByPathIfExists(table_path);
    }
    if (unit_for_union != NULL) {
      int res = result_output->prepare(unit_for_union->item_list, unit_for_union);
      if (res) {
        STONEDB_LOG(LogCtl_Level::ERROR, "Error: Unsupported UNION");
        my_message(ER_UNKNOWN_ERROR, "StoneDB: unsupported UNION", MYF(0));
        throw ReturnMeToMySQLWithError();
      }
      if (export_file_name)
        sender.reset(new ResultExportSender(unit_for_union->thd, result_output, unit_for_union->item_list));
      else
        sender.reset(new ResultSender(unit_for_union->thd, result_output, unit_for_union->item_list));
    } else {
      if (export_file_name)
        sender.reset(new ResultExportSender(selects_list->join->thd, result_output, selects_list->item_list));
      else
        sender.reset(new ResultSender(selects_list->join->thd, result_output, selects_list->item_list));
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
    return (handle_exceptions(thd, current_tx, with_error));
  }
  return RCBASE_QUERY_ROUTE;
}

int handle_exceptions(THD *thd, Transaction *cur_connection, bool with_error) {
  try {
    std::string msg = "Query terminated with exception: ";
    msg += thd_query_string(thd)->str;
    STONEDB_LOG(LogCtl_Level::INFO, msg);
    throw;
  } catch (common::NotImplementedException const &x) {
    rccontrol.lock(cur_connection->GetThreadID()) << "Switched to MySQL: " << x.what() << system::unlock;
    my_message(ER_UNKNOWN_ERROR,
               (std::string("The query includes syntax that is not supported "
                            "by the storage engine. StoneDB: ") +
                x.what())
                   .c_str(),
               MYF(0));
    if (with_error) {
      std::string msg(x.what());
      msg.append(" Can't switch to MySQL execution path");
      throw common::InternalException(msg);
    }
    return RETURN_QUERY_TO_MYSQL_ROUTE;
  } catch (common::OutOfMemoryException const &x) {
    rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(static_cast<int>(common::ErrorCode::OUT_OF_MEMORY),
               (std::string("StoneDB out of resources error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (common::DataTypeConversionException const &x) {
    rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(static_cast<int>(common::ErrorCode::DATACONVERSION),
               (std::string("StoneDB specific error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (common::DBObjectException const &x) {  // the subselect had more than one row in a comparison
                                                  // without ANY or ALL
    rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(ER_SYNTAX_ERROR, (std::string("StoneDB specific error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (common::KilledException const &) {
    rccontrol.lock(cur_connection->GetThreadID()) << "Stopped by user. " << system::unlock;
    my_message(ER_UNKNOWN_ERROR, (std::string("Stopped by user.")).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (common::FileException const &e) {
    rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << e.what() << system::unlock;
    my_message(static_cast<int>(common::ErrorCode::CANNOT_OPEN_FILE_OR_PIPE),
               (std::string("StoneDB specific error: ") + e.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (common::Exception const &x) {
    rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << system::unlock;
    my_message(ER_UNKNOWN_ERROR, "StoneDB other specific error", MYF(0));
    throw ReturnMeToMySQLWithError();
  } catch (std::bad_alloc const &) {
    rccontrol.lock(cur_connection->GetThreadID()) << "Error: std::bad_alloc caught" << system::unlock;
    my_message(ER_UNKNOWN_ERROR, (std::string("StoneDB out of memory error")).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  }
  return RETURN_QUERY_TO_MYSQL_ROUTE;
}
}  // namespace core
}  // namespace stonedb

int st_select_lex_unit::optimize_for_stonedb() {
  // copied from sql_union.cpp from the beginning of st_select_lex_unit::exec()
  SELECT_LEX *lex_select_save = thd->lex->current_select;
  SELECT_LEX *select_cursor = first_select();

  if (executed && !uncacheable && !describe) return FALSE;
  executed = 1;

  if (uncacheable || !item || !item->assigned() || describe) {
    if (item) item->reset_value_registration();
    if (optimized && item) {
      if (item->assigned()) {
        item->assigned(0);  // We will reinit & rexecute unit
        item->reset();
        table->file->ha_delete_all_rows();
      }
      // re-enabling indexes for next subselect iteration
      if (union_distinct && table->file->ha_enable_indexes(HA_KEY_SWITCH_ALL)) DEBUG_ASSERT(0);
    }
    for (SELECT_LEX *sl = select_cursor; sl; sl = sl->next_select()) {
      thd->lex->current_select = sl;

      if (optimized)
        sl->join->reset();
      else {
        set_limit(sl);
        if (sl == global_parameters || describe) {
          offset_limit_cnt = 0;
          // We can't use LIMIT at this stage if we are using ORDER BY for the
          // whole query
          if (sl->order_list.first || describe) select_limit_cnt = HA_POS_ERROR;
        }

        // When using braces, SQL_CALC_FOUND_ROWS affects the whole query:
        // we don't calculate found_rows() per union part.
        // Otherwise, SQL_CALC_FOUND_ROWS should be done on all sub parts.
        sl->join->select_options = (select_limit_cnt == HA_POS_ERROR || sl->braces)
                                       ? sl->options & ~OPTION_FOUND_ROWS
                                       : sl->options | found_rows_for_union;
        saved_error = sl->join->optimize(1);
      }

      // HERE ends the code from bool st_select_lex_unit::exec()
      if (saved_error) {
        thd->lex->current_select = lex_select_save;
        return saved_error;
      }
    }
  }
  /* code from st_select_lex_unit::exec*/
  if (!saved_error && !thd->is_fatal_error) {
    /* Send result to 'result' */
    saved_error = true;
    set_limit(global_parameters);
    if (init_prepare_fake_select_lex(thd, true)) return saved_error;
    JOIN *join = fake_select_lex->join;
    if (!join->optimized) {
      saved_error = join->prepare(fake_select_lex->table_list.first, 0, 0, global_parameters->order_list.elements,
                                  global_parameters->order_list.first, NULL, NULL, fake_select_lex, this);
    } else {
      join->examined_rows = 0;
      join->reset();
    }

    fake_select_lex->table_list.empty();
  }

  optimized = 1;
  thd->lex->current_select = lex_select_save;
  return FALSE;
}

int st_select_lex_unit::optimize_after_stonedb() {
  SELECT_LEX *lex_select_save = thd->lex->current_select;
  for (SELECT_LEX *sl = first_select(); sl; sl = sl->next_select()) {
    thd->lex->current_select = sl;
    int res = sl->join->optimize(2);
    if (res) {
      thd->lex->current_select = lex_select_save;
      return res;
    }
  }
  if (fake_select_lex && fake_select_lex->join) {
    // fake_select_lex->join must be cleaned up before returning to
    // MySQL route, otherwise sub select + union would coredump.
    thd->lex->current_select = fake_select_lex;
    fake_select_lex->cleanup();
  }
  executed = 0;
  thd->lex->current_select = lex_select_save;
  return FALSE;
}
