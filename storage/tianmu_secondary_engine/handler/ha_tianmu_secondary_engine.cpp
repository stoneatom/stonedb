/* Copyright (c) 2018, 2022, Oracle and/or its affiliates.

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

#include "../handler/ha_tianmu_secondary_engine.h"

#include <stddef.h>

#include <algorithm>
#include <cassert>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../imcs/common/common_def.h"
#include "../imcs/imcs.h"
#include "../imcs/imcs_base.h"
#include "current_thd.h"
#include "ha_tianmu_secondary_engine.h"
#include "lex_string.h"
#include "log.h"  // adaptor for "trx0sys.h"
#include "my_alloc.h"
#include "my_compiler.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_sys.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "sess0sess.h"
#include "sql/dd/types/abstract_table.h"
#include "sql/debug_sync.h"
#include "sql/handler.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/join_optimizer/make_join_hypergraph.h"
#include "sql/join_optimizer/walk_access_paths.h"
#include "sql/log.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_const.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_table.h"
#include "sql/table.h"
#include "template_utils.h"
#include "thr_lock.h"
#include "trx0sys.h"
#include "../imcs/buffer/populate.h"

namespace dd {
class Table;
}

extern trx_sys_t *trx_sys;

struct log_t;

LoadedTables *loaded_tables{nullptr};

namespace {

/**
  Execution context class for the Tianmu seocondary engine. It allocates some
  data on the heap when it is constructed, and frees it when it is destructed,
  so that LeakSanitizer and Valgrind can detect if the server doesn't destroy
  the object when the query execution has completed.
*/
class Tianmu_secondary_engine_execution_context
    : public Secondary_engine_execution_context {
 public:
  Tianmu_secondary_engine_execution_context()
      : m_data(std::make_unique<char[]>(10)) {}
  /**
    Checks if the specified cost is the lowest cost seen so far for executing
    the given JOIN.
  */
  bool BestPlanSoFar(const JOIN &join, double cost) {
    if (&join != m_current_join) {
      // No plan has been seen for this join. The current one is best so far.
      m_current_join = &join;
      m_best_cost = cost;
      return true;
    }

    // Check if the current plan is the best seen so far.
    const bool cheaper = cost < m_best_cost;
    m_best_cost = std::min(m_best_cost, cost);
    return cheaper;
  }

 private:
  std::unique_ptr<char[]> m_data;
  /// The JOIN currently being optimized.
  const JOIN *m_current_join{nullptr};
  /// The cost of the best plan seen so far for the current JOIN.
  double m_best_cost;
};

}  // namespace

/******************************************************************
 * Start namespace of Tianmu_secondary_engine
 *******************************************************************/
namespace Tianmu_secondary_engine {

ha_tianmu_secondary::ha_tianmu_secondary(handlerton *hton,
                                         TABLE_SHARE *table_share_arg)
    : handler(hton, table_share_arg),
      primary_inited_(false),
      ent_type_(ENGINE_TYPE::ET_IN_MEMORY),
      cond_(nullptr) {}

int ha_tianmu_secondary::create(const char *, TABLE *, HA_CREATE_INFO *,
                                dd::Table *) {
  // We do not use 'create' command directly.
  assert(false);
  return HA_ERR_WRONG_COMMAND;
}

int ha_tianmu_secondary::open(const char *, int, unsigned int,
                              const dd::Table *) {
  Tianmu::IMCS::TianmuSecondaryShare *share =
      loaded_tables->get(table_share->db.str, table_share->table_name.str);
  if (share == nullptr) {
    // The table has not been loaded into the secondary storage engine yet.
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "Table has not been loaded");
    return HA_ERR_GENERIC;
  }
  thr_lock_data_init(&share->lock, &m_lock, nullptr);
  return 0;
}

int ha_tianmu_secondary::close() {
  Tianmu::IMCS::TianmuSecondaryShare *share =
      loaded_tables->get(table_share->db.str, table_share->table_name.str);
  if (share == nullptr) {
    // The table has not been loaded into the secondary storage engine yet.
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "Table has not been loaded");
    return HA_ERR_GENERIC;
  }
  return 0;
}

trx_t *&thd_to_trx(THD *thd) {
  LEX_CSTRING innodb_name = {innobase_hton_name, strlen(innobase_hton_name)};
  plugin_ref plugin = ha_resolve_by_name(thd, &innodb_name, false);
  handlerton *innodb_hton_ptr = plugin_data<handlerton *>(plugin);
  innodb_session_t *&innodb_session =
      *(innodb_session_t **)thd_ha_data(thd, innodb_hton_ptr);
  ut_ad(innodb_session != nullptr);

  return (innodb_session->m_trx);
}

int ha_tianmu_secondary::rnd_init(bool) {
  trx_t *trx = thd_to_trx(current_thd);
  trx_sys_mutex_enter();
  ReadView *read_view =
      trx_sys->mvcc->get_view_created_by_trx_id_rapid(trx->id);
  trx_sys_mutex_exit();
  if (read_view == nullptr) {
    trx_sys->mvcc->view_open(read_view, trx);
    read_view_ = read_view;
  }
  cur_row_id_ = 0;
  return 0;
}

int ha_tianmu_secondary::rnd_next(uchar *buf) {
  int ret = RET_SUCCESS;

  // compute offset for every field
  uint32 field_count = table->s->fields;
  Tianmu::IMCS::TianmuSecondaryShare *share =
      loaded_tables->get(table->s->db.str, table->s->table_name.str);
  if (share == nullptr) {
    // handle error
  }
  Tianmu::IMCS::Imcs_context context(filter_set_, share, buf, &cur_row_id_,
                                     cond_, buf, read_view_);
  // clear null list
  memset(buf, 0, table->s->null_bytes);
  for (uint32 i = 0; i < field_count; ++i) {
    Field *field = table->field[i];
    // Skip columns marked as NOT SECONDARY.
    if (field->is_flag_set(NOT_SECONDARY_FLAG)) continue;
    uint32 offset = field->offset(table->record[0]);

    auto it = share->field_to_col_idx_.find(field->field_name);
    assert(it != share->field_to_col_idx_.end());

    context.cid_to_offset.insert({it->second, offset});
  }

  Tianmu::IMCS::Imcs *imcs =
      Tianmu::IMCS::IMCS_Instance::GetInstance("Tianmu_rapid");
  uint64 found_idx;
  if (RET_FAIL(imcs->scan(context))) {
    if (ret == RET_NOT_FOUND) {
      return HA_ERR_END_OF_FILE;
    }
    // handle error
  }
  return 0;
}

int ha_tianmu_secondary::rnd_end() {
  if (read_view_ != nullptr) {
    trx_sys_mutex_enter();
    trx_sys->mvcc->view_close(read_view_, true);
    trx_sys_mutex_exit();
  }
}

const Item *ha_tianmu_secondary::cond_push(const Item *cond) {
  Tianmu::IMCS::TianmuSecondaryShare *share =
      loaded_tables->get(table_share->db.str, table_share->table_name.str);
  Item *item = const_cast<Item *>(cond);
  cond_ = item;
  filter_set_ = std::move(Tianmu::IMCS::create_filter_set(share, item));
  return nullptr;
}

int ha_tianmu_secondary::info(unsigned int flags) {
  // Get the cardinality statistics from the primary storage engine.
  handler *primary = ha_get_primary_handler();
  int ret = primary->info(flags);
  if (ret == 0) {
    stats.records = primary->stats.records;
  }
  return ret;
}

handler::Table_flags ha_tianmu_secondary::table_flags() const {
  // Secondary engines do not support index access. Indexes are only used for
  // cost estimates.
  return HA_NO_INDEX_ACCESS;
}

unsigned long ha_tianmu_secondary::index_flags(unsigned int idx,
                                               unsigned int part,
                                               bool all_parts) const {
  const handler *primary = ha_get_primary_handler();
  const unsigned long primary_flags =
      primary == nullptr ? 0 : primary->index_flags(idx, part, all_parts);

  // Inherit the following index flags from the primary handler, if they are
  // set:
  //
  // HA_READ_RANGE - to signal that ranges can be read from the index, so that
  // the optimizer can use the index to estimate the number of rows in a range.
  //
  // HA_KEY_SCAN_NOT_ROR - to signal if the index returns records in rowid
  // order. Used to disable use of the index in the range optimizer if it is not
  // in rowid order.
  return ((HA_READ_RANGE | HA_KEY_SCAN_NOT_ROR) & primary_flags);
}

ha_rows ha_tianmu_secondary::records_in_range(unsigned int index,
                                              key_range *min_key,
                                              key_range *max_key) {
  // Get the number of records in the range from the primary storage engine.
  return ha_get_primary_handler()->records_in_range(index, min_key, max_key);
}

THR_LOCK_DATA **ha_tianmu_secondary::store_lock(THD *, THR_LOCK_DATA **to,
                                                thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE && m_lock.type == TL_UNLOCK)
    m_lock.type = lock_type;
  *to++ = &m_lock;
  return to;
}

int ha_tianmu_secondary::load_table(THD *thd, const TABLE &table_arg) {
  assert(table_arg.file != nullptr);

  int ret = RET_SUCCESS;

  // the table has already loaded into icms. Here, some new error code and error
  // message should be added. [ER_LOAD_INFO, HA_ERR_GENERIC] must be replace
  // with specific error code and error message.
  if (loaded_tables->get(table_arg.s->db.str, table_arg.s->table_name.str) !=
      nullptr) {
    my_error(ER_SECONDARY_ENGINE_LOAD, MYF(0), table_arg.s->db.str,
             table_arg.s->table_name.str);
    return HA_ERR_GENERIC;
  }

  // check if primary key is missing
  if (table_arg.s->is_missing_primary_key()) {
    my_error(ER_REQUIRES_PRIMARY_KEY, MYF(0));
    return HA_ERR_GENERIC;
  }

  KEY *key = table_arg.key_info + table_arg.s->primary_key;

  if (key->actual_key_parts != 1) {
    my_error(ER_RAPID_DA_ONLY_SUPPORT_SINGLE_PRIMARY_KEY, MYF(0),
             table_arg.s->db.str, table_arg.s->table_name.str,
             key->actual_key_parts);
    return HA_ERR_GENERIC;
  }

  lsn_t flush_to_disk_lsn =
      log_sys->flushed_to_disk_lsn.load(std::memory_order_relaxed);

  // Scan the primary table and read the records.
  if (table_arg.file->inited == NONE && table_arg.file->ha_rnd_init(true)) {
    my_error(ER_NO_SUCH_TABLE, MYF(0), table_arg.s->db.str,
             table_arg.s->table_name.str);
    return HA_ERR_GENERIC;
  }

  const char *primary_key_name = key->key_part->field->field_name;

  Tianmu::IMCS::TianmuSecondaryShare *share =
      new Tianmu::IMCS::TianmuSecondaryShare(table_arg);

  int tmp;
  uchar *record = table_arg.record[0];
  while ((tmp = table_arg.file->ha_rnd_next(record)) != HA_ERR_END_OF_FILE) {
    /*
      ha_rnd_next can return RECORD_DELETED for MyISAM when one thread is
      reading and another deleting without locks. Now, do full scan, but
      multi-thread scan will impl in future.
    */
    if (tmp == HA_ERR_KEY_NOT_FOUND) break;

    std::vector<Field *> field_lst;
    uint32 field_count = table_arg.s->fields;
    Field *field_ptr = nullptr;
    uint32 primary_key_idx = field_count;
    for (uint32 index = 0; index < field_count; index++) {
      field_ptr = *(table_arg.field + index);

      // Skip columns marked as NOT SECONDARY. │
      if ((field_ptr)->is_flag_set(NOT_SECONDARY_FLAG)) continue;

      field_lst.push_back(field_ptr);

      if (field_ptr->is_null())
        ;

      const char *field_name = field_ptr->field_name;

      if (!strcmp(field_name, primary_key_name)) {
        primary_key_idx = (int)field_lst.size() - 1;
      }

      char buff[MAX_FIELD_WIDTH] = {0};
      String str(buff, sizeof(buff), &my_charset_bin);
#ifndef NDEBUG
      my_bitmap_map *old_map = 0;
      TABLE *table = const_cast<TABLE *>(&table_arg);
      if (table && table->file)
        old_map = dbug_tmp_use_all_columns(table, table->read_set);
#endif
        // Here, we get the col data in string format. And, here, do insertion
        // to IMCS.
        // String *res = field_ptr->val_str(&str);

#ifndef NDEBUG
      if (old_map) dbug_tmp_restore_column_map(table->read_set, old_map);
#endif
      ha_statistic_increment(&System_status_var::ha_read_rnd_count);
    }

    // get trx id
    field_ptr = *(table_arg.field + field_count);
    assert(field_ptr->type() == MYSQL_SYS_TYPE_TRX_ID);
    uint64_t trx_id = field_ptr->val_int();

    // denote primary key has NOT_SECONDARY flag
    if (primary_key_idx == field_count) {
      my_error(ER_RAPID_DA_PRIMARY_KEY_CAN_NOT_HAVE_NOT_SECONDARY_FLAG, MYF(0),
               table_arg.s->db.str, table_arg.s->table_name.str);
      table_arg.file->ha_rnd_end();
      return HA_ERR_GENERIC;
    }

    Tianmu::IMCS::Imcs *imcs =
        Tianmu::IMCS::IMCS_Instance::GetInstance("Tianmu_rapid");

    uint64 row_id;
    if (RET_FAIL(
            imcs->insert(share, field_lst, primary_key_idx, &row_id, trx_id))) {
      my_error(ER_RAPID_DA_INSERT_FAILED, MYF(0));
      table_arg.file->ha_rnd_end();
      return HA_ERR_GENERIC;
    }

    /*
      stores all the records in innodb table to imcs. Here, to open icms table
      share and write the decoded record into imcs table.
    */
    if (tmp == HA_ERR_RECORD_DELETED && !thd->killed) continue;
  }

  table_arg.file->ha_rnd_end();

  // wait rapid_lsn to flush_to_disk_lsn
  if (log_sys->rapid_lsn.load() < flush_to_disk_lsn) {
    size_t slot = Tianmu::IMCS::compute_rapid_event_slot(flush_to_disk_lsn);
    int64_t sig_count = os_event_reset(log_sys->rapid_events[slot]);
    os_event_wait_time_low(log_sys->rapid_events[slot],
                           std::chrono::microseconds::max(), sig_count);
  }

  loaded_tables->add(table_arg.s->db.str, table_arg.s->table_name.str, share);

  return 0;
}

int ha_tianmu_secondary::unload_table(THD *thd [[maybe_unused]],
                                      const char *db_name,
                                      const char *table_name,
                                      bool error_if_not_loaded) {
  if (error_if_not_loaded &&
      loaded_tables->get(db_name, table_name) == nullptr) {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "Table is not loaded on a secondary engine");
    return 1;
  } else {
    auto share = loaded_tables->get(db_name, table_name);
    if (share == nullptr) {
      my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
               "Table is not loaded on a secondary engine");
      return 1;
    }
    Tianmu::IMCS::Imcs *imcs =
        Tianmu::IMCS::IMCS_Instance::GetInstance("Tianmu_rapid");
    for (auto imcu_idx : share->imcu_idx_list_) {
      auto imcu = imcs->getIMCU(imcu_idx);
      if (imcu == nullptr) {
        continue;
      }
      delete imcu->arena();
      imcs->unloadIMCU(imcu_idx);
    }
    meta_rpd_columns.erase(
        std::remove_if(meta_rpd_columns.begin(), meta_rpd_columns.end(),
                       [&table_name](const rpd_columns_info &rpd_column) {
                         return strcmp(rpd_column.table_name, table_name) == 0;
                       }),
        meta_rpd_columns.end());
    loaded_tables->erase(db_name, table_name);
    return 0;
  }
}

int ha_tianmu_secondary::rnd_pos(unsigned char *buffer, unsigned char *pos) {
  return HA_ERR_WRONG_COMMAND;
}

}  // namespace Tianmu_secondary_engine

// flag of whether tianmu inited or not.
static bool tianmu_rapid_inited = false;
static struct handlerton *tianmu_rapid_hton_ptr = nullptr;

static bool PrepareSecondaryEngine(THD *thd, LEX *lex) {
  DBUG_EXECUTE_IF("tianmu_secondary_engine_prepare_error", {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "");
    return true;
  });

  auto context = new (thd->mem_root) Tianmu_secondary_engine_execution_context;
  if (context == nullptr) return true;
  lex->set_secondary_engine_execution_context(context);

  // Disable use of constant tables and evaluation of subqueries during
  // optimization.
  lex->add_statement_options(OPTION_NO_CONST_TABLES |
                             OPTION_NO_SUBQUERY_DURING_OPTIMIZATION);

  return false;
}

static void AssertSupportedPath(const AccessPath *path) {
  switch (path->type) {
    // The only supported join type is hash join. Other join types are disabled
    // in handlerton::secondary_engine_flags.
    case AccessPath::NESTED_LOOP_JOIN: /* purecov: deadcode */
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
    case AccessPath::BKA_JOIN:
    // Index access is disabled in ha_tianmu_secondary::table_flags(), so we
    // should see none of these access types.
    case AccessPath::INDEX_SCAN:
    case AccessPath::REF:
    case AccessPath::REF_OR_NULL:
    case AccessPath::EQ_REF:
    case AccessPath::PUSHED_JOIN_REF:
    case AccessPath::INDEX_RANGE_SCAN:
    case AccessPath::INDEX_SKIP_SCAN:
    case AccessPath::GROUP_INDEX_SKIP_SCAN:
    case AccessPath::ROWID_INTERSECTION:
    case AccessPath::ROWID_UNION:
    case AccessPath::DYNAMIC_INDEX_RANGE_SCAN:
      assert(false); /* purecov: deadcode */
      break;
    default:
      break;
  }

  // This secondary storage engine does not yet store anything in the auxiliary
  // data member of AccessPath.
  assert(path->secondary_engine_data == nullptr);
}

static bool OptimizeSecondaryEngine(THD *thd [[maybe_unused]], LEX *lex) {
  // The context should have been set by PrepareSecondaryEngine.
  assert(lex->secondary_engine_execution_context() != nullptr);

  DBUG_EXECUTE_IF("tianmu_secondary_engine_optimize_error", {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "");
    return true;
  });

  DEBUG_SYNC(thd, "before_tianmu_secondary_engine_optimize");

  if (lex->using_hypergraph_optimizer) {
    WalkAccessPaths(lex->unit->root_access_path(), nullptr,
                    WalkAccessPathPolicy::ENTIRE_TREE,
                    [](AccessPath *path, const JOIN *) {
                      AssertSupportedPath(path);
                      return false;
                    });
  }

  return false;
}

static bool CompareJoinCost(THD *thd, const JOIN &join, double optimizer_cost,
                            bool *use_best_so_far, bool *cheaper,
                            double *secondary_engine_cost) {
  *use_best_so_far = false;

  DBUG_EXECUTE_IF("tianmu_secondary_engine_compare_cost_error", {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "");
    return true;
  });

  DBUG_EXECUTE_IF("tianmu_secondary_engine_choose_first_plan", {
    *use_best_so_far = true;
    *cheaper = true;
    *secondary_engine_cost = optimizer_cost;
  });

  // Just use the cost calculated by the optimizer by default.
  *secondary_engine_cost = optimizer_cost;

  // This debug flag makes the cost function prefer orders where a table with
  // the alias "X" is closer to the beginning.
  DBUG_EXECUTE_IF("tianmu_secondary_engine_change_join_order", {
    double cost = join.tables;
    for (size_t i = 0; i < join.tables; ++i) {
      const TABLE_LIST *ref = join.positions[i].table->table_ref;
      if (std::string(ref->alias) == "X") {
        cost += i;
      }
    }
    *secondary_engine_cost = cost;
  });

  // Check if the calculated cost is cheaper than the best cost seen so far.
  *cheaper = down_cast<Tianmu_secondary_engine_execution_context *>(
                 thd->lex->secondary_engine_execution_context())
                 ->BestPlanSoFar(join, *secondary_engine_cost);

  return false;
}

/** Implements the SHOW ENGINE TIANMU RAPID STATUS command. Sends the output of
the Tianmu Rapid Monitor to the client.
@param[in]      hton            the tianmu handlerton
@param[in]      thd             the MySQL query thread of the caller
@param[in]      stat_print      print function
@return 0 on success */
static int tianmu_rapid_show_status(handlerton *hton, THD *thd,
                                    stat_print_fn *stat_print) {
  static const char truncated_msg[] = "... truncated...\n";
  const long MAX_STATUS_SIZE = 1048576;

  bool ret_val;

  assert(hton == tianmu_rapid_hton_ptr);

  // trx_t *trx = check_trx_exists(thd);

  return ret_val;
}

/** Implements the SHOW MUTEX STATUS command.
@param[in,out]  hton            the tianmu handlerton
@param[in,out]  thd             the MySQL query thread of the caller
@param[in,out]  stat_print      function for printing statistics
@return 0 on success. */
static int tianmu_rapid_show_latch_status(handlerton *hton, THD *thd,
                                          stat_print_fn *stat_print) {
#if 0
  int ret = innodb_show_mutex_status(hton, thd, stat_print);

  if (ret != 0) {
    return (ret);
  }

  return (innodb_show_rwlock_status(hton, thd, stat_print));
#endif
  return 0;
}

/** Return 0 on success and non-zero on failure. Note: the bool return type
seems to be abused here, should be an int.
@param[in]      hton            the innodb handlerton
@param[in]      thd             the MySQL query thread of the caller
@param[in]      stat_print      print function
@param[in]      stat_type       status to show */
static bool tianmu_base_rapid_show_status(handlerton *hton, THD *thd,
                                          stat_print_fn *stat_print,
                                          enum ha_stat_type stat_type) {
  assert(hton == tianmu_rapid_hton_ptr);

  switch (stat_type) {
    case HA_ENGINE_STATUS:
      /* Non-zero return value means there was an error. */
      return (tianmu_rapid_show_status(hton, thd, stat_print) != 0);

    case HA_ENGINE_MUTEX:
      return (tianmu_rapid_show_latch_status(hton, thd, stat_print) != 0);

    case HA_ENGINE_LOGS:
      /* Not handled */
      break;
  }

  /* Success */
  return (false);
}

static bool ModifyAccessPathCost(THD *thd [[maybe_unused]],
                                 const JoinHypergraph &hypergraph
                                 [[maybe_unused]],
                                 AccessPath *path) {
  assert(!thd->is_error());
  assert(hypergraph.query_block()->join == hypergraph.join());
  AssertSupportedPath(path);
  return false;
}

static handler *Tianmu_Rapid_Create(handlerton *hton, TABLE_SHARE *table_share,
                                    bool, MEM_ROOT *mem_root) {
  return new (mem_root)
      Tianmu_secondary_engine::ha_tianmu_secondary(hton, table_share);
}

static int Tianmu_rapid_init(MYSQL_PLUGIN p) {
  loaded_tables = new LoadedTables();

  handlerton *tianmu_rapid_hton = static_cast<handlerton *>(p);
  tianmu_rapid_hton_ptr = tianmu_rapid_hton;

  tianmu_rapid_hton->create = Tianmu_Rapid_Create;
  tianmu_rapid_hton->state = SHOW_OPTION_YES;
  tianmu_rapid_hton->flags = HTON_IS_SECONDARY_ENGINE;
  tianmu_rapid_hton->db_type = DB_TYPE_UNKNOWN;
  tianmu_rapid_hton->prepare_secondary_engine = PrepareSecondaryEngine;
  tianmu_rapid_hton->optimize_secondary_engine = OptimizeSecondaryEngine;
  tianmu_rapid_hton->compare_secondary_engine_cost = CompareJoinCost;
  tianmu_rapid_hton->show_status = tianmu_base_rapid_show_status;
  tianmu_rapid_hton->secondary_engine_flags =
      MakeSecondaryEngineFlags(SecondaryEngineFlag::SUPPORTS_HASH_JOIN);
  tianmu_rapid_hton->secondary_engine_modify_access_path_cost =
      ModifyAccessPathCost;

  assert(tianmu_rapid_inited == false);
  tianmu_rapid_inited = !tianmu_rapid_inited;

  Tianmu::IMCS::Imcs *imcs_ =
      Tianmu::IMCS::IMCS_Instance::GetInstance("Tianmu_rapid");
  if (!imcs_) {
    log_errlog(ERROR_LEVEL, ER_SECONDARY_ENGINE, "Tianmu_rapid engien failed.");
    return HA_ERR_INITIALIZATION;
  }
  if (tianmu_rapid_hton && imcs_) tianmu_rapid_hton->data = imcs_;

  Tianmu::IMCS::start_backgroud_threads(*log_sys);

  return 0;
}

static int Tianmu_rapid_deinit(MYSQL_PLUGIN) {
  delete loaded_tables;
  loaded_tables = nullptr;

  assert(tianmu_rapid_inited == true);
  tianmu_rapid_inited = !tianmu_rapid_inited;
  return 0;
}

// FOR tianmu_rapid
/*********************************************************************
 *  Start to define the sys var for tianmu rapid.
 *
 *********************************************************************/
/// extern unsigned long Tianmu::IMCS::srv_rapid_memory_size;
/// extern unsigned long Tianmu::IMCS::srv_rapid_populate_buffer_size;

static SHOW_VAR tianmu_rapid_status_variables[] = {
    {"srv_rapid_memory_size",
     (char *)&Tianmu::IMCS::export_vars.srv_memory_size, SHOW_CHAR,
     SHOW_SCOPE_GLOBAL},
    {"srv_rapid_populate_buffer_size",
     (char *)&Tianmu::IMCS::export_vars.srv_populate_buffer_size, SHOW_CHAR,
     SHOW_SCOPE_GLOBAL},
    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

/**
 Update the system variable innodb_io_capacity_max using the "saved"
 value. This function is registered as a callback with MySQL. */
static void rapid_populate_buffer_size_update(
    /*===========================*/
    THD *thd,                       /*!< in: thread handle */
    SYS_VAR *var [[maybe_unused]],  /*!< in: pointer to system variable */
    void *var_ptr [[maybe_unused]], /*!< out: where the formal string goes */
    const void *save) /*!< in: immediate result from check function */
{
  ulong in_val = *static_cast<const ulong *>(save);

  if (in_val < Tianmu::IMCS::srv_rapid_populate_buffer_size) {
    in_val = Tianmu::IMCS::srv_rapid_populate_buffer_size;
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                        "srv_rapid_populate_buffer_size cannot be"
                        " set more than srv_rapid_memory_size.");
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                        "Setting srv_rapid_populate_buffer_size to %lu",
                        Tianmu::IMCS::srv_rapid_populate_buffer_size);
  }

  Tianmu::IMCS::srv_rapid_populate_buffer_size = in_val;
}

/**
 Update the system variable innodb_io_capacity_max using the "saved"
 value. This function is registered as a callback with MySQL. */
static void rapid_memory_size_update(
    /*===========================*/
    THD *thd,                       /*!< in: thread handle */
    SYS_VAR *var [[maybe_unused]],  /*!< in: pointer to system variable */
    void *var_ptr [[maybe_unused]], /*!< out: where the formal string goes */
    const void *save) /*!< in: immediate result from check function */
{
  ulong in_val = *static_cast<const ulong *>(save);

  if (in_val < Tianmu::IMCS::srv_rapid_memory_size) {
    in_val = Tianmu::IMCS::srv_rapid_memory_size;
    push_warning_printf(
        thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
        "srv_rapid_memory_size cannot be set more than srv buffer.");
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                        "Setting srv_rapid_memory_size to %lu",
                        Tianmu::IMCS::srv_rapid_memory_size);
  }

  Tianmu::IMCS::srv_rapid_populate_buffer_size = in_val;
}

/** Here we export Tianmu status variables to MySQL. */
static void tianmu_rapid_export_status() {
  if (Tianmu::IMCS::IMCS_Instance::GetInstance("IMCS")->initalized()) {
    Tianmu::IMCS::IMCS_Instance::GetInstance("IMCS")
        ->srv_export_tianmu_rapid_status();
  }
}

/** Callback function for accessing the InnoDB variables from MySQL:
 SHOW VARIABLES. */
static int show_tianmu_rapid_vars(THD *, SHOW_VAR *var, char *) {
  tianmu_rapid_export_status();
  var->type = SHOW_ARRAY;
  var->value = (char *)&tianmu_rapid_status_variables;
  var->scope = SHOW_SCOPE_GLOBAL;

  return (0);
}

static SHOW_VAR rapid_staus_variables_export[] = {
    {"Tianmu_rapid", (char *)&show_tianmu_rapid_vars, SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},
    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

static st_mysql_storage_engine tianmu_storage_engine{
    MYSQL_HANDLERTON_INTERFACE_VERSION};

static MYSQL_SYSVAR_ULONG(
    rapid_memory_size, Tianmu::IMCS::srv_rapid_memory_size,
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
    "Number of memory size that used for Tianmu rapid engine, and it must "
    "not be oversize physical mem size.",
    nullptr, rapid_memory_size_update,
    Tianmu::IMCS::DEFUALT_RAPID_SVR_MEMORY_SIZE, 0,
    Tianmu::IMCS::MAX_RAPID_SVR_MEMORY_SIZE, 0);

static MYSQL_SYSVAR_ULONG(rapid_populate_buffer_size,
                          Tianmu::IMCS::srv_rapid_populate_buffer_size,
                          PLUGIN_VAR_RQCMDARG,
                          "Number of populate buffer size that must not be 10% "
                          "rapid_populate_buffer size.",
                          NULL, rapid_populate_buffer_size_update, 10, 0, 64,
                          0);

static struct SYS_VAR *tianmu_rapid_system_variables[] = {
    MYSQL_SYSVAR(rapid_memory_size),
    MYSQL_SYSVAR(rapid_populate_buffer_size),
    nullptr,
};

mysql_declare_plugin(tianmu_rapdi_engine){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &tianmu_storage_engine,
    "TIANMU_RAPID",
    PLUGIN_AUTHOR_STONEATOM,
    "Tianmu Secondary Storage Engine",
    PLUGIN_LICENSE_GPL,
    Tianmu_rapid_init,             /* Plugin Init */
    nullptr,                       /* Plugin Check uninstall */
    Tianmu_rapid_deinit,           /* Plugin Deinit */
    0x0001,                        /* tianmu rapid version 0.1 */
    rapid_staus_variables_export,  /* status variables  */
    tianmu_rapid_system_variables, /* system variables  */
    nullptr,                       /* config options    */
    0,                             /* flags for plugin */
} mysql_declare_plugin_end;
