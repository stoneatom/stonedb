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
#ifndef STONEDB_CORE_ENGINE_H_
#define STONEDB_CORE_ENGINE_H_
#pragma once

#include <condition_variable>
#include <list>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "common/assert.h"
#include "common/exception.h"
#include "core/data_cache.h"
#include "core/object_cache.h"
#include "core/query.h"
#include "core/rc_table.h"
#include "core/table_share.h"
#include "core/temp_table.h"
#include "exporter/data_exporter.h"
#include "exporter/export2file.h"
#include "index/rc_table_index.h"
#include "system/io_parameters.h"
#include "system/rc_system.h"
#include "util/fs.h"
#include "util/mapped_circular_buffer.h"
#include "util/thread_pool.h"

extern handlerton *rcbase_hton;
class Field;

namespace stonedb {
namespace types {
class RCDataType;
}  // namespace types
namespace system {
class ResourceManager;
}  // namespace system

namespace index {
class RCTableIndex;
class KVStore;
}  // namespace index

namespace exporter {
class select_sdb_export;
}  // namespace exporter

namespace core {
struct AttrInfo;
class TableShare;
class Transaction;
class RSIndex;
class TaskExecutor;
class RCMemTable;

class Engine final {
 public:
  Engine();
  ~Engine();

  int Init(uint engine_slot);
  void CreateTable(const std::string &table, TABLE *from);
  void DeleteTable(const char *table, THD *thd);
  void TruncateTable(const std::string &table_path, THD *thd);
  void RenameTable(Transaction *trans, const std::string &from, const std::string &to, THD *thd);
  void PrepareAlterTable(const std::string &table_path, std::vector<Field *> &new_cols, std::vector<Field *> &old_cols,
                         THD *thd);

  uint32_t GetNextTableId();

  unsigned long GetIPM() const { return IPM; }
  unsigned long GetIT() const { return IT; }
  unsigned long GetQT() const { return QT; }
  unsigned long GetQPM() const { return QPM; }
  unsigned long GetLPM() const { return LPM; }
  unsigned long GetLT() const { return LT; }
  unsigned long GetLDPM() const { return LDPM; }
  unsigned long GetLDT() const { return LDT; }
  unsigned long GetUPM() const { return UPM; }
  unsigned long GetUT() const { return UT; }
  void IncStonedbStatUpdate() { ++stonedb_stat.update; }
  std::vector<AttrInfo> GetTableAttributesInfo(const std::string &table_path, TABLE_SHARE *table_share);
  void UpdateAndStoreColumnComment(TABLE *table, int field_id, Field *source_field, int source_field_id,
                                   CHARSET_INFO *cs);
  void GetTableIterator(const std::string &table_path, RCTable::Iterator &iter_begin, RCTable::Iterator &iter_end,
                        std::shared_ptr<RCTable> &table, const std::vector<bool> &, THD *thd);
  common::SDBError RunLoader(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg);
  void CommitTx(THD *thd, bool all);
  void Rollback(THD *thd, bool all, bool force_error_message = false);
  Transaction *CreateTx(THD *thd);
  Transaction *GetTx(THD *thd);
  void ClearTx(THD *thd);
  int HandleSelect(THD *thd, LEX *lex, select_result *&result_output, ulong setup_tables_done_option, int &res,
                   int &optimize_after_sdb, int &sdb_free_join, int with_insert = false);
  system::ResourceManager *getResourceManager() const { return m_resourceManager; }
  std::shared_ptr<RCTable> GetTableRD(const std::string &table_path);
  int InsertRow(const std::string &tablename, Transaction *trans, TABLE *table, std::shared_ptr<TableShare> &share);
  void InsertDelayed(const std::string &table_path, int tid, TABLE *table);
  void InsertMemRow(const std::string &table_path, std::shared_ptr<TableShare> &share, TABLE *table);
  std::string DelayedBufferStat() { return insert_buffer.Status(); }
  std::string RowStoreStat();
  void UnRegisterTable(const std::string &table_path);
  std::shared_ptr<TableShare> GetTableShare(const TABLE_SHARE *table_share);
  common::TX_ID MinXID() const { return min_xid; }
  common::TX_ID MaxXID() const { return max_xid; }
  void DeferRemove(const fs::path &file, int32_t cookie);
  void HandleDeferredJobs();
  // support for primary key
  void AddTableIndex(const std::string &table_path, TABLE *table, THD *thd);
  std::shared_ptr<index::RCTableIndex> GetTableIndex(const std::string &table_path);
  bool has_pk(TABLE *table) const { return table->s->primary_key != MAX_INDEXES; }
  void RenameRdbTable(const std::string &from, const std::string &to);
  void DropSignal() { cv_drop.notify_one(); }
  void ResetTaskExecutor(int percent);
  TaskExecutor *GetTaskExecutor() const { return task_executor.get(); }
  void AddMemTable(TABLE *form, std::shared_ptr<TableShare> share);
  void UnregisterMemTable(const std::string &from, const std::string &to);

 public:
  utils::thread_pool delay_insert_thread_pool;
  utils::thread_pool load_thread_pool;
  utils::thread_pool query_thread_pool;
  DataCache cache;
  ObjectCache<FilterCoordinate, RSIndex, FilterCoordinate> filter_cache;

 public:
  static common::CT GetCorrespondingType(const Field &field);
  static AttributeTypeInfo GetCorrespondingATI(Field &field);
  static AttributeTypeInfo GetAttrTypeInfo(const Field &field);
  static common::CT GetCorrespondingType(const enum_field_types &eft);
  static bool IsSDBTable(TABLE *table);
  static bool ConvertToField(Field *field, types::RCDataType &rcitem, std::vector<uchar> *blob_buf);
  static int Convert(int &is_null, my_decimal *value, types::RCDataType &rcitem, int output_scale = -1);
  static int Convert(int &is_null, int64_t &value, types::RCDataType &rcitem, enum_field_types f_type);
  static int Convert(int &is_null, double &value, types::RCDataType &rcitem);
  static int Convert(int &is_null, String *value, types::RCDataType &rcitem, enum_field_types f_type);
  static void ComputeTimeZoneDiffInMinutes(THD *thd, short &sign, short &minutes);
  static std::string GetTablePath(TABLE *table);
  static common::SDBError GetIOP(std::unique_ptr<system::IOParameters> &io_params, THD &thd, sql_exchange &ex,
                                 TABLE *table = 0, void *arg = NULL, bool for_exporter = false);
  static common::SDBError GetRejectFileIOParameters(THD &thd, std::unique_ptr<system::IOParameters> &io_params);
  static fs::path GetNextDataDir();

 private:
  void AddTx(Transaction *tx);
  void RemoveTx(Transaction *tx);
  int Execute(THD *thd, LEX *lex, select_result *result_output, SELECT_LEX_UNIT *unit_for_union = NULL);
  int SetUpCacheFolder(const std::string &cachefolder_path);

  static bool AreConvertible(types::RCDataType &rcitem, enum_field_types my_type, uint length = 0);
  static bool IsSDBRoute(THD *thd, TABLE_LIST *table_list, SELECT_LEX *selects_list,
                         int &in_case_of_failure_can_go_to_mysql, int with_insert);
  static const char *GetFilename(SELECT_LEX *selects_list, int &is_dumpfile);
  static std::unique_ptr<system::IOParameters> CreateIOParameters(const std::string &path, void *arg);
  static std::unique_ptr<system::IOParameters> CreateIOParameters(THD *thd, TABLE *table, void *arg);
  void LogStat();
  std::shared_ptr<TableOption> GetTableOption(const std::string &table, TABLE *form);
  std::shared_ptr<TableShare> getTableShare(const std::string &table_path);
  void ProcessDelayedInsert();
  void ProcessDelayedMerge();
  std::unique_ptr<char[]> GetRecord(size_t &len);
  void EncodeRecord(const std::string &table_path, int tid, Field **field, size_t col, size_t blobs,
                    std::unique_ptr<char[]> &buf, uint32_t &size);

 private:
  struct StonedbStat {
    unsigned long loaded;
    unsigned long load_cnt;
    unsigned long delayinsert;
    unsigned long failed_delayinsert;
    unsigned long select;
    unsigned long loaded_dup;
    unsigned long update;
  } stonedb_stat{};

  std::thread m_load_thread;
  std::thread m_merge_thread;
  std::thread m_monitor_thread;
  std::thread m_purge_thread;

  struct purge_task {
    fs::path file;
    common::TX_ID xmin;
    int32_t cookie;
  };
  // obsolete version files to purge
  std::list<purge_task> gc_tasks;
  std::mutex gc_tasks_mtx;

  bool exiting = false;

  std::condition_variable cv;
  std::mutex cv_mtx;
  std::condition_variable cv_merge;
  std::mutex cv_merge_mtx;

  system::ResourceManager *m_resourceManager = nullptr;

  uint m_slot = 0;

  struct TrxCmp {
    bool operator()(Transaction *l, Transaction *r) const;
  };

  // The set is sorted by transaction ID so we can get min_xid/max_xid in
  // constant time.
  std::set<Transaction *, TrxCmp> m_txs;
  std::mutex m_txs_mtx;
  common::TX_ID min_xid;
  common::TX_ID max_xid;

  std::unordered_map<std::string, std::shared_ptr<TableShare>> table_share_map;
  std::mutex table_share_mutex;

  fs::path stonedb_data_dir;

  utils::MappedCircularBuffer insert_buffer;

  // Engine statistics
  unsigned long IPM = 0;   // Insert per minute
  unsigned long IT = 0;    // Insert total
  unsigned long QPM = 0;   // query per minute
  unsigned long QT = 0;    // query total
  unsigned long LPM = 0;   // Load per minute
  unsigned long LT = 0;    // Load total
  unsigned long LDPM = 0;  // Load dup per minute
  unsigned long LDT = 0;   // Load dup total
  unsigned long UPM = 0;   // Update per minute
  unsigned long UT = 0;    // Update total

  std::unordered_map<std::string, std::shared_ptr<index::RCTableIndex>> m_table_keys;
  std::unordered_map<std::string, std::shared_ptr<RCMemTable>> mem_table_map;
  std::shared_mutex tables_keys_mutex;
  std::shared_mutex mem_table_mutex;
  std::thread m_drop_idx_thread;
  std::condition_variable cv_drop;
  std::mutex cv_drop_mtx;
  std::unique_ptr<TaskExecutor> task_executor;
};

class ResultSender {
 public:
  ResultSender(THD *thd, select_result *res, List<Item> &fields);
  virtual ~ResultSender();

  void Send(TempTable *t);
  void Send(TempTable::RecordIterator &iter);
  void SendRow(const std::vector<std::unique_ptr<types::RCDataType>> &record, TempTable *owner);

  void SetLimits(int64_t *_offset, int64_t *_limit) {
    offset = _offset;
    limit = _limit;
  }
  void SetAffectRows(int64_t _affect_rows) { affect_rows = _affect_rows; }
  virtual void CleanUp();
  virtual void SendEof();

  void Finalize(TempTable *result);
  int64_t SentRows() const { return rows_sent; }

 protected:
  THD *thd;
  select_result *res;
  std::map<int, Item *> items_backup;
  uint *buf_lens;
  List<Item> &fields;
  bool is_initialized;
  int64_t *offset;
  int64_t *limit;
  int64_t rows_sent;
  int64_t affect_rows;

  virtual void Init(TempTable *t);
  virtual void SendRecord(const std::vector<std::unique_ptr<types::RCDataType>> &record);
};

class ResultExportSender final : public ResultSender {
 public:
  ResultExportSender(THD *thd, select_result *result, List<Item> &fields);

  void CleanUp() override {}
  void SendEof() override;

 protected:
  void Init(TempTable *t) override;
  void SendRecord(const std::vector<std::unique_ptr<types::RCDataType>> &record) override;

  exporter::select_sdb_export *export_res;
  std::unique_ptr<exporter::DataExporter> rcde;
  std::shared_ptr<system::LargeBuffer> rcbuffer;
};

enum class sdb_var_name {
  SDB_DATAFORMAT,
  SDB_PIPEMODE,
  SDB_NULL,
  SDB_THROTTLE,
  SDB_STONEDBEXPRESSIONS,
  SDB_PARALLEL_AGGR,
  SDB_REJECT_FILE_PATH,
  SDB_ABORT_ON_COUNT,
  SDB_ABORT_ON_THRESHOLD,
  SDB_VAR_LIMIT  // KEEP THIS LAST
};

static std::string sdb_var_name_strings[] = {"STONEDB_LOAD_TIMEOUT",        "STONEDB_LOAD_DATAFORMAT",
                                             "STONEDB_LOAD_PIPEMODE",       "STONEDB_LOAD_NULL",
                                             "STONEDB_LOAD_THROTTLE",       "STONEDB_LOAD_STONEDBEXPRESSIONS",
                                             "STONEDB_LOAD_PARALLEL_AGGR",  "STONEDB_LOAD_REJECT_FILE",
                                             "STONEDB_LOAD_ABORT_ON_COUNT", "STONEDB_LOAD_ABORT_ON_THRESHOLD"};

std::string get_parameter_name(enum sdb_var_name vn);

int get_parameter(THD *thd, enum sdb_var_name vn, longlong &result, std::string &s_result);

// return 0 on success
// 1 if parameter was not specified
// 2 if was specified but with wrong type
int get_parameter(THD *thd, enum sdb_var_name vn, double &value);
int get_parameter(THD *thd, enum sdb_var_name vn, int64_t &value);
int get_parameter(THD *thd, enum sdb_var_name vn, std::string &value);

bool parameter_equals(THD *thd, enum sdb_var_name vn, longlong value);
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_ENGINE_H_
