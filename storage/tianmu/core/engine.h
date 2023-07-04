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
#ifndef TIANMU_CORE_ENGINE_H_
#define TIANMU_CORE_ENGINE_H_
#pragma once

#include <atomic>
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
#include "core/table_share.h"
#include "core/temp_table.h"
#include "core/tianmu_table.h"
#include "executor/combined_iterator.h"
#include "exporter/data_exporter.h"
#include "exporter/export2file.h"
#include "index/tianmu_table_index.h"
#include "log.h"
#include "sql_table.h"
#include "system/io_parameters.h"
#include "system/tianmu_system.h"
#include "util/fs.h"
#include "util/mapped_circular_buffer.h"
#include "util/thread_pool.h"

extern handlerton *tianmu_hton;
class Field;

namespace Tianmu {
namespace types {
class TianmuDataType;
}  // namespace types
namespace system {
class ResourceManager;
}  // namespace system

namespace index {
class TianmuTableIndex;
class KVStore;
}  // namespace index

namespace exporter {
class select_tianmu_export;
}  // namespace exporter

namespace core {
struct AttrInfo;
class TableShare;
class Transaction;
class RSIndex;
class TaskExecutor;
class DeltaTable;
class DeltaIterator;

class Engine final {
 public:
  Engine();
  ~Engine();

  int Init(uint engine_slot);
  index::KVStore *getStore() const { return store_; }

  void CreateTable(const std::string &table, TABLE *from, HA_CREATE_INFO *create_info);
  int DeleteTable(const char *table, THD *thd);
  void TruncateTable(const std::string &table_path, THD *thd);
  int RenameTable(Transaction *trans_, const std::string &from, const std::string &to, THD *thd);
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
  void IncTianmuStatUpdate() { ++tianmu_stat.update; }
  std::vector<AttrInfo> GetTableAttributesInfo(const std::string &table_path, TABLE_SHARE *table_share);
  void UpdateAndStoreColumnComment(TABLE *table, int field_id, Field *source_field, int source_field_id,
                                   CHARSET_INFO *cs);
  common::TianmuError RunLoader(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg);
  void CommitTx(THD *thd, bool all);
  void Rollback(THD *thd, bool all, bool force_error_message = false);
  Transaction *CreateTx(THD *thd);
  Transaction *GetTx(THD *thd);
  void ClearTx(THD *thd);
  QueryRouteTo HandleSelect(THD *thd, LEX *lex, Query_result *&result_output, ulong setup_tables_done_option, int &res,
                            int &is_optimize_after_tianmu, int &tianmu_free_join, int with_insert = false);
  system::ResourceManager *getResourceManager() const { return m_resourceManager; }
  std::shared_ptr<TianmuTable> GetTableRD(const std::string &table_path);
  int InsertRow(const std::string &tablename, Transaction *trans_, TABLE *table, std::shared_ptr<TableShare> &share);
  int UpdateRow(const std::string &tablename, TABLE *table, std::shared_ptr<TableShare> &share, uint64_t row_id,
                const uchar *old_data, uchar *new_data);
  int DeleteRow(const std::string &tablename, TABLE *table, std::shared_ptr<TableShare> &share, uint64_t row_id);
  void InsertDelayed(const std::string &table_path, TABLE *table);
  int InsertToDelta(const std::string &table_path, std::shared_ptr<TableShare> &share, TABLE *table);
  void UpdateToDelta(const std::string &table_path, std::shared_ptr<TableShare> &share, TABLE *table, uint64_t row_id,
                     const uchar *old_data, uchar *new_data);
  void DeleteToDelta(std::shared_ptr<TableShare> &share, TABLE *table, uint64_t row_id);
  std::string DelayedBufferStat() { return insert_buffer.Status(); }
  std::string DeltaStoreStat();
  void UnRegisterTable(const std::string &table_path);
  std::shared_ptr<TableShare> GetTableShare(const TABLE_SHARE *table_share);
  common::TX_ID MinXID() const { return min_xid; }
  common::TX_ID MaxXID() const { return max_xid; }
  void DeferRemove(const fs::path &file, int32_t cookie);
  void HandleDeferredJobs();
  // add primary key
  void AddTableIndex(const std::string &table_path, TABLE *table, THD *thd);
  // delete primary key
  bool DeleteTableIndex(const std::string &table_path, THD *thd);
  std::shared_ptr<index::TianmuTableIndex> GetTableIndex(const std::string &table_path);
  bool has_pk(TABLE *table) const { return table->s->primary_key != MAX_INDEXES; }
  void RenameRdbTable(const std::string &from, const std::string &to);
  void DropSignal() { cv_drop_.notify_one(); }
  void ResetTaskExecutor(int percent);
  TaskExecutor *GetTaskExecutor() const { return task_executor.get(); }
  void AddTableDelta(TABLE *form, std::shared_ptr<TableShare> share);
  void UnregisterDeltaTable(const std::string &from, const std::string &to);

  size_t GetDeltaSyncStats(std::ostringstream& buf, std::unordered_set<std::string>& filter_set);

 public:
  utils::thread_pool bg_load_thread_pool;
  utils::thread_pool load_thread_pool;
  utils::thread_pool query_thread_pool;
  utils::thread_pool delete_or_update_thread_pool;
  DataCache cache;
  ObjectCache<FilterCoordinate, RSIndex, FilterCoordinate> filter_cache;

 public:
  static common::ColumnType GetCorrespondingType(const Field &field);
  static AttributeTypeInfo GetCorrespondingATI(Field &field);
  static AttributeTypeInfo GetAttrTypeInfo(const Field &field);
  static common::ColumnType GetCorrespondingType(const enum_field_types &eft);
  static bool IsTianmuTable(TABLE *table);
  static bool ConvertToField(Field *field, types::TianmuDataType &tianmu_item, std::vector<uchar> *blob_buf);
  static int Convert(int &is_null, my_decimal *value, types::TianmuDataType &tianmu_item, int output_scale = -1);
  // Add args unsigned_flag here is much more easier to construct TianmuNum in Convert function, another way is
  // add unsigned_flag in TianmuNum, it's more complex.
  static int Convert(int &is_null, int64_t &value, types::TianmuDataType &tianmu_item, enum_field_types f_type,
                     bool unsigned_flag);
  static int Convert(int &is_null, double &value, types::TianmuDataType &tianmu_item);
  static int Convert(int &is_null, String *value, types::TianmuDataType &tianmu_item, enum_field_types f_type);
  static bool DecodeInsertRecordToField(const char *ptr, Field **fields);
  static void DecodeUpdateRecordToField(const char *ptr, Field **fields);
  static void ComputeTimeZoneDiffInMinutes(THD *thd, short &sign, short &minutes);
  static std::string GetTablePath(TABLE *table);
  static common::TianmuError GetIOP(std::unique_ptr<system::IOParameters> &io_params, THD &thd, sql_exchange &ex,
                                    TABLE *table = 0, void *arg = nullptr, bool for_exporter = false);
  static common::TianmuError GetRejectFileIOParameters(THD &thd, std::unique_ptr<system::IOParameters> &io_params);
  static fs::path GetNextDataDir();

  static const char *StrToFiled(const char *ptr, Field *field, DeltaRecordHead *deltaRecord, int col_num);
  static char *FiledToStr(char *ptr, Field *field, DeltaRecordHead *deltaRecord, int col_num, THD *thd);

  void setExtra(ha_extra_function extra) { extra_info = extra; }
  ha_extra_function getExtra() { return extra_info; }

 private:
  void AddTx(Transaction *tx);
  void RemoveTx(Transaction *tx);
  QueryRouteTo Execute(THD *thd, LEX *lex, Query_result *result_output, SELECT_LEX_UNIT *unit_for_union = nullptr);
  int SetUpCacheFolder(const std::string &cachefolder_path);

  static bool AreConvertible(types::TianmuDataType &tianmu_item, enum_field_types my_type, uint length = 0);
  static bool IsTIANMURoute(THD *thd, TABLE_LIST *table_list, SELECT_LEX *selects_list,
                            int &in_case_of_failure_can_go_to_mysql, int with_insert);
  static const char *GetFilename(SELECT_LEX *selects_list, int &is_dumpfile);
  static std::unique_ptr<system::IOParameters> CreateIOParameters(const std::string &path, void *arg);
  static std::unique_ptr<system::IOParameters> CreateIOParameters(THD *thd, TABLE *table, void *arg);
  void LogStat();
  std::shared_ptr<TableOption> GetTableOption(const std::string &table, TABLE *form, HA_CREATE_INFO *create_info);
  std::shared_ptr<TableShare> getTableShare(const std::string &table_path);
  std::unique_ptr<char[]> GetRecord(size_t &len);

  static void EncodeInsertRecord(const std::string &table_path, Field **field, size_t col, size_t blobs,
                                 std::unique_ptr<char[]> &buf, uint32_t &size, THD *thd);
  static void EncodeUpdateRecord(const std::string &table_path, std::unordered_map<uint, Field *> update_fields,
                                 size_t field_count, size_t blobs, std::unique_ptr<char[]> &buf, uint32_t &buf_size,
                                 THD *thd);
  static void EncodeDeleteRecord(std::unique_ptr<char[]> &buf, uint32_t &buf_size);
  void ProcessInsertBufferMerge();
  void ProcessDeltaStoreMerge();

 private:
  struct TianmuStat {
    unsigned long loaded;
    unsigned long load_cnt;
    unsigned long delta_insert;
    unsigned long failed_delta_insert;
    unsigned long delta_update;
    unsigned long failed_delta_update;
    unsigned long delta_delete;
    unsigned long failed_delta_delete;
    unsigned long select;
    unsigned long loaded_dup;
    unsigned long update;
  } tianmu_stat{};

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
  //?????
  // atomic_uint32_t tableID_;

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

  fs::path tianmu_data_dir;

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

  std::unordered_map<std::string, std::shared_ptr<index::TianmuTableIndex>> m_table_keys;
  std::unordered_map<std::string, std::shared_ptr<DeltaTable>> m_table_deltas;
  std::shared_mutex tables_keys_mutex;
  std::shared_mutex mem_table_mutex;
  std::thread m_drop_idx_thread;
  std::condition_variable cv_drop_;
  std::mutex cv_drop_mtx_;
  std::unique_ptr<TaskExecutor> task_executor;
  uint64_t m_mem_available_ = 0;
  uint64_t m_swap_used_ = 0;
  index::KVStore *store_;

  ha_extra_function extra_info;
};

class ResultSender {
 public:
  ResultSender(THD *thd, Query_result *res, List<Item> &fields);
  virtual ~ResultSender();

  void Send(TempTable *t);
  void Send(TempTable::RecordIterator &iter);
  void SendRow(std::vector<std::unique_ptr<types::TianmuDataType>> &record, TempTable *owner);

  void SetLimits(int64_t *_offset, int64_t *_limit) {
    offset = _offset;
    limit = _limit;
  }
  void SetAffectRows(int64_t _affect_rows) { affect_rows = _affect_rows; }
  virtual void CleanUp();
  virtual void SendEof();

  void Finalize(TempTable *result);
  int64_t SentRows() const { return rows_sent; }

 public:
  List<Item> &fields;

 protected:
  THD *thd;
  Query_result *res;
  std::map<int, Item *> items_backup;
  uint *buf_lens;
  bool is_initialized;
  int64_t *offset;
  int64_t *limit;
  int64_t rows_sent;
  int64_t affect_rows;

  virtual void Init(TempTable *t);
  virtual void SendRecord(std::vector<std::unique_ptr<types::TianmuDataType>> &record);
};

class ResultExportSender final : public ResultSender {
 public:
  ResultExportSender(THD *thd, Query_result *result, List<Item> &fields);

  void CleanUp() override {}
  void SendEof() override;

 protected:
  void Init(TempTable *t) override;
  void SendRecord(std::vector<std::unique_ptr<types::TianmuDataType>> &record) override;

  exporter::select_tianmu_export *export_res_;
  std::unique_ptr<exporter::DataExporter> tianmu_data_exp_;
  std::shared_ptr<system::LargeBuffer> tiammu_buffer_;
};

enum class tianmu_param_name {
  TIANMU_TIMEOUT = 0,
  TIANMU_DATAFORMAT,
  TIANMU_PIPEMODE,
  TIANMU_NULL,
  TIANMU_THROTTLE,
  TIANMU_TIANMUEXPRESSIONS,
  TIANMU_PARALLEL_AGGR,
  TIANMU_REJECT_FILE_PATH,
  TIANMU_ABORT_ON_COUNT,
  TIANMU_ABORT_ON_THRESHOLD,
  TIANMU_VAR_LIMIT  // KEEP THIS LAST
};

static std::string tianmu_var_name_strings[] = {"TIANMU_LOAD_TIMEOUT",        "TIANMU_LOAD_DATAFORMAT",
                                                "TIANMU_LOAD_PIPEMODE",       "TIANMU_LOAD_NULL",
                                                "TIANMU_LOAD_THROTTLE",       "TIANMU_LOAD_TIANMUEXPRESSIONS",
                                                "TIANMU_LOAD_PARALLEL_AGGR",  "TIANMU_LOAD_REJECT_FILE",
                                                "TIANMU_LOAD_ABORT_ON_COUNT", "TIANMU_LOAD_ABORT_ON_THRESHOLD"};

std::string get_parameter_name(enum tianmu_param_name vn);

int get_parameter(THD *thd, enum tianmu_param_name vn, longlong &result, std::string &s_result);

// return 0 on success
// 1 if parameter was not specified
// 2 if was specified but with wrong type
int get_parameter(THD *thd, enum tianmu_param_name vn, double &value);
int get_parameter(THD *thd, enum tianmu_param_name vn, int64_t &value);
int get_parameter(THD *thd, enum tianmu_param_name vn, std::string &value);

bool parameter_equals(THD *thd, enum tianmu_param_name vn, longlong value);

/** The maximum length of an encode table name in bytes.  The max
+table and database names are NAME_CHAR_LEN (64) characters. After the
+encoding, the max length would be NAME_CHAR_LEN (64) *
+FILENAME_CHARSET_MAXNAMLEN (5) = 320 bytes. The number does not include a
+terminating '\0'. InnoDB can handle longer names internally */
#define MAX_TABLE_NAME_LEN 320

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_ENGINE_H_
