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
#ifndef TIANMU_INDEX_KV_STORE_H_
#define TIANMU_INDEX_KV_STORE_H_
#pragma once

#include <mutex>
#include <string>
#include <thread>

#include "common/common_definitions.h"
#include "index/rdb_meta_manager.h"
#include "index/rdb_utils.h"

#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/fs.h"

namespace Tianmu {
namespace index {

/**key-value interface**/
class KVStore final {
 public:
  KVStore(const KVStore &) = delete;
  // mysql_real_data_home, pls refer to the system params.
  KVStore() : kv_data_dir_(mysql_real_data_home){};

  KVStore &operator=(const KVStore &) = delete;
  virtual ~KVStore() { UnInit(); }

  // initialize rocksdb engine
  void Init();
  // deinitialize rocksdb engine
  void UnInit();

  // sending signal DeleteData
  void DeleteDataSignal() { cv_drop_.notify_one(); }
  // async Droping Data
  void AsyncDropData();
  // gets rocksdb handler
  rocksdb::TransactionDB *GetRdb() const { return txn_db_; }

  uint GetNextIndexId() { return ddl_manager_.get_and_update_next_number(&dict_manager_); }
  // gets the column family
  rocksdb::ColumnFamilyHandle *GetCfHandle(std::string &cf_name) {
    return cf_manager_.get_or_create_cf(txn_db_, cf_name);
  }
  // gets the column family by ID
  rocksdb::ColumnFamilyHandle *GetCfHandleByID(const uint32_t id) { return cf_manager_.get_cf_by_id(id); }
  // drops the index by an index ID
  bool IndexDroping(GlobalId &index) {
    return dict_manager_.is_drop_index_ongoing(index, MetaType::DDL_DROP_INDEX_ONGOING);
  }
  // kv table meta operation
  // find a table by table name returns this table handler
  std::shared_ptr<RdbTable> FindTable(std::string &name) { return ddl_manager_.find(name); }
  // Put table definition of `tbl` into the mapping, and also write it to the
  // on-disk data dictionary.
  common::ErrorCode KVWriteTableMeta(std::shared_ptr<RdbTable> tbl);
  common::ErrorCode KVDelTableMeta(const std::string &tablename);
  common::ErrorCode KVRenameTableMeta(const std::string &s_name, const std::string &d_name);

  // kv memory table meta operation
  // as KVWriteTableMeta does, but not to on-disk but in-mem
  std::shared_ptr<core::DeltaTable> FindDeltaTable(std::string &name) { return ddl_manager_.find_delta(name); }
  common::ErrorCode KVWriteDeltaMeta(std::shared_ptr<core::DeltaTable> delta);
  common::ErrorCode KVDelDeltaMeta(std::string table_name);
  common::ErrorCode KVRenameDeltaMeta(std::string s_name, std::string d_name);

  // kv data operation
  bool KVDeleteKey(rocksdb::WriteOptions &wopts, rocksdb::ColumnFamilyHandle *cf, rocksdb::Slice &key);
  rocksdb::Iterator *GetScanIter(rocksdb::ReadOptions &ropts, rocksdb::ColumnFamilyHandle *cf) {
    return txn_db_->NewIterator(ropts, cf);
  }
  // write mult-rows in batch mode with write options.
  bool KVWriteBatch(rocksdb::WriteOptions &wopts, rocksdb::WriteBatch *batch);
  // gets snapshot from rocksdb.
  const rocksdb::Snapshot *GetRdbSnapshot() { return txn_db_->GetSnapshot(); }
  // release the specific snapshot
  void ReleaseRdbSnapshot(const rocksdb::Snapshot *snapshot) { txn_db_->ReleaseSnapshot(snapshot); }
  // gets the column family name by table handler.
  static std::string generate_cf_name(uint index, TABLE *table);
  // creates a ith key of rocksdb table.
  static void create_rdbkey(TABLE *table, uint pos, std::shared_ptr<RdbKey> &new_key_def,
                            rocksdb::ColumnFamilyHandle *cf_handle);
  // create keys and column family for a rocksdb table.
  static common::ErrorCode create_keys_and_cf(TABLE *table, std::shared_ptr<RdbTable> rdb_tbl);
  // Returns index of primary key
  static uint pk_index(const TABLE *const table, std::shared_ptr<RdbTable> tbl_def);

 private:
  // initializationed?
  bool inited_ = false;
  // path where data located
  fs::path kv_data_dir_;
  // async drop thread
  std::thread drop_kv_thread_;
  // drop mutex
  std::mutex cv_drop_mtx_;
  // condition var for drop table
  std::condition_variable cv_drop_;

  // bb table options
  rocksdb::BlockBasedTableOptions bb_table_option_;
  // rocksdb transaction
  rocksdb::TransactionDB *txn_db_;
  // meta data manager
  DICTManager dict_manager_;
  // column family manager
  CFManager cf_manager_;
  // ddl manager
  DDLManager ddl_manager_;
};

// application write a compaction filter that can filter out keys from deleted
// range during background compaction
class IndexCompactFilter : public rocksdb::CompactionFilter {
 public:
  explicit IndexCompactFilter(uint32_t _cf_id) : cf_id_(_cf_id) {}
  virtual ~IndexCompactFilter() {}

  IndexCompactFilter(const IndexCompactFilter &) = delete;
  IndexCompactFilter &operator=(const IndexCompactFilter &) = delete;

  // set a filter with input params
  bool Filter(int level, const rocksdb::Slice &key, const rocksdb::Slice &existing_value, std::string *new_value,
              bool *value_changed) const override;

  bool IgnoreSnapshots() const override { return true; }
  // gets the name of index compact filter. fixed value
  const char *Name() const override { return "IndexCompactFilter"; }

 private:
  const uint32_t cf_id_;
  mutable GlobalId prev_index_ = {0, 0};
  mutable bool should_delete_ = false;
};

class IndexCompactFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  IndexCompactFilterFactory() {}
  virtual ~IndexCompactFilterFactory() {}

  IndexCompactFilterFactory(const IndexCompactFilterFactory &) = delete;
  IndexCompactFilterFactory &operator=(const IndexCompactFilterFactory &) = delete;

  const char *Name() const override { return "IndexCompactFilterFactory"; }

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new IndexCompactFilter(context.column_family_id));
  }
};

}  // namespace index
}  // namespace Tianmu

#endif  // TIANMU_INDEX_KV_STORE_H_
