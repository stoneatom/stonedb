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
#ifndef STONEDB_INDEX_KV_STORE_H_
#define STONEDB_INDEX_KV_STORE_H_
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

namespace stonedb {
namespace index {

class KVStore final {
 public:
  KVStore(const KVStore &) = delete;
  KVStore() : kv_data_dir(mysql_real_data_home){};
  KVStore &operator=(const KVStore &) = delete;
  ~KVStore() { UnInit(); }
  void Init();
  void UnInit();
  void DelDataSignal() { cv_drop.notify_one(); }
  void AsyncDropData();
  rocksdb::TransactionDB *GetRdb() const { return rdb; }
  uint GetNextIndexId() { return ddl_manager.get_and_update_next_number(&dict_manager); }
  rocksdb::ColumnFamilyHandle *GetCfHandle(std::string &cf_name) { return cf_manager.get_or_create_cf(rdb, cf_name); }
  rocksdb::ColumnFamilyHandle *GetCfHandleByID(const uint32_t id) { return cf_manager.get_cf_by_id(id); }
  bool indexdroping(GlobalId &index) {
    return dict_manager.is_drop_index_ongoing(index, MetaType::DDL_DROP_INDEX_ONGOING);
  }
  // kv table meta operation
  std::shared_ptr<RdbTable> FindTable(std::string &name) { return ddl_manager.find(name); }
  common::ErrorCode KVWriteTableMeta(std::shared_ptr<RdbTable> tbl);
  common::ErrorCode KVDelTableMeta(const std::string &tablename);
  common::ErrorCode KVRenameTableMeta(const std::string &s_name, const std::string &d_name);
  // kv memory table meta operation
  std::shared_ptr<core::RCMemTable> FindMemTable(std::string &name) { return ddl_manager.find_mem(name); }
  common::ErrorCode KVWriteMemTableMeta(std::shared_ptr<core::RCMemTable> tb_mem);
  common::ErrorCode KVDelMemTableMeta(std::string table_name);
  common::ErrorCode KVRenameMemTableMeta(std::string s_name, std::string d_name);
  // kv data operation
  bool KVDeleteKey(rocksdb::WriteOptions &wopts, rocksdb::ColumnFamilyHandle *cf, rocksdb::Slice &key);
  rocksdb::Iterator *GetScanIter(rocksdb::ReadOptions &ropts, rocksdb::ColumnFamilyHandle *cf) {
    return rdb->NewIterator(ropts, cf);
  }
  bool KVWriteBatch(rocksdb::WriteOptions &wopts, rocksdb::WriteBatch *batch);

  const rocksdb::Snapshot *GetRdbSnapshot() { return rdb->GetSnapshot(); }
  void ReleaseRdbSnapshot(const rocksdb::Snapshot *snapshot) { rdb->ReleaseSnapshot(snapshot); }

 private:
  bool exiting = false;
  fs::path kv_data_dir;
  std::thread drop_kv_thread;
  std::mutex cv_drop_mtx;
  std::condition_variable cv_drop;
  rocksdb::BlockBasedTableOptions bbto_;

  rocksdb::TransactionDB *rdb;
  DICTManager dict_manager;
  CFManager cf_manager;
  DDLManager ddl_manager;
};

// application write a compaction filter that can filter out keys from deleted
// range during background compaction
class IndexCompactFilter : public rocksdb::CompactionFilter {
 public:
  IndexCompactFilter(const IndexCompactFilter &) = delete;
  IndexCompactFilter &operator=(const IndexCompactFilter &) = delete;

  explicit IndexCompactFilter(uint32_t _cf_id) : m_cf_id(_cf_id) {}
  ~IndexCompactFilter() {}
  bool Filter(int level, const rocksdb::Slice &key, const rocksdb::Slice &existing_value, std::string *new_value,
              bool *value_changed) const override;

  bool IgnoreSnapshots() const override { return true; }
  const char *Name() const override { return "IndexCompactFilter"; }

 private:
  const uint32_t m_cf_id;
  mutable GlobalId m_prev_index = {0, 0};
  mutable bool m_should_delete = false;
};

class IndexCompactFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  IndexCompactFilterFactory(const IndexCompactFilterFactory &) = delete;
  IndexCompactFilterFactory &operator=(const IndexCompactFilterFactory &) = delete;
  IndexCompactFilterFactory() {}
  ~IndexCompactFilterFactory() {}

  const char *Name() const override { return "IndexCompactFilterFactory"; }

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new IndexCompactFilter(context.column_family_id));
  }
};

}  // namespace index
}  // namespace stonedb

#endif  // STONEDB_INDEX_KV_STORE_H_
