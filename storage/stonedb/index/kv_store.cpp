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

#include "index/kv_store.h"

#include "core/engine.h"

namespace stonedb {
namespace index {

void KVStore::Init() {
  rocksdb::TransactionDBOptions txn_db_options;
  std::vector<std::string> cf_names;
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descr;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
  rocksdb::Options options;
  options.create_if_missing = true;
  if (stonedb_sysvar_index_cache_size != 0) {
    bbto_.no_block_cache = false;
    bbto_.cache_index_and_filter_blocks = true;
    bbto_.block_cache = rocksdb::NewLRUCache(stonedb_sysvar_index_cache_size << 20);
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto_));
  }
  rocksdb::DBOptions db_option(options);
  auto rocksdb_datadir = kv_data_dir / ".index";
  int max_compact_threads = std::thread::hardware_concurrency() / 4;
  max_compact_threads = (max_compact_threads > 1) ? max_compact_threads : 1;
  db_option.max_background_compactions = max_compact_threads;
  db_option.max_subcompactions = max_compact_threads;
  db_option.env->SetBackgroundThreads(max_compact_threads, rocksdb::Env::Priority::LOW);
  db_option.statistics = rocksdb::CreateDBStatistics();
  // get column family names from manifest file
  rocksdb::Status status = rocksdb::DB::ListColumnFamilies(db_option, rocksdb_datadir, &cf_names);
  if (!status.ok() &&
      ((status.subcode() == rocksdb::Status::kNone))) {
    STONEDB_LOG(LogCtl_Level::INFO, "First init rocksdb, create default column family");
    cf_names.push_back(DEFAULT_CF_NAME);
  }

  rocksdb::ColumnFamilyOptions rs_cf_option(options);
  rocksdb::ColumnFamilyOptions index_cf_option(options);
  // Disable compactions to prevent compaction start before compaction filter is
  // ready.
  index_cf_option.disable_auto_compactions = true;
  index_cf_option.compaction_filter_factory.reset(new IndexCompactFilterFactory);
  for (auto &cfn : cf_names) {
    if (IsRowStoreCF(cfn))
      cf_descr.emplace_back(cfn, rs_cf_option);
    else
      cf_descr.emplace_back(cfn, index_cf_option);
  }
  // open db, get column family handles
  status = rocksdb::TransactionDB::Open(options, txn_db_options, rocksdb_datadir, cf_descr, &cf_handles, &rdb);
  if (!status.ok()) {
    throw common::Exception("Error opening rocks instance. status msg: " + status.ToString());
  }

  cf_manager.init(cf_handles);
  if (!dict_manager.init(rdb->GetBaseDB(), &cf_manager)) {
    throw common::Exception("RocksDB: Failed to initialize data dictionary.");
  }

  if (!ddl_manager.init(&dict_manager, &cf_manager)) {
    throw common::Exception("RocksDB: Failed to initialize DDL manager.");
  }
  // Enable compaction, things needed for compaction filter are finished
  // initializing
  status = rdb->EnableAutoCompaction(cf_handles);
  if (!status.ok()) {
    throw common::Exception("RocksDB: Failed to enable compaction.");
  }
  drop_kv_thread = std::thread([this] { this->AsyncDropData(); });
}
void KVStore::UnInit() {
  exiting = true;
  DelDataSignal();
  drop_kv_thread.join();
  // flush all memtables
  for (const auto &cf_handle : cf_manager.get_all_cf()) {
    rdb->Flush(rocksdb::FlushOptions(), cf_handle);
  }
  // Stop all rocksdb background work
  rocksdb::CancelAllBackgroundWork(rdb->GetBaseDB(), true);
  // clear primary key info
  ddl_manager.cleanup();
  dict_manager.cleanup();
  cf_manager.cleanup();
  if (bbto_.block_cache) bbto_.block_cache->EraseUnRefEntries();
  delete rdb;
  rdb = nullptr;

  kv_data_dir.clear();
}

void KVStore::AsyncDropData() {
  while (!exiting) {
    std::unique_lock<std::mutex> lk(cv_drop_mtx);
    cv_drop.wait_for(lk, std::chrono::seconds(5 * 60));

    std::vector<GlobalId> del_index_ids;
    dict_manager.get_ongoing_index(del_index_ids, MetaType::DDL_DROP_INDEX_ONGOING);
    for (auto d : del_index_ids) {
      rocksdb::CompactRangeOptions options;
      options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
      options.exclusive_manual_compaction = false;
      uchar buf_begin[INDEX_NUMBER_SIZE];
      uchar buf_end[INDEX_NUMBER_SIZE];
      be_store_index(buf_begin, d.index_id);
      be_store_index(buf_end, d.index_id + 1);
      rocksdb::Range range =
          rocksdb::Range({(const char *)buf_begin, INDEX_NUMBER_SIZE}, {(const char *)buf_end, INDEX_NUMBER_SIZE});
      rocksdb::ColumnFamilyHandle *cfh = cf_manager.get_cf_by_id(d.cf_id);
      DBUG_ASSERT(cfh);
      rocksdb::Status status = rocksdb::DeleteFilesInRange(rdb->GetBaseDB(), cfh, &range.start, &range.limit);
      if (!status.ok()) {
        STONEDB_LOG(LogCtl_Level::ERROR, "RocksDB: delete file range fail, status: %s ", status.ToString().c_str());
        if (status.IsShutdownInProgress()) {
          break;
        }
      }
      status = rdb->CompactRange(options, cfh, &range.start, &range.limit);
      if (!status.ok()) {
        STONEDB_LOG(LogCtl_Level::ERROR, "RocksDB: Compact range index fail, status: %s ", status.ToString().c_str());
        if (status.IsShutdownInProgress()) {
          break;
        }
      }
    }
    dict_manager.finish_indexes(del_index_ids, MetaType::DDL_DROP_INDEX_ONGOING);
  }
  STONEDB_LOG(LogCtl_Level::INFO, "KVStore drop Table thread exiting...");
}

common::ErrorCode KVStore::KVWriteTableMeta(std::shared_ptr<RdbTable> tbl) {
  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager.begin();
  rocksdb::WriteBatch *const batch = wb.get();
  dict_manager.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager.unlock(); });
  ddl_manager.put_and_write(tbl, batch);
  if (!dict_manager.commit(batch)) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Commit table metadata fail!");
    return common::ErrorCode::FAILED;
  }
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode KVStore::KVDelTableMeta(const std::string &tablename) {
  //  Find the table in the hash
  std::shared_ptr<RdbTable> tbl = ddl_manager.find(tablename);
  if (!tbl) return common::ErrorCode::FAILED;

  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager.begin();
  rocksdb::WriteBatch *const batch = wb.get();
  dict_manager.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager.unlock(); });
  // Remove the table entry in data dictionary (this will also remove it from
  // the persistent data dictionary).
  dict_manager.add_drop_table(tbl->m_rdbkeys, batch);
  ddl_manager.remove(tbl, batch);
  if (!dict_manager.commit(batch)) {
    return common::ErrorCode::FAILED;
  }
  DelDataSignal();
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode KVStore::KVRenameTableMeta(const std::string &s_name, const std::string &d_name) {
  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager.begin();
  rocksdb::WriteBatch *const batch = wb.get();
  dict_manager.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager.unlock(); });
  if (!ddl_manager.rename(s_name, d_name, batch)) {
    STONEDB_LOG(LogCtl_Level::ERROR, "rename table %s not exsit", s_name.c_str());
    return common::ErrorCode::FAILED;
  }

  if (!dict_manager.commit(batch)) {
    return common::ErrorCode::FAILED;
  }
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode KVStore::KVWriteMemTableMeta(std::shared_ptr<core::RCMemTable> tb_mem) {
  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager.begin();
  rocksdb::WriteBatch *const batch = wb.get();
  dict_manager.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager.unlock(); });
  ddl_manager.put_mem(tb_mem, batch);
  if (!dict_manager.commit(batch)) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Commit memory table metadata fail!");
    return common::ErrorCode::FAILED;
  }
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode KVStore::KVDelMemTableMeta(std::string table_name) {
  std::shared_ptr<core::RCMemTable> tb_mem = ddl_manager.find_mem(table_name);
  if (!tb_mem) return common::ErrorCode::FAILED;

  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager.begin();
  rocksdb::WriteBatch *const batch = wb.get();
  dict_manager.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager.unlock(); });
  std::vector<GlobalId> dropped_index_ids;
  GlobalId gid;
  gid.cf_id = tb_mem->GetCFHandle()->GetID();
  gid.index_id = tb_mem->GetMemID();
  dropped_index_ids.push_back(gid);
  dict_manager.add_drop_index(dropped_index_ids, batch);
  ddl_manager.remove_mem(tb_mem, batch);
  if (!dict_manager.commit(batch)) return common::ErrorCode::FAILED;
  DelDataSignal();

  return common::ErrorCode::SUCCESS;
}

common::ErrorCode KVStore::KVRenameMemTableMeta(std::string s_name, std::string d_name) {
  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager.begin();
  rocksdb::WriteBatch *const batch = wb.get();
  dict_manager.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager.unlock(); });
  if (!ddl_manager.rename_mem(s_name, d_name, batch)) {
    STONEDB_LOG(LogCtl_Level::ERROR, "rename table %s failed", s_name.c_str());
    return common::ErrorCode::FAILED;
  }

  if (!dict_manager.commit(batch)) return common::ErrorCode::FAILED;

  return common::ErrorCode::SUCCESS;
}

bool KVStore::KVDeleteKey(rocksdb::WriteOptions &wopts, rocksdb::ColumnFamilyHandle *cf, rocksdb::Slice &key) {
  rocksdb::Status s = rdb->Delete(wopts, cf, key);
  if (!s.ok()) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Rdb delete key fail: %s", s.ToString().c_str());
    return false;
  }

  return true;
}

bool KVStore::KVWriteBatch(rocksdb::WriteOptions &wopts, rocksdb::WriteBatch *batch) {
  const rocksdb::Status s = rdb->GetBaseDB()->Write(wopts, batch);
  if (!s.ok()) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Rdb write batch fail: %s", s.ToString().c_str());
    return false;
  }
  return true;
}

bool IndexCompactFilter::Filter([[maybe_unused]] int level, const rocksdb::Slice &key,
                                [[maybe_unused]] const rocksdb::Slice &existing_value,
                                [[maybe_unused]] std::string *new_value, [[maybe_unused]] bool *value_changed) const {
  GlobalId gl_index_id;
  gl_index_id.cf_id = m_cf_id;
  gl_index_id.index_id = be_to_uint32((const uchar *)key.data());

  if (gl_index_id.index_id != m_prev_index.index_id) {
    m_should_delete = kvstore->indexdroping(gl_index_id);
    m_prev_index = gl_index_id;
  }

  if (m_should_delete) {
    return true;
  }

  return false;
}

}  // namespace index
}  // namespace stonedb
