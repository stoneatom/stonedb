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
#include "core/merge_operator.h"

namespace Tianmu {
namespace index {

void KVStore::Init() {
  rocksdb::Options options;
  options.create_if_missing = true;

  if (tianmu_sysvar_index_cache_size != 0) {
    bb_table_option_.no_block_cache = false;
    bb_table_option_.cache_index_and_filter_blocks = true;
    bb_table_option_.block_cache = rocksdb::NewLRUCache(tianmu_sysvar_index_cache_size << 20);
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bb_table_option_));
  }

  // get column family names from manfest file
  rocksdb::TransactionDBOptions txn_db_options;
  std::vector<std::string> cf_names;
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descr;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
  rocksdb::DBOptions db_option(options);

  // set the DBOptions's params.
  auto rocksdb_datadir = kv_data_dir_ / ".index";
  int max_compact_threads = std::thread::hardware_concurrency() / 4;
  max_compact_threads = (max_compact_threads > 1) ? max_compact_threads : 1;
  db_option.max_background_compactions = max_compact_threads;
  db_option.max_subcompactions = max_compact_threads;
  db_option.env->SetBackgroundThreads(max_compact_threads, rocksdb::Env::Priority::LOW);
  db_option.statistics = rocksdb::CreateDBStatistics();
  rocksdb::Status status = rocksdb::DB::ListColumnFamilies(db_option, rocksdb_datadir, &cf_names);
  if (!status.ok() &&
      ((status.subcode() == rocksdb::Status::kNone) || (status.subcode() == rocksdb::Status::kPathNotFound))) {
    TIANMU_LOG(LogCtl_Level::INFO, "First init rocksdb, create default cloum family");
    cf_names.push_back(DEFAULT_CF_NAME);
  }

  // Only delta store cf need merge operator
  rocksdb::ColumnFamilyOptions delta_cf_option(options);
  delta_cf_option.merge_operator = std::make_shared<core::RecordMergeOperator>();

  // Disable compactions to prevent compaction start before compaction filter is ready.
  rocksdb::ColumnFamilyOptions index_cf_option(options);
  index_cf_option.disable_auto_compactions = true;
  index_cf_option.compaction_filter_factory.reset(new IndexCompactFilterFactory);

  for (auto &cfn : cf_names) {
    IsDeltaStoreCF(cfn) ? cf_descr.emplace_back(cfn, delta_cf_option) : cf_descr.emplace_back(cfn, index_cf_option);
  }

  // open db, get column family handles
  status = rocksdb::TransactionDB::Open(options, txn_db_options, rocksdb_datadir, cf_descr, &cf_handles, &txn_db_);
  if (!status.ok()) {
    throw common::Exception("Error opening rocks instance. status msg: " + status.ToString());
  }
  // init the cf manager and ddl manager with dict manager.
  cf_manager_.init(cf_handles);
  if (!dict_manager_.init(txn_db_->GetBaseDB(), &cf_manager_)) {
    throw common::Exception("RocksDB: Failed to initialize data dictionary.");
  }
  if (!ddl_manager_.init(&dict_manager_, &cf_manager_)) {
    throw common::Exception("RocksDB: Failed to initialize DDL manager.");
  }

  // Enable compaction, things needed for compaction filter are finished
  // initializing
  status = txn_db_->EnableAutoCompaction(cf_handles);
  if (!status.ok()) {
    throw common::Exception("RocksDB: Failed to enable compaction.");
  }

  drop_kv_thread_ = std::thread([this] { this->AsyncDropData(); });
  inited_ = true;
}

void KVStore::UnInit() {
  inited_ = !inited_;

  DeleteDataSignal();
  drop_kv_thread_.join();

  // flush all memtables
  for (const auto &cf_handle : cf_manager_.get_all_cf()) {
    txn_db_->Flush(rocksdb::FlushOptions(), cf_handle);
  }
  // Stop all rocksdb background work
  rocksdb::CancelAllBackgroundWork(txn_db_->GetBaseDB(), true);

  // clear primary key info
  ddl_manager_.cleanup();
  dict_manager_.cleanup();
  cf_manager_.cleanup();

  // earase unref entries in single cache shard.
  if (bb_table_option_.block_cache)
    bb_table_option_.block_cache->EraseUnRefEntries();

  // release the rocksdb handler, txn_db_.
  if (txn_db_) {
    delete txn_db_;
    txn_db_ = nullptr;
  }
}

void KVStore::AsyncDropData() {
  while (inited_) {
    std::unique_lock<std::mutex> lk(cv_drop_mtx_);
    // wait for 5 seconds.
    cv_drop_.wait_for(lk, std::chrono::seconds(5 * 60));

    // id set of index wich will be deleted.
    std::vector<GlobalId> del_index_ids;
    dict_manager_.get_ongoing_index(del_index_ids, MetaType::DDL_DROP_INDEX_ONGOING);
    for (auto id : del_index_ids) {
      // set compaction options.
      rocksdb::CompactRangeOptions options;
      options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
      options.exclusive_manual_compaction = false;

      uchar buf_begin[INDEX_NUMBER_SIZE] = {0};
      uchar buf_end[INDEX_NUMBER_SIZE] = {0};
      be_store_index(buf_begin, id.index_id);
      be_store_index(buf_end, id.index_id + 1);

      rocksdb::Range range = rocksdb::Range({reinterpret_cast<const char *>(buf_begin), INDEX_NUMBER_SIZE},
                                            {reinterpret_cast<const char *>(buf_end), INDEX_NUMBER_SIZE});
      rocksdb::ColumnFamilyHandle *cfh = cf_manager_.get_cf_by_id(id.cf_id);
      DEBUG_ASSERT(cfh);
      // delete files by range, [start_index, end_index]
      // for more info: http://rocksdb.org/blog/2018/11/21/delete-range.html
      rocksdb::Status status = rocksdb::DeleteFilesInRange(txn_db_->GetBaseDB(), cfh, &range.start, &range.limit);
      if (!status.ok()) {
        TIANMU_LOG(LogCtl_Level::ERROR, "RocksDB: delete file range fail, status: %s ", status.ToString().c_str());
        if (status.IsShutdownInProgress()) {
          break;
        }
      }
      // star to do compaction.
      status = txn_db_->CompactRange(options, cfh, &range.start, &range.limit);
      if (!status.ok()) {
        TIANMU_LOG(LogCtl_Level::ERROR, "RocksDB: Compact range index fail, status: %s ", status.ToString().c_str());
        if (status.IsShutdownInProgress()) {
          break;
        }
      }
    }

    dict_manager_.finish_indexes(del_index_ids, MetaType::DDL_DROP_INDEX_ONGOING);
  }

  TIANMU_LOG(LogCtl_Level::INFO, "KVStore drop Table thread exiting...");
}

common::ErrorCode KVStore::KVWriteTableMeta(std::shared_ptr<RdbTable> tbl) {
  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager_.begin();
  rocksdb::WriteBatch *const batch = wb.get();

  dict_manager_.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager_.unlock(); });

  // writes the tbl defs in batch.
  ddl_manager_.put_and_write(tbl, batch);
  if (!dict_manager_.commit(batch)) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Commit table metadata fail!");
    return common::ErrorCode::FAILED;
  }

  return common::ErrorCode::SUCCESS;
}

common::ErrorCode KVStore::KVDelTableMeta(const std::string &tablename) {
  //  Find the table in the hash
  std::shared_ptr<RdbTable> tbl = ddl_manager_.find(tablename);
  if (!tbl)
    return common::ErrorCode::FAILED;

  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager_.begin();
  rocksdb::WriteBatch *const batch = wb.get();

  dict_manager_.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager_.unlock(); });

  // Remove the table entry in data dictionary (this will also remove it from
  // the persistent data dictionary).
  dict_manager_.add_drop_table(tbl->GetRdbTableKeys(), batch);
  ddl_manager_.remove(tbl, batch);
  if (!dict_manager_.commit(batch)) {
    return common::ErrorCode::FAILED;
  }

  // notify Delete data signal.
  DeleteDataSignal();

  return common::ErrorCode::SUCCESS;
}

common::ErrorCode KVStore::KVRenameTableMeta(const std::string &s_name, const std::string &d_name) {
  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager_.begin();
  rocksdb::WriteBatch *const batch = wb.get();

  dict_manager_.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager_.unlock(); });

  // start to rename.
  if (!ddl_manager_.rename(s_name, d_name, batch)) {
    TIANMU_LOG(LogCtl_Level::ERROR, "rename table %s not exsit", s_name.c_str());
    return common::ErrorCode::FAILED;
  }

  return dict_manager_.commit(batch) ? common::ErrorCode::SUCCESS : common::ErrorCode::FAILED;
}

common::ErrorCode KVStore::KVWriteDeltaMeta(std::shared_ptr<core::DeltaTable> delta) {
  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager_.begin();
  rocksdb::WriteBatch *const batch = wb.get();

  dict_manager_.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager_.unlock(); });

  // put the tb_mem into mem cache and stores into dict data.
  ddl_manager_.put_delta(delta, batch);
  if (!dict_manager_.commit(batch)) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Commit memory table metadata fail!");
    return common::ErrorCode::FAILED;
  }

  return common::ErrorCode::SUCCESS;
}

common::ErrorCode KVStore::KVDelDeltaMeta(std::string table_name) {
  std::shared_ptr<core::DeltaTable> delta_table = ddl_manager_.find_delta(table_name);
  if (!delta_table)
    return common::ErrorCode::FAILED;

  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager_.begin();
  rocksdb::WriteBatch *const batch = wb.get();

  dict_manager_.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager_.unlock(); });

  std::vector<GlobalId> dropped_index_ids;
  GlobalId gid;
  // gets column family id.
  gid.cf_id = delta_table->GetCFHandle()->GetID();
  // gets index id.
  gid.index_id = delta_table->GetDeltaTableID();
  dropped_index_ids.push_back(gid);
  dict_manager_.add_drop_index(dropped_index_ids, batch);
  // removes from mem cache and dict data.
  ddl_manager_.remove_delta(delta_table, batch);
  if (!dict_manager_.commit(batch))
    return common::ErrorCode::FAILED;

  // notify the drop data signal.
  DeleteDataSignal();

  return common::ErrorCode::SUCCESS;
}

common::ErrorCode KVStore::KVRenameDeltaMeta(std::string s_name, std::string d_name) {
  const std::unique_ptr<rocksdb::WriteBatch> wb = dict_manager_.begin();
  rocksdb::WriteBatch *const batch = wb.get();

  dict_manager_.lock();
  std::shared_ptr<void> defer(nullptr, [this](...) { dict_manager_.unlock(); });
  // rename the memtable.
  if (!ddl_manager_.rename_delta(s_name, d_name, batch)) {
    TIANMU_LOG(LogCtl_Level::ERROR, "rename table %s failed", s_name.c_str());
    return common::ErrorCode::FAILED;
  }
  if (!dict_manager_.commit(batch))
    return common::ErrorCode::FAILED;

  return common::ErrorCode::SUCCESS;
}

bool KVStore::KVDeleteKey(rocksdb::WriteOptions &wopts, rocksdb::ColumnFamilyHandle *cf, rocksdb::Slice &key) {
  rocksdb::Status s = txn_db_->Delete(wopts, cf, key);
  if (!s.ok()) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Rdb delete key fail: %s", s.ToString().c_str());
    return false;
  }

  return true;
}

bool KVStore::KVWriteBatch(rocksdb::WriteOptions &wopts, rocksdb::WriteBatch *batch) {
  const rocksdb::Status s = txn_db_->Write(wopts, batch);
  if (!s.ok()) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Rdb write batch fail: %s", s.ToString().c_str());
    return false;
  }

  return true;
}

std::string KVStore::generate_cf_name(uint index, TABLE *table) {
  char *comment = table->key_info[index].comment.str;
  std::string key_comment = comment ? comment : "";
  std::string cf_name = RdbKey::parse_comment(key_comment);
  if (cf_name.empty() && !key_comment.empty())
    return key_comment;

  return cf_name;
}
void KVStore::create_rdbkey(TABLE *table, uint pos, std::shared_ptr<RdbKey> &new_key_def,
                            rocksdb::ColumnFamilyHandle *cf_handle) {
  // assign a new id for this index.
  uint index_id = ha_kvstore_->GetNextIndexId();

  std::vector<ColAttr> vcols;
  KEY *key_info = &table->key_info[pos];
  bool unsigned_flag;

  for (uint n = 0; n < key_info->actual_key_parts; n++) {
    Field *f = key_info->key_part[n].field;
    switch (f->type()) {
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_TINY: {
        unsigned_flag = ((Field_num *)f)->unsigned_flag;

      } break;
      default:
        unsigned_flag = false;
        break;
    }
    vcols.emplace_back(ColAttr{key_info->key_part[n].field->field_index, f->type(), unsigned_flag});
  }

  const char *const key_name = table->key_info[pos].name;
  ///* primary_key: Primary key index number(aka:pos), used in TABLE::key_info[] */
  uchar index_type = (pos == table->s->primary_key) ? static_cast<uchar>(IndexType::INDEX_TYPE_PRIMARY)
                                                    : static_cast<uchar>(IndexType::INDEX_TYPE_SECONDARY);
  uint16_t index_ver = (key_info->actual_key_parts > 1)
                           ? static_cast<uint16_t>(IndexInfoType::INDEX_INFO_VERSION_COLS)
                           : static_cast<uint16_t>(IndexInfoType::INDEX_INFO_VERSION_INITIAL);

  new_key_def = std::make_shared<RdbKey>(index_id, pos, cf_handle, index_ver, index_type, false, key_name, vcols);
}

common::ErrorCode KVStore::create_keys_and_cf(TABLE *table, std::shared_ptr<RdbTable> rdb_tbl) {
  // processing the all indexes in order.
  for (uint pos = 0; pos < rdb_tbl->GetRdbTableKeys().size(); pos++) {
    // gens the column family(cf) name of ith key.
    std::string cf_name = generate_cf_name(pos, table);
    if (cf_name == DEFAULT_SYSTEM_CF_NAME)
      throw common::Exception("column family not valid for storing index data. cf: " + DEFAULT_SYSTEM_CF_NAME);

    // isnot default cf, then get the cf name.
    rocksdb::ColumnFamilyHandle *cf_handle = ha_kvstore_->GetCfHandle(cf_name);

    if (!cf_handle) {
      return common::ErrorCode::FAILED;
    }
    // create the ith index key with cf.
    create_rdbkey(table, pos, rdb_tbl->GetRdbTableKeys().at(pos), cf_handle);
  }

  return common::ErrorCode::SUCCESS;
}

uint KVStore::pk_index(const TABLE *const table, std::shared_ptr<RdbTable> tbl_def) {
  return table->s->primary_key == MAX_INDEXES ? tbl_def->GetRdbTableKeys().size() - 1 : table->s->primary_key;
}

bool IndexCompactFilter::Filter([[maybe_unused]] int level, const rocksdb::Slice &key,
                                [[maybe_unused]] const rocksdb::Slice &existing_value,
                                [[maybe_unused]] std::string *new_value, [[maybe_unused]] bool *value_changed) const {
  GlobalId gl_index_id;
  gl_index_id.cf_id = cf_id_;
  gl_index_id.index_id = be_to_uint32(reinterpret_cast<const uchar *>(key.data()));

  if (gl_index_id.index_id != prev_index_.index_id) {
    should_delete_ = ha_kvstore_->IndexDroping(gl_index_id);
    prev_index_ = gl_index_id;
  }

  if (should_delete_) {
    return true;
  }

  return false;
}

}  // namespace index
}  // namespace Tianmu
