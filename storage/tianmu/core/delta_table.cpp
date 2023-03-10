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

#include "core/delta_table.h"
#include "core/table_share.h"
#include "core/transaction.h"
#include "delta_table.h"
#include "index/kv_transaction.h"
#include "index/rdb_meta_manager.h"

namespace Tianmu {
namespace core {

/// DeltaTable

DeltaTable::DeltaTable(const std::string &name, uint32_t delta_id, uint32_t cf_id)
    : fullname_(name), delta_tid_(delta_id), cf_handle_(ha_kvstore_->GetCfHandleByID(cf_id)) {
  ASSERT(cf_handle_, "column family handle not exist " + name);
}

std::shared_ptr<DeltaTable> DeltaTable::CreateDeltaTable(const std::shared_ptr<TableShare> &share,
                                                         const std::string &cf_prefix) {
  std::string table_name = share->Path();
  std::string normalized_name;
  if (!index::NormalizeName(table_name, normalized_name)) {
    throw common::Exception("Normalized Delta Store name failed " + table_name);
    return nullptr;
  }
  std::shared_ptr<DeltaTable> delta = ha_kvstore_->FindDeltaTable(normalized_name);
  if (delta)
    return delta;

  if (cf_prefix == index::DEFAULT_SYSTEM_CF_NAME)
    throw common::Exception("Insert Delta Store name should not be " + index::DEFAULT_SYSTEM_CF_NAME);
  std::string cf_name =
      cf_prefix.empty() ? index::DEFAULT_ROWSTORE_NAME : index::DEFAULT_DELTA_STORE_PREFIX + cf_prefix;
  uint32_t cf_id = ha_kvstore_->GetCfHandle(cf_name)->GetID();
  uint32_t delta_id = ha_kvstore_->GetNextIndexId();
  delta = std::make_shared<DeltaTable>(normalized_name, delta_id, cf_id);
  ha_kvstore_->KVWriteDeltaMeta(delta);
  TIANMU_LOG(LogCtl_Level::INFO, "Create Delta Store: %s, CF ID: %d, Delta Store ID: %u", normalized_name.c_str(),
             cf_id, delta_id);

  return delta;
}

common::ErrorCode DeltaTable::Rename(const std::string &to) {
  std::string dname;
  if (!index::NormalizeName(to, dname)) {
    return common::ErrorCode::FAILED;
  }
  if (ha_kvstore_->FindDeltaTable(dname)) {
    return common::ErrorCode::FAILED;
  }

  ha_kvstore_->KVRenameDeltaMeta(fullname_, dname);
  fullname_ = dname;
  return common::ErrorCode::SUCCESS;
}

void DeltaTable::FillRowByRowid(Transaction *tx, TABLE *table, int64_t obj) {
  uchar key[sizeof(uint32_t) + sizeof(uint64_t)];
  size_t key_pos = 0;
  index::KVTransaction &kv_trans = tx->KVTrans();
  // table id
  index::be_store_index(key + key_pos, delta_tid_);
  key_pos += sizeof(uint32_t);
  // row id
  index::be_store_uint64(key + key_pos, obj);
  key_pos += sizeof(uint64_t);
  std::string delta_record;
  rocksdb::Status status = kv_trans.GetData(cf_handle_, {(char *)key, key_pos}, &delta_record);
  if (!status.ok()) {
    throw common::Exception("Error, kv_trans.GetData failed, key: %s" + std::string((char *)key, key_pos));
  }
  core::Engine::DecodeInsertRecordToField(delta_record.data(), table->field);
}

common::ErrorCode DeltaTable::DropDeltaTable(const std::string &table_name) {
  std::string normalized_name;
  if (!index::NormalizeName(table_name, normalized_name)) {
    throw common::Exception("Normalized memtable name failed " + table_name);
    return common::ErrorCode::FAILED;
  }
  auto delta = ha_kvstore_->FindDeltaTable(normalized_name);
  if (!delta)
    return common::ErrorCode::SUCCESS;

  TIANMU_LOG(LogCtl_Level::INFO, "Dropping RowStore: %s, CF ID: %d, RowStore ID: %u", normalized_name.c_str(),
             delta->GetCFHandle()->GetID(), delta->GetDeltaTableID());
  return ha_kvstore_->KVDelDeltaMeta(normalized_name);
}

void DeltaTable::Init(uint64_t base_row_num) {
  index::KVTransaction kv_trans;
  uchar entry_key[sizeof(uint32_t)];
  index::be_store_index(entry_key, delta_tid_);
  rocksdb::Slice prefix((char *)entry_key, sizeof(uint32_t));
  rocksdb::ReadOptions read_options;
  read_options.total_order_seek = true;
  std::unique_ptr<rocksdb::Iterator> iter(kv_trans.GetDataIterator(read_options, cf_handle_));
  iter->Seek(prefix);
  row_id.store(base_row_num);
  if (iter->Valid() && iter->key().starts_with(prefix)) {
    // row_id
    auto type = *reinterpret_cast<RecordType *>(const_cast<char *>(iter->value().data()));
    if (type == RecordType::kInsert) {
      row_id.fetch_add(1);
    }
    // load_id
    uint32_t load_num = *reinterpret_cast<uint32_t *>(const_cast<char *>(iter->value().data()) + sizeof(RecordType));
    load_id.fetch_add(load_num);
    iter->Next();
  }
}

void DeltaTable::AddInsertRecord(Transaction *tx, uint64_t row_id, std::unique_ptr<char[]> buf, uint32_t size) {
  uchar key[sizeof(uint32_t) + sizeof(uint64_t)];
  size_t key_pos = 0;
  index::KVTransaction &kv_trans = tx->KVTrans();
  // table id
  index::be_store_index(key + key_pos, delta_tid_);
  key_pos += sizeof(uint32_t);
  // row id
  index::be_store_uint64(key + key_pos, row_id);
  key_pos += sizeof(uint64_t);

  rocksdb::Status status = kv_trans.PutData(cf_handle_, {(char *)key, key_pos}, {buf.get(), size});
  if (!status.ok()) {
    throw common::Exception("Error,kv_trans.PutData failed,date size: " + std::to_string(size) +
                            " date:" + std::string(buf.get()));
  }
  tx->AddInsertRowNum();
  if (tx->GetInsertRowNum() >= tianmu_sysvar_insert_write_batch_size) {
    kv_trans.Commit();
    kv_trans.ResetWriteBatch();
    kv_trans.Acquiresnapshot();
    tx->ResetInsertRowNum();
  }
  load_id++;
  stat.write_cnt++;
  stat.write_bytes += size;
}

void DeltaTable::AddRecord(Transaction *tx, uint64_t row_id, std::unique_ptr<char[]> buf, uint32_t size) {
  uchar key[sizeof(uint32_t) + sizeof(uint64_t)];
  size_t key_pos = 0;
  index::KVTransaction &kv_trans = tx->KVTrans();
  // table id
  index::be_store_index(key + key_pos, delta_tid_);
  key_pos += sizeof(uint32_t);
  // row id
  index::be_store_uint64(key + key_pos, row_id);
  key_pos += sizeof(uint64_t);

  rocksdb::Status status = kv_trans.MergeData(cf_handle_, {(char *)key, key_pos}, {buf.get(), size});
  if (!status.ok()) {
    throw common::Exception("Error,kv_trans.PutData failed,date size: " + std::to_string(size) +
                            " date:" + std::string(buf.get()));
  }
  load_id++;
  stat.write_cnt++;
  stat.write_bytes += size;
}

void DeltaTable::Truncate(Transaction *tx) {
  ASSERT(tx, "No truncate transaction.");
  // todo(dfx): just use DropColumnFamily() and CreateColumnFamily(), maybe faster
  uchar entry_key[sizeof(uint32_t) + sizeof(uint64_t)];
  size_t key_pos = 0;
  index::be_store_index(entry_key + key_pos, delta_tid_);
  key_pos += sizeof(uint32_t);
  rocksdb::Slice entry_slice((char *)entry_key, key_pos);
  rocksdb::ReadOptions ropts;
  std::unique_ptr<rocksdb::Iterator> iter(tx->KVTrans().GetDataIterator(ropts, cf_handle_));
  iter->Seek(entry_slice);
  while (iter->Valid()) {
    tx->KVTrans().SingleDeleteData(cf_handle_, iter->key());
    iter->Next();
  }
  row_id.store(0);
  load_id.store(0);
  merge_id.store(0);
  stat.write_cnt.store(0);
  stat.write_bytes.store(0);
  stat.read_cnt.store(0);
  stat.read_bytes.store(0);

  return;
}

bool DeltaTable::BaseRowIsDeleted(Transaction *tx, uint64_t row_id) const {
  uchar key[sizeof(uint32_t) + sizeof(uint64_t)];
  size_t key_pos = 0;
  index::KVTransaction &kv_trans = tx->KVTrans();
  // table id
  index::be_store_index(key + key_pos, delta_tid_);
  key_pos += sizeof(uint32_t);
  // row id
  index::be_store_uint64(key + key_pos, row_id);
  key_pos += sizeof(uint64_t);
  std::string delta_record;
  rocksdb::Status status = kv_trans.GetData(cf_handle_, {(char *)key, key_pos}, &delta_record);
  if (status.IsBusy() && !status.IsDeadlock()) {
    kv_trans.Releasesnapshot();
    kv_trans.Acquiresnapshot();
    status = kv_trans.GetData(cf_handle_, {(char *)key, key_pos}, &delta_record);
  }
  if (status.IsNotFound() || delta_record.empty()) {
    return false;
  } else if (status.ok()) {
    if (DeltaRecordHead::GetRecordType(delta_record.data()) == RecordType::kDelete) {
      return true;
    }
  }
  return false;
}
/// DeltaIterator

DeltaIterator::DeltaIterator(DeltaTable *table, const std::vector<bool> &attrs) : table_(table), attrs_(attrs) {
  // get snapshot for rocksdb, snapshot will release when DeltaIterator is destructured
  auto snapshot = ha_kvstore_->GetRdbSnapshot();
  rocksdb::ReadOptions read_options;
  read_options.total_order_seek = true;
  read_options.snapshot = snapshot;
  it_ = std::unique_ptr<rocksdb::Iterator>(ha_kvstore_->GetRdb()->NewIterator(read_options, table_->GetCFHandle()));
  uchar entry_key[sizeof(uint32_t)];
  uint32_t table_id = table_->GetDeltaTableID();
  index::be_store_index(entry_key, table_id);
  rocksdb::Slice prefix = rocksdb::Slice((char *)entry_key, sizeof(uint32_t));
  it_->Seek(prefix);
  if (RdbKeyValid()) {
    position_ = CurrentRowId();
    start_position_ = position_;
  } else {
    position_ = -1;
  }
}

bool DeltaIterator::operator==(const DeltaIterator &other) {
  return table_->GetDeltaTableID() == other.table_->GetDeltaTableID() && position_ == other.position_;
}

bool DeltaIterator::operator!=(const DeltaIterator &other) { return !(*this == other); }

void DeltaIterator::Next() {
  it_->Next();
  if (RdbKeyValid()) {
    position_ = CurrentRowId();
  } else {
    position_ = -1;
  }
  current_record_fetched_ = false;
}

std::string &DeltaIterator::GetData() {
  record_ = it_->value().ToString();
  current_record_fetched_ = true;
  return record_;
}

void DeltaIterator::SeekTo(int64_t row_id) {
  uchar key[sizeof(uint32_t) + sizeof(uint64_t)];
  size_t key_pos = 0;
  // table id
  index::be_store_index(key + key_pos, table_->GetDeltaTableID());
  key_pos += sizeof(uint32_t);
  // row id
  index::be_store_uint64(key + key_pos, row_id);
  key_pos += sizeof(uint64_t);
  rocksdb::Slice prefix_key{(char *)key, key_pos};
  it_->Seek(prefix_key);
  if (it_->Valid() && it_->key() == prefix_key) {  // need check valid
    position_ = CurrentRowId();
  } else {
    position_ = -1;
  }
  current_record_fetched_ = false;
}

bool DeltaIterator::IsInsertType() { return CurrentType() == RecordType::kInsert && CurrentDeleteFlag() == 'n'; }

bool DeltaIterator::RdbKeyValid() {
  uchar entry_key[sizeof(uint32_t)];
  uint32_t table_id = table_->GetDeltaTableID();
  index::be_store_index(entry_key, table_id);
  rocksdb::Slice prefix = rocksdb::Slice((char *)entry_key, sizeof(uint32_t));
  return it_->Valid() && it_->key().starts_with(prefix);
}

inline RecordType DeltaIterator::CurrentType() { return static_cast<RecordType>(it_->value().data()[0]); }

inline uchar DeltaIterator::CurrentDeleteFlag() {
  return static_cast<uchar>((it_->value().data() + sizeof(RecordType) + sizeof(uint32))[0]);
}

inline uint32_t DeltaIterator::CurrentTableId() {
  return index::be_to_uint32(reinterpret_cast<const uchar *>(it_->key().data()));
}
inline uint64_t DeltaIterator::CurrentRowId() {
  return index::be_to_uint64(reinterpret_cast<const uchar *>(it_->key().data()) + sizeof(uint32_t));
}

}  // namespace core
}  // namespace Tianmu
