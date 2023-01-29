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
    throw common::Exception("Normalized rowstore name failed " + table_name);
    return nullptr;
  }
  std::shared_ptr<DeltaTable> delta = ha_kvstore_->FindDeltaTable(normalized_name);
  if (delta)
    return delta;

  if (cf_prefix == index::DEFAULT_SYSTEM_CF_NAME)
    throw common::Exception("Insert rowstore name should not be " + index::DEFAULT_SYSTEM_CF_NAME);
  std::string cf_name =
      cf_prefix.empty() ? index::DEFAULT_ROWSTORE_NAME : index::DEFAULT_DELTA_STORE_PREFIX + cf_prefix;
  uint32_t cf_id = ha_kvstore_->GetCfHandle(cf_name)->GetID();
  uint32_t delta_id = ha_kvstore_->GetNextIndexId();
  delta = std::make_shared<DeltaTable>(normalized_name, delta_id, cf_id);
  ha_kvstore_->KVWriteDeltaMeta(delta);
  TIANMU_LOG(LogCtl_Level::INFO, "Create RowStore: %s, CF ID: %d, RowStore ID: %u", normalized_name.c_str(), cf_id,
             delta_id);

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
  uchar entry_key[12];
  size_t entry_pos = 0;
  index::be_store_index(entry_key + entry_pos, delta_tid_);
  entry_pos += sizeof(uint32_t);
  rocksdb::Slice entry_slice((char *)entry_key, entry_pos);
  rocksdb::ReadOptions ropts;
  std::unique_ptr<rocksdb::Iterator> iter(kv_trans.GetDataIterator(ropts, cf_handle_));
  iter->Seek(entry_slice);

  next_row_id.store(base_row_num);
  if (iter->Valid()) {
    load_id++;
    if (static_cast<RecordType>(iter->value()[0]) == RecordType::kInsert) {
      next_row_id++;
    }
    iter->Next();
  }
  stat.write_cnt.store(load_id);
}

void DeltaTable::AddInsertRecord(uint64_t row_id, std::unique_ptr<char[]> buf, uint32_t size) {
  uchar key[12];
  size_t key_pos = 0;
  index::KVTransaction kv_trans;
  // table id
  index::be_store_index(key + key_pos, delta_tid_);
  key_pos += sizeof(uint32_t);
  // row id
  index::be_store_uint64(key + key_pos, row_id);
  key_pos += sizeof(uint64_t);

  rocksdb::Status status = kv_trans.PutData(cf_handle_, {(char *)key, key_pos}, {buf.get(), size});
  if (!status.ok()) {
    throw common::Exception("Error,kv_trans.PutData failed,date size: " + std::to_string(size)+ " date:" + std::string(buf.get()));
  }
  kv_trans.Commit();
  load_id++;
  stat.write_cnt++;
  stat.write_bytes += size;
}

void DeltaTable::AddRecord(Transaction *tx, uint64_t row_id, std::unique_ptr<char[]> buf, uint32_t size) {
  uchar key[12];
  size_t key_pos = 0;
  index::KVTransaction &kv_trans=tx->KVTrans();
  // table id
  index::be_store_index(key + key_pos, delta_tid_);
  key_pos += sizeof(uint32_t);
  // row id
  index::be_store_uint64(key + key_pos, row_id);
  key_pos += sizeof(uint64_t);

  rocksdb::Status status = kv_trans.MergeData(cf_handle_, {(char *)key, key_pos}, {buf.get(), size});
  if (!status.ok()) {
    throw common::Exception("Error,kv_trans.PutData failed,date size: " + std::to_string(size)+ " date:" + std::string(buf.get()));
  }
  load_id++;
  stat.write_cnt++;
  stat.write_bytes += size;
}

void DeltaTable::Truncate(Transaction *tx) {
  ASSERT(tx, "No truncate transaction.");
  // todo(dfx): just use DropColumnFamily() and CreateColumnFamily(), maybe faster
  uchar entry_key[12];
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
  next_row_id.store(0);
  load_id.store(0);
  merge_id.store(0);
  stat.write_cnt.store(0);
  stat.write_bytes.store(0);
  stat.read_cnt.store(0);
  stat.read_bytes.store(0);

  return;
}

/// DeltaIterator

DeltaIterator::DeltaIterator(DeltaTable *table, const std::vector<bool> &attrs) : table_(table), attrs_(attrs) {
  // get snapshot for rocksdb, snapshot will release when DeltaIterator is destructured
  auto snapshot = ha_kvstore_->GetRdbSnapshot();
  rocksdb::ReadOptions read_options(true, snapshot);
  it_ = std::unique_ptr<rocksdb::Iterator>(ha_kvstore_->GetRdb()->NewIterator(read_options, table_->GetCFHandle()));
  uchar entry_key[12];
  size_t key_pos = 0;
  uint32_t table_id = table_->GetDeltaTableID();
  index::be_store_index(entry_key + key_pos, table_id);
  key_pos += sizeof(uint32_t);
  rocksdb::Slice entry_slice((char *)entry_key, key_pos);
  // ==== for debug ====
  it_->Seek(entry_slice);
  while (it_->Valid()) {
    auto row_id = GetCurrRowIdFromRecord();
    TIANMU_LOG(LogCtl_Level::DEBUG, " this table id: %d, row id: %d, type: %d, this record value: %s", table_id, row_id,
               static_cast<RecordType>(it_->value().data()[0]), it_->value());
    it_->Next();
  }
  // ==== for debug ====
  it_->Seek(entry_slice);
  while (it_->Valid() && !IsCurrInsertType()) {
    it_->Next();
  }
  if (it_->Valid()) {
    position_ = GetCurrRowIdFromRecord();
    start_position_ = position_;
  } else {
    position_ = -1;
  }
}

DeltaIterator::DeltaIterator(DeltaIterator &&other) noexcept {
  table_ = other.table_;
  position_ = other.position_;
  start_position_ = other.start_position_;
  current_record_fetched_ = other.current_record_fetched_;
  it_ = std::move(other.it_);
  record_ = std::move(other.record_);
  attrs_ = std::move(other.attrs_);
}

DeltaIterator &DeltaIterator::operator=(DeltaIterator &&other) noexcept {
  table_ = other.table_;
  position_ = other.position_;
  start_position_ = other.start_position_;
  current_record_fetched_ = other.current_record_fetched_;
  it_ = std::move(other.it_);
  record_ = std::move(other.record_);
  attrs_ = std::move(other.attrs_);
  return *this;
}

bool DeltaIterator::operator==(const DeltaIterator &other) {
  return table_->GetDeltaTableID() == other.table_->GetDeltaTableID() && position_ == other.position_;
}

bool DeltaIterator::operator!=(const DeltaIterator &other) { return !(*this == other); }

void DeltaIterator::operator++(int) {
  it_->Next();
  while (it_->Valid() && !IsCurrInsertType()) {
    it_->Next();
  }
  if (it_->Valid()) {
    DEBUG_ASSERT(IsCurrInsertType());
    position_ = GetCurrRowIdFromRecord();
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

void DeltaIterator::MoveTo(int64_t row_id) {
  uchar key[12];
  size_t key_pos = 0;
  // table id
  index::be_store_index(key + key_pos, table_->GetDeltaTableID());
  key_pos += sizeof(uint32_t);
  // row id
  index::be_store_uint64(key + key_pos, row_id);
  key_pos += sizeof(uint64_t);
  it_->Seek({(char *)key, key_pos});
  if (it_->Valid()) {  // need check valid
    DEBUG_ASSERT(GetCurrRowIdFromRecord() == row_id);
    position_ = GetCurrRowIdFromRecord();
  } else {
    position_ = -1;
  }
  current_record_fetched_ = false;
}

bool DeltaIterator::IsCurrInsertType() {
  return static_cast<RecordType>(it_->value().data()[0]) == RecordType::kInsert;
}

uint64_t DeltaIterator::GetCurrRowIdFromRecord() {
  return index::be_to_uint64(reinterpret_cast<const uchar *>(it_->key().data()) + sizeof(uint32_t));
}

}  // namespace core
}  // namespace Tianmu
