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

#include "core/delta_store.h"
#include "common/common_definitions.h"
#include "core/table_share.h"
#include "core/tianmu_table.h"
#include "core/transaction.h"
#include "index/kv_store.h"
#include "index/kv_transaction.h"
#include "index/rdb_meta_manager.h"

namespace Tianmu {
namespace core {
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
  uchar entry_key[32];
  size_t entry_pos = 0;
  index::be_store_index(entry_key + entry_pos, delta_tid_);
  entry_pos += sizeof(uint32_t);
  rocksdb::Slice entry_slice((char *)entry_key, entry_pos);
  rocksdb::ReadOptions ropts;
  std::unique_ptr<rocksdb::Iterator> iter(kv_trans.GetDataIterator(ropts, cf_handle_));
  iter->Seek(entry_slice);

  next_row_id.store(base_row_num);
  if (iter->Valid() && iter->key().starts_with(entry_slice)) {
    load_id++;
    if (static_cast<RecordType>(iter->value()[0]) == RecordType::kInsert) {
      next_row_id++;
    }
    iter->Next();
  }
  stat.write_cnt.store(load_id);
}

void DeltaTable::AddInsertRecord(uint64_t row_id, std::unique_ptr<char[]> buf, uint32_t size) {
  uchar key[32];
  size_t key_pos = 0;
  index::KVTransaction kv_trans;
  // table id
  index::be_store_index(key + key_pos, delta_tid_);
  key_pos += sizeof(uint32_t);
  // row id
  index::be_store_uint64(key + key_pos, row_id);
  key_pos += sizeof(uint64_t);

  kv_trans.PutData(cf_handle_, {(char *)key, key_pos}, {buf.get(), size});
  kv_trans.Commit();
  load_id++;
  stat.write_cnt++;
  stat.write_bytes += size;
}

void DeltaTable::AddUpdateRecord(uint64_t row_id, std::unique_ptr<char[]> buf, uint32_t size) {
  uchar key[32];
  size_t key_pos = 0;
  index::KVTransaction kv_trans;
  // table id
  index::be_store_index(key + key_pos, delta_tid_);
  key_pos += sizeof(uint32_t);
  // row id
  index::be_store_uint64(key + key_pos, row_id);
  key_pos += sizeof(uint64_t);

  kv_trans.PutData(cf_handle_, {(char *)key, key_pos}, {buf.get(), size});
  kv_trans.Commit();
  load_id++;
  stat.write_cnt++;
  stat.write_bytes += size;
}

void DeltaTable::Truncate(Transaction *tx) {
  ASSERT(tx, "No truncate transaction.");
  uchar entry_key[32];
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


}  // namespace core
}  // namespace Tianmu
