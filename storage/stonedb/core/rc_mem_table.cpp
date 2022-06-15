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

#include "core/rc_mem_table.h"
#include "common/common_definitions.h"
#include "core/rc_table.h"
#include "core/table_share.h"
#include "core/transaction.h"
#include "index/kv_store.h"
#include "index/kv_transaction.h"
#include "index/rdb_meta_manager.h"

namespace stonedb {
namespace core {
RCMemTable::RCMemTable(const std::string name, const uint32_t mem_id, const uint32_t cf_id)
    : fullname_(name), mem_id_(mem_id), cf_handle_(kvstore->GetCfHandleByID(cf_id)) {
  ASSERT(cf_handle_, "column family handle not exist " + name);

  index::KVTransaction kv_trans;
  uchar entry_key[32];
  size_t entry_pos = 0;
  index::be_store_index(entry_key + entry_pos, mem_id_);
  entry_pos += sizeof(uint32_t);
  index::be_store_byte(entry_key + entry_pos, static_cast<uchar>(RecordType::kInsert));
  entry_pos += sizeof(uchar);
  rocksdb::Slice entry_slice((char *)entry_key, entry_pos);

  uchar upper_key[32];
  size_t upper_pos = 0;
  index::be_store_index(upper_key + upper_pos, mem_id_);
  upper_pos += sizeof(uint32_t);
  uchar next_key = static_cast<int>(RecordType::kInsert) + 1;
  index::be_store_byte(upper_key + upper_pos, next_key);
  upper_pos += sizeof(uchar);
  rocksdb::Slice upper_slice((char *)upper_key, upper_pos);

  rocksdb::ReadOptions ropts;
  ropts.iterate_upper_bound = &upper_slice;
  std::unique_ptr<rocksdb::Iterator> iter(kv_trans.GetDataIterator(ropts, cf_handle_));
  iter->Seek(entry_slice);

  if (iter->Valid() && iter->key().starts_with(entry_slice)) {
    next_load_id_ = index::be_to_uint64((uchar *)iter->key().data() + entry_pos);
    iter->SeekToLast();
    next_insert_id_ = index::be_to_uint64((uchar *)iter->key().data() + entry_pos) + 1;
  } else {
    next_load_id_ = next_insert_id_ = 0;
  }
  stat.write_cnt = next_insert_id_.load() - next_load_id_.load();
}

std::shared_ptr<RCMemTable> RCMemTable::CreateMemTable(std::shared_ptr<TableShare> share, const std::string mem_name) {
  std::string table_name = share->Path();
  std::string normalized_name;
  if (!index::NormalizeName(table_name, normalized_name)) {
    throw common::Exception("Normalized rowstore name failed " + table_name);
    return nullptr;
  }
  std::shared_ptr<RCMemTable> tb_mem = kvstore->FindMemTable(normalized_name);
  if (tb_mem) return tb_mem;

  if (mem_name == index::DEFAULT_SYSTEM_CF_NAME)
    throw common::Exception("Insert rowstore name should not be " + index::DEFAULT_SYSTEM_CF_NAME);
  std::string cf_name = mem_name.empty() ? index::DEFAULT_ROWSTORE_NAME : index::DEFAULT_ROWSTORE_PREFIX + mem_name;
  uint32_t cf_id = kvstore->GetCfHandle(cf_name)->GetID();
  uint32_t mem_id = kvstore->GetNextIndexId();
  tb_mem = std::make_shared<RCMemTable>(normalized_name, mem_id, cf_id);
  kvstore->KVWriteMemTableMeta(tb_mem);
  STONEDB_LOG(LogCtl_Level::INFO, "Create RowStore: %s, CF ID: %d, RowStore ID: %u", normalized_name.c_str(), cf_id,
              mem_id);

  return tb_mem;
}

common::ErrorCode RCMemTable::Rename(const std::string &to) {
  std::string dname;
  if (!index::NormalizeName(to, dname)) {
    return common::ErrorCode::FAILED;
  }
  if (kvstore->FindMemTable(dname)) {
    return common::ErrorCode::FAILED;
  }

  kvstore->KVRenameMemTableMeta(fullname_, dname);
  fullname_ = dname;
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode RCMemTable::DropMemTable(std::string table_name) {
  std::string normalized_name;
  if (!index::NormalizeName(table_name, normalized_name)) {
    throw common::Exception("Normalized memtable name failed " + table_name);
    return common::ErrorCode::FAILED;
  }
  auto tb_mem = kvstore->FindMemTable(normalized_name);
  if (!tb_mem) return common::ErrorCode::SUCCESS;

  STONEDB_LOG(LogCtl_Level::INFO, "Dropping RowStore: %s, CF ID: %d, RowStore ID: %u", normalized_name.c_str(),
              tb_mem->GetCFHandle()->GetID(), tb_mem->GetMemID());
  return kvstore->KVDelMemTableMeta(normalized_name);
}

void RCMemTable::InsertRow(std::unique_ptr<char[]> buf, uint32_t size) {
  // insert rowset data
  int64_t row_id = next_insert_id_++;
  if (row_id < next_load_id_) next_load_id_ = row_id;

  uchar key[32];
  size_t key_pos = 0;
  index::KVTransaction kv_trans;
  index::be_store_index(key + key_pos, mem_id_);
  key_pos += sizeof(uint32_t);
  index::be_store_byte(key + key_pos, static_cast<uchar>(RecordType::kInsert));
  key_pos += sizeof(uchar);
  index::be_store_uint64(key + key_pos, row_id);
  key_pos += sizeof(uint64_t);
  kv_trans.PutData(cf_handle_, {(char *)key, key_pos}, {buf.get(), size});
  kv_trans.Commit();
  stat.write_cnt++;
  stat.write_bytes += size;
}

void RCMemTable::Truncate(Transaction *tx) {
  ASSERT(tx, "No truncate transaction.");
  uchar entry_key[32];
  size_t key_pos = 0;
  index::be_store_index(entry_key + key_pos, mem_id_);
  key_pos += sizeof(uint32_t);
  index::be_store_byte(entry_key + key_pos, static_cast<uchar>(RCMemTable::RecordType::kInsert));
  key_pos += sizeof(uchar);
  rocksdb::Slice entry_slice((char *)entry_key, key_pos);

  uchar upper_key[32];
  size_t upper_pos = 0;
  index::be_store_index(upper_key + upper_pos, mem_id_);
  upper_pos += sizeof(uint32_t);
  uchar upkey = static_cast<int>(RCMemTable::RecordType::kInsert) + 1;
  index::be_store_byte(upper_key + upper_pos, upkey);
  upper_pos += sizeof(uchar);
  rocksdb::Slice upper_slice((char *)upper_key, upper_pos);

  rocksdb::ReadOptions ropts;
  ropts.iterate_upper_bound = &upper_slice;
  std::unique_ptr<rocksdb::Iterator> iter(tx->KVTrans().GetDataIterator(ropts, cf_handle_));
  iter->Seek(entry_slice);
  while (iter->Valid()) {
    tx->KVTrans().SingleDeleteData(cf_handle_, iter->key());
    iter->Next();
  }
  next_load_id_.store(0);
  next_insert_id_.store(0);
  stat.write_cnt.store(0);
  stat.write_bytes.store(0);
  stat.read_cnt.store(0);
  stat.read_bytes.store(0);

  return;
}
}  // namespace core
}  // namespace stonedb
