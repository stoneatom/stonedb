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

#include "index/kv_transaction.h"

#include "common/common_definitions.h"
#include "index/kv_store.h"

namespace stonedb {
namespace index {

KVTransaction::~KVTransaction() {
  Releasesnapshot();
  m_index_batch->Clear();
  m_data_batch->Clear();
}

rocksdb::Status KVTransaction::Get(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                                   std::string *value) {
  m_read_opts.total_order_seek = false;
  return m_index_batch->GetFromBatchAndDB(kvstore->GetRdb(), m_read_opts, column_family, key, value);
}

rocksdb::Status KVTransaction::Put(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                                   const rocksdb::Slice &value) {
  m_index_batch->Put(column_family, key, value);
  return rocksdb::Status::OK();
}

rocksdb::Status KVTransaction::Delete(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key) {
  m_index_batch->Delete(column_family, key);
  return rocksdb::Status::OK();
}

rocksdb::Iterator *KVTransaction::GetIterator(rocksdb::ColumnFamilyHandle *const column_family, bool skip_filter) {
  if (skip_filter) {
    m_read_opts.total_order_seek = true;
  } else {
    m_read_opts.total_order_seek = false;
    m_read_opts.prefix_same_as_start = true;
  }
  return m_index_batch->NewIteratorWithBase(kvstore->GetRdb()->NewIterator(m_read_opts, column_family));
}

rocksdb::Status KVTransaction::GetData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                                       std::string *value) {
  m_read_opts.total_order_seek = false;
  return kvstore->GetRdb()->Get(m_read_opts, column_family, key, value);
}

rocksdb::Status KVTransaction::PutData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                                       const rocksdb::Slice &value) {
  m_data_batch->Put(column_family, key, value);
  return rocksdb::Status::OK();
}

rocksdb::Status KVTransaction::SingleDeleteData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key) {
  // notice: if a key is overwritten (by calling Put() multiple times), then the
  // result of calling SingleDelete() on this key is undefined, delete is better
  m_data_batch->SingleDelete(column_family, key);
  return rocksdb::Status::OK();
}

rocksdb::Iterator *KVTransaction::GetDataIterator(rocksdb::ReadOptions &ropts,
                                                  rocksdb::ColumnFamilyHandle *const column_family) {
  return kvstore->GetRdb()->NewIterator(ropts, column_family);
}

void KVTransaction::Acquiresnapshot() {
  if (m_read_opts.snapshot == nullptr) m_read_opts.snapshot = kvstore->GetRdbSnapshot();
}

void KVTransaction::Releasesnapshot() {
  if (m_read_opts.snapshot != nullptr) {
    kvstore->ReleaseRdbSnapshot(m_read_opts.snapshot);
    m_read_opts.snapshot = nullptr;
  }
}

bool KVTransaction::Commit() {
  bool res = true;
  Releasesnapshot();
  auto index_write_batch = m_index_batch->GetWriteBatch();
  if (index_write_batch && index_write_batch->Count() > 0 && !kvstore->KVWriteBatch(write_opts, index_write_batch)) {
    res = false;
  }
  if (res && m_data_batch->Count() > 0 && !kvstore->KVWriteBatch(write_opts, m_data_batch.get())) {
    res = false;
  }
  m_index_batch->Clear();
  m_data_batch->Clear();
  return res;
}

void KVTransaction::Rollback() {
  Releasesnapshot();
  m_index_batch->Clear();
  m_data_batch->Clear();
}

}  // namespace index
}  // namespace stonedb
