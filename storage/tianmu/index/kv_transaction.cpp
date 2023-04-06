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
#include "kv_transaction.h"

namespace Tianmu {
namespace index {

KVTransaction::~KVTransaction() {
  Releasesnapshot();
  index_batch_->Clear();
  data_batch_->Clear();
}

rocksdb::Status KVTransaction::Get(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                                   std::string *value) {
  read_opts_.total_order_seek = false;
  return index_batch_->GetFromBatchAndDB(ha_kvstore_->GetRdb(), read_opts_, column_family, key, value);
}

rocksdb::Status KVTransaction::Put(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                                   const rocksdb::Slice &value) {
  return index_batch_->Put(column_family, key, value);
}

rocksdb::Status KVTransaction::Delete(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key) {
  return index_batch_->Delete(column_family, key);
}

rocksdb::Iterator *KVTransaction::GetIterator(rocksdb::ColumnFamilyHandle *const column_family, bool skip_filter) {
  if (skip_filter) {
    read_opts_.total_order_seek = true;
  } else {
    read_opts_.total_order_seek = false;
    read_opts_.prefix_same_as_start = true;
  }

  return index_batch_->NewIteratorWithBase(ha_kvstore_->GetRdb()->NewIterator(read_opts_, column_family));
}

rocksdb::Status KVTransaction::GetData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                                       std::string *value) {
  read_opts_.total_order_seek = false;
  return ha_kvstore_->GetRdb()->Get(read_opts_, column_family, key, value);
}

rocksdb::Status KVTransaction::PutData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                                       const rocksdb::Slice &value) {
  return data_batch_->Put(column_family, key, value);
}

rocksdb::Status KVTransaction::MergeData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                                         const rocksdb::Slice &value) {
  return data_batch_->Merge(column_family, key, value);
}

rocksdb::Status KVTransaction::SingleDeleteData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key) {
  // notice: if a key is overwritten (by calling Put() multiple times), then the
  // result of calling SingleDelete() on this key is undefined, delete is better
  return data_batch_->SingleDelete(column_family, key);
}

rocksdb::Iterator *KVTransaction::GetDataIterator(rocksdb::ReadOptions &ropts,
                                                  rocksdb::ColumnFamilyHandle *const column_family) {
  return ha_kvstore_->GetRdb()->NewIterator(ropts, column_family);
}

void KVTransaction::Acquiresnapshot() {
  if (read_opts_.snapshot == nullptr)
    read_opts_.snapshot = ha_kvstore_->GetRdbSnapshot();
}

void KVTransaction::Releasesnapshot() {
  if (read_opts_.snapshot != nullptr) {
    ha_kvstore_->ReleaseRdbSnapshot(read_opts_.snapshot);
    read_opts_.snapshot = nullptr;
  }
}

bool KVTransaction::Commit() {
  bool res = true;
  // firstly, release the snapshot.
  Releasesnapshot();
  rocksdb::WriteOptions write_opts;
  // if we have data to commit, then do writing index data ops by KVWriteBatch.
  auto index_write_batch = index_batch_->GetWriteBatch();
  if (index_write_batch && index_write_batch->Count() > 0 &&
      !ha_kvstore_->KVWriteBatch(write_opts, index_write_batch)) {
    // write failed.
    res = false;
  }
  // write the data.
  if (res && data_batch_->Count() > 0 && !ha_kvstore_->KVWriteBatch(write_opts, data_batch_.get())) {
    // write failed.
    res = false;
  }

  // writes the data sucessfully, then clean up xxx_batch.
  index_batch_->Clear();
  data_batch_->Clear();
  return res;
}

void KVTransaction::Rollback() {
  Releasesnapshot();
  index_batch_->Clear();
  data_batch_->Clear();
}

void KVTransaction::ResetWriteBatch() {
  index_batch_ = nullptr;
  data_batch_ = nullptr;

  index_batch_ = std::make_unique<rocksdb::WriteBatchWithIndex>(rocksdb::BytewiseComparator(), 0, true);
  data_batch_ = std::make_unique<rocksdb::WriteBatch>();
}
}  // namespace index
}  // namespace Tianmu
