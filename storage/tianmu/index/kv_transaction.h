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
#ifndef TIANMU_INDEX_KV_TRANSACTION_H_
#define TIANMU_INDEX_KV_TRANSACTION_H_
#pragma once

#include "index/tianmu_table_index.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace Tianmu {
namespace index {

// key-value transaction based on rocksdb
class KVTransaction {
 public:
  // ctor.
  KVTransaction()
      : index_batch_(std::make_unique<rocksdb::WriteBatchWithIndex>(rocksdb::BytewiseComparator(), 0, true)),
        data_batch_(std::make_unique<rocksdb::WriteBatch>()),
        key_iter_(std::make_shared<KeyIterator>(this)) {}
  // dctor.
  virtual ~KVTransaction();

  // gets a value by key and cf.
  rocksdb::Status Get(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key, std::string *value);
  // stores a (key-value) pair into column_family.
  rocksdb::Status Put(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                      const rocksdb::Slice &value);
  // deletes a value by key from column_family.
  rocksdb::Status Delete(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key);
  // gets a filter of specific colum_family.
  rocksdb::Iterator *GetIterator(rocksdb::ColumnFamilyHandle *const column_family, bool skip_filter);
  // gets the value by key and column_family.
  rocksdb::Status GetData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key, std::string *value);
  // stores the (key-value) to column_family.
  rocksdb::Status PutData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                          const rocksdb::Slice &value);
  // merge the (key-value) to column_family.
  rocksdb::Status MergeData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                            const rocksdb::Slice &value);
  // delete one 'row' by the key.
  rocksdb::Status SingleDeleteData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key);
  // gets the iterator of column_family with specific read options.
  rocksdb::Iterator *GetDataIterator(rocksdb::ReadOptions &ropts, rocksdb::ColumnFamilyHandle *const column_family);
  // gets a KeyIterator.
  std::shared_ptr<KeyIterator> KeyIter() { return key_iter_; }
  // acquire a snapshot
  void Acquiresnapshot();
  // release a snapshot
  void Releasesnapshot();
  // commit the transaction
  bool Commit();
  // rollback the transaction
  void Rollback();
  //
  void ResetWriteBatch();

 private:
  // writes 'rows' in batch with index.
  std::unique_ptr<rocksdb::WriteBatchWithIndex> index_batch_;
  // wites 'rows' in batch.
  std::unique_ptr<rocksdb::WriteBatch> data_batch_;
  // reads options.
  rocksdb::ReadOptions read_opts_;
  // iterator of key.
  std::shared_ptr<KeyIterator> key_iter_;
};

}  // namespace index
}  // namespace Tianmu

#endif  // TIANMU_INDEX_KV_TRANSACTION_H_
