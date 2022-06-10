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
#ifndef STONEDB_INDEX_KV_TRANSACTION_H_
#define STONEDB_INDEX_KV_TRANSACTION_H_
#pragma once

#include "index/rc_table_index.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace stonedb {
namespace index {

class KVTransaction {
 private:
  std::unique_ptr<rocksdb::WriteBatchWithIndex> m_index_batch;
  std::unique_ptr<rocksdb::WriteBatch> m_data_batch;
  rocksdb::WriteOptions write_opts;
  rocksdb::ReadOptions m_read_opts;
  std::shared_ptr<KeyIterator> keyiter;

 public:
  KVTransaction()
      : m_index_batch(std::make_unique<rocksdb::WriteBatchWithIndex>(rocksdb::BytewiseComparator(), 0, true)),
        m_data_batch(std::make_unique<rocksdb::WriteBatch>()),
        keyiter(std::make_shared<KeyIterator>(this)) {}
  ~KVTransaction();
  rocksdb::Status Get(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key, std::string *value);
  rocksdb::Status Put(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                      const rocksdb::Slice &value);
  rocksdb::Status Delete(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key);
  rocksdb::Iterator *GetIterator(rocksdb::ColumnFamilyHandle *const column_family, bool skip_filter);
  rocksdb::Status GetData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key, std::string *value);
  rocksdb::Status PutData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                          const rocksdb::Slice &value);
  rocksdb::Status SingleDeleteData(rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key);
  rocksdb::Iterator *GetDataIterator(rocksdb::ReadOptions &ropts, rocksdb::ColumnFamilyHandle *const column_family);
  std::shared_ptr<KeyIterator> KeyIter() { return keyiter; }
  void Acquiresnapshot();
  void Releasesnapshot();
  bool Commit();
  void Rollback();
};

}  // namespace index
}  // namespace stonedb

#endif  // STONEDB_INDEX_KV_TRANSACTION_H_
