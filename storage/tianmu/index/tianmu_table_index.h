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
#ifndef TIANMU_TABLE_INDEX_H_
#define TIANMU_TABLE_INDEX_H_
#pragma once

#include <string>

#include "rocksdb/db.h"

#include "common/common_definitions.h"
#include "index/rdb_utils.h"

namespace Tianmu {
namespace core {
class Transaction;
}

namespace index {
class RdbKey;
class RdbTable;
class KVTransaction;

class TianmuTableIndex final {
 public:
  TianmuTableIndex(const TianmuTableIndex &) = delete;
  TianmuTableIndex(const std::string &name, TABLE *table);

  TianmuTableIndex &operator=(TianmuTableIndex &) = delete;
  TianmuTableIndex() = delete;
  virtual ~TianmuTableIndex() = default;

  const std::vector<uint> &KeyCols() { return index_of_columns_; }

  static common::ErrorCode CreateIndexTable(const std::string &name, TABLE *table);
  static common::ErrorCode DropIndexTable(const std::string &name);
  static bool FindIndexTable(const std::string &name);

  void TruncateIndexTable();

  common::ErrorCode RefreshIndexTable(const std::string &name);
  common::ErrorCode RenameIndexTable(const std::string &from, const std::string &to);
  common::ErrorCode InsertIndex(core::Transaction *tx, std::vector<std::string> &fields, uint64_t row);
  common::ErrorCode UpdateIndex(core::Transaction *tx, std::string &nkey, std::string &okey, uint64_t row);
  common::ErrorCode DeleteIndex(core::Transaction *tx, std::vector<std::string> &fields, uint64_t row);
  common::ErrorCode GetRowByKey(core::Transaction *tx, std::vector<std::string> &fields, uint64_t &row);

 public:
  std::shared_ptr<RdbTable> rocksdb_tbl_;
  std::shared_ptr<RdbKey> rocksdb_key_;
  std::vector<uint> index_of_columns_;

  uint keyid_ = 0;

 private:
  common::ErrorCode CheckUniqueness(core::Transaction *tx, const rocksdb::Slice &pk_slice);
};

class KeyIterator final {
 public:
  KeyIterator() = delete;
  KeyIterator(const KeyIterator &sec) : valid(sec.valid), iter_(sec.iter_), rocksdb_key_(sec.rocksdb_key_){};
  KeyIterator(KVTransaction *tx) : txn_(tx){};
  void ScanToKey(std::shared_ptr<TianmuTableIndex> tab, std::vector<std::string> &fields, common::Operator op);
  void ScanToEdge(std::shared_ptr<TianmuTableIndex> tab, bool forward);
  common::ErrorCode GetCurKV(std::vector<std::string> &keys, uint64_t &row);
  common::ErrorCode GetRowid(uint64_t &row) {
    std::vector<std::string> keys;
    return GetCurKV(keys, row);
  }
  bool IsValid() const { return valid; }
  KeyIterator &operator++();
  KeyIterator &operator--();

 protected:
  bool valid = false;
  std::shared_ptr<rocksdb::Iterator> iter_;
  std::shared_ptr<RdbKey> rocksdb_key_;
  KVTransaction *txn_;
};

}  // namespace index
}  // namespace Tianmu

#endif  // TIANMU_TABLE_INDEX_H_
