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
#ifndef STONEDB_INDEX_RC_TABLE_INDEX_H_
#define STONEDB_INDEX_RC_TABLE_INDEX_H_
#pragma once

#include <string>

#include "rocksdb/db.h"

#include "common/common_definitions.h"
#include "index/rdb_utils.h"

namespace stonedb {
namespace core {
class Transaction;
}
namespace index {
class RdbKey;
class RdbTable;
class KVTransaction;
class RCTableIndex final {
 public:
  RCTableIndex(const RCTableIndex &) = delete;
  RCTableIndex &operator=(RCTableIndex &) = delete;
  RCTableIndex(const std::string &name, TABLE *table);
  RCTableIndex() = delete;
  ~RCTableIndex() = default;

  bool Enable() const { return m_enable; }
  const std::vector<uint> &KeyCols() { return m_cols; }
  static common::ErrorCode CreateIndexTable(const std::string &name, TABLE *table);
  static common::ErrorCode DropIndexTable(const std::string &name);
  static bool FindIndexTable(const std::string &name);
  static bool NormalLizeName(const std::string &path, std::string &name);
  common::ErrorCode RefreshIndexTable(const std::string &name);
  common::ErrorCode RenameIndexTable(const std::string &from, const std::string &to);
  void TruncateIndexTable();
  common::ErrorCode InsertIndex(core::Transaction *tx, std::vector<std::string_view> &fields, uint64_t row);
  common::ErrorCode UpdateIndex(core::Transaction *tx, std::string_view &nkey, std::string_view &okey, uint64_t row);
  common::ErrorCode GetRowByKey(core::Transaction *tx, std::vector<std::string_view> &fields, uint64_t &row);

 private:
  common::ErrorCode CheckUniqueness(core::Transaction *tx, const rocksdb::Slice &pk_slice);

 public:
  std::shared_ptr<RdbTable> m_tbl;
  std::shared_ptr<RdbKey> m_rdbkey;
  std::vector<uint> m_cols;
  bool m_enable = false;
  uint m_keyid = 0;
};

class KeyIterator final {
 public:
  KeyIterator() = delete;
  KeyIterator(const KeyIterator &sec) : valid(sec.valid), iter(sec.iter), rdbkey(sec.rdbkey){};
  KeyIterator(KVTransaction *tx) : trans(tx){};
  void ScanToKey(std::shared_ptr<RCTableIndex> tab, std::vector<std::string_view> &fields, common::Operator op);
  void ScanToEdge(std::shared_ptr<RCTableIndex> tab, bool forward);
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
  std::shared_ptr<rocksdb::Iterator> iter;
  std::shared_ptr<RdbKey> rdbkey;
  KVTransaction *trans;
};

}  // namespace index
}  // namespace stonedb

#endif  // STONEDB_INDEX_RC_TABLE_INDEX_H_
