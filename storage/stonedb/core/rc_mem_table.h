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
#ifndef STONEDB_CORE_RC_MEM_TABLE_H_
#define STONEDB_CORE_RC_MEM_TABLE_H_
#pragma once

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"

#include "common/exception.h"

namespace stonedb {
namespace core {
class TableShare;
class Transaction;

class RCMemTable {
 public:
  enum class RecordType { RecordType_min, kSchema, kInsert, kUpdate, kDelete, RecordType_max };

  RCMemTable() = default;
  ~RCMemTable() = default;
  RCMemTable(const std::string name, const uint32_t mem_id, const uint32_t cf_id);
  static std::shared_ptr<RCMemTable> CreateMemTable(std::shared_ptr<TableShare> share, const std::string mem_name);
  static common::ErrorCode DropMemTable(std::string table_name);

  std::string FullName() { return fullname_; }
  uint32_t GetMemID() { return mem_id_; }
  int64_t CountRecords() { return (next_insert_id_.load() - next_load_id_.load()); }
  rocksdb::ColumnFamilyHandle *GetCFHandle() { return cf_handle_; }
  common::ErrorCode Rename(const std::string &to);
  void InsertRow(std::unique_ptr<char[]> buf, uint32_t size);
  void Truncate(Transaction *tx);

  struct Stat {
    std::atomic_ulong write_cnt{0};
    std::atomic_ulong write_bytes{0};
    std::atomic_ulong read_cnt{0};
    std::atomic_ulong read_bytes{0};
  } stat;
  std::atomic<std::int64_t> next_load_id_ = 0;
  std::atomic<std::int64_t> next_insert_id_ = 0;

 private:
  std::string fullname_;
  uint32_t mem_id_ = 0;
  rocksdb::ColumnFamilyHandle *cf_handle_ = nullptr;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_RC_MEM_TABLE_H_
