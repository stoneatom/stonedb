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
#ifndef TIANMU_CORE_DELTA_TABLE_H_
#define TIANMU_CORE_DELTA_TABLE_H_
#pragma once

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"

#include "common/exception.h"
#include "types/tianmu_data_types.h"

namespace Tianmu {
namespace core {
class TableShare;
class Transaction;

enum class RecordType { RecordType_min, kSchema, kInsert, kUpdate, kDelete, RecordType_max };
class DeltaTable {
 public:
  DeltaTable() = default;
  DeltaTable(const std::string &name, uint32_t delta_id, uint32_t cf_id);
  ~DeltaTable() = default;
  static std::shared_ptr<DeltaTable> CreateDeltaTable(const std::shared_ptr<TableShare> &share,
                                                      const std::string &cf_prefix);
  static common::ErrorCode DropDeltaTable(const std::string &table_name);

  void Init(uint64_t base_row_num);
  std::string FullName() { return fullname_; }
  [[nodiscard]] uint32_t GetDeltaTableID() const { return delta_tid_; }
  uint64_t CountRecords() {
    return load_id.load() - merge_id.load();
  }
  rocksdb::ColumnFamilyHandle *GetCFHandle() { return cf_handle_; }
  common::ErrorCode Rename(const std::string &to);
  void AddInsertRecord(uint64_t row_id, std::unique_ptr<char[]> buf, uint32_t size);
  void AddUpdateRecord(uint64_t row_id, std::unique_ptr<char[]> buf, uint32_t size);
  void AddDeleteRecord(std::unique_ptr<char[]> buf, uint32_t size);

  void Truncate(Transaction *tx);

  struct Stat {
    std::atomic_ulong write_cnt{0};
    std::atomic_ulong write_bytes{0};
    std::atomic_ulong read_cnt{0};
    std::atomic_ulong read_bytes{0};
  } stat;
  std::atomic<uint64_t> load_id{0};
  std::atomic<uint64_t> merge_id{0};
  std::atomic<uint64_t> next_row_id{0};

 private:
  std::string fullname_;
  uint32_t delta_tid_ = 0;
  rocksdb::ColumnFamilyHandle *cf_handle_ = nullptr;

 public:
  class Iterator final {
    friend class DeltaTable;

   public:
    Iterator() = default;
    ~Iterator() = default;

   private:
    Iterator(DeltaTable *table) : table(table){};

   public:
    bool operator==(const Iterator &iter);
    bool operator!=(const Iterator &iter) { return !(*this == iter); }
    void operator++(int);

    std::shared_ptr<types::TianmuDataType> &GetData(int col) {}

    void MoveToRow(int64_t row_id);
    int64_t GetCurrentRowId() const { return position; }
    bool Inited() const { return table != nullptr; }

   private:
    DeltaTable *table = nullptr;
    int64_t position = -1;
    Transaction *conn = nullptr;
    bool current_record_fetched = false;
    std::vector<std::shared_ptr<types::TianmuDataType>> record;

   private:
    static Iterator CreateBegin(DeltaTable *table) { Iterator iter(table); };
    static Iterator CreateEnd();
  };
};

}  // namespace core
}  // namespace Tianmu
#endif  // TIANMU_CORE_DELTA_TABLE_H_
