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

#include "common/exception.h"
#include "core/tianmu_attr.h"
#include "core/delta_record_head.h"
#include "index/kv_store.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "types/tianmu_data_types.h"

namespace Tianmu {
namespace core {
class TableShare;
class Transaction;
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
  uint64_t CountRecords() { return load_id.load() - merge_id.load(); }
  rocksdb::ColumnFamilyHandle *GetCFHandle() { return cf_handle_; }
  common::ErrorCode Rename(const std::string &to);
  void AddInsertRecord(uint64_t row_id, std::unique_ptr<char[]> buf, uint32_t size);
  void AddRecord(Transaction *tx, uint64_t row_id, std::unique_ptr<char[]> buf, uint32_t size);

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
  //
  // public:
  //  DeltaIterator Begin(const std::vector<bool> &attrs) { return DeltaIterator::CreateBegin(this, attrs); }
  //  DeltaIterator End() { return DeltaIterator::CreateEnd(); }
};

class DeltaIterator {
  friend class DeltaTable;

 public:
  DeltaIterator() = default;
  ~DeltaIterator() = default;
  DeltaIterator(const DeltaIterator &) = delete;
  DeltaIterator &operator=(const DeltaIterator &) = delete;
  DeltaIterator(DeltaIterator &&other) noexcept {
    table_ = other.table_;
    position_ = other.position_;
    start_position_ = other.start_position_;
    current_record_fetched_ = other.current_record_fetched_;
    it_ = std::move(other.it_);
    record_ = std::move(other.record_);
    attrs_ = std::move(other.attrs_);
  };
  DeltaIterator &operator=(DeltaIterator &&other) noexcept {
    table_ = other.table_;
    position_ = other.position_;
    start_position_ = other.start_position_;
    current_record_fetched_ = other.current_record_fetched_;
    it_ = std::move(other.it_);
    record_ = std::move(other.record_);
    attrs_ = std::move(other.attrs_);
    return *this;
  }
  DeltaIterator(DeltaTable *table, const std::vector<bool> &attrs);
  bool operator==(const DeltaIterator &iter) {
    return table_->GetDeltaTableID() == iter.table_->GetDeltaTableID() && position_ == iter.position_;
  };
  bool operator!=(const DeltaIterator &iter) { return !(*this == iter); }
  void operator++(int) {
    it_->Next();
    int64_t new_pos;
    if (it_->Valid()) {
      // new_pos = it->key(); parse the real row_id
      ;
    } else {
      new_pos = -1;
    }
    position_ = new_pos;
    current_record_fetched_ = false;
  };

  bool Inited() const { return table_ != nullptr; }
  std::string &GetData() {
    // parse value
    // check type=Insert
    // read data to record
    current_record_fetched_ = true;
    return record_;
  }
  void MoveTo(int64_t row_id) {
    // combined key{delta_id+row_id}
    // it = new iterator().seek(key)
    int64_t new_pos;
    if (it_->Valid()) {  // need check valid
      // new_pos = it->key(); parse the real row_id
      ;
    } else {
      new_pos = -1;
    }
    position_ = new_pos;
    current_record_fetched_ = false;
  }
  int64_t Position() const { return position_; }
  int64_t StartPosition() const { return start_position_; }
  bool Valid() const { return position_ != -1; }

 private:
  DeltaTable *table_ = nullptr;
  int64_t position_ = -1;
  int64_t start_position_ = -1;  // this is dividing point between delta and base
  //  Transaction *conn_ = nullptr;
  [[maybe_unused]] bool current_record_fetched_ = false;
  std::unique_ptr<rocksdb::Iterator> it_ = nullptr;
  std::string record_;
  std::vector<bool> attrs_;
};

}  // namespace core
}  // namespace Tianmu
#endif  // TIANMU_CORE_DELTA_TABLE_H_
