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
#ifndef TIANMU_CORE_HASH_TABLE_H_
#define TIANMU_CORE_HASH_TABLE_H_
#pragma once

#include <memory>
#include <vector>

#include "base/util/spinlock.h"
#include "core/bin_tools.h"
#include "mm/traceable_object.h"

namespace Tianmu {
namespace core {
class HashTable : public mm::TraceableObject {
 public:
  struct CreateParams {
    std::vector<int> keys_length;
    std::vector<int> tuples_length;
    int64_t max_table_size = 0;
    bool easy_roughable = false;
  };

  class Finder;

  HashTable(const std::vector<int> &keys_length, const std::vector<int> &tuples_length);
  explicit HashTable(const CreateParams &create_params);
  HashTable(HashTable &&table) = default;
  ~HashTable();

  void Initialize(int64_t max_table_size, bool easy_roughable);

  size_t GetKeyBufferWidth() const { return key_buf_width_; }
  int64_t GetCount() const { return rows_count_; }
  int64_t AddKeyValue(const std::string &key_buffer, bool *too_many_conflicts = nullptr);
  void SetTupleValue(int col, int64_t row, int64_t value);
  int64_t GetTupleValue(int col, int64_t row);

 private:
  // Overridden from mm::TraceableObject:
  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }

  std::vector<int> column_size_;
  bool for_count_only_ = false;
  std::vector<int> column_offset_;
  size_t key_buf_width_ = 0;  // in bytes
  size_t total_width_ = 0;    // in bytes
  int multi_offset_ = 0;
  int64_t rows_count_ = 0;
  int64_t rows_limit_ = 0;  // a capacity of one memory buffer
  int64_t rows_of_occupied_ = 0;
  unsigned char *buffer_ = nullptr;
  // Rows locks, max is 10000, if fill the hash table, lock N %
  // rows_lock_.size().
  std::vector<base::util::spinlock> rows_lock_;
};

class HashTable::Finder {
 public:
  Finder(HashTable *hash_table, std::string *key_buffer);
  ~Finder() = default;

  int64_t GetMatchedRows() const { return matched_rows_; }
  // If null rows, return common::NULL_VALUE_64.
  int64_t GetNextRow();

 private:
  void LocateMatchedPosition();

  HashTable *hash_table_ = nullptr;
  std::string *key_buffer_ = nullptr;
  int64_t matched_rows_ = 0;
  int64_t current_row_ = 0;     // row value to be returned by the next GetNextRow() function
  int64_t to_be_returned_ = 0;  // the number of rows left for GetNextRow()
  int64_t current_iterate_step_ = 0;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_HASH_TABLE_H_
