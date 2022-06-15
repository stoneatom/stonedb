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

#include <numeric>

#include "common/assert.h"
#include "core/hash_table.h"
#include "core/rough_multi_index.h"
#include "core/transaction.h"
#include "system/fet.h"

namespace stonedb {
namespace core {
namespace {
const int kMaxHashConflicts = 128;
const int prime_steps_no = 55;
const int64_t min_prime_not_in_table = 277;
int prime_steps[prime_steps_no] = {7,   11,  13,  17,  19,  23,  29,  31,  37,  41,  43,  47,  53,  59,
                                   61,  67,  71,  73,  79,  83,  89,  97,  101, 103, 107, 109, 113, 127,
                                   131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197,
                                   199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271};
}  // namespace

HashTable::HashTable(const std::vector<int> &keys_length, const std::vector<int> &tuples_length) {
  // Table format: <key_1><key_2>...<key_n><tuple_1>...<tuple_m><multiplier>

  // Aligment, e.g. 1->4, 12->12, 19->20
  key_buf_width_ = 4 * ((std::accumulate(keys_length.begin(), keys_length.end(), 0) + 3) / 4);
  for_count_only_ = tuples_length.empty();

  column_size_.assign(keys_length.begin(), keys_length.end());
  std::copy(tuples_length.begin(), tuples_length.end(), std::back_inserter(column_size_));
  column_size_.push_back(8);  // 8 is multiplier column width

  column_offset_.resize(column_size_.size());

  total_width_ = key_buf_width_;
  for (size_t index = keys_length.size(); index < column_size_.size(); ++index) {
    column_offset_[index] = total_width_;
    total_width_ += column_size_[index];
  }
  multi_offset_ = column_offset_[column_size_.size() - 1];
}

HashTable::HashTable(const CreateParams &create_params) {
  // Table format: <key_1><key_2>...<key_n><tuple_1>...<tuple_m><multiplier>

  // Aligment, e.g. 1->4, 12->12, 19->20
  key_buf_width_ =
      4 * ((std::accumulate(create_params.keys_length.begin(), create_params.keys_length.end(), 0) + 3) / 4);
  for_count_only_ = create_params.tuples_length.empty();

  column_size_.assign(create_params.keys_length.begin(), create_params.keys_length.end());
  std::copy(create_params.tuples_length.begin(), create_params.tuples_length.end(), std::back_inserter(column_size_));
  column_size_.push_back(8);  // 8 is multiplier column width

  column_offset_.resize(column_size_.size());

  total_width_ = key_buf_width_;
  for (size_t index = create_params.keys_length.size(); index < column_size_.size(); ++index) {
    column_offset_[index] = total_width_;
    total_width_ += column_size_[index];
  }
  multi_offset_ = column_offset_[column_size_.size() - 1];

  Initialize(create_params.max_table_size, create_params.easy_roughable);
}

HashTable::~HashTable() { dealloc(buffer_); }

void HashTable::Initialize(int64_t max_table_size, [[maybe_unused]] bool easy_roughable) {
  max_table_size = std::max(max_table_size, (int64_t)2);

  rows_count_ = int64_t((max_table_size + 1) * 1.25);
  rccontrol.lock(current_tx->GetThreadID())
      << "Establishing hash table need " << rows_count_ * total_width_ / 1024 / 1024 << "MB" << system::unlock;

  // calculate vertical size (not dividable by a set of some prime numbers)
  if (rows_count_ < min_prime_not_in_table)  // too less rows => high collision probability
    rows_count_ = min_prime_not_in_table;
  else
    rows_count_ = (rows_count_ / min_prime_not_in_table + 1) * min_prime_not_in_table;

  // make sure that the size is not dividable by any of prime_steps
  bool increased = false;
  do {
    increased = false;
    for (int i = 0; i < prime_steps_no; i++)
      if (rows_count_ % prime_steps[i] == 0) {
        increased = true;
        rows_count_ += min_prime_not_in_table;
        break;
      }
  } while (increased);

  rows_limit_ = int64_t(rows_count_ * 0.9);

  rows_lock_.resize(std::min<size_t>(rows_count_, 10000u));

  // No need to cache on disk.
  buffer_ = (unsigned char *)alloc(((total_width_ / 4) * rows_count_) * 4, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
  std::memset(buffer_, 0, total_width_ * rows_count_);
}

int64_t HashTable::AddKeyValue(const std::string &key_buffer, bool *too_many_conflicts) {
  DEBUG_ASSERT(key_buffer.size() == key_buf_width_);

  if (rows_of_occupied_ >= rows_limit_)  // no more space
    return common::NULL_VALUE_64;

  unsigned int crc_code = HashValue(key_buffer.c_str(), key_buffer.size());
  int64_t row = crc_code % rows_count_;
  // vertical table size is not dividable by the following step values, so they
  // will eventually iterate through the whole table
  int64_t iterate_step = prime_steps[crc_code % prime_steps_no];
  int64_t startrow = row;
  do {
    std::scoped_lock lk(rows_lock_[row % rows_lock_.size()]);
    unsigned char *cur_t = buffer_ + row * total_width_;
    int64_t *multiplier = (int64_t *)(cur_t + multi_offset_);
    if (*multiplier != 0) {
      if (std::memcmp(cur_t, key_buffer.c_str(), key_buf_width_) == 0) {
        // i.e. identical row found
        int64_t last_multiplier = *multiplier;
        DEBUG_ASSERT(last_multiplier > 0);
        (*multiplier)++;
        if (for_count_only_) return row;
        // iterate several steps forward to find some free location
        row += iterate_step * last_multiplier;
        if (row >= rows_count_) row = row % rows_count_;

        if (*multiplier > kMaxHashConflicts && too_many_conflicts)  // a threshold for switching sides
          *too_many_conflicts = true;
      } else {  // some other value found
        // iterate one step forward
        row += iterate_step;
        if (row >= rows_count_) row = row % rows_count_;
      }
    } else {
      *multiplier = 1;
      std::memcpy(cur_t, key_buffer.c_str(), key_buf_width_);
      rows_of_occupied_++;
      return row;
    }
  } while (row != startrow);
  DEBUG_ASSERT("Never reach here!");
  return common::NULL_VALUE_64;  // search deadlock (we returned to the same
                                 // position)
}

void HashTable::SetTupleValue(int col, int64_t row, int64_t value) {
  if (column_size_[col] == 4) {
    if (value == common::NULL_VALUE_64)
      *((int *)(buffer_ + row * total_width_ + column_offset_[col])) = 0;
    else
      *((int *)(buffer_ + row * total_width_ + column_offset_[col])) = int(value + 1);
  } else {
    if (value == common::NULL_VALUE_64)
      *((int64_t *)(buffer_ + row * total_width_ + column_offset_[col])) = 0;
    else
      *((int64_t *)(buffer_ + row * total_width_ + column_offset_[col])) = value + 1;
  }
}

int64_t HashTable::GetTupleValue(int col, int64_t row) {
  if (column_size_[col] == 4) {
    int value = *((int *)(buffer_ + row * total_width_ + column_offset_[col]));
    if (value == 0) return common::NULL_VALUE_64;
    return value - 1;
  } else {
    int64_t value = *((int64_t *)(buffer_ + row * total_width_ + column_offset_[col]));
    if (value == 0) return common::NULL_VALUE_64;
    return value - 1;
  }
}

HashTable::Finder::Finder(HashTable *hash_table, std::string *key_buffer)
    : hash_table_(hash_table), key_buffer_(key_buffer) {
  DEBUG_ASSERT(key_buffer_->size() == hash_table_->key_buf_width_);
  LocateMatchedPosition();
}

void HashTable::Finder::LocateMatchedPosition() {
  unsigned int crc_code = HashValue(key_buffer_->data(), key_buffer_->size());
  current_row_ = crc_code % hash_table_->rows_count_;
  // vertical table size is not dividable by the following step values, so they
  // will eventually iterate through the whole table
  current_iterate_step_ = prime_steps[crc_code % prime_steps_no];
  int64_t startrow = current_row_;
  do {
    unsigned char *cur_t = hash_table_->buffer_ + current_row_ * hash_table_->total_width_;
    int64_t multiplier = *(int64_t *)(cur_t + hash_table_->multi_offset_);
    if (multiplier != 0) {
      if (std::memcmp(cur_t, key_buffer_->data(), key_buffer_->size()) == 0) {
        // i.e. identical row found
        to_be_returned_ = multiplier;
        matched_rows_ = multiplier;
        return;
      }
      // some other value found, iterate one step forward
      current_row_ += current_iterate_step_;
      if (current_row_ >= hash_table_->rows_count_) current_row_ = current_row_ % hash_table_->rows_count_;
    } else {
      current_row_ = common::NULL_VALUE_64;  // not found
      return;
    }
  } while (current_row_ != startrow);
  current_row_ = common::NULL_VALUE_64;  // not found at all
}

int64_t HashTable::Finder::GetNextRow() {
  if (to_be_returned_ == 0) return common::NULL_VALUE_64;

  int64_t row_to_return = current_row_;
  to_be_returned_--;
  if (to_be_returned_ == 0) return row_to_return;

  // now prepare the next row to be returned
  int64_t startrow = current_row_;
  current_row_ += current_iterate_step_;
  if (current_row_ >= hash_table_->rows_count_) current_row_ = current_row_ % hash_table_->rows_count_;
  do {
    DEBUG_ASSERT(
        *((int *)(hash_table_->buffer_ + current_row_ * hash_table_->total_width_ + hash_table_->multi_offset_)) != 0);
    if (std::memcmp(hash_table_->buffer_ + current_row_ * hash_table_->total_width_, key_buffer_->data(),
                    key_buffer_->size()) == 0) {
      // i.e. identical row found - keep the current_row_
      return row_to_return;
    } else {  // some other value found - iterate one step forward
      current_row_ += current_iterate_step_;
      if (current_row_ >= hash_table_->rows_count_) current_row_ = current_row_ % hash_table_->rows_count_;
    }
  } while (current_row_ != startrow);
  return row_to_return;
}
}  // namespace core
}  // namespace stonedb
