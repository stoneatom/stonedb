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
#ifndef TIANMU_LOADER_VALUE_CACHE_H_
#define TIANMU_LOADER_VALUE_CACHE_H_
#pragma once

#include <optional>
#include <vector>

#include "common/common_definitions.h"
#include "types/tianmu_data_types.h"

namespace Tianmu {
namespace loader {

class ValueCache final {
 public:
  ValueCache(size_t, size_t);
  ValueCache(const ValueCache &) = delete;
  ValueCache(ValueCache &&) = default;
  ~ValueCache() {
    if (data_ != nullptr)
      std::free(data_);
  }

  void *Prepare(size_t);

  void Commit() {
    values_.push_back(size_);
    nulls_.push_back(expected_null_);
    if (expected_null_)
      null_cnt_++;
    size_ += expected_size_;
    DEBUG_ASSERT(size_ <= capacity_);
    expected_size_ = 0;
    expected_null_ = false;
    if (expected_delete_) {
      deletes_.emplace(values_.size() - 1, true);
      expected_delete_ = false;
    }
  }

  void Rollback() {
    size_ = values_.back();
    values_.pop_back();
    expected_null_ = nulls_.back();
    nulls_.pop_back();
    if (expected_null_)
      null_cnt_--;
    expected_size_ = 0;
    expected_null_ = false;
  }

  void Realloc(size_t newCapacity) {
    data_ = std::realloc(data_, newCapacity);
    capacity_ = newCapacity;
  }

  void SetNull(size_t ono, bool null) {
    DEBUG_ASSERT(ono < values_.size());
    nulls_[ono] = null;
    if (null)
      null_cnt_++;
  }

  void SetDelete(size_t ono) {
    DEBUG_ASSERT(ono < values_.size());
    deletes_.emplace(ono, true);
  }

  void ExpectedSize(size_t expectedSize) {
    DEBUG_ASSERT((size_ + expectedSize) <= capacity_);
    expected_size_ = expectedSize;
  }

  size_t ExpectedSize() const { return expected_size_; }
  void ExpectedNull(bool null) { expected_null_ = null; }
  void ExpectedDelete() {
    expected_delete_ = true;
    expected_null_ = true;
  }
  bool ExpectedNull() const { return expected_null_; }
  void *PreparedBuffer() { return (static_cast<char *>(data_) + size_); }

  size_t SumarizedSize() const { return size_; }
  size_t Size(size_t ono) const {
    DEBUG_ASSERT(ono < values_.size());
    auto end(ono < (values_.size() - 1) ? values_[ono + 1] : size_);
    return (end - values_[ono]);
  }

  char *GetDataBytesPointer(size_t ono) {
    DEBUG_ASSERT(ono < values_.size());
    return (static_cast<char *>(data_) + values_[ono]);
  }

  char const *GetDataBytesPointer(size_t ono) const {
    DEBUG_ASSERT(ono < values_.size());
    return (static_cast<char *>(data_) + values_[ono]);
  }

  bool IsNull(size_t ono) const { return nulls_[ono]; }
  bool NotNull(size_t ono) const { return !nulls_[ono]; }

  bool IsDelete(size_t ono) const { return (deletes_.find(ono) != deletes_.end()); }

  size_t NumOfNulls() const { return null_cnt_; }
  size_t NumOfValues() const { return values_.size(); }

  size_t NumOfDeletes() const { return deletes_.size(); }

  void CalcIntStats(std::optional<common::double_int_t> nv);
  void CalcRealStats(std::optional<common::double_int_t> nv);
  void CalcStrStats(types::BString &min_s, types::BString &max_s, uint &maxlen, const DTCollation &col) const;

  int64_t MinInt() const { return min_i_; }
  int64_t MaxInt() const { return max_i_; }
  int64_t SumInt() const { return sum_i_; }
  double MinDouble() const { return min_d_; }
  double MaxDouble() const { return max_d_; }
  double SumDouble() const { return sum_d_; }

 private:
  void *data_ = nullptr;
  size_t size_ = 0;
  size_t capacity_ = 0;
  size_t value_count_;
  size_t expected_size_ = 0;
  bool expected_null_ = false;
  bool expected_delete_ = false;

  std::vector<size_t> values_;
  std::vector<bool> nulls_;
  // The delta layer is used to merge data of insert type and delete type
  std::map<size_t, bool> deletes_;
  size_t null_cnt_ = 0;

  int64_t min_i_, max_i_, sum_i_;
  double min_d_, max_d_, sum_d_;
};

}  // namespace loader
}  // namespace Tianmu

#endif  // TIANMU_LOADER_VALUE_CACHE_H_
