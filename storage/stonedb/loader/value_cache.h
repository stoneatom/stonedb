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
#ifndef STONEDB_LOADER_VALUE_CACHE_H_
#define STONEDB_LOADER_VALUE_CACHE_H_
#pragma once

#include <optional>
#include <vector>

#include "common/common_definitions.h"
#include "types/rc_data_types.h"

namespace stonedb {
namespace loader {

class ValueCache final {
 public:
  ValueCache(size_t, size_t);
  ValueCache(const ValueCache &) = delete;
  ValueCache(ValueCache &&) = default;

  ~ValueCache() { std::free(data); }
  void *Prepare(size_t);
  void Commit() {
    values.push_back(size);
    nulls.push_back(expected_null);
    if (expected_null) null_cnt++;
    size += expected_size;
    DEBUG_ASSERT(size <= capacity);
    expected_size = 0;
    expected_null = false;
  }
  void Rollback() {
    size = values.back();
    values.pop_back();
    expected_null = nulls.back();
    nulls.pop_back();
    if (expected_null) null_cnt--;
    expected_size = 0;
    expected_null = false;
  }
  void Realloc(size_t);
  void SetNull(size_t ono, bool null) {
    DEBUG_ASSERT(ono < values.size());
    nulls[ono] = null;
    if (null) null_cnt++;
  }
  void ExpectedSize(size_t expectedSize) {
    DEBUG_ASSERT((size + expectedSize) <= capacity);
    expected_size = expectedSize;
  }
  size_t ExpectedSize() const { return expected_size; }
  void ExpectedNull(bool null) { expected_null = null; }
  bool ExpectedNull() const { return expected_null; }
  void *PreparedBuffer() { return (static_cast<char *>(data) + size); }
  size_t SumarizedSize() const { return size; }
  size_t Size(size_t ono) const {
    DEBUG_ASSERT(ono < values.size());
    auto end(ono < (values.size() - 1) ? values[ono + 1] : size);
    return (end - values[ono]);
  }

  char *GetDataBytesPointer(size_t ono) {
    DEBUG_ASSERT(ono < values.size());
    return (static_cast<char *>(data) + values[ono]);
  }
  char const *GetDataBytesPointer(size_t ono) const {
    DEBUG_ASSERT(ono < values.size());
    return (static_cast<char *>(data) + values[ono]);
  }

  bool IsNull(size_t ono) const { return nulls[ono]; }
  bool NotNull(size_t ono) const { return !nulls[ono]; }
  size_t NoNulls() const { return null_cnt; }
  size_t NoValues() const { return values.size(); }
  void CalcIntStats(std::optional<common::double_int_t> nv);
  void CalcRealStats(std::optional<common::double_int_t> nv);
  void CalcStrStats(types::BString &min_s, types::BString &max_s, uint &maxlen, const DTCollation &col) const;
  int64_t MinInt() const { return min_i; }
  int64_t MaxInt() const { return max_i; }
  int64_t SumInt() const { return sum_i; }
  double MinDouble() const { return min_d; }
  double MaxDouble() const { return max_d; }
  double SumDouble() const { return sum_d; }

 private:
  void *data = nullptr;
  size_t size = 0;
  size_t capacity = 0;
  size_t value_count;
  size_t expected_size = 0;
  bool expected_null = false;

  std::vector<size_t> values;
  std::vector<bool> nulls;
  size_t null_cnt = 0;

  int64_t min_i, max_i, sum_i;
  double min_d, max_d, sum_d;
};

}  // namespace loader
}  // namespace stonedb

#endif  // STONEDB_LOADER_VALUE_CACHE_H_
