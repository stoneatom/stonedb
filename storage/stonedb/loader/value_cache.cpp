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

#include "loader/value_cache.h"

#include <algorithm>

#include "common/assert.h"
#include "types/rc_data_types.h"

namespace stonedb {
namespace loader {

ValueCache::ValueCache(size_t valueCount, size_t initialCapacity) : value_count(valueCount) {
  Realloc(initialCapacity);
  values.reserve(valueCount);
  nulls.reserve(valueCount);
}

void *ValueCache::Prepare(size_t valueSize) {
  size_t newSize(size + valueSize);
  if (newSize > capacity) {
    auto newCapacity = capacity;
    while (newSize > newCapacity) newCapacity <<= 1;
    auto vals = values.size();
    if ((capacity > 0) && (vals > 0)) { /* second allocation, first reallocation */
      auto valSize = size / vals;
      newCapacity = std::max(newCapacity, valSize * value_count);
    }
    Realloc(newCapacity);
  }
  if (!data) return nullptr;
  return (static_cast<char *>(data) + size);
}

void ValueCache::Realloc(size_t newCapacity) {
  data = std::realloc(data, newCapacity);
  capacity = newCapacity;
}

void ValueCache::CalcIntStats(std::optional<common::double_int_t> nv) {
  min_i = common::PLUS_INF_64;
  max_i = common::MINUS_INF_64;
  sum_i = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    if (!nulls[i]) {
      auto v = *(int64_t *)GetDataBytesPointer(i);
      sum_i += v;
      if (min_i > v) min_i = v;
      if (max_i < v) max_i = v;
    }
  }
  if (NoNulls() > 0 && nv.has_value()) {
    if (min_i > nv->i) min_i = nv->i;
    if (max_i < nv->i) max_i = nv->i;
  }
}

void ValueCache::CalcRealStats(std::optional<common::double_int_t> nv) {
  min_d = common::PLUS_INF_64;
  max_d = common::MINUS_INF_64;
  sum_d = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    if (!nulls[i]) {
      auto v = *(double *)GetDataBytesPointer(i);
      sum_d += v;
      if (min_d > v) min_d = v;
      if (max_d < v) max_d = v;
    }
  }
  if (NoNulls() > 0 && nv.has_value()) {
    if (min_d > nv->d) min_d = nv->d;
    if (max_d < nv->d) max_d = nv->d;
  }
}

void ValueCache::CalcStrStats(types::BString &min_s, types::BString &max_s, uint &maxlen,
                              const DTCollation &col) const {
  maxlen = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    if (!nulls[i]) {
      types::BString v((Size(i) ? GetDataBytesPointer(i) : ZERO_LENGTH_STRING), Size(i));
      if (v.len > maxlen) maxlen = v.len;

      if (min_s.IsNull())
        min_s = v;
      else if (types::RequiresUTFConversions(col)) {
        if (CollationStrCmp(col, min_s, v) > 0) min_s = v;
      } else if (min_s > v)
        min_s = v;

      if (max_s.IsNull())
        max_s = v;
      else if (types::RequiresUTFConversions(col)) {
        if (CollationStrCmp(col, max_s, v) < 0) max_s = v;
      } else if (max_s < v)
        max_s = v;
    }
  }
  types::BString nv(ZERO_LENGTH_STRING, 0);
  if (NoNulls() > 0) {
    if (min_s > nv) min_s = nv;
    if (max_s < nv) max_s = nv;
  }
}

}  // namespace loader
}  // namespace stonedb
