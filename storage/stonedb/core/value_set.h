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
#ifndef STONEDB_CORE_VALUE_SET_H_
#define STONEDB_CORE_VALUE_SET_H_
#pragma once

#include <unordered_set>

#include "common/common_definitions.h"
#include "types/rc_data_types.h"

namespace stonedb {

namespace types {
class RCDataType;
class RCValueObject;
class TextStat;
}  // namespace types

namespace utils {
class Hash64;
}
namespace core {

class Filter;

#define VS_EASY_TABLE_SIZE 12
// Results of tuning VS_EASY_TABLE_SIZE:
// - for up to 12 values easy table is faster,
// - for more than 16 values hash64 is faster,
// - between 12 and 16 the results are comparable, so 12 is chosen to minimize
// memory (static) footprint of ValueSet.

class ValueSet {
  // ValueSet might be considered as thread safe object only in case it is used
  // for one type of values and various threads would not try to prepare it to
  // different types
 public:
  ValueSet(uint32_t power);
  ValueSet(ValueSet &sec);
  ~ValueSet();

  void Add64(int64_t v);  // add as RCNum, scale 0
  void Add(std::unique_ptr<types::RCDataType> rcdt);
  void Add(const types::RCValueObject &rcv);
  void AddNull() { contains_nulls = true; }
  void ForcePreparation() { prepared = false; }
  void Prepare(common::CT at, int scale, DTCollation);
  bool Contains(int64_t v);          // easy case, for non-null integers
  bool Contains(types::BString &v);  // easy case, for strings
  bool EasyMode() { return (use_easy_table || easy_vals != NULL || easy_hash != NULL) && prepared; }
  bool Contains(const types::RCValueObject &v, DTCollation);
  bool Contains(const types::RCDataType &v, DTCollation);
  void Clear();

  bool IsEmpty() const { return (NoVals() == 0 && !contains_nulls); }
  bool ContainsNulls() const { return contains_nulls; }
  types::RCDataType *Min() const { return min; }
  types::RCDataType *Max() const { return max; }
  int NoVals() const {
    if (values.empty()) return no_obj;
    return values.size();
  }

  auto begin() {
    if (!prepared) return values.cend();
    return values.cbegin();
  }

  auto end() { return values.cend(); }

  bool CopyCondition(types::CondArray &condition, DTCollation coll);
  bool CopyCondition(common::CT at, std::shared_ptr<utils::Hash64> &condition, DTCollation coll);

 private:
  bool isContains(const types::RCDataType &v, DTCollation coll);
  bool IsPrepared(common::CT at, int scale, DTCollation);

 protected:
  bool contains_nulls;
  int no_obj;  // number of values added
  bool prepared;
  common::CT prep_type;  // type to which it is prepared
  int prep_scale;        // scale to which it is prepared
  DTCollation prep_collation;

  std::unordered_set<types::RCDataType *, types::rc_hash_compare<types::RCDataType *>,
                     types::rc_hash_compare<types::RCDataType *>>
      values;

  types::RCDataType *min;
  types::RCDataType *max;

  // easy implementation for numerical values
  int64_t easy_min;
  int64_t easy_max;
  int64_t easy_table[VS_EASY_TABLE_SIZE];
  bool use_easy_table;

  std::unique_ptr<Filter> easy_vals;  // contains a characteristic set of values between
                                      // easy_min and easy_max, including
  std::unique_ptr<utils::Hash64> easy_hash;
  std::unique_ptr<types::TextStat> easy_text;  // an alternative way of storing text
                                               // values (together with easy_hash)
  uint32_t pack_power;

 public:
  typedef decltype(values)::const_iterator const_iterator;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_VALUE_SET_H_
