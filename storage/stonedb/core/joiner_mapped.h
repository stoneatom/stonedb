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
#ifndef STONEDB_CORE_JOINER_MAPPED_H_
#define STONEDB_CORE_JOINER_MAPPED_H_
#pragma once

#include <memory>
#include <vector>

#include "core/ctask.h"
#include "core/joiner.h"
#include "core/multi_index_builder.h"

namespace stonedb {
namespace core {
class JoinerMapFunction;

class JoinerMapped : public TwoDimensionalJoiner {
  /*
   * There are two sides of join:
   * - "traversed", which is analyzed by a function generator creating join
   * function F : attr_values -> row_number,
   * - "matched", which is scanned to create resulting pairs: (n1,
   * Fetch(key(n1))). Assumptions: only for the easiest cases, i.e. integers
   * without encoding, one equality condition, "traversed" has unique values.
   *
   * Algorithm:
   * 1. determine all traversed and matched dimensions,
   * 2. create mapping function F by scanning the "traversed" dimension,
   * 3. scan the "matched" dimension, get the key values and calculate
   * "traversed" row number using F,
   * 4. submit all the joined tuples as the result of join.
   *
   */
 public:
  JoinerMapped(MultiIndex *_mind, TempTable *_table, JoinTips &_tips);

  void ExecuteJoinConditions(Condition &cond) override;

 protected:
  // dimensions description
  DimensionVector traversed_dims;  // the mask of dimension numbers of traversed
                                   // dimensions to be put into the join result
  DimensionVector matched_dims;    // the mask of dimension numbers of matched
                                   // dimensions to be put into the join result
  virtual std::unique_ptr<JoinerMapFunction> GenerateFunction(vcolumn::VirtualColumn *vc);
};

class JoinerParallelMapped : public JoinerMapped {
 public:
  JoinerParallelMapped(MultiIndex *_mind, TempTable *_table, JoinTips &_tips) : JoinerMapped(_mind, _table, _tips){};

  void ExecuteJoinConditions(Condition &cond) override;

 private:
  int64_t ExecuteMatchLoop(std::shared_ptr<MultiIndexBuilder::BuildItem> *build_item, std::vector<uint32_t> &packrows,
                           int64_t *matched_tuples, vcolumn::VirtualColumn *_vc, CTask task,
                           JoinerMapFunction *map_function);
  bool outer_join = false;
  bool outer_nulls_only = false;
  std::shared_ptr<MultiIndexBuilder> new_mind = std::make_shared<MultiIndexBuilder>(mind, tips);
};

class JoinerMapFunction {  // a superclass for all implemented types of join
                           // mapping
 public:
  JoinerMapFunction(Transaction *_m_conn) : m_conn(_m_conn) {}
  virtual ~JoinerMapFunction() {}
  virtual void Fetch(int64_t key_val,
                     std::vector<int64_t> &row_nums) = 0;  // mapping itself: transform key value into row number
  virtual bool ImpossibleValues(
      int64_t local_min, int64_t local_max) const = 0;  // true if the given interval is out of scope of the function
  virtual bool CertainValues(int64_t local_min, int64_t local_max) const = 0;  // true if the given
                                                                               // interval is fully
                                                                               // covered by the
                                                                               // function values (all
                                                                               // rows will be joined)
  virtual int64_t ApproxDistinctVals() const = 0;  // number of distinct vals found (upper approx.)
  Transaction *m_conn;
};

class OffsetMapFunction : public JoinerMapFunction {
 public:
  OffsetMapFunction(Transaction *_m_conn) : JoinerMapFunction(_m_conn) {}
  ~OffsetMapFunction() = default;

  bool Init(vcolumn::VirtualColumn *vc, MIIterator &mit);

  void Fetch(int64_t key_val, std::vector<int64_t> &row_nums) override;
  bool ImpossibleValues(int64_t local_min, int64_t local_max) const override {
    return local_min > key_max || local_max < key_min;
  }
  bool CertainValues(int64_t local_min, int64_t local_max) const override {
    return local_min >= key_min && local_max <= key_continuous_max;
  }
  int64_t ApproxDistinctVals() const override { return dist_vals_found; }
  std::vector<unsigned char> key_status;    // every key value has its own status here: 255 - not exists,
                                            // other
                                            // - row offset is stored in offset_table
  std::array<int64_t, 255> offset_table{};  // a table of all offsets for this join

  int64_t key_table_min{0};                          // approximated min (used as an offset in key_status table)
  int64_t key_min{common::PLUS_INF_64};              // real min encountered in the table
                                                     // (for ImpossibleValues)
  int64_t key_max{common::MINUS_INF_64};             // real max encountered in the table
                                                     // (for ImpossibleValues)
  int64_t key_continuous_max{common::MINUS_INF_64};  // all values between the real min and this value
                                                     // are guaranteed to exist
  int64_t dist_vals_found{0};
};

class MultiMapsFunction : public JoinerMapFunction {
 public:
  MultiMapsFunction(Transaction *_m_conn) : JoinerMapFunction(_m_conn) {}
  ~MultiMapsFunction(){};

  bool Init(vcolumn::VirtualColumn *vc, MIIterator &mit);

  void Fetch(int64_t key_val, std::vector<int64_t> &row_nums) override;
  bool ImpossibleValues(int64_t local_min, int64_t local_max) const override {
    return local_min > key_max || local_max < key_min;
  }
  bool CertainValues([[maybe_unused]] int64_t local_min, [[maybe_unused]] int64_t local_max) const override {
    return false;
  }
  int64_t ApproxDistinctVals() const override {
    return (key_max - key_min + 1) < vals_found ? (key_max - key_min + 1) : vals_found;
  }
  int64_t key_table_min = 0;               // approximated min (used as an offset in key_status table)
  int64_t key_min = common::PLUS_INF_64;   // real min encountered in the table
                                           // (for ImpossibleValues)
  int64_t key_max = common::MINUS_INF_64;  // real max encountered in the table
                                           // (for ImpossibleValues)
  int64_t vals_found = 0;                  // real max encountered in the table (for ImpossibleValues)
  std::vector<std::multimap<int64_t, int64_t>> keys_value_maps;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_JOINER_MAPPED_H_
