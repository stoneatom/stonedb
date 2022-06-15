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
#ifndef STONEDB_CORE_MI_NEW_CONTENTS_H_
#define STONEDB_CORE_MI_NEW_CONTENTS_H_
#pragma once

#include "core/dimension_group.h"

namespace stonedb {
namespace core {
class Filter;
class IndexTable;
class JoinTips;
class MINewContentsRSorter;
class MultiIndex;

// a class to store new (future) contents of multiindex
class MINewContents final {
 public:
  enum class enumMINCType { MCT_UNSPECIFIED, MCT_MATERIAL, MCT_FILTER_FORGET, MCT_VIRTUAL_DIM } content_type;

  MINewContents(MultiIndex *m, JoinTips &tips);
  ~MINewContents();

  void SetDimensions(DimensionVector &dims);  // add all these dimensions as to be involved
  void Init(int64_t initial_size);            // initialize temporary structures (set
                                              // approximate size)

  void SetNewTableValue(int dim,
                        int64_t val)  // add a value (common::NULL_VALUE_64 is a
                                      // null object index)
  {
    new_value[dim] = val;
  }
  void CommitNewTableValues();  // move all values set by SetNew...(); roughsort
                                // if needed; if the index is larger than the
                                // current size, enlarge table automatically
  bool CommitPack(int pack);    // in case of single filter as a result: set a pack as not
                                // changed (return false if cannot do it)

  bool NoMoreTuplesPossible();         // for optimized cases: the join may be ended
  void Commit(int64_t joined_tuples);  // commit changes to multiindex - must be called
                                       // at the end, or changes will be lost
  void CommitCountOnly(int64_t joined_tuples);

  int OptimizedCaseDimension() { return (content_type == enumMINCType::MCT_FILTER_FORGET ? optimized_dim_stay : -1); }

 private:
  void InitTnew(int dim, int64_t initial_size);  // create t_new of proper size
  void DisableOptimized();                       // switch from optimized mode to normal mode

  MINewContentsRSorter *roughsorter;  // NULL if not needed

  MultiIndex *mind;              // external pointer
  IndexTable **t_new;            // new tables
  int64_t obj;                   // a number of values set in new tables
  uint32_t pack_power;           // 2^pack_power per pack
  int no_dims;                   // a number of dimensions in mind
  int64_t *new_value;            // the buffer for new tuple values (no_dimensions, may be
                                 // mostly not used)
  DimensionVector dim_involved;  // true for dimensions which are about to have new values
  bool *nulls_possible;          // true if a null row was set anywhere for this dimension
  bool *forget_now;              // true if the dimension should not be filled by any
                                 // contents (except # of obj)
  int ignore_repetitions_dim;    // if not -1, then row numbers repetitions may be
                                 // ignored
  int min_block_shift;
  // for optimized case
  int optimized_dim_stay;  // the dimension which may remain a filter after join
  Filter *f_opt;           // new filter for optimized case
  IndexTable *t_opt;       // an index table prepared to switch the optimized case off
  int64_t f_opt_max_ones;  // number of ones in the original filter, for
                           // optimization purposes
  int64_t max_filter_val;  // used to check if we are not trying to update
                           // positions already passed
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_MI_NEW_CONTENTS_H_
