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
#ifndef STONEDB_CORE_MI_ROUGH_SORTER_H_
#define STONEDB_CORE_MI_ROUGH_SORTER_H_
#pragma once

#include "core/multi_index.h"

namespace stonedb {
namespace core {
class MINewContentsRSorterWorker;

class MINewContentsRSorter {  // Rough sorter of the new contents of MultiIndex
 public:
  MINewContentsRSorter(MultiIndex *_m, IndexTable **t_new, int min_block_shift);
  ~MINewContentsRSorter();

  void AddColumn(IndexTable *t,
                 int dim);   // add a column which was not present in t_new table
  void Commit(int64_t obj);  // sort roughly the current t_new contents, if
                             // needed; obj = the number of all rows

  inline void CommitValues(int64_t *new_value,
                           int64_t obj) {      // analyze whether to sort roughly
                                               // the current t_new contents
    if ((obj > 0 && obj % block_size == 0) ||  // end of IndexTable block => sort if needed, reset stats
        (anything_changed && obj - start_sorting > MAX_SORT_SIZE) ||
        (!anything_changed && obj - start_sorting > OMIT_IF_SORTED)) {
      Commit(obj);
      if (obj % block_size == 0) Barrier();
    }

    bool changed_now = false;
    for (int dim = 0; dim < no_dim; dim++) {
      if (tcheck[dim] && new_value[dim] != common::NULL_VALUE_64) {
        int cur_pack = int(new_value[dim] >> p_power);
        if (last_pack[dim] != cur_pack) {
          if (last_pack[dim] != -1) anything_changed = true;
          changed_now = true;
          last_pack[dim] = cur_pack;
        }
      }
    }
    if (changed_now) {
      if (sorting_needed && obj - start_sorting > OPT_SORT_SIZE) {
        Commit(obj);
        for (int dim = 0; dim < no_dim; dim++)
          if (tcheck[dim] && new_value[dim] != common::NULL_VALUE_64) last_pack[dim] = int(new_value[dim] >> p_power);
      }
      if (!sorting_needed) {
        uint minihash = 0;
        for (int dim = 0; dim < no_dim; dim++) minihash = (443 * minihash + last_pack[dim]) % 919;
        if (minihash_table[minihash] == '1') sorting_needed = true;  // we're back in one of the pack previously seen
        minihash_table[minihash] = '1';
      }
    }
  }
  void Barrier();  // finish all unfinished work, if any

  void RoughQSort(uint *bound_queue, uint64_t start_tuple, uint64_t stop_tuple);

 private:
  // Deciding when to sort:

  static const int64_t OMIT_IF_SORTED = 500;    // a minimal portion of sorted data which will not
                                                // be included in sorting (if placed on the
                                                // beginning of a new sorting)
  static const int64_t OPT_SORT_SIZE = 60000;   // after finding such number of rows worth sorting,
                                                // find the first appropriate place and sort
  static const int64_t MAX_SORT_SIZE = 200000;  // never sort more rows than this number
  // Note: too large sortings may degrade performance due to CPU L2 memory
  // paging

  void ResetStats();

  // Sorting itself:

  inline void SwitchMaterial(uint64_t tuple1,
                             uint64_t tuple2) {  // one transposition of the whole tuples (for sorting)
    for (int i = 0; i < no_cols_to_sort; i++) tsort[i]->Swap(tuple1, tuple2);
  }
  inline int RoughCompare(uint64_t tuple1, uint64_t tuple2) {
    uint64_t v1, v2;
    for (int i = 0; i < no_cols_to_compare; i++) {
      v1 = (tcomp[i]->Get64InsideBlock(tuple1) - 1) >> rough_comp_bit_offset;
      v2 = (tcomp[i]->Get64InsideBlock(tuple2) - 1) >> rough_comp_bit_offset;
      if (v1 < v2) return -1;
      if (v1 > v2) return 1;
    }
    return 0;
  }

  inline void FillMidValues(uint64_t tuple, uint64_t *mid) {
    for (int i = 0; i < no_cols_to_compare; i++)
      mid[i] = (tcomp[i]->Get64InsideBlock(tuple) - 1) >> rough_comp_bit_offset;
  }

  inline int RoughCompareWithMid(uint64_t tuple1, uint64_t *mid) {
    uint64_t v1;
    for (int i = 0; i < no_cols_to_compare; i++) {
      v1 = (tcomp[i]->Get64InsideBlock(tuple1) - 1) >> rough_comp_bit_offset;
      if (v1 < mid[i]) return -1;
      if (v1 > mid[i]) return 1;
    }
    return 0;
  }

  uint32_t p_power;
  int no_dim;
  IndexTable **tall;   // a vector of all IndexTable pointers, natural dim numbers
  IndexTable **tcomp;  // a list of IndexTable pointers to be compared
  IndexTable **tsort;  // a list of IndexTable pointers to be sorted
  int no_cols_to_compare;
  int no_cols_to_sort;
  bool *tcheck;  // a set of IndexTables to be analyzed for potential pack changes
  int rough_comp_bit_offset;
  MultiIndex *mind;  // external link

  MINewContentsRSorterWorker *worker;
  int bound_queue_size;  // quick sort (cyclic) queue

  int *last_pack;
  bool anything_changed;     // true if at least one interesting dimension changed
                             // its pack
  bool sorting_needed;       // the sorting is needed, i.e. packs were changing back
                             // and forth
  int64_t block_size;        // the size of IndexTable block (the minimal one = the
                             // common denominator)
  int64_t start_sorting;     // the first row to (potentially) sort next time
  char minihash_table[919];  // a table of pack numbers seen before
  bool synchronized;         // true if Synchronize() was called and no more sortings
                             // were started
};

class MINewContentsRSorterWorker {
 public:
  MINewContentsRSorterWorker() : bound_queue_size(0), bound_queue(NULL), rough_sorter(NULL) {}
  MINewContentsRSorterWorker(int _bound_queue_size, MINewContentsRSorter *_rough_sorter);
  virtual ~MINewContentsRSorterWorker();

  virtual void RoughSort(uint64_t start_tuple, uint64_t stop_tuple) {
    rough_sorter->RoughQSort(bound_queue, start_tuple, stop_tuple);
  }
  virtual void Barrier() {}

 protected:
  int bound_queue_size;
  uint *bound_queue;
  MINewContentsRSorter *rough_sorter;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_MI_ROUGH_SORTER_H_
