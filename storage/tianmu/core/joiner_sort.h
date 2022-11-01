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
#ifndef TIANMU_CORE_JOINER_SORT_H_
#define TIANMU_CORE_JOINER_SORT_H_
#pragma once

#include "core/column_bin_encoder.h"
#include "core/joiner.h"
#include "mm/traceable_object.h"
#include "system/cacheable_item.h"

namespace Tianmu {
namespace core {
class JoinerSortWrapper;

class JoinerSort : public TwoDimensionalJoiner {
  /*
   * Algorithm:
   *  1. Encode both columns with a common ColumnBinEncoder.
   *  2. Insert sorted values (keys) and all dimension data into two sorters.
   *	  - omit roughly packs that are outside the scope.
   *  3. Sort them by keys.
   *  4. Traverse in parallel both sorters:
   *     - Take a row from the first one. Match it and join with the current
   *tuples cache.
   *     - Take rows from the second sorter, putting them into cache.
   *  5. Check all additional conditions before inserting resulting rows into
   *output multiindex.
   */
 public:
  JoinerSort(MultiIndex *_mind, TempTable *_table, JoinTips &_tips)
      : TwoDimensionalJoiner(_mind, _table, _tips),
        other_cond_exist(false),
        watch_traversed(false),
        watch_matched(false),
        outer_filter(nullptr),
        outer_nulls_only(false) {}
  ~JoinerSort();

  void ExecuteJoinConditions(Condition &cond) override;

 private:
  void AddTuples(MINewContents &new_mind, JoinerSortWrapper &sort_wrapper, unsigned char *matched_row,
                 int64_t &result_size, DimensionVector &dims_used_in_other_cond);
  int64_t AddOuterTuples(MINewContents &new_mind, JoinerSortWrapper &sort_encoder, DimensionVector &iterate_watched);

  bool other_cond_exist;  // if true, then check of other conditions is needed
  std::vector<Descriptor> other_cond;

  // Outer join part
  // If one of the following is true, we are in outer join situation:
  bool watch_traversed;   // true if we need to watch which traversed tuples are
                          // used
  bool watch_matched;     // true if we need to watch which matched tuples are used
  Filter *outer_filter;   // used to remember which traversed or matched tuples
                          // are involved in join
  bool outer_nulls_only;  // true if only null (outer) rows may exists in result
};

class JoinerSortWrapper : public mm::TraceableObject {
 public:
  JoinerSortWrapper(bool _less);
  ~JoinerSortWrapper();

  bool SetKeyColumns(vcolumn::VirtualColumn *v1,
                     vcolumn::VirtualColumn *v2);  // return false if not compatible
  void SetDimensions(MultiIndex *mind, DimensionVector &dim_tr, DimensionVector &dim_match, DimensionVector &dim_other,
                     bool count_only);
  void InitCache(int64_t no_of_rows);  // prepare cache table

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }
  int KeyBytes() { return key_bytes; }
  int TraverseBytes() { return traverse_bytes; }
  int MatchBytes() { return match_bytes; }
  unsigned char *EncodeForSorter1(vcolumn::VirtualColumn *v, MIIterator &mit,
                                  int64_t outer_pos);  // for Traversed
  unsigned char *EncodeForSorter2(vcolumn::VirtualColumn *v, MIIterator &mit,
                                  int64_t outer_pos);  // for Matched
  bool AddToCache(unsigned char *);                    // return false if there is no more space in cache
                                                       // (i.e. the next operation would fail)
  void ClearCache() { cur_cache_used = 0; }
  int64_t CacheUsed() { return cur_cache_used; }
  bool PackPossible(vcolumn::VirtualColumn *v,
                    MIIterator &mit);  // return false if a Matched data pack
                                       // defined by (v, mit) is outside
                                       // Traversed statistics, given a
                                       // comparison direction stored in less

  // Dimension encoding section
  bool DimEncoded(int d) { return (dim_size[d] > 0); }
  int64_t DimValue(int dim, int cache_position,
                   unsigned char *matched_buffer);  // a stored dimension value
                                                    // (common::NULL_VALUE_64
                                                    // for null object)

  // Outer joins
  void WatchMatched() { watch_matched = true; }
  void WatchTraversed() { watch_traversed = true; }
  int64_t GetOuterIndex(int cache_position,
                        unsigned char *matched_buffer);  // take it from
                                                         // traversed or
                                                         // matched, depending
                                                         // on the context

 private:
  bool less;
  ColumnBinEncoder *encoder;

  int64_t cache_size;      // in rows
  int64_t cur_cache_used;  // in rows

  int key_bytes;       // size of key data for both sorters
  int traverse_bytes;  // total size of the first ("traverse") sorter
  int match_bytes;     // total size of the second ("match") sorter
  int cache_bytes;     // derivable: traverse_bytes - key_byts
  unsigned char *buf;  // a temporary buffer for encoding, size:
                       // max(traverse_bytes, match_bytes)
  int buf_bytes;       // buf size, derivable: max(traverse_bytes, match_bytes)

  unsigned char *min_traversed;  // minimal key set as traversed (a buffer of
                                 // key_bytes size; nullptr
                                 // - not set yet)

  unsigned char *cache;  // a buffer for cache

  // dimension mapping section
  int no_dims;
  std::vector<bool> dim_traversed;  // true if this dim is stored in cache
  std::vector<int> dim_size;        // byte size of dim value
  std::vector<int> dim_offset;      // offset of dim. value in cache or matched
                                    // sorter buffer (excluding the key section)

  // outer join index encoding
  int outer_offset;      // 64-bit value; the offset excludes the key section
  bool watch_traversed;  // true if we need to watch which traversed tuples are
                         // used
  bool watch_matched;    // true if we need to watch which matched tuples are used
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_JOINER_SORT_H_
