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
#ifndef STONEDB_CORE_JONER_HASH_H_
#define STONEDB_CORE_JONER_HASH_H_
#pragma once

#include "core/joiner.h"
#include "core/joiner_hash_table.h"

namespace stonedb {
namespace core {
class JoinerHash : public TwoDimensionalJoiner {
  /*
   * There are two sides of join:
   * - "traversed", which is put partially (chunk by chunk) into the hash table,
   * - "matched", which is scanned completely for every chunk gathered in the
   * hash table.
   *
   * Algorithm:
   * 1. determine all traversed and matched dimensions,
   * 2. create hash table,
   * 3. traverse the main "traversed" dimension and put key values into the hash
   * table,
   * 4. put there also information about row numbers of all traversed dimensions
   *    (i.e. the main one and all already joined with it),
   * 5. scan the "matched" dimension, find the key values in the hash table,
   * 6. submit all the joined tuples as the result of join
   *    (take all needed tuple numbers from the hash table and "matched" part of
   * multiindex),
   * 7. if the "traversed" side was not fully scanned, clear hash table and go
   * to 4 with the next chunk.
   *
   */
 public:
  JoinerHash(MultiIndex *_mind, TempTable *_table, JoinTips &_tips);
  ~JoinerHash() = default;

  void ExecuteJoinConditions(Condition &cond) override;
  void ForceSwitchingSides() override { force_switching_sides = true; }

 private:
  int64_t TraverseDim(MINewContents &new_mind, MIIterator &mit,
                      int64_t &outer_tuples);  // new_mind is used only for outer joins
  int64_t MatchDim(MINewContents &new_mind, MIIterator &mit);
  int64_t NewMatchDim(MINewContents *new_mind, MIUpdatingIterator *task_mit, Transaction *ci,
                      JoinerHashTable *hash_table, int *join_tuple, int stonedb_sysvar_jointhreadpool);

  void ExecuteJoin();

  void InitOuter(Condition &cond);
  int64_t SubmitOuterMatched(MIIterator &mit,
                             MINewContents &new_mind);  // return the number of newly added tuples
  int64_t SubmitOuterTraversed(MINewContents &new_mind);

  JoinerHashTable jhash;
  uint32_t p_power;  // 2^power

  DimensionVector traversed_dims;  // the mask of dimension numbers of traversed
                                   // dimensions to be put into the join result
  DimensionVector matched_dims;    // the mask of dimension numbers of matched
                                   // dimensions to be put into the join result
  int no_of_traversed_dims;
  std::vector<int> traversed_hash_column;  // the number of hash column containing tuple
                                           // number for this dimension

  std::vector<vcolumn::VirtualColumn *> vc1;
  std::vector<vcolumn::VirtualColumn *> vc2;
  int cond_hashed;

  bool force_switching_sides;  // set true if the join should be done in
                               // different order than optimizer suggests
  bool too_many_conflicts;     // true if the algorithm is in the state of exiting
                               // and forcing switching sides
  bool other_cond_exist;       // if true, then check of other conditions is needed
                               // on matching
  std::vector<Descriptor> other_cond;

  // Statistics
  int64_t packrows_omitted;  // roughly omitted by by matching
  int64_t packrows_matched;

  int64_t actually_traversed_rows;  // "traversed" side rows, which had a chance to
                                    // be in the result (for VC distinct values)

  // Outer join part
  // If one of the following is true, we are in outer join situation:
  bool watch_traversed;   // true if we need to watch which traversed tuples are
                          // used
  bool watch_matched;     // true if we need to watch which matched tuples are used
  bool outer_nulls_only;  // true if only null (outer) rows may exists in result

  std::unique_ptr<Filter> outer_filter;  // used to remember which traversed or
                                         // matched tuples are involved in join
  std::mutex joiner_mutex;

  void SubmitJoinedTuple(int64_t hash_row, MIIterator &mit, MINewContents &new_mind);
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_JONER_HASH_H_
