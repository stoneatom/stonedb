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
#ifndef TIANMU_CORE_PARALLEL_HASH_JOIN_H_
#define TIANMU_CORE_PARALLEL_HASH_JOIN_H_
#pragma once

#include <atomic>
#include <list>
#include <vector>

#include "core/column_bin_encoder.h"
#include "core/hash_table.h"
#include "core/joiner.h"
#include "core/multi_index_builder.h"

namespace Tianmu {
namespace core {
class MITaskIterator {
 public:
  MITaskIterator(MultiIndex *mind, DimensionVector &dimensions, int task_id, int task_count, int64_t rows_length);
  virtual ~MITaskIterator();

  const MIIterator *GetIter() const { return iter_.get(); }
  MIIterator *GetIter() { return iter_.get(); }
  int64_t GetStartPackrows() const { return start_packrows_; }
  int64_t GetRowsLength() const { return rows_length_; }
  virtual bool IsValid(MIIterator *iter) const { return iter->IsValid(); }
  bool IsValid() const { return IsValid(iter_.get()); }

 protected:
  std::unique_ptr<MIIterator> iter_;
  int64_t start_packrows_ = 0;
  int64_t rows_length_ = 0;
};

class MutexFilter {
 public:
  MutexFilter(int64_t count, uint32_t power, bool all_ones = false);
  ~MutexFilter();

  int64_t GetOnesCount() const { return filter_->NumOfOnes(); }
  bool IsEmpty() { return filter_->IsEmpty(); }
  bool Get(int64_t position) { return filter_->Get(position); }
  void Set(int64_t position, bool should_mutex = false);
  void Reset(int64_t position, bool should_mutex = false);
  void ResetDelayed(uint64_t position, bool should_mutex = false);
  void Commit(bool should_mutex = false);

 private:
  std::unique_ptr<Filter> filter_;
  std::mutex mutex_;
};

class TraversedHashTable {
 public:
  TraversedHashTable(const std::vector<int> &keys_length, const std::vector<int> &tuples_length, int64_t max_table_size,
                     uint32_t pack_power, bool watch_traversed);
  TraversedHashTable(TraversedHashTable &&t)
      : keys_length_(std::move(t.keys_length_)),
        tuples_length_(std::move(t.tuples_length_)),
        max_table_size_(t.max_table_size_),
        pack_power_(t.pack_power_),
        hash_table_(std::move(t.hash_table_)),
        watch_traversed_(t.watch_traversed_),
        outer_filter_(std::move(t.outer_filter_)),
        column_bin_encoder_(std::move(t.column_bin_encoder_)) {}
  ~TraversedHashTable();

  void Initialize();

  HashTable *hash_table() const { return hash_table_.get(); }
  MutexFilter *outer_filter() const { return outer_filter_.get(); }
  void AssignColumnEncoder(const std::vector<ColumnBinEncoder> &column_bin_encoder);
  void GetColumnEncoder(std::vector<ColumnBinEncoder> *column_bin_encoder);
  ColumnBinEncoder *GetColumnEncoder(size_t col);

 private:
  std::vector<int> keys_length_;
  std::vector<int> tuples_length_;
  int64_t max_table_size_ = 2;
  uint32_t pack_power_ = 0;
  std::unique_ptr<HashTable> hash_table_;
  bool watch_traversed_ = false;
  std::unique_ptr<MutexFilter> outer_filter_;
  std::vector<ColumnBinEncoder> column_bin_encoder_;
};

/*
 * There are two sides of join:
 * - "traversed", which is put partially (chunk by chunk) into the hash table,
 * - "matched", which is scanned completely for every chunk gathered in the hash
 * table.
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
 * 7. if the "traversed" side was not fully scanned, clear hash table and go to
 * 4 with the next chunk.
 *
 */
class ParallelHashJoiner : public TwoDimensionalJoiner {
  struct TraverseTaskParams {
    TraversedHashTable *traversed_hash_table = nullptr;
    std::shared_ptr<MultiIndexBuilder::BuildItem> build_item;
    bool too_many_conflicts = false;  // For output.
    int outer_tuples = 0;             // For output.
    bool no_space_left = false;       // For output.
    MITaskIterator *task_miter = nullptr;

    ~TraverseTaskParams();
  };

  struct MatchTaskParams {
    std::shared_ptr<MultiIndexBuilder::BuildItem> build_item;
    MITaskIterator *task_miter = nullptr;
    std::vector<ColumnBinEncoder> column_bin_encoder;

    ~MatchTaskParams();
  };

 public:
  ParallelHashJoiner(MultiIndex *multi_index, TempTable *temp_table, JoinTips &join_tips);
  ~ParallelHashJoiner();

  // Overridden from TwoDimensionalJoiner:
  void ExecuteJoinConditions(Condition &cond) override;
  void ForceSwitchingSides() override;

 private:
  bool PrepareBeforeJoin(Condition &cond);
  bool AddKeyColumn(vcolumn::VirtualColumn *vc, vcolumn::VirtualColumn *vc_matching);
  int64_t TraverseDim(MIIterator &mit, int64_t *outer_tuples);
  int64_t MatchDim(MIIterator &mit);
  int64_t AsyncTraverseDim(TraverseTaskParams *params);
  int64_t AsyncMatchDim(MatchTaskParams *params);

  void ExecuteJoin();

  bool CreateMatchingTasks(MIIterator &mit, int64_t rows_count, std::vector<MITaskIterator *> *task_iterators,
                           std::string *splitting_type);

  void InitOuter(Condition &cond);
  void SubmitJoinedTuple(MultiIndexBuilder::BuildItem *build_item, TraversedHashTable *traversed_hash_table,
                         int64_t hash_row, MIIterator &mit);
  int64_t SubmitOuterTraversed();
  int64_t SubmitOuterMatched(MIIterator &miter);

  struct OuterMatchedParams;
  void AsyncSubmitOuterMatched(OuterMatchedParams *params, MutexFilter *outer_matched_filter);

  template <typename T>
  bool ImpossibleValues(size_t col, T &pack_min, T &pack_max) {
    for (auto &it : traversed_hash_tables_) {
      if (!it.GetColumnEncoder(col)->ImpossibleValues(pack_min, pack_max))
        return false;
    }

    return true;
  }

  std::vector<ColumnBinEncoder> column_bin_encoder_;
  std::vector<int> hash_table_key_size_;
  std::vector<int> hash_table_tuple_size_;

  std::vector<TraversedHashTable> traversed_hash_tables_;

  uint32_t pack_power_;  // 2 ^ power
  // dimensions description
  DimensionVector traversed_dims_;          // the mask of dimension numbers of traversed dimensions
                                            // to be put into the join result
  DimensionVector matched_dims_;            // the mask of dimension numbers of matched
                                            // dimensions to be put into the join result
  std::vector<int> traversed_hash_column_;  // the number of hash column containing tuple
                                            // number for this dimension

  std::vector<vcolumn::VirtualColumn *> vc1_;
  std::vector<vcolumn::VirtualColumn *> vc2_;
  int cond_hashed_;

  bool force_switching_sides_;  // set true if the join should be done in
                                // different order than optimizer suggests
  bool too_many_conflicts_;     // true if the algorithm is in the state of exiting
                                // and forcing switching sides
  bool other_cond_exist_;       // if true, then check of other conditions is needed
                                // on matching
  std::vector<Descriptor> other_cond_;

  // Statistics
  std::atomic<int64_t> packrows_omitted_;  // roughly omitted by by matching
  std::atomic<int64_t> packrows_matched_;

  std::atomic<int64_t> actually_traversed_rows_;  // "traversed" side rows, which had a chance to
                                                  // be in the
  // result (for VC distinct values)
  std::atomic<bool> interrupt_matching_;

  // Outer join part
  // If one of the following is true, we are in outer join situation:
  bool watch_traversed_;  // true if we need to watch which traversed tuples are
                          // used
  bool watch_matched_;    // true if we need to watch which matched tuples are used
  int64_t outer_tuples_ = 0;
  std::unique_ptr<MutexFilter> outer_matched_filter_;
  bool outer_nulls_only_;  // true if only null (outer) rows may exists in result
  std::unique_ptr<MultiIndexBuilder> multi_index_builder_;
};

std::unique_ptr<TwoDimensionalJoiner> CreateHashJoiner(MultiIndex *multi_index, TempTable *temp_table,
                                                       JoinTips &join_tips);
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_PARALLEL_HASH_JOIN_H_
