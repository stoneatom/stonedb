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

#include <list>

#include "common/assert.h"
#include "core/engine.h"
#include "core/join_thread_table.h"
#include "core/joiner_hash.h"
#include "core/parallel_hash_join.h"
#include "core/proxy_hash_joiner.h"
#include "core/task_executor.h"
#include "core/temp_table.h"
#include "core/transaction.h"
#include "system/fet.h"
#include "util/thread_pool.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
namespace {
const int kJoinSplittedMinPacks = 5;
const int kTraversedPacksPerFragment = 30;

int EvaluateTraversedFragments(int packs_count) {
  const int kMaxTraversedFragmentCount = 8;
  return std::max(std::min(packs_count / kTraversedPacksPerFragment, kMaxTraversedFragmentCount), 1);
}

int EvaluateMatchedFragmentsWithPacks(int packs_count) {
  int needed_fragments = (packs_count + kJoinSplittedMinPacks) / kJoinSplittedMinPacks;
  int max_fragments = tianmu_sysvar_query_threads ? tianmu_sysvar_query_threads : std::thread::hardware_concurrency();
  int fragments = std::min(needed_fragments, max_fragments);
  return std::max(fragments, 1);
}

int EvaluateMatchedFragmentsWithRows([[maybe_unused]] uint32_t pack_power, int64_t split_unit, int64_t rows_count) {
  int needed_fragments = (rows_count + split_unit) / split_unit;
  int max_fragments = tianmu_sysvar_query_threads ? tianmu_sysvar_query_threads : std::thread::hardware_concurrency();
  int fragments = std::min(needed_fragments, max_fragments);
  return std::max(fragments, 1);
}

}  // namespace

//----------------------------------------------MITaskIterator-----------------------------------------------
MITaskIterator::MITaskIterator(MultiIndex *mind, DimensionVector &dimensions, int task_id, int task_count,
                               int64_t rows_length)
    : iter_(new MIIterator(new MultiIndex(*mind, true), dimensions)) {
  iter_->SetTaskNum(task_count);
  iter_->SetTaskId(task_id);
  rows_length_ = rows_length * 1.5;
}

MITaskIterator::~MITaskIterator() {
  std::unique_ptr<MultiIndex> mind(iter_->GetMultiIndex());
  // MIIterator must delete before MultiIndex.
  iter_.reset(nullptr);
}

//------------------------------MILinearPackTaskIterator--------------------------------------------------------
class MILinearPackTaskIterator : public MITaskIterator {
 public:
  MILinearPackTaskIterator(uint32_t pack_power, MultiIndex *mind, DimensionVector &dimensions, int task_id,
                           int task_count, int64_t rows_length, int packs_started, int packs_ended)
      : MITaskIterator(mind, dimensions, task_id, task_count, rows_length) {
    start_packrows_ = int64_t(packs_started) << pack_power;
    iter_->SetNoPacksToGo(packs_ended);
    iter_->RewindToPack(packs_started);
  }
};

//------------------------------MILinearRowTaskIterator--------------------------------------------------------
class MILinearRowTaskIterator : public MITaskIterator {
 public:
  MILinearRowTaskIterator(MultiIndex *mind, DimensionVector &dimensions, int task_id, int task_count,
                          int64_t rows_length, int64_t rows_started, int64_t rows_ended)
      : MITaskIterator(mind, dimensions, task_id, task_count, rows_length), rows_ended_(rows_ended) {
    start_packrows_ = rows_started;
    iter_->RewindToRow(rows_started);
  }

  bool IsValid(MIIterator *iter) const override {
    DEBUG_ASSERT(iter->GetOneFilterDim() > -1);
    int64_t cur_pos = (*iter)[iter->GetOneFilterDim()];
    // If not set rows_ended, the rows_ended_ is -1.
    // If the rows_ended_ isn't -1, here should compare with cur_pos.
    return iter->IsValid() && ((rows_ended_ == -1) || (cur_pos <= rows_ended_));
  }

 private:
  int64_t rows_ended_ = -1;
};

//----------------------------MIFixedTaskIterator-----------------------------------------------------------------------
class MIFixedTaskIterator : public MITaskIterator {
 public:
  MIFixedTaskIterator(uint32_t /*pack_power*/, MultiIndex *mind, DimensionVector &dimensions, int task_id,
                      int task_count, int64_t rows_length, int64_t rows_started, int fixed_block_index)
      : MITaskIterator(mind, dimensions, task_id, task_count, rows_length) {
    start_packrows_ = rows_started;
    iter_->RewindToPack(fixed_block_index);
  }
};

//-----------------------------MutexFilter------------------------------------------
MutexFilter::MutexFilter(int64_t count, uint32_t power, bool all_ones) : filter_(new Filter(count, power, all_ones)) {}

MutexFilter::~MutexFilter() {}

void MutexFilter::Set(int64_t position, bool should_mutex) {
  if (should_mutex) {
    std::scoped_lock lk(mutex_);
    filter_->Set(position);
  } else
    filter_->Set(position);
}

void MutexFilter::Reset(int64_t position, bool should_mutex) {
  if (should_mutex) {
    std::scoped_lock lk(mutex_);
    filter_->Reset(position);
  } else
    filter_->Reset(position);
}

void MutexFilter::ResetDelayed(uint64_t position, bool should_mutex) {
  if (should_mutex) {
    std::scoped_lock lk(mutex_);
    filter_->ResetDelayed(position);
  } else
    filter_->ResetDelayed(position);
}

void MutexFilter::Commit(bool should_mutex) {
  if (should_mutex) {
    std::scoped_lock lk(mutex_);
    filter_->Commit();
  } else
    filter_->Commit();
}

//------------------------------------------------TraversedHashTable----------------------------------------------
TraversedHashTable::TraversedHashTable(const std::vector<int> &keys_length, const std::vector<int> &tuples_length,
                                       int64_t max_table_size, uint32_t pack_power, bool watch_traversed)
    : keys_length_(keys_length),
      tuples_length_(tuples_length),
      max_table_size_(max_table_size),
      pack_power_(pack_power),
      watch_traversed_(watch_traversed) {}

void TraversedHashTable::Initialize() {
  hash_table_.reset(new HashTable(keys_length_, tuples_length_));
  hash_table_->Initialize(max_table_size_, false);
  if (watch_traversed_) {
    outer_filter_.reset(new MutexFilter(hash_table_->GetCount(), pack_power_));
  }
}

TraversedHashTable::~TraversedHashTable() {}

void TraversedHashTable::AssignColumnEncoder(const std::vector<ColumnBinEncoder> &column_bin_encoder) {
  column_bin_encoder_.assign(column_bin_encoder.begin(), column_bin_encoder.end());
}

void TraversedHashTable::GetColumnEncoder(std::vector<ColumnBinEncoder> *column_bin_encoder) {
  column_bin_encoder->assign(column_bin_encoder_.begin(), column_bin_encoder_.end());
}

ColumnBinEncoder *TraversedHashTable::GetColumnEncoder(size_t col) {
  DEBUG_ASSERT(col < column_bin_encoder_.size());
  return &column_bin_encoder_[col];
}

ParallelHashJoiner::TraverseTaskParams::~TraverseTaskParams() {
  if (task_miter) {
    delete task_miter;
    task_miter = nullptr;
  }
}

ParallelHashJoiner::MatchTaskParams::~MatchTaskParams() {
  if (task_miter) {
    delete task_miter;
    task_miter = nullptr;
  }
}

// ParallelHashJoiner
ParallelHashJoiner::ParallelHashJoiner(MultiIndex *multi_index, TempTable *temp_table, JoinTips &join_tips)
    : TwoDimensionalJoiner(multi_index, temp_table, join_tips), interrupt_matching_(false) {
  pack_power_ = multi_index->ValueOfPower();
  traversed_dims_ = DimensionVector(multi_index->NumOfDimensions());
  matched_dims_ = DimensionVector(multi_index->NumOfDimensions());
  for (int i = 0; i < multi_index->NumOfDimensions(); i++) traversed_hash_column_.push_back(-1);

  cond_hashed_ = 0;
  other_cond_exist_ = false;
  packrows_omitted_ = 0;
  packrows_matched_ = 0;
  actually_traversed_rows_ = 0;
  watch_traversed_ = false;
  watch_matched_ = false;
  force_switching_sides_ = false;
  too_many_conflicts_ = false;
  outer_nulls_only_ = false;
}

ParallelHashJoiner::~ParallelHashJoiner() {}

// Overridden from TwoDimensionalJoiner:
void ParallelHashJoiner::ExecuteJoinConditions(Condition &cond) {
  MEASURE_FET("ParallelHashJoiner::ExecuteJoinConditions(...)");

  if (PrepareBeforeJoin(cond))
    ExecuteJoin();

  why_failed = too_many_conflicts_ ? JoinFailure::FAIL_WRONG_SIDES : JoinFailure::NOT_FAILED;
}

void ParallelHashJoiner::ForceSwitchingSides() { force_switching_sides_ = true; }

bool ParallelHashJoiner::PrepareBeforeJoin(Condition &cond) {
  why_failed = JoinFailure::FAIL_HASH;

  std::vector<int> hash_descriptors;
  // Prepare all descriptor information
  bool first_found = true;
  DimensionVector dims1(mind->NumOfDimensions());  // Initial dimension descriptions
  DimensionVector dims2(mind->NumOfDimensions());
  DimensionVector dims_other(mind->NumOfDimensions());  // dimensions for other conditions, if needed
  for (uint i = 0; i < cond.Size(); i++) {
    bool added = false;
    if (cond[i].IsType_JoinSimple() && cond[i].op == common::Operator::O_EQ) {
      if (first_found) {
        hash_descriptors.push_back(i);
        added = true;
        cond[i].attr.vc->MarkUsedDims(dims1);
        cond[i].val1.vc->MarkUsedDims(dims2);
        mind->MarkInvolvedDimGroups(dims1);
        mind->MarkInvolvedDimGroups(dims2);
        // Add dimensions for nested outer joins
        if (dims1.Intersects(cond[i].right_dims))
          dims1.Plus(cond[i].right_dims);
        if (dims2.Intersects(cond[i].right_dims))
          dims2.Plus(cond[i].right_dims);
        first_found = false;
      } else {
        DimensionVector sec_dims1(mind->NumOfDimensions());  // Make sure the local descriptions are
                                                             // compatible
        DimensionVector sec_dims2(mind->NumOfDimensions());
        cond[i].attr.vc->MarkUsedDims(sec_dims1);
        cond[i].val1.vc->MarkUsedDims(sec_dims2);
        if (dims1.Includes(sec_dims1) && dims2.Includes(sec_dims2)) {
          hash_descriptors.push_back(i);
          added = true;
        } else if (dims1.Includes(sec_dims2) && dims2.Includes(sec_dims1)) {
          cond[i].SwitchSides();
          hash_descriptors.push_back(i);
          added = true;
        }
      }
    }
    if (!added) {
      other_cond_exist_ = true;
      cond[i].DimensionUsed(dims_other);
      other_cond_.push_back(cond[i]);
    }
  }
  cond_hashed_ = int(hash_descriptors.size());
  if (cond_hashed_ == 0) {
    why_failed = JoinFailure::FAIL_HASH;
    return false;
  }
  /*
    Check the proper direction: the (much) smaller dimension should be
    traversed, the larger one should be matched but some special cases may
    change the direction. Rules:
    - traverse dim1 if it is 3 times smaller than dim2,
    - if neither is 3 times smaller than other, traverse the one which is less
    repeatable
  */
  bool switch_sides = false;
  int64_t dim1_size = mind->NumOfTuples(dims1);
  int64_t dim2_size = mind->NumOfTuples(dims2);
  if (std::min(dim1_size, dim2_size) > 100000) {  // approximate criteria for large tables (many packs)
    if (dim1_size > 2 * dim2_size)
      switch_sides = true;
    else if (2 * dim1_size > dim2_size) {  // within reasonable range - check
                                           // again whether to change sides
      int64_t dim1_distinct = cond[hash_descriptors[0]].attr.vc->GetApproxDistVals(false);  // exclude nulls
      int64_t dim2_distinct = cond[hash_descriptors[0]].val1.vc->GetApproxDistVals(false);
      // check whether there are any nontrivial differencies in distinct value
      // distributions
      if (dim1_distinct > dim1_size * 0.9 && dim2_distinct > dim2_size * 0.9) {
        if (dim1_size > dim2_size)  // no difference - just check table sizes
          switch_sides = true;
      } else if (double(dim1_distinct) / dim1_size <
                 double(dim2_distinct) / dim2_size)  // switch if dim1 has more repeating values
        switch_sides = true;
    }
  } else if (dim1_size > dim2_size)
    switch_sides = true;

  if (force_switching_sides_)
    switch_sides = !switch_sides;
  if (switch_sides) {
    for (int i = 0; i < cond_hashed_; i++)  // switch sides of joining conditions
      cond[hash_descriptors[i]].SwitchSides();
    traversed_dims_ = dims2;
    matched_dims_ = dims1;
  } else {
    traversed_dims_ = dims1;
    matched_dims_ = dims2;
  }

  // enlarge matched or traversed dimension lists by adding non-hashed
  // conditions, if any
  if (other_cond_exist_) {
    if (matched_dims_.Intersects(cond[0].right_dims) && !cond[0].right_dims.Includes(dims_other)) {
      // special case: do not add non-right-side other dimensions to right-side
      // matched
      dims_other.Minus(matched_dims_);
      traversed_dims_.Plus(dims_other);
    } else {
      // default: other dims as matched
      dims_other.Minus(traversed_dims_);
      matched_dims_.Plus(dims_other);
    }
  }
  // check whether we should take into account more dims
  mind->MarkInvolvedDimGroups(traversed_dims_);
  mind->MarkInvolvedDimGroups(matched_dims_);

  vc1_.resize(cond_hashed_);
  vc2_.resize(cond_hashed_);
  bool compatible = true;
  for (int i = 0; i < cond_hashed_; i++) {  // add all key columns
    vc1_[i] = cond[hash_descriptors[i]].attr.vc;
    vc2_[i] = cond[hash_descriptors[i]].val1.vc;
    compatible = AddKeyColumn(vc1_[i], vc2_[i]) && compatible;
  }

  if (traversed_dims_.Intersects(matched_dims_) || !compatible) {
    // both materialized - we should rather use a simple loop
    // could not prepare common encoding
    why_failed = JoinFailure::FAIL_HASH;
    return false;
  }
  // prepare columns for traversed dimension numbers in hash table
  int num_of_traversed_dims = 0;
  for (int i = 0; i < mind->NumOfDimensions(); i++) {
    if (traversed_dims_[i] && !(tips.count_only && !other_cond_exist_)) {  // storing tuples may be omitted if
                                                                           // (count_only_now && !other_cond_exist_)
      traversed_hash_column_[i] = cond_hashed_ + num_of_traversed_dims;    // jump over the joining key columns
      num_of_traversed_dims++;
      int bin_index_size = 4;
      if (mind->OrigSize(i) > 0x000000007FFFFF00)
        bin_index_size = 8;
      hash_table_tuple_size_.push_back(bin_index_size);
    }
  }

  int key_buf_width = 0;
  for (size_t index = 0; index < column_bin_encoder_.size(); ++index) {
    column_bin_encoder_[index].SetPrimaryOffset(key_buf_width);
    key_buf_width += hash_table_key_size_[index];
  }

  InitOuter(cond);

  return true;
}

bool ParallelHashJoiner::AddKeyColumn(vcolumn::VirtualColumn *vc, vcolumn::VirtualColumn *vc_matching) {
  size_t column_index = column_bin_encoder_.size();
  // Comparable, non-monotonic, non-decodable.
  column_bin_encoder_.push_back(ColumnBinEncoder(ColumnBinEncoder::ENCODER_IGNORE_NULLS));
  // common::CT::TIMESTAMP is omitted by ColumnValueEncoder::SecondColumn.
  vcolumn::VirtualColumn *second_column =
      (vc->Type().GetTypeName() == common::ColumnType::TIMESTAMP) ? nullptr : vc_matching;
  bool success = column_bin_encoder_[column_index].PrepareEncoder(vc, second_column);
  hash_table_key_size_.push_back(column_bin_encoder_[column_index].GetPrimarySize());
  return success;
}

void ParallelHashJoiner::ExecuteJoin() {
  MEASURE_FET("ParallelHashJoiner::ExecuteJoin(...)");

  int64_t joined_tuples = 0;

  // Preparing the new multiindex tables.
  MIIterator traversed_mit(mind, traversed_dims_);
  MIIterator match_mit(mind, matched_dims_);
  uint64_t traversed_dims_size = traversed_mit.NumOfTuples();
  uint64_t matched_dims_size = match_mit.NumOfTuples();

  uint64_t approx_join_size = traversed_dims_size;
  if (matched_dims_size > approx_join_size)
    approx_join_size = matched_dims_size;

  std::vector<bool> traverse_keys_unique;
  for (int i = 0; i < cond_hashed_; i++) traverse_keys_unique.push_back(vc1_[i]->IsDistinctInTable());

  mind->LockAllForUse();

  multi_index_builder_.reset(new MultiIndexBuilder(mind, tips));
  std::vector<DimensionVector> dims_involved = {traversed_dims_, matched_dims_};
  multi_index_builder_->Init(approx_join_size, dims_involved);

  actually_traversed_rows_ = 0;
  outer_tuples_ = 0;

  if (traversed_dims_size > 0 && matched_dims_size > 0) {
    int64_t outer_tuples = 0;
    TraverseDim(traversed_mit, &outer_tuples);

    outer_tuples_ += outer_tuples;

    if (too_many_conflicts_) {
      tianmu_control_.lock(m_conn->GetThreadID()) << "Too many hash conflicts: restarting join." << system::unlock;
      return;
    }

    joined_tuples += MatchDim(match_mit);

    if (watch_traversed_)
      outer_tuples_ += SubmitOuterTraversed();
  }

  int64_t outer_tuples_matched = 0;
  if (watch_matched_ && !outer_matched_filter_->IsEmpty())
    outer_tuples_matched = SubmitOuterMatched(match_mit);
  outer_tuples_ += outer_tuples_matched;

  if (outer_tuples_ > 0)
    tianmu_control_.lock(m_conn->GetThreadID())
        << "Added " << outer_tuples_ << " null tuples by outer join." << system::unlock;
  joined_tuples += outer_tuples_;
  // revert multiindex to the updated tables
  if (packrows_omitted_ > 0)
    tianmu_control_.lock(m_conn->GetThreadID())
        << "Roughly omitted " << int(packrows_omitted_ / double(packrows_matched_) * 10000.0) / 100.0 << "% packrows."
        << system::unlock;

  multi_index_builder_->Commit(joined_tuples, tips.count_only);

  // update local statistics - wherever possible
  int64_t traversed_dist_limit = actually_traversed_rows_;
  for (int i = 0; i < cond_hashed_; i++)
    if (!vc2_[i]->Type().IsString()) {  // otherwise different collations may be
                                        // a counterexample
      vc2_[i]->SetLocalDistVals(actually_traversed_rows_ + outer_tuples_matched);  // matched values: not more than the
                                                                                   // traversed ones in case of equality
      if (traverse_keys_unique[i])  // unique => there is no more rows than
                                    // distinct values
        traversed_dist_limit =
            std::min(traversed_dist_limit, vc2_[i]->GetApproxDistVals(false) + (outer_tuples_ - outer_tuples_matched));
    }
  for (int i = 0; i < mind->NumOfDimensions(); i++)
    if (traversed_dims_[i])
      table->SetVCDistinctVals(i,
                               traversed_dist_limit);  // all dimensions involved in traversed side
  mind->UnlockAllFromUse();
}

int64_t ParallelHashJoiner::TraverseDim(MIIterator &mit, int64_t *outer_tuples) {
  int64_t rows_count = mind->NumOfTuples(traversed_dims_);
  int availabled_packs = (int)((rows_count + (1 << pack_power_) - 1) >> pack_power_);

  std::string splitting_type("none");
  std::vector<MITaskIterator *> task_iterators;
  MIIterator::SliceCapability slice_capability = mit.GetSliceCapability();
  if (slice_capability.type == MIIterator::SliceCapability::Type::kFixed) {
    DEBUG_ASSERT(!slice_capability.slices.empty());
    splitting_type = "fixed";
    size_t slices_size = slice_capability.slices.size();
    int64_t rows_started = 0;
    for (size_t index = 0; index < slices_size; ++index) {
      MITaskIterator *iter = new MIFixedTaskIterator(pack_power_, mind, traversed_dims_, index, slices_size,
                                                     slice_capability.slices[index], rows_started, index);
      rows_started += slice_capability.slices[index];
      task_iterators.push_back(iter);
    }
  } else if ((slice_capability.type == MIIterator::SliceCapability::Type::kLinear) &&
             (availabled_packs > kTraversedPacksPerFragment * 2)) {
    int64_t origin_size = rows_count;
    for (int index = 0; index < mind->NumOfDimensions(); index++) {
      if (traversed_dims_[index]) {
        origin_size = std::max<int64_t>(origin_size, mind->OrigSize(index));
      }
    }

    splitting_type = "packs";
    int packs_count = (int)((origin_size + (1 << pack_power_) - 1) >> pack_power_);
    int split_count = EvaluateTraversedFragments(packs_count);
    int packs_per_fragment = packs_count / split_count;
    int64_t rows_length = origin_size / split_count;
    for (int index = 0; index < split_count; ++index) {
      int packs_started = index * packs_per_fragment;
      if (packs_started >= packs_count)
        break;

      int packs_increased = (index == split_count - 1) ? (-1 - packs_started) : (packs_per_fragment - 1);

      MITaskIterator *iter = new MILinearPackTaskIterator(pack_power_, mind, traversed_dims_, index, split_count,
                                                          rows_length, packs_started, packs_started + packs_increased);
      task_iterators.push_back(iter);
    }
  } else {
    MITaskIterator *iter = new MITaskIterator(mind, traversed_dims_, 0, 1, rows_count);
    task_iterators.push_back(iter);
  }

  int traversed_fragment_count = (int)task_iterators.size();
  tianmu_control_.lock(m_conn->GetThreadID()) << "Begin traversed with " << traversed_fragment_count << " threads with "
                                              << splitting_type << " type." << system::unlock;

  std::vector<TraverseTaskParams> traverse_task_params;
  traverse_task_params.reserve(task_iterators.size());
  traversed_hash_tables_.reserve(task_iterators.size());

  // Preload packs data of TempTable.
  TempTablePackLocker temptable_pack_locker(vc1_, cond_hashed_, availabled_packs);

  int64_t traversed_rows = 0;
  bool no_except = true;
  utils::result_set<int64_t> res;
  try {
    for (MITaskIterator *iter : task_iterators) {
      auto &ht = traversed_hash_tables_.emplace_back(hash_table_key_size_, hash_table_tuple_size_,
                                                     iter->GetRowsLength(), pack_power_, watch_traversed_);

      ht.AssignColumnEncoder(column_bin_encoder_);

      auto &params = traverse_task_params.emplace_back();
      params.traversed_hash_table = &ht;
      params.build_item = multi_index_builder_->CreateBuildItem();
      params.task_miter = iter;

      res.insert(ha_tianmu_engine_->query_thread_pool.add_task(&ParallelHashJoiner::AsyncTraverseDim, this, &params));
    }
  } catch (std::exception &e) {
    res.get_all_with_except();
    throw e;
  } catch (...) {
    res.get_all_with_except();
    throw;
  }

  for (size_t i = 0; i < res.size(); i++) try {
      traversed_rows += res.get(i);
    } catch (std::exception &e) {
      no_except = false;
      TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
    } catch (...) {
      no_except = false;
      TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
    }
  if (!no_except) {
    throw common::Exception("Parallel hash join failed.");
  }

  if (m_conn->Killed())
    throw common::KilledException();

  tianmu_control_.lock(m_conn->GetThreadID())
      << "End traversed " << traversed_rows << "/" << rows_count << " rows." << system::unlock;

  for (auto &params : traverse_task_params) {
    if (params.too_many_conflicts && !tianmu_sysvar_join_disable_switch_side) {
      if (!force_switching_sides_ && !too_many_conflicts_)
        too_many_conflicts_ = true;
    }

    if (params.no_space_left)
      tianmu_control_.lock(m_conn->GetThreadID()) << "No space left of hash table. " << system::unlock;

    *outer_tuples += params.outer_tuples;
    multi_index_builder_->AddBuildItem(params.build_item);
  }

  for (int index = 0; index < cond_hashed_; ++index) {
    vc1_[index]->UnlockSourcePacks();
  }

  return traversed_rows;
}

int64_t ParallelHashJoiner::AsyncTraverseDim(TraverseTaskParams *params) {
  params->traversed_hash_table->Initialize();

  HashTable *hash_table = params->traversed_hash_table->hash_table();

  std::string key_input_buffer(hash_table->GetKeyBufferWidth(), 0);

  MIIterator &miter(*params->task_miter->GetIter());

  int traversed_rows = 0;

  while (params->task_miter->IsValid()) {
    if (m_conn->Killed())
      break;

    if (miter.PackrowStarted()) {
      for (int index = 0; index < cond_hashed_; ++index) vc1_[index]->LockSourcePacks(miter);
    }

    // Put key value to encoding.
    bool omit_this_row = false;
    for (int index = 0; index < cond_hashed_; index++) {
      if (vc1_[index]->IsNull(miter)) {
        omit_this_row = true;
        break;
      }

      ColumnBinEncoder *column_bin_encoder = params->traversed_hash_table->GetColumnEncoder(index);
      column_bin_encoder->Encode(reinterpret_cast<unsigned char *>(key_input_buffer.data()), miter, nullptr, true);
    }

    if (!omit_this_row) {  // else go to the next row - equality cannot be
                           // fulfilled
      int64_t hash_row = hash_table->AddKeyValue(key_input_buffer, &params->too_many_conflicts);
      if (hash_row == common::NULL_VALUE_64) {
        params->no_space_left = true;
        break;  // no space left - stop for now and then restart from the
                // current row
      }

      if (!force_switching_sides_ && params->too_many_conflicts && !tianmu_sysvar_join_disable_switch_side)
        break;  // and exit the function

      if (watch_traversed_)
        params->traversed_hash_table->outer_filter()->Set(hash_row);

      actually_traversed_rows_++;

      // Put the tuple column. Note: needed also for count_only_now, because
      // another conditions may need it.
      if (!tips.count_only || other_cond_exist_) {
        for (int index = 0; index < mind->NumOfDimensions(); ++index)
          if (traversed_dims_[index])
            hash_table->SetTupleValue(traversed_hash_column_[index], hash_row, miter[index]);
      }
    } else if (watch_traversed_) {
      for (int index = 0; index < mind->NumOfDimensions(); ++index) {
        if (matched_dims_[index]) {
          params->build_item->SetTableValue(index, common::NULL_VALUE_64);
        } else if (traversed_dims_[index]) {
          params->build_item->SetTableValue(index, miter[index]);
        }
      }

      params->build_item->CommitTableValues();

      actually_traversed_rows_++;
      params->outer_tuples++;
    }
    ++miter;
    traversed_rows++;
  }

  params->build_item->Finish();

  return traversed_rows;
}

bool ParallelHashJoiner::CreateMatchingTasks(MIIterator &mit, int64_t rows_count,
                                             std::vector<MITaskIterator *> *task_iterators,
                                             std::string *splitting_type) {
  if (other_cond_exist_) {
    MITaskIterator *iter = new MITaskIterator(mind, matched_dims_, 0, 1, 0);
    task_iterators->push_back(iter);
    return true;
  }

  MIIterator::SliceCapability slice_capability = mit.GetSliceCapability();
  for (auto &j : other_cond_) {
    if (j.IsType_Subquery()) {
      slice_capability.type = MIIterator::SliceCapability::Type::kDisable;
      break;
    }
  }
  if (slice_capability.type == MIIterator::SliceCapability::Type::kFixed) {
    DEBUG_ASSERT(!slice_capability.slices.empty());
    size_t slices_size = slice_capability.slices.size();
    int64_t rows_started = 0;
    for (size_t index = 0; index < slices_size; ++index) {
      MITaskIterator *iter = new MIFixedTaskIterator(pack_power_, mind, matched_dims_, index, slices_size,
                                                     slice_capability.slices[index], rows_started, index);
      rows_started += slice_capability.slices[index];
      task_iterators->push_back(iter);
    }
    *splitting_type = "fixed";
  } else if (slice_capability.type == MIIterator::SliceCapability::Type::kLinear) {
    int packs_count = (int)((rows_count + (1 << pack_power_) - 1) >> pack_power_);
    for (int index = 0; index < mind->NumOfDimensions(); index++) {
      if (matched_dims_[index]) {
        Filter *filter = mind->GetFilter(index);
        if (filter)
          packs_count = filter->NumOfBlocks();
      }
    }

    if (packs_count > kJoinSplittedMinPacks) {
      // Splitting using packs.
      int split_count = (tianmu_sysvar_join_parallel > 1) ? tianmu_sysvar_join_parallel
                                                          : EvaluateMatchedFragmentsWithPacks(packs_count);
      int packs_per_fragment = (packs_count + kJoinSplittedMinPacks) / split_count;
      for (int index = 0; index < split_count; ++index) {
        int packs_started = index * packs_per_fragment;
        if (packs_started >= packs_count)
          break;

        int packs_increased = (index == split_count - 1) ? (-1 - packs_started) : (packs_per_fragment - 1);
        MITaskIterator *iter = new MILinearPackTaskIterator(pack_power_, mind, matched_dims_, index, split_count, 0,
                                                            packs_started, packs_started + packs_increased);
        task_iterators->push_back(iter);
      }
      *splitting_type = "packs";
    } else if (tianmu_sysvar_join_splitrows > 0) {
      // Splitting using rows.
      uint64_t origin_size = rows_count;
      for (int index = 0; index < mind->NumOfDimensions(); index++) {
        if (matched_dims_[index]) {
          origin_size = std::max(origin_size, mind->OrigSize(index));
        }
      }

      int64_t rows_unit = (1 << pack_power_) / 4;
      int split_count = (tianmu_sysvar_join_splitrows > 1)
                            ? tianmu_sysvar_join_splitrows
                            : EvaluateMatchedFragmentsWithRows(pack_power_, rows_unit, origin_size);
      int64_t rows_per_fragment = (origin_size + rows_unit) / split_count;
      for (int index = 0; index < split_count; ++index) {
        int rows_started = index * rows_per_fragment;
        int rows_increased = (index == split_count - 1) ? (-1 - rows_started) : (rows_per_fragment - 1);
        MITaskIterator *iter = new MILinearRowTaskIterator(mind, matched_dims_, index, split_count, 0, rows_started,
                                                           rows_started + rows_increased);
        task_iterators->push_back(iter);
      }
      *splitting_type = "rows";
    } else {
      MITaskIterator *iter = new MITaskIterator(mind, matched_dims_, 0, 1, 0);
      task_iterators->push_back(iter);
    }
  } else {
    MITaskIterator *iter = new MITaskIterator(mind, matched_dims_, 0, 1, 0);
    task_iterators->push_back(iter);
  }

  return true;
}

int64_t ParallelHashJoiner::MatchDim(MIIterator &mit) {
  int64_t rows_count = mind->NumOfTuples(matched_dims_);

  std::string splitting_type("none");
  std::vector<MITaskIterator *> task_iterators;
  CreateMatchingTasks(mit, rows_count, &task_iterators, &splitting_type);

  tianmu_control_.lock(m_conn->GetThreadID())
      << "Begin match dim of " << rows_count << " rows, spliting into " << task_iterators.size() << " threads with "
      << splitting_type << " type." << system::unlock;

  auto &ftht = traversed_hash_tables_[0];

  // Preload packs data of TempTable.
  int availabled_packs = (int)((rows_count + (1 << pack_power_) - 1) >> pack_power_);
  TempTablePackLocker temptable_pack_locker(vc2_, cond_hashed_, availabled_packs);

  std::vector<MatchTaskParams> match_task_params;
  match_task_params.reserve(task_iterators.size());
  int64_t matched_rows = 0;
  if (task_iterators.size() > 1) {
    bool no_except = true;
    utils::result_set<int64_t> res;
    try {
      for (MITaskIterator *iter : task_iterators) {
        auto &params = match_task_params.emplace_back();
        ftht.GetColumnEncoder(&params.column_bin_encoder);
        params.build_item = multi_index_builder_->CreateBuildItem();
        params.task_miter = iter;

        res.insert(ha_tianmu_engine_->query_thread_pool.add_task(&ParallelHashJoiner::AsyncMatchDim, this, &params));
      }
    } catch (std::exception &e) {
      res.get_all_with_except();
      throw e;
    } catch (...) {
      res.get_all_with_except();
      throw;
    }
    for (size_t i = 0; i < res.size(); i++) try {
        matched_rows += res.get(i);
      } catch (std::exception &e) {
        no_except = false;
        TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
      } catch (...) {
        no_except = false;
        TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
      }
    if (!no_except) {
      throw common::Exception("Parallel hash join failed.");
    }
  } else if (task_iterators.size() == 1) {
    auto &params = match_task_params.emplace_back();
    ftht.GetColumnEncoder(&params.column_bin_encoder);
    params.build_item = multi_index_builder_->CreateBuildItem();
    params.task_miter = *task_iterators.begin();
    matched_rows = AsyncMatchDim(&params);
  }

  if (m_conn->Killed())
    throw common::KilledException();

  tianmu_control_.lock(m_conn->GetThreadID())
      << "End match dim. Produced tuples:" << matched_rows << "/" << rows_count << system::unlock;

  for (auto &params : match_task_params) {
    multi_index_builder_->AddBuildItem(params.build_item);
  }

  for (int index = 0; index < cond_hashed_; ++index) {
    vc2_[index]->UnlockSourcePacks();
  }
  for (auto &j : other_cond_) j.UnlockSourcePacks();

  return matched_rows;
}

int64_t ParallelHashJoiner::AsyncMatchDim(MatchTaskParams *params) {
  MEASURE_FET("ParallelHashJoiner::AsyncMatchDim(...)");

  DEBUG_ASSERT(!traversed_hash_tables_.empty());

  // ftht: first_traversed_hash_table.
  auto &ftht = traversed_hash_tables_[0];
  std::vector<ColumnBinEncoder> &column_bin_encoder(params->column_bin_encoder);

  std::string key_input_buffer(ftht.hash_table()->GetKeyBufferWidth(), 0);
  MIIterator &miter(*params->task_miter->GetIter());

  int64_t joined_tuples = 0;
  int64_t hash_row = 0;

  int64_t matching_row = params->task_miter->GetStartPackrows();
  MIDummyIterator combined_mit(mind);  // a combined iterator for checking non-hashed conditions, if any

  while (params->task_miter->IsValid() && !interrupt_matching_) {
    if (m_conn->Killed())
      break;

    // Rough and locking part
    bool omit_this_packrow = false;
    bool packrow_uniform = false;  // if the packrow is uniform, process it massively
    if (miter.PackrowStarted()) {
      packrow_uniform = true;

      for (int index = 0; index < cond_hashed_; ++index) {
        if (column_bin_encoder[index].IsString()) {
          if (!vc2_[index]->Type().Lookup()) {  // lookup treated as string, when the
                                                // dictionaries aren't convertible
            types::BString local_min = vc2_[index]->GetMinString(miter);
            types::BString local_max = vc2_[index]->GetMaxString(miter);
            if (!local_min.IsNull() && !local_max.IsNull() && ImpossibleValues(index, local_min, local_max)) {
              omit_this_packrow = true;
              break;
            }
          }
          packrow_uniform = false;
        } else {
          int64_t local_min = vc2_[index]->GetMinInt64(miter);
          int64_t local_max = vc2_[index]->GetMaxInt64(miter);
          if (local_min == common::NULL_VALUE_64 || local_max == common::NULL_VALUE_64 ||  // common::NULL_VALUE_64
                                                                                           // only for nulls only
              ImpossibleValues(index, local_min, local_max)) {
            omit_this_packrow = true;
            break;
          }
          if (other_cond_exist_ || local_min != local_max || vc2_[index]->IsNullsPossible()) {
            packrow_uniform = false;
          }
        }
      }

      packrows_matched_++;

      if (packrow_uniform && !omit_this_packrow) {
        for (int index = 0; index < cond_hashed_; ++index) {
          int64_t local_min = vc2_[index]->GetMinInt64(miter);
          column_bin_encoder[index].PutValue64(reinterpret_cast<unsigned char *>(key_input_buffer.data()), local_min,
                                               true, false);
        }

        for (auto &traversed_hash_table : traversed_hash_tables_) {
          HashTable *hash_table = traversed_hash_table.hash_table();
          HashTable::Finder hash_table_finder(hash_table, &key_input_buffer);
          int64_t matching_rows = hash_table_finder.GetMatchedRows() * miter.GetPackSizeLeft();
          if (!tips.count_only)
            while ((hash_row = hash_table_finder.GetNextRow()) != common::NULL_VALUE_64) {
              MIIterator mit_this_pack(miter);
              int64_t matching_this_pack = matching_row;
              do {
                SubmitJoinedTuple(params->build_item.get(), &traversed_hash_table, hash_row, mit_this_pack);
                if (watch_matched_) {
                  outer_matched_filter_->ResetDelayed(matching_this_pack, true);
                }
                ++mit_this_pack;
                matching_this_pack++;
              } while (params->task_miter->IsValid(&mit_this_pack) && !mit_this_pack.PackrowStarted());
            }
          else if (watch_traversed_) {
            while ((hash_row = hash_table_finder.GetNextRow()) != common::NULL_VALUE_64) {
              traversed_hash_table.outer_filter()->Reset(hash_row, true);
            }
          }
          joined_tuples += matching_rows;
        }

        omit_this_packrow = true;
      }
      if (omit_this_packrow) {
        matching_row += miter.GetPackSizeLeft();
        miter.NextPackrow();
        packrows_omitted_++;
        continue;  // here we are jumping out for impossible or uniform packrow
      }

      for (int i = 0; i < cond_hashed_; i++) vc2_[i]->LockSourcePacks(miter);
    }
    // Exact part - make the key row ready for comparison
    bool null_found = false;
    bool non_matching_sizes = false;
    for (int index = 0; index < cond_hashed_; ++index) {
      if (vc2_[index]->IsNull(miter)) {
        null_found = true;
        break;
      }
      column_bin_encoder[index].Encode(reinterpret_cast<unsigned char *>(key_input_buffer.data()), miter, vc2_[index]);
    }

    if (!null_found && !non_matching_sizes) {  // else go to the next row -
                                               // equality cannot be fulfilled
      for (auto &traversed_hash_table : traversed_hash_tables_) {
        HashTable *hash_table = traversed_hash_table.hash_table();
        HashTable::Finder hash_table_finder(hash_table, &key_input_buffer);
        int64_t matching_rows = hash_table_finder.GetMatchedRows();
        // Find all matching rows
        if (!other_cond_exist_) {
          // Basic case - just equalities
          if (!tips.count_only)
            while ((hash_row = hash_table_finder.GetNextRow()) != common::NULL_VALUE_64)
              SubmitJoinedTuple(params->build_item.get(), &traversed_hash_table, hash_row, miter);
          else if (watch_traversed_) {
            while ((hash_row = hash_table_finder.GetNextRow()) != common::NULL_VALUE_64) {
              traversed_hash_table.outer_filter()->Reset(hash_row, true);
            }
          }
          if (watch_matched_ && matching_rows > 0) {
            outer_matched_filter_->ResetDelayed(matching_row, true);
          }
          joined_tuples += matching_rows;
        } else {
          // Complex case: different types of join conditions mixed together
          combined_mit.Combine(miter);
          while ((hash_row = hash_table_finder.GetNextRow()) != common::NULL_VALUE_64) {
            bool other_cond_true = true;
            for (int i = 0; i < mind->NumOfDimensions(); i++) {
              if (traversed_dims_[i])
                combined_mit.Set(i, hash_table->GetTupleValue(traversed_hash_column_[i], hash_row));
            }
            for (auto &j : other_cond_) {
              j.LockSourcePacks(combined_mit);
              if (j.CheckCondition(combined_mit) == false) {
                other_cond_true = false;
                break;
              }
            }
            if (other_cond_true) {
              if (!tips.count_only)
                SubmitJoinedTuple(params->build_item.get(), &traversed_hash_table, hash_row,
                                  miter);  // use the multiindex iterator position
              else if (watch_traversed_) {
                traversed_hash_table.outer_filter()->Reset(hash_row, true);
              }

              joined_tuples++;
              if (watch_matched_) {
                outer_matched_filter_->ResetDelayed(matching_row, true);
              }
            }
          }
        }
      }

      if (!outer_nulls_only_) {
        if (tips.limit != -1 && tips.limit <= joined_tuples) {
          interrupt_matching_ = true;
          break;
        }
      }
    }
    ++miter;
    matching_row++;
  }

  if (watch_matched_)
    outer_matched_filter_->Commit(true);  // Commit the delayed resetsC.

  params->build_item->Finish();

  if (outer_nulls_only_)
    joined_tuples = 0;  // outer tuples added later
  return joined_tuples;
}

void ParallelHashJoiner::SubmitJoinedTuple(MultiIndexBuilder::BuildItem *build_item,
                                           TraversedHashTable *traversed_hash_table, int64_t hash_row,
                                           MIIterator &mit) {
  if (watch_traversed_) {
    traversed_hash_table->outer_filter()->Reset(hash_row, true);
  }
  // assumption: SetNewTableValue is called once for each dimension involved (no
  // integrity checking)
  if (!outer_nulls_only_) {
    HashTable *hash_table = traversed_hash_table->hash_table();
    for (int index = 0; index < mind->NumOfDimensions(); ++index) {
      if (matched_dims_[index]) {
        build_item->SetTableValue(index, mit[index]);
      } else if (traversed_dims_[index]) {
        build_item->SetTableValue(index, hash_table->GetTupleValue(traversed_hash_column_[index], hash_row));
      }
    }
    build_item->CommitTableValues();
  }
}

// outer part

void ParallelHashJoiner::InitOuter(Condition &cond) {
  DimensionVector outer_dims(cond[0].right_dims);  // outer_dims will be filled with nulls for
                                                   // non-matching tuples
  if (!outer_dims.IsEmpty()) {
    if (traversed_dims_.Includes(outer_dims)) {
      // Watch the non-outer dim for unmatched tuples and add them with nulls on
      // outer dim.
      watch_matched_ = true;

      uint64_t origin_size = mind->NumOfTuples(matched_dims_);
      for (int index = 0; index < mind->NumOfDimensions(); index++) {
        if (matched_dims_[index]) {
          origin_size = std::max(origin_size, mind->OrigSize(index));
        }
      }

      outer_matched_filter_.reset(new MutexFilter(origin_size, pack_power_, true));
    } else if (matched_dims_.Includes(outer_dims)) {
      watch_traversed_ = true;
    }
    outer_nulls_only_ = true;
    for (int j = 0; j < outer_dims.Size(); j++)
      if (outer_dims[j] && tips.null_only[j] == false)
        outer_nulls_only_ = false;
  }
}

int64_t ParallelHashJoiner::SubmitOuterTraversed() {
  MEASURE_FET("ParallelHashJoiner::SubmitOuterTraversed(...)");

  std::shared_ptr<MultiIndexBuilder::BuildItem> build_item = multi_index_builder_->CreateBuildItem();
  DEBUG_ASSERT(build_item);

  int64_t outer_added = 0;
  for (auto &it : traversed_hash_tables_) {
    MutexFilter *outer_filter = it.outer_filter();
    if (tips.count_only) {
      outer_added += outer_filter->GetOnesCount();
    } else {
      HashTable *hash_table = it.hash_table();
      for (int64_t hash_row = 0; hash_row < hash_table->GetCount(); ++hash_row) {
        if (outer_filter->Get(hash_row)) {
          for (int index = 0; index < mind->NumOfDimensions(); ++index) {
            if (matched_dims_[index])
              build_item->SetTableValue(index, common::NULL_VALUE_64);
            else if (traversed_dims_[index])
              build_item->SetTableValue(index, hash_table->GetTupleValue(traversed_hash_column_[index], hash_row));
          }
          build_item->CommitTableValues();
          outer_added++;
          actually_traversed_rows_++;
        }
      }
    }
  }

  build_item->Finish();

  multi_index_builder_->AddBuildItem(build_item);

  return outer_added;
}

struct ParallelHashJoiner::OuterMatchedParams {
  MITaskIterator *task_iter = nullptr;
  std::shared_ptr<MultiIndexBuilder::BuildItem> build_item;  // External pointer.
  int64_t outer_added = 0;

  ~OuterMatchedParams() {
    if (task_iter) {
      delete task_iter;
      task_iter = nullptr;
    }
  }
};

void ParallelHashJoiner::AsyncSubmitOuterMatched(OuterMatchedParams *params, MutexFilter *outer_matched_filter) {
  MIIterator &miter(*params->task_iter->GetIter());

  int64_t matched_row = params->task_iter->GetStartPackrows();
  while (params->task_iter->IsValid()) {
    if (outer_matched_filter->Get(matched_row)) {  // if the matched tuple is still unused, add it
                                                   // with nulls
      for (int index = 0; index < mind->NumOfDimensions(); ++index) {
        if (matched_dims_[index])
          params->build_item->SetTableValue(index, miter[index]);
        else if (traversed_dims_[index])
          params->build_item->SetTableValue(index, common::NULL_VALUE_64);
      }
      params->build_item->CommitTableValues();
      params->outer_added++;
    }
    ++miter;
    matched_row++;
  }

  params->build_item->Finish();
}

int64_t ParallelHashJoiner::SubmitOuterMatched(MIIterator &miter) {
  MEASURE_FET("ParallelHashJoiner::SubmitOuterMatched(...)");
  // miter - an iterator through the matched dimensions
  DEBUG_ASSERT(outer_matched_filter_ && watch_matched_);
  if (tips.count_only)
    return outer_matched_filter_->GetOnesCount();

  int64_t rows_count = mind->NumOfTuples(matched_dims_);
  std::string splitting_type("none");
  std::vector<MITaskIterator *> task_iterators;
  CreateMatchingTasks(miter, rows_count, &task_iterators, &splitting_type);

  std::vector<OuterMatchedParams> outer_matched_params;
  outer_matched_params.reserve(task_iterators.size());
  utils::result_set<void> res;
  try {
    for (MITaskIterator *iter : task_iterators) {
      auto &params = outer_matched_params.emplace_back();
      params.task_iter = iter;
      params.build_item = multi_index_builder_->CreateBuildItem();

      res.insert(ha_tianmu_engine_->query_thread_pool.add_task(&ParallelHashJoiner::AsyncSubmitOuterMatched, this,
                                                               &params, outer_matched_filter_.get()));
    }
  } catch (std::exception &e) {
    res.get_all_with_except();
    throw e;
  } catch (...) {
    res.get_all_with_except();
    throw;
  }
  res.get_all_with_except();

  int64_t outer_added = 0;
  for (auto &params : outer_matched_params) {
    multi_index_builder_->AddBuildItem(params.build_item);
    outer_added += params.outer_added;
  }

  return outer_added;
}

std::unique_ptr<TwoDimensionalJoiner> CreateHashJoiner(MultiIndex *multi_index, TempTable *temp_table,
                                                       JoinTips &join_tips) {
  TwoDimensionalJoiner *joiner = nullptr;
  if (tianmu_sysvar_join_parallel > 0) {
    if (tianmu_sysvar_async_join_setting.is_enabled() && GetTaskExecutor()) {
      joiner = new ProxyHashJoiner(multi_index, temp_table, join_tips);
    } else {
      joiner = new ParallelHashJoiner(multi_index, temp_table, join_tips);
    }
  } else {
    joiner = new JoinerHash(multi_index, temp_table, join_tips);
  }
  return std::unique_ptr<TwoDimensionalJoiner>(joiner);
}
}  // namespace core
}  // namespace Tianmu
