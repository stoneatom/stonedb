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

#include <atomic>
#include <vector>

#include "base/core/future_util.h"
#include "base/core/print.h"
#include "base/util/defer.h"
#include "common/assert.h"
#include "common/exception.h"
#include "core/column_bin_encoder.h"
#include "core/engine.h"
#include "core/join_thread_table.h"
#include "core/mi_step_iterator.h"
#include "core/multi_index_builder.h"
#include "core/task_executor.h"
#include "core/temp_table.h"
#include "core/transaction.h"
#include "proxy_hash_joiner.h"
#include "system/fet.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
namespace stdexp = std::experimental;

using ColumnBinEncoderPtr = std::shared_ptr<std::vector<ColumnBinEncoder>>;

// Poll one pack from MIIterator every time.
class MIIteratorPoller {
 public:
  MIIteratorPoller(std::shared_ptr<MIIterator> miter, int start_pack = 0) : miter_(miter), cur_pos_(start_pack) {
    slice_capability_ = miter_->GetSliceCapability();

    origin_size_ = miter_->NumOfTuples();
    for (int index = 0; index < miter_->NumOfDimensions(); ++index) {
      if (miter_->DimUsed(index)) {
        origin_size_ = std::max<int64_t>(origin_size_, miter_->OrigSize(index));
      }
    }

    slice_type_ = "none";
    if (slice_capability_.type != MIIterator::SliceCapability::Type::kDisable) {
      if (slice_capability_.type == MIIterator::SliceCapability::Type::kLinear) {
        if (tianmu_sysvar_async_join_setting.pack_per_step > 0)
          slice_type_ = base::sprint("per %d packs", tianmu_sysvar_async_join_setting.pack_per_step);
        else {
          DEBUG_ASSERT(tianmu_sysvar_async_join_setting.rows_per_step > 0);
          slice_type_ = base::sprint("per %d rows", tianmu_sysvar_async_join_setting.rows_per_step);
        }
      } else {
        slice_type_ = "fixed";
      }
    }
  }

  std::string GetSliceType() const { return slice_type_; }

  std::shared_ptr<MIIterator> Poll() {
    if (no_more_)
      return std::shared_ptr<MIIterator>();

    std::shared_ptr<MIIterator> pack_iter;
    if (slice_capability_.type != MIIterator::SliceCapability::Type::kDisable) {
      size_t actual_pos = cur_pos_;
      size_t step = 1;
      int64_t sentry_pos = slice_capability_.slices.size();
      if (slice_capability_.type == MIIterator::SliceCapability::Type::kLinear) {
        DEBUG_ASSERT(tianmu_sysvar_async_join_setting.pack_per_step > 0 ||
                     tianmu_sysvar_async_join_setting.rows_per_step > 0);
        // Preferred iterating by pack.
        if (tianmu_sysvar_async_join_setting.pack_per_step > 0) {
          pack_iter.reset(new MIPackStepIterator(cur_pos_, tianmu_sysvar_async_join_setting.pack_per_step, *miter_));
          step = tianmu_sysvar_async_join_setting.pack_per_step;
          // Get actual pack, because RewindToPack of MIIterator will skip the
          // un-hitted package.
          for (auto index : boost::irange<int>(0, pack_iter->NumOfDimensions())) {
            if (pack_iter->DimUsed(index)) {
              actual_pos = pack_iter->GetCurPackrow(index);
              break;
            }
          }
        } else {
          DEBUG_ASSERT(tianmu_sysvar_async_join_setting.rows_per_step > 0);

          sentry_pos = origin_size_;

          pack_iter.reset(
              new MIRowsStepIterator(cur_pos_, tianmu_sysvar_async_join_setting.rows_per_step, origin_size_, *miter_));
          if (pack_iter->IsValid()) {
            step = tianmu_sysvar_async_join_setting.rows_per_step;
            // Get actual pack, because RewindToPack of MIIterator will skip the
            // un-hitted package.
            for (auto index : boost::irange<int>(0, pack_iter->NumOfDimensions())) {
              if (pack_iter->DimUsed(index)) {
                actual_pos = pack_iter->operator[](index);
                break;
              }
            }
          }
        }
      } else {
        DEBUG_ASSERT(slice_capability_.type == MIIterator::SliceCapability::Type::kFixed);
        pack_iter.reset(new MIStepIterator(cur_pos_, tianmu_sysvar_async_join_setting.pack_per_step, *miter_));
      }

      // Get next pack.
      cur_pos_ = actual_pos + step;

      no_more_ = !pack_iter->IsValid() || (cur_pos_ >= sentry_pos);
    } else {
      pack_iter = miter_;
      no_more_ = true;
    }

    return pack_iter;
  }

 private:
  std::shared_ptr<MIIterator> miter_;
  bool no_more_ = false;
  MIIterator::SliceCapability slice_capability_;
  int64_t cur_pos_;
  int64_t origin_size_;
  std::string slice_type_;
};

class ProxyHashJoiner::Action {
  using DescriptorsPtr = std::shared_ptr<std::vector<Descriptor>>;

 public:
  Action(ProxyHashJoiner *parent, bool force_switching_sides)
      : parent_(parent), mind_(parent->mind), force_switching_sides_(force_switching_sides) {
    pack_power_ = mind_->ValueOfPower();
    int dim_count = mind_->NumOfDimensions();
    traversed_dims_ = DimensionVector(dim_count);
    matched_dims_ = DimensionVector(dim_count);
    traversed_hash_column_.resize(dim_count, -1);
  }

  ~Action() = default;

  bool Init(Condition &cond) {
    auto result_defer = base::defer([this] { parent_->why_failed = JoinFailure::FAIL_HASH; });

    std::vector<int> hash_descriptors;
    bool first_found = true;
    DimensionVector dims1(mind_->NumOfDimensions());  // Initial dimension descriptions
    DimensionVector dims2(mind_->NumOfDimensions());
    // Dimensions for other conditions, if needed.
    DimensionVector dims_other(mind_->NumOfDimensions());
    for (uint index = 0; index < cond.Size(); ++index) {
      bool added = false;
      if (cond[index].IsType_JoinSimple() && cond[index].op == common::Operator::O_EQ) {
        if (first_found) {
          hash_descriptors.push_back(index);
          added = true;
          cond[index].attr.vc->MarkUsedDims(dims1);
          cond[index].val1.vc->MarkUsedDims(dims2);
          mind_->MarkInvolvedDimGroups(dims1);
          mind_->MarkInvolvedDimGroups(dims2);
          // Add dimensions for nested outer joins
          if (dims1.Intersects(cond[index].right_dims))
            dims1.Plus(cond[index].right_dims);
          if (dims2.Intersects(cond[index].right_dims))
            dims2.Plus(cond[index].right_dims);
          first_found = false;
        } else {
          // Make sure the local descriptions are compatible.
          DimensionVector sec_dims1(mind_->NumOfDimensions());
          DimensionVector sec_dims2(mind_->NumOfDimensions());
          cond[index].attr.vc->MarkUsedDims(sec_dims1);
          cond[index].val1.vc->MarkUsedDims(sec_dims2);
          if (dims1.Includes(sec_dims1) && dims2.Includes(sec_dims2)) {
            hash_descriptors.push_back(index);
            added = true;
          } else if (dims1.Includes(sec_dims2) && dims2.Includes(sec_dims1)) {
            cond[index].SwitchSides();
            hash_descriptors.push_back(index);
            added = true;
          }
        }
      }
      if (!added) {
        other_cond_exist_ = true;
        cond[index].DimensionUsed(dims_other);
        other_cond_.push_back(cond[index]);
      }
    }

    if (hash_descriptors.empty())
      return false;
    cond_hashed_ = hash_descriptors.size();

    /*
                Check the proper direction: the (much) smaller dimension should
       be traversed, the larger one should be matched but some special cases may
       change the direction. Rules:
                - traverse dim1 if it is 3 times smaller than dim2,
                - if neither is 3 times smaller than other, traverse the one
       which is less repeatable
        */
    bool switch_sides = false;
    int64_t dim1_size = mind_->NumOfTuples(dims1);
    int64_t dim2_size = mind_->NumOfTuples(dims2);
    if (std::min(dim1_size, dim2_size) > 100000) {
      // Approximate criteria for large tables (many packs).
      if (dim1_size > 2 * dim2_size)
        switch_sides = true;
      else if (2 * dim1_size > dim2_size) {
        // Within reasonable range check again whether to change sides.
        int64_t dim1_distinct = cond[hash_descriptors[0]].attr.vc->GetApproxDistVals(false);
        int64_t dim2_distinct = cond[hash_descriptors[0]].val1.vc->GetApproxDistVals(false);
        /*
        check whether there are any nontrivial differencies in distinct
                value distributions
        */
        if (dim1_distinct > dim1_size * 0.9 && dim2_distinct > dim2_size * 0.9) {
          // No difference - just check table sizes.
          if (dim1_size > dim2_size)
            switch_sides = true;
        } else if (double(dim1_distinct) / dim1_size < double(dim2_distinct) / dim2_size) {
          // Switch if dim1 has more repeating values.
          switch_sides = true;
        }
      }
    } else if (dim1_size > dim2_size) {
      switch_sides = true;
    }

    if (force_switching_sides_)
      switch_sides = !switch_sides;
    if (switch_sides) {
      // Switch sides of joining conditions.
      for (size_t index = 0; index < cond_hashed_; ++index) cond[hash_descriptors[index]].SwitchSides();
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
        // special case: do not add non-right-side other dimensions to
        // right-side matched
        dims_other.Minus(matched_dims_);
        traversed_dims_.Plus(dims_other);
      } else {
        // default: other dims as matched
        dims_other.Minus(traversed_dims_);
        matched_dims_.Plus(dims_other);
      }
    }
    // Check whether we should take into account more dims.
    mind_->MarkInvolvedDimGroups(traversed_dims_);
    mind_->MarkInvolvedDimGroups(matched_dims_);

    vc1_.resize(cond_hashed_);
    vc2_.resize(cond_hashed_);

    // Add all key columns.
    std::vector<ColumnBinEncoder> column_encoder;
    std::vector<int> hash_table_key_size;
    int primary_offset = 0;
    bool compatible = true;
    for (size_t index = 0; index < cond_hashed_; ++index) {
      vc1_[index] = cond[hash_descriptors[index]].attr.vc;
      vc2_[index] = cond[hash_descriptors[index]].val1.vc;
      int primary_size = 0;
      compatible = AddKeyColumn(&column_encoder, vc1_[index], vc2_[index], primary_offset, &primary_size) && compatible;
      primary_offset += primary_size;
      hash_table_key_size.push_back(primary_size);
    }

    if (traversed_dims_.Intersects(matched_dims_) || !compatible)
      return false;

    InitOuterScene(cond);

    InitHashTables(std::move(hash_table_key_size), std::move(column_encoder));

    result_defer.cancel();

    return true;
  }

  base::future<> Execute() {
    // Preparing the new multiindex tables.
    int64_t traversed_dims_size = mind_->NumOfTuples(traversed_dims_);
    int64_t matched_dims_size = mind_->NumOfTuples(matched_dims_);

    std::vector<bool> traverse_keys_unique;
    for (auto vc : vc1_) traverse_keys_unique.push_back(vc->IsDistinctInTable());

    mind_->LockAllForUse();

    mind_builder_ = std::make_unique<MultiIndexBuilder>(mind_, parent_->tips);
    std::vector<DimensionVector> dims_involved = {traversed_dims_, matched_dims_};
    mind_builder_->Init(std::max(traversed_dims_size, matched_dims_size), dims_involved);

    base::future<> ready_future = base::make_ready_future<>();
    if (traversed_dims_size > 0 && matched_dims_size > 0) {
      auto traverse_ready_future = TraverseDim(traversed_dims_size);
      ready_future = traverse_ready_future.then([this, matched_dims_size] {
        return too_many_conflicts_ ? base::make_ready_future<>() : MatchDim(matched_dims_size);
      });
    }

    auto final_functor = [this, traverse_keys_unique = std::move(traverse_keys_unique)]() {
      auto _ = base::defer([this] { mind_->UnlockAllFromUse(); });

      if (too_many_conflicts_) {
        parent_->why_failed = JoinFailure::FAIL_WRONG_SIDES;
        return;
      }

      outer_tuples_traversed_ += SubmitOuterTraversed();

      int64_t outer_tuples = outer_tuples_traversed_ + outer_tuples_matched_;
      if (outer_tuples > 0) {
        tianmu_control_.lock(parent_->m_conn->GetThreadID())
            << "Added " << outer_tuples << " null tuples by outer join." << system::unlock;
      }
      actually_matched_rows_ += outer_tuples;

      // Revert multiindex to the updated tables.
      if (packrows_omitted_ > 0) {
        int ommitted = int(packrows_omitted_ / double(packrows_matched_) * 10000.0) / 100.0;
        tianmu_control_.lock(parent_->m_conn->GetThreadID())
            << "Roughly omitted " << ommitted << "% packrows." << system::unlock;
      }

      mind_builder_->Commit(actually_matched_rows_, parent_->tips.count_only);

      // update local statistics - wherever possible
      int64_t traversed_dist_limit = actually_traversed_rows_;
      for (size_t index = 0; index < cond_hashed_; ++index) {
        if (!vc2_[index]->Type().IsString()) {
          // Otherwise different collations maybe a counterexample.
          // matched values: not more than the traversed ones in case of
          // equality.
          vc2_[index]->SetLocalDistVals(actually_traversed_rows_ + outer_tuples_matched_);
          if (traverse_keys_unique[index]) {
            // unique => there is no more rows than distinct values
            traversed_dist_limit = std::min(
                traversed_dist_limit, vc2_[index]->GetApproxDistVals(false) + (outer_tuples - outer_tuples_matched_));
          }
        }
      }

      for (int index = 0; index < mind_->NumOfDimensions(); index++) {
        // All dimensions involved in traversed side.
        if (traversed_dims_[index])
          parent_->table->SetVCDistinctVals(index, traversed_dist_limit);
      }
    };

    return ready_future.then(final_functor);
  }

 private:
  bool AddKeyColumn(std::vector<ColumnBinEncoder> *column_encoder, vcolumn::VirtualColumn *vc,
                    vcolumn::VirtualColumn *vc_matching, int primary_offset, int *primary_size) {
    // Comparable, non-monotonic, non-decodable.
    ColumnBinEncoder &encoder = column_encoder->emplace_back((int)ColumnBinEncoder::ENCODER_IGNORE_NULLS);
    // RC_TIMESTAMP is omitted by ColumnValueEncoder::SecondColumn.
    vcolumn::VirtualColumn *second_column =
        (vc->Type().GetTypeName() == common::ColumnType::TIMESTAMP) ? nullptr : vc_matching;
    bool success = encoder.PrepareEncoder(vc, second_column);
    encoder.SetPrimaryOffset(primary_offset);
    *primary_size = encoder.GetPrimarySize();
    return success;
  }

  void InitOuterScene(Condition &cond) {
    // The outer_dims will be filled with nulls for non-matching tuples.
    DimensionVector outer_dims(cond[0].right_dims);
    if (!outer_dims.IsEmpty()) {
      if (traversed_dims_.Includes(outer_dims)) {
        // Watch the non-outer dim for unmatched tuples and add them with nulls
        // on outer dim.
        watch_matched_ = true;
      } else if (matched_dims_.Includes(outer_dims)) {
        watch_traversed_ = true;
      }

      outer_nulls_only_ = true;
      for (int index = 0; index < outer_dims.Size(); ++index)
        if (outer_dims[index] && parent_->tips.null_only[index] == false)
          outer_nulls_only_ = false;
    }
  }

  void InitHashTables(std::vector<int> &&hash_table_key_size, std::vector<ColumnBinEncoder> &&column_encoder) {
    // Prepare columns for traversed dimension numbers in hash table.
    std::vector<int> hash_table_tuple_size;
    int num_of_traversed_dims = 0;
    for (int index = 0; index < mind_->NumOfDimensions(); ++index) {
      if (traversed_dims_[index] && !(parent_->tips.count_only && !other_cond_exist_)) {
        // Storing tuples may be omitted if (count_only && !other_cond_exist_).
        // Jump over the joining key columns.
        traversed_hash_column_[index] = cond_hashed_ + num_of_traversed_dims++;
        hash_table_tuple_size.push_back((mind_->OrigSize(index) > 0x000000007FFFFF00) ? 8 : 4);
      }
    }

    // Create hash tables.
    int hash_table_count = base::smp::count - 1;  // Don't occupy the base main thread.
    if (tianmu_sysvar_async_join_setting.traverse_slices > 0)
      hash_table_count = std::min(hash_table_count, tianmu_sysvar_async_join_setting.traverse_slices);
    HashTable::CreateParams hash_table_create_params;
    hash_table_create_params.keys_length = std::move(hash_table_key_size);
    hash_table_create_params.tuples_length = std::move(hash_table_tuple_size);
    hash_table_create_params.max_table_size = int64_t(mind_->NumOfTuples(traversed_dims_) * 1.5);
    hash_table_create_params.easy_roughable = false;
    tables_manager_.reset(JoinThreadTableManager::CreateSharedTableManager(
        pack_power_, hash_table_count, hash_table_create_params, std::move(column_encoder), watch_traversed_,
        force_switching_sides_ || tianmu_sysvar_join_disable_switch_side));
  }

  template <typename T>
  inline bool ImpossibleValues(size_t col, T &pack_min, T &pack_max) {
    for (size_t index : boost::irange<size_t>(0, tables_manager_->GetTableCount())) {
      ColumnBinEncoder *encoder = tables_manager_->GetTable(index)->GetColumnEncoder(col);
      if (!encoder->ImpossibleValues(pack_min, pack_max))
        return false;
    }
    return true;
  }

  base::future<stdexp::optional<bool>> TraversePackPerThread(unsigned thread_index, unsigned thread_count,
                                                             std::shared_ptr<MIIteratorPoller> poller,
                                                             MultiIndexBuilder::BuildItemPtr build_item,
                                                             JoinThreadTable *thread_table) {
    std::shared_ptr<MIIterator> miter = poller->Poll();
    if (!miter || too_many_conflicts_) {
      if (!build_item->IsEmpty()) {
        build_item->Finish();
        mind_builder_->AddBuildItem(build_item);
      }
      return base::make_ready_future<stdexp::optional<bool>>(true);
    }

    // Using SetTaskId/SetTaskNum for support multithreads of
    // vcolumn::VirtualColumn's LockSourcePacks.
    miter->SetTaskId(thread_index);
    miter->SetTaskNum(thread_count);

    auto functor = [this, miter, build_item, thread_table]() mutable {
      std::string key_buffer(thread_table->GetKeyBufferWidth(), 0);
      while (miter->IsValid() && !too_many_conflicts_) {
        if (parent_->m_conn->Killed())
          throw common::KilledException();

        if (miter->PackrowStarted()) {
          for (size_t index = 0; index < cond_hashed_; ++index) vc1_[index]->LockSourcePacks(*miter);
        }

        // Put key value to encoding.
        bool should_ignore_row = false;
        for (size_t index = 0; index < cond_hashed_; index++) {
          if (vc1_[index]->IsNull(*miter)) {
            should_ignore_row = true;
            break;
          }

          ColumnBinEncoder *encoder = thread_table->GetColumnEncoder(index);
          encoder->Encode(reinterpret_cast<unsigned char *>(key_buffer.data()), *miter, nullptr, true);
        }

        if (!should_ignore_row) {
          bool too_many_conflicts = false;
          int64_t hash_row = thread_table->AddKeyValue(key_buffer, &too_many_conflicts);
          if (hash_row == common::NULL_VALUE_64)
            throw std::runtime_error("No space left");

          if (too_many_conflicts) {
            too_many_conflicts_ = true;
            throw std::runtime_error("Too many conflicts");
          }

          actually_traversed_rows_++;

          // Put the tuple column. Note: needed also for count_only_now, because
          // another conditions may need it.
          if (!parent_->tips.count_only || other_cond_exist_) {
            for (int index = 0; index < mind_->NumOfDimensions(); ++index)
              if (traversed_dims_[index])
                thread_table->SetTupleValue(traversed_hash_column_[index], hash_row, (*miter)[index]);
          }
        } else if (watch_traversed_) {
          for (int index = 0; index < mind_->NumOfDimensions(); ++index) {
            if (matched_dims_[index]) {
              build_item->SetTableValue(index, common::NULL_VALUE_64);
            } else if (traversed_dims_[index]) {
              build_item->SetTableValue(index, (*miter)[index]);
            }
          }
          build_item->CommitTableValues();
          actually_traversed_rows_++;
          outer_tuples_traversed_++;
        }

        miter->Increment();
      }
    };

    return base::smp::submit_to(thread_index + 1, std::move(functor)).then_wrapped([this](auto &&f) {
      auto result = stdexp::optional<bool>(stdexp::nullopt);
      try {
        f.get();
      } catch (std::exception &ex) {
        tianmu_control_.lock(parent_->m_conn->GetThreadID())
            << "Exception on traversing pack: " << ex.what() << system::unlock;
        result = stdexp::optional<bool>(false);
      }
      return result;
    });
  }

  base::future<stdexp::optional<bool>> MatchPackPerThread(unsigned thread_index, unsigned thread_count,
                                                          std::shared_ptr<MIIteratorPoller> poller,
                                                          MultiIndexBuilder::BuildItemPtr build_item,
                                                          DescriptorsPtr other_cond,
                                                          ColumnBinEncoderPtr column_encoder) {
    std::shared_ptr<MIIterator> miter = poller->Poll();
    if (!miter || stop_matching_) {
      if (!build_item->IsEmpty()) {
        build_item->Finish();
        mind_builder_->AddBuildItem(build_item);
      }
      return base::make_ready_future<stdexp::optional<bool>>(true);
    }

    // Using SetTaskId/SetTaskNum for support multithreads of
    // vcolumn::VirtualColumn's LockSourcePacks.
    miter->SetTaskId(thread_index);
    miter->SetTaskNum(thread_count);

    auto functor = [this, build_item, miter, other_cond, column_encoder]() mutable {
      std::string key_buffer(tables_manager_->GetTable(0)->GetKeyBufferWidth(), 0);
      MIDummyIterator combined_mit(mind_);
      combined_mit.SetTaskId(miter->GetTaskId());
      combined_mit.SetTaskNum(miter->GetTaskNum());

      while (miter->IsValid() && !stop_matching_) {
        if (parent_->m_conn->Killed())
          throw common::KilledException();

        bool ignore_this_packrow = false;
        if (miter->PackrowStarted()) {
          for (size_t index = 0; index < cond_hashed_; ++index) {
            if (column_encoder->at(index).IsString()) {
              if (!vc2_[index]->Type().Lookup()) {
                types::BString local_min = vc2_[index]->GetMinString(*miter);
                types::BString local_max = vc2_[index]->GetMaxString(*miter);
                if (!local_min.IsNull() && !local_max.IsNull() && ImpossibleValues(index, local_min, local_max)) {
                  ignore_this_packrow = true;
                  break;
                }
              }
            } else {
              int64_t local_min = vc2_[index]->GetMinInt64(*miter);
              int64_t local_max = vc2_[index]->GetMaxInt64(*miter);
              if (local_min == common::NULL_VALUE_64 || local_max == common::NULL_VALUE_64 ||
                  ImpossibleValues(index, local_min, local_max)) {
                ignore_this_packrow = true;
                break;
              }
            }
          }

          packrows_matched_++;

          if (ignore_this_packrow) {
            if (watch_matched_) {
              MIIterator mit_this_pack(*miter);
              do {
                for (int index = 0; index < mind_->NumOfDimensions(); ++index) {
                  if (matched_dims_[index])
                    build_item->SetTableValue(index, mit_this_pack[index]);
                  else if (traversed_dims_[index])
                    build_item->SetTableValue(index, common::NULL_VALUE_64);
                }
                build_item->CommitTableValues();
                ++mit_this_pack;
              } while (mit_this_pack.IsValid() && !mit_this_pack.PackrowStarted());
            }

            miter->NextPackrow();
            packrows_omitted_++;
            continue;
          }

          for (size_t index = 0; index < cond_hashed_; ++index) vc2_[index]->LockSourcePacks(*miter);
        }

        bool null_found = false;
        for (size_t index = 0; index < cond_hashed_; ++index) {
          if (vc2_[index]->IsNull(*miter)) {
            null_found = true;
            break;
          }
          column_encoder->at(index).Encode(reinterpret_cast<unsigned char *>(key_buffer.data()), *miter, vc2_[index]);
        }

        if (!null_found) {
          size_t table_count_limit = tables_manager_->IsShared() ? 1u : tables_manager_->GetTableCount();
          for (size_t index : boost::irange<size_t>(0, table_count_limit)) {
            JoinThreadTable *thread_table = tables_manager_->GetTable(index);
            JoinThreadTable::Finder table_finder(thread_table, &key_buffer);
            int64_t matching_rows = table_finder.GetMatchedRows();
            int64_t hash_row = 0;
            actually_matched_rows_ += matching_rows;
            if (!other_cond_exist_) {
              if (!parent_->tips.count_only || watch_traversed_) {
                while ((hash_row = table_finder.GetNextRow()) != common::NULL_VALUE_64) {
                  if (!parent_->tips.count_only || !outer_nulls_only_) {
                    for (int index = 0; index < mind_->NumOfDimensions(); ++index) {
                      if (matched_dims_[index]) {
                        build_item->SetTableValue(index, (*miter)[index]);
                      } else if (traversed_dims_[index]) {
                        build_item->SetTableValue(index,
                                                  thread_table->GetTupleValue(traversed_hash_column_[index], hash_row));
                      }
                    }
                    build_item->CommitTableValues();
                  }
                }
              }

              if (watch_matched_ && matching_rows == 0) {
                // Add outer join of matched.
                for (int index = 0; index < mind_->NumOfDimensions(); ++index) {
                  if (matched_dims_[index])
                    build_item->SetTableValue(index, (*miter)[index]);
                  else if (traversed_dims_[index])
                    build_item->SetTableValue(index, common::NULL_VALUE_64);
                }
                build_item->CommitTableValues();
                outer_tuples_matched_++;
                ;
              }
            } else {
              // Complex case: different types of join conditions mixed together
              combined_mit.Combine(*miter);
              while ((hash_row = table_finder.GetNextRow(false)) != common::NULL_VALUE_64) {
                bool other_cond_true = true;
                for (int index = 0; index < mind_->NumOfDimensions(); index++) {
                  if (traversed_dims_[index])
                    combined_mit.Set(index, thread_table->GetTupleValue(traversed_hash_column_[index], hash_row));
                }
                for (auto &desc : *other_cond) {
                  desc.LockSourcePacks(combined_mit);
                  if (!desc.CheckCondition(combined_mit)) {
                    other_cond_true = false;
                    break;
                  }
                }
                if (other_cond_true) {
                  if (!parent_->tips.count_only) {
                    for (int index = 0; index < mind_->NumOfDimensions(); ++index) {
                      if (matched_dims_[index]) {
                        build_item->SetTableValue(index, (*miter)[index]);
                      } else if (traversed_dims_[index]) {
                        build_item->SetTableValue(index,
                                                  thread_table->GetTupleValue(traversed_hash_column_[index], hash_row));
                      }
                    }
                    build_item->CommitTableValues();
                  }

                  thread_table->ResetTraversed(hash_row);
                  actually_matched_rows_++;
                } else if (watch_matched_) {
                  for (int index = 0; index < mind_->NumOfDimensions(); ++index) {
                    if (matched_dims_[index])
                      build_item->SetTableValue(index, (*miter)[index]);
                    else if (traversed_dims_[index])
                      build_item->SetTableValue(index, common::NULL_VALUE_64);
                  }
                  build_item->CommitTableValues();
                  outer_tuples_matched_++;
                }
              }
            }
          }
        }

        if (!outer_nulls_only_) {
          if (parent_->tips.limit != -1 && parent_->tips.limit <= actually_matched_rows_) {
            stop_matching_ = true;
            break;
          }
        }

        miter->Increment();
      }
    };
    return base::smp::submit_to(thread_index + 1, std::move(functor)).then_wrapped([this](auto &&) {
      return stdexp::optional<bool>(stdexp::nullopt);
    });
  }

  base::future<> TraverseDim(int64_t wait_traversed_rows) {
    size_t slice_count = tables_manager_->GetTableCount();
    std::shared_ptr<MIIterator> miter(new MIIterator(mind_, traversed_dims_));
    std::shared_ptr<MIIteratorPoller> miter_poller(new MIIteratorPoller(miter));

    tianmu_control_.lock(parent_->m_conn->GetThreadID())
        << "Start traverse " << miter->NumOfTuples() << " rows with " << slice_count << " threads using "
        << miter_poller->GetSliceType() << system::unlock;
    auto start_traversing = base::steady_clock_type::now();
    int availabled_packs = (int)((wait_traversed_rows + (1 << pack_power_) - 1) >> pack_power_);
    std::unique_ptr<TempTablePackLocker> temp_locker =
        std::make_unique<TempTablePackLocker>(vc1_, cond_hashed_, availabled_packs);

    base::future<> traverse_future =
        base::parallel_for_each(boost::irange<unsigned>(0, slice_count), [this, slice_count, miter_poller](unsigned c) {
          JoinThreadTable *thread_table = tables_manager_->GetTable(c);
          MultiIndexBuilder::BuildItemPtr build_item = mind_builder_->CreateBuildItem();
          return base::repeat_until_value(std::bind(&Action::TraversePackPerThread, this, c, slice_count, miter_poller,
                                                    build_item, thread_table))
              .then_wrapped([this](auto &&f) {
                try {
                  f.get();
                } catch (std::exception &ex) {
                  tianmu_control_.lock(parent_->m_conn->GetThreadID())
                      << "Exception on traversing: " << ex.what() << system::unlock;
                }
              });
        });

    return traverse_future.then([this, start_traversing, temp_locker = std::move(temp_locker)] {
      auto elapsed = base::steady_clock_type::now() - start_traversing;
      tianmu_control_.lock(parent_->m_conn->GetThreadID())
          << "Traversed " << actually_traversed_rows_ << " completed, cost "
          << static_cast<double>(elapsed.count() / 1000000000.0) << " s" << system::unlock;
      for (auto &vc : vc1_) {
        vc->UnlockSourcePacks();
      }
    });
  }

  base::future<> MatchDim(int64_t wait_matched_rows) {
    size_t slice_count = base::smp::count - 1;  // Don't occupy the base main thread.
    if (tianmu_sysvar_async_join_setting.match_slices > 0)
      slice_count = std::min<size_t>(slice_count, tianmu_sysvar_async_join_setting.match_slices);

    std::shared_ptr<MIIterator> miter(new MIIterator(mind_, matched_dims_));
    std::shared_ptr<MIIteratorPoller> miter_poller(new MIIteratorPoller(miter));
    tianmu_control_.lock(parent_->m_conn->GetThreadID())
        << "Start match " << miter->NumOfTuples() << " rows with " << slice_count << " threads using "
        << miter_poller->GetSliceType() << system::unlock;
    auto start_matching = base::steady_clock_type::now();
    int availabled_packs = (int)((wait_matched_rows + (1 << pack_power_) - 1) >> pack_power_);
    std::unique_ptr<TempTablePackLocker> temp_locker =
        std::make_unique<TempTablePackLocker>(vc2_, cond_hashed_, availabled_packs);

    actually_matched_rows_ = 0;

    base::future<> match_future =
        base::parallel_for_each(boost::irange<unsigned>(0, slice_count), [this, slice_count, miter_poller](unsigned c) {
          MultiIndexBuilder::BuildItemPtr build_item = mind_builder_->CreateBuildItem();
          DescriptorsPtr other_cond(new std::vector<Descriptor>(other_cond_));
          ColumnBinEncoderPtr column_encoder(new std::vector<ColumnBinEncoder>());
          tables_manager_->GetTable(0)->GetColumnEncoder(column_encoder.get());
          return base::repeat_until_value(std::bind(&Action::MatchPackPerThread, this, c, slice_count, miter_poller,
                                                    build_item, other_cond, column_encoder))
              .then_wrapped([this](auto &&f) {
                try {
                  f.get();
                } catch (std::exception &ex) {
                  tianmu_control_.lock(parent_->m_conn->GetThreadID())
                      << "Exception on matching: " << ex.what() << system::unlock;
                }
              });
        });

    return match_future.then([this, start_matching, temp_locker = std::move(temp_locker)] {
      auto elapsed = base::steady_clock_type::now() - start_matching;
      tianmu_control_.lock(parent_->m_conn->GetThreadID())
          << "Produced " << actually_matched_rows_ << " rows, cost "
          << static_cast<double>(elapsed.count() / 1000000000.0) << " s" << system::unlock;
      for (auto &vc : vc2_) {
        vc->UnlockSourcePacks();
      }
      for (auto &cond : other_cond_) cond.UnlockSourcePacks();

      if (outer_nulls_only_)
        actually_matched_rows_ = 0;
    });
  }

  int64_t SubmitOuterTraversed() {
    if (!watch_traversed_)
      return 0;

    MultiIndexBuilder::BuildItemPtr build_item = mind_builder_->CreateBuildItem();
    int64_t outer_added = 0;
    size_t table_count_limit = tables_manager_->IsShared() ? 1u : tables_manager_->GetTableCount();
    for (size_t index : boost::irange<size_t>(0, table_count_limit)) {
      JoinThreadTable *thread_table = tables_manager_->GetTable(index);

      if (parent_->tips.count_only) {
        outer_added += thread_table->GetOuterTraversedCount();
      } else {
        int64_t count = thread_table->GetHashCount();
        for (int64_t hash_row = 0; hash_row < count; ++hash_row) {
          if (thread_table->IsOuterTraversed(hash_row)) {
            for (int index = 0; index < mind_->NumOfDimensions(); ++index) {
              if (matched_dims_[index])
                build_item->SetTableValue(index, common::NULL_VALUE_64);
              else if (traversed_dims_[index])
                build_item->SetTableValue(index, thread_table->GetTupleValue(traversed_hash_column_[index], hash_row));
            }
            build_item->CommitTableValues();
            outer_added++;
            actually_traversed_rows_++;
          }
        }
      }
    }

    build_item->Finish();
    mind_builder_->AddBuildItem(build_item);

    return outer_added;
  }

  ProxyHashJoiner *parent_;
  MultiIndex *mind_;

  uint32_t pack_power_ = 16;  // 2 ^ power
  DimensionVector traversed_dims_;
  DimensionVector matched_dims_;
  bool watch_traversed_ = false;
  bool watch_matched_ = false;

  std::vector<int> traversed_hash_column_;

  std::unique_ptr<JoinThreadTableManager> tables_manager_;

  std::unique_ptr<MultiIndexBuilder> mind_builder_;

  bool force_switching_sides_ = false;
  std::atomic<bool> too_many_conflicts_ = false;
  std::atomic<bool> stop_matching_ = false;

  std::vector<vcolumn::VirtualColumn *> vc1_;
  std::vector<vcolumn::VirtualColumn *> vc2_;
  size_t cond_hashed_ = 0;
  // If true, then check of other conditions is needed on matching.
  bool other_cond_exist_ = false;
  std::vector<Descriptor> other_cond_;

  std::atomic<int64_t> actually_traversed_rows_ = 0;
  std::atomic<int64_t> actually_matched_rows_ = 0;
  // Statistics, roughly omitted by by matching.
  std::atomic<int64_t> packrows_omitted_ = 0;
  std::atomic<int64_t> packrows_matched_ = 0;
  std::atomic<int64_t> outer_tuples_traversed_ = 0;
  std::atomic<int64_t> outer_tuples_matched_ = 0;

  // If true only null (outer) rows may exists in result.
  bool outer_nulls_only_ = false;
};

// ProxyHashJoiner
ProxyHashJoiner::ProxyHashJoiner(MultiIndex *mind, TempTable *table, JoinTips &tips)
    : TwoDimensionalJoiner(mind, table, tips) {}

ProxyHashJoiner::~ProxyHashJoiner() {}

// Overridden from TwoDimensionalJoiner:
void ProxyHashJoiner::ExecuteJoinConditions(Condition &cond) {
  auto started = base::steady_clock_type::now();
  auto wait_future = ScheduleAsyncTask([this, started, &cond]() {
    base::future<> join_ready = base::make_ready_future<>();
    action_ = std::make_unique<Action>(this, force_switching_sides_);
    if (action_->Init(cond))
      join_ready = action_->Execute();
    return join_ready.then([this, started]() mutable {
      auto elapsed = base::steady_clock_type::now() - started;
      tianmu_control_.lock(m_conn->GetThreadID())
          << "Join task cost " << static_cast<double>(elapsed.count() / 1000000000.0) << " s" << system::unlock;

      action_.reset();
    });
  });

  wait_future.wait();
}

}  // namespace core
}  // namespace Tianmu
