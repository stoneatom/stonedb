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

#include "multi_index_builder.h"

#include "core/dimension_group_multiple.h"
#include "core/joiner.h"
#include "core/mi_rough_sorter.h"
#include "core/multi_index.h"

namespace Tianmu {
namespace core {

MultiIndexBuilder::BuildItem::BuildItem(MultiIndexBuilder *builder) : builder_(builder) {
  cached_values_.resize(builder_->dims_count_);
  nulls_possible_.resize(builder_->dims_count_);
  index_table_ = new IndexTable *[builder_->dims_count_];
  memset(index_table_, 0, sizeof(IndexTable *) * builder_->dims_count_);
}

MultiIndexBuilder::BuildItem::~BuildItem() {
  for (int dim = 0; dim < builder_->dims_count_; ++dim) {
    if (index_table_[dim]) {
      delete index_table_[dim];
      index_table_[dim] = nullptr;
    }
  }
  delete[] index_table_;
}

void MultiIndexBuilder::BuildItem::Initialize(int64_t initial_size) {
  initial_size = std::max(initial_size, (int64_t)8);
  MultiIndex *mind = builder_->multi_index_;
  bool rough_sort_needed = false;
  int min_block_shift = 64;
  for (int dim = 0; dim < builder_->dims_count_; ++dim) {
    nulls_possible_[dim] = false;
    index_table_[dim] = nullptr;

    if (builder_->dims_involved_[dim]) {
      if (builder_->forget_now_[dim])
        continue;

      index_table_[dim] = new IndexTable(initial_size, mind->OrigSize(dim), 0);
      index_table_[dim]->SetNumOfLocks(mind->group_for_dim[dim]->NumOfLocks(dim));
      min_block_shift = std::min(min_block_shift, index_table_[dim]->BlockShift());

      if (initial_size > (1U << mind->ValueOfPower()))
        rough_sort_needed = true;
    }
  }

  if (rough_sort_needed)
    rough_sorter_.reset(new MINewContentsRSorter(mind, index_table_, min_block_shift));
}

void MultiIndexBuilder::BuildItem::SetTableValue(int dim, int64_t val) { cached_values_[dim] = val; }

void MultiIndexBuilder::BuildItem::CommitTableValues() {
  if (rough_sorter_)
    rough_sorter_->CommitValues(&cached_values_[0], added_count_);

  for (int dim = 0; dim < builder_->dims_count_; ++dim) {
    if (index_table_[dim]) {
      if ((uint64_t)added_count_ >= index_table_[dim]->N()) {
        if (rough_sorter_)
          rough_sorter_->Barrier();
        index_table_[dim]->ExpandTo(added_count_ < 2048 ? 2048 : added_count_ * 4);
      }
      if (cached_values_[dim] == common::NULL_VALUE_64) {
        index_table_[dim]->Set64(added_count_, 0);
        nulls_possible_[dim] = true;
      } else {
        index_table_[dim]->Set64(added_count_, cached_values_[dim] + 1);
      }
    }
  }

  added_count_++;
}

void MultiIndexBuilder::BuildItem::Finish() {
  if (rough_sorter_) {
    rough_sorter_->Commit(added_count_);
    rough_sorter_->Barrier();
  }
}

IndexTable *MultiIndexBuilder::BuildItem::ReleaseIndexTable(int dim) {
  IndexTable *index_table = index_table_[dim];
  index_table_[dim] = nullptr;  // Transfer the life of IndexTable.
  return index_table;
}

//-------------------------------MultiIndexBuilder---------------------------------
MultiIndexBuilder::MultiIndexBuilder(MultiIndex *multi_index, const JoinTips &tips) : multi_index_(multi_index) {
  initial_size_ = 0;
  dims_count_ = multi_index_->NumOfDimensions();
  dims_involved_ = DimensionVector(dims_count_);
  forget_now_.resize(dims_count_);

  for (int dim = 0; dim < dims_count_; ++dim) {
    forget_now_[dim] = tips.forget_now[dim];
  }
}

void MultiIndexBuilder::Init(int64_t initial_size, const std::vector<DimensionVector> &dims_involved) {
  initial_size_ = initial_size;
  for (DimensionVector dims : dims_involved) {
    dims_involved_.Plus(dims);
    multi_index_->MarkInvolvedDimGroups(dims_involved_);
  }

  for (int dim = 0; dim < dims_count_; dim++) {
    if (dims_involved_[dim])
      multi_index_->LockForGetIndex(dim);  // Locking for creation.
  }
}

std::shared_ptr<MultiIndexBuilder::BuildItem> MultiIndexBuilder::CreateBuildItem() {
  std::shared_ptr<MultiIndexBuilder::BuildItem> build_item;
  if (!build_items_.empty()) {
    build_item = build_items_.front();
    build_items_.pop_front();
  }

  if (!build_item) {
    build_item = std::make_shared<MultiIndexBuilder::BuildItem>(this);
    build_item->Initialize(initial_size_);
  }

  return build_item;
}

void MultiIndexBuilder::AddBuildItem(std::shared_ptr<BuildItem> build_item) { build_items_.push_back(build_item); }

void MultiIndexBuilder::Commit(int64_t joined_tuples, bool count_only) {
  if (count_only) {
    CommitCountOnly(joined_tuples);
    return;
  }

  std::vector<int> no_locks(dims_count_);
  for (int dim = 0; dim < dims_count_; dim++) {
    if (dims_involved_[dim]) {
      no_locks[dim] = multi_index_->group_for_dim[dim]->NumOfLocks(dim);
    }
  }

  // dims_involved_ contains full original groups (to be deleted)
  for (int dim = 0; dim < dims_count_; dim++) {
    if (dims_involved_[dim]) {
      int group_no = multi_index_->group_num_for_dim[dim];
      if (multi_index_->dim_groups[group_no]) {  // otherwise already deleted
        delete multi_index_->dim_groups[group_no];
        multi_index_->dim_groups[group_no] = nullptr;
      }
    }
  }

  DimensionGroupMultiMaterialized *ng =
      new DimensionGroupMultiMaterialized(joined_tuples, dims_involved_, multi_index_->ValueOfPower());
  multi_index_->dim_groups.push_back(ng);

  for (auto &build_item : build_items_) {
    if (build_item->GetCount() > 0) {
      for (int dim = 0; dim < dims_count_; dim++) {
        IndexTable *index_table = build_item->ReleaseIndexTable(dim);
        if (dims_involved_[dim] && !forget_now_[dim]) {
          index_table->SetNumOfLocks(no_locks[dim]);
          ng->AddDimensionContent(dim, index_table, build_item->GetCount(), build_item->NullExisted(dim));
        }
      }
    }
  }

  multi_index_->FillGroupForDim();
  multi_index_->UpdateNumOfTuples();
  for (int dim = 0; dim < dims_count_; dim++)
    if (dims_involved_[dim] && !forget_now_[dim])
      multi_index_->UnlockFromGetIndex(dim);
}

}  // namespace core
}  // namespace Tianmu
