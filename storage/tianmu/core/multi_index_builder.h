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
#ifndef TIANMU_CORE_MULTI_INDEX_BUILDER_H_
#define TIANMU_CORE_MULTI_INDEX_BUILDER_H_
#pragma once

#include <list>
#include <memory>
#include <vector>

#include "core/bin_tools.h"
#include "core/cq_term.h"
#include "core/dimension_group.h"
#include "core/filter.h"
#include "core/index_table.h"
#include "core/mi_rough_sorter.h"
#include "core/multi_index.h"

namespace Tianmu {
namespace core {
class JoinTips;

class MultiIndexBuilder {
 public:
  class BuildItem {
   public:
    explicit BuildItem(MultiIndexBuilder *builder);
    ~BuildItem();

    void SetTableValue(int dim, int64_t val);
    void CommitTableValues();
    void Finish();

    IndexTable *ReleaseIndexTable(int dim);
    bool NullExisted(int dim) const { return nulls_possible_[dim]; }
    uint64_t GetCount() const { return added_count_; }
    bool IsEmpty() const { return (added_count_ == 0) ? true : false; }

   private:
    friend class MultiIndexBuilder;

    void Initialize(int64_t initial_size);

    MultiIndexBuilder *builder_;
    std::vector<int64_t> cached_values_;
    std::vector<bool> nulls_possible_;

    uint64_t added_count_ = 0;
    IndexTable **index_table_;
    std::unique_ptr<MINewContentsRSorter> rough_sorter_;
  };

  using BuildItemPtr = std::shared_ptr<BuildItem>;

  MultiIndexBuilder(MultiIndex *multi_index, const JoinTips &tips);
  ~MultiIndexBuilder() = default;

  void Init(int64_t initial_size, const std::vector<DimensionVector> &dims_involved);

  std::shared_ptr<BuildItem> CreateBuildItem();
  void AddBuildItem(std::shared_ptr<BuildItem> build_item);

  void Commit(int64_t joined_tuples, bool count_only = false);
  void CommitCountOnly(int64_t joined_tuples) {
    multi_index_->MakeCountOnly(joined_tuples, dims_involved_);
    for (int dim = 0; dim < dims_count_; dim++)
      if (dims_involved_[dim])
        multi_index_->UnlockFromGetIndex(dim);
    return;
  }

 private:
  friend class BuildItem;

  int dims_count_;
  MultiIndex *multi_index_;
  int64_t initial_size_;
  DimensionVector dims_involved_;
  std::vector<bool> forget_now_;
  std::list<std::shared_ptr<BuildItem>> build_items_;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_MULTI_INDEX_BUILDER_H_
