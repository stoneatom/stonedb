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

#include "multi_index.h"

#include "core/group_distinct_table.h"
#include "core/joiner.h"
#include "core/mi_iterator.h"
#include "core/mi_new_contents.h"
#include "core/tools.h"

namespace Tianmu {
namespace core {
#define MATERIAL_TUPLES_LIMIT 150000000000LL  // = ~1 TB of cache needed for one dimension
#define MATERIAL_TUPLES_WARNING 2500000000LL  // = 10-20 GB of cache for one dimension

MultiIndex::MultiIndex(uint32_t power) : m_conn(current_txn_) {
  // update Clear() on any change
  p_power_ = power;
  num_of_dimensions_ = 0;
  num_of_tuples_ = 0;
  has_tuples_too_big_ = false;

  dim_size_ = nullptr;
  dimension_group_ = nullptr;
  group_num_for_dimension_ = nullptr;
  iterator_lock_ = 0;
}

MultiIndex::MultiIndex(const MultiIndex &s) : m_conn(s.m_conn) {
  p_power_ = s.p_power_;
  num_of_dimensions_ = s.num_of_dimensions_;
  num_of_tuples_ = s.num_of_tuples_;
  has_tuples_too_big_ = s.has_tuples_too_big_;

  if (num_of_dimensions_ > 0) {
    dim_size_ = new int64_t[num_of_dimensions_];
    dimension_group_ = new DimensionGroup *[num_of_dimensions_];
    group_num_for_dimension_ = new int[num_of_dimensions_];
    used_in_output_ = s.used_in_output_;
    can_be_distinct_ = s.can_be_distinct_;

    for (uint i = 0; i < s.dim_groups_.size(); i++) dim_groups_.push_back(s.dim_groups_[i]->Clone(false));

    for (uint32_t i = 0; i < num_of_dimensions_; i++) dim_size_[i] = s.dim_size_[i];

    FillGroupForDim();
  } else {
    dim_size_ = nullptr;
    dimension_group_ = nullptr;
    group_num_for_dimension_ = nullptr;
  }

  iterator_lock_ = 0;
}

MultiIndex::MultiIndex(MultiIndex &s, bool shallow) : m_conn(s.m_conn) {
  p_power_ = s.p_power_;
  num_of_dimensions_ = s.num_of_dimensions_;
  num_of_tuples_ = s.num_of_tuples_;
  has_tuples_too_big_ = s.has_tuples_too_big_;

  if (num_of_dimensions_ > 0) {
    dimension_group_ = new DimensionGroup *[num_of_dimensions_];
    group_num_for_dimension_ = new int[num_of_dimensions_];
    dim_size_ = new int64_t[num_of_dimensions_];
    used_in_output_ = s.used_in_output_;
    can_be_distinct_ = s.can_be_distinct_;

    for (uint i = 0; i < s.dim_groups_.size(); i++) dim_groups_.push_back(s.dim_groups_[i]->Clone(shallow));

    for (uint32_t i = 0; i < num_of_dimensions_; i++) dim_size_[i] = s.dim_size_[i];

    FillGroupForDim();
  } else {
    dim_size_ = nullptr;
    dimension_group_ = nullptr;
    group_num_for_dimension_ = nullptr;
  }
  iterator_lock_ = 0;
}

MultiIndex::~MultiIndex() {
  for (uint i = 0; i < dim_groups_.size(); i++) {
    delete dim_groups_[i];
    dim_groups_[i] = nullptr;
  }
  delete[] dim_size_;
  delete[] dimension_group_;
  delete[] group_num_for_dimension_;
}

void MultiIndex::Clear() {
  try {
    for (uint i = 0; i < dim_groups_.size(); i++) {
      delete dim_groups_[i];
      dim_groups_[i] = nullptr;
    }
  } catch (...) {
    DEBUG_ASSERT(!"exception from destructor");
  }
  delete[] dim_size_;
  delete[] dimension_group_;
  delete[] group_num_for_dimension_;
  dim_groups_.clear();
  num_of_dimensions_ = 0;
  num_of_tuples_ = 0;
  has_tuples_too_big_ = false;
  dim_size_ = nullptr;
  dimension_group_ = nullptr;
  group_num_for_dimension_ = nullptr;
  iterator_lock_ = 0;
  can_be_distinct_.clear();
  used_in_output_.clear();
}

void MultiIndex::FillGroupForDim() {
  MEASURE_FET("MultiIndex::FillGroupForDim(...)");
  int move_groups = 0;

  for (uint i = 0; i < dim_groups_.size(); i++) {  // pack all holes
    if (dim_groups_[i] == nullptr) {
      while (i + move_groups < dim_groups_.size() && dim_groups_[i + move_groups] == nullptr) move_groups++;
      if (i + move_groups < dim_groups_.size()) {
        dim_groups_[i] = dim_groups_[i + move_groups];
        dim_groups_[i + move_groups] = nullptr;
      } else
        break;
    }
  }

  for (int i = 0; i < move_groups; i++) dim_groups_.pop_back();  // clear nulls from the end

  for (uint32_t d = 0; d < num_of_dimensions_; d++) {
    dimension_group_[d] = nullptr;
    group_num_for_dimension_[d] = -1;
  }

  for (uint32_t i = 0; i < dim_groups_.size(); i++) {
    for (uint32_t d = 0; d < num_of_dimensions_; d++)
      if (dim_groups_[i]->DimUsed(d)) {
        dimension_group_[d] = dim_groups_[i];
        group_num_for_dimension_[d] = i;
      }
  }
}

void MultiIndex::Empty(int dim_to_make_empty) {
  if (dim_to_make_empty != -1)
    dimension_group_[dim_to_make_empty]->Empty();
  else {
    for (uint i = 0; i < dim_groups_.size(); i++) dim_groups_[i]->Empty();
  }

  for (uint32_t i = 0; i < num_of_dimensions_; i++) {
    if (dim_to_make_empty == -1 || dimension_group_[dim_to_make_empty]->DimUsed(i)) {
      can_be_distinct_[i] = true;
      used_in_output_[i] = true;
    }
  }

  num_of_tuples_ = 0;
  has_tuples_too_big_ = false;
}

void MultiIndex::AddDimension() {
  num_of_dimensions_++;
  int64_t *ns = new int64_t[num_of_dimensions_];
  DimensionGroup **ng = new DimensionGroup *[num_of_dimensions_];
  int *ngn = new int[num_of_dimensions_];

  for (uint32_t i = 0; i < num_of_dimensions_ - 1; i++) {
    ns[i] = dim_size_[i];
    ng[i] = dimension_group_[i];
    ngn[i] = group_num_for_dimension_[i];
  }

  delete[] dim_size_;
  delete[] dimension_group_;
  delete[] group_num_for_dimension_;
  dim_size_ = ns;
  dimension_group_ = ng;
  group_num_for_dimension_ = ngn;
  dim_size_[num_of_dimensions_ - 1] = 0;
  dimension_group_[num_of_dimensions_ - 1] = nullptr;
  group_num_for_dimension_[num_of_dimensions_ - 1] = -1;
  // Temporary code, for rare cases when we add a dimension after other joins
  // (smk_33):
  for (uint i = 0; i < dim_groups_.size(); i++) dim_groups_[i]->AddDimension();

  return;  // Note: other functions will use AddDimension() to enlarge tables
}

void MultiIndex::AddDimension_cross(uint64_t size) {
  AddDimension();
  int new_dim = num_of_dimensions_ - 1;
  used_in_output_.push_back(true);

  if (num_of_dimensions_ > 1)
    MultiplyNoTuples(size);
  else
    num_of_tuples_ = size;

  DimensionGroupFilter *nf = nullptr;
  if (size > 0) {
    dim_size_[new_dim] = size;
    nf = new DimensionGroupFilter(new_dim, size, p_power_);  // redo
  } else {  // size == 0   => prepare a dummy dimension with empty filter
    dim_size_[new_dim] = 1;
    nf = new DimensionGroupFilter(new_dim, dim_size_[new_dim], p_power_);  // redo
    nf->Empty();
  }

  dim_groups_.push_back(nf);
  dimension_group_[new_dim] = nf;
  group_num_for_dimension_[new_dim] = int(dim_groups_.size() - 1);
  can_be_distinct_.push_back(true);  // may be modified below
  CheckIfVirtualCanBeDistinct();
}

void MultiIndex::MultiplyNoTuples(uint64_t factor) {
  num_of_tuples_ = SafeMultiplication(num_of_tuples_, factor);
  if (num_of_tuples_ == static_cast<uint64_t>(common::NULL_VALUE_64))
    has_tuples_too_big_ = true;
}

void MultiIndex::CheckIfVirtualCanBeDistinct()  // updates can_be_distinct_ table
                                                // in case of virtual multiindex
{
  // check whether can_be_distinct_ can be true
  // it is possible only when there are only 1-object dimensions
  // and one multiobject (then the multiobject one can be distinct, the rest
  // cannot)
  if (num_of_dimensions_ > 1) {
    int non_one_found = 0;
    for (uint32_t i = 0; i < num_of_dimensions_; i++) {
      if (dim_size_[i] > 1)
        non_one_found++;
    }

    if (non_one_found == 1) {
      for (uint32_t j = 0; j < num_of_dimensions_; j++)
        if (dim_size_[j] == 1)
          can_be_distinct_[j] = false;
        else
          can_be_distinct_[j] = true;
    }

    if (non_one_found > 1)
      for (uint32_t j = 0; j < num_of_dimensions_; j++) can_be_distinct_[j] = false;
  }
}

void MultiIndex::LockForGetIndex(int dim) { dimension_group_[dim]->Lock(dim); }

void MultiIndex::UnlockFromGetIndex(int dim) { dimension_group_[dim]->Unlock(dim); }

uint64_t MultiIndex::DimSize(int dim)  // the size of one dimension: material_no_tuples for materialized,
                                       // NumOfOnes for virtual
{
  return dimension_group_[dim]->NumOfTuples();
}

void MultiIndex::LockAllForUse() {
  for (uint32_t dim = 0; dim < num_of_dimensions_; dim++) LockForGetIndex(dim);
}

void MultiIndex::UnlockAllFromUse() {
  for (uint32_t dim = 0; dim < num_of_dimensions_; dim++) UnlockFromGetIndex(dim);
}

void MultiIndex::MakeCountOnly(int64_t mat_tuples, DimensionVector &dims_to_materialize) {
  MEASURE_FET("MultiIndex::MakeCountOnly(...)");

  MarkInvolvedDimGroups(dims_to_materialize);
  for (int i = 0; i < NumOfDimensions(); i++) {
    if (dims_to_materialize[i] && dimension_group_[i] != nullptr) {
      // delete this group
      int dim_group_to_delete = group_num_for_dimension_[i];
      for (int j = i; j < NumOfDimensions(); j++)
        if (group_num_for_dimension_[j] == dim_group_to_delete) {
          dimension_group_[j] = nullptr;
          group_num_for_dimension_[j] = -1;
        }

      delete dim_groups_[dim_group_to_delete];
      dim_groups_[dim_group_to_delete] = nullptr;  // these holes will be packed in FillGroupForDim()
    }
  }

  DimensionGroupMaterialized *count_only_group = new DimensionGroupMaterialized(dims_to_materialize);
  count_only_group->SetNumOfObj(mat_tuples);
  dim_groups_.push_back(count_only_group);
  FillGroupForDim();
  UpdateNumOfTuples();
}

void MultiIndex::UpdateNumOfTuples() {
  // assumptions:
  // - no_material_tuples is correct, even if all t[...] are nullptr (forgotten).
  //   However, if all f[...] are not nullptr, then the index is set to IT_VIRTUAL
  //   and no_material_tuples = 0
  // - IT_MIXED or IT_VIRTUAL may be in fact IT_ORDERED (must be set properly on
  // output)
  // - if no_material_tuples > 0, then new index_type is not IT_VIRTUAL
  has_tuples_too_big_ = false;
  if (dim_groups_.size() == 0)
    num_of_tuples_ = 0;
  else {
    num_of_tuples_ = 1;
    for (uint i = 0; i < dim_groups_.size(); i++) {
      dim_groups_[i]->UpdateNumOfTuples();
      MultiplyNoTuples(dim_groups_[i]->NumOfTuples());
    }
  }
}

int64_t MultiIndex::NumOfTuples(DimensionVector &dimensions,
                                bool fail_on_overflow)  // for a given subset of dimensions
{
  std::vector<int> dg = ListInvolvedDimGroups(dimensions);
  if (dg.size() == 0)
    return 0;

  int64_t res = 1;
  for (uint i = 0; i < dg.size(); i++) {
    dim_groups_[dg[i]]->UpdateNumOfTuples();
    res = SafeMultiplication(res, dim_groups_[dg[i]]->NumOfTuples());
  }

  if (res == common::NULL_VALUE_64 && fail_on_overflow)
    throw common::OutOfMemoryException("Too many tuples.  (1428)");
  return res;
}

int MultiIndex::MaxNumOfPacks(int dim)  // maximal (upper approx.) number of different nonempty data
                                        // packs for the given dimension
{
  int max_packs = 0;
  Filter *f = dimension_group_[dim]->GetFilter(dim);
  if (f) {
    for (size_t p = 0; p < f->NumOfBlocks(); p++)
      if (!f->IsEmpty(p))
        max_packs++;
  } else {
    max_packs = int((dim_size_[dim]) >> p_power_) + 1;
    if (dimension_group_[dim]->NumOfTuples() < max_packs)
      max_packs = (int)dimension_group_[dim]->NumOfTuples();
  }
  return max_packs;
}

// Logical operations

void MultiIndex::MIFilterAnd(MIIterator &mit,
                             Filter &fd)  // limit the MultiIndex by excluding
                                          // all tuples which are not present in
                                          // fd, in order given by mit
{
  MEASURE_FET("MultiIndex::MIFilterAnd(...)");
  LockAllForUse();

  if (num_of_dimensions_ == 1 && dimension_group_[0]->GetFilter(0)) {
    Filter *f = dimension_group_[0]->GetFilter(0);
    FilterOnesIterator fit(f, p_power_);
    int64_t cur_pos = 0;
    while (fit.IsValid()) {
      if (!fd.Get(cur_pos))
        fit.ResetDelayed();
      ++fit;
      cur_pos++;
    }
    f->Commit();
    UpdateNumOfTuples();
    UnlockAllFromUse();
    return;
  }

  DimensionVector dim_used(mit.DimsUsed());
  MarkInvolvedDimGroups(dim_used);
  int64_t new_no_tuples = fd.NumOfOnes();
  JoinTips tips(*this);
  MINewContents new_mind(this, tips);
  new_mind.SetDimensions(dim_used);
  new_mind.Init(new_no_tuples);

  mit.Rewind();

  int64_t f_pos = 0;
  while (mit.IsValid()) {
    if (fd.Get(f_pos)) {
      for (uint32_t d = 0; d < num_of_dimensions_; d++)
        if (dim_used[d])
          new_mind.SetNewTableValue(d, mit[d]);
      new_mind.CommitNewTableValues();
    }
    ++mit;
    f_pos++;
  }

  new_mind.Commit(new_no_tuples);
  UpdateNumOfTuples();
  UnlockAllFromUse();
}

bool MultiIndex::MarkInvolvedDimGroups(
    DimensionVector &v)  // if any dimension is marked, then mark the rest of its group
{
  bool added = false;
  for (uint32_t i = 0; i < num_of_dimensions_; i++) {
    if (!v[i]) {
      for (uint32_t j = 0; j < num_of_dimensions_; j++) {
        if (v[j] && group_num_for_dimension_[i] == group_num_for_dimension_[j]) {
          v[i] = true;
          added = true;
          break;
        }
      }
    }
  }
  return added;
}

std::vector<int> MultiIndex::ListInvolvedDimGroups(DimensionVector &v)  // List all internal numbers of groups touched
                                                                        // by the set of dimensions
{
  std::vector<int> res;
  int cur_group_number;
  for (uint32_t i = 0; i < num_of_dimensions_; i++) {
    if (v[i]) {
      cur_group_number = group_num_for_dimension_[i];
      bool added = false;
      for (uint j = 0; j < res.size(); j++)
        if (res[j] == cur_group_number) {
          added = true;
          break;
        }
      if (!added)
        res.push_back(cur_group_number);
    }
  }

  return res;
}

std::string MultiIndex::Display() {
  std::vector<int> ind_tab_no;
  int it_count = 0;
  for (uint i = 0; i < dim_groups_.size(); i++) {
    if (dim_groups_[i]->Type() == DimensionGroup::DGType::DG_INDEX_TABLE) {
      it_count++;
      ind_tab_no.push_back(it_count);  // calculate a number of a materialized dim. group
    } else
      ind_tab_no.push_back(0);
  }

  std::string s;
  s = "[";
  for (uint32_t i = 0; i < num_of_dimensions_; i++) {
    if (!dimension_group_[i]->DimEnabled(i))
      s += "-";
    else if (dimension_group_[i]->Type() == DimensionGroup::DGType::DG_FILTER)
      s += "f";
    else if (dimension_group_[i]->Type() == DimensionGroup::DGType::DG_INDEX_TABLE)
      s += (ind_tab_no[group_num_for_dimension_[i]] > 9 ? 'T' : '0' + ind_tab_no[group_num_for_dimension_[i]]);
    else if (dimension_group_[i]->Type() == DimensionGroup::DGType::DG_VIRTUAL)
      s += (GetFilter(i) ? 'F' : 'v');
    else
      s += "?";
  }

  s += "]";
  return s;
}

}  // namespace core
}  // namespace Tianmu
