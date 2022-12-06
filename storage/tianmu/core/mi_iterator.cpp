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

#include "mi_iterator.h"

#include "core/pack_orderer.h"

namespace Tianmu {
namespace core {
std::vector<PackOrderer> null_order;  // use null_order in constructor if ordering not desired

MIIterator::MIIterator() : po(null_order) {
  p_power = common::DFT_PSS;
  mind = nullptr;
  no_dims = 0;
  mind_created_locally = false;
  cur_pos = nullptr;
  cur_pack = nullptr;
  it_for_dim = nullptr;
  valid = false;
  omitted_factor = 1;
  next_pack_started = false;
  one_filter_dim = -1;
  one_filter_it = nullptr;
  mii_type = MIIteratorType::MII_DUMMY;
  TaskId = 0;
  TasksNum = 1;
}

MIIterator::MIIterator(MultiIndex *_mind, DimensionVector &_dimensions) : mind(_mind), po(null_order) {
  p_power = mind->ValueOfPower();
  no_dims = mind->NumOfDimensions();
  mind_created_locally = false;
  cur_pos = nullptr;
  cur_pack = nullptr;
  it_for_dim = nullptr;
  one_filter_dim = -1;
  one_filter_it = nullptr;
  dimensions = _dimensions;
  TaskId = 0;
  TasksNum = 1;
  Init();
}

MIIterator::MIIterator(MultiIndex *_mind, DimensionVector &_dimensions, std::vector<PackOrderer> &_po)
    : mind(_mind),
      po(_po)  // Ordered iterator
{
  p_power = mind->ValueOfPower();
  no_dims = mind->NumOfDimensions();
  mind_created_locally = false;
  cur_pos = nullptr;
  cur_pack = nullptr;
  it_for_dim = nullptr;
  one_filter_dim = -1;
  one_filter_it = nullptr;
  dimensions = _dimensions;
  TaskId = 0;
  TasksNum = 1;
  Init();
}

MIIterator::MIIterator(MultiIndex *_mind, uint32_t power) : mind(_mind), po(null_order) {
  if (mind == nullptr) {
    mind_created_locally = true;
    mind = new MultiIndex(power);  // redo-power
    mind->AddDimension_cross(1);   // just one row
  } else
    mind_created_locally = false;
  p_power = mind->ValueOfPower();
  no_dims = mind->NumOfDimensions();
  cur_pos = nullptr;
  cur_pack = nullptr;
  it_for_dim = nullptr;
  one_filter_dim = -1;
  one_filter_it = nullptr;
  TaskId = 0;
  TasksNum = 1;
  dimensions = DimensionVector(no_dims);
  dimensions.SetAll();
  Init();
}

MIIterator::MIIterator(MultiIndex *_mind, int one_dimension, bool lock) : mind(_mind), po(null_order) {
  p_power = mind->ValueOfPower();
  no_dims = mind->NumOfDimensions();
  mind_created_locally = false;
  cur_pos = nullptr;
  cur_pack = nullptr;
  dimensions = DimensionVector(no_dims);
  one_filter_dim = -1;
  one_filter_it = nullptr;
  TaskId = 0;
  TasksNum = 1;
  if (one_dimension == -1)
    dimensions.SetAll();
  else
    dimensions[one_dimension] = true;
  Init(lock);
}

MIIterator::MIIterator(const MIIterator &sec, bool lock)
    : dg(sec.dg),
      it_for_dim(nullptr),
      mind(sec.mind),
      mind_created_locally(sec.mind_created_locally),
      no_dims(sec.no_dims),
      dimensions(sec.dimensions),
      cur_pos(nullptr),  // will be initialized below
      cur_pack(nullptr),
      valid(sec.valid),
      omitted_factor(sec.omitted_factor),
      no_obj(sec.no_obj),
      pack_size_left(sec.pack_size_left),
      next_pack_started(sec.next_pack_started),
      mii_type(sec.mii_type),
      po(sec.po),
      one_filter_dim(sec.one_filter_dim) {
  p_power = sec.p_power;
  if (sec.mind_created_locally) {
    mind = new MultiIndex(*sec.mind);
    DEBUG_ASSERT(no_dims == 1);
    dg[0] = mind->dim_groups[0];  // assuming that the local minds are
                                  // one-dimensional
  }
  if (no_dims > 0) {
    cur_pos = new int64_t[no_dims];
    cur_pack = new int[no_dims];
    it_for_dim = new int[no_dims];
  }
  if (no_dims > 0 && lock) {
    mind->IteratorLock();
  }
  for (int i = 0; i < no_dims; i++) {
    it_for_dim[i] = sec.it_for_dim[i];
    cur_pos[i] = sec.cur_pos[i];
    cur_pack[i] = sec.cur_pack[i];
  }
  if (dimensions.Size() > 0)  // may be not initialized for dummy iterators
    for (int i = 0; i < no_dims; i++)
      if (dimensions[i])
        mind->LockForGetIndex(i);
  for (uint i = 0; i < sec.it.size(); i++) it.push_back(sec.dg[i]->CopyIterator(sec.it[i], p_power));

  one_filter_it = nullptr;
  if (one_filter_dim > -1) {
    if (po.size() == 0)
      one_filter_it = (DimensionGroupFilter::DGFilterIterator *)(it[0]);
    else
      one_filter_it = (DimensionGroupFilter::DGFilterOrderedIterator *)(it[0]);
  }
  TaskId = 0;
  TasksNum = 1;
}

void MIIterator::swap(MIIterator &i) {
  DEBUG_ASSERT(po.size() == 0 && "not working due to pack orderer swap");
  std::swap(p_power, i.p_power);
  std::swap(mind, i.mind);
  std::swap(mind_created_locally, i.mind_created_locally);
  std::swap(no_dims, i.no_dims);
  std::swap(cur_pos, i.cur_pos);
  std::swap(cur_pack, i.cur_pack);
  std::swap(valid, i.valid);
  std::swap(it_for_dim, i.it_for_dim);
  std::swap(omitted_factor, i.omitted_factor);
  std::swap(no_obj, i.no_obj);
  std::swap(pack_size_left, i.pack_size_left);
  std::swap(dimensions, i.dimensions);
  std::swap(it, i.it);
  std::swap(dg, i.dg);
  std::swap(next_pack_started, i.next_pack_started);
  std::swap(mii_type, i.mii_type);
  std::swap(po, i.po);
  std::swap(one_filter_dim, i.one_filter_dim);
  std::swap(one_filter_it, i.one_filter_it);
}

void MIIterator::Init(bool lock) {
  mii_type = MIIteratorType::MII_NORMAL;
  bool *dim_group_used = new bool[mind->dim_groups.size()];
  for (uint i = 0; i < mind->dim_groups.size(); i++) dim_group_used[i] = false;
  for (int i = 0; i < no_dims; i++)
    if (dimensions[i]) {
      dim_group_used[mind->group_num_for_dim[i]] = true;
      mind->LockForGetIndex(i);
    }

  no_obj = 1;  // special case of 0 will be tested soon
  omitted_factor = 1;
  bool zero_tuples = true;
  for (uint i = 0; i < mind->dim_groups.size(); i++) {
    if (dim_group_used[i]) {
      no_obj = SafeMultiplication(no_obj, mind->dim_groups[i]->NumOfTuples());
      zero_tuples = false;
    } else {
      omitted_factor = SafeMultiplication(omitted_factor, mind->dim_groups[i]->NumOfTuples());
    }
  }
  if (zero_tuples)
    no_obj = 0;

  if (no_dims > 0) {
    it_for_dim = new int[no_dims];

    // Create group iterators: ordered filter-based first, if any
    if (po.size() > 0) {
      for (int i = 0; i < no_dims; i++)
        if (dim_group_used[mind->group_num_for_dim[i]] && mind->GetFilter(i) && po[i].Initialized()) {
          DEBUG_ASSERT(mind->IsOrderable(i));
          it.push_back(mind->group_for_dim[i]->NewOrderedIterator(dimensions, &po[i], p_power));
          dg.push_back(mind->group_for_dim[i]);
          dim_group_used[mind->group_num_for_dim[i]] = false;
        }
    }

    // Create group iterators: other filter-based
    std::vector<std::pair<int, int>> ordering_filters;
    for (uint i = 0; i < mind->dim_groups.size(); i++)
      if (dim_group_used[i] && (mind->dim_groups[i]->Type() == DimensionGroup::DGType::DG_FILTER ||
                                mind->dim_groups[i]->Type() == DimensionGroup::DGType::DG_VIRTUAL))  // filters first
        ordering_filters.push_back(std::pair<int, int>(65537 - mind->dim_groups[i]->GetFilter(-1)->DensityWeight(),
                                                       i));  // -1: the default filter for this group
    sort(ordering_filters.begin(),
         ordering_filters.end());  // order filters starting from the
                                   // densest one (for pack
                                   // decompression efficiency)

    for (uint i = 0; i < ordering_filters.size(); i++) {  // create iterators for DimensionGroup numbers from sorter
      it.push_back(mind->dim_groups[ordering_filters[i].second]->NewIterator(dimensions, p_power));
      dg.push_back(mind->dim_groups[ordering_filters[i].second]);
      dim_group_used[ordering_filters[i].second] = false;
    }

    // Create group iterators: materialized
    for (uint i = 0; i < mind->dim_groups.size(); i++)
      if (dim_group_used[i]) {
        it.push_back(mind->dim_groups[i]->NewIterator(dimensions, p_power));
        dg.push_back(mind->dim_groups[i]);
      }

    for (int i = 0; i < no_dims; i++)
      if (dimensions[i]) {
        for (uint j = 0; j < dg.size(); j++) {
          if (mind->group_for_dim[i] == dg[j]) {
            it_for_dim[i] = j;
            break;
          }
        }
      }

    if (lock) {
      mind->IteratorLock();
    }
    cur_pos = new int64_t[no_dims];
    cur_pack = new int[no_dims];
    std::fill(cur_pos, cur_pos + no_dims, common::NULL_VALUE_64);
    std::fill(cur_pack, cur_pack + no_dims, -1);

    // Check the optimized case
    if (dg.size() == 1 && dg[0]->Type() == DimensionGroup::DGType::DG_FILTER) {
      for (int i = 0; i < no_dims; i++)
        if (dimensions[i]) {
          one_filter_dim = i;
          break;
        }
      if (po.size() == 0)
        one_filter_it = (DimensionGroupFilter::DGFilterIterator *)(it[0]);
      else
        one_filter_it = (DimensionGroupFilter::DGFilterOrderedIterator *)(it[0]);
    }
  }

  delete[] dim_group_used;

  Rewind();
  if (!IsValid())
    no_obj = 0;
}

MIIterator::~MIIterator() {
  if (mind && no_dims > 0) {
    mind->IteratorUnlock();
  }
  for (uint i = 0; i < it.size(); i++) {
    delete it[i];
  }
  if (dimensions.Size() > 0)  // may be not initialized for dummy iterators
    for (int i = 0; i < no_dims; i++)
      if (dimensions[i])
        mind->UnlockFromGetIndex(i);
  delete[] cur_pos;
  delete[] cur_pack;
  delete[] it_for_dim;
  if (mind_created_locally)
    delete mind;
}

void MIIterator::Rewind() {
  valid = true;
  pack_size_left = 0;
  if (it.size() == 0) {
    valid = false;
    return;
  }
  for (uint i = 0; i < it.size(); i++) {
    it[i]->Rewind();
    if (!it[i]->IsValid()) {
      valid = false;
      return;
    }
    dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);
  }
  next_pack_started = true;
  InitNextPackrow();
}

void MIIterator::GeneralIncrement() {
  bool done = false;  // packwise order: iterate pack-by-pack
  for (uint i = 0; i < it.size(); i++) {
    bool still_in_pack = it[i]->NextInsidePack();
    if (still_in_pack)
      done = true;
    dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);

    if (done)
      break;
  }
  if (!done) {  // pack switched on every dimension - make progress in the whole
                // pack numbering
    for (uint i = 0; i < it.size(); i++) {
      it[i]->NextPackrow();
      if (it[i]->IsValid())
        done = true;
      else {
        it[i]->Rewind();  // rewind completely this dimension, increase packrow
                          // in the next one
        if (!it[i]->IsValid())
          break;
      }
      dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);

      if (done)
        break;
    }
    if (!done)
      valid = false;
  }
}

void MIIterator::NextPackrow() {
  if (!valid)
    return;
  bool done = false;
  for (uint i = 0; i < it.size(); i++) {
    it[i]->NextPackrow();
    if (it[i]->IsValid()) {
      done = true;
      dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);
    } else if (i < it.size() - 1) {
      it[i]->Rewind();
      if (!it[i]->IsValid())  // possible e.g. for updating iterator
        break;                // exit and set valid = false
      dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);
    }
    if (done)
      break;
  }
  if (!done)
    valid = false;
  else
    InitNextPackrow();
  DEBUG_ASSERT(one_filter_dim == -1 || cur_pos[one_filter_dim] >> p_power == cur_pack[one_filter_dim]);
}

bool MIIterator::WholePack(int dim) const {
  // the only possibility: just one virtual dimension, all other must be omitted
  return (it.size() == 1 && it[it_for_dim[dim]]->WholePack(dim));
}

int MIIterator::GetNextPackrow(int dim, int ahead) const { return it[it_for_dim[dim]]->GetNextPackrow(dim, ahead); }

void MIIterator::Skip(int64_t offset) {
  int64_t skipped = 0;
  while (valid && skipped + pack_size_left < offset) {
    skipped += pack_size_left;
    NextPackrow();
  }
  while (valid && skipped < offset) {
    ++skipped;
    ++(*this);
  }
}

bool MIIterator::IsThreadSafe() {
  for (uint d = 0; d < dg.size(); d++)
    if (!dg[d]->IsThreadSafe())
      return false;
  return true;
}

bool MIIterator::BarrierAfterPackrow() {
  if (!valid)
    return false;
  for (uint i = 0; i < it.size(); i++)
    if (it[i]->BarrierAfterPackrow())
      return true;
  return false;
}

MIDummyIterator::MIDummyIterator(MultiIndex *_mind) {
  mind = _mind;
  SetPower(mind->ValueOfPower());
  valid = true;
  no_dims = mind->NumOfDimensions();
  mind_created_locally = false;
  it_for_dim = nullptr;
  cur_pos = new int64_t[no_dims];
  cur_pack = new int[no_dims];
  pack_size_left = -1;
  omitted_factor = -1;
  no_obj = -1;
}

void MIDummyIterator::Combine(const MIIterator &sec)  // copy position from the second iterator (only the
                                                      // positions used)
{
  for (int i = 0; i < no_dims; i++)
    if (sec.DimUsed(i)) {
      cur_pos[i] = sec[i];
      cur_pack[i] = sec.GetCurPackrow(i);
    }
}

void MIDummyIterator::Set(int dim, int64_t val)  // set a position manually
{
  DEBUG_ASSERT(dim >= 0 && dim < no_dims);
  cur_pos[dim] = val;
  if (val == common::NULL_VALUE_64)
    cur_pack[dim] = -1;
  else
    cur_pack[dim] = int(val >> p_power);
}

void MIDummyIterator::SetPack(int dim, int p)  // set a position manually
{
  DEBUG_ASSERT(dim >= 0 && dim < no_dims);
  cur_pos[dim] = (int64_t(p) << p_power);
  cur_pack[dim] = p;
}

MIDummyIterator::MIDummyIterator(int dims) : MIIterator() {
  cur_pos = new int64_t[dims];
  cur_pack = new int[dims];
  for (int i = 0; i < dims; ++i) cur_pos[i] = cur_pack[i] = 0;
  no_dims = dims;
  valid = true;
}

bool MIDummyIterator::NullsPossibleInPack() const {
  for (int i = 0; i < no_dims; i++)
    if (cur_pos[i] == common::NULL_VALUE_64)
      return true;
  return false;
}

void MIInpackIterator::GeneralIncrement() {
  bool done = false;  // packwise order: iterate pack-by-pack
  for (uint i = 0; i < it.size(); i++) {
    bool still_in_pack = it[i]->NextInsidePack();
    if (still_in_pack)
      done = true;
    dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);
    if (done)
      break;
  }
  if (!done)  // pack switched on every dimension
    valid = false;
}

void MIInpackIterator::swap(MIInpackIterator &i) {
  std::swap(mind, i.mind);
  std::swap(mind_created_locally, i.mind_created_locally);
  std::swap(no_dims, i.no_dims);
  std::swap(cur_pos, i.cur_pos);
  std::swap(cur_pack, i.cur_pack);
  std::swap(valid, i.valid);
  std::swap(it_for_dim, i.it_for_dim);
  std::swap(omitted_factor, i.omitted_factor);
  std::swap(no_obj, i.no_obj);
  std::swap(pack_size_left, i.pack_size_left);
  std::swap(dimensions, i.dimensions);
  std::swap(it, i.it);
  std::swap(dg, i.dg);
  std::swap(next_pack_started, i.next_pack_started);
  std::swap(mii_type, i.mii_type);
  std::swap(one_filter_dim, i.one_filter_dim);
  std::swap(one_filter_it, i.one_filter_it);
}

int MIInpackIterator::GetNextPackrow([[maybe_unused]] int dim, [[maybe_unused]] int ahead) const {
  //	return it[it_for_dim[dim]]->GetNextPackrow(dim, ahead);

  return -1;  // do not prefetch basing on this iterator
}

void MIIterator::SetNoPacksToGo(int n) {
  DEBUG_ASSERT(one_filter_it);
  one_filter_it->SetNoPacksToGo(n);
}

bool MIIterator::RewindToPack(const int pack) {
  bool res = false;
  if (one_filter_it) {
    // if SetNoPacksToGo() has been used, then RewindToPack can be done only to
    // the previous pack
    res = one_filter_it->RewindToPack(pack);
    valid = one_filter_it->IsValid();
    if (valid) {
      cur_pos[one_filter_dim] = one_filter_it->GetCurPos(one_filter_dim);
      cur_pack[one_filter_dim] = int(cur_pos[one_filter_dim] >> p_power);
      pack_size_left = one_filter_it->GetPackSizeLeft();
      next_pack_started = true;
    }
  } else {
    if (dg.size() == 1) {
      res = it[0]->RewindToPack(pack);
      valid = it[0]->IsValid();
      if (valid) {
        dg[0]->FillCurrentPos(it[0], cur_pos, cur_pack, dimensions);
        next_pack_started = true;
        InitNextPackrow();
      }
    }
  }
  return res;
}

MIIterator::SliceCapability MIIterator::GetSliceCapability() const {
  // Default is SliceCapability::Type::kDisable.
  SliceCapability capability;
  if (one_filter_dim > -1) {
    capability.type = SliceCapability::Type::kLinear;
    one_filter_it->GetSlices(&capability.slices);
  } else {
    if (dg.size() == 1) {
      if (it[0]->GetSlices(&capability.slices))
        capability.type = SliceCapability::Type::kFixed;
    }
  }
  return capability;
}

void MIIterator::RewindToRow(int64_t row) {
  DEBUG_ASSERT(one_filter_it);
  one_filter_it->RewindToRow(row);
  valid = one_filter_it->IsValid();
  if (valid) {
    cur_pos[one_filter_dim] = one_filter_it->GetCurPos(one_filter_dim);
    cur_pack[one_filter_dim] = int(cur_pos[one_filter_dim] >> p_power);
    pack_size_left = one_filter_it->GetPackSizeLeft();
    next_pack_started = true;
  }
}
}  // namespace core
}  // namespace Tianmu
