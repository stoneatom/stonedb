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

#include "dimension_group_virtual.h"
#include <vector>

namespace Tianmu {
namespace core {
DimensionGroupVirtual::DimensionGroupVirtual(DimensionVector &dims, int bdim, Filter *f_source,
                                             int copy_mode)  // copy_mode: 0 - copy filter, 1 - ShallowCopy filter, 2 -
                                                             // grab pointer
{
  dims_used = dims;
  base_dim = bdim;
  no_dims = dims.Size();
  f = nullptr;
  if (copy_mode == 0)
    f = new Filter(*f_source);
  else if (copy_mode == 1)
    f = Filter::ShallowCopy(*f_source);
  else if (copy_mode == 2)
    f = f_source;
  dim_group_type = DGType::DG_VIRTUAL;
  no_obj = f->NumOfOnes();
  pack_pos = nullptr;  // created if needed
  t = new IndexTable *[no_dims];
  nulls_possible = new bool[no_dims];
  for (int i = 0; i < no_dims; i++) {
    t[i] = nullptr;
    nulls_possible[i] = false;
  }
}

DimensionGroupVirtual::~DimensionGroupVirtual() {
  delete f;
  for (int i = 0; i < no_dims; i++) delete t[i];
  delete[] t;
  delete[] pack_pos;
  delete[] nulls_possible;
}

DimensionGroup *DimensionGroupVirtual::Clone(bool shallow) {
  DimensionGroupVirtual *new_value = new DimensionGroupVirtual(dims_used, base_dim, f, (shallow ? 1 : 0));

  // Member t is a pointer pointer, it stores an array of index-table pointer.
  // The index-table pointer should not be missed when shallow is true.
  for (int i = 0; i < no_dims; i++) {
    if (!t[i]) {
      continue;
    }

    new_value->nulls_possible[i] = nulls_possible[i];
    if (shallow) {
      new_value->t[i] = t[i];
    } else {
      t[i]->Lock();
      new_value->t[i] = new IndexTable(*t[i]);
      t[i]->Unlock();
    }
  }
  return new_value;
}

void DimensionGroupVirtual::Empty() {
  f->Reset();
  for (int i = 0; i < no_dims; i++) {
    delete t[i];
    t[i] = nullptr;
  }
  no_obj = 0;
}

void DimensionGroupVirtual::NewDimensionContent(int dim, IndexTable *tnew,
                                                bool nulls)  // tnew will be added (as a pointer to be deleted by
                                                             // destructor) on a dimension dim
{
  DEBUG_ASSERT(dims_used[dim]);
  delete t[dim];
  t[dim] = tnew;
  nulls_possible[dim] = nulls;
}

DimensionGroup::Iterator *DimensionGroupVirtual::NewIterator(DimensionVector &dim, uint32_t power) {
  return new DGVirtualIterator(f, base_dim, dim, t, power);
}

DimensionGroup::Iterator *DimensionGroupVirtual::NewOrderedIterator(DimensionVector &dim, PackOrderer *po,
                                                                    uint32_t power) {
  if (pack_pos == nullptr) {  // not used yet - create
    int no_packs = f->NumOfBlocks();
    pack_pos = new int64_t[no_packs];
    int64_t cur_pack_start = 0;
    for (int i = 0; i < no_packs; i++) {
      pack_pos[i] = cur_pack_start;
      cur_pack_start += f->NumOfOnes(i);
    }
  }
  return new DGVirtualOrderedIterator(f, base_dim, dim, pack_pos, t, po, power);
}

void DimensionGroupVirtual::FillCurrentPos(DimensionGroup::Iterator *it, int64_t *cur_pos, int *cur_pack,
                                           DimensionVector &dims) {
  if (dims[base_dim]) {
    cur_pos[base_dim] = it->GetCurPos(base_dim);
    cur_pack[base_dim] = it->GetCurPackrow(base_dim);
  }
  for (int d = 0; d < no_dims; d++)
    if (dims[d] && t[d]) {
      cur_pos[d] = it->GetCurPos(d);
      cur_pack[d] = it->GetCurPackrow(d);
    }
}

DimensionGroup::Iterator *DimensionGroupVirtual::CopyIterator(DimensionGroup::Iterator *s, uint32_t power) {
  DGVirtualIterator *sfit = (DGVirtualIterator *)s;
  if (sfit->Ordered())
    return new DGVirtualOrderedIterator(*s, power);
  return new DGVirtualIterator(*s, power);
}

void DimensionGroupVirtual::UpdateNumOfTuples() {
  no_obj = f->NumOfOnes();
  for (int d = 0; d < no_dims; d++)
    if (t[d])
      DEBUG_ASSERT((uint64_t)no_obj <= t[d]->N());  // N() is an upper size of buffer
}

bool DimensionGroupVirtual::IsOrderable() {
  // orderable only if all IndexTables are one-block, otherwise shuffling will
  // occur (and IT block may end inside a packrow)
  for (int d = 0; d < no_dims; d++)
    if (t[d] && t[d]->EndOfCurrentBlock(0) < t[d]->N())
      return false;
  return true;
}

DimensionGroupVirtual::DGVirtualIterator::DGVirtualIterator(Filter *f_to_iterate, int b_dim, DimensionVector &dims,
                                                            IndexTable **tt, uint32_t power)
    : fi(f_to_iterate, power), f(f_to_iterate), t(tt) {
  valid = !f->IsEmpty();
  base_dim = b_dim;
  no_dims = dims.Size();
  nulls_found = new bool[no_dims];
  cur_pack = new int[no_dims];
  no_obj = f->NumOfOnes();
  for (int d = 0; d < no_dims; d++) {
    nulls_found[d] = false;
    cur_pack[d] = -1;
    if (t[d] && dims[d]) {
      for (int64_t i = 0; i < no_obj; i++) {
        uint64_t p = t[d]->Get64(i);
        if (p != 0) {
          cur_pack[d] = int((p - 1) >> power);
          break;
        }
      }
    }
  }
  dim_pos = 0;
  cur_pack_start = 0;
}

DimensionGroupVirtual::DGVirtualIterator::~DGVirtualIterator() {
  delete[] nulls_found;
  delete[] cur_pack;
}

DimensionGroupVirtual::DGVirtualIterator::DGVirtualIterator(const Iterator &sec, [[maybe_unused]] uint32_t power)
    : DimensionGroup::Iterator(sec) {
  DGVirtualIterator *s = (DGVirtualIterator *)(&sec);
  fi = s->fi;
  dim_pos = s->dim_pos;
  cur_pack_start = s->cur_pack_start;
  base_dim = s->base_dim;
  f = s->f;
  no_dims = s->no_dims;
  no_obj = s->no_obj;
  t = s->t;
  nulls_found = new bool[no_dims];
  cur_pack = new int[no_dims];
  for (int dim = 0; dim < no_dims; dim++) {
    nulls_found[dim] = s->nulls_found[dim];
    cur_pack[dim] = s->cur_pack[dim];
  }
}

void DimensionGroupVirtual::DGVirtualIterator::operator++() {
  DEBUG_ASSERT(valid);
  ++fi;
  valid = fi.IsValid();
  dim_pos++;
  // WARNING: do not mix iterating with ++ and NextInsidePack(), as the value
  // cur_pack_start is not updated here (for performance reasons)
}

bool DimensionGroupVirtual::DGVirtualIterator::NextInsidePack() {
  bool r = fi.NextInsidePack();
  valid = fi.IsValid();
  dim_pos++;
  if (!r)
    dim_pos = cur_pack_start;
  return r;
}

void DimensionGroupVirtual::DGVirtualIterator::NextPackrow() {
  DEBUG_ASSERT(valid);
  dim_pos += fi.GetPackSizeLeft();
  cur_pack_start = dim_pos;
  fi.NextPack();
  valid = fi.IsValid();
}

int64_t DimensionGroupVirtual::DGVirtualIterator::GetCurPos(int dim) {
  DEBUG_ASSERT(dim_pos < no_obj);
  if (dim == base_dim)
    return (*fi);
  int64_t res = t[dim]->Get64(dim_pos);
  if (res == 0)
    return common::NULL_VALUE_64;
  return res - 1;
}

bool DimensionGroupVirtual::DGVirtualIterator::BarrierAfterPackrow() {
  // Barrier if either the current or the next block is on a boundary
  int64_t next_2_packs_start =
      dim_pos + fi.GetPackSizeLeft() + (1 << f->GetPower());  // upper bound for next 2 packs //65536
  if (dim_pos + fi.GetPackSizeLeft() >= no_obj)               // important e.g. for case when we're restarting
                                                              // the same dimension group (iterating on a
                                                              // product of many groups)
    return true;
  for (int i = 0; i < no_dims; i++)
    if (t[i] && (uint64_t)next_2_packs_start >= t[i]->EndOfCurrentBlock(dim_pos))
      return true;
  return false;
}

DimensionGroupVirtual::DGVirtualOrderedIterator::DGVirtualOrderedIterator(Filter *f_to_iterate, int b_dim,
                                                                          DimensionVector &dims, int64_t *ppos,
                                                                          IndexTable **tt, PackOrderer *po,
                                                                          uint32_t power)
    : fi(f_to_iterate, po, power), f(f_to_iterate), t(tt) {
  valid = !f->IsEmpty();
  base_dim = b_dim;
  no_dims = dims.Size();
  pack_pos = ppos;
  nulls_found = new bool[no_dims];
  cur_pack = new int[no_dims];
  int64_t no_obj = f->NumOfOnes();
  for (int d = 0; d < no_dims; d++) {
    nulls_found[d] = false;
    cur_pack[d] = -1;
    if (t[d] && dims[d]) {
      for (int64_t i = 0; i < no_obj; i++) {
        uint64_t p = t[d]->Get64(i);
        if (p != 0) {
          cur_pack[d] = int((p - 1) >> power);
          break;
        }
      }
    }
  }
  dim_pos = -1;
  if (valid)
    dim_pos = pack_pos[fi.GetCurrPack()];
  cur_pack_start = dim_pos;
}

DimensionGroupVirtual::DGVirtualOrderedIterator::DGVirtualOrderedIterator(const Iterator &sec,
                                                                          [[maybe_unused]] uint32_t power)
    : DimensionGroup::Iterator(sec) {
  DGVirtualOrderedIterator *s = (DGVirtualOrderedIterator *)(&sec);
  fi = s->fi;
  f = s->f;
  dim_pos = s->dim_pos;
  cur_pack_start = s->cur_pack_start;
  base_dim = s->base_dim;
  no_dims = s->no_dims;
  t = s->t;
  pack_pos = s->pack_pos;
  nulls_found = new bool[no_dims];
  cur_pack = new int[no_dims];
  for (int dim = 0; dim < no_dims; dim++) {
    nulls_found[dim] = s->nulls_found[dim];
    cur_pack[dim] = s->cur_pack[dim];
  }
}

DimensionGroupVirtual::DGVirtualOrderedIterator::~DGVirtualOrderedIterator() {
  delete[] nulls_found;
  delete[] cur_pack;
}

void DimensionGroupVirtual::DGVirtualOrderedIterator::operator++() {
  DEBUG_ASSERT(valid);
  bool new_pack = (fi.GetPackSizeLeft() <= 1);
  ++fi;
  valid = fi.IsValid();
  dim_pos++;
  if (new_pack && valid) {
    dim_pos = pack_pos[fi.GetCurrPack()];
    cur_pack_start = dim_pos;
  }
}

bool DimensionGroupVirtual::DGVirtualOrderedIterator::NextInsidePack() {
  bool r = fi.NextInsidePack();
  valid = fi.IsValid();
  dim_pos++;
  if (!r)
    dim_pos = cur_pack_start;
  return r;
}

void DimensionGroupVirtual::DGVirtualOrderedIterator::Rewind() {
  fi.Rewind();
  valid = fi.IsValid();
  if (valid)
    dim_pos = pack_pos[fi.GetCurrPack()];
  cur_pack_start = dim_pos;
}

void DimensionGroupVirtual::DGVirtualOrderedIterator::NextPackrow() {
  DEBUG_ASSERT(valid);
  fi.NextPack();
  valid = fi.IsValid();
  if (valid)
    dim_pos = pack_pos[fi.GetCurrPack()];
  cur_pack_start = dim_pos;
}

int64_t DimensionGroupVirtual::DGVirtualOrderedIterator::GetCurPos(int dim) {
  if (dim == base_dim)
    return (*fi);
  int64_t res = t[dim]->Get64(dim_pos);
  if (res == 0)
    return common::NULL_VALUE_64;
  return res - 1;
}
}  // namespace core
}  // namespace Tianmu
