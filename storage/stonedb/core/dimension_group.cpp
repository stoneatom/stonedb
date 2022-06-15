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

#include "dimension_group.h"
#include <vector>

namespace stonedb {
namespace core {
DimensionGroupFilter::DimensionGroupFilter(int dim, int64_t size, uint32_t power) {
  base_dim = dim;
  f = new Filter(size, power, true);  // all ones
  dim_group_type = DGType::DG_FILTER;
  no_obj = size;
}

// copy_mode: 0 - copy filter, 1 - ShallowCopy filter, 2 - grab pointer
DimensionGroupFilter::DimensionGroupFilter(int dim, Filter *f_source, int copy_mode, [[maybe_unused]] uint32_t power) {
  base_dim = dim;
  f = NULL;
  if (copy_mode == 0)
    f = new Filter(*f_source);
  else if (copy_mode == 1)
    f = Filter::ShallowCopy(*f_source);
  else if (copy_mode == 2)
    f = f_source;
  dim_group_type = DGType::DG_FILTER;
  no_obj = f->NoOnes();
}

DimensionGroupFilter::~DimensionGroupFilter() { delete f; }

DimensionGroup::Iterator *DimensionGroupFilter::NewIterator([[maybe_unused]] DimensionVector &dim, uint32_t power) {
  DEBUG_ASSERT(dim[base_dim]);
  return new DGFilterIterator(f, power);
}

DimensionGroup::Iterator *DimensionGroupFilter::NewOrderedIterator([[maybe_unused]] DimensionVector &dim,
                                                                   PackOrderer *po, uint32_t power) {
  DEBUG_ASSERT(dim[base_dim]);
  return new DGFilterOrderedIterator(f, po, power);
}

DimensionGroupFilter::DGFilterIterator::DGFilterIterator(const Iterator &sec, [[maybe_unused]] uint32_t power)
    : DimensionGroup::Iterator(sec) {
  DGFilterIterator *s = (DGFilterIterator *)(&sec);
  fi = s->fi;
  f = s->f;
}

DimensionGroupFilter::DGFilterOrderedIterator::DGFilterOrderedIterator(const Iterator &sec,
                                                                       [[maybe_unused]] uint32_t power)
    : DimensionGroup::Iterator(sec) {
  DGFilterOrderedIterator *s = (DGFilterOrderedIterator *)(&sec);
  fi = s->fi;
  f = s->f;
}

DimensionGroup::Iterator *DimensionGroupFilter::CopyIterator(DimensionGroup::Iterator *s, uint32_t power) {
  DGFilterIterator *sfit = (DGFilterIterator *)s;
  if (sfit->Ordered()) return new DGFilterOrderedIterator(*s, power);
  return new DGFilterIterator(*s, power);
}

DimensionGroupMaterialized::DimensionGroupMaterialized(DimensionVector &dims) {
  dim_group_type = DGType::DG_INDEX_TABLE;
  dims_used = dims;
  no_dims = dims.Size();
  no_obj = 0;
  t = new IndexTable *[no_dims];
  nulls_possible = new bool[no_dims];
  for (int i = 0; i < no_dims; i++) {
    t[i] = NULL;
    nulls_possible[i] = false;
  }
}

DimensionGroup *DimensionGroupMaterialized::Clone(bool shallow) {
  DimensionGroupMaterialized *new_value = new DimensionGroupMaterialized(dims_used);
  new_value->no_obj = no_obj;
  if (shallow) return new_value;
  for (int i = 0; i < no_dims; i++) {
    if (t[i]) {
      new_value->nulls_possible[i] = nulls_possible[i];
      t[i]->Lock();
      new_value->t[i] = new IndexTable(*t[i]);
      t[i]->Unlock();
    }
  }
  return new_value;
}

DimensionGroupMaterialized::~DimensionGroupMaterialized() {
  for (int i = 0; i < no_dims; i++) delete t[i];
  delete[] t;
  delete[] nulls_possible;
}

void DimensionGroupMaterialized::Empty() {
  for (int i = 0; i < no_dims; i++) {
    delete t[i];
    t[i] = NULL;
  }
  no_obj = 0;
}

void DimensionGroupMaterialized::NewDimensionContent(int dim, IndexTable *tnew,
                                                     bool nulls)  // tnew will be added (as a pointer to be deleted by
                                                                  // destructor) on a dimension dim
{
  DEBUG_ASSERT(dims_used[dim]);
  delete t[dim];
  t[dim] = tnew;
  nulls_possible[dim] = nulls;
}

void DimensionGroupMaterialized::FillCurrentPos(DimensionGroup::Iterator *it, int64_t *cur_pos, int *cur_pack,
                                                DimensionVector &dims) {
  for (int d = 0; d < no_dims; d++)
    if (dims[d] && t[d]) {
      cur_pos[d] = it->GetCurPos(d);
      cur_pack[d] = it->GetCurPackrow(d);
    }
}

DimensionGroup::Iterator *DimensionGroupMaterialized::NewIterator(DimensionVector &dim, uint32_t power) {
  DEBUG_ASSERT(no_dims == dim.Size());  // otherwise incompatible dimensions
  return new DGMaterializedIterator(no_obj, dim, t, nulls_possible, power);
}

// Iterator:

DimensionGroupMaterialized::DGMaterializedIterator::DGMaterializedIterator(int64_t _no_obj, DimensionVector &dims,
                                                                           IndexTable **_t, bool *nulls,
                                                                           uint32_t power) {
  p_power = power;
  no_obj = _no_obj;
  cur_pack_start = 0;
  no_dims = dims.Size();
  t = new IndexTable *[no_dims];
  one_packrow = new bool[no_dims];  // dimensions containing values from one packrow only
  nulls_found = new bool[no_dims];
  next_pack = new int64_t[no_dims];
  ahead1 = new int64_t[no_dims];
  ahead2 = new int64_t[no_dims];
  ahead3 = new int64_t[no_dims];
  cur_pack = new int[no_dims];
  nulls_possible = nulls;
  inside_one_pack = false;  // to be determined later...
  for (int dim = 0; dim < no_dims; dim++) {
    one_packrow[dim] = false;
    nulls_found[dim] = false;
    cur_pack[dim] = -1;
    next_pack[dim] = 0;
    ahead1[dim] = ahead2[dim] = ahead3[dim] = -1;
    if (_t[dim] && dims[dim]) {
      t[dim] = _t[dim];
      one_packrow[dim] =
          (t[dim]->OrigSize() <= ((1 << p_power) - 1)) && (t[dim]->EndOfCurrentBlock(0) >= (uint64_t)no_obj);
    } else
      t[dim] = NULL;
  }
  Rewind();
}

DimensionGroupMaterialized::DGMaterializedIterator::DGMaterializedIterator(const Iterator &sec, uint32_t power)
    : DimensionGroup::Iterator(sec) {
  p_power = power;
  DGMaterializedIterator *s = (DGMaterializedIterator *)(&sec);
  no_obj = s->no_obj;
  cur_pack_start = s->cur_pack_start;
  no_dims = s->no_dims;
  t = new IndexTable *[no_dims];
  one_packrow = new bool[no_dims];  // dimensions containing values from one packrow only
  nulls_found = new bool[no_dims];
  next_pack = new int64_t[no_dims];
  ahead1 = new int64_t[no_dims];
  ahead2 = new int64_t[no_dims];
  ahead3 = new int64_t[no_dims];
  cur_pack = new int[no_dims];
  nulls_possible = s->nulls_possible;
  inside_one_pack = s->inside_one_pack;
  for (int dim = 0; dim < no_dims; dim++) {
    one_packrow[dim] = s->one_packrow[dim];
    nulls_found[dim] = s->nulls_found[dim];
    next_pack[dim] = s->next_pack[dim];
    ahead1[dim] = s->ahead1[dim];
    ahead2[dim] = s->ahead2[dim];
    ahead3[dim] = s->ahead3[dim];
    cur_pack[dim] = s->cur_pack[dim];
    t[dim] = s->t[dim];
  }
  pack_size_left = s->pack_size_left;
  cur_pos = s->cur_pos;
}

DimensionGroupMaterialized::DGMaterializedIterator::~DGMaterializedIterator() {
  delete[] t;
  delete[] one_packrow;
  delete[] nulls_found;
  delete[] next_pack;
  delete[] ahead1;
  delete[] ahead2;
  delete[] ahead3;
  delete[] cur_pack;
}

void DimensionGroupMaterialized::DGMaterializedIterator::Rewind() {
  cur_pos = 0;
  cur_pack_start = 0;
  valid = true;
  for (int dim = 0; dim < no_dims; dim++) {
    next_pack[dim] = 0;
    ahead1[dim] = ahead2[dim] = ahead3[dim] = -1;
  }
  InitPackrow();
}

void DimensionGroupMaterialized::DGMaterializedIterator::InitPackrow() {
  if (cur_pos >= no_obj) {
    valid = false;
    return;
  }
  int64_t cur_end_packrow = no_obj;
  for (int i = 0; i < no_dims; i++)
    if (t[i]) {
      if (cur_pos >= next_pack[i]) {
        if (!one_packrow[i] || nulls_possible[i])
          FindPackEnd(i);
        else {
          next_pack[i] = std::min(no_obj, (int64_t)(t[i]->EndOfCurrentBlock(cur_pos)));
          cur_pack[i] = int((t[i]->Get64(cur_pos) - 1) >> p_power);
          ahead1[i] = ahead2[i] = ahead3[i] = -1;
        }
      }
      if (next_pack[i] < cur_end_packrow) cur_end_packrow = next_pack[i];
    }
  pack_size_left = cur_end_packrow - cur_pos;
  cur_pack_start = cur_pos;
  if (cur_pos == 0 && pack_size_left == no_obj) inside_one_pack = true;
  DEBUG_ASSERT(pack_size_left > 0);
}

bool DimensionGroupMaterialized::DGMaterializedIterator::NextInsidePack() {
  cur_pos++;
  pack_size_left--;
  if (pack_size_left == 0) {
    pack_size_left = cur_pos - cur_pack_start;
    cur_pos = cur_pack_start;
    return false;
  }
  return true;
}

void DimensionGroupMaterialized::DGMaterializedIterator::JumpToNextPack(uint64_t &loc_iterator, IndexTable *cur_t,
                                                                        uint64_t loc_limit) {
  if (loc_iterator >= loc_limit) return;
  auto loc_pack = ((cur_t->Get64(loc_iterator) - 1) >> p_power);
  ++loc_iterator;
  while (loc_iterator < loc_limit &&  // find the first row from another pack
                                      // (but the same block)
         (cur_t->Get64InsideBlock(loc_iterator) - 1) >> p_power == loc_pack) {
    ++loc_iterator;
  }
}

void DimensionGroupMaterialized::DGMaterializedIterator::FindPackEnd(int dim) {
  IndexTable *cur_t = t[dim];
  uint64_t loc_iterator = next_pack[dim];
  uint64_t loc_limit = std::min((uint64_t)no_obj, cur_t->EndOfCurrentBlock(loc_iterator));
  uint loc_pack = -1;
  nulls_found[dim] = false;
  if (!nulls_possible[dim]) {
    cur_pack[dim] = int((cur_t->Get64(loc_iterator) - 1) >> p_power);
    if (cur_t->OrigSize() <= ((1 << p_power) - 1)) {
      next_pack[dim] = loc_limit;
      ahead1[dim] = ahead2[dim] = ahead3[dim] = -1;
    } else {
      uint64_t pos_ahead = loc_iterator;
      if (ahead1[dim] == -1) {
        JumpToNextPack(pos_ahead, cur_t, loc_limit);
      } else
        pos_ahead = ahead1[dim];
      next_pack[dim] = pos_ahead;
      if (ahead2[dim] == -1) {
        JumpToNextPack(pos_ahead, cur_t, loc_limit);
      } else
        pos_ahead = ahead2[dim];
      ahead1[dim] = (pos_ahead < loc_limit ? pos_ahead : -1);
      if (ahead3[dim] == -1) {
        JumpToNextPack(pos_ahead, cur_t, loc_limit);
      } else
        pos_ahead = ahead3[dim];
      ahead2[dim] = (pos_ahead < loc_limit ? pos_ahead : -1);
      JumpToNextPack(pos_ahead, cur_t, loc_limit);
      ahead3[dim] = (pos_ahead < loc_limit ? pos_ahead : -1);
    }
  } else {
    // Nulls possible: do not use ahead1...3, because the current pack has to be
    // checked for nulls anyway
    while (loc_iterator < loc_limit &&  // find the first non-NULL row (NULL row
                                        // is when Get64() = 0)
           cur_t->Get64(loc_iterator) == 0) {
      nulls_found[dim] = true;
      ++loc_iterator;
    }
    if (loc_iterator < loc_limit) {
      loc_pack = ((cur_t->Get64(loc_iterator) - 1) >> p_power);
      ++loc_iterator;
      uint64_t ndx;
      while (loc_iterator < loc_limit &&  // find the first non-NULL row from
                                          // another pack (but the same block)
             ((ndx = cur_t->Get64InsideBlock(loc_iterator)) == 0 || ((ndx - 1) >> p_power) == loc_pack)) {
        if (ndx == 0) nulls_found[dim] = true;
        ++loc_iterator;
      }
    }
    cur_pack[dim] = loc_pack;
    next_pack[dim] = loc_iterator;
  }
  DEBUG_ASSERT(next_pack[dim] > cur_pos);
}

int DimensionGroupMaterialized::DGMaterializedIterator::GetNextPackrow(int dim, int ahead) {
  MEASURE_FET("DGMaterializedIterator::GetNextPackrow(int dim, int ahead)");
  if (ahead == 0) return GetCurPackrow(dim);
  IndexTable *cur_t = t[dim];
  if (cur_t == NULL) return -1;
  uint64_t end_block = cur_t->EndOfCurrentBlock(cur_pos);
  if (next_pack[dim] >= no_obj || uint64_t(next_pack[dim]) >= end_block) return -1;
  uint64_t ahead_pos = 0;
  //	cout << "dim " << dim << ",  " << next_pack[dim] << " -> " <<
  // ahead1[dim] << "  " <<
  // ahead2[dim] << "  " << ahead3[dim] << "    (" << ahead << ")" << endl;
  if (ahead == 1)
    ahead_pos = t[dim]->Get64InsideBlock(next_pack[dim]);
  else if (ahead == 2 && ahead1[dim] != -1)
    ahead_pos = t[dim]->Get64InsideBlock(ahead1[dim]);
  else if (ahead == 3 && ahead2[dim] != -1)
    ahead_pos = t[dim]->Get64InsideBlock(ahead2[dim]);
  else if (ahead == 4 && ahead3[dim] != -1)
    ahead_pos = t[dim]->Get64InsideBlock(ahead3[dim]);
  if (ahead_pos == 0) return -1;
  return int((ahead_pos - 1) >> p_power);

  return -1;
}

bool DimensionGroupMaterialized::DGMaterializedIterator::BarrierAfterPackrow() {
  int64_t next_pack_start = cur_pos + pack_size_left;
  if (next_pack_start >= no_obj)  // important e.g. for case when we're restarting the same
                                  // dimension group (iterating on a product of many groups)
    return true;
  for (int i = 0; i < no_dims; i++)
    if (t[i] && (uint64_t)next_pack_start >= t[i]->EndOfCurrentBlock(cur_pos)) return true;
  return false;
}
}  // namespace core
}  // namespace stonedb
