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

#include "core/dimension_group_multiple.h"

namespace Tianmu {
namespace core {
bool IndexTableItem::FindPackEnd(int64_t pos, int64_t *cur_pack, int64_t *next_pack_pos) {
  if (pos < 0 || pos >= count_)
    return false;

  // Initial set next_pack_pos to the last pos.
  *next_pack_pos = count_;

  if (!nulls_possible_) {
    // Because the IndexTable is sorted by pack, so traversal it to get the
    // first different pack.
    *cur_pack = int((table_->Get64(pos) - 1) >> power_);
    for (int index = pos + 1; index < count_; ++index) {
      int pack = int((table_->Get64(index) - 1) >> power_);
      if (pack != *cur_pack) {
        *next_pack_pos = index;
        break;
      }
    }
  } else {
    while (pos < count_ && table_->Get64(pos) == 0) {
      parent_index_->SetNullsExist();
      ++pos;
    }
    if (pos < count_) {
      *cur_pack = int((table_->Get64(pos++) - 1) >> power_);

      while (pos < count_) {
        uint64_t ndx = table_->Get64InsideBlock(pos);
        if (ndx == 0) {
          parent_index_->SetNullsExist();
        } else if ((ndx - 1) >> power_ != static_cast<uint64_t>(*cur_pack)) {
          *next_pack_pos = pos;
          break;
        }
        ++pos;
      }
    }
  }

  return true;
}

//-------------------------------MultiIndexTable::Iterator------------------------------------------

// For iterator.
void MultiIndexTable::Iterator::Rewind() {
  valid_ = true;
  table_index_ = 0;
  cur_pos_ = 0;
  cur_pack_start_ = 0;

  InitPackrow();
}

void MultiIndexTable::Iterator::Increment() {
  cur_pos_++;
  pack_size_left_--;

  if (pack_size_left_ == 0)
    InitPackrow();
}

int64_t MultiIndexTable::Iterator::GetCurPos() {
  DEBUG_ASSERT(table_index_ < index_table_->table_items_.size());
  DEBUG_ASSERT(cur_pos_ >= 0);
  IndexTableItem &table(index_table_->table_items_[table_index_]);
  if (cur_pos_ >= table.GetCount())
    return common::NULL_VALUE_64;
  return (int64_t)table.GetTable()->Get64(cur_pos_);
}

void MultiIndexTable::Iterator::Skip(int64_t offset) {
  DEBUG_ASSERT(offset <= pack_size_left_);

  cur_pos_ += offset;
  pack_size_left_ -= offset;

  if (pack_size_left_ == 0)
    InitPackrow();
}

bool MultiIndexTable::Iterator::NextInsidePack() {
  cur_pos_++;
  pack_size_left_--;
  if (pack_size_left_ == 0) {
    return false;
  }
  return true;
}

bool MultiIndexTable::Iterator::BarrierAfterPackrow() {
  if (valid_) {
    int64_t next_pack_start = cur_pos_ + index_table_->table_items_[table_index_].GetCount() + pack_size_left_;
    if (next_pack_start >= index_table_->total_count_)
      return true;
  }
  return false;
}

void MultiIndexTable::Iterator::SetFixedBlockIndex(size_t index) {
  DEBUG_ASSERT(index < index_table_->table_items_.size());
  valid_ = true;
  table_index_ = index;
  cur_pos_ = 0;
  fixed_ = true;

  InitPackrow();
}

void MultiIndexTable::Iterator::InitPackrow() {
  if (table_index_ >= index_table_->table_items_.size()) {
    valid_ = false;
    return;
  }

  IndexTableItem *index_table = &index_table_->table_items_[table_index_];
  while (cur_pos_ >= index_table->GetCount()) {
    cur_pos_ -= index_table->GetCount();
    if (fixed_ || ++table_index_ >= index_table_->table_items_.size()) {
      valid_ = false;
      return;
    }

    index_table = &index_table_->table_items_[table_index_];
  }

  int64_t next_pack_pos = 0;
  index_table->FindPackEnd(cur_pos_, &cur_pack_, &next_pack_pos);
  pack_size_left_ = next_pack_pos - cur_pos_;
  cur_pack_start_ = cur_pos_;

  DEBUG_ASSERT(pack_size_left_ > 0);
}

//-------------------------------MultiIndexTable------------------------------------------
MultiIndexTable::MultiIndexTable(uint32_t power) : power_(power) {}

MultiIndexTable::MultiIndexTable(MultiIndexTable &tables)
    : nulls_exist_(tables.nulls_exist_), nulls_possible_(tables.nulls_possible_), table_items_(tables.table_items_) {}

void MultiIndexTable::AddTable(IndexTable *table, uint64_t count, bool nulls_possible) {
  nulls_possible_ = nulls_possible_ || nulls_possible;
  table_items_.emplace_back(this, table, count, power_, nulls_possible);
  total_count_ += count;
}

void MultiIndexTable::Lock(int n) {
  for (auto &table_item : table_items_) {
    for (int index = 0; index < n; index++) table_item.GetTable()->Lock();
  }
}

void MultiIndexTable::Unlock() {
  for (auto &table_item : table_items_) {
    table_item.GetTable()->Unlock();
  }
}

int MultiIndexTable::NumOfLocks() {
  int locks = 0;
  if (!table_items_.empty())
    locks = table_items_[0].GetTable()->NumOfLocks();
  return locks;
}

//----------------------------DimensionGroupMultiMaterialized-------------------------------------------
// The no_obj need to preset, because AddDimensionContent maybe not called on
// uninvolved scenes.
DimensionGroupMultiMaterialized::DimensionGroupMultiMaterialized(int64_t obj, DimensionVector &dims, uint32_t power,
                                                                 bool is_shallow_memory)
    : power_(power), dims_used_(dims), is_shallow_memory(is_shallow_memory) {
  dim_group_type = DGType::DG_INDEX_TABLE;
  no_obj = obj;
  dims_count_ = dims_used_.Size();
  dim_tables_.resize(dims_count_);
}

DimensionGroupMultiMaterialized::~DimensionGroupMultiMaterialized() {
  if (is_shallow_memory) {
    return;
  }

  for (auto it : dim_tables_) delete it;
}

DimensionGroup *DimensionGroupMultiMaterialized::Clone(bool shallow) {
  DimensionGroupMultiMaterialized *new_value = new DimensionGroupMultiMaterialized(no_obj, dims_used_, power_, shallow);
  for (int index = 0; index < dims_count_; ++index) {
    MultiIndexTable *tables = dim_tables_[index];
    if (tables) {
      tables->Lock();
      if (shallow) {
        new_value->dim_tables_[index] = tables;
      } else {
        new_value->dim_tables_[index] = new MultiIndexTable(*tables);
      }

      tables->Unlock();
    }
  }
  return new_value;
}

void DimensionGroupMultiMaterialized::Empty() {
  for (int index = 0; index < dims_count_; ++index) {
    delete dim_tables_[index];
    dim_tables_[index] = nullptr;
  }
  no_obj = 0;
}

void DimensionGroupMultiMaterialized::AddDimensionContent(int dim, IndexTable *table, int count, bool nulls) {
  DEBUG_ASSERT(dims_used_[dim]);

  MultiIndexTable *tables = dim_tables_[dim];
  if (!tables) {
    tables = new MultiIndexTable(power_);
    dim_tables_[dim] = tables;
  }

  tables->AddTable(table, count, nulls);
}

void DimensionGroupMultiMaterialized::FillCurrentPos(DimensionGroup::Iterator *iter, int64_t *cur_pos, int *cur_pack,
                                                     DimensionVector &dims) {
  for (int d = 0; d < dims_count_; d++)
    if (dims[d] && dim_tables_[d]) {
      cur_pos[d] = iter->GetCurPos(d);
      cur_pack[d] = iter->GetCurPackrow(d);
    }
}

void DimensionGroupMultiMaterialized::Lock(int dim, int n) {
  MultiIndexTable *tables = dim_tables_[dim];
  if (tables)
    tables->Lock(n);
}

void DimensionGroupMultiMaterialized::Unlock(int dim) {
  MultiIndexTable *tables = dim_tables_[dim];
  if (tables)
    tables->Unlock();
}

int DimensionGroupMultiMaterialized::NumOfLocks(int dim) {
  MultiIndexTable *tables = dim_tables_[dim];
  return (tables ? tables->NumOfLocks() : 0);
}

DimensionGroup::Iterator *DimensionGroupMultiMaterialized::NewIterator(DimensionVector &dim, uint32_t power) {
  DEBUG_ASSERT(dims_count_ == dim.Size());  // Otherwise incompatible dimensions.
  return new DGIterator(no_obj, dim, dim_tables_, power);
}

DimensionGroup::Iterator *DimensionGroupMultiMaterialized::CopyIterator(DimensionGroup::Iterator *iter,
                                                                        uint32_t power) {
  return new DGIterator(*iter, power);
}

//------------------------DimensionGroupMultiMaterialized::DGIterator------------------------------------
DimensionGroupMultiMaterialized::DGIterator::DGIterator(int64_t total_count, DimensionVector &dims,
                                                        std::vector<MultiIndexTable *> &dim_tables, uint32_t power)
    : total_count_(total_count), power_(power) {
  dim_tables_iterator_.resize(dims.Size());
  for (int dim = 0; dim < dims.Size(); dim++) {
    auto dim_table = dim_tables[dim];
    if (dim_table && dims[dim])
      dim_tables_iterator_[dim] = std::make_unique<MultiIndexTable::Iterator>(dim_table);
  }

  Rewind();
}

DimensionGroupMultiMaterialized::DGIterator::DGIterator(const Iterator &sec, uint32_t power)
    : DimensionGroup::Iterator(sec), power_(power) {
  const DGIterator &s = dynamic_cast<const DGIterator &>(sec);
  total_count_ = s.total_count_;
  dim_tables_iterator_.resize(s.dim_tables_iterator_.size());
  for (size_t dim = 0; dim < s.dim_tables_iterator_.size(); dim++) {
    auto &src_iter = s.dim_tables_iterator_[dim];
    if (src_iter)
      dim_tables_iterator_[dim] = std::make_unique<MultiIndexTable::Iterator>(*src_iter);
  }
}

void DimensionGroupMultiMaterialized::DGIterator::operator++() {
  for (auto &it : dim_tables_iterator_) {
    if (it) {
      it->Increment();
      valid = valid && it->IsValid();
    }
  }
}

void DimensionGroupMultiMaterialized::DGIterator::Rewind() {
  // If total_count_ > 0 and dim_tables_iterator_ has null iterators,
  // because there are only constants in the diplaying, e.g. select 1 from xxx
  // join xx.
  valid = (total_count_ > 0) ? true : false;
  for (auto &it : dim_tables_iterator_) {
    if (it) {
      it->Rewind();
      valid = valid && it->IsValid();
    }
  }
}

bool DimensionGroupMultiMaterialized::DGIterator::NextInsidePack() {
  bool nex_inside_pack = true;
  for (auto &it : dim_tables_iterator_) {
    if (it) {
      if (!it->NextInsidePack())
        nex_inside_pack = false;
    }
  }
  return nex_inside_pack;
}

int64_t DimensionGroupMultiMaterialized::DGIterator::GetPackSizeLeft() {
  int64_t pack_size_left = total_count_;
  for (auto &it : dim_tables_iterator_) {
    if (it) {
      int64_t dim_pack_size_left = it->GetPackSizeLeft();
      if (pack_size_left > dim_pack_size_left)
        pack_size_left = dim_pack_size_left;
    }
  }
  return pack_size_left;
}

bool DimensionGroupMultiMaterialized::DGIterator::NullsExist(int dim) {
  auto &it = dim_tables_iterator_[dim];
  return it && it->GetIndexTable()->NullsExist();
}

void DimensionGroupMultiMaterialized::DGIterator::NextPackrow() {
  int64_t pack_size_left = GetPackSizeLeft();
  for (auto &it : dim_tables_iterator_) {
    if (it) {
      it->Skip(pack_size_left);
      valid = valid && it->IsValid();
    }
  }
}

int DimensionGroupMultiMaterialized::DGIterator::GetNextPackrow(int dim, int ahead) {
  if (ahead == 0)
    return GetCurPackrow(dim);
  return 0;
}

bool DimensionGroupMultiMaterialized::DGIterator::BarrierAfterPackrow() {
  for (auto &it : dim_tables_iterator_) {
    if (it) {
      if (!it->BarrierAfterPackrow())
        return false;
    }
  }

  return true;
}

bool DimensionGroupMultiMaterialized::DGIterator::RewindToPack(int pack) {
  for (auto &it : dim_tables_iterator_) {
    if (it) {
      it->SetFixedBlockIndex(pack);
      valid = valid && it->IsValid();
    }
  }
  return valid;
}

bool DimensionGroupMultiMaterialized::DGIterator::GetSlices(std::vector<int64_t> *slices) const {
  bool supported = false;

  for (auto &it : dim_tables_iterator_) {
    if (it) {
      MultiIndexTable *multi_table = it->GetIndexTable();
      size_t table_count = multi_table->GetTableCount();
      for (size_t index = 0; index < table_count; ++index) {
        const IndexTableItem *table = multi_table->GetTable(index);
        slices->push_back(table->GetCount());
      }
      supported = true;
      break;
    }
  }

  return supported;
}
}  // namespace core
}  // namespace Tianmu
