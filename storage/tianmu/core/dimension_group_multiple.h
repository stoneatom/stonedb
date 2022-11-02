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
#ifndef TIANMU_CORE_DIMENSION_GROUP_MULTIPLE_H_
#define TIANMU_CORE_DIMENSION_GROUP_MULTIPLE_H_
#pragma once

#include <list>

#include "core/bin_tools.h"
#include "core/dimension_group.h"
#include "core/filter.h"
#include "core/index_table.h"

namespace Tianmu {
namespace core {
class MultiIndexTable;

class IndexTableItem {
 public:
  IndexTableItem(MultiIndexTable *parent_index, IndexTable *table, int64_t count, uint32_t power, bool nulls_possible)
      : parent_index_(parent_index), table_(table), count_(count), power_(power), nulls_possible_(nulls_possible) {}
  IndexTableItem(const IndexTableItem &table_item)
      : parent_index_(table_item.parent_index_),
        table_(new IndexTable(*table_item.table_.get())),
        count_(table_item.count_),
        power_(table_item.power_),
        nulls_possible_(table_item.nulls_possible_) {}
  IndexTableItem(IndexTableItem &&table_item) = default;
  ~IndexTableItem() = default;

  IndexTable *GetTable() { return table_.get(); }
  int64_t GetCount() const { return count_; }
  bool FindPackEnd(int64_t pos, int64_t *cur_pack, int64_t *next_pack_pos);

 private:
  MultiIndexTable *parent_index_ = nullptr;
  std::unique_ptr<IndexTable> table_;
  int64_t count_ = 0;
  uint32_t power_ = 16;
  bool nulls_possible_ = false;
};

class MultiIndexTable {
 public:
  class Iterator {
   public:
    explicit Iterator(MultiIndexTable *index_table) : index_table_(index_table) {}
    Iterator(Iterator &iter)
        : index_table_(iter.index_table_),
          table_index_(iter.table_index_),
          valid_(iter.valid_),
          cur_pos_(iter.cur_pos_),
          cur_pack_(iter.cur_pack_),
          pack_size_left_(iter.pack_size_left_),
          fixed_(iter.fixed_) {}
    ~Iterator() = default;

    MultiIndexTable *GetIndexTable() { return index_table_; }
    bool IsValid() const { return valid_; }
    void Increment();
    void Rewind();
    int64_t GetCurPos();
    int GetCurPack() const { return cur_pack_; }
    void Skip(int64_t offset);
    int64_t GetPackSizeLeft() const { return pack_size_left_; }
    bool NextInsidePack();

    bool BarrierAfterPackrow();

    void SetFixedBlockIndex(size_t index);

   private:
    void InitPackrow();

    // External pointer.
    MultiIndexTable *index_table_ = nullptr;

    size_t table_index_ = 0;
    bool valid_ = false;
    int64_t cur_pos_ = 0;
    int64_t cur_pack_start_ = 0;
    int64_t next_pack_start_ = 0;
    int64_t cur_pack_ = 0;
    int64_t pack_size_left_ = 0;
    bool fixed_ = false;  // Specific one index for traversed.
  };

  explicit MultiIndexTable(uint32_t power);
  MultiIndexTable(MultiIndexTable &tables);
  ~MultiIndexTable() = default;

  bool NullsExist() const { return nulls_exist_; }
  void SetNullsExist() { nulls_exist_ = true; }
  bool NullsPossible() const { return nulls_possible_; }
  void AddTable(IndexTable *table, uint64_t count, bool nulls_possible);

  void Lock(int n = 1);
  void Unlock();
  int NumOfLocks();

  int64_t GetCount() const { return total_count_; }
  size_t GetTableCount() const { return table_items_.size(); }
  const IndexTableItem *GetTable(size_t index) const { return &table_items_[index]; }

 private:
  friend class Iterator;

  int64_t total_count_ = 0;
  uint32_t power_ = 16;
  bool nulls_exist_ = false;
  bool nulls_possible_ = false;
  std::vector<IndexTableItem> table_items_;
};

class DimensionGroupMultiMaterialized : public DimensionGroup {
  class DGIterator : public DimensionGroup::Iterator {
   public:
    // NOTE: works also for "count only" (all t[i] are nullptr)。
    DGIterator(int64_t total_count, DimensionVector &dims, std::vector<MultiIndexTable *> &dim_tables, uint32_t power);
    DGIterator(const Iterator &sec, uint32_t power);
    ~DGIterator() = default;

    // Overridden from DimensionGroup::Iterator:
    void operator++() override;
    void Rewind() override;
    bool NextInsidePack() override;
    int64_t GetPackSizeLeft() override;
    bool WholePack([[maybe_unused]] int dim) override { return false; }
    bool InsideOnePack() override { return false; }
    bool NullsExist(int dim) override;
    void NextPackrow() override;
    int64_t GetCurPos(int dim) override {
      int64_t val = dim_tables_iterator_[dim] ? dim_tables_iterator_[dim]->GetCurPos() : 0;
      return (val == 0 ? common::NULL_VALUE_64 : val - 1);
    }
    int GetCurPackrow(int dim) override {
      return dim_tables_iterator_[dim] ? dim_tables_iterator_[dim]->GetCurPack() : 0;
    }
    int GetNextPackrow(int dim, int ahead) override;
    bool BarrierAfterPackrow() override;
    bool RewindToPack(int pack) override;
    bool GetSlices(std::vector<int64_t> *slices) const override;

   private:
    int64_t total_count_ = 0;
    uint32_t power_ = 16;  // 2^power_ per pack
    std::vector<std::unique_ptr<MultiIndexTable::Iterator>> dim_tables_iterator_;
  };

 public:
  // NOTE: works also for "count only" (all t[i] are nullptr, only no_obj set)
  DimensionGroupMultiMaterialized(int64_t obj, DimensionVector &dims, uint32_t power, bool is_shallow_memory = false);
  ~DimensionGroupMultiMaterialized() override;

  // The table will be added (as a pointer to be deleted by destructor) on a
  // dimension dim.
  void AddDimensionContent(int dim, IndexTable *table, int count, bool nulls);

  // Overridden from DimensionGroup:
  DimensionGroup *Clone(bool shallow) override;
  bool DimUsed(int dim) override { return dims_used_[dim]; }
  bool DimEnabled(int dim) override { return (dim_tables_[dim] != nullptr); }
  bool NullsPossible(int dim) override {
    MultiIndexTable *dim_table = dim_tables_[dim];
    return dim_table && dim_table->NullsPossible();
  }

  void Empty() override;
  void FillCurrentPos(DimensionGroup::Iterator *iter, int64_t *cur_pos, int *cur_pack, DimensionVector &dims) override;
  void Lock(int dim, int n = 1) override;
  void Unlock(int dim) override;
  int NumOfLocks(int dim) override;
  bool IsThreadSafe() override { return true; }
  bool IsOrderable() override { return false; }
  DimensionGroup::Iterator *NewIterator(DimensionVector &, uint32_t power) override;
  DimensionGroup::Iterator *CopyIterator(DimensionGroup::Iterator *s, uint32_t power) override;

 private:
  uint32_t power_ = 16;
  DimensionVector dims_used_;
  // Number of all possible dimensions (or just the last used one).
  int dims_count_ = 0;
  // nullptr for not used (natural numbering).
  std::vector<MultiIndexTable *> dim_tables_;
  bool is_shallow_memory;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_DIMENSION_GROUP_MULTIPLE_H_
