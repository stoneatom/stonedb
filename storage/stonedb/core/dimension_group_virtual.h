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
#ifndef STONEDB_CORE_DIMENSION_GROUP_VIRTUAL_H_
#define STONEDB_CORE_DIMENSION_GROUP_VIRTUAL_H_
#pragma once

#include "core/dimension_group.h"

namespace stonedb {
namespace core {

class DimensionGroupVirtual : public DimensionGroup {
 public:
  DimensionGroupVirtual(DimensionVector &dims, int base_dim, Filter *f_source,
                        int copy_mode = 0);  // copy_mode: 0 - copy filter, 1 - ShallowCopy
                                             // filter, 2 - grab pointer
  virtual ~DimensionGroupVirtual();
  DimensionGroup *Clone(bool shallow) override;

  void FillCurrentPos(DimensionGroup::Iterator *it, int64_t *cur_pos, int *cur_pack, DimensionVector &dims) override;
  void UpdateNoTuples() override;
  Filter *GetFilter(int dim) const override { return (base_dim == dim || dim == -1 ? f : NULL); }
  // For this type of filter: dim == -1 means the only existing one
  // Note: GetUpdatableFilter remains default (NULL)
  bool DimUsed(int dim) override { return (base_dim == dim || dims_used[dim]); }
  bool DimEnabled(int dim) override { return (base_dim == dim || t[dim] != NULL); }
  bool NullsPossible(int dim) override { return nulls_possible[dim]; }
  void Empty() override;
  void NewDimensionContent(int dim, IndexTable *tnew,
                           bool nulls);  // tnew will be added (as a
                                         // pointer to be deleted by
                                         // destructor) on a dimension
                                         // dim

  void Lock(int dim, int n = 1) override {
    if (t[dim]) {
      for (int i = 0; i < n; i++) t[dim]->Lock();
    } else if (base_dim == dim)
      locks += n;
  }
  void Unlock(int dim) override {
    if (t[dim])
      t[dim]->Unlock();
    else if (base_dim == dim)
      locks--;
  }
  int NoLocks(int dim) override {
    if (t[dim]) return t[dim]->NoLocks();
    if (base_dim == dim) return locks;
    return 0;
  }
  bool IsThreadSafe() override { return true; }
  bool IsOrderable() override;

  class DGVirtualIterator : public DimensionGroup::Iterator {
   public:
    DGVirtualIterator(Filter *f_to_iterate, int b_dim, DimensionVector &dims, IndexTable **tt, uint32_t power);
    DGVirtualIterator(const Iterator &sec, uint32_t power);
    ~DGVirtualIterator();

    void operator++() override;
    void Rewind() override {
      fi.Rewind();
      valid = fi.IsValid();
      dim_pos = 0;
      cur_pack_start = 0;
    }
    int64_t GetPackSizeLeft() override { return fi.GetPackSizeLeft(); }
    bool NextInsidePack() override;
    bool WholePack(int dim) override { return (dim == base_dim ? f->IsFull(fi.GetCurrPack()) : false); }
    bool InsideOnePack() override { return fi.InsideOnePack(); }
    bool NullsExist(int dim) override { return nulls_found[dim]; }
    void NextPackrow() override;
    int GetCurPackrow(int dim) override { return (dim == base_dim ? fi.GetCurrPack() : cur_pack[dim]); }
    int64_t GetCurPos(int dim) override;
    int GetNextPackrow(int dim, int ahead) override { return (dim == base_dim ? fi.Lookahead(ahead) : cur_pack[dim]); }
    bool BarrierAfterPackrow() override;

    virtual bool Ordered() { return false; }  // check if it is an ordered iterator
   private:
    FilterOnesIterator fi;
    int64_t dim_pos;
    int64_t no_obj;
    int64_t cur_pack_start;  // for dim
    int no_dims;
    int base_dim;
    bool *nulls_found;  // in these dimensions nulls were found in this pack
    int *cur_pack;      // the number of the current pack (must be stored separately
                        // because there may be nulls on the beginning of pack
    // external pointers:
    Filter *f;
    IndexTable **t;
  };
  class DGVirtualOrderedIterator : public DimensionGroup::Iterator {
   public:
    DGVirtualOrderedIterator(Filter *f_to_iterate, int b_dim, DimensionVector &dims, int64_t *ppos, IndexTable **tt,
                             PackOrderer *po, uint32_t power);
    DGVirtualOrderedIterator(const Iterator &sec, uint32_t power);
    ~DGVirtualOrderedIterator();

    void operator++() override;
    void Rewind() override;
    int64_t GetPackSizeLeft() override { return fi.GetPackSizeLeft(); }
    bool NextInsidePack() override;
    bool WholePack(int dim) override { return (dim == base_dim ? f->IsFull(fi.GetCurrPack()) : false); }
    bool InsideOnePack() override { return fi.InsideOnePack(); }
    bool NullsExist(int dim) override { return nulls_found[dim]; }
    void NextPackrow() override;
    int GetCurPackrow(int dim) override { return (dim == base_dim ? fi.GetCurrPack() : cur_pack[dim]); }
    int64_t GetCurPos(int dim) override;
    int GetNextPackrow(int dim, int ahead) override { return (dim == base_dim ? fi.Lookahead(ahead) : cur_pack[dim]); }
    bool BarrierAfterPackrow() override { return false; }
    virtual bool Ordered() { return true; }  // check if it is an ordered iterator
   private:
    FilterOnesIteratorOrdered fi;
    int64_t dim_pos;
    int64_t cur_pack_start;  // for dim
    int no_dims;
    int base_dim;
    bool *nulls_found;  // in these dimensions nulls were found in this pack
    int *cur_pack;      // the number of the current pack (must be stored separately
                        // because there may be nulls on the beginning of pack
    // external pointers:
    int64_t *pack_pos;
    Filter *f;
    IndexTable **t;
  };

  DimensionGroup::Iterator *NewIterator(DimensionVector &,
                                        uint32_t power) override;  // create a new iterator (to be deleted by user)
  DimensionGroup::Iterator *NewOrderedIterator(DimensionVector &, PackOrderer *po,
                                               uint32_t power) override;  // create a new ordered iterator, if possible
  DimensionGroup::Iterator *CopyIterator(DimensionGroup::Iterator *s, uint32_t power) override;

 private:
  int base_dim;  // dim for filter
  Filter *f;
  DimensionVector dims_used;
  int no_dims;        // number of all possible dimensions (or just the last used one)
  IndexTable **t;     // NULL for not used (natural numbering) and for base_dim
  int64_t *pack_pos;  // table of size = number of packs in base_dim; the first
                      // position of a given pack in all IndexTables
  bool *nulls_possible;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_DIMENSION_GROUP_VIRTUAL_H_
