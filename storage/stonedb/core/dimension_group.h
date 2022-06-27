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
#ifndef STONEDB_CORE_DIMENSION_GROUP_H_
#define STONEDB_CORE_DIMENSION_GROUP_H_
#pragma once

#include "core/bin_tools.h"
#include "core/dimension_vector.h"
#include "core/filter.h"
#include "core/index_table.h"

namespace stonedb {
namespace core {
class PackOrderer;

class DimensionGroup {
 public:
  enum class DGType { DG_FILTER, DG_INDEX_TABLE, DG_VIRTUAL, DG_NOT_KNOWN } dim_group_type;

  DimensionGroup() : dim_group_type(DGType::DG_NOT_KNOWN), no_obj(0), locks(0) {}
  virtual DimensionGroup *Clone(bool shallow) = 0;  // create a new group with the same (copied) contents
  virtual ~DimensionGroup() {}
  int64_t NoTuples() { return no_obj; }
  virtual void UpdateNoTuples() {}          // may be not needed for some group types
  DGType Type() { return dim_group_type; }  // type selector
  virtual Filter *GetFilter([[maybe_unused]] int dim) const {
    return NULL;
  }  // Get the pointer to a filter attached to a dimension. NOTE: will be NULL
     // if not applicable
  virtual Filter *GetUpdatableFilter([[maybe_unused]] int dim) const {
    return NULL;
  }                                       // Get the pointer to a filter, if it may be changed. NOTE: will be NULL if
                                          // not applicable
  virtual bool DimUsed(int d) = 0;        // true if the dimension is involved in this group
  virtual bool DimEnabled(int d) = 0;     // true if the dimension is used and has
                                          // values (is not forgotten)
  virtual bool NullsPossible(int d) = 0;  // true if there are any outer nulls
                                          // possible in the dimension
  virtual void Empty() = 0;               // make the group empty (0 tuples)

  virtual void Lock([[maybe_unused]] int dim, int n = 1) {
    locks += n;
  }  // lock for getting value (if needed), or just remember the number of locks
  virtual void Unlock([[maybe_unused]] int dim) { locks--; }
  virtual int NoLocks([[maybe_unused]] int dim) { return locks; }
  virtual bool IsThreadSafe() = 0;  // true if the dimension group may be used
                                    // in parallel for reading
  virtual bool IsOrderable() = 0;   // true if the dimension group may provide an
                                    // ordered (nontrivial) iterator

  // Temporary code, for rare cases when we add a dimension after other joins
  // (smk_33):
  virtual void AddDimension() {}

  class Iterator {
   public:
    // note: retrieving a value depends on DimensionGroup type
    Iterator() { valid = false; }
    Iterator(const Iterator &sec) { valid = sec.valid; }
    virtual ~Iterator(){};

    virtual void operator++() = 0;
    virtual void Rewind() = 0;
    virtual bool NextInsidePack() = 0;
    bool IsValid() const { return valid; }
    virtual int64_t GetPackSizeLeft() = 0;
    virtual bool WholePack(int dim) = 0;   // True, if the current packrow contain exactly the
                                           // whole pack for a dimension (no repetitions, no null
                                           // objects)
    virtual bool InsideOnePack() = 0;      // true if there is only one packrow possible for
                                           // dimensions in this iterator
    virtual bool NullsExist(int dim) = 0;  // return true if there exist any 0 value (always
                                           // false for virtual dimensions)
    virtual void NextPackrow() = 0;
    virtual int64_t GetCurPos(int dim) = 0;
    virtual int GetCurPackrow(int dim) = 0;
    virtual int GetNextPackrow(int dim,
                               int ahead) = 0;  // ahead = 1 => the next packrow
                                                // after this one, etc., return
                                                // -1 if not known
    virtual bool BarrierAfterPackrow() = 0;     // true, if we must synchronize
                                                // threads before NextPackrow()

    // Updating, if possible:
    virtual bool InternallyUpdatable() {
      return false;
    }                                      // true if the subclass implements in-group updating functions, like
                                           // below:
    virtual void ResetCurrent() {}         // reset the current position
    virtual void ResetCurrentPackrow() {}  // reset the whole current packrow
    virtual void CommitUpdate() {}         // commit the previous resets
    virtual void SetNoPacksToGo([[maybe_unused]] int n) {}
    virtual void RewindToRow([[maybe_unused]] int64_t n) {}
    virtual bool RewindToPack([[maybe_unused]] int pack) { return false; }  // true if the pack is nonempty
    virtual int NoOnesUncommited([[maybe_unused]] uint pack) { return -1; }
    // Extend the capability of splitting.
    virtual bool GetSlices([[maybe_unused]] std::vector<int64_t> *slices) const { return false; }

   protected:
    bool valid;
  };

  // create a new iterator (to be deleted by user)
  virtual DimensionGroup::Iterator *NewIterator(DimensionVector &, uint32_t power) = 0;
  virtual DimensionGroup::Iterator *NewOrderedIterator(DimensionVector &, [[maybe_unused]] PackOrderer *po,
                                                       [[maybe_unused]] uint32_t power) {
    return NULL;
  }
  // create a new ordered iterator, if possible
  virtual DimensionGroup::Iterator *CopyIterator(DimensionGroup::Iterator *, uint32_t power) = 0;

  // fill only relevant dimensions basing on a local iterator
  virtual void FillCurrentPos(DimensionGroup::Iterator *it, int64_t *cur_pos, int *cur_pack, DimensionVector &dims) = 0;

 protected:
  int64_t no_obj;
  int locks;
};

class DimensionGroupFilter : public DimensionGroup {
 public:
  DimensionGroupFilter(int dim, int64_t size, uint32_t power);
  DimensionGroupFilter(int dim, Filter *f_source, int copy_mode = 0,
                       uint32_t power = 0);  // copy_mode: 0 - copy filter, 1 - ShallowCopy
                                             // filter, 2 - grab pointer
  virtual ~DimensionGroupFilter();
  DimensionGroup *Clone(bool shallow) override {
    return new DimensionGroupFilter(base_dim, f, (shallow ? 1 : 0), f->GetPower());
  }

  void FillCurrentPos(DimensionGroup::Iterator *it, int64_t *cur_pos, int *cur_pack, DimensionVector &dims) override {
    if (dims[base_dim]) {
      cur_pos[base_dim] = it->GetCurPos(base_dim);
      cur_pack[base_dim] = it->GetCurPackrow(base_dim);
    }
  }
  void UpdateNoTuples() override { no_obj = f->NoOnes(); }
  Filter *GetFilter([[maybe_unused]] int dim) const override {
    DEBUG_ASSERT(dim == base_dim || dim == -1);
    return f;
  }
  // For this type of filter: dim == -1 means the only existing one
  Filter *GetUpdatableFilter([[maybe_unused]] int dim) const override {
    DEBUG_ASSERT(dim == base_dim || dim == -1);
    return f;
  }
  bool DimUsed(int dim) override { return base_dim == dim; }
  bool DimEnabled(int dim) override { return base_dim == dim; }
  bool NullsPossible([[maybe_unused]] int d) override { return false; }
  void Empty() override {
    no_obj = 0;
    f->Reset();
  }
  bool IsThreadSafe() override { return true; }
  bool IsOrderable() override { return true; }

  class DGFilterIterator : public DimensionGroup::Iterator {
   public:
    DGFilterIterator(Filter *f_to_iterate, uint32_t power) : fi(f_to_iterate, power), f(f_to_iterate) {
      valid = !f->IsEmpty();
    }
    DGFilterIterator(const Iterator &sec, uint32_t power);

    void operator++() override {
      DEBUG_ASSERT(valid);
      ++fi;
      valid = fi.IsValid();
    }
    void Rewind() override {
      fi.Rewind();
      valid = fi.IsValid();
    }
    bool NextInsidePack() override {
      bool r = fi.NextInsidePack();
      valid = fi.IsValid();
      return r;
    }
    int64_t GetPackSizeLeft() override { return fi.GetPackSizeLeft(); }
    bool WholePack([[maybe_unused]] int dim) override { return f->IsFull(fi.GetCurrPack()); }
    bool InsideOnePack() override { return fi.InsideOnePack(); }
    bool NullsExist([[maybe_unused]] int dim) override { return false; }
    void NextPackrow() override {
      DEBUG_ASSERT(valid);
      fi.NextPack();
      valid = fi.IsValid();
    }
    int GetCurPackrow([[maybe_unused]] int dim) override { return fi.GetCurrPack(); }
    int64_t GetCurPos([[maybe_unused]] int dim) override { return (*fi); }
    int GetNextPackrow([[maybe_unused]] int dim, int ahead) override { return fi.Lookahead(ahead); }
    bool BarrierAfterPackrow() override { return false; }
    // Updating
    bool InternallyUpdatable() override { return true; }
    void ResetCurrent() override { fi.ResetDelayed(); }
    void ResetCurrentPackrow() override { fi.ResetCurrentPackrow(); }
    void CommitUpdate() override { f->Commit(); }
    void SetNoPacksToGo(int n) override { fi.SetNoPacksToGo(n); }
    void RewindToRow(int64_t n) override {
      fi.RewindToRow(n);
      valid = fi.IsValid();
    }
    bool RewindToPack(int pack) override {
      bool r = fi.RewindToPack(pack);
      valid = fi.IsValid();
      return r;
    }

    bool GetSlices(std::vector<int64_t> *slices) const override {
      slices->resize(fi.GetPackCount(), 1 << fi.GetPackPower());
      return true;
    }

    int NoOnesUncommited(uint pack) override { return f->NoOnesUncommited(pack); }
    virtual bool Ordered() { return false; }  // check if it is an ordered iterator
   private:
    FilterOnesIterator fi;
    // external pointer:
    Filter *f;
  };

  class DGFilterOrderedIterator : public DimensionGroup::Iterator {
   public:
    DGFilterOrderedIterator(Filter *f_to_iterate, PackOrderer *po, uint32_t power)
        : fi(f_to_iterate, po, power), f(f_to_iterate) {
      valid = !f->IsEmpty();
    }
    DGFilterOrderedIterator(const Iterator &sec, uint32_t power);

    void operator++() override {
      DEBUG_ASSERT(valid);
      ++fi;
      valid = fi.IsValid();
    }
    void Rewind() override {
      fi.Rewind();
      valid = fi.IsValid();
    }
    bool NextInsidePack() override {
      bool r = fi.NextInsidePack();
      valid = fi.IsValid();
      return r;
    }
    int64_t GetPackSizeLeft() override { return fi.GetPackSizeLeft(); }
    bool WholePack([[maybe_unused]] int dim) override { return f->IsFull(fi.GetCurrPack()); }
    bool InsideOnePack() override { return fi.InsideOnePack(); }
    bool NullsExist([[maybe_unused]] int dim) override { return false; }
    void NextPackrow() override {
      DEBUG_ASSERT(valid);
      fi.NextPack();
      valid = fi.IsValid();
    }
    int GetCurPackrow([[maybe_unused]] int dim) override { return fi.GetCurrPack(); }
    int64_t GetCurPos([[maybe_unused]] int dim) override { return (*fi); }
    int GetNextPackrow([[maybe_unused]] int dim, int ahead) override { return fi.Lookahead(ahead); }
    bool BarrierAfterPackrow() override { return (fi.NaturallyOrdered() == false); }
    // Updating
    bool InternallyUpdatable() override { return true; }
    void ResetCurrent() override { fi.ResetDelayed(); }
    void ResetCurrentPackrow() override { fi.ResetCurrentPackrow(); }
    void CommitUpdate() override { f->Commit(); }
    void SetNoPacksToGo(int n) override { fi.SetNoPacksToGo(n); }
    void RewindToRow(int64_t n) override {
      fi.RewindToRow(n);
      valid = fi.IsValid();
    }
    bool RewindToPack(int pack) override {
      bool r = fi.RewindToPack(pack);
      valid = fi.IsValid();
      return r;
    }

    bool GetSlices(std::vector<int64_t> *slices) const override {
      slices->resize(fi.GetPackCount(), 1 << fi.GetPackPower());
      return true;
    }

    int NoOnesUncommited(uint pack) override { return f->NoOnesUncommited(pack); }
    virtual bool Ordered() { return true; }  // check if it is an ordered iterator
   private:
    FilterOnesIteratorOrdered fi;
    // external pointer:
    Filter *f;
  };

  DimensionGroup::Iterator *NewIterator(DimensionVector &,
                                        uint32_t power) override;  // create a new iterator (to be deleted by user)
  DimensionGroup::Iterator *NewOrderedIterator(DimensionVector &, PackOrderer *po,
                                               uint32_t power) override;  // create a new ordered iterator, if possible
  DimensionGroup::Iterator *CopyIterator(DimensionGroup::Iterator *s, uint32_t power) override;

 private:
  int base_dim;
  Filter *f;
};

class DimensionGroupMaterialized : public DimensionGroup {
 public:
  // NOTE: works also for "count only" (all t[i] are NULL, only no_obj set)
  DimensionGroupMaterialized(DimensionVector &dims);
  virtual ~DimensionGroupMaterialized();
  DimensionGroup *Clone(bool shallow) override;

  void NewDimensionContent(int dim, IndexTable *tnew,
                           bool nulls);  // tnew will be added (as a
                                         // pointer to be deleted by
                                         // destructor) on a dimension
                                         // dim
  void SetNoObj(int64_t _no_obj) { no_obj = _no_obj; }
  bool DimUsed(int dim) override { return dims_used[dim]; }
  bool DimEnabled(int dim) override { return (t[dim] != NULL); }
  bool NullsPossible(int dim) override { return nulls_possible[dim]; }
  void Empty() override;

  void FillCurrentPos(DimensionGroup::Iterator *it, int64_t *cur_pos, int *cur_pack, DimensionVector &dims) override;

  void Lock(int dim, int n = 1) override {
    if (t[dim])
      for (int i = 0; i < n; i++) t[dim]->Lock();
  }
  void Unlock(int dim) override {
    if (t[dim]) t[dim]->Unlock();
  }
  int NoLocks(int dim) override { return (t[dim] ? t[dim]->NoLocks() : 0); }
  bool IsThreadSafe() override { return true; }  // BarrierAfterPackrow() must be used for parallel execution
  bool IsOrderable() override { return false; }

  class DGMaterializedIterator : public DimensionGroup::Iterator {
   public:
    // NOTE: works also for "count only" (all t[i] are NULL)
    DGMaterializedIterator(int64_t _no_obj, DimensionVector &dims, IndexTable **_t, bool *nulls, uint32_t power);
    DGMaterializedIterator(const Iterator &sec, uint32_t power);
    ~DGMaterializedIterator();

    int64_t CurPos() { return cur_pos; }
    void operator++() override {
      cur_pos++;
      pack_size_left--;
      if (pack_size_left == 0) InitPackrow();
    }
    void Rewind() override;
    bool NextInsidePack() override;
    int64_t GetPackSizeLeft() override { return pack_size_left; }
    bool WholePack([[maybe_unused]] int dim) override { return false; }
    bool InsideOnePack() override { return inside_one_pack; }
    bool NullsExist(int dim) override { return nulls_found[dim]; }
    void NextPackrow() override {
      cur_pos += pack_size_left;
      InitPackrow();
    }
    int64_t GetCurPos(int dim) override {
      int64_t v = t[dim]->Get64(cur_pos);
      return (v == 0 ? common::NULL_VALUE_64 : v - 1);
    }
    int GetCurPackrow(int dim) override { return cur_pack[dim]; }
    int GetNextPackrow(int dim, int ahead) override;
    bool BarrierAfterPackrow() override;
    void JumpToNextPack(uint64_t &loc_iterator, IndexTable *cur_t, uint64_t loc_limit);

   private:
    void InitPackrow();
    void FindPackEnd(int dim);

    int64_t pack_size_left;
    int64_t cur_pack_start;
    int64_t cur_pos;
    int64_t no_obj;
    uint32_t p_power;  // 2^p_power per pack
    int no_dims;
    bool inside_one_pack;  // all dimensions contain values from one packrow
                           // only (determined on InitPackrow())

    bool *one_packrow;   // dimensions containing values from one packrow only
                         // (no nulls). Approximation: false => still may be one
                         // packrow
    bool *nulls_found;   // in these dimensions nulls were found in this pack
    int *cur_pack;       // the number of the current pack (must be stored separately
                         // because there may be nulls on the beginning of pack
    int64_t *next_pack;  // beginning of the next pack (or no_obj if there is no
                         // other pack)
    int64_t *ahead1, *ahead2,
        *ahead3;  // beginning of the three next packs after next_pack, or
                  // -1 if not determined properly

    // External pointers:
    IndexTable **t;  // table is local, pointers are from DimensionGroupMaterialized,
                     // NULL for not used (not iterated)
    bool *nulls_possible;
  };
  // create a new iterator (to be deleted by user)
  DimensionGroup::Iterator *NewIterator(DimensionVector &, uint32_t power) override;
  DimensionGroup::Iterator *CopyIterator(DimensionGroup::Iterator *s, uint32_t power) override {
    return new DGMaterializedIterator(*s, power);
  }

 private:
  DimensionVector dims_used;
  int no_dims;     // number of all possible dimensions (or just the last used one)
  IndexTable **t;  // NULL for not used (natural numbering)
  bool *nulls_possible;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_DIMENSION_GROUP_H_