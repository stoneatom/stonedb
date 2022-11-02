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
#ifndef TIANMU_CORE_MULTI_INDEX_H_
#define TIANMU_CORE_MULTI_INDEX_H_
#pragma once

#include "core/bin_tools.h"
#include "core/cq_term.h"
#include "core/dimension_group.h"
#include "core/filter.h"
#include "core/index_table.h"

namespace Tianmu {
namespace core {
class MIIterator;

class MultiIndex {
 public:
  MultiIndex(uint32_t power);  // = PACK_DEFAUL
  MultiIndex(const MultiIndex &s);
  MultiIndex(MultiIndex &s, bool shallow);
  ~MultiIndex();

  void Clear();  // clear the multiindex completely (like destructor + Multiindex())

  // Construction of the index

  // max_value - upper limit of indexes of newly added objects (e.g. number of
  // all objects in a table to be joined)
  void AddDimension_cross(uint64_t size);  // calculate a cross product of the previous value of
                                           // index and the full table (trivial filter) of 'size'
                                           // objects

  // retrieve information
  uint32_t ValueOfPower() { return p_power; }
  int NumOfDimensions() const { return no_dimensions; }  // number of dimensions
  int64_t NumOfTuples() const {                          // number of all tuples
    if (!no_tuples_too_big)
      return no_tuples;
    throw common::OutOfMemoryException("Too many tuples.    (85)");
    return 0;
  }
  int64_t NumOfTuples(DimensionVector &dimensions,
                      bool fail_on_overflow = true);  // for a given subset of dimensions

  bool ZeroTuples() { return (!no_tuples_too_big && no_tuples == 0); }
  bool TooManyTuples() { return no_tuples_too_big; }
  Filter *GetFilter(int dim) const  // Get the pointer to a filter attached to a dimension.
                                    // NOTE: will be nullptr in case of materialized MultiIndex!
  {
    return no_dimensions > 0 ? group_for_dim[dim]->GetFilter(dim) : nullptr;
  }
  Filter *GetUpdatableFilter(int dim) const  // Get the pointer to a filter, if it may be changed.
                                             // NOTE: will be nullptr in case of materialized
                                             // MultiIndex!
  {
    return no_dimensions > 0 ? group_for_dim[dim]->GetUpdatableFilter(dim) : nullptr;
  }
  bool NullsExist(int dim) {
    return no_dimensions > 0 ? group_for_dim[dim]->NullsPossible(dim) : false;
  }                                                // return true if there exist any 0 value (always false for virtual
                                                   // dimensions)
  bool MarkInvolvedDimGroups(DimensionVector &v);  // if any dimension is marked, then mark the
                                                   // rest of this class. Return true if anything
                                                   // new marked.
  bool IsOrderable(int dim) { return no_dimensions > 0 ? group_for_dim[dim]->IsOrderable() : true; }
  uint64_t DimSize(int dim);  // the size of one dimension: NumOfOnes for virtual,
                              // number of materialized tuples for materialized
  uint64_t OrigSize(int dim) { return dim_size[dim]; }
  // the maximal size of one dimension (e.g. the size of a table, the maximal
  // index possible)
  // Locking

  void LockForGetIndex(int dim);  // must precede GetIndex(...)
  void UnlockFromGetIndex(int dim);
  void LockAllForUse();
  void UnlockAllFromUse();

  bool IteratorLock() {  // register a normal iterator; false: already locked
                         // for updating
    if (iterator_lock > -1)
      iterator_lock++;
    return (iterator_lock > -1);
  }
  bool IteratorUpdatingLock() {  // register an updating iterator; false:
                                 // already locked
    if (iterator_lock == 0) {
      iterator_lock = -1;
      return true;
    }
    return false;
  }
  void IteratorUnlock() {
    if (iterator_lock > 0)
      iterator_lock--;
    else
      iterator_lock = 0;
  }

  // operations on the index

  void MIFilterAnd(MIIterator &mit,
                   Filter &fd);  // limit the MultiIndex by excluding all tuples
                                 // which are not present in fd, in order given
                                 // by mit

  bool CanBeDistinct(int dim) const {
    return can_be_distinct[dim];
  }  // true if ( distinct(orig. column) => distinct( result ) ), false if we
     // cannot guarantee this
  bool IsForgotten(int dim) {
    return group_for_dim[dim] ? !group_for_dim[dim]->DimEnabled(dim) : false;
  }  // true if the dimension is forgotten (not valid for getting value)
  bool IsUsedInOutput(int dim) { return used_in_output[dim]; }  // true if the dimension is used in output columns
  void SetUsedInOutput(int dim) { used_in_output[dim] = true; }
  void ResetUsedInOutput(int dim) { used_in_output[dim] = false; }
  void Empty(int dim_to_make_empty = -1);  // make an index empty (delete all
                                           // tuples) with the same dimensions
  // if parameter is set, then do not delete any virtual filter except this one
  void UpdateNumOfTuples();  // recalculate the number of tuples
  void MakeCountOnly(int64_t mat_tuples, DimensionVector &dims_to_materialize);
  // recalculate the number of tuples, assuming mat_tuples is the new
  // material_no_tuples and the dimensions from the list are deleted

  int MaxNumOfPacks(int dim);  // maximal (upper approx.) number of different
                               // nonempty data packs for the given dimension
  std::string Display();       // MultiIndex structure: f - Filter, i - IndexTable

  Transaction &ConnInfo() const { return *m_conn; }

  Transaction *m_conn;

  friend class MINewContents;
  friend class MIIterator;

  friend class MultiIndexBuilder;

 private:
  void AddDimension();                                         // declare a new dimension (internal)
  void CheckIfVirtualCanBeDistinct();                          // updates can_be_distinct table in case
                                                               // of virtual multiindex
  std::vector<int> ListInvolvedDimGroups(DimensionVector &v);  // List all internal numbers of
                                                               // groups touched by the set of
                                                               // dimensions

  int no_dimensions;
  int64_t *dim_size;   // the size of a dimension
  uint64_t no_tuples;  // actual number of tuples (also in case of virtual
                       // index); should be updated in any change of index
  uint32_t p_power;
  bool no_tuples_too_big;             // this flag is set if a virtual number of tuples
                                      // exceeds 2^64
  std::vector<bool> can_be_distinct;  // true if the dimension contain only one copy of
                                      // original rows, false if we cannot guarantee this
  std::vector<bool> used_in_output;   // true if given dimension is used for
                                      // generation of output columns

  // DimensionGroup stuff
  void FillGroupForDim();
  std::vector<DimensionGroup *> dim_groups;  // all active dimension groups
  DimensionGroup **group_for_dim;            // pointers to elements of dim_groups, for
                                             // faster dimension identification
  int *group_num_for_dim;                    // an element number of dim_groups, for faster
                                             // dimension identification

  // Some technical functions
  void MultiplyNoTuples(uint64_t factor);  // the same as "no_tuples*=factor", but set
                                           // no_tuples_too_big whenever needed

  int iterator_lock;        // 0 - unlocked, >0 - normal iterator exists, -1 -
                            // updating iterator exists
  bool shallow_dim_groups;  // Indicates whether dim_groups is a shallow copy
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_MULTI_INDEX_H_
