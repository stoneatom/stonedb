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
#ifndef STONEDB_CORE_ROUGH_MULTI_INDEX_H_
#define STONEDB_CORE_ROUGH_MULTI_INDEX_H_
#pragma once

#include "core/bin_tools.h"
#include "core/cq_term.h"
#include "core/filter.h"

namespace stonedb {
namespace core {
class RoughMultiIndex {
 public:
  RoughMultiIndex(std::vector<int> no_of_packs);  // initialize by dimensions definition
                                                  // (i.e. pack numbers)
  RoughMultiIndex(const RoughMultiIndex &);
  ~RoughMultiIndex();

  common::RSValue GetPackStatus(int dim, int pack) { return rf[dim][pack]; };
  void SetPackStatus(int dim, int pack, common::RSValue v) { rf[dim][pack] = v; };

  int NoDimensions() { return no_dims; };
  int NoPacks(int dim) { return no_packs[dim]; };
  int NoConditions(int dim) { return int(local_desc[dim].size()); };
  int GlobalDescNum(int dim, int local_desc_num) { return (local_desc[dim])[local_desc_num]->desc_num; }
  /*
          Example of query processing steps:
          1. Add new rough filter (RF) by GetLocalDescFilter for a condition.
     Note that it will have common::RSValue::RS_NONE if global RF is common::CT::NONE.
          2. Update local RF for all non-common::RSValue::RS_NONE packs.
          3. Update global RF by UpdateGlobalRoughFilter, common::CT::NONEto
     optimize access for next conditions.
  */

  common::RSValue *GetRSValueTable(int dim) { return rf[dim]; };
  common::RSValue *GetLocalDescFilter(int dim, int desc_num, bool read_only = false);
  // if not exists, create one (unless read_only is set)
  void ClearLocalDescFilters();  // clear all desc info, for reusing the rough
                                 // filter e.g. in subqueries

  bool UpdateGlobalRoughFilter(int dim,
                               int desc_num);  // make projection from local to
                                               // global filter for dimension
  // return false if global filter is empty
  void UpdateGlobalRoughFilter(int dim,
                               Filter *loc_f);  // if the filter is nontrivial, then copy pack status
  void UpdateLocalRoughFilters(int dim);        // make projection from global filters to all local for the
                                                // given dimension
  void MakeDimensionSuspect(int dim = -1);      // common::RSValue::RS_ALL -> common::RSValue::RS_SOME
                                                // for a dimension (or all of them)
  void MakeDimensionEmpty(int dim = -1);

  std::vector<int> GetReducedDimensions();  // find dimensions having more omitted packs than
                                            // recorded in no_empty_packs
  void UpdateReducedDimension(int d);       // update no_empty_packs
  void UpdateReducedDimension();            // update no_empty_packs for all dimensions

 private:
  int no_dims;
  int *no_packs;  // number of packs in each dimension

  common::RSValue **rf;  // rough filters for packs (global)

  int *no_empty_packs;  // a number of (globally) empty packs for each dimension,
  // used to check whether projections should be made (and updated therein)

  class RFDesc {  // rough description of one condition (descriptor)
   public:
    RFDesc(int packs, int d);
    RFDesc(const RFDesc &);
    ~RFDesc();

    int desc_num;              // descriptor number
    int no_packs;              // table size (for copying)
    common::RSValue *desc_rf;  // rough filter for desc; note that all values
                               // are interpretable here
  };

  std::vector<RFDesc *> *local_desc;  // a table of vectors of conditions for each dimension
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_ROUGH_MULTI_INDEX_H_
