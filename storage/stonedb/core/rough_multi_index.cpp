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

#include "rough_multi_index.h"
#include "core/rough_value.h"

namespace stonedb {
namespace core {
RoughMultiIndex::RoughMultiIndex(std::vector<int> no_of_packs) {
  no_dims = int(no_of_packs.size());
  no_packs = new int[no_dims];
  no_empty_packs = new int[no_dims];
  rf = new common::RSValue *[no_dims];
  local_desc = new std::vector<RFDesc *>[no_dims];
  for (int d = 0; d < no_dims; d++) {
    no_packs[d] = no_of_packs[d];
    no_empty_packs[d] = 0;
    if (no_packs[d] > 0)
      rf[d] = new common::RSValue[no_packs[d]];
    else
      rf[d] = NULL;
    for (int p = 0; p < no_packs[d]; p++) rf[d][p] = common::RSValue::RS_UNKNOWN;
  }
}

RoughMultiIndex::RoughMultiIndex(const RoughMultiIndex &rmind) {
  no_dims = rmind.no_dims;
  no_packs = new int[no_dims];
  no_empty_packs = new int[no_dims];
  rf = new common::RSValue *[no_dims];
  local_desc = new std::vector<RFDesc *>[no_dims];
  for (int d = 0; d < no_dims; d++) {
    no_packs[d] = rmind.no_packs[d];
    no_empty_packs[d] = rmind.no_empty_packs[d];
    for (uint i = 0; i < rmind.local_desc[d].size(); i++) local_desc[d].push_back(new RFDesc(*rmind.local_desc[d][i]));
    // local_desc[d] = rmind.local_desc[d];			// copying of the
    // vectors
    // - copying constructor of RFDesc in use?
    if (no_packs[d] > 0)
      rf[d] = new common::RSValue[no_packs[d]];
    else
      rf[d] = NULL;
    for (int p = 0; p < no_packs[d]; p++) rf[d][p] = rmind.rf[d][p];
  }
}

RoughMultiIndex::~RoughMultiIndex() {
  for (int d = 0; d < no_dims; d++) {
    if (rf[d]) delete[] rf[d];
    for (uint i = 0; i < local_desc[d].size(); i++) delete local_desc[d][i];
  }
  delete[] local_desc;
  delete[] rf;
  delete[] no_packs;
  delete[] no_empty_packs;
}

common::RSValue *RoughMultiIndex::GetLocalDescFilter(int dim, int desc_num, bool read_only) {
  if (desc_num < 0) return NULL;
  uint j;
  for (j = 0; j < local_desc[dim].size(); j++) {
    if ((local_desc[dim])[j]->desc_num == desc_num) return (local_desc[dim])[j]->desc_rf;
  }
  if (read_only) return NULL;  // no local filter available

  // still here? Now j is the first unused descriptor slot, and we should
  // prepare a new table.
  local_desc[dim].push_back(new RFDesc(no_packs[dim], desc_num));
  for (int p = 0; p < no_packs[dim]; p++) {
    if (rf[dim][p] == common::RSValue::RS_NONE)  // check global dimension filter
      (local_desc[dim])[j]->desc_rf[p] = common::RSValue::RS_NONE;
    else
      (local_desc[dim])[j]->desc_rf[p] = common::RSValue::RS_UNKNOWN;
  }
  return (local_desc[dim])[j]->desc_rf;
}

void RoughMultiIndex::ClearLocalDescFilters() {
  for (int d = 0; d < no_dims; d++) {
    for (uint i = 0; i < local_desc[d].size(); ++i) delete local_desc[d][i];
    local_desc[d].clear();
  }
}

void RoughMultiIndex::MakeDimensionSuspect(int dim)  // common::RSValue::RS_ALL -> common::RSValue::RS_SOME for a
                                                     // dimension (or all of them)
{
  for (int d = 0; d < no_dims; d++)
    if ((dim == d || dim == -1) && rf[d]) {
      for (int p = 0; p < no_packs[d]; p++)
        if (rf[d][p] == common::RSValue::RS_ALL) rf[d][p] = common::RSValue::RS_SOME;
    }
}

void RoughMultiIndex::MakeDimensionEmpty(int dim /*= -1*/) {
  for (int d = 0; d < no_dims; d++)
    if ((dim == d || dim == -1) && rf[d]) {
      for (int p = 0; p < no_packs[d]; p++) rf[d][p] = common::RSValue::RS_NONE;
    }
}

bool RoughMultiIndex::UpdateGlobalRoughFilter(int dim, int desc_num) {
  bool any_nonempty = false;
  common::RSValue *loc_rs = GetLocalDescFilter(dim, desc_num, true);
  if (loc_rs == NULL) return true;
  for (int p = 0; p < no_packs[dim]; p++) {
    if (rf[dim][p] == common::RSValue::RS_UNKNOWN) {  // not known yet - get the first
                                                      // information available
      rf[dim][p] = loc_rs[p];
      if (rf[dim][p] != common::RSValue::RS_NONE) any_nonempty = true;
    } else if (loc_rs[p] == common::RSValue::RS_NONE)
      rf[dim][p] = common::RSValue::RS_NONE;
    else if (rf[dim][p] != common::RSValue::RS_NONE) {  // else no change
      any_nonempty = true;
      if (loc_rs[p] != common::RSValue::RS_ALL ||
          rf[dim][p] != common::RSValue::RS_ALL)  // else rf[..] remains common::RSValue::RS_ALL
        rf[dim][p] = common::RSValue::RS_SOME;
    }
  }
  return any_nonempty;
}

void RoughMultiIndex::UpdateGlobalRoughFilter(int dim,
                                              Filter *loc_f)  // if the filter is nontrivial, then copy pack status
{
  if (!loc_f) return;
  for (int p = 0; p < no_packs[dim]; p++) {
    if (loc_f->IsEmpty(p)) rf[dim][p] = common::RSValue::RS_NONE;
  }
}

void RoughMultiIndex::UpdateLocalRoughFilters(int dim)  // make projection from global filters to all local ones for
                                                        // given dimensions
{
  for (int p = 0; p < no_packs[dim]; p++) {
    if (rf[dim][p] == common::RSValue::RS_NONE) {
      for (uint i = 0; i < local_desc[dim].size(); i++) (local_desc[dim])[i]->desc_rf[p] = common::RSValue::RS_NONE;
    }
  }
}

void RoughMultiIndex::UpdateReducedDimension() {
  for (int i = 0; i < no_dims; i++) {
    UpdateReducedDimension(i);
  }
}

void RoughMultiIndex::UpdateReducedDimension(int d) {
  int omitted = 0;
  for (int i = 0; i < no_packs[d]; i++)
    if (rf[d][i] == common::RSValue::RS_NONE) omitted++;
  no_empty_packs[d] = omitted;
}

std::vector<int> RoughMultiIndex::GetReducedDimensions() {
  std::vector<int> dims;
  for (int d = 0; d < no_dims; d++) {
    int omitted = 0;
    for (int i = 0; i < no_packs[d]; i++)
      if (rf[d][i] == common::RSValue::RS_NONE) omitted++;
    if (no_empty_packs[d] < omitted) dims.push_back(d);
  }
  return dims;
}

RoughMultiIndex::RFDesc::RFDesc(int packs, int d) {
  desc_num = d;
  no_packs = packs;
  desc_rf = new common::RSValue[no_packs];
}

RoughMultiIndex::RFDesc::RFDesc(const RFDesc &sec) {
  no_packs = sec.no_packs;
  desc_num = sec.desc_num;
  desc_rf = new common::RSValue[no_packs];
  for (int i = 0; i < no_packs; i++) desc_rf[i] = sec.desc_rf[i];
}

RoughMultiIndex::RFDesc::~RFDesc() { delete[] desc_rf; }
}  // namespace core
}  // namespace stonedb
