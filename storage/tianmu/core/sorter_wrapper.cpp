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

#include "sorter_wrapper.h"
#include "core/engine.h"
#include "core/mi_iterator.h"
#include "core/pack_orderer.h"
#include "core/transaction.h"
#include "util/thread_pool.h"

namespace Tianmu {
namespace core {
SorterWrapper::SorterWrapper(MultiIndex &_mind, int64_t _limit) : cur_mit(&_mind) {
  no_of_rows = _mind.NumOfTuples();
  limit = _limit;
  s = nullptr;
  mi_encoder = nullptr;
  buf_size = 0;
  cur_val = nullptr;
  input_buf = nullptr;
  no_values_encoded = 0;
  rough_sort_by = -1;
}

SorterWrapper::~SorterWrapper() {
  delete s;
  delete mi_encoder;
  delete[] input_buf;
}

void SorterWrapper::AddSortedColumn(vcolumn::VirtualColumn *col, int sort_order, bool in_output) {
  input_cols.push_back(SorterColumnDescription(col, sort_order, in_output));
}

void SorterWrapper::InitOrderByVector(std::vector<int> &order_by) {
  // Identify columns to order by, exclude unnecessary ones
  for (uint i = 0; i < input_cols.size(); i++) order_by.push_back(-1);  // -1 means "no column"

  for (uint i = 0; i < input_cols.size(); i++) {
    int ord = input_cols[i].sort_order;
    if (ord < 0)
      ord = -ord;
    if (ord != 0) {
      DEBUG_ASSERT(order_by[ord - 1] == -1);
      order_by[ord - 1] = i;
    }
  }
  for (uint i = 0; i < input_cols.size(); i++)  // check whether all sorting columns are really needed
    if (order_by[i] != -1) {
      DEBUG_ASSERT(i == 0 || order_by[i - 1] != -1);  // assure continuity
      if (input_cols[order_by[i]].col->IsDistinct()) {
        // do not sort by any other column:
        for (uint j = i + 1; j < input_cols.size(); j++)
          if (order_by[j] != -1) {
            input_cols[order_by[j]].sort_order = 0;
            order_by[j] = -1;
          }
        break;
      }
    }
}

void SorterWrapper::InitSorter(MultiIndex &mind, bool implicit_logic) {
  DimensionVector implicit_dims(mind.NumOfDimensions());  // these dimensions will be implicit
  DimensionVector one_pack_dims(mind.NumOfDimensions());  // these dimensions contain only one used pack
                                                          // (potentially implicit)

  // Prepare optimization guidelines
  for (int dim = 0; dim < mind.NumOfDimensions(); dim++) {
    int no_packs = mind.MaxNumOfPacks(dim);
    if (no_packs == 1)
      one_pack_dims[dim] = true;  // possibly implicit if only one pack
                                  /*
                                          //disable implicit logic for SorterWrapper Merge case(multi thread
                                     case)
                                          //sortedtable and its sub-sorttable would has different limit value
                                     (limit=sortrows)
                                          //which means implicit_dims[dim] may be different sortedtable and
                                     its sub-sortedtable.
                                          //This would cause total_bytes being different between them, while
                                     PutValues(SorterWrapper &st)
                                          //require them to be the same.
                                  */
    if ((no_packs > (limit == -1 ? no_of_rows : limit)) && implicit_logic)
      implicit_dims[dim] = true;  // implicit if there is more opened packs than output rows
  }

  // Identify columns to order by
  std::vector<int> order_by;
  InitOrderByVector(order_by);

  // Determine encodings
  scol.reserve(input_cols.size());
  for (uint i = 0; i < input_cols.size(); i++) {
    int flags = 0;
    if (input_cols[i].sort_order != 0)
      flags |= ColumnBinEncoder::ENCODER_MONOTONIC;
    if (input_cols[i].sort_order < 0)
      flags |= ColumnBinEncoder::ENCODER_DESCENDING;
    if (input_cols[i].in_output) {
      flags |= ColumnBinEncoder::ENCODER_DECODABLE;
      if (input_cols[i].sort_order == 0)  // decodable, but not comparable (store values)
        flags |= ColumnBinEncoder::ENCODER_NONCOMPARABLE;
    }
    scol.push_back(ColumnBinEncoder(flags));
    scol[i].PrepareEncoder(input_cols[i].col);
    // neither in_output nor sorting keys => disable them, we'll not use it in
    // any way
    if (!input_cols[i].in_output && input_cols[i].sort_order == 0)
      scol[i].Disable();
    for (uint col = 0; col < i; col++) {
      if (scol[i].DefineAsEquivalent(scol[col])) {
        scol[i].SetDupCol(col);
        break;
      }
    }
  }

  // Identify implicitly encoded columns
  for (int dim = 0; dim < mind.NumOfDimensions(); dim++)
    if (one_pack_dims[dim] && !implicit_dims[dim]) {  // potentially implicit, check sizes
      uint col_sizes_for_dim = 0;
      for (uint i = 0; i < input_cols.size(); i++) {
        if (input_cols[i].sort_order == 0 && input_cols[i].col->GetDim() == dim && scol[i].IsEnabled()) {
          // check only non-ordered columns from the proper dimension
          col_sizes_for_dim += scol[i].GetPrimarySize();
        }
      }
      if (col_sizes_for_dim > MultiindexPositionEncoder::DimByteSize(&mind, dim))
        implicit_dims[dim] = true;  // actual data are larger than row number
    }

  DimensionVector implicit_now(mind.NumOfDimensions());  // these dimensions will be made implicit
  for (uint i = 0; i < input_cols.size(); i++) {
    if (input_cols[i].sort_order == 0 && scol[i].IsEnabled()) {
      DimensionVector local_dims(mind.NumOfDimensions());
      input_cols[i].col->MarkUsedDims(local_dims);
      if (local_dims.NoDimsUsed() > 0 && implicit_dims.Includes(local_dims)) {
        // all dims of the column are marked as implicit
        implicit_now.Plus(local_dims);
        scol[i].SetImplicit();
      }
    }
  }
  if (implicit_now.NoDimsUsed() > 0)  // nontrivial implicit columns present
    mi_encoder = new MultiindexPositionEncoder(&mind, implicit_now);

  // Define data offsets in sort buffers
  uint j = 0;
  int total_bytes = 0;
  int key_bytes = 0;
  rough_sort_by = -1;
  while (j < input_cols.size() && order_by[j] != -1) {
    scol[order_by[j]].SetPrimaryOffset(key_bytes);
    key_bytes += scol[order_by[j]].GetPrimarySize();
    total_bytes += scol[order_by[j]].GetPrimarySize();
    j++;
  }
  for (uint i = 0; i < scol.size(); i++) {
    if (scol[i].IsEnabled()) {
      if (input_cols[i].sort_order == 0) {
        scol[i].SetPrimaryOffset(total_bytes);
        total_bytes += scol[i].GetPrimarySize();
      } else if (scol[i].GetSecondarySize() > 0) {  // secondary place needed for sorted columns?
        scol[i].SetSecondaryOffset(total_bytes);
        total_bytes += scol[i].GetSecondarySize();
      }
      if (rough_sort_by == -1 && scol[i].IsNontrivial() &&
          (input_cols[i].sort_order == -1 || input_cols[i].sort_order == 1))
        rough_sort_by = i;
    }
  }
  if (rough_sort_by > -1 &&
      (input_cols[rough_sort_by].col->GetDim() == -1 ||  // switch off this optimization for harder cases
       scol[rough_sort_by].IsString() || input_cols[rough_sort_by].col->Type().IsFloat() ||
       input_cols[rough_sort_by].col->Type().Lookup() ||
       input_cols[rough_sort_by].col->GetMultiIndex()->OrigSize(input_cols[rough_sort_by].col->GetDim()) < 65536))
    rough_sort_by = -1;
  if (mi_encoder) {
    mi_encoder->SetPrimaryOffset(total_bytes);
    total_bytes += mi_encoder->GetPrimarySize();
  }

  // Everything prepared - create sorter
  if (total_bytes > 0) {  // the sorter is nontrivial
    s = Sorter3::CreateSorter(no_of_rows, key_bytes, total_bytes, limit);
    input_buf = new unsigned char[total_bytes];
    if (mi_encoder)
      tianmu_control_.lock(s->conn->GetThreadID())
          << s->Name() << " begin, initialized for " << no_of_rows << " rows, " << key_bytes << "+"
          << total_bytes - key_bytes - mi_encoder->GetPrimarySize() << "+" << mi_encoder->GetPrimarySize()
          << " bytes each." << system::unlock;
    else
      tianmu_control_.lock(s->conn->GetThreadID())
          << s->Name() << " begin, initialized for " << no_of_rows << " rows, " << key_bytes << "+"
          << total_bytes - key_bytes << " bytes each." << system::unlock;
  }
}

void SorterWrapper::SortRoughly(std::vector<PackOrderer> &po) {
  if (rough_sort_by > -1) {  // nontrivial rough_sort_by => order based on one dimension
    int dim = input_cols[rough_sort_by].col->GetDim();
    bool asc = (input_cols[rough_sort_by].sort_order > 0);  // ascending sort
    // start with best packs to possibly roughly exclude others
    po[dim].Init(input_cols[rough_sort_by].col,
                 (asc ? PackOrderer::OrderType::kMaxAsc : PackOrderer::OrderType::kMinDesc));
  }
}

bool SorterWrapper::InitPackrow(MIIterator &mit)  // return true if the packrow may be skipped
{
  if (s == nullptr)
    return false;  // trivial sorter (constant values)

  // rough sort: exclude packs which are for sure out of scope
  if (rough_sort_by > -1 && limit > -1 && no_values_encoded >= limit) {
    // nontrivial rough_sort_by => numerical case only
    vcolumn::VirtualColumn *vc = input_cols[rough_sort_by].col;
    if (vc->GetNumOfNulls(mit) == 0) {
      bool asc = (input_cols[rough_sort_by].sort_order > 0);  // ascending sort
      int64_t local_min = (asc ? vc->GetMinInt64(mit) : common::MINUS_INF_64);
      int64_t local_max = (asc ? common::PLUS_INF_64 : vc->GetMaxInt64(mit));
      if (scol[rough_sort_by].ImpossibleValues(local_min, local_max))
        return true;  // exclude
    }
  }

  TIANMU_LOG(LogCtl_Level::DEBUG, "InitPackrow: no_values_encoded %d, begin to loadpacks scol size %d ",
             no_values_encoded, scol.size());
  // Not excluded: lock packs
  if (!ha_tianmu_engine_->query_thread_pool.is_owner()) {
    utils::result_set<void> res;
    for (uint i = 0; i < scol.size(); i++)
      res.insert(ha_tianmu_engine_->query_thread_pool.add_task(&ColumnBinEncoder::LoadPacks, &scol[i], &mit));
    res.get_all_with_except();
  } else {
    for (uint i = 0; i < scol.size(); i++)
      if (scol[i].IsEnabled() && scol[i].IsNontrivial())  // constant num. columns are trivial
        scol[i].LockSourcePacks(mit);
  }
  return false;
}

bool SorterWrapper::PutValues(MIIterator &mit) {
  if (s == nullptr)
    return false;  // trivial sorter (constant values)
  if (mi_encoder)
    mi_encoder->Encode(input_buf, mit);
  bool update_stats =
      (rough_sort_by > -1 && limit > -1 && no_values_encoded <= limit);  // otherwise statistics are either not used
                                                                         // or already prepared
  for (uint i = 0; i < scol.size(); i++)
    if (scol[i].IsEnabled() && scol[i].IsNontrivial())  // constant num. columns are trivial
      scol[i].Encode(input_buf, mit, nullptr, update_stats);
  no_values_encoded++;
  return s->PutValue(input_buf);
}

bool SorterWrapper::PutValues(SorterWrapper &sw) {
  if (s == nullptr)
    return false;  // trivial sorter (constant values)
  no_values_encoded += sw.GetEncodedValNum();
  TIANMU_LOG(LogCtl_Level::DEBUG, "PutValues: no_values_encoded %d \n", no_values_encoded);
  return s->PutValue(sw.GetSorter());
}

bool SorterWrapper::FetchNextRow() {
  if (s == nullptr) {
    no_of_rows--;
    return (no_of_rows >= 0);  // trivial sorter (constant values, cur_val is always nullptr)
  }
  cur_val = s->GetNextValue();
  if (cur_val == nullptr)
    return false;
  if (mi_encoder)
    mi_encoder->GetValue(cur_val,
                         cur_mit);  // decode the MIIterator (for implicit/virtual values)
  return true;
}
}  // namespace core
}  // namespace Tianmu
