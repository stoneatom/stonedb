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

#include "joiner_mapped.h"

#include "common/common_definitions.h"
#include "core/ctask.h"
#include "core/engine.h"
#include "core/mi_new_contents.h"
#include "core/transaction.h"
#include "util/log_ctl.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
JoinerMapped::JoinerMapped(MultiIndex *_mind, TempTable *_table, JoinTips &_tips)
    : TwoDimensionalJoiner(_mind, _table, _tips) {
  traversed_dims = DimensionVector(_mind->NumOfDimensions());
  matched_dims = DimensionVector(_mind->NumOfDimensions());
}

void JoinerMapped::ExecuteJoinConditions(Condition &cond) {
  MEASURE_FET("JoinerMapped::ExecuteJoinConditions(...)");

  std::vector<int64_t> rownums;
  rownums.reserve(128);
  why_failed = JoinFailure::FAIL_1N_TOO_HARD;
  auto &desc(cond[0]);
  if (cond.Size() > 1 || !desc.IsType_JoinSimple() || desc.op != common::Operator::O_EQ)
    return;

  vcolumn::VirtualColumn *vc1 = desc.attr.vc;
  vcolumn::VirtualColumn *vc2 = desc.val1.vc;

  if (!vc1->Type().IsFixed() || !vc2->Type().IsFixed() || vc1->Type().GetScale() != vc2->Type().GetScale() ||
      vc1->Type().Lookup() || vc2->Type().Lookup())
    return;

  vc1->MarkUsedDims(traversed_dims);  // "traversed" is unique now (a "dimension" table)
  vc2->MarkUsedDims(matched_dims);    // a "fact" table for 1:n relation
  mind->MarkInvolvedDimGroups(traversed_dims);
  mind->MarkInvolvedDimGroups(matched_dims);
  if (traversed_dims.Intersects(matched_dims))  // both materialized - we should
                                                // rather use a simple loop
    return;

  MIIterator mit_m(mind, matched_dims);
  uint64_t dim_m_size = mit_m.NumOfTuples();
  MIIterator mit_t(mind, traversed_dims);
  uint64_t dim_t_size = mit_t.NumOfTuples();

  if ((vc2->IsDistinct() && !vc1->IsDistinct()) || traversed_dims.NoDimsUsed() > 1 || dim_m_size < dim_t_size) {
    desc.SwitchSides();
    vc1 = desc.attr.vc;
    vc2 = desc.val1.vc;
    DimensionVector temp_dims(traversed_dims);
    traversed_dims = matched_dims;
    matched_dims = temp_dims;
  }
  if (traversed_dims.NoDimsUsed() > 1 ||         // more than just a number
      matched_dims.Intersects(desc.right_dims))  // only traversed dimension may be outer
    return;
  int traversed_dim = traversed_dims.GetOneDim();
  DEBUG_ASSERT(traversed_dim > -1);

  // Prepare mapping function
  auto map_function = GenerateFunction(vc1);
  if (!map_function)
    return;

  bool outer_join = !(desc.right_dims.IsEmpty());  // traversed is outer: scan facts, add nulls
                                                   // for all non-matching dimension values
  bool outer_nulls_only = outer_join;
  if (tips.null_only[traversed_dim] == false)
    outer_nulls_only = false;  // leave only those tuples which obtain outer nulls

  MIIterator mit(mind, matched_dims);
  uint64_t dim2_size = mit.NumOfTuples();

  mind->LockAllForUse();
  MINewContents new_mind(mind, tips);
  new_mind.SetDimensions(traversed_dims);
  new_mind.SetDimensions(matched_dims);
  if (!tips.count_only)
    new_mind.Init(dim2_size);
  else if (outer_join) {  // outer join on keys verified as unique - a trivial
                          // answer
    new_mind.CommitCountOnly(dim2_size);
    why_failed = JoinFailure::NOT_FAILED;
    mind->UnlockAllFromUse();
    return;
  }

  // Matching loop itself
  int64_t joined_tuples = 0;
  int64_t packrows_omitted = 0;
  int64_t packrows_matched = 0;
  int single_filter_dim = new_mind.OptimizedCaseDimension();  // indicates a special case: the
                                                              // fact table remains a filter and
                                                              // the dimension table is forgotten
  if (single_filter_dim != -1 && !matched_dims[single_filter_dim])
    single_filter_dim = -1;
  while (mit.IsValid()) {
    bool omit_this_packrow = false;
    if (mit.PackrowStarted()) {
      if (m_conn->Killed()) {
        throw common::KilledException();
      }
      int64_t local_min = common::MINUS_INF_64;
      int64_t local_max = common::PLUS_INF_64;
      if (vc2->GetNumOfNulls(mit) == mit.GetPackSizeLeft()) {
        omit_this_packrow = true;
      } else {
        local_min = vc2->GetMinInt64(mit);
        local_max = vc2->GetMaxInt64(mit);
        if (map_function->ImpossibleValues(local_min, local_max)) {
          omit_this_packrow = true;
        }
      }
      packrows_matched++;
      bool roughly_all = (vc2->GetNumOfNulls(mit) == 0 && map_function->CertainValues(local_min, local_max));
      if (single_filter_dim != -1 && roughly_all) {
        if (new_mind.CommitPack(mit.GetCurPackrow(single_filter_dim))) {  // processed as a pack
          joined_tuples += mit.GetPackSizeLeft();
          mit.NextPackrow();
          packrows_omitted++;
          continue;  // here we are jumping out for processed packrow
        }
      } else if (omit_this_packrow && !outer_join) {
        mit.NextPackrow();
        packrows_omitted++;
        continue;  // here we are jumping out for impossible packrow
      }
      if (new_mind.NoMoreTuplesPossible())
        break;  // stop the join if nothing new may be obtained in some
                // optimized cases

      vc2->LockSourcePacks(mit);
    }

    // Exact part
    rownums.clear();
    if (!vc2->IsNull(mit))
      map_function->Fetch(vc2->GetNotNullValueInt64(mit), rownums);
    if (rownums.size()) {
      if (!outer_nulls_only) {
        for (auto rownum : rownums) {
          joined_tuples++;
          if (tips.count_only)
            continue;
          for (int i = 0; i < mind->NumOfDimensions(); i++)
            if (matched_dims[i])
              new_mind.SetNewTableValue(i, mit[i]);
          new_mind.SetNewTableValue(traversed_dim, rownum);
          new_mind.CommitNewTableValues();
        }
      }
    } else if (outer_join) {
      joined_tuples++;
      if (!tips.count_only) {
        for (int i = 0; i < mind->NumOfDimensions(); i++)
          if (matched_dims[i])
            new_mind.SetNewTableValue(i, mit[i]);
        new_mind.SetNewTableValue(traversed_dim, common::NULL_VALUE_64);
        new_mind.CommitNewTableValues();
      }
    }
    if (tips.limit != -1 && tips.limit <= joined_tuples)
      break;
    ++mit;
  }

  vc2->UnlockSourcePacks();

  // Cleaning up
  tianmu_control_.lock(m_conn->GetThreadID()) << "Produced " << joined_tuples << " tuples." << system::unlock;
  if (packrows_omitted > 0)
    tianmu_control_.lock(m_conn->GetThreadID())
        << "Roughly omitted " << int(packrows_omitted / double(packrows_matched) * 10000.0) / 100.0 << "% packrows."
        << system::unlock;
  if (tips.count_only)
    new_mind.CommitCountOnly(joined_tuples);
  else
    new_mind.Commit(joined_tuples);

  // update local statistics - wherever possible
  int64_t dist_vals_found = map_function->ApproxDistinctVals();
  if (dist_vals_found != common::NULL_VALUE_64) {
    vc1->SetLocalDistVals(dist_vals_found);
    if (!outer_join)
      vc2->SetLocalDistVals(dist_vals_found);  // matched values: not more than the traversed
                                               // ones in case of equality
  }

  mind->UnlockAllFromUse();
  why_failed = JoinFailure::NOT_FAILED;
}

std::unique_ptr<JoinerMapFunction> JoinerMapped::GenerateFunction(vcolumn::VirtualColumn *vc) {
  MIIterator mit(mind, traversed_dims);
  auto map_function = std::make_unique<MultiMapsFunction>(m_conn);
  if (!map_function->Init(vc, mit))
    return nullptr;

  tianmu_control_.lock(m_conn->GetThreadID())
      << "Join mapping (multimaps) created on " << mit.NumOfTuples() << " rows." << system::unlock;
  return map_function;
}

int64_t JoinerParallelMapped::ExecuteMatchLoop(std::shared_ptr<MultiIndexBuilder::BuildItem> *indextable,
                                               std::vector<uint32_t> &packrows, int64_t *match_tuples,
                                               vcolumn::VirtualColumn *_vc, CTask task,
                                               JoinerMapFunction *map_function) {
  std::vector<int64_t> rownums;
  rownums.reserve(128);

  common::SetMySQLTHD(m_conn->Thd());
  current_txn_ = m_conn;

  MIIterator mit(mind, matched_dims);
  mit.SetTaskId(task.dwTaskId);
  mit.SetTaskNum(task.dwPackNum);

  std::unique_ptr<vcolumn::VirtualColumn> vc(CreateVCCopy(_vc));
  *indextable = new_mind->CreateBuildItem();

  // Matching loop itself
  int64_t joined_tuples = 0;
  int64_t packrows_omitted = 0;
  int64_t packrows_matched = 0;
  int traversed_dim = traversed_dims.GetOneDim();
  int32_t i = task.dwStartPackno;
  while (i < task.dwEndPackno) {
    mit.SetNoPacksToGo(packrows[i]);
    mit.RewindToPack(packrows[i]);
    // for all non-matching dimension values

    while (mit.IsValid()) {
      bool omit_this_packrow = false;
      if (mit.PackrowStarted()) {
        if (m_conn->Killed()) {
          vc->UnlockSourcePacks();
          throw common::KilledException();
        }
        if (vc->GetNumOfNulls(mit) == mit.GetPackSizeLeft()) {
          omit_this_packrow = true;
        } else {
          if (map_function->ImpossibleValues(vc->GetMinInt64(mit), vc->GetMaxInt64(mit))) {
            omit_this_packrow = true;
          }
        }
        packrows_matched++;

        if (omit_this_packrow && !outer_join) {
          mit.NextPackrow();
          packrows_omitted++;
          continue;  // here we are jumping out for impossible packrow
        }
        vc->LockSourcePacks(mit);
      }

      // Exact part
      rownums.clear();
      if (!vc->IsNull(mit)) {
        map_function->Fetch(vc->GetNotNullValueInt64(mit), rownums);
      }
      if (rownums.size()) {
        if (!outer_nulls_only) {
          for (auto rownum : rownums) {
            joined_tuples++;
            if (tips.count_only)
              continue;
            for (int i = 0; i < mind->NumOfDimensions(); i++)
              if (matched_dims[i])
                (*indextable)->SetTableValue(i, mit[i]);
            (*indextable)->SetTableValue(traversed_dim, rownum);
            (*indextable)->CommitTableValues();
          }
        }
      } else if (outer_join) {
        joined_tuples++;
        if (!tips.count_only) {
          for (int i = 0; i < mind->NumOfDimensions(); i++)
            if (matched_dims[i])
              (*indextable)->SetTableValue(i, mit[i]);
          (*indextable)->SetTableValue(traversed_dim, common::NULL_VALUE_64);
          (*indextable)->CommitTableValues();
        }
      }
      if (tips.limit != -1 && tips.limit <= joined_tuples)
        break;
      ++mit;
    }
  }

  if (packrows_omitted > 0)
    tianmu_control_.lock(m_conn->GetThreadID())
        << "Roughly omitted " << int(packrows_omitted / double(packrows_matched) * 10000.0) / 100.0 << "% packrows."
        << system::unlock;

  *match_tuples = joined_tuples;
  return joined_tuples;
}

void JoinerParallelMapped::ExecuteJoinConditions(Condition &cond) {
  MEASURE_FET("JoinerParallelMapped::ExecuteJoinConditions(...)");

  why_failed = JoinFailure::FAIL_1N_TOO_HARD;
  auto &desc(cond[0]);
  if (cond.Size() > 1 || !desc.IsType_JoinSimple() || desc.op != common::Operator::O_EQ)
    return;

  vcolumn::VirtualColumn *vc1 = desc.attr.vc;
  vcolumn::VirtualColumn *vc2 = desc.val1.vc;

  if (!vc1->Type().IsFixed() || !vc2->Type().IsFixed() || vc1->Type().GetScale() != vc2->Type().GetScale() ||
      vc1->Type().Lookup() || vc2->Type().Lookup())
    return;

  vc1->MarkUsedDims(traversed_dims);  // "traversed" is unique now (a "dimension" table)
  vc2->MarkUsedDims(matched_dims);    // a "fact" table for 1:n relation
  mind->MarkInvolvedDimGroups(traversed_dims);
  mind->MarkInvolvedDimGroups(matched_dims);
  if (traversed_dims.Intersects(matched_dims))  // both materialized - we should
                                                // rather use a simple loop
    return;

  MIIterator mit_m(mind, matched_dims);
  MIIterator mit_t(mind, traversed_dims);

  if ((vc2->IsDistinct() && !vc1->IsDistinct()) || traversed_dims.NoDimsUsed() > 1 ||
      mit_m.NumOfTuples() < mit_t.NumOfTuples()) {
    desc.SwitchSides();
    vc1 = desc.attr.vc;
    vc2 = desc.val1.vc;
    DimensionVector temp_dims(traversed_dims);
    traversed_dims = matched_dims;
    matched_dims = temp_dims;
  }
  if (traversed_dims.NoDimsUsed() > 1 ||         // more than just a number
      matched_dims.Intersects(desc.right_dims))  // only traversed dimension may be outer
    return;

  // Prepare mapping function
  auto map_function = GenerateFunction(vc1);
  if (map_function == nullptr)
    return;

  outer_join = !(desc.right_dims.IsEmpty());  // traversed is outer: scan facts, add nulls
                                              // for all non-matching dimension values
  outer_nulls_only = outer_join;
  if (tips.null_only[traversed_dims.GetOneDim()] == false)
    outer_nulls_only = false;  // leave only those tuples which obtain outer nulls

  MIIterator mit(mind, matched_dims);
  uint64_t dim2_size = mit.NumOfTuples();
  std::vector<DimensionVector> dims_involved = {traversed_dims, matched_dims};
  mind->LockAllForUse();
  if (!tips.count_only)
    new_mind->Init(dim2_size, dims_involved);
  else if (outer_join) {  // outer join on keys verified as unique - a trivial
                          // answer
    new_mind->CommitCountOnly(dim2_size);
    why_failed = JoinFailure::NOT_FAILED;
    mind->UnlockAllFromUse();
    return;
  }

  std::vector<uint32_t> packrows;
  while (mit.IsValid()) {
    packrows.push_back(mit.GetCurPackrow(mit.GetOneFilterDim()));
    mit.NextPackrow();
  }
  mit.Rewind();

  uint32_t packnums = packrows.size();
  int64_t joined_tuples = 0;
  int tids =
      (packnums < std::thread::hardware_concurrency() / 2) ? packnums : (std::thread::hardware_concurrency() / 2);
  if (tids <= 2)
    tids = 1;
  std::vector<std::shared_ptr<MultiIndexBuilder::BuildItem>> indextable(tids);
  utils::result_set<int64_t> res;
  std::vector<int64_t> matched_tuples(tids);
  CTask task;
  task.dwPackNum = tids;
  for (int table_id = 0; table_id < tids; table_id++) {
    task.dwTaskId = table_id;
    task.dwStartPackno = table_id * (packnums / tids);
    task.dwEndPackno = (table_id == tids - 1) ? packnums : (table_id + 1) * (packnums / tids);
    res.insert(ha_tianmu_engine_->query_thread_pool.add_task(&JoinerParallelMapped::ExecuteMatchLoop, this,
                                                             &indextable[table_id], packrows, &matched_tuples[table_id],
                                                             vc2, task, map_function.get()));
  }
  res.get_all();

  vc2->UnlockSourcePacks();

  // Cleaning up
  for (int i = 0; i < tids; i++) {
    joined_tuples += matched_tuples[i];
    new_mind->AddBuildItem(indextable[i]);
  }
  tianmu_control_.lock(m_conn->GetThreadID()) << "Produced " << joined_tuples << " tuples." << system::unlock;

  new_mind->Commit(joined_tuples, tips.count_only);

  // update local statistics - wherever possible
  int64_t dist_vals_found = map_function->ApproxDistinctVals();
  if (dist_vals_found != common::NULL_VALUE_64) {
    vc1->SetLocalDistVals(dist_vals_found);
    if (!outer_join)
      vc2->SetLocalDistVals(dist_vals_found);  // matched values: not more than the traversed
                                               // ones in case of equality
  }

  mind->UnlockAllFromUse();
  why_failed = JoinFailure::NOT_FAILED;
}

void OffsetMapFunction::Fetch(int64_t key_val, std::vector<int64_t> &keys_value) {
  if (key_val < key_min || key_val > key_max)
    return;
  unsigned char s = key_status[key_val - key_table_min];
  if (s == 255)
    return;
  keys_value.push_back(key_val + offset_table[s]);
  return;  // Offset map has one unique key
}

bool OffsetMapFunction::Init(vcolumn::VirtualColumn *vc, MIIterator &mit) {
  int dim = vc->GetDim();
  if (dim == -1)
    return false;
  key_table_min = vc->RoughMin();
  int64_t key_table_max = vc->RoughMax();
  if (key_table_min == common::NULL_VALUE_64 || key_table_min == common::MINUS_INF_64 ||
      key_table_max == common::PLUS_INF_64 || key_table_max == common::NULL_VALUE_64)
    return false;
  int64_t span = key_table_max - key_table_min + 1;
  if (span < 0 || size_t(span) > 32_MB || (span < mit.NumOfTuples() && !vc->IsNullsPossible()))
    return false;

  key_status.resize(span, 255);

  int offsets_used = 0;
  while (mit.IsValid()) {
    if (mit.PackrowStarted())
      vc->LockSourcePacks(mit);
    int64_t val = vc->GetValueInt64(mit);
    if (val != common::NULL_VALUE_64) {
      if (val < key_min)
        key_min = val;
      if (val > key_max)
        key_max = val;
      int64_t row_offset = mit[dim] - val;
      int s;
      for (s = 0; s < offsets_used; s++) {
        if (offset_table[s] == row_offset)
          break;
      }
      if (s == offsets_used) {  // new offset found
        if (offsets_used == 255) {
          vc->UnlockSourcePacks();
          return false;  // too many offsets
        }
        offset_table[s] = row_offset;
        offsets_used++;
      }
      val = val - key_table_min;
      if (key_status[val] != 255) {
        vc->UnlockSourcePacks();
        return false;  // key repetition found
      }
      key_status[val] = s;
      dist_vals_found++;
    }
    ++mit;
  }
  vc->UnlockSourcePacks();
  key_continuous_max = key_min;
  while (key_continuous_max < key_max && key_status[key_continuous_max - key_table_min] != 255) key_continuous_max++;
  return true;
}

void MultiMapsFunction::Fetch(int64_t key_val, std::vector<int64_t> &keys_value) {
  for (auto &iter : keys_value_maps) {
    auto search = iter.equal_range(key_val);
    for (auto it = search.first; it != search.second; it++) {
      keys_value.push_back(it->second);  // record the row number in traversed dim
    }
  }

  return;
}

bool MultiMapsFunction::Init(vcolumn::VirtualColumn *vc, MIIterator &mit) {
  int dim = vc->GetDim();
  if (dim == -1)
    return false;
  key_table_min = vc->RoughMin();
  std::multimap<int64_t, int64_t> keys_value;
  int64_t key_table_max = vc->RoughMax();
  if (key_table_min == common::NULL_VALUE_64 || key_table_min == common::MINUS_INF_64 ||
      key_table_max == common::PLUS_INF_64 || key_table_max == common::NULL_VALUE_64)
    return false;
  int64_t span = key_table_max - key_table_min + 1;
  if (span < 0)
    return false;
  tianmu_control_.lock(m_conn->GetThreadID())
      << "MultiMapsFunction: constructing a multimap with " << mit.NumOfTuples() << " tuples." << system::unlock;
  while (mit.IsValid()) {
    if (mit.PackrowStarted())
      vc->LockSourcePacks(mit);
    int64_t val = vc->GetValueInt64(mit);
    if (val != common::NULL_VALUE_64) {
      if (val < key_min)
        key_min = val;
      if (val > key_max)
        key_max = val;
      keys_value.emplace(val, mit[dim]);
      ++vals_found;
    }
    ++mit;
  }
  vc->UnlockSourcePacks();
  tianmu_control_.lock(m_conn->GetThreadID()) << "MultiMapsFunction: keys_value construction done. " << system::unlock;
  keys_value_maps.emplace_back(keys_value);
  return true;
}
}  // namespace core
}  // namespace Tianmu
