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

#include "joiner_hash.h"

#include "common/assert.h"
#include "core/engine.h"
#include "core/join_thread_table.h"
#include "core/mi_new_contents.h"
#include "core/rough_multi_index.h"
#include "core/temp_table.h"
#include "core/transaction.h"
#include "system/fet.h"
#include "util/thread_pool.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
JoinerHash::JoinerHash(MultiIndex *_mind, TempTable *_table, JoinTips &_tips)
    : TwoDimensionalJoiner(_mind, _table, _tips) {
  p_power = _mind->ValueOfPower();
  traversed_dims = DimensionVector(_mind->NumOfDimensions());
  matched_dims = DimensionVector(_mind->NumOfDimensions());
  for (int i = 0; i < _mind->NumOfDimensions(); i++) traversed_hash_column.push_back(-1);
  no_of_traversed_dims = 0;
  cond_hashed = 0;
  other_cond_exist = false;
  packrows_omitted = 0;
  packrows_matched = 0;
  actually_traversed_rows = 0;
  watch_traversed = false;
  watch_matched = false;
  force_switching_sides = false;
  too_many_conflicts = false;
  outer_nulls_only = false;
}

void JoinerHash::ExecuteJoinConditions(Condition &cond) {
  MEASURE_FET("JoinerHash::ExecuteJoinConditions(...)");
  why_failed = JoinFailure::FAIL_HASH;

  std::vector<int> hash_descriptors;
  // Prepare all descriptor information
  bool first_found = true;
  DimensionVector dims1(mind->NumOfDimensions());  // Initial dimension descriptions
  DimensionVector dims2(mind->NumOfDimensions());
  DimensionVector dims_other(mind->NumOfDimensions());  // dimensions for other conditions, if needed
  for (uint i = 0; i < cond.Size(); i++) {
    bool added = false;
    if (cond[i].IsType_JoinSimple() && cond[i].op == common::Operator::O_EQ) {
      if (first_found) {
        hash_descriptors.push_back(i);
        added = true;
        cond[i].attr.vc->MarkUsedDims(dims1);
        cond[i].val1.vc->MarkUsedDims(dims2);
        mind->MarkInvolvedDimGroups(dims1);
        mind->MarkInvolvedDimGroups(dims2);
        // Add dimensions for nested outer joins
        if (dims1.Intersects(cond[i].right_dims))
          dims1.Plus(cond[i].right_dims);
        if (dims2.Intersects(cond[i].right_dims))
          dims2.Plus(cond[i].right_dims);
        first_found = false;
      } else {
        DimensionVector sec_dims1(mind->NumOfDimensions());  // Make sure the local descriptions are
                                                             // compatible
        DimensionVector sec_dims2(mind->NumOfDimensions());
        cond[i].attr.vc->MarkUsedDims(sec_dims1);
        cond[i].val1.vc->MarkUsedDims(sec_dims2);
        if (dims1.Includes(sec_dims1) && dims2.Includes(sec_dims2)) {
          hash_descriptors.push_back(i);
          added = true;
        } else if (dims1.Includes(sec_dims2) && dims2.Includes(sec_dims1)) {
          cond[i].SwitchSides();
          hash_descriptors.push_back(i);
          added = true;
        }
      }
    }
    if (!added) {
      other_cond_exist = true;
      cond[i].DimensionUsed(dims_other);
      other_cond.push_back(cond[i]);
    }
  }
  cond_hashed = int(hash_descriptors.size());
  if (cond_hashed == 0) {
    why_failed = JoinFailure::FAIL_HASH;
    return;
  }

  // Check the proper direction: the (much) smaller dimension should be
  // traversed, the larger one should be matched but some special cases may
  // change the direction. Rules:
  // - traverse dim1 if it is 3 times smaller than dim2,
  // - if neither is 3 times smaller than other, traverse the one which is less
  // repeatable
  bool switch_sides = false;
  int64_t dim1_size = mind->NumOfTuples(dims1);
  int64_t dim2_size = mind->NumOfTuples(dims2);
  bool easy_roughable = false;
  if (std::min(dim1_size, dim2_size) > 100000) {  // approximate criteria for large tables (many packs)
    if (dim1_size > 2 * dim2_size)
      switch_sides = true;
    else if (2 * dim1_size > dim2_size) {  // within reasonable range - check
                                           // again whether to change sides
      int64_t dim1_distinct = cond[hash_descriptors[0]].attr.vc->GetApproxDistVals(false);  // exclude nulls
      int64_t dim2_distinct = cond[hash_descriptors[0]].val1.vc->GetApproxDistVals(false);
      // check whether there are any nontrivial differencies in distinct value
      // distributions
      if (dim1_distinct > dim1_size * 0.9 && dim2_distinct > dim2_size * 0.9) {
        if (dim1_size > dim2_size)  // no difference - just check table sizes
          switch_sides = true;
      } else if (double(dim1_distinct) / dim1_size <
                 double(dim2_distinct) / dim2_size)  // switch if dim1 has more repeating values
        switch_sides = true;
    }
  } else if (dim1_size > dim2_size)
    switch_sides = true;

  if (force_switching_sides)
    switch_sides = !switch_sides;
  if (switch_sides) {
    for (int i = 0; i < cond_hashed; i++)  // switch sides of joining conditions
      cond[hash_descriptors[i]].SwitchSides();
    traversed_dims = dims2;
    matched_dims = dims1;
  } else {
    traversed_dims = dims1;
    matched_dims = dims2;
  }

  // jhash is a class field, initialized as empty
  vc1.reserve(cond_hashed);
  vc2.reserve(cond_hashed);
  bool compatible = true;
  for (int i = 0; i < cond_hashed; i++) {  // add all key columns
    vc1.push_back(cond[hash_descriptors[i]].attr.vc);
    vc2.push_back(cond[hash_descriptors[i]].val1.vc);
    compatible = jhash.AddKeyColumn(vc1[i], vc2[i]) && compatible;
  }
  // enlarge matched or traversed dimension lists by adding non-hashed
  // conditions, if any
  if (other_cond_exist) {
    if (matched_dims.Intersects(cond[0].right_dims) && !cond[0].right_dims.Includes(dims_other)) {
      // special case: do not add non-right-side other dimensions to right-side
      // matched
      dims_other.Minus(matched_dims);
      traversed_dims.Plus(dims_other);
    } else {
      // default: other dims as matched
      dims_other.Minus(traversed_dims);
      matched_dims.Plus(dims_other);
    }
  }
  // check whether we should take into account more dims
  mind->MarkInvolvedDimGroups(traversed_dims);
  mind->MarkInvolvedDimGroups(matched_dims);

  if (traversed_dims.Intersects(matched_dims) ||  // both materialized - we should rather use a simple
                                                  // loop
      !compatible) {                              // could not prepare common encoding
    why_failed = JoinFailure::FAIL_HASH;
    return;
  }
  // prepare columns for traversed dimension numbers in hash table
  no_of_traversed_dims = 0;
  for (int i = 0; i < mind->NumOfDimensions(); i++) {
    if (traversed_dims[i] && !(tips.count_only && !other_cond_exist)) {  // storing tuples may be omitted if
                                                                         // (count_only_now && !other_cond_exist)
      traversed_hash_column[i] = cond_hashed + no_of_traversed_dims;     // jump over the joining key columns
      no_of_traversed_dims++;
      int bin_index_size = 4;
      if (mind->OrigSize(i) > 0x000000007FFFFF00)
        bin_index_size = 8;
      jhash.AddTupleColumn(bin_index_size);
    }
  }
  // calculate the size of hash table
  jhash.Initialize(int64_t(mind->NumOfTuples(traversed_dims) * 1.5), easy_roughable);

  // jhash prepared, perform the join
  InitOuter(cond);
  ExecuteJoin();
  if (too_many_conflicts)
    why_failed = JoinFailure::FAIL_WRONG_SIDES;
  else
    why_failed = JoinFailure::NOT_FAILED;
}

void JoinerHash::ExecuteJoin() {
  MEASURE_FET("JoinerHash::ExecuteJoin(...)");
  int64_t joined_tuples = 0;

  // preparing the new multiindex tables
  MIIterator tr_mit(mind, traversed_dims);
  MIIterator match_mit(mind, matched_dims);
  uint64_t dim1_size = tr_mit.NumOfTuples();
  uint64_t dim2_size = match_mit.NumOfTuples();

  uint64_t approx_join_size = dim1_size;
  if (dim2_size > approx_join_size)
    approx_join_size = dim2_size;

  std::vector<bool> traverse_keys_unique;
  for (int i = 0; i < cond_hashed; i++) traverse_keys_unique.push_back(vc1[i]->IsDistinctInTable());

  mind->LockAllForUse();
  MINewContents new_mind(mind, tips);
  new_mind.SetDimensions(traversed_dims);
  new_mind.SetDimensions(matched_dims);
  if (!tips.count_only)
    new_mind.Init(approx_join_size);
  // joining itself

  int64_t traversed_tuples = 0;
  actually_traversed_rows = 0;
  int64_t outer_tuples = 0;

  if (dim1_size > 0 && dim2_size > 0)
    while (tr_mit.IsValid()) {
      traversed_tuples += TraverseDim(new_mind, tr_mit, outer_tuples);
      if (tr_mit.IsValid())
        tianmu_control_.lock(m_conn->GetThreadID())
            << "Traversed " << traversed_tuples << "/" << dim1_size << " rows." << system::unlock;
      else
        tianmu_control_.lock(m_conn->GetThreadID()) << "Traversed all " << dim1_size << " rows." << system::unlock;

      if (too_many_conflicts) {
        tianmu_control_.lock(m_conn->GetThreadID()) << "Too many hash conflicts: restarting join." << system::unlock;
        return;  // without committing new_mind
      }
      joined_tuples += MatchDim(new_mind, match_mit);
      if (watch_traversed)
        outer_tuples += SubmitOuterTraversed(new_mind);

      tianmu_control_.lock(m_conn->GetThreadID()) << "Produced " << joined_tuples << " tuples." << system::unlock;
      if (!outer_nulls_only) {
        if (tips.limit != -1 && tips.limit <= joined_tuples)
          break;
      }
    }
  // outer join part
  int64_t outer_tuples_matched = 0;
  if (watch_matched && !outer_filter->IsEmpty())
    outer_tuples_matched = SubmitOuterMatched(match_mit, new_mind);
  outer_tuples += outer_tuples_matched;
  if (outer_tuples > 0)
    tianmu_control_.lock(m_conn->GetThreadID())
        << "Added " << outer_tuples << " null tuples by outer join." << system::unlock;
  joined_tuples += outer_tuples;
  // revert multiindex to the updated tables
  if (packrows_omitted > 0)
    tianmu_control_.lock(m_conn->GetThreadID())
        << "Roughly omitted " << int(packrows_omitted / double(packrows_matched) * 10000.0) / 100.0 << "% packrows."
        << system::unlock;
  if (tips.count_only)
    new_mind.CommitCountOnly(joined_tuples);
  else
    new_mind.Commit(joined_tuples);

  // update local statistics - wherever possible
  int64_t traversed_dist_limit = actually_traversed_rows;
  for (int i = 0; i < cond_hashed; i++)
    if (!vc2[i]->Type().IsString()) {  // otherwise different collations may be
                                       // a counterexample
      vc2[i]->SetLocalDistVals(actually_traversed_rows + outer_tuples_matched);  // matched values: not more than the
                                                                                 // traversed ones in case of equality
      if (traverse_keys_unique[i])                                               // unique => there is no more rows than
                                                                                 // distinct values
        traversed_dist_limit =
            std::min(traversed_dist_limit, vc2[i]->GetApproxDistVals(false) + (outer_tuples - outer_tuples_matched));
    }
  for (int i = 0; i < mind->NumOfDimensions(); i++)
    if (traversed_dims[i])
      table->SetVCDistinctVals(i,
                               traversed_dist_limit);  // all dimensions involved in traversed side
  mind->UnlockAllFromUse();
}

int64_t JoinerHash::TraverseDim(MINewContents &new_mind, MIIterator &mit, int64_t &outer_tuples)
// new_mind and outer_tuples (the counter) are used only for outer joins
{
  MEASURE_FET("JoinerHash::TraverseMaterialDim(...)");
  jhash.ClearAll();

  int availabled_packs = (int)((mind->NumOfTuples(traversed_dims) + (1 << p_power) - 1) >> p_power);
  TempTablePackLocker temptable_pack_locker(vc1, cond_hashed, availabled_packs);

  int64_t hash_row = 0;  // hash_row = 0, otherwise deadlock for null on the first position
  int64_t traversed_rows = 0;

  if (watch_traversed)
    outer_filter->Reset();  // set only occupied hash positions
  bool first_run = true;    // to prevent opening unlocked packs on the next traverse

  while (mit.IsValid()) {
    if (m_conn->Killed())
      throw common::KilledException();
    if (mit.PackrowStarted() || first_run) {
      for (int i = 0; i < cond_hashed; i++) vc1[i]->LockSourcePacks(mit);
      first_run = false;
    }
    bool omit_row = false;
    for (int i = 0; i < cond_hashed; i++) {
      if (vc1[i]->IsNull(mit)) {
        omit_row = true;
        break;
      }
      jhash.PutKeyValue(i, mit);
    }
    if (!omit_row) {  // else go to the next row - equality cannot be fulfilled
      hash_row = jhash.FindAndAddCurrentRow();
      if (hash_row == common::NULL_VALUE_64)
        break;  // no space left - stop for now and then restart from the
                // current row
      if (!force_switching_sides && jhash.TooManyConflicts()) {
        too_many_conflicts = true;
        break;  // and exit the function
      }
      if (watch_traversed)
        outer_filter->Set(hash_row);
      actually_traversed_rows++;
      // Put the tuple column. Note: needed also for count_only_now, because
      // another conditions may need it.
      if (!tips.count_only || other_cond_exist) {
        for (int i = 0; i < mind->NumOfDimensions(); i++)
          if (traversed_dims[i])
            jhash.PutTupleValue(traversed_hash_column[i], hash_row, mit[i]);
      }
    } else if (watch_traversed) {
      // Add outer tuples for omitted hash positions
      for (int i = 0; i < mind->NumOfDimensions(); i++) {
        if (traversed_dims[i])
          new_mind.SetNewTableValue(i, mit[i]);
        else if (matched_dims[i])
          new_mind.SetNewTableValue(i, common::NULL_VALUE_64);
      }
      new_mind.CommitNewTableValues();
      actually_traversed_rows++;
      outer_tuples++;
    }
    ++mit;
    traversed_rows++;
  }

  for (int i = 0; i < cond_hashed; i++) {
    vc1[i]->UnlockSourcePacks();
  }

  return traversed_rows;
}

int64_t JoinerHash::MatchDim(MINewContents &new_mind, MIIterator &mit) {
  MEASURE_FET("JoinerHash::MatchMaterialDim(...)");
  int64_t joined_tuples = 0;
  int64_t hash_row;
  int64_t no_of_matching_rows;
  mit.Rewind();
  int64_t matching_row = 0;
  MIDummyIterator combined_mit(mind);  // a combined iterator for checking non-hashed conditions, if any

  int availabled_packs = (int)((mind->NumOfTuples(matched_dims) + (1 << p_power) - 1) >> p_power);
  TempTablePackLocker temptable_pack_locker(vc2, cond_hashed, availabled_packs);

  while (mit.IsValid()) {
    if (m_conn->Killed())
      throw common::KilledException();
    // Rough and locking part
    bool omit_this_packrow = false;
    bool packrow_uniform = false;  // if the packrow is uniform, process it massively
    if (mit.PackrowStarted()) {
      packrow_uniform = true;
      for (int i = 0; i < cond_hashed; i++) {
        if (jhash.StringEncoder(i)) {
          if (!vc2[i]->Type().Lookup()) {  // lookup treated as string, when the
                                           // dictionaries aren't convertible
            types::BString local_min = vc2[i]->GetMinString(mit);
            types::BString local_max = vc2[i]->GetMaxString(mit);
            if (!local_min.IsNull() && !local_max.IsNull() && jhash.ImpossibleValues(i, local_min, local_max)) {
              omit_this_packrow = true;
              break;
            }
          }
          packrow_uniform = false;
        } else {
          int64_t local_min = vc2[i]->GetMinInt64(mit);
          int64_t local_max = vc2[i]->GetMaxInt64(mit);
          if (local_min == common::NULL_VALUE_64 || local_max == common::NULL_VALUE_64 ||  // common::NULL_VALUE_64
                                                                                           // only for nulls only
              jhash.ImpossibleValues(i, local_min, local_max)) {
            omit_this_packrow = true;
            break;
          }
          if (other_cond_exist || local_min != local_max || vc2[i]->IsNullsPossible()) {
            packrow_uniform = false;
          }
        }
      }
      packrows_matched++;
      if (packrow_uniform && !omit_this_packrow) {
        for (int i = 0; i < cond_hashed; i++) {
          int64_t local_min = vc2[i]->GetMinInt64(mit);
          jhash.PutMatchedValue(i, local_min);
        }
        no_of_matching_rows = jhash.InitCurrentRowToGet() * mit.GetPackSizeLeft();
        if (!tips.count_only)
          while ((hash_row = jhash.GetNextRow()) != common::NULL_VALUE_64) {
            MIIterator mit_this_pack(mit);
            int64_t matching_this_pack = matching_row;
            do {
              SubmitJoinedTuple(hash_row, mit_this_pack, new_mind);
              if (watch_matched)
                outer_filter->ResetDelayed(matching_this_pack);
              ++mit_this_pack;
              matching_this_pack++;
            } while (mit_this_pack.IsValid() && !mit_this_pack.PackrowStarted());
          }
        else if (watch_traversed) {
          while ((hash_row = jhash.GetNextRow()) != common::NULL_VALUE_64) outer_filter->Reset(hash_row);
        }

        joined_tuples += no_of_matching_rows;
        omit_this_packrow = true;
      }
      if (omit_this_packrow) {
        matching_row += mit.GetPackSizeLeft();
        mit.NextPackrow();
        packrows_omitted++;
        continue;  // here we are jumping out for impossible or uniform packrow
      }
      if (new_mind.NoMoreTuplesPossible())
        break;  // stop the join if nothing new may be obtained in some
                // optimized cases

      for (int i = 0; i < cond_hashed; i++) vc2[i]->LockSourcePacks(mit);
    }
    // Exact part - make the key row ready for comparison
    bool null_found = false;
    bool non_matching_sizes = false;
    for (int i = 0; i < cond_hashed; i++) {
      if (vc2[i]->IsNull(mit)) {
        null_found = true;
        break;
      }
      jhash.PutMatchedValue(i, vc2[i], mit);
    }
    if (!null_found && !non_matching_sizes) {  // else go to the next row -
                                               // equality cannot be fulfilled
      no_of_matching_rows = jhash.InitCurrentRowToGet();
      // Find all matching rows
      if (!other_cond_exist) {
        // Basic case - just equalities
        if (!tips.count_only)
          while ((hash_row = jhash.GetNextRow()) != common::NULL_VALUE_64)
            SubmitJoinedTuple(hash_row, mit,
                              new_mind);  // use the multiindex iterator position
        else if (watch_traversed) {
          while ((hash_row = jhash.GetNextRow()) != common::NULL_VALUE_64) outer_filter->Reset(hash_row);
        }
        if (watch_matched && no_of_matching_rows > 0)
          outer_filter->ResetDelayed(matching_row);
        joined_tuples += no_of_matching_rows;
      } else {
        // Complex case: different types of join conditions mixed together
        combined_mit.Combine(mit);
        while ((hash_row = jhash.GetNextRow()) != common::NULL_VALUE_64) {
          bool other_cond_true = true;
          for (int i = 0; i < mind->NumOfDimensions(); i++) {
            if (traversed_dims[i])
              combined_mit.Set(i, jhash.GetTupleValue(traversed_hash_column[i], hash_row));
          }
          for (auto &j : other_cond) {
            j.LockSourcePacks(combined_mit);
            if (j.CheckCondition(combined_mit) == false) {
              other_cond_true = false;
              break;
            }
          }
          if (other_cond_true) {
            if (!tips.count_only)
              SubmitJoinedTuple(hash_row, mit,
                                new_mind);  // use the multiindex iterator position
            else if (watch_traversed)
              outer_filter->Reset(hash_row);
            joined_tuples++;
            if (watch_matched)
              outer_filter->ResetDelayed(matching_row);
          }
        }
      }
      if (!outer_nulls_only) {
        if (tips.limit != -1 && tips.limit <= joined_tuples)
          break;
      }
    }
    ++mit;
    matching_row++;
  }
  if (watch_matched)
    outer_filter->Commit();  // commit the delayed resets

  for (int i = 0; i < cond_hashed; i++) {
    vc2[i]->UnlockSourcePacks();
  }
  for (auto &j : other_cond) j.UnlockSourcePacks();

  if (outer_nulls_only)
    joined_tuples = 0;  // outer tuples added later
  return joined_tuples;
}

void JoinerHash::SubmitJoinedTuple(int64_t hash_row, MIIterator &mit, MINewContents &new_mind) {
  std::scoped_lock guard(joiner_mutex);
  MEASURE_FET("JoinerHash::SubmitJoinedTuple(...)");
  if (watch_traversed)
    outer_filter->Reset(hash_row);
  // assumption: SetNewTableValue is called once for each dimension involved (no
  // integrity checking)
  if (!outer_nulls_only) {
    for (int i = 0; i < mind->NumOfDimensions(); i++) {
      if (matched_dims[i])
        new_mind.SetNewTableValue(i, mit[i]);
      else if (traversed_dims[i])
        new_mind.SetNewTableValue(i, jhash.GetTupleValue(traversed_hash_column[i], hash_row));
    }
    new_mind.CommitNewTableValues();
  }
}

// outer part

void JoinerHash::InitOuter(Condition &cond) {
  DimensionVector outer_dims(cond[0].right_dims);  // outer_dims will be filled with nulls for
                                                   // non-matching tuples
  if (!outer_dims.IsEmpty()) {
    if (traversed_dims.Includes(outer_dims)) {
      watch_matched = true;  // watch the non-outer dim for unmatched tuples and add them
                             // with nulls on outer dim the filter updated for each matching
      outer_filter.reset(new Filter(mind->NumOfTuples(matched_dims), p_power, true));
    } else if (matched_dims.Includes(outer_dims)) {
      watch_traversed = true;
      outer_filter.reset(new Filter(jhash.NoRows(), p_power));  // the filter reused for each traverse
    }
    outer_nulls_only = true;
    for (int j = 0; j < outer_dims.Size(); j++)
      if (outer_dims[j] && tips.null_only[j] == false)
        outer_nulls_only = false;
  }
}

int64_t JoinerHash::SubmitOuterMatched(MIIterator &mit, MINewContents &new_mind) {
  MEASURE_FET("JoinerHash::SubmitOuterMatched(...)");
  // mit - an iterator through the matched dimensions
  DEBUG_ASSERT(outer_filter && watch_matched);
  if (tips.count_only)
    return outer_filter->NumOfOnes();
  mit.Rewind();
  int64_t matched_row = 0;
  int64_t outer_added = 0;
  while (mit.IsValid()) {
    if (outer_filter->Get(matched_row)) {  // if the matched tuple is still
                                           // unused, add it with nulls
      for (int i = 0; i < mind->NumOfDimensions(); i++) {
        if (matched_dims[i])
          new_mind.SetNewTableValue(i, mit[i]);
        else if (traversed_dims[i])
          new_mind.SetNewTableValue(i, common::NULL_VALUE_64);
      }
      new_mind.CommitNewTableValues();
      outer_added++;
    }
    ++mit;
    matched_row++;
  }
  return outer_added;
}

int64_t JoinerHash::NewMatchDim(MINewContents *new_mind1, MIUpdatingIterator *task_mit1, Transaction *ci,
                                JoinerHashTable *hash_table, int *join_tuple, int tianmu_sysvar_jointhreadpool) {
  JoinerHashTable &tmp_jhash = *hash_table;
  TIANMU_LOG(LogCtl_Level::INFO, "NewMatchDim start, taskid %d, tianmu_sysvar_jointhreadpool %d",
             task_mit1->GetTaskId(), tianmu_sysvar_jointhreadpool);
  common::SetMySQLTHD(ci->Thd());
  current_txn_ = ci;
  for (int i = 0; i < cond_hashed; i++) {
    tmp_jhash.AddKeyColumn(vc1[i], vc2[i]);
  }
  for (int i = 0; i < mind->NumOfDimensions(); i++) {
    if (traversed_dims[i] && !(tips.count_only && !other_cond_exist)) {  // storing tuples may be omitted if
                                                                         // (count_only_now && !other_cond_exist)
      int bin_index_size = 4;
      if (mind->OrigSize(i) > 0x000000007FFFFF00)
        bin_index_size = 8;
      tmp_jhash.AddTupleColumn(bin_index_size);
    }
  }
  bool easy_roughable = false;
  tmp_jhash.Initialize(int64_t(mind->NumOfTuples(traversed_dims) * 1.5), easy_roughable);
  {
    std::scoped_lock guard(joiner_mutex);
    MIIterator tr_mit(mind, traversed_dims);
    while (tr_mit.IsValid()) {
      if (tr_mit.PackrowStarted()) {
        for (int i = 0; i < cond_hashed; i++) {
          vc1[i]->LockSourcePacks(tr_mit);
        }
      }
      bool omit_row = false;
      for (int i = 0; i < cond_hashed; i++) {
        if (vc1[i]->IsNull(tr_mit)) {
          omit_row = true;
          break;
        }
        tmp_jhash.PutKeyValue(i, tr_mit);
      }
      if (!omit_row) {  // else go to the next row - equality cannot be
                        // fulfilled
        int64_t hash_row = tmp_jhash.FindAndAddCurrentRow();
        if (hash_row == common::NULL_VALUE_64)
          break;  // no space left - stop for now and then restart from the
                  // current row
        if (!force_switching_sides && tmp_jhash.TooManyConflicts()) {
          too_many_conflicts = true;
          break;  // and exit the function
        }
        if (!tips.count_only || other_cond_exist) {
          for (int i = 0; i < mind->NumOfDimensions(); i++)
            if (traversed_dims[i])
              tmp_jhash.PutTupleValue(traversed_hash_column[i], hash_row, tr_mit[i]);
        }
      }
      ++tr_mit;
    }
    for (int i = 0; i < cond_hashed; i++) {
      vc1[i]->UnlockSourcePacks();
    }
  }

  MINewContents &new_mind = *new_mind1;
  MIUpdatingIterator &task_mit = *task_mit1;
  int64_t joined_tuples = 0;
  int64_t hash_row;
  int64_t no_of_matching_rows;
  int64_t matching_row = 0;

  MIDummyIterator combined_mit(mind);
  current_txn_ = ci;
  while (task_mit.IsValid()) {
    if (m_conn->Killed())
      throw common::KilledException();
    // Rough and locking part
    bool omit_this_packrow = false;
    bool packrow_uniform = false;  // if the packrow is uniform, process it massively
    if (task_mit.PackrowStarted()) {
      packrow_uniform = true;
      for (int i = 0; i < cond_hashed; i++) {
        if (tmp_jhash.StringEncoder(i)) {
          if (!vc2[i]->Type().Lookup()) {  // lookup treated as string, when the
                                           // dictionaries aren't convertible
            types::BString local_min = vc2[i]->GetMinString(task_mit);
            types::BString local_max = vc2[i]->GetMaxString(task_mit);
            if (!local_min.IsNull() && !local_max.IsNull() && tmp_jhash.ImpossibleValues(i, local_min, local_max)) {
              omit_this_packrow = true;
              TIANMU_LOG(LogCtl_Level::DEBUG, "JoinerHash::test omit");
              break;
            }
          }
          packrow_uniform = false;
        } else {
          int64_t local_min = vc2[i]->GetMinInt64(task_mit);
          int64_t local_max = vc2[i]->GetMaxInt64(task_mit);
          if (local_min == common::NULL_VALUE_64 || local_max == common::NULL_VALUE_64 ||  // common::NULL_VALUE_64
                                                                                           // only for nulls only
              tmp_jhash.ImpossibleValues(i, local_min, local_max)) {
            omit_this_packrow = true;
            break;
          }
          if (other_cond_exist || local_min != local_max || vc2[i]->IsNullsPossible()) {
            packrow_uniform = false;
          }
        }
      }
      packrows_matched++;
      if (packrow_uniform && !omit_this_packrow) {
        for (int i = 0; i < cond_hashed; i++) {
          int64_t local_min = vc2[i]->GetMinInt64(task_mit);
          tmp_jhash.PutMatchedValue(i, local_min);
        }
        no_of_matching_rows = tmp_jhash.InitCurrentRowToGet() * task_mit.GetPackSizeLeft();
        if (!tips.count_only)
          while ((hash_row = tmp_jhash.GetNextRow()) != common::NULL_VALUE_64) {
            MIIterator mit_this_pack(task_mit);
            int64_t matching_this_pack = matching_row;
            do {
              // global_mutex is locked inside SubmitJoinedTuple
              SubmitJoinedTuple(hash_row, mit_this_pack, new_mind);
              if (watch_matched) {
                outer_filter->ResetDelayed(matching_this_pack);
              }
              ++mit_this_pack;
              matching_this_pack++;
            } while (mit_this_pack.IsValid() && !mit_this_pack.PackrowStarted());
          }
        else if (watch_traversed) {
          while ((hash_row = tmp_jhash.GetNextRow()) != common::NULL_VALUE_64) {
            outer_filter->Reset(hash_row);
          }
        }
        if (no_of_matching_rows > 65536 || no_of_matching_rows < 0)
          TIANMU_LOG(LogCtl_Level::DEBUG, "no_of_matching_rows %d", no_of_matching_rows);
        joined_tuples += no_of_matching_rows;
        omit_this_packrow = true;
      }
      if (omit_this_packrow) {
        matching_row += task_mit.GetPackSizeLeft();
        task_mit.NextPackrow();
        packrows_omitted++;
        continue;  // here we are jumping out for impossible or uniform packrow
      }
      // benq: may need to lock here
      if (new_mind.NoMoreTuplesPossible())
        break;  // stop the join if nothing new may be obtained in some
                // optimized cases

      for (int i = 0; i < cond_hashed; i++)
        // benq: may need to lock here
        vc2[i]->LockSourcePacks(task_mit);
    }
    // Exact part - make the key row ready for comparison
    bool null_found = false;
    bool non_matching_sizes = false;
    for (int i = 0; i < cond_hashed; i++) {
      if (vc2[i]->IsNull(task_mit)) {
        null_found = true;
        break;
      }
      tmp_jhash.PutMatchedValue(i, vc2[i], task_mit);
    }
    if (!null_found && !non_matching_sizes) {  // else go to the next row -
                                               // equality cannot be fulfilled
      no_of_matching_rows = tmp_jhash.InitCurrentRowToGet();
      // Find all matching rows
      if (!other_cond_exist) {
        // Basic case - just equalities
        if (!tips.count_only)
          while ((hash_row = tmp_jhash.GetNextRow()) != common::NULL_VALUE_64)
            SubmitJoinedTuple(hash_row, task_mit,
                              new_mind);  // use the multiindex iterator position
        else if (watch_traversed) {
          while ((hash_row = tmp_jhash.GetNextRow()) != common::NULL_VALUE_64) outer_filter->Reset(hash_row);
        }
        if (watch_matched && no_of_matching_rows > 0)
          outer_filter->ResetDelayed(matching_row);
        joined_tuples += no_of_matching_rows;
      } else {
        // Complex case: different types of join conditions mixed together
        combined_mit.Combine(task_mit);
        while ((hash_row = tmp_jhash.GetNextRow()) != common::NULL_VALUE_64) {
          bool other_cond_true = true;
          for (int i = 0; i < mind->NumOfDimensions(); i++) {
            if (traversed_dims[i])
              combined_mit.Set(i, tmp_jhash.GetTupleValue(traversed_hash_column[i], hash_row));
          }
          for (auto &j : other_cond) {
            j.LockSourcePacks(combined_mit);
            if (j.CheckCondition(combined_mit) == false) {
              other_cond_true = false;
              break;
            }
          }
          if (other_cond_true) {
            if (!tips.count_only)
              SubmitJoinedTuple(hash_row, task_mit,
                                new_mind);  // use the multiindex iterator position
            else if (watch_traversed)
              outer_filter->Reset(hash_row);
            joined_tuples++;
            if (watch_matched)
              outer_filter->ResetDelayed(matching_row);
          }
        }
      }
      if (tips.limit != -1 && tips.limit <= joined_tuples)
        break;
    }
    ++task_mit;
    matching_row++;
  }
  if (outer_nulls_only)
    joined_tuples = 0;  // outer tuples added later
  TIANMU_LOG(LogCtl_Level::DEBUG, "JoinerHash::test %d,matching_row %d", joined_tuples, matching_row);
  *join_tuple = joined_tuples;
  return joined_tuples;
}

int64_t JoinerHash::SubmitOuterTraversed(MINewContents &new_mind) {
  MEASURE_FET("JoinerHash::SubmitOuterTraversed(...)");
  DEBUG_ASSERT(outer_filter && watch_traversed);
  if (tips.count_only)
    return outer_filter->NumOfOnes();
  int64_t outer_added = 0;
  for (int64_t hash_row = 0; hash_row < jhash.NoRows(); hash_row++) {
    if (outer_filter->Get(hash_row)) {
      for (int i = 0; i < mind->NumOfDimensions(); i++) {
        if (matched_dims[i])
          new_mind.SetNewTableValue(i, common::NULL_VALUE_64);
        else if (traversed_dims[i])
          new_mind.SetNewTableValue(i, jhash.GetTupleValue(traversed_hash_column[i], hash_row));
      }
      new_mind.CommitNewTableValues();
      outer_added++;
      actually_traversed_rows++;
    }
  }
  return outer_added;
}
}  // namespace core
}  // namespace Tianmu
