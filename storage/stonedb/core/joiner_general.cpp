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

#include "core/descriptor.h"
#include "core/joiner.h"
#include "core/mi_new_contents.h"
#include "core/mi_updating_iterator.h"
#include "core/transaction.h"
#include "vc/virtual_column.h"

namespace stonedb {
namespace core {
void JoinerGeneral::ExecuteJoinConditions(Condition &cond) {
  MEASURE_FET("JoinerGeneral::ExecuteJoinConditions(...)");
  int no_desc = cond.Size();
  DimensionVector all_dims(mind->NoDimensions());  // "false" for all dimensions
  std::vector<bool> pack_desc_locked;
  bool false_desc_found = false;
  bool non_true_desc_found = false;
  for (int i = 0; i < no_desc; i++) {
    if (cond[i].IsFalse()) false_desc_found = true;
    if (!cond[i].IsTrue()) non_true_desc_found = true;
    pack_desc_locked.push_back(false);
    cond[i].DimensionUsed(all_dims);
  }
  if (!non_true_desc_found) return;
  mind->MarkInvolvedDimGroups(all_dims);
  DimensionVector outer_dims(cond[0].right_dims);
  if (!outer_dims.IsEmpty()) {
    // outer_dims will be filled with nulls for non-matching tuples
    all_dims.Plus(outer_dims);
    all_dims.Plus(cond[0].left_dims);
  }
  if (false_desc_found && outer_dims.IsEmpty()) {
    all_dims.Plus(cond[0].left_dims);  // for FALSE join condition
                                       // DimensionUsed() does not mark anything
    for (int i = 0; i < mind->NoDimensions(); i++)
      if (all_dims[i]) mind->Empty(i);
    return;  // all done
  }
  MIIterator mit(mind, all_dims);
  MINewContents new_mind(mind, tips);
  new_mind.SetDimensions(all_dims);
  int64_t approx_size = (tips.limit > -1 ? tips.limit : mit.NoTuples() / 4);
  if (!tips.count_only) new_mind.Init(approx_size);  // an initial size of IndexTable

  int64_t tuples_in_output = 0;
  bool loc_result;
  bool stop_execution = false;  // early stop for LIMIT

  rccontrol.lock(m_conn->GetThreadID()) << "Starting joiner loop (" << mit.NoTuples() << " rows)." << system::unlock;
  // The main loop for checking conditions
  int64_t rows_passed = 0;
  int64_t rows_omitted = 0;

  if (!outer_dims.IsEmpty())
    ExecuteOuterJoinLoop(cond, new_mind, all_dims, outer_dims, tuples_in_output, tips.limit);
  else {
    while (mit.IsValid() && !stop_execution) {
      if (mit.PackrowStarted()) {
        bool omit_this_packrow = false;
        for (int i = 0; (i < no_desc && !omit_this_packrow); i++)
          if (cond[i].EvaluateRoughlyPack(mit) == common::RSValue::RS_NONE) omit_this_packrow = true;
        for (int i = 0; i < no_desc; i++) pack_desc_locked[i] = false;  // delay locking
        if (new_mind.NoMoreTuplesPossible())
          break;  // stop the join if nothing new may be obtained in some
                  // optimized cases
        if (omit_this_packrow) {
          rows_omitted += mit.GetPackSizeLeft();
          rows_passed += mit.GetPackSizeLeft();
          mit.NextPackrow();
          continue;
        }
      }
      loc_result = true;
      for (int i = 0; (i < no_desc && loc_result); i++) {
        if (!pack_desc_locked[i]) {  // delayed locking - maybe will not be
                                     // locked at all?
          cond[i].LockSourcePacks(mit);
          pack_desc_locked[i] = true;
        }
        if (types::RequiresUTFConversions(cond[i].GetCollation())) {
          if (cond[i].CheckCondition_UTF(mit) == false) loc_result = false;
        } else {
          if (cond[i].CheckCondition(mit) == false) loc_result = false;
        }
      }
      if (loc_result) {
        if (!tips.count_only) {
          for (int i = 0; i < mind->NoDimensions(); i++)
            if (all_dims[i]) new_mind.SetNewTableValue(i, mit[i]);
          new_mind.CommitNewTableValues();
        }
        tuples_in_output++;
      }
      ++mit;
      rows_passed++;
      if (m_conn->Killed()) throw common::KilledException();
      if (tips.limit > -1 && tuples_in_output >= tips.limit) stop_execution = true;
    }
  }
  if (rows_passed > 0 && rows_omitted > 0)
    rccontrol.lock(m_conn->GetThreadID())
        << "Roughly omitted " << int(rows_omitted / double(rows_passed) * 1000) / 10.0 << "% rows." << system::unlock;
  // Postprocessing and cleanup
  if (tips.count_only)
    new_mind.CommitCountOnly(tuples_in_output);
  else
    new_mind.Commit(tuples_in_output);
  for (int i = 0; i < no_desc; i++) {
    cond[i].UnlockSourcePacks();
    cond[i].done = true;
  }
  why_failed = JoinFailure::NOT_FAILED;
}

void JoinerGeneral::ExecuteOuterJoinLoop(Condition &cond, MINewContents &new_mind, DimensionVector &all_dims,
                                         DimensionVector &outer_dims, int64_t &tuples_in_output, int64_t output_limit) {
  MEASURE_FET("JoinerGeneral::ExecuteOuterJoinLoop(...)");
  bool stop_execution = false;
  bool loc_result = false;
  bool tuple_used = false;
  bool packrow_started = false;
  int no_desc = cond.Size();
  bool outer_nulls_only = true;  // true => omit all non-null tuples
  for (int j = 0; j < outer_dims.Size(); j++)
    if (outer_dims[j] && tips.null_only[j] == false) outer_nulls_only = false;

  mind->MarkInvolvedDimGroups(outer_dims);
  DimensionVector non_outer_dims(all_dims);
  non_outer_dims.Minus(outer_dims);

  MIIterator nout_mit(mind, non_outer_dims);
  MIIterator out_mit(mind, outer_dims);
  while (nout_mit.IsValid() && !stop_execution) {
    packrow_started = true;
    tuple_used = false;
    out_mit.Rewind();
    MIDummyIterator complex_mit(nout_mit);

    while (out_mit.IsValid() && !stop_execution) {
      if (out_mit.PackrowStarted()) packrow_started = true;
      complex_mit.Combine(out_mit);
      if (packrow_started) {
        for (int i = 0; i < no_desc; i++) cond[i].LockSourcePacks(complex_mit);
        packrow_started = false;
      }
      loc_result = true;
      for (int i = 0; (i < no_desc && loc_result); i++) {
        if (types::RequiresUTFConversions(cond[i].GetCollation())) {
          if (cond[i].CheckCondition_UTF(complex_mit) == false) loc_result = false;
        } else {
          if (cond[i].CheckCondition(complex_mit) == false) loc_result = false;
        }
      }
      if (loc_result) {
        if (!outer_nulls_only) {
          if (!tips.count_only) {
            for (int i = 0; i < mind->NoDimensions(); i++)
              if (all_dims[i]) new_mind.SetNewTableValue(i, complex_mit[i]);
            new_mind.CommitNewTableValues();
          }
          tuples_in_output++;
        }
        tuple_used = true;
      }
      ++out_mit;
      if (m_conn->Killed()) throw common::KilledException();
      if (output_limit > -1 && tuples_in_output >= output_limit) stop_execution = true;
    }
    if (!tuple_used && !stop_execution) {
      if (!tips.count_only) {
        for (int i = 0; i < mind->NoDimensions(); i++) {
          if (non_outer_dims[i])
            new_mind.SetNewTableValue(i, nout_mit[i]);
          else if (outer_dims[i])
            new_mind.SetNewTableValue(i, common::NULL_VALUE_64);
        }
        new_mind.CommitNewTableValues();
      }
      tuples_in_output++;
    }
    ++nout_mit;
  }
}
}  // namespace core
}  // namespace stonedb
