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

/***
 This is a part of TempTable implementation concerned with the query execution
low-level mechanisms
***/

#include "aggregation_algorithm.h"

#include "core/ctask.h"
#include "core/engine.h"
#include "core/mi_iterator.h"
#include "core/pack_guardian.h"
#include "core/transaction.h"
#include "system/fet.h"
#include "system/rc_system.h"

namespace stonedb {
namespace core {
void AggregationAlgorithm::Aggregate(bool just_distinct, int64_t &limit, int64_t &offset, ResultSender *sender) {
  MEASURE_FET("TempTable::Aggregate(...)");
  thd_proc_info(m_conn->Thd(), "aggregation");

  bool group_by_found = false;
  bool has_lookup = false;

  GroupByWrapper gbw(t->NumOfAttrs(), just_distinct, m_conn, t->Getpackpower());
  int64_t upper_approx_of_groups = 1;  // will remain 1 if there is no grouping columns (aggreg. only)
  int64_t min_v = common::MINUS_INF_64;
  int64_t max_v = common::PLUS_INF_64;

  for (uint i = 0; i < t->NumOfAttrs(); i++) {  // first pass: find all grouping attributes
    TempTable::Attr &cur_a = *(t->GetAttrP(i));
    if (cur_a.mode == common::ColOperation::DELAYED)  // delayed column (e.g. complex exp. on
                                                      // aggregations)
      continue;
    if ((just_distinct && cur_a.alias) || cur_a.mode == common::ColOperation::GROUP_BY) {
      if (cur_a.mode == common::ColOperation::GROUP_BY) group_by_found = true;
      bool already_added = false;
      for (uint j = 0; j < i; j++) {
        if (*(t->GetAttrP(j)) == cur_a) {
          already_added = true;
          gbw.DefineAsEquivalent(i, j);
          break;
        }
      }
      if (already_added == false) {
        int new_attr_number = gbw.NoGroupingAttr();
        gbw.AddGroupingColumn(new_attr_number, i,
                              *(t->GetAttrP(i)));  // GetAttrP(i) is needed

        // approximate a number of groups
        if (upper_approx_of_groups < mind->NoTuples()) {
          int64_t dist_vals = gbw.ApproxDistinctVals(new_attr_number, mind);
          upper_approx_of_groups = SafeMultiplication(upper_approx_of_groups, dist_vals);
          if (upper_approx_of_groups == common::NULL_VALUE_64 || upper_approx_of_groups > mind->NoTuples())
            upper_approx_of_groups = mind->NoTuples();
        }
      }
    }
  }

  for (uint i = 0; i < t->NumOfAttrs(); i++) {  // second pass: find all aggregated attributes
    TempTable::Attr &cur_a = *(t->GetAttrP(i));
    if (cur_a.mode == common::ColOperation::DELAYED) {  // delayed column (e.g. complex exp.
                                                        // on aggregations)
      MIDummyIterator m(1);
      cur_a.term.vc->LockSourcePacks(m);
      continue;
    }
    if ((!just_distinct && cur_a.mode != common::ColOperation::GROUP_BY) ||  // aggregation
        (just_distinct && cur_a.alias == NULL)) {                            // special case: hidden column for DISTINCT
      bool already_added = false;
      for (uint j = 0; j < i; j++) {
        if (*(t->GetAttrP(j)) == cur_a) {
          already_added = true;
          gbw.DefineAsEquivalent(i, j);
          break;
        }
      }
      if (already_added) continue;
      int64_t max_no_of_distinct = mind->NoTuples();
      min_v = common::MINUS_INF_64;
      max_v = common::PLUS_INF_64;
      uint max_size = cur_a.Type().GetInternalSize();

      if (cur_a.term.vc) {
        if (!has_lookup) has_lookup = cur_a.term.vc->Type().IsLookup();
        max_size = cur_a.term.vc->MaxStringSize();
        min_v = cur_a.term.vc->RoughMin();
        max_v = cur_a.term.vc->RoughMax();
        if (cur_a.distinct && cur_a.term.vc->IsDistinct() && cur_a.mode != common::ColOperation::LISTING)
          cur_a.distinct = false;  // "distinct" not needed, as values are distinct anyway
        else if (cur_a.distinct) {
          max_no_of_distinct = cur_a.term.vc->GetApproxDistVals(false);  // no nulls included
          if (rccontrol.isOn())
            rccontrol.lock(m_conn->GetThreadID()) << "Adding dist. column, min = " << min_v << ",  max = " << max_v
                                                  << ",  dist = " << max_no_of_distinct << system::unlock;
        }
      }
      if (max_no_of_distinct == 0)
        max_no_of_distinct = 1;  // special case: aggregations on empty result (should not
                                 // be 0, because it triggers max. buffer settings)
      gbw.AddAggregatedColumn(i, cur_a, max_no_of_distinct, min_v, max_v, max_size);
    }
  }

  t->SetAsMaterialized();
  t->SetNumOfMaterialized(0);
  if ((just_distinct || group_by_found) && mind->ZeroTuples()) return;

  bool limit_less_than_no_groups = false;
  // Optimization for cases when limit is much less than a number of groups (but
  // this optimization disables multithreading)
  if (limit != -1 && upper_approx_of_groups / 10 > offset + limit &&
      !(t->HasHavingConditions())) {  // HAVING should disable this optimization
    upper_approx_of_groups = offset + limit;
    limit_less_than_no_groups = true;
  }

  gbw.Initialize(upper_approx_of_groups);
  if ((gbw.IsCountOnly() || gbw.IsCountDistinctOnly()) && (offset > 0 || limit == 0)) {
    --offset;
    return;  // one row, already omitted
  }

  // TODO: do all these special cases here in one loop, and left the unresolved
  // aggregations for normal run Special cases: SELECT COUNT(*) FROM ..., SELECT
  // 1, 2, MIN(a), SUM(b) WHERE false
  bool all_done_in_one_row = false;
  int64_t row = 0;
  if (gbw.IsCountOnly() || mind->ZeroTuples()) {
    DimensionVector dims(mind->NoDimensions());
    dims.SetAll();
    MIIterator mit(mind, dims);
    gbw.AddAllGroupingConstants(mit);
    gbw.FindCurrentRow(row);  // needed to initialize grouping buffer
    gbw.AddAllAggregatedConstants(mit);
    gbw.AddAllCountStar(row, mit, mind->NoTuples());
    all_done_in_one_row = true;
  }  // Special case 2, if applicable: SELECT COUNT(DISTINCT col) FROM .....;
  else if (gbw.IsCountDistinctOnly()) {
    int64_t count_distinct = t->GetAttrP(0)->term.vc->GetExactDistVals();  // multiindex checked inside
    if (count_distinct != common::NULL_VALUE_64) {
      int64_t row = 0;
      gbw.FindCurrentRow(row);  // needed to initialize grouping buffer
      gbw.PutAggregatedValueForCount(0, row, count_distinct);
      all_done_in_one_row = true;
    }
  }  // Special case 3: SELECT MIN(col) FROM ..... or SELECT MAX(col) FROM
     // .....;
  else if (t->GetWhereConds().Size() == 0 && ((gbw.IsMinOnly() && t->NumOfAttrs() == 1 && min_v != common::MINUS_INF_64) ||
                                              (gbw.IsMaxOnly() && t->NumOfAttrs() == 1 && max_v != common::PLUS_INF_64))) {
    if (stonedb_sysvar_minmax_speedup && !has_lookup) {
      int64_t value = min_v;
      if (gbw.IsMaxOnly()) value = max_v;
      gbw.FindCurrentRow(row);  // needed to initialize grouping buffer
      gbw.PutAggregatedValueForMinMax(0, row, value);
      all_done_in_one_row = true;
    }
  }

  if (all_done_in_one_row) {
    for (uint i = 0; i < t->NumOfAttrs(); i++) {  // left as uninitialized (NULL or 0)
      t->GetAttrP(i)->page_size = 1;
      t->GetAttrP(i)->CreateBuffer(1);
    }
    if (limit == -1 || (offset == 0 && limit >= 1)) {  // limit is -1 (off), or a positive
                                                       // number, 0 means nothing should be
                                                       // displayed.
      --limit;
      AggregateFillOutput(gbw, row, offset);
      if (sender) {
        sender->SetAffectRows(t->NumOfObj());
        TempTable::RecordIterator iter = t->begin();
        sender->Send(iter);
      }
    }
  } else {
    int64_t local_limit = limit == -1 ? upper_approx_of_groups : limit;
    MultiDimensionalGroupByScan(gbw, local_limit, offset, sender, limit_less_than_no_groups);
    if (limit != -1) limit = local_limit;
  }
  t->ClearMultiIndexP();  // cleanup (i.e. regarded as materialized,
                          // one-dimensional)
  if (t->HasHavingConditions())
    t->ClearHavingConditions();  // to prevent another execution of HAVING on
                                 // DISTINCT+GROUP BY
}

void AggregationAlgorithm::MultiDimensionalGroupByScan(GroupByWrapper &gbw, int64_t &limit, int64_t &offset,
                                                       ResultSender *sender, bool limit_less_than_no_groups) {
  MEASURE_FET("TempTable::MultiDimensionalGroupByScan(...)");
  bool first_pass = true;
  // tuples are numbered according to tuple_left filter (not used, if tuple_left
  // is null)
  int64_t cur_tuple = 0;
  int64_t displayed_no_groups = 0;

  // Determine dimensions to be iterated
  bool no_dims_found = true;
  DimensionVector dims(mind->NoDimensions());
  gbw.FillDimsUsed(dims);
  for (int i = 0; i < mind->NoDimensions(); i++)
    if (dims[i]) {
      no_dims_found = false;
      break;
    }
  if (no_dims_found) dims[0] = true;  // at least one dimension is needed

  std::vector<PackOrderer> po(mind->NoDimensions());
  MIIterator mit(mind, dims, po);

  factor = mit.Factor();
  if (mit.NoTuples() == common::NULL_VALUE_64 ||
      mit.NoTuples() > common::MAX_ROW_NUMBER) {  // 2^47, a limit for filter below
    throw common::OutOfMemoryException("Aggregation is too large.");
  }
  gbw.SetDistinctTuples(mit.NoTuples());

  int thd_cnt = 1;
  if (ParallelAllowed(gbw) && !limit_less_than_no_groups) {
    thd_cnt = std::thread::hardware_concurrency() / 4;  // For concurrence reason, don't swallow all cores once.
  }

  AggregationWorkerEnt ag_worker(gbw, mind, thd_cnt, this);

  if (!gbw.IsOnePass()) gbw.InitTupleLeft(mit.NoTuples());
  bool rewind_needed = false;
  try {
    do {
      if (rccontrol.isOn()) {
        if (gbw.UpperApproxOfGroups() == 1 || first_pass)
          rccontrol.lock(m_conn->GetThreadID())
              << "Aggregating: " << mit.NoTuples() << " tuples left." << system::unlock;
        else
          rccontrol.lock(m_conn->GetThreadID()) << "Aggregating: " << gbw.TuplesNoOnes() << " tuples left, "
                                                << displayed_no_groups << " gr. found so far" << system::unlock;
      }
      cur_tuple = 0;
      gbw.ClearNoGroups();         // count groups locally created in this pass
      gbw.ClearDistinctBuffers();  // reset buffers for a new contents
      gbw.AddAllGroupingConstants(mit);
      ag_worker.Init(mit);
      if (rewind_needed)
        mit.Rewind();  // aggregated rows will be massively omitted packrow by
                       // packrow
      rewind_needed = true;
      for (uint i = 0; i < t->NumOfAttrs(); i++) {  // left as uninitialized (NULL or 0)
        if (t->GetAttrP(i)->mode == common::ColOperation::DELAYED) {
          MIDummyIterator m(1);
          t->GetAttrP(i)->term.vc->LockSourcePacks(m);
        }
      }
      if (ag_worker.ThreadsUsed() > 1) {
        ag_worker.DistributeAggreTaskAverage(mit);
      } else {
        while (mit.IsValid()) {  // need muti thread
                                 // First stage -
                                 //  some distincts may be delayed
          if (m_conn->Killed()) throw common::KilledException();

          // Grouping on a packrow
          int64_t packrow_length = mit.GetPackSizeLeft();
          int grouping_result = AggregatePackrow(gbw, &mit, cur_tuple);
          if (sender) {
            sender->SetAffectRows(gbw.NoGroups());
          }
          if (grouping_result == 2) throw common::KilledException();
          if (grouping_result != 5) packrows_found++;  // for statistics
          if (grouping_result == 1) break;             // end of the aggregation
          if (!gbw.IsFull() && gbw.MemoryBlocksLeft() == 0) {
            gbw.SetAsFull();
          }
          cur_tuple += packrow_length;
        }
      }
      gbw.ClearDistinctBuffers();              // reset buffers for a new contents
      MultiDimensionalDistinctScan(gbw, mit);  // if not needed, no effect
      ag_worker.Commit();

      // Now it is time to prepare output values
      if (first_pass) {
        first_pass = false;
        int64_t upper_groups = gbw.NoGroups() + gbw.TuplesNoOnes();  // upper approximation: the current size +
                                                                     // all other possible rows (if any)
        t->CalculatePageSize(upper_groups);
        if (upper_groups > gbw.UpperApproxOfGroups())
          upper_groups = gbw.UpperApproxOfGroups();  // another upper limitation: not more
                                                     // than theoretical number of
                                                     // combinations

        MIDummyIterator m(1);
        for (uint i = 0; i < t->NumOfAttrs(); i++) {
          if (t->GetAttrP(i)->mode == common::ColOperation::GROUP_CONCAT) {
            t->GetAttrP(i)->SetTypeName(common::CT::VARCHAR);
            t->GetAttrP(i)->OverrideStringSize(stonedb_group_concat_max_len);
          }
          t->GetAttrP(i)->CreateBuffer(upper_groups);  // note: may be more than needed
          if (t->GetAttrP(i)->mode == common::ColOperation::DELAYED) t->GetAttrP(i)->term.vc->LockSourcePacks(m);
        }
      }
      rccontrol.lock(m_conn->GetThreadID()) << "Group/Aggregate end. Begin generating output." << system::unlock;
      rccontrol.lock(m_conn->GetThreadID()) << "Output rows: " << gbw.NoGroups() + gbw.TuplesNoOnes()
                                            << ", output table row limit: " << t->GetPageSize() << system::unlock;
      int64_t output_size = (gbw.NoGroups() + gbw.TuplesNoOnes()) * t->GetOneOutputRecordSize();
      gbw.RewindRows();
      if (t->GetPageSize() >= (gbw.NoGroups() + gbw.TuplesNoOnes()) && output_size > (1L << 29) &&
          !t->HasHavingConditions() && stonedb_sysvar_parallel_filloutput) {
        // Turn on parallel output when:
        // 1. output page is large enough to hold all output rows
        // 2. output result is larger than 512MB
        // 3. no have condition
        rccontrol.lock(m_conn->GetThreadID()) << "Start parallel output" << system::unlock;
        ParallelFillOutputWrapper(gbw, offset, limit, mit);
      } else {
        while (gbw.RowValid()) {
          // copy GroupTable into TempTable, row by row
          if (t->NumOfObj() >= limit) break;
          AggregateFillOutput(gbw, gbw.GetCurrentRow(),
                              offset);  // offset is decremented for each row, if positive
          if (sender && t->NumOfObj() > (1 << mind->NoPower()) - 1) {
            TempTable::RecordIterator iter = t->begin();
            for (int64_t i = 0; i < t->NumOfObj(); i++) {
              sender->Send(iter);
              ++iter;
            }
            displayed_no_groups += t->NumOfObj();
            limit -= t->NumOfObj();
            t->SetNumOfObj(0);
          }
          gbw.NextRow();
        }
      }
      if (sender) {
        TempTable::RecordIterator iter = t->begin();
        for (int64_t i = 0; i < t->NumOfObj(); i++) {
          sender->Send(iter);
          ++iter;
        }
        displayed_no_groups += t->NumOfObj();
        limit -= t->NumOfObj();
        t->SetNumOfObj(0);
      } else
        displayed_no_groups = t->NumOfObj();
      if (t->NumOfObj() >= limit) break;
      if (gbw.AnyTuplesLeft()) gbw.ClearUsed();  // prepare for the next pass, if needed
    } while (gbw.AnyTuplesLeft());               // do the next pass, if anything left
  } catch (...) {
    ag_worker.Commit(false);
    throw;
  }
  if (rccontrol.isOn())
    rccontrol.lock(m_conn->GetThreadID())
        << "Generating output end. "
        << "Aggregated (" << displayed_no_groups << " group). Omitted packrows: " << gbw.packrows_omitted << " + "
        << gbw.packrows_part_omitted << " partially, out of " << packrows_found << " total." << system::unlock;
}

void AggregationAlgorithm::MultiDimensionalDistinctScan(GroupByWrapper &gbw, MIIterator &mit) {
  // NOTE: to maintain distinct cache compatibility, rows must be visited in the
  // same order!
  MEASURE_FET("TempTable::MultiDimensionalDistinctScan(GroupByWrapper& gbw)");
  while (gbw.AnyOmittedByDistinct()) {  // any distincts omitted? =>
                                        // another pass needed
    // Some displays
    int64_t max_size_for_display = 0;
    for (int i = gbw.NoGroupingAttr(); i < gbw.NoAttr(); i++)
      if (gbw.distinct_watch.OmittedFilter(i) && gbw.distinct_watch.OmittedFilter(i)->NoOnes() > max_size_for_display)
        max_size_for_display = gbw.distinct_watch.OmittedFilter(i)->NoOnes();
    rccontrol.lock(m_conn->GetThreadID())
        << "Next distinct pass: " << max_size_for_display << " rows left" << system::unlock;

    gbw.RewindDistinctBuffers();  // reset buffers for a new contents, rewind
                                  // cache
    for (int distinct_attr = gbw.NoGroupingAttr(); distinct_attr < gbw.NoAttr(); distinct_attr++) {
      Filter *omit_filter = gbw.distinct_watch.OmittedFilter(distinct_attr);
      if (omit_filter && !omit_filter->IsEmpty()) {
        mit.Rewind();
        int64_t cur_tuple = 0;
        int64_t uniform_pos = common::NULL_VALUE_64;
        bool require_locking = true;
        while (mit.IsValid()) {
          if (mit.PackrowStarted()) {
            if (m_conn->Killed()) throw common::KilledException();
            // All packrow-level operations
            omit_filter->Commit();
            gbw.ResetPackrow();
            bool skip_packrow = false;
            bool packrow_done = false;
            bool part_omitted = false;
            bool stop_all = false;
            int64_t packrow_length = mit.GetPackSizeLeft();
            // Check whether the packrow contain any not aggregated rows
            if (omit_filter->IsEmptyBetween(cur_tuple, cur_tuple + packrow_length - 1))
              skip_packrow = true;
            else {
              int64_t rows_in_pack = omit_filter->NoOnesBetween(cur_tuple, cur_tuple + packrow_length - 1);
              bool agg_not_changeable = false;
              AggregateRough(gbw, mit, packrow_done, part_omitted, agg_not_changeable, stop_all, uniform_pos,
                             rows_in_pack, 1, distinct_attr);
              if (packrow_done) {  // This packrow will not be needed any more
                omit_filter->ResetBetween(cur_tuple, cur_tuple + packrow_length - 1);
                gbw.OmitInCache(distinct_attr, packrow_length);
              }
            }
            if (skip_packrow) {
              mit.NextPackrow();
              cur_tuple += packrow_length;
              continue;
            }
            require_locking = true;  // a new packrow, so locking will be needed
          }

          // All row-level operations
          if (omit_filter->Get(cur_tuple)) {
            bool value_successfully_aggregated = false;
            if (gbw.CacheValid(distinct_attr)) {
              value_successfully_aggregated = gbw.PutCachedValue(distinct_attr);
            } else {
              // Locking etc.
              if (require_locking) {
                gbw.LockPack(distinct_attr, mit);
                if (uniform_pos != common::PLUS_INF_64)
                  for (int gr_a = 0; gr_a < gbw.NoGroupingAttr(); gr_a++) gbw.LockPack(gr_a, mit);
                require_locking = false;
              }

              int64_t pos = 0;
              bool existed = true;
              if (uniform_pos != common::PLUS_INF_64)
                pos = uniform_pos;  // existed == true, as above
              else {                // Construct the grouping vector
                for (int gr_a = 0; gr_a < gbw.NoGroupingAttr(); gr_a++) {
                  if (gbw.ColumnNotOmitted(gr_a)) gbw.PutGroupingValue(gr_a, mit);
                }
                existed = gbw.FindCurrentRow(pos);
              }
              ASSERT(existed && pos != common::NULL_VALUE_64, "row does not exist");
              value_successfully_aggregated = gbw.PutAggregatedValue(distinct_attr, pos, mit);
            }
            if (value_successfully_aggregated) omit_filter->ResetDelayed(cur_tuple);
            gbw.distinct_watch.NextRead(distinct_attr);
          }
          cur_tuple++;
          ++mit;
        }
        omit_filter->Commit();  // committing delayed resets
      }
    }
    gbw.UpdateDistinctCaches();  // take into account values already counted
  }
}

int AggregationAlgorithm::AggregatePackrow(GroupByWrapper &gbw, MIIterator *mit, int64_t cur_tuple) {
  int64_t packrow_length = mit->GetPackSizeLeft();
  if (!gbw.AnyTuplesLeft(cur_tuple, cur_tuple + packrow_length - 1)) {
    mit->NextPackrow();
    return 5;
  }
  int64_t uniform_pos = common::NULL_VALUE_64;
  bool skip_packrow = false;
  bool packrow_done = false;
  bool part_omitted = false;
  bool stop_all = false;
  bool aggregations_not_changeable = false;

  bool require_locking_ag = true;  // a new packrow, so locking will be needed
  bool require_locking_gr = true;  // do not lock if the grouping row is uniform

  if (require_locking_gr) {
    for (int gr_a = 0; gr_a < gbw.NoGroupingAttr(); gr_a++)
      gbw.LockPackAlways(gr_a, *mit);  // note: ColumnNotOmitted checked
                                       // inside//»á¼ÓÔØ½âÑ¹group byÁÐÊý¾Ý°ü
    require_locking_gr = false;
  }
  if (require_locking_ag) {
    for (int gr_a = gbw.NoGroupingAttr(); gr_a < gbw.NoAttr(); gr_a++)
      gbw.LockPackAlways(gr_a, *mit);  // note: ColumnNotOmitted checked inside
    require_locking_ag = false;
  }

  gbw.ResetPackrow();
  int64_t rows_in_pack = gbw.TuplesLeftBetween(cur_tuple, cur_tuple + packrow_length - 1);
  DEBUG_ASSERT(rows_in_pack > 0);

  skip_packrow = AggregateRough(gbw, *mit, packrow_done, part_omitted, aggregations_not_changeable, stop_all,
                                uniform_pos, rows_in_pack, factor);
  if (t->NumOfObj() + gbw.NoGroups() == gbw.UpperApproxOfGroups()) {  // no more groups!
    gbw.SetNoMoreGroups();
    if (skip_packrow)  // no aggr. changeable and no new groups possible?
      packrow_done = true;
    if (gbw.NoGroupingAttr() == gbw.NoAttr()  // just DISTINCT without grouping
        || stop_all) {                        // or aggregation already done on rough level
      gbw.TuplesResetAll();                   // no more rows needed, just produce output
      return 1;                               // aggregation finished
    }
  }
  if (skip_packrow)
    gbw.packrows_omitted++;
  else if (part_omitted)
    gbw.packrows_part_omitted++;
  if (packrow_done) {  // This packrow will not be needed any more
    gbw.TuplesResetBetween(cur_tuple, cur_tuple + packrow_length - 1);
  }

  if (packrow_done || skip_packrow) {
    mit->NextPackrow();
    return 0;  // success - roughly omitted
  }

  // bool require_locking_ag  = true;					// a new packrow,
  // so locking will be needed bool require_locking_gr	= (uniform_pos ==
  // common::NULL_VALUE_64);	// do not lock if the grouping row is uniform

  while (mit->IsValid()) {           // becomes invalid on pack end
    if (m_conn->Killed()) return 2;  // killed
    if (gbw.TuplesGet(cur_tuple)) {
      if (require_locking_gr) {
        for (int gr_a = 0; gr_a < gbw.NoGroupingAttr(); gr_a++)
          gbw.LockPack(gr_a, *mit);  // note: ColumnNotOmitted checked inside
        require_locking_gr = false;
      }

      int64_t pos = 0;
      bool existed = true;
      if (uniform_pos != common::NULL_VALUE_64)  // either uniform because of KNs, or = 0,
                                                 // because there is no grouping columns
        pos = uniform_pos;                       // existed == true, as above
      else {
        for (int gr_a = 0; gr_a < gbw.NoGroupingAttr(); gr_a++)
          if (gbw.ColumnNotOmitted(gr_a)) gbw.PutGroupingValue(gr_a, *mit);
        existed = gbw.FindCurrentRow(pos);
      }

      if (pos != common::NULL_VALUE_64) {  // Any place left? If not, just omit
                                           // the tuple.
        gbw.TuplesReset(cur_tuple);        // internally delayed for optimization
                                           // purposes - must be committed at the end
        if (!existed) {
          aggregations_not_changeable = false;
          gbw.AddGroup();                                                  // successfully added
          if (t->NumOfObj() + gbw.NoGroups() == gbw.UpperApproxOfGroups()) {  // no more groups!
            gbw.SetNoMoreGroups();
            if (gbw.NoGroupingAttr() == gbw.NoAttr()) {  // just DISTINCT without grouping
              gbw.TuplesResetAll();                      // no more rows needed, just produce output
              return 1;                                  // aggregation finished
            }
          }
        }
        if (!aggregations_not_changeable) {
          // Lock packs if needed
          if (require_locking_ag) {
            for (int gr_a = gbw.NoGroupingAttr(); gr_a < gbw.NoAttr(); gr_a++)
              gbw.LockPack(gr_a,
                           *mit);  // note: ColumnNotOmitted checked inside
            require_locking_ag = false;
          }

          // Prepare packs for aggregated columns
          for (int gr_a = gbw.NoGroupingAttr(); gr_a < gbw.NoAttr(); gr_a++)
            if (gbw.ColumnNotOmitted(gr_a)) {
              bool value_successfully_aggregated = gbw.PutAggregatedValue(gr_a, pos, *mit, factor);
              if (!value_successfully_aggregated) gbw.DistinctlyOmitted(gr_a, cur_tuple);
            }
        }
      }
    }
    cur_tuple++;
    mit->Increment();
    if (mit->PackrowStarted()) break;
  }
  gbw.CommitResets();
  return 0;  // success
}

void AggregationAlgorithm::AggregateFillOutput(GroupByWrapper &gbw, int64_t gt_pos, int64_t &omit_by_offset) {
  MEASURE_FET("TempTable::AggregateFillOutput(...)");
  // OFFSET without HAVING
  if (!(t->HasHavingConditions()) && omit_by_offset > 0) {  // note that the rows not meeting conditions should
                                                            // not count in offset
    omit_by_offset--;
    return;
  }
  int64_t cur_output_tuple;
  {
    std::scoped_lock guard(mtx);
    cur_output_tuple = t->NumOfObj();
    t->SetNumOfMaterialized(cur_output_tuple + 1);  // needed to allow value reading from this TempTable
  }

  // Fill aggregations and grouping columns
  for (uint i = 0; i < t->NumOfAttrs(); i++) {
    TempTable::Attr *a = t->GetAttrP(i);  // change to pointer - for speed
    int gt_column = gbw.AttrMapping(i);
    if (gt_column == -1)  // delayed column (e.g. complex exp. on aggregations)
      continue;
    if (ATI::IsStringType(a->TypeName())) {
      a->SetValueString(cur_output_tuple, gbw.GetValueT(gt_column, gt_pos));
    } else {
      int64_t v = gbw.GetValue64(gt_column, gt_pos);
      a->SetValueInt64(cur_output_tuple, v);
    }
  }

  // Materialize delayed attrs (e.g. expressions on aggregation results)
  MIDummyIterator it(1);  // one-dimensional dummy iterator to iterate the result
  it.Set(0, cur_output_tuple);
  types::BString vals;
  for (uint i = 0; i < t->NumOfAttrs(); i++) {
    TempTable::Attr *a = t->GetAttrP(i);
    if (a->mode != common::ColOperation::DELAYED) continue;
    vcolumn::VirtualColumn *vc = a->term.vc;
    switch (a->TypeName()) {
      case common::CT::STRING:
      case common::CT::VARCHAR:
        vc->GetValueString(vals, it);
        a->SetValueString(cur_output_tuple, vals);
        break;
      case common::CT::BIN:
      case common::CT::BYTE:
      case common::CT::VARBYTE:
      case common::CT::LONGTEXT:
        if (!vc->IsNull(it))
          vc->GetNotNullValueString(vals, it);
        else
          vals = types::BString();
        a->SetValueString(cur_output_tuple, vals);
        break;
      default:
        a->SetValueInt64(cur_output_tuple, vc->GetValueInt64(it));
        break;
    }
  }

  // HAVING
  if (t->HasHavingConditions()) {
    if (!(t->CheckHavingConditions(it))) {     // condition not met - forget about this row (will be
                                               // overwritten soon)
      t->SetNumOfMaterialized(cur_output_tuple);  // i.e. no_obj--;
      for (uint i = 0; i < t->NumOfAttrs(); i++)
        t->GetAttrP(i)->InvalidateRow(cur_output_tuple);  // change to pointer - for speed
    } else {
      // OFFSET with HAVING
      if (omit_by_offset > 0) {
        omit_by_offset--;
        t->SetNumOfMaterialized(cur_output_tuple);  // i.e. no_obj--;
      }
    }
  }
}

bool AggregationAlgorithm::AggregateRough(GroupByWrapper &gbw, MIIterator &mit, bool &packrow_done, bool &part_omitted,
                                          bool &aggregations_not_changeable, bool &stop_all, int64_t &uniform_pos,
                                          int64_t rows_in_pack, int64_t local_factor, int just_one_aggr) {
  MEASURE_FET("TempTable::AggregateRough(...)");
  // Return situations:
  // a) ignore the packrow and check it in future (next grouping pass) =>
  // return true; / b) ignore the packrow forever =>  packrow_done = true;
  // return true; / c) ignore just some columns => gbw.OmitColumnForPackrow(i);
  // return false;
  // If just_one_aggr > -1, then it is just for one aggregation column, plus
  // all grouping
  // columns (next distinct pass)
  packrow_done = false;
  part_omitted = false;
  aggregations_not_changeable = false;
  stop_all = false;
  bool grouping_packrow_uniform = true;
  for (int i = 0; i < gbw.NoGroupingAttr(); i++) {  // first test: whether the grouping values are not all uniform
    if (gbw.AddPackIfUniform(i, mit)) {
      gbw.OmitColumnForPackrow(i);  // rowpack may be partially uniform;
                                    // grouping value already in buffer
      part_omitted = true;
    } else
      grouping_packrow_uniform = false;
  }
  uniform_pos = common::NULL_VALUE_64;               // not changed => no uniform packrow
  if (grouping_packrow_uniform) {                    // the whole packrow is defined by uniform
                                                     // values
    bool existed = gbw.FindCurrentRow(uniform_pos);  // the row is fully prepared
    if (uniform_pos != common::NULL_VALUE_64) {      // Successfully found? The whole packrow goes
                                                     // to one group.
      if (!existed) {
        gbw.AddGroup();  // successfully added
        gbw.InvalidateAggregationStatistics();
      }
      packrow_done = true;
      for (int i = gbw.NoGroupingAttr(); i < gbw.NoAttr(); i++)
        if (just_one_aggr == -1 || just_one_aggr == i) {
          if (gbw.AggregatePackInOneGroup(i, mit, uniform_pos, rows_in_pack, local_factor)) {
            gbw.OmitColumnForPackrow(i);  // the column is marked as already done
            part_omitted = true;
          } else
            packrow_done = false;
        }
      if (packrow_done) return true;  // pack done, get the next one
    } else
      return true;  // no space to add uniform values in this pass, omit it for
                    // now
  }
  DEBUG_ASSERT(!packrow_done);

  // the next step: check if anything may be changed (when no new groups can be
  // added))
  bool all_done = false;

  if (gbw.IsFull()) {
    bool any_gr_column_left = false;  // no gr. column left => all uniform, do not ignore them
    for (int i = 0; i < gbw.NoGroupingAttr(); i++)
      if (gbw.ColumnNotOmitted(i)) {
        any_gr_column_left = true;
        if (!gbw.AttrMayBeUpdatedByPack(i, mit)) {  // grouping values out of scope?
          all_done = true;
          break;
        }
      }
    if (all_done && any_gr_column_left)
      return true;  // packrow is done for now (to be checked in the future),
                    // get the next one

    all_done = true;
    if (gbw.NoGroupingAttr() == gbw.NoAttr())  // no aggregations, i.e. select
                                               // distinct ... - cannot omit
      all_done = false;
  }

  aggregations_not_changeable = true;  // note that it is for current groups only, the flag will
                                       // be ignored if any new group is found
  for (int i = gbw.NoGroupingAttr(); i < gbw.NoAttr(); i++)
    if (just_one_aggr == -1 || just_one_aggr == i) {
      if (gbw.ColumnNotOmitted(i)) {
        if (gbw.PackWillNotUpdateAggregation(i, mit)) {
          if (gbw.IsFull()) {
            gbw.OmitColumnForPackrow(i);
            part_omitted = true;
          }
        } else
          aggregations_not_changeable = false;
      }
      if (gbw.ColumnNotOmitted(i))
        all_done = false;  // i.e. there is something to be done in case of
                           // existing grouping values
    }

  // note that we may need to open the packrow to localize groups for future
  // scans, except:
  if (all_done && gbw.IsFull() && gbw.NoMoreGroups()) {  // no aggregation may be changed by this packrow?
    // anything may be changed by the end of data?
    stop_all = true;
    for (int i = gbw.NoGroupingAttr(); i < gbw.NoAttr(); i++)
      if (just_one_aggr == -1 || just_one_aggr == i)
        if (!gbw.DataWillNotUpdateAggregation(i)) stop_all = false;

    packrow_done = true;
    return true;  // packrow is excluded from the search, get the next one
  }
  return all_done;
}

void AggregationAlgorithm::ParallelFillOutputWrapper(GroupByWrapper &gbw, int64_t offset, int64_t limit,
                                                     [[maybe_unused]] MIIterator &mit) {
  // Prepare the data
  int64_t no_rows = gbw.GetRowsNo();

  int thd_cnt = 10;
  std::vector<GroupByWrapper> vgbw;
  Transaction *conn = current_tx;
  vgbw.reserve(thd_cnt);

  for (int i = 0; i < thd_cnt; i++) {
    GroupByWrapper gbwtmp(gbw);
    // Rewind the wrapper
    gbwtmp.SetCurrentRow(no_rows * i / thd_cnt);
    if (i != thd_cnt - 1) gbwtmp.SetEndRow(no_rows * (i + 1) / thd_cnt);

    vgbw.push_back(gbwtmp);
  }

  utils::result_set<void> res;
  for (auto &gb : vgbw) {
    res.insert(
        rceng->query_thread_pool.add_task(&AggregationAlgorithm::TaskFillOutput, this, &gb, conn, offset, limit));
  }
  res.get_all_with_except();
}

void AggregationAlgorithm::TaskFillOutput(GroupByWrapper *gbw, Transaction *ci, int64_t offset, int64_t limit) {
  common::SetMySQLTHD(ci->Thd());
  current_tx = ci;
  while (gbw->RowValid()) {
    if (t->NumOfObj() >= limit) {
      break;
    }
    AggregateFillOutput(*gbw, gbw->GetCurrentRow(), offset);
    gbw->NextRow();
  }
}

void AggregationWorkerEnt::TaskAggrePacks(MIUpdatingIterator *taskIterator, [[maybe_unused]] DimensionVector *dims,
                                          [[maybe_unused]] MIIterator *mit, [[maybe_unused]] int pstart,
                                          [[maybe_unused]] int pend, int tuple, GroupByWrapper *gbw, Transaction *ci) {
  int i = 0;
  int64_t cur_tuple = tuple;
  common::SetMySQLTHD(ci->Thd());
  current_tx = ci;
  while (taskIterator->IsValid()) {
    int64_t packrow_length = taskIterator->GetPackSizeLeft();
    int grouping_result = aa->AggregatePackrow(*gbw, taskIterator, cur_tuple);
    if (grouping_result != 5) i++;
    if (grouping_result == 1) break;
    if (grouping_result == 2) throw common::KilledException();
    if (grouping_result == 3 || grouping_result == 4) throw common::NotImplementedException("Aggregation overflow.");
    cur_tuple += packrow_length;
  }
  STONEDB_LOG(LogCtl_Level::DEBUG, "TaskAggrePacks routine ends. Task id %d", taskIterator->GetTaskNum());
}

void AggregationWorkerEnt::PrepShardingCopy(MIIterator *mit, GroupByWrapper *gb_sharding,
                                            std::vector<std::unique_ptr<GroupByWrapper>> *vGBW) {
  DimensionVector dims(mind->NoDimensions());
  std::unique_ptr<GroupByWrapper> gbw_ptr(new GroupByWrapper(*gb_sharding));
  gbw_ptr->FillDimsUsed(dims);
  gbw_ptr->SetDistinctTuples(mit->NoTuples());
  if (!gbw_ptr->IsOnePass()) gbw_ptr->InitTupleLeft(mit->NoTuples());
  gbw_ptr->AddAllGroupingConstants(*mit);
  std::scoped_lock guard(mtx);
  vGBW->emplace_back(std::move(gbw_ptr));
}

/*Average allocation task*/
void AggregationWorkerEnt::DistributeAggreTaskAverage(MIIterator &mit) {
  Transaction *conn = current_tx;
  DimensionVector dims(mind->NoDimensions());
  std::vector<CTask> vTask;
  std::vector<std::unique_ptr<GroupByWrapper>> vGBW;
  vGBW.reserve(m_threads);
  vTask.reserve(m_threads);
  if (rccontrol.isOn()) rccontrol.lock(conn->GetThreadID()) << "Prepare data for parallel aggreation" << system::unlock;

  int packnum = 0;
  while (mit.IsValid()) {
    packnum++;
    mit.NextPackrow();
  }

  int loopcnt = (packnum < m_threads) ? packnum : m_threads;

  int mod = packnum % loopcnt;
  int num = packnum / loopcnt;
  utils::result_set<void> res;
  for (int i = 0; i < loopcnt; ++i) {
    res.insert(rceng->query_thread_pool.add_task(&AggregationWorkerEnt::PrepShardingCopy, this, &mit, gb_main, &vGBW));
    CTask tmp;
    tmp.dwTaskId = i;
    tmp.dwPackNum = num + mod + i * num;
    vTask.push_back(tmp);
  }
  res.get_all_with_except();

  if (rccontrol.isOn())
    rccontrol.lock(conn->GetThreadID()) << "Prepare data for parallel aggreation done. Total packnum " << packnum
                                        << system::unlock;
  packnum = 0;
  mit.Rewind();
  int curtuple = 0;
  while (mit.IsValid()) {
    int64_t packrow_length = mit.GetPackSizeLeft();
    packnum++;
    curtuple += packrow_length;
    for (auto &it : vTask) {
      if (packnum == it.dwPackNum) {
        it.dwEndPackno = mit.GetCurPackrow(0);
        it.dwTuple = curtuple;
      }
    }

    if (rccontrol.isOn())
      rccontrol.lock(conn->GetThreadID()) << " GetCurPackrow: " << mit.GetCurPackrow(0) << " packnum: " << packnum
                                          << " cur_tuple: " << curtuple << system::unlock;

    mit.NextPackrow();
  }

  std::vector<MultiIndex> mis;
  mis.reserve(vTask.size());

  std::vector<MIUpdatingIterator> taskIterator;
  taskIterator.reserve(vTask.size());

  utils::result_set<void> res1;
  for (uint i = 0; i < vTask.size(); ++i) {
    auto &mi = mis.emplace_back(*mind, true);
    if (dims.NoDimsUsed() == 0) dims.SetAll();
    auto &mii = taskIterator.emplace_back(&mi, dims);
    mii.SetTaskNum(vTask.size());
    mii.SetTaskId(i);
    mii.SetNoPacksToGo(vTask[i].dwEndPackno);
    mii.RewindToPack((i == 0) ? 0 : vTask[i - 1].dwEndPackno + 1);
  }

  res1.insert(rceng->query_thread_pool.add_task(&AggregationWorkerEnt::TaskAggrePacks, this, &taskIterator[0], &dims,
                                                &mit, 0, vTask[0].dwEndPackno, 0, gb_main, conn));
  for (size_t i = 1; i < vTask.size(); ++i) {
    res1.insert(rceng->query_thread_pool.add_task(&AggregationWorkerEnt::TaskAggrePacks, this, &taskIterator[i], &dims,
                                                  &mit, vTask[i - 1].dwEndPackno + 1, vTask[i].dwEndPackno,
                                                  vTask[i - 1].dwTuple, vGBW[i].get(), conn));
  }
  res1.get_all_with_except();

  for (size_t i = 0; i < vTask.size(); ++i) {
    // Merge aggreation data together
    if (i != 0) gb_main->Merge(*(vGBW[i]));
  }
}
}  // namespace core
}  // namespace stonedb
