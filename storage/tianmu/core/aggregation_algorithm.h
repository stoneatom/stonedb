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
#ifndef TIANMU_CORE_AGGREGATION_ALGORITHM_H_
#define TIANMU_CORE_AGGREGATION_ALGORITHM_H_
#pragma once

#include <mutex>
#include "core/ctask.h"
#include "core/groupby_wrapper.h"
#include "core/mi_iterator.h"
#include "core/query.h"
#include "core/temp_table.h"

namespace Tianmu {
namespace core {
enum class AggregaGroupingResult {
  AGR_OK = 0,           // success
  AGR_FINISH = 1,       // finish
  AGR_KILLED = 2,       // killed
  AGR_OVERFLOW = 3,     // overflow
  AGR_OTHER_ERROR = 4,  // other error
  AGR_NO_LEFT = 5       // pack already aggregated
};

class AggregationAlgorithm {
 public:
  AggregationAlgorithm(TempTable *tt)
      : t(tt), m_conn(tt->m_conn), mind(tt->GetMultiIndexP()), factor(1), packrows_found(0) {}

  void Aggregate(bool just_distinct, int64_t &limit, int64_t &offset, ResultSender *sender);

  bool AggregateRough(GroupByWrapper &gbw, MIIterator &mit, bool &packrow_done, bool &part_omitted,
                      bool &ag_not_changeabe, bool &stop_all, int64_t &uniform_pos, int64_t rows_in_pack,
                      int64_t local_factor, int just_one_aggr = -1);
  void MultiDimensionalGroupByScan(GroupByWrapper &gbw, int64_t &limit, int64_t &offset, ResultSender *sender,
                                   [[maybe_unused]] bool limit_less_than_no_groups);
  void MultiDimensionalDistinctScan(GroupByWrapper &gbw, MIIterator &mit);
  void AggregateFillOutput(GroupByWrapper &gbw, int64_t gt_pos, int64_t &omit_by_offset);

  // Return code for AggregatePackrow: 0 - success, 1 - stop aggregation
  // (finished), 5 - pack already aggregated (skip)
  AggregaGroupingResult AggregatePackrow(GroupByWrapper &gbw, MIIterator *mit, int64_t cur_tuple,
                                         uint64_t *mem_used = nullptr);

  // No parallel for subquery/join/distinct cases
  bool ParallelAllowed(GroupByWrapper &gbw) {
    return (tianmu_sysvar_groupby_speedup && !t->HasTempTable() && (mind->NumOfDimensions() == 1) &&
            gbw.MayBeParallel());
  }
  void TaskFillOutput(GroupByWrapper *gbw, Transaction *ci, int64_t offset, int64_t limit);
  void ParallelFillOutputWrapper(GroupByWrapper &gbw, int64_t offset, int64_t limit, MIIterator &mit);
  TempTable *GetTempTable() { return t; }

 private:
  // just pointers:
  TempTable *t;
  Transaction *m_conn;
  MultiIndex *mind;

  int64_t factor;  // multiindex factor - how many actual rows is processed by
                   // one iterator step

  // Some statistics for display:
  int64_t packrows_found;  // all packrows, except these completely omitted (as
                           // aggregated before)
  std::mutex mtx;
};

class AggregationWorkerEnt {
 public:
  AggregationWorkerEnt(GroupByWrapper &gbw, MultiIndex *_mind, int thd_cnt, AggregationAlgorithm *_aa)
      : gb_main(&gbw), mind(_mind), m_threads(thd_cnt), aa(_aa) {}
  bool MayBeParallel([[maybe_unused]] MIIterator &mit) { return true; }
  void Init([[maybe_unused]] MIIterator &mit) {}
  // Return code for AggregatePackrow: 0 - success, 1 - stop aggregation
  // (finished), 2 - killed, 3
  // - overflow, 4 - other error, 5 - pack already aggregated (skip)
  AggregaGroupingResult AggregatePackrow(MIIterator &lmit, int64_t cur_tuple) {
    return aa->AggregatePackrow(*gb_main, &lmit, cur_tuple);
  }
  AggregaGroupingResult AggregatePackrow(MIInpackIterator &lmit, int64_t cur_tuple) {
    return aa->AggregatePackrow(*gb_main, &lmit, cur_tuple);
  }
  void Commit([[maybe_unused]] bool do_merge = true) { gb_main->CommitResets(); }
  void ReevaluateNumberOfThreads([[maybe_unused]] MIIterator &mit) {}
  int ThreadsUsed() { return m_threads; }
  void Barrier() {}
  void TaskAggrePacks(MIIterator *taskIterator, DimensionVector *dims, MIIterator *mit, CTask *task,
                      GroupByWrapper *gbw, Transaction *ci, uint64_t *mem_used = nullptr);
  void DistributeAggreTaskAverage(MIIterator &mit, uint64_t *mem_used = nullptr);
  void PrepShardingCopy(MIIterator *mit, GroupByWrapper *gb_sharding,
                        std::vector<std::unique_ptr<GroupByWrapper>> *vGBW);

 protected:
  GroupByWrapper *gb_main;
  MultiIndex *mind;
  int m_threads;
  AggregationAlgorithm *aa;
  std::mutex mtx;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_AGGREGATION_ALGORITHM_H_
