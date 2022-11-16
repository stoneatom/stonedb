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
#ifndef TIANMU_CORE_GROUPBY_WRAPPER_H_
#define TIANMU_CORE_GROUPBY_WRAPPER_H_
#pragma once

#include "core/group_distinct_cache.h"
#include "core/group_table.h"
#include "core/pack_guardian.h"
#include "core/temp_table.h"
#include "core/tools.h"
#include "system/tianmu_system.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
// a class for remembering the status of distinct aggregations (rows omitted)
class DistinctWrapper {
  friend class GroupByWrapper;

 public:
  DistinctWrapper(uint32_t power) : p_power(power) {}
  ~DistinctWrapper() = default;

  void Initialize(int _no_attr);
  void InitTuples(int64_t _no_obj,
                  const GroupTable &gt);  // gt is needed for column width
  void DeclareAsDistinct(int attr) { is_dist[attr] = true; }
  bool AnyOmitted();
  void SetAsOmitted(int attr, int64_t obj) { f[attr]->Set(obj); }
  Filter *OmittedFilter(int attr) { return f[attr].get(); }
  bool NextRead(size_t col) { return gd_cache[col].NextRead(); }

 private:
  int no_attr{0};
  uint32_t p_power;

  std::vector<GroupDistinctCache> gd_cache;
  std::vector<std::unique_ptr<Filter>> f;
  std::vector<bool> is_dist;
};

class Transaction;

class GroupByWrapper final {
 public:
  GroupByWrapper(int attrs_size, bool distinct, Transaction *conn, uint32_t power);
  GroupByWrapper(const GroupByWrapper &sec);
  GroupByWrapper &operator=(const GroupByWrapper &) = delete;
  ~GroupByWrapper();

  void AddGroupingColumn(int attr_no, int orig_no, TempTable::Attr &a);
  void AddAggregatedColumn(int orig_no, TempTable::Attr &a, int64_t max_no_vals = 0,
                           int64_t min_v = common::MINUS_INF_64, int64_t max_v = common::PLUS_INF_64, int max_size = 0);

  // Optimizations:
  bool AddPackIfUniform(int attr_no, MIIterator &mit);
  void AddAllGroupingConstants(MIIterator &mit);    // set buffers for all constants
  void AddAllAggregatedConstants(MIIterator &mit);  // set buffers for all constants in aggregations
  void AddAllCountStar(int64_t row, MIIterator &mit,
                       int64_t val);  // set all count(*) values to val, or to 0 in case of nulls
  bool AggregatePackInOneGroup(int attr_no, MIIterator &mit, int64_t uniform_pos, int64_t rows_in_pack, int64_t factor);
  bool AttrMayBeUpdatedByPack(int i, MIIterator &mit);
  bool PackWillNotUpdateAggregation(int i, MIIterator &mit);  // ...because min/max worse than already found
  bool DataWillNotUpdateAggregation(int i);                   // as above, for the whole dataset
  void InvalidateAggregationStatistics() { gt.InvalidateAggregationStatistics(); }
  void DefineAsEquivalent(int i, int j) { attr_mapping[i] = attr_mapping[j]; }
  void PutGroupingValue(int gr_a, MIIterator &mit) { gt.PutGroupingValue(gr_a, mit); }
  bool PutAggregatedNull(int gr_a, int64_t pos);
  bool PutAggregatedValue(int gr_a, int64_t pos, MIIterator &mit, int64_t factor = 1);
  // return value: true if value checked in, false if not (DISTINCT buffer
  // overflow) functionalities around DISTINCT
  bool PutAggregatedValueForCount(int gr_a, int64_t pos,
                                  int64_t factor);  // a simplified version for counting only
  bool PutAggregatedValueForMinMax(int gr_a, int64_t pos,
                                   int64_t factor);  // a simplified version for MIN or MAX
  bool PutCachedValue(int gr_a);                     // current value from distinct cache
  bool CacheValid(int gr_a);                         // true if there is a value cached for current row
  void UpdateDistinctCaches();                       // take into account which values are already counted
  void OmitInCache(int attr, int64_t obj_to_omit);
  void DistinctlyOmitted(int attr, int64_t obj);
  bool AnyOmittedByDistinct() { return distinct_watch.AnyOmitted(); }
  int64_t ApproxDistinctVals(int gr_a, MultiIndex *mind = nullptr);

  int NumOfAttrs() { return no_attr; }
  int NumOfGroupingAttrs() { return no_grouping_attr; }
  bool DistinctAggr(int col) { return gt.AttrDistinct(col); }
  void Initialize(int64_t upper_approx_of_groups, bool parallel_allowed = true);
  int64_t NumOfGroups() { return no_groups; }
  void AddGroup() { no_groups++; }
  void ClearNoGroups() { no_groups = 0; }
  int64_t UpperApproxOfGroups() { return gt.UpperApproxOfGroups(); }
  // a position in the current GroupTable, row==common::NULL_VALUE_64 if not
  // found
  bool FindCurrentRow(int64_t &row) { return gt.FindCurrentRow(row); }
  int64_t GetValue64(int col, int64_t row) { return gt.GetValue64(col, row); }
  // iteration through resulting rows
  void RewindRows() { gt.RewindRows(); }
  int64_t GetCurrentRow() { return gt.GetCurrentRow(); }
  void NextRow() { gt.NextRow(); }
  bool RowValid() { return gt.RowValid(); }
  bool SetCurrentRow(int64_t row) { return gt.SetCurrentRow(row); }
  bool SetEndRow(int64_t row) { return gt.SetEndRow(row); }
  int64_t GetRowsNo() { return gt.GetNoOfGroups(); }
  types::BString GetValueT(int col, int64_t row);

  void Clear();  // reset all contents of the grouping table and statistics
  void ClearUsed() { gt.ClearUsed(); }
  void ClearDistinctBuffers();   // reset distinct buffers and distinct cache
  void RewindDistinctBuffers();  // reset distinct buffers, rewind distinct
                                 // cache to use it contents
  void SetDistinctTuples(int64_t no_tuples) { distinct_watch.InitTuples(no_tuples, gt); }
  bool IsFull() { return gt.IsFull(); }
  void SetAsFull() { gt.SetAsFull(); }
  bool IsAllGroupsFound() { return no_more_groups; }
  void SetAllGroupsFound() { no_more_groups = true; }
  void FillDimsUsed(DimensionVector &dims);  // set true on all dimensions used
  int AttrMapping(int a) { return attr_mapping[a]; }
  bool IsCountOnly(int a = -1);  // true, if an attribute is count(*)/count(const), or if
                                 // there is no columns except constants and count (when no
                                 // attr specified)
  bool IsMinOnly();              // true, if an attribute is min(column)
  bool IsMaxOnly();              // true, if an attribute is max(column)
  bool IsCountDistinctOnly() {
    return no_grouping_attr == 0 && no_aggregated_attr == 1 && gt.AttrOper(0) == GT_Aggregation::GT_COUNT_NOT_NULL &&
           gt.AttrDistinct(0);
  }
  bool MayBeParallel() const;
  bool IsOnePass() { return gt.IsOnePass(); }
  int MemoryBlocksLeft() { return gt.MemoryBlocksLeft(); }  // no place left for more packs (soft limit)
  void Merge(GroupByWrapper &sec);
  // A filter of rows to be aggregated

  void InitTupleLeft(int64_t n);
  bool AnyTuplesLeft(int64_t from, int64_t to);
  bool AnyTuplesLeft() { return (tuple_left != nullptr) && !tuple_left->IsEmpty(); }
  int64_t TuplesLeftBetween(int64_t from, int64_t to);
  void CommitResets() {
    if (tuple_left)
      tuple_left->Commit();
  }
  void TuplesResetAll() {
    if (tuple_left)
      tuple_left->Reset();
  }
  void TuplesResetBetween(int64_t from, int64_t to) {
    if (tuple_left)
      tuple_left->ResetBetween(from, to);
  }
  void TuplesReset(int64_t pos) {
    if (tuple_left)
      tuple_left->ResetDelayed(pos);
  }
  bool TuplesGet(int64_t pos) { return (tuple_left == nullptr) || tuple_left->Get(pos); }
  int64_t TuplesNoOnes() { return (tuple_left == nullptr ? 0 : tuple_left->NumOfOnes()); }
  // Locking packs etc.

  void LockPack(int i, MIIterator &mit);
  void LockPackAlways(int i, MIIterator &mit);
  void ResetPackrow();
  bool ColumnNotOmitted(int a) { return pack_not_omitted[a]; }
  void OmitColumnForPackrow(int a) { pack_not_omitted[a] = false; }
  vcolumn::VirtualColumn *SourceColumn(int a) { return virt_col[a]; }
  vcolumn::VirtualColumn *GetColumn(int i) {
    DEBUG_ASSERT(i < no_attr);
    return virt_col[i];
  }
  bool JustDistinct() { return just_distinct; }

  DistinctWrapper distinct_watch;

  // Statistics:
  int64_t packrows_omitted;
  int64_t packrows_part_omitted;
  Transaction *m_conn;

 private:
  enum class GBInputMode {
    GBIMODE_NOT_SET,
    GBIMODE_NO_VALUE,  // e.g. count(*)
    GBIMODE_AS_INT64,
    GBIMODE_AS_TEXT
  };
  uint32_t p_power;
  int attrs_size;
  int no_grouping_attr;
  int no_aggregated_attr;
  int no_attr;
  bool no_more_groups;  // true if we know that all groups are already found

  int64_t no_groups;  // number of groups found

  vcolumn::VirtualColumn **virt_col;  // a table of grouping/aggregated columns
  GBInputMode *input_mode;            // text/numerical/no_value

  bool *is_lookup;  // is the original column a lookup one?

  int *attr_mapping;       // output attr[j] <-> gt group[attr_mapping[j]]
  bool *pack_not_omitted;  // pack status for columns in current packrow
  int64_t *dist_vals;      // distinct values for column - upper approximation

  GroupTable gt;

  Filter *tuple_left;  // a mask of all rows still to be aggregated
  bool just_distinct;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_GROUPBY_WRAPPER_H_
