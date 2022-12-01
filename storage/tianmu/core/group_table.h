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
#ifndef TIANMU_CORE_GROUP_TABLE_H_
#define TIANMU_CORE_GROUP_TABLE_H_
#pragma once

#include "core/aggregator_advanced.h"
#include "core/aggregator_basic.h"
#include "core/bin_tools.h"
#include "core/column_bin_encoder.h"
#include "core/filter.h"
#include "core/group_distinct_cache.h"
#include "core/group_distinct_table.h"
#include "core/temp_table.h"
#include "core/value_matching_table.h"

namespace Tianmu {
namespace core {

// GroupTable - a tool for storing values and counters

enum class GT_Aggregation {
  GT_LIST,  // GT_LIST - just store the first value
  GT_COUNT,
  GT_COUNT_NOT_NULL,
  GT_SUM,
  GT_MIN,
  GT_MAX,
  GT_AVG,
  GT_STD_POP,
  GT_STD_SAMP,
  GT_VAR_POP,
  GT_VAR_SAMP,
  GT_BIT_AND,
  GT_BIT_OR,
  GT_BIT_XOR,
  GT_GROUP_CONCAT
};

class GroupTable : public mm::TraceableObject {
 public:
  GroupTable(uint32_t power);
  GroupTable(const GroupTable &sec);
  ~GroupTable();

  // Group table construction

  void AddGroupingColumn(vcolumn::VirtualColumn *vc);
  void AddAggregatedColumn(vcolumn::VirtualColumn *vc, GT_Aggregation operation, bool distinct, common::ColumnType type,
                           int b_size, int precision, DTCollation in_collation, SI si);
  void AggregatedColumnStatistics(int ag_col, int64_t max_no_vals, int64_t min_v = common::MINUS_INF_64,
                                  int64_t max_v = common::PLUS_INF_64) {
    aggregated_desc[ag_col].max_no_values = max_no_vals;
    aggregated_desc[ag_col].min = min_v;
    aggregated_desc[ag_col].max = max_v;
  }

  void Initialize(int64_t max_no_groups,
                  bool parallel_allowed);  // initialize buffers basing on
                                           // the previously defined
                                           // grouping/aggregated columns
  // and the best possible upper approximation of number of groups;
  // note: max_no_groups may also be used for "select ... limit n", because we
  // will not produce more groups

  // Group table usage
  //  put values to a temporary buffer (note that it will contain the previous
  //  values, which may be reused
  void PutGroupingValue(int col, MIIterator &mit) {
    // Encoder statistics are not updated here
    encoder[col]->Encode(input_buffer.data(), mit);
  }
  void PutUniformGroupingValue(int col, MIIterator &mit);

  bool FindCurrentRow(int64_t &row);  // a position in the current GroupTable,
                                      // row==common::NULL_VALUE_64 if not found
  // return value: true if already existing, false if put as a new row
  GDTResult FindDistinctValue(int col, int64_t row,
                              int64_t v);  // just check whether exists, do not add
  GDTResult AddDistinctValue(int col, int64_t row,
                             int64_t v);  // just add to a list of distincts, not to aggregators

  // columns have common numbering, both grouping and aggregated ones
  // return value: true if value checked in, false if not (possible only on
  // DISTINCT buffer overflow)

  bool PutAggregatedValue(int col, int64_t row,
                          int64_t factor);  // for aggregations which do not need any value
  bool PutAggregatedValue(int col, int64_t row, MIIterator &mit, int64_t factor, bool as_string);
  bool PutAggregatedNull(int col, int64_t row, bool as_string);
  // mainly for numerics, and only some aggregators
  bool PutCachedValue(int col, GroupDistinctCache &cache, bool as_text);
  // a size of distinct cache for one value
  int GetCachedWidth(int col) const { return gdistinct[col]->InputBufferSize(); }

  bool AggregatePack(int col,
                     int64_t row);  // aggregate based on parameters stored in the aggregator

  void AddCurrentValueToCache(int col, GroupDistinctCache &cache);
  void Merge(GroupTable &sec,
             Transaction *m_conn);  // merge values from another (compatible) GroupTable
  // Group table output and info

  bool IsFull() { return !not_full; }  // no place left or all groups found
  void SetAsFull() { not_full = false; }
  bool MayBeParallel() const {
    if (distinct_present)
      return false;

    for (auto &ag : aggregated_desc) {
      if (ag.operation == GT_Aggregation::GT_GROUP_CONCAT)
        return false;
    }
    return true;
  }
  bool IsOnePass() { return !distinct_present && vm_tab->IsOnePass(); }  // assured one-pass scan
  bool SetCurrentRow(int64_t row) { return vm_tab->SetCurrentRow(row); }
  bool SetEndRow(int64_t row) { return vm_tab->SetEndRow(row); }
  int64_t GetNoOfGroups() { return vm_tab->NoRows(); }
  int MemoryBlocksLeft();  // no place left for more packs (soft limit)

  int64_t GetValue64(int col, int64_t row);  // columns have common numbering
  types::BString GetValueT(int col, int64_t row);

  void ClearAll();       // initialize all
  void ClearUsed();      // initialize only used rows (for next pass)
  void ClearDistinct();  // reset the tables of distinct values, if present

  void UpdateAttrStats(int col);                          // update the current statistics for a column, if needed
  bool AttrMayBeUpdatedByPack(int col, MIIterator &mit);  // false if a grouping attribute pack is
                                                          // out of scope of the current contents
  void InvalidateAggregationStatistics();                 // force recalculate rough statistics
                                                          // for aggregators

  GT_Aggregation AttrOper(int col) { return operation[col]; }
  TIANMUAggregator *AttrAggregator(int col) { return aggregator[col]; }
  bool AttrDistinct(int col) { return distinct[col]; }
  int64_t UpperApproxOfGroups() { return declared_max_no_groups; }
  // iteration through resulting rows
  void RewindRows() { vm_tab->Rewind(); }
  int64_t GetCurrentRow() { return vm_tab->GetCurrentRow(); }
  void NextRow() { vm_tab->NextRow(); }
  bool RowValid() { return vm_tab->RowValid(); }
  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }

 private:
  std::vector<unsigned char> input_buffer;
  std::vector<int> aggregated_col_offset;  // a table of byte offsets of
                                           // aggregated column beginnings wrt.
                                           // "grouping_and_UTF_width" position

  std::unique_ptr<ValueMatchingTable> vm_tab;  // abstract value container (for hash searching etc.)

  int grouping_buf_width;          // in bytes
  int grouping_and_UTF_width;      // in bytes, total buffer size for grouping and
                                   // UTF originals
  int total_width;                 // in bytes
  int64_t declared_max_no_groups;  // maximal number of groups calculated from
                                   // KNs etc.

  // column/operation descriptions

  bool initialized;

  // temporary description - prior to buffers initialization, then sometimes
  // used for complex aggregations
  struct ColTempDesc {
    ColTempDesc() {
      vc = nullptr;
      min = common::MINUS_INF_64;
      max = common::PLUS_INF_64;
      max_no_values = 0;
      operation = GT_Aggregation::GT_LIST;
      distinct = false;
      size = 0;
      precision = 0;
      type = common::ColumnType::INT;
      si.order = ORDER::ORDER_NOT_RELEVANT;  // direction for GROUP_CONCAT order
                                             // by 0/1-ASC/2-DESC
      si.separator = ',';                    // for GROUP_CONCAT
    }

    vcolumn::VirtualColumn *vc;
    GT_Aggregation operation;  // not used for grouping columns
    bool distinct;             // not used for grouping columns
    common::ColumnType type;
    int size;
    int precision;
    // optimization statistics, not used for grouping columns:
    int64_t min;
    int64_t max;
    int64_t max_no_values;  // upper approximation of a number of distinct
                            // values (without null), 0 - don't know
    DTCollation collation;
    SI si;
  };
  std::vector<ColTempDesc> grouping_desc;  // input fields description
  std::vector<ColTempDesc> aggregated_desc;

  std::vector<char> vc_owner;
  std::vector<GT_Aggregation> operation;  // Note: these tables will be created in Initialize()
  std::vector<bool> distinct;
  std::vector<vcolumn::VirtualColumn *> vc;
  std::vector<TIANMUAggregator *> aggregator;  // a table of actual aggregators
  std::vector<ColumnBinEncoder *> encoder;     // encoders for grouping columns

  // "distinct" part
  std::vector<std::shared_ptr<GroupDistinctTable>> gdistinct;  // Empty if not used

  // general descriptions
  uint32_t p_power;
  int no_attr;
  int no_grouping_attr;
  bool not_full;  // true if hash conflicts persisted
  bool distinct_present;

  // some memory managing
  int64_t max_total_size;
  std::mutex mtx;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_GROUP_TABLE_H_
