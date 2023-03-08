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
#include <limits>

#include "group_table.h"

#include "core/group_distinct_table.h"
#include "core/mi_iterator.h"
#include "core/transaction.h"
#include "core/value_matching_table.h"
#include "system/fet.h"

namespace Tianmu {
namespace core {
GroupTable::GroupTable(uint32_t power) {
  initialized = false;
  grouping_buf_width = 0;
  grouping_and_UTF_width = 0;
  total_width = 0;
  max_total_size = 0;
  no_attr = 0;
  no_grouping_attr = 0;
  not_full = true;
  declared_max_no_groups = 0;
  distinct_present = false;
  p_power = power;
}

GroupTable::~GroupTable() {
  for (auto &it : aggregator) delete it;

  for (auto &it : encoder) delete it;

  for (int i = 0; i < no_attr; i++) {
    if (vc_owner.size() != 0 && vc_owner[i])
      delete vc[i];
  }
}

GroupTable::GroupTable(const GroupTable &sec) : mm::TraceableObject(sec) {
  DEBUG_ASSERT(sec.initialized);  // can only copy initialized GroupTables!
  // Some fields are omitted (empty vectors), as they are used only for
  // Initialize()
  initialized = true;
  p_power = sec.p_power;
  grouping_buf_width = sec.grouping_buf_width;
  grouping_and_UTF_width = sec.grouping_and_UTF_width;
  total_width = sec.total_width;
  no_attr = sec.no_attr;
  no_grouping_attr = sec.no_grouping_attr;
  not_full = sec.not_full;
  distinct_present = sec.distinct_present;
  declared_max_no_groups = sec.declared_max_no_groups;
  input_buffer = sec.input_buffer;

  gdistinct = sec.gdistinct;
  aggregator.resize(no_attr);
  encoder.resize(no_attr);
  vc.resize(no_attr);

  if (sec.vm_tab)
    vm_tab.reset(sec.vm_tab->Clone());

  max_total_size = sec.max_total_size;
  vc_owner.reserve(no_attr);
  aggregated_col_offset = sec.aggregated_col_offset;
  distinct = sec.distinct;
  operation = sec.operation;
  for (int i = 0; i < no_attr; i++) {
    vc[i] = sec.vc[i];

    char vco = 0;
    if (sec.vc[i] && static_cast<int>(sec.vc[i]->IsSingleColumn())) {
      vc[i] = CreateVCCopy(sec.vc[i]);
      if (vc[i] != sec.vc[i]) {
        // success
        vco = 1;
      }
    }
    vc_owner.push_back(vco);

    if (sec.aggregator[i])
      aggregator[i] = sec.aggregator[i]->Copy();  // get a new object of appropriate subclass

    if (sec.encoder[i])
      encoder[i] = new ColumnBinEncoder(*sec.encoder[i]);
  }
}

void GroupTable::AddGroupingColumn(vcolumn::VirtualColumn *vc) {
  GroupTable::ColTempDesc desc;
  desc.vc = vc;
  grouping_desc.push_back(desc);
}

void GroupTable::AddAggregatedColumn(vcolumn::VirtualColumn *vc, GT_Aggregation operation, bool distinct,
                                     common::ColumnType type, int size, int precision, DTCollation in_collation,
                                     SI si) {
  GroupTable::ColTempDesc desc;
  desc.type = type;  // put defaults first, then check in Initialize() what will
                     // be actual result definition
  // Overwriting size in some cases:
  switch (type) {
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
    case common::ColumnType::BYTEINT:
    case common::ColumnType::SMALLINT:
      size = 4;
      break;  // minimal field is one int (4 bytes)
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
      // left as is
      break;
    // Note: lookup strings will have type common::CT::STRING or similar, and
    // size 4 (stored as int!)
    default:
      size = 8;
  }
  desc.vc = vc;
  desc.size = size;
  desc.precision = precision;
  desc.operation = operation;
  desc.distinct = distinct;
  desc.collation = in_collation;
  if (operation == GT_Aggregation::GT_GROUP_CONCAT) {
    desc.si = si;
  }
  aggregated_desc.push_back(desc);
}

void GroupTable::Initialize(int64_t max_no_groups, bool parallel_allowed) {
  MEASURE_FET("GroupTable::Initialize(...)");
  declared_max_no_groups = max_no_groups;
  if (initialized)
    return;
  no_grouping_attr = int(grouping_desc.size());
  no_attr = no_grouping_attr + int(aggregated_desc.size());
  if (no_attr == 0)
    return;

  operation.resize(no_attr);
  distinct.resize(no_attr);
  aggregated_col_offset.resize(no_attr);
  gdistinct.resize(no_attr);
  aggregator.resize(no_attr);
  encoder.resize(no_attr);
  vc.resize(no_attr);

  // rewrite column descriptions (defaults, to be verified)
  int no_columns_with_distinct = 0;
  GroupTable::ColTempDesc desc;
  for (int i = 0; i < no_attr; i++) {
    if (i < no_grouping_attr) {
      desc = grouping_desc[i];
      vc[i] = desc.vc;
      // TODO: not always decodable? (hidden cols)
      encoder[i] = new ColumnBinEncoder(ColumnBinEncoder::ENCODER_DECODABLE);
      encoder[i]->PrepareEncoder(desc.vc);
    } else {
      desc = aggregated_desc[i - no_grouping_attr];
      vc[i] = desc.vc;
      // Aggregations:

      // COUNT(...)
      if (desc.operation == GT_Aggregation::GT_COUNT || desc.operation == GT_Aggregation::GT_COUNT_NOT_NULL) {
        if (desc.max_no_values > std::numeric_limits<int>::max())
          aggregator[i] = new AggregatorCount64(desc.max_no_values);
        else
          aggregator[i] = new AggregatorCount32(int(desc.max_no_values));

      } else {  // SUM(...)				Note: strings are parsed
                // to double
        if (desc.operation == GT_Aggregation::GT_SUM) {
          if (ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
            aggregator[i] = new AggregatorSumD;
          else
            aggregator[i] = new AggregatorSum64;

        } else {  // AVG(...)				Note: strings are parsed
                  // to double
          if (desc.operation == GT_Aggregation::GT_AVG) {
            if (ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
              aggregator[i] = new AggregatorAvgD;
            else if (desc.type == common::ColumnType::YEAR)
              aggregator[i] = new AggregatorAvgYear;
            else
              aggregator[i] = new AggregatorAvg64(desc.precision);

          } else {  // MIN(...)
            if (desc.operation == GT_Aggregation::GT_MIN) {
              if (ATI::IsStringType(desc.type)) {
                if (types::RequiresUTFConversions(desc.collation))
                  aggregator[i] = new AggregatorMinT_UTF(desc.size, desc.collation);
                else
                  aggregator[i] = new AggregatorMinT(desc.size);
              } else if (ATI::IsRealType(desc.type))
                aggregator[i] = new AggregatorMinD;
              else if (desc.min < std::numeric_limits<int>::min() || desc.max > std::numeric_limits<int>::max())
                aggregator[i] = new AggregatorMin64;
              else
                aggregator[i] = new AggregatorMin32;

            } else {  // MAX(...)
              if (desc.operation == GT_Aggregation::GT_MAX) {
                if (ATI::IsStringType(desc.type)) {
                  if (types::RequiresUTFConversions(desc.collation))
                    aggregator[i] = new AggregatorMaxT_UTF(desc.size, desc.collation);
                  else
                    aggregator[i] = new AggregatorMaxT(desc.size);
                } else if (ATI::IsRealType(desc.type))
                  aggregator[i] = new AggregatorMaxD;
                else if (desc.min < std::numeric_limits<int>::min() || desc.max > std::numeric_limits<int>::max())
                  aggregator[i] = new AggregatorMax64;
                else
                  aggregator[i] = new AggregatorMax32;
              } else {  // LIST - just a first value found
                if (desc.operation == GT_Aggregation::GT_LIST) {
                  if (ATI::IsStringType(desc.type))
                    aggregator[i] = new AggregatorListT(desc.size);
                  else if (ATI::IsRealType(desc.type) ||
                           (desc.min < std::numeric_limits<int>::min() || desc.max > std::numeric_limits<int>::max()))
                    aggregator[i] = new AggregatorList64;
                  else
                    aggregator[i] = new AggregatorList32;

                } else {  // VAR_POP(...)
                  if (desc.operation == GT_Aggregation::GT_VAR_POP) {
                    if (ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
                      aggregator[i] = new AggregatorVarPopD;
                    else
                      aggregator[i] = new AggregatorVarPop64(desc.precision);

                  } else {  // VAR_SAMP(...)
                    if (desc.operation == GT_Aggregation::GT_VAR_SAMP) {
                      if (ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
                        aggregator[i] = new AggregatorVarSampD;
                      else
                        aggregator[i] = new AggregatorVarSamp64(desc.precision);

                    } else {  // STD_POP(...)
                      if (desc.operation == GT_Aggregation::GT_STD_POP) {
                        if (ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
                          aggregator[i] = new AggregatorStdPopD;
                        else
                          aggregator[i] = new AggregatorStdPop64(desc.precision);

                      } else {  // STD_SAMP(...)
                        if (desc.operation == GT_Aggregation::GT_STD_SAMP) {
                          if (ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
                            aggregator[i] = new AggregatorStdSampD;
                          else
                            aggregator[i] = new AggregatorStdSamp64(desc.precision);

                        } else {  // BIT_AND(...)
                          if (desc.operation == GT_Aggregation::GT_BIT_AND) {
                            aggregator[i] = new AggregatorBitAnd;

                          } else {  // BIT_Or(...)
                            if (desc.operation == GT_Aggregation::GT_BIT_OR) {
                              aggregator[i] = new AggregatorBitOr;

                            } else {  // BIT_XOR(...)
                              if (desc.operation == GT_Aggregation::GT_BIT_XOR) {
                                aggregator[i] = new AggregatorBitXor;
                              } else {  // GT_Aggregation::GT_GROUP_CONCAT(...)
                                if (desc.operation == GT_Aggregation::GT_GROUP_CONCAT) {
                                  aggregator[i] = new AggregatorGroupConcat(desc.si, desc.type);
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      if (aggregator[i]->IgnoreDistinct())
        desc.distinct = false;
    }
    operation[i] = desc.operation;
    distinct[i] = desc.distinct;
    if (distinct[i])
      no_columns_with_distinct++;
  }
  if (no_columns_with_distinct > 0)
    distinct_present = true;
  // calculate column byte sizes
  grouping_buf_width = 0;
  grouping_and_UTF_width = 0;
  total_width = 0;
  for (int i = 0; i < no_grouping_attr; i++) {
    encoder[i]->SetPrimaryOffset(grouping_buf_width);
    grouping_buf_width += encoder[i]->GetPrimarySize();
  }
  grouping_and_UTF_width = grouping_buf_width;

  // UTF originals, if any
  for (int i = 0; i < no_grouping_attr; i++) {
    if (encoder[i]->GetSecondarySize() > 0) {
      encoder[i]->SetSecondaryOffset(grouping_and_UTF_width);
      grouping_and_UTF_width += encoder[i]->GetSecondarySize();
    }
  }
  // round up to 4-byte alignment (for numerical counters)
  grouping_and_UTF_width = 4 * ((grouping_and_UTF_width + 3) / 4);  // e.g. 1->4, 12->12, 19->20
  total_width = grouping_and_UTF_width;

  // Aggregators
  for (int i = no_grouping_attr; i < no_attr; i++) {
    aggregated_col_offset[i] = total_width - grouping_and_UTF_width;
    total_width += aggregator[i]->BufferByteSize();
    total_width = 4 * ((total_width + 3) / 4);  // e.g. 1->4, 12->12, 19->20
  }

  // create buffers
  //  Memory settings
  if (total_width < 1)
    total_width = 1;  // rare case: all constants, no actual buffer needed (but
                      // we create one anyway, just to avoid a special execution
                      // path for this boundary case)

  // pre-allocation of group by memory
  int64_t max_size = std::max(max_no_groups * total_width * (1 + no_columns_with_distinct),
                              parallel_allowed ? max_no_groups * total_width * 4 : 0);

  max_total_size = mm::TraceableObject::MaxBufferSizeForAggr(int64_t(ceil(max_size * 1.3)));
  // Check memory only for larger groupings. More aggressive memory settings for
  // distinct.

  // calculate sizes
  int64_t primary_total_size = max_total_size;  // size of grouping table, not distinct;

  // Optimization of group value search
  int64_t max_group_code = common::PLUS_INF_64;
  if (grouping_buf_width == 1 && encoder[0]->MaxCode() > 0)
    max_group_code = encoder[0]->MaxCode();
  if (grouping_buf_width == 2 && no_grouping_attr == 2 && encoder[1]->MaxCode() > 0)
    max_group_code = encoder[1]->MaxCode() * 256 + encoder[0]->MaxCode();  // wider than one-byte encoders are hard to
                                                                           // interpret, because of endianess swap

  vm_tab.reset(ValueMatchingTable::CreateNew_ValueMatchingTable(primary_total_size, declared_max_no_groups,
                                                                max_group_code, total_width, grouping_and_UTF_width,
                                                                grouping_buf_width, p_power, false));

  input_buffer.resize(grouping_and_UTF_width);
  // pre-allocation of distinct memory
  int distinct_width = 0;
  int64_t distinct_size = 0;
  for (int i = no_grouping_attr; i < no_attr; i++)
    if (distinct[i]) {
      desc = aggregated_desc[i - no_grouping_attr];
      distinct_width = vc[i]->MaxStringSize() + 2;
      distinct_size += desc.max_no_values * distinct_width;
    }
  distinct_size = int64_t(ceil(distinct_size * 1.3));
  // initialize distinct tables
  for (int i = no_grouping_attr; i < no_attr; i++)
    if (distinct[i]) {
      desc = aggregated_desc[i - no_grouping_attr];
      DEBUG_ASSERT(distinct_size > 0);
      gdistinct[i] = std::make_shared<GroupDistinctTable>(p_power);
      gdistinct[i]->InitializeVC(vm_tab->RowNumberScope(), vc[i], desc.max_no_values,
                                 distinct_size / no_columns_with_distinct,
                                 (operation[i] != GT_Aggregation::GT_COUNT_NOT_NULL));  // otherwise must be decodable
      gdistinct[i]->CopyFromValueMatchingTable(vm_tab.get());
      distinct_size -= gdistinct[i]->BytesTaken();
      no_columns_with_distinct--;
    }

  // initialize everything
  Transaction *m_conn = current_txn_;
  double size_mb = (double)vm_tab->ByteSize();
  size_mb =
      (size_mb > 1024 ? (size_mb > 1024 * 1024 ? int64_t(size_mb / 1024 * 1024) : int64_t(size_mb / 1024) / 1024.0)
                      : 0.001);
  tianmu_control_.lock(m_conn->GetThreadID())
      << "GroupTable begin, initialized for up to " << max_no_groups << " groups, " << grouping_buf_width << "+"
      << total_width - grouping_buf_width << " bytes (" << size_mb << " MB)" << system::unlock;
  ClearAll();
  initialized = true;
}

void GroupTable::PutUniformGroupingValue(int col, MIIterator &mit) {
  int64_t uniform_value = vc[col]->GetMinInt64Exact(mit);
  bool success = false;
  if (uniform_value != common::NULL_VALUE_64)
    if (encoder[col]->PutValue64(input_buffer.data(), uniform_value, false))
      success = true;
  if (!success) {
    vc[col]->LockSourcePacks(mit);
    encoder[col]->Encode(input_buffer.data(), mit);
  }
}

bool GroupTable::FindCurrentRow(int64_t &row)  // a position in the current GroupTable,
                                               // row==common::NULL_VALUE_64 if not found and not added
{
  // return value: true if already existing, false if put as a new row
  // MEASURE_FET("GroupTable::FindCurrentRow(...)");
  // copy input_buffer into t.blocks if cannot find input_buffer in t.blocks
  bool existed = vm_tab->FindCurrentRow(input_buffer.data(), row, not_full);
  if (!existed && row != common::NULL_VALUE_64) {
    if (vm_tab->NoMoreSpace())
      not_full = false;
    if (no_grouping_attr > 0) {
      unsigned char *p = vm_tab->GetGroupingRow(row);
      for (int col = 0; col < no_grouping_attr; col++)
        encoder[col]->UpdateStatistics(p);  // encoders have their offsets inside
    }
    unsigned char *p = vm_tab->GetAggregationRow(row);
    for (int col = no_grouping_attr; col < no_attr; col++)
      aggregator[col]->Reset(p + aggregated_col_offset[col]);  // prepare the row for contents
  }
  return existed;
}

int GroupTable::MemoryBlocksLeft() { return vm_tab->MemoryBlocksLeft(); }

void GroupTable::Merge(GroupTable &sec, Transaction *m_conn) {
  DEBUG_ASSERT(total_width == sec.total_width);
  sec.vm_tab->Rewind(true);
  int64_t sec_row;
  int64_t row;
  not_full = true;  // ensure all the new values will be added
  while (sec.vm_tab->RowValid()) {
    if (m_conn->Killed())
      throw common::KilledException();
    sec_row = sec.vm_tab->GetCurrentRow();
    if (grouping_and_UTF_width > 0)
      std::memcpy(input_buffer.data(), sec.vm_tab->GetGroupingRow(sec_row), grouping_and_UTF_width);
    FindCurrentRow(row);  // find the value on another position or add as a new one
    if (row != common::NULL_VALUE_64) {
      DEBUG_ASSERT(row != common::NULL_VALUE_64);
      unsigned char *p1 = vm_tab->GetAggregationRow(row);
      unsigned char *p2 = sec.vm_tab->GetAggregationRow(sec_row);
      for (int col = no_grouping_attr; col < no_attr; col++) {
        aggregator[col]->Merge(p1 + aggregated_col_offset[col], p2 + sec.aggregated_col_offset[col]);
      }
    }
    sec.vm_tab->NextRow();
  }
  sec.vm_tab->Clear();
}

int64_t GroupTable::GetValue64(int col, int64_t row) {
  if (col >= no_grouping_attr) {
    return aggregator[col]->GetValue64(vm_tab->GetAggregationRow(row) + aggregated_col_offset[col]);
  }
  // Grouping column
  MIDummyIterator mit(1);
  bool is_null;
  return encoder[col]->GetValue64(vm_tab->GetGroupingRow(row), mit, is_null);
}

types::BString GroupTable::GetValueT(int col, int64_t row) {
  if (col >= no_grouping_attr) {
    return aggregator[col]->GetValueT(vm_tab->GetAggregationRow(row) + aggregated_col_offset[col]);
  }
  // Grouping column
  MIDummyIterator mit(1);
  return encoder[col]->GetValueT(vm_tab->GetGroupingRow(row), mit);
}

void GroupTable::ClearAll() {
  not_full = true;
  vm_tab->Clear();

  // initialize statistics
  for (int i = 0; i < no_grouping_attr; i++) encoder[i]->ClearStatistics();

  // initialize aggregated values
  for (int i = no_grouping_attr; i < no_attr; i++) {
    aggregator[i]->ResetStatistics();
    if (gdistinct[i])
      gdistinct[i]->Clear();
  }
}

void GroupTable::ClearUsed() {
  not_full = true;
  vm_tab->Clear();

  // initialize statistics
  for (int i = 0; i < no_grouping_attr; i++) encoder[i]->ClearStatistics();

  for (int i = no_grouping_attr; i < no_attr; i++) {
    aggregator[i]->ResetStatistics();
    if (gdistinct[i])
      gdistinct[i]->Clear();
  }
}

void GroupTable::ClearDistinct()  // reset the table for distinct values
{
  for (int i = no_grouping_attr; i < no_attr; i++) {
    aggregator[i]->ResetStatistics();
    if (gdistinct[i])
      gdistinct[i]->Clear();
  }
}

void GroupTable::AddCurrentValueToCache(int col, GroupDistinctCache &cache) {
  cache.SetCurrentValue(gdistinct[col]->InputBuffer());
}

bool GroupTable::AggregatePack(int col,
                               int64_t row)  // aggregate based on parameters stored in the aggregator
{
  //	unsigned char* p = t + row * total_width + aggregated_col_offset[col];
  return aggregator[col]->AggregatePack(vm_tab->GetAggregationRow(row) + aggregated_col_offset[col]);
}

bool GroupTable::PutAggregatedValue(int col, int64_t row,
                                    int64_t factor)  // for agregations which do not need any value
{
  if (factor == common::NULL_VALUE_64 && aggregator[col]->FactorNeeded())
    throw common::NotImplementedException("Aggregation overflow.");
  aggregator[col]->PutAggregatedValue(vm_tab->GetAggregationRow(row) + aggregated_col_offset[col], factor);
  return true;
}

GDTResult GroupTable::FindDistinctValue(int col, int64_t row,
                                        int64_t v)  // for all numerical values
{
  DEBUG_ASSERT(gdistinct[col]);
  if (v == common::NULL_VALUE_64)  // works also for double
    return GDTResult::GDT_EXISTS;  // null omitted
  return gdistinct[col]->Find(row, v);
}

GDTResult GroupTable::AddDistinctValue(int col, int64_t row,
                                       int64_t v)  // for all numerical values
{
  DEBUG_ASSERT(gdistinct[col]);
  if (v == common::NULL_VALUE_64)  // works also for double
    return GDTResult::GDT_EXISTS;  // null omitted
  return gdistinct[col]->Add(row, v);
}

bool GroupTable::PutAggregatedValue(int col, int64_t row, MIIterator &mit, int64_t factor, bool as_string) {
  if (distinct[col]) {
    // Repetition? Return without action.
    DEBUG_ASSERT(gdistinct[col]);
    if (vc[col]->IsNull(mit))
      return true;  // omit nulls
    GDTResult res = gdistinct[col]->Add(row, mit);
    if (res == GDTResult::GDT_EXISTS)
      return true;  // value found, do not aggregate it again
    if (res == GDTResult::GDT_FULL) {
      // if (gdistinct[col]->AlreadyFull())
      // not_full = false;  // disable also the main grouping table (if it is a
      // persistent rejection)
      // return false;        // value not found in DISTINCT buffer, which is already
      // full
    }
    factor = 1;  // ignore repetitions for distinct
  }
  TIANMUAggregator *cur_aggr = aggregator[col];
  if (factor == common::NULL_VALUE_64 && cur_aggr->FactorNeeded())
    throw common::NotImplementedException("Aggregation overflow.");
  if (as_string) {
    types::BString v;
    vc[col]->GetValueString(v, mit);
    cur_aggr->PutAggregatedValue(vm_tab->GetAggregationRow(row) + aggregated_col_offset[col], v, factor);
  } else {
    // note: it is too costly to check nulls separately (e.g. for complex
    // expressions)
    int64_t v = vc[col]->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64 && cur_aggr->IgnoreNulls())
      return true;
    cur_aggr->PutAggregatedValue(vm_tab->GetAggregationRow(row) + aggregated_col_offset[col], v, factor);
  }
  return true;
}

bool GroupTable::PutAggregatedNull(int col, int64_t row, bool as_string) {
  if (distinct[col])
    return true;  // null omitted
  if (aggregator[col]->IgnoreNulls())
    return true;  // null omitted
  unsigned char *p = vm_tab->GetAggregationRow(row) + aggregated_col_offset[col];
  if (as_string) {
    types::BString v;
    aggregator[col]->PutAggregatedValue(p, v, 1);
  } else
    aggregator[col]->PutAggregatedValue(p, common::NULL_VALUE_64, 1);
  return true;
}

bool GroupTable::PutCachedValue(int col, GroupDistinctCache &cache,
                                bool as_text)  // for all numerical values
{
  DEBUG_ASSERT(distinct[col]);
  GDTResult res = gdistinct[col]->AddFromCache(cache.GetCurrentValue());
  if (res == GDTResult::GDT_EXISTS)
    return true;  // value found, do not aggregate it again
  if (res == GDTResult::GDT_FULL) {
    if (gdistinct[col]->AlreadyFull())
      not_full = false;  // disable also the main grouping table (if it is a
                         // persistent rejection)
    return false;        // value not found in DISTINCT buffer, which is already full
  }
  int64_t row = gdistinct[col]->GroupNoFromInput();  // restore group number
  unsigned char *p = vm_tab->GetAggregationRow(row) + aggregated_col_offset[col];
  if (operation[col] == GT_Aggregation::GT_COUNT_NOT_NULL)
    aggregator[col]->PutAggregatedValue(p, 1);  // factor = 1, because we are just after distinct
  else {
    if (as_text) {
      types::BString v;
      if (ATI::IsTxtType(vc[col]->TypeName())) {
        gdistinct[col]->ValueFromInput(v);
      } else {
        v = vc[col]->DecodeValue_S(gdistinct[col]->ValueFromInput());
      }
      aggregator[col]->PutAggregatedValue(p, v, 1);
    } else {
      int64_t v = gdistinct[col]->ValueFromInput();
      aggregator[col]->PutAggregatedValue(p, v, 1);
    }
  }
  return true;
}

void GroupTable::UpdateAttrStats(int col)  // update the current statistics for a column, if needed
{
  MEASURE_FET("GroupTable::UpdateAttrStats(...)");
  if (!aggregator[col]->StatisticsNeedsUpdate())
    return;
  bool stop_updating = false;
  aggregator[col]->ResetStatistics();
  vm_tab->Rewind();
  while (!stop_updating && vm_tab->RowValid()) {
    stop_updating = aggregator[col]->UpdateStatistics(vm_tab->GetAggregationRow(vm_tab->GetCurrentRow()) +
                                                      aggregated_col_offset[col]);
    vm_tab->NextRow();
  }
  aggregator[col]->SetStatisticsUpdated();
}

bool GroupTable::AttrMayBeUpdatedByPack(int col, MIIterator &mit)  // get the current statistics for a column
{
  if (vc[col]->Type().IsFloat() || vc[col]->Type().IsString())
    return true;  // min/max not calculated properly for these types, see Mojo
                  // 782681
  int64_t local_min = vc[col]->GetMinInt64(mit);
  int64_t local_max = vc[col]->GetMaxInt64(mit);
  if (encoder[col]->ImpossibleValues(local_min, local_max))
    return false;
  return true;
}

void GroupTable::InvalidateAggregationStatistics() {
  for (int i = no_grouping_attr; i < no_attr; i++) {
    aggregator[i]->ResetStatistics();
  }
}
}  // namespace core
}  // namespace Tianmu
