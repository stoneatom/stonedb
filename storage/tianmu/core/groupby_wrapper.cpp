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

#include "groupby_wrapper.h"

#include "core/tianmu_attr.h"

namespace Tianmu {
namespace core {
GroupByWrapper::GroupByWrapper(int a_size, bool distinct, Transaction *conn, uint32_t power)
    : distinct_watch(power), m_conn(conn), gt(power), just_distinct(distinct) {
  p_power = power;
  attrs_size = a_size;
  virt_col = new vcolumn::VirtualColumn *[attrs_size];
  input_mode = new GBInputMode[attrs_size];
  is_lookup = new bool[attrs_size];
  attr_mapping = new int[attrs_size];  // output attr[j] <-> gt group[attr_mapping[j]]
  dist_vals = new int64_t[attrs_size];

  for (int i = 0; i < attrs_size; i++) {
    virt_col[i] = nullptr;
    input_mode[i] = GBInputMode::GBIMODE_NOT_SET;
    is_lookup[i] = false;
    attr_mapping[i] = -1;
    dist_vals[i] = common::NULL_VALUE_64;
  }

  pack_not_omitted = nullptr;
  no_grouping_attr = 0;
  no_aggregated_attr = 0;
  no_attr = 0;
  no_more_groups = false;
  no_groups = 0;
  packrows_omitted = 0;
  packrows_part_omitted = 0;
  tuple_left = nullptr;
}

GroupByWrapper::~GroupByWrapper() {
  for (int i = 0; i < no_attr; i++) {
    if (virt_col[i])
      virt_col[i]->UnlockSourcePacks();
  }
  delete[] input_mode;
  delete[] is_lookup;
  delete[] attr_mapping;
  delete[] pack_not_omitted;
  delete[] dist_vals;
  delete[] virt_col;
  delete tuple_left;
}

GroupByWrapper::GroupByWrapper(const GroupByWrapper &sec)
    : distinct_watch(sec.p_power), m_conn(sec.m_conn), gt(sec.gt) {
  p_power = sec.p_power;
  attrs_size = sec.attrs_size;
  just_distinct = sec.just_distinct;
  virt_col = new vcolumn::VirtualColumn *[attrs_size];
  input_mode = new GBInputMode[attrs_size];
  is_lookup = new bool[attrs_size];
  attr_mapping = new int[attrs_size];  // output attr[j] <-> gt group[attr_mapping[j]]
  dist_vals = new int64_t[attrs_size];

  for (int i = 0; i < attrs_size; i++) {
    attr_mapping[i] = sec.attr_mapping[i];
    virt_col[i] = sec.virt_col[i];
    input_mode[i] = sec.input_mode[i];
    is_lookup[i] = sec.is_lookup[i];
    dist_vals[i] = sec.dist_vals[i];
  }
  no_grouping_attr = sec.no_grouping_attr;
  no_aggregated_attr = sec.no_aggregated_attr;
  no_more_groups = sec.no_more_groups;
  no_groups = sec.no_groups;
  no_attr = sec.no_attr;
  pack_not_omitted = new bool[no_attr];
  packrows_omitted = 0;
  packrows_part_omitted = 0;
  for (int i = 0; i < no_attr; i++) pack_not_omitted[i] = sec.pack_not_omitted[i];

  tuple_left = nullptr;
  if (sec.tuple_left)
    tuple_left = new Filter(*sec.tuple_left);  // a copy of filter
  // init distinct_watch to make copy ctor has all Initialization logic
  distinct_watch.Initialize(no_attr);
  for (int gr_a = 0; gr_a < no_attr; gr_a++) {
    if (gt.AttrDistinct(gr_a)) {
      distinct_watch.DeclareAsDistinct(gr_a);
    }
  }
}

void GroupByWrapper::AddGroupingColumn(int attr_no, int orig_attr_no, TempTable::Attr &a) {
  virt_col[attr_no] = a.term.vc;

  DEBUG_ASSERT(virt_col[attr_no]);

  // Not used for grouping columns:
  is_lookup[attr_no] = false;
  dist_vals[attr_no] = common::NULL_VALUE_64;
  input_mode[attr_no] = GBInputMode::GBIMODE_NOT_SET;

  attr_mapping[orig_attr_no] = attr_no;
  gt.AddGroupingColumn(virt_col[attr_no]);
  no_grouping_attr++;
  no_attr++;
}

void GroupByWrapper::AddAggregatedColumn(int orig_attr_no, TempTable::Attr &a, int64_t max_no_vals, int64_t min_v,
                                         int64_t max_v, int max_size)

{
  // MEASURE_FET("GroupByWrapper::AddAggregatedColumn(...)");
  GT_Aggregation ag_oper;
  common::ColumnType ag_type = a.TypeName();  // original type, not the output one (it
                                              // is important e.g. for AVG)
  int ag_size = max_size;
  int ag_prec = a.Type().GetScale();
  bool ag_distinct = a.distinct;
  int attr_no = no_attr;  // i.e. add at the end (after all grouping cols and
                          // previous aggr.cols)
  DTCollation ag_collation = a.GetCollation();

  virt_col[attr_no] = a.term.vc;

  DEBUG_ASSERT(virt_col[attr_no] || a.term.IsNull());  // the second possibility is count(*)

  is_lookup[attr_no] = false;
  dist_vals[attr_no] = max_no_vals;

  switch (a.mode) {
    case common::ColOperation::SUM:
      ag_oper = GT_Aggregation::GT_SUM;
      ag_type = virt_col[attr_no]->TypeName();
      ag_prec = virt_col[attr_no]->Type().GetScale();
      break;
    case common::ColOperation::AVG:
      ag_oper = GT_Aggregation::GT_AVG;
      ag_type = virt_col[attr_no]->TypeName();
      ag_prec = virt_col[attr_no]->Type().GetScale();
      break;
    case common::ColOperation::MIN:
      ag_oper = GT_Aggregation::GT_MIN;
      break;
    case common::ColOperation::MAX:
      ag_oper = GT_Aggregation::GT_MAX;
      break;
    case common::ColOperation::COUNT:
      if (a.term.IsNull() || (!ag_distinct && virt_col[attr_no]->IsConst())) {
        if (virt_col[attr_no] && virt_col[attr_no]->IsConst()) {
          MIIterator dummy(nullptr, p_power);
          if (virt_col[attr_no]->IsNull(dummy)) {
            ag_oper = GT_Aggregation::GT_COUNT_NOT_NULL;
            ag_type = virt_col[attr_no]->TypeName();
            ag_size = max_size;
          } else {
            virt_col[attr_no] = nullptr;  // forget about constant in count(...), except null
            ag_oper = GT_Aggregation::GT_COUNT;
          }
        } else {
          virt_col[attr_no] = nullptr;  // forget about constant in count(...), except null
          ag_oper = GT_Aggregation::GT_COUNT;
        }
      } else {
        ag_oper = GT_Aggregation::GT_COUNT_NOT_NULL;
        ag_type = virt_col[attr_no]->TypeName();
        ag_size = max_size;
      }
      break;
    case common::ColOperation::LISTING:
      ag_oper = GT_Aggregation::GT_LIST;
      break;
    case common::ColOperation::VAR_POP:
      ag_oper = GT_Aggregation::GT_VAR_POP;
      ag_type = virt_col[attr_no]->TypeName();
      ag_prec = virt_col[attr_no]->Type().GetScale();
      break;
    case common::ColOperation::VAR_SAMP:
      ag_oper = GT_Aggregation::GT_VAR_SAMP;
      ag_type = virt_col[attr_no]->TypeName();
      ag_prec = virt_col[attr_no]->Type().GetScale();
      break;
    case common::ColOperation::STD_POP:
      ag_oper = GT_Aggregation::GT_STD_POP;
      ag_type = virt_col[attr_no]->TypeName();
      ag_prec = virt_col[attr_no]->Type().GetScale();
      break;
    case common::ColOperation::STD_SAMP:
      ag_oper = GT_Aggregation::GT_STD_SAMP;
      ag_type = virt_col[attr_no]->TypeName();
      ag_prec = virt_col[attr_no]->Type().GetScale();
      break;
    case common::ColOperation::BIT_AND:
      ag_oper = GT_Aggregation::GT_BIT_AND;
      break;
    case common::ColOperation::BIT_OR:
      ag_oper = GT_Aggregation::GT_BIT_OR;
      break;
    case common::ColOperation::BIT_XOR:
      ag_oper = GT_Aggregation::GT_BIT_XOR;
      break;
    case common::ColOperation::GROUP_CONCAT:
      ag_oper = GT_Aggregation::GT_GROUP_CONCAT;
      break;
    default:
      throw common::NotImplementedException("Aggregation not implemented");
  }

  if (virt_col[attr_no] && virt_col[attr_no]->Type().Lookup() &&
      !types::RequiresUTFConversions(virt_col[attr_no]->GetCollation()) &&
      (ag_oper == GT_Aggregation::GT_COUNT || ag_oper == GT_Aggregation::GT_COUNT_NOT_NULL ||
       ag_oper == GT_Aggregation::GT_LIST)) {
    // lookup for these operations may use codes
    ag_size = 4;  // integer
    ag_prec = 0;
    ag_type = common::ColumnType::INT;
    is_lookup[attr_no] = true;
  }
  if (ag_oper == GT_Aggregation::GT_COUNT)
    input_mode[attr_no] = GBInputMode::GBIMODE_NO_VALUE;
  else if (ag_oper == GT_Aggregation::GT_GROUP_CONCAT)
    input_mode[attr_no] = GBInputMode::GBIMODE_AS_TEXT;
  else
    input_mode[attr_no] =
        (ATI::IsStringType(virt_col[attr_no]->TypeName()) &&
                 (!is_lookup[attr_no] || types::RequiresUTFConversions(virt_col[attr_no]->GetCollation()))
             ? GBInputMode::GBIMODE_AS_TEXT
             : GBInputMode::GBIMODE_AS_INT64);

  TIANMU_LOG(LogCtl_Level::DEBUG,
             "attr_no %d, input_mode[attr_no] %d, a.alias %s, a.si.separator "
             "%s, direction %d, ag_type %d, ag_size %d",
             attr_no, input_mode[attr_no], a.alias, a.si.separator.c_str(), a.si.order, ag_type, ag_size);

  gt.AddAggregatedColumn(virt_col[attr_no], ag_oper, ag_distinct, ag_type, ag_size, ag_prec, ag_collation,
                         a.si);  // note: size will be automatically calculated for all numericals
  gt.AggregatedColumnStatistics(no_aggregated_attr, max_no_vals, min_v, max_v);
  attr_mapping[orig_attr_no] = attr_no;
  no_aggregated_attr++;
  no_attr++;
}

void GroupByWrapper::Initialize(int64_t upper_approx_of_groups, bool parallel_allowed) {
  // MEASURE_FET("GroupByWrapper::Initialize(...)");
  gt.Initialize(upper_approx_of_groups, parallel_allowed);
  distinct_watch.Initialize(no_attr);
  for (int gr_a = 0; gr_a < no_attr; gr_a++) {
    if (gt.AttrDistinct(gr_a)) {
      distinct_watch.DeclareAsDistinct(gr_a);
    }
  }
  pack_not_omitted = new bool[no_attr];
}

void GroupByWrapper::Merge(GroupByWrapper &sec) {
  int64_t old_groups = gt.GetNoOfGroups();
  gt.Merge(sec.gt, m_conn);
  if (tuple_left)
    tuple_left->And(*(sec.tuple_left));
  packrows_omitted += sec.packrows_omitted;
  packrows_part_omitted += sec.packrows_part_omitted;

  // note that no_groups may be different than gt->..., because it is global
  no_groups += gt.GetNoOfGroups() - old_groups;
}

bool GroupByWrapper::AggregatePackInOneGroup(int attr_no, MIIterator &mit, int64_t uniform_pos, int64_t rows_in_pack,
                                             int64_t factor) {
  bool no_omitted = (rows_in_pack == mit.GetPackSizeLeft());
  bool aggregated_roughly = false;
  if (virt_col[attr_no] && virt_col[attr_no]->GetNumOfNulls(mit) == mit.GetPackSizeLeft() &&
      gt.AttrAggregator(attr_no)->IgnoreNulls())
    return true;  // no operation needed - the pack is ignored
  if (!gt.AttrAggregator(attr_no)->PackAggregationDistinctIrrelevant() && gt.AttrDistinct(attr_no) &&
      virt_col[attr_no]) {
    // Aggregated values for distinct cases (if possible)
    std::vector<int64_t> val_list = virt_col[attr_no]->GetListOfDistinctValues(mit);
    if (val_list.size() == 0 || (val_list.size() > 1 && !no_omitted))
      return false;
    auto val_it = val_list.begin();
    while (val_it != val_list.end()) {
      GDTResult res = gt.FindDistinctValue(attr_no, uniform_pos, *val_it);
      if (res == GDTResult::GDT_FULL)
        return false;  // no chance to optimize
      if (res == GDTResult::GDT_EXISTS)
        val_it = val_list.erase(val_it);
      else
        ++val_it;
    }
    if (val_list.size() == 0)
      return true;  // no need to analyze pack - all values already found
    if (gt.AttrAggregator(attr_no)->PackAggregationNeedsSize())
      gt.AttrAggregator(attr_no)->SetAggregatePackNoObj(val_list.size());
    if (gt.AttrAggregator(attr_no)->PackAggregationNeedsNotNulls())
      gt.AttrAggregator(attr_no)->SetAggregatePackNotNulls(val_list.size());
    if (gt.AttrAggregator(attr_no)->PackAggregationNeedsSum()) {
      int64_t i_sum = 0;
      val_it = val_list.begin();
      while (val_it != val_list.end()) {
        i_sum += *val_it;  // not working for more than one double value
        ++val_it;
      }
      gt.AttrAggregator(attr_no)->SetAggregatePackSum(i_sum, 1);  // no factor for distinct
    }
    if (gt.AttrAggregator(attr_no)->PackAggregationNeedsMin() && virt_col[attr_no]) {
      int64_t i_min = virt_col[attr_no]->GetMinInt64Exact(mit);
      if (i_min == common::NULL_VALUE_64)
        return false;  // aggregation no longer possible
      else
        gt.AttrAggregator(attr_no)->SetAggregatePackMin(i_min);
    }
    if (gt.AttrAggregator(attr_no)->PackAggregationNeedsMax() && virt_col[attr_no]) {
      int64_t i_max = virt_col[attr_no]->GetMaxInt64Exact(mit);
      if (i_max == common::NULL_VALUE_64)
        return false;  // aggregation no longer possible
      else
        gt.AttrAggregator(attr_no)->SetAggregatePackMax(i_max);
    }
    aggregated_roughly = gt.AggregatePack(attr_no,
                                          uniform_pos);  // check the aggregation basing on the above parameters
    if (aggregated_roughly) {
      // commit the added values
      val_it = val_list.begin();
      while (val_it != val_list.end()) {
        gt.AddDistinctValue(attr_no, uniform_pos, *val_it);
        ++val_it;
      }
    }
  } else {
    // Aggregated values for non-distinct cases

    if (gt.AttrAggregator(attr_no)->FactorNeeded() && factor == common::NULL_VALUE_64)
      throw common::NotImplementedException("Aggregation overflow.");
    if (gt.AttrAggregator(attr_no)->PackAggregationNeedsSize())
      gt.AttrAggregator(attr_no)->SetAggregatePackNoObj(SafeMultiplication(rows_in_pack, factor));
    if (gt.AttrAggregator(attr_no)->PackAggregationNeedsNotNulls() && virt_col[attr_no]) {
      int64_t no_nulls = virt_col[attr_no]->GetNumOfNulls(mit);
      if (no_nulls == common::NULL_VALUE_64)
        return false;  // aggregation no longer possible
      else
        gt.AttrAggregator(attr_no)->SetAggregatePackNotNulls(SafeMultiplication(rows_in_pack - no_nulls, factor));
    }
    if (gt.AttrAggregator(attr_no)->PackAggregationNeedsSum() && virt_col[attr_no]) {
      bool nonnegative = false;  // not used anyway
      int64_t i_sum = virt_col[attr_no]->GetSum(mit, nonnegative);
      if (i_sum == common::NULL_VALUE_64)
        return false;  // aggregation no longer possible
      else
        gt.AttrAggregator(attr_no)->SetAggregatePackSum(i_sum, factor);
    }
    if (gt.AttrAggregator(attr_no)->PackAggregationNeedsMin() && virt_col[attr_no]) {
      int64_t i_min = virt_col[attr_no]->GetMinInt64Exact(mit);
      if (i_min == common::NULL_VALUE_64)
        return false;  // aggregation no longer possible
      else
        gt.AttrAggregator(attr_no)->SetAggregatePackMin(i_min);
    }
    if (gt.AttrAggregator(attr_no)->PackAggregationNeedsMax() && virt_col[attr_no]) {
      int64_t i_max = virt_col[attr_no]->GetMaxInt64Exact(mit);
      if (i_max == common::NULL_VALUE_64)
        return false;  // aggregation no longer possible
      else
        gt.AttrAggregator(attr_no)->SetAggregatePackMax(i_max);
    }
    aggregated_roughly = gt.AggregatePack(attr_no,
                                          uniform_pos);  // check the aggregation basing on the above parameters
  }
  return aggregated_roughly;
}

bool GroupByWrapper::AddPackIfUniform(int attr_no, MIIterator &mit) {
  if (virt_col[attr_no] && virt_col[attr_no]->GetPackOntologicalStatus(mit) == PackOntologicalStatus::UNIFORM &&
      !mit.NullsPossibleInPack()) {
    // Put constant values for the grouping vector (will not be changed for this
    // pack)
    gt.PutUniformGroupingValue(attr_no, mit);
    return true;
  }
  return false;
}

void GroupByWrapper::AddAllGroupingConstants(MIIterator &mit) {
  for (int attr_no = 0; attr_no < no_grouping_attr; attr_no++)
    if (virt_col[attr_no] && virt_col[attr_no]->IsConst()) {
      PutGroupingValue(attr_no, mit);
    }
}

void GroupByWrapper::AddAllAggregatedConstants(MIIterator &mit) {
  for (int attr_no = no_grouping_attr; attr_no < no_attr; attr_no++)
    if (virt_col[attr_no] && virt_col[attr_no]->IsConst()) {
      if (mit.NumOfTuples() > 0 || gt.AttrOper(attr_no) == GT_Aggregation::GT_LIST) {
        if (!(gt.AttrOper(attr_no) == GT_Aggregation::GT_COUNT && virt_col[attr_no]->IsNull(mit)))  // else left as 0
          PutAggregatedValue(attr_no, 0, mit);
      } else
        PutAggregatedNull(attr_no, 0);
    }
}

void GroupByWrapper::AddAllCountStar(int64_t row, MIIterator &mit,
                                     int64_t val)  // set all count(*) values
{
  for (int gr_a = no_grouping_attr; gr_a < no_attr; gr_a++) {
    if ((virt_col[gr_a] == nullptr || virt_col[gr_a]->IsConst()) && gt.AttrOper(gr_a) == GT_Aggregation::GT_COUNT &&
        !gt.AttrDistinct(gr_a)) {
      if (virt_col[gr_a] && virt_col[gr_a]->IsNull(mit))
        PutAggregatedValueForCount(gr_a, row, 0);
      else
        PutAggregatedValueForCount(gr_a, row,
                                   val);  // note: mit.NumOfTuples() may be 0 in some not-used cases
    }
  }
}

bool GroupByWrapper::AttrMayBeUpdatedByPack(int i, MIIterator &mit)  // false, if pack is out of (grouping) scope
{
  return gt.AttrMayBeUpdatedByPack(i, mit);
}

bool GroupByWrapper::PackWillNotUpdateAggregation(int i, MIIterator &mit)  // false, if counters can be changed
{
  // MEASURE_FET("GroupByWrapper::PackWillNotUpdateAggregation(...)");
  DEBUG_ASSERT(input_mode[i] != GBInputMode::GBIMODE_NOT_SET);
  if (((is_lookup[i] || input_mode[i] == GBInputMode::GBIMODE_AS_TEXT) &&
       (gt.AttrOper(i) == GT_Aggregation::GT_MIN || gt.AttrOper(i) == GT_Aggregation::GT_MAX)) ||
      virt_col[i] == nullptr)
    return false;

  // Optimization: do not recalculate statistics if there is too much groups
  if (gt.GetNoOfGroups() > 1024)
    return false;

  // Statistics of aggregator:
  gt.UpdateAttrStats(i);  // Warning: slow if there is a lot of groups involved

  // Statistics of data pack:
  if (virt_col[i]->GetNumOfNulls(mit) == mit.GetPackSizeLeft())
    return true;  // nulls only - omit pack

  if (gt.AttrAggregator(i)->PackAggregationNeedsMin()) {
    int64_t i_min = virt_col[i]->GetMinInt64(mit);
    gt.AttrAggregator(i)->SetAggregatePackMin(i_min);
  }
  if (gt.AttrAggregator(i)->PackAggregationNeedsMax()) {
    int64_t i_max = virt_col[i]->GetMaxInt64(mit);
    gt.AttrAggregator(i)->SetAggregatePackMax(i_max);
  }

  // Check the possibility of omitting pack
  return gt.AttrAggregator(i)->PackCannotChangeAggregation();  // true => omit
}

bool GroupByWrapper::DataWillNotUpdateAggregation(int i)  // false, if counters can be changed
{
  // Identical with PackWillNot...(), but calculated for global statistics
  DEBUG_ASSERT(input_mode[i] != GBInputMode::GBIMODE_NOT_SET);
  if (((is_lookup[i] || input_mode[i] == GBInputMode::GBIMODE_AS_TEXT) &&
       (gt.AttrOper(i) == GT_Aggregation::GT_MIN || gt.AttrOper(i) == GT_Aggregation::GT_MAX)) ||
      virt_col[i] == nullptr)
    return false;

  // Optimization: do not recalculate statistics if there is too much groups
  if (gt.GetNoOfGroups() > 1024)
    return false;

  // Statistics of aggregator:
  gt.UpdateAttrStats(i);  // Warning: slow if there is a lot of groups involved

  // Statistics of the whole data:
  if (virt_col[i]->IsRoughNullsOnly())
    return true;  // nulls only - omit pack

  if (gt.AttrAggregator(i)->PackAggregationNeedsMin()) {
    int64_t i_min = virt_col[i]->RoughMin();
    gt.AttrAggregator(i)->SetAggregatePackMin(i_min);
  }
  if (gt.AttrAggregator(i)->PackAggregationNeedsMax()) {
    int64_t i_max = virt_col[i]->RoughMax();
    gt.AttrAggregator(i)->SetAggregatePackMax(i_max);
  }

  // Check the possibility of omitting the rest of packs
  return gt.AttrAggregator(i)->PackCannotChangeAggregation();  // true => omit
}

bool GroupByWrapper::PutAggregatedValueForCount(int gr_a, int64_t pos, int64_t factor) {
  DEBUG_ASSERT(gt.AttrOper(gr_a) == GT_Aggregation::GT_COUNT || gt.AttrOper(gr_a) == GT_Aggregation::GT_COUNT_NOT_NULL);
  return gt.PutAggregatedValue(gr_a, pos, factor);
}

bool GroupByWrapper::PutAggregatedValueForMinMax(int gr_a, int64_t pos, int64_t factor) {
  DEBUG_ASSERT(gt.AttrOper(gr_a) == GT_Aggregation::GT_MIN || gt.AttrOper(gr_a) == GT_Aggregation::GT_MAX);
  return gt.PutAggregatedValue(gr_a, pos, factor);
}

bool GroupByWrapper::PutAggregatedNull(int gr_a, int64_t pos) {
  DEBUG_ASSERT(input_mode[gr_a] != GBInputMode::GBIMODE_NOT_SET);
  return gt.PutAggregatedNull(gr_a, pos, (input_mode[gr_a] == GBInputMode::GBIMODE_AS_TEXT));
  return false;
}

bool GroupByWrapper::PutAggregatedValue(int gr_a, int64_t pos, MIIterator &mit, int64_t factor) {
  DEBUG_ASSERT(input_mode[gr_a] != GBInputMode::GBIMODE_NOT_SET);
  if (input_mode[gr_a] == GBInputMode::GBIMODE_NO_VALUE)
    return gt.PutAggregatedValue(gr_a, pos, factor);
  return gt.PutAggregatedValue(gr_a, pos, mit, factor, (input_mode[gr_a] == GBInputMode::GBIMODE_AS_TEXT));
}

types::BString GroupByWrapper::GetValueT(int col, int64_t row) {
  if (is_lookup[col]) {
    int64_t v = GetValue64(col, row);  // lookup code
    return virt_col[col]->DecodeValue_S(v);
  }
  return gt.GetValueT(col, row);
}

void GroupByWrapper::FillDimsUsed(DimensionVector &dims)  // set true on all dimensions used
{
  for (int i = 0; i < no_attr; i++) {
    if (virt_col[i])
      virt_col[i]->MarkUsedDims(dims);
  }
}

int64_t GroupByWrapper::ApproxDistinctVals(int gr_a, [[maybe_unused]] MultiIndex *mind) {
  if (dist_vals[gr_a] == common::NULL_VALUE_64)
    dist_vals[gr_a] = virt_col[gr_a]->GetApproxDistVals(true);  // incl. nulls, because they may define a group
  return dist_vals[gr_a];
}

void GroupByWrapper::DistinctlyOmitted(int attr, int64_t obj) {
  distinct_watch.SetAsOmitted(attr, obj);
  gt.AddCurrentValueToCache(attr, distinct_watch.gd_cache[attr]);
  if (distinct_watch.gd_cache[attr].NextWrite() == false)
    throw common::OutOfMemoryException();  // check limitations in
                                           // GroupDistinctCache::Initialize
}

void GroupByWrapper::RewindDistinctBuffers() {
  gt.ClearDistinct();
  for (int i = 0; i < no_attr; i++)
    if (DistinctAggr(i))
      distinct_watch.gd_cache[i].Rewind();
}

void GroupByWrapper::Clear()  // reset all contents of the grouping table and
                              // statistics
{
  packrows_omitted = 0;
  packrows_part_omitted = 0;
  no_groups = 0;
  gt.ClearAll();
}

void GroupByWrapper::ClearDistinctBuffers() {
  gt.ClearDistinct();
  for (int i = 0; i < no_attr; i++)
    if (DistinctAggr(i))
      distinct_watch.gd_cache[i].Reset();
}

bool GroupByWrapper::PutCachedValue(int gr_a)  // current value from distinct cache
{
  DEBUG_ASSERT(input_mode[gr_a] != GBInputMode::GBIMODE_NOT_SET);
  bool added =
      gt.PutCachedValue(gr_a, distinct_watch.gd_cache[gr_a], (input_mode[gr_a] == GBInputMode::GBIMODE_AS_TEXT));
  if (!added)
    distinct_watch.gd_cache[gr_a].MarkCurrentAsPreserved();
  return added;
}

bool GroupByWrapper::CacheValid(int gr_a)  // true if there is a value cached for current row
{
  return (distinct_watch.gd_cache[gr_a].GetCurrentValue() != nullptr);
}

void GroupByWrapper::OmitInCache(int gr_a, int64_t obj_to_omit) { distinct_watch.gd_cache[gr_a].Omit(obj_to_omit); }

void GroupByWrapper::UpdateDistinctCaches()  // take into account which values
                                             // are already counted
{
  for (int i = 0; i < no_attr; i++)
    if (DistinctAggr(i))
      distinct_watch.gd_cache[i].SwitchToPreserved();  // ignored if n/a
}

bool GroupByWrapper::IsCountOnly(int gr_a)  // true, if an attribute is count(*)/count(const), or if
                                            // there is no columns except constants and count (when
                                            // no attr specified)
{
  if (gr_a != -1) {
    return (virt_col[gr_a] == nullptr || virt_col[gr_a]->IsConst()) && gt.AttrOper(gr_a) == GT_Aggregation::GT_COUNT &&
           !gt.AttrDistinct(gr_a);
  }
  bool count_found = false;
  for (int i = 0; i < no_attr; i++) {  // function should return true for e.g.: "SELECT 1, 2,
                                       // 'ala', COUNT(*), 5, COUNT(4) FROM ..."
    if (gt.AttrOper(i) == GT_Aggregation::GT_COUNT)
      count_found = true;
    if (!((virt_col[i] == nullptr || virt_col[i]->IsConst()) && gt.AttrOper(i) == GT_Aggregation::GT_COUNT &&
          !gt.AttrDistinct(i))                                                      // count(*) or count(const)
        && (gt.AttrOper(i) != GT_Aggregation::GT_LIST || !virt_col[i]->IsConst()))  // a constant
      return false;
  }
  return count_found;
}

bool GroupByWrapper::IsMinOnly()  // true, if an attribute is min(column), and
                                  // the attribute is the only attribute in the
                                  // SQL
{
  // function should return true for e.g.: "SELECT MIN(ID) FROM ...",
  // otherwise return false.
  if (no_attr != 1) {
    return false;
  }
  if (gt.AttrOper(0) == GT_Aggregation::GT_MIN && no_grouping_attr == 0 && no_aggregated_attr == 1) {
    return true;
  }
  return false;
}

bool GroupByWrapper::IsMaxOnly()  // true, if an attribute is max(column), and
                                  // the attribute is the only attribute in the
                                  // SQL
{
  // function should return true for e.g.: "SELECT MAX(ID) FROM ..."
  // otherwise return false.
  if (no_attr != 1) {
    return false;
  }
  if (gt.AttrOper(0) == GT_Aggregation::GT_MAX && no_grouping_attr == 0 && no_aggregated_attr == 1) {
    return true;
  }
  return false;
}

void GroupByWrapper::InitTupleLeft(int64_t n) {
  if (tuple_left) {
    delete tuple_left;
    tuple_left = nullptr;
  }
  DEBUG_ASSERT(tuple_left == nullptr);
  tuple_left = new Filter(n, p_power);
  tuple_left->Set();
}

bool GroupByWrapper::AnyTuplesLeft(int64_t from, int64_t to) {
  if (tuple_left == nullptr)
    return true;
  return !tuple_left->IsEmptyBetween(from, to);
}

int64_t GroupByWrapper::TuplesLeftBetween(int64_t from, int64_t to) {
  if (tuple_left == nullptr)
    return to - from + 1;
  return tuple_left->NumOfOnesBetween(from, to);
}

bool GroupByWrapper::MayBeParallel() const {
  if (!gt.MayBeParallel())
    return false;
  for (int n = 0; n < attrs_size; n++)
    if (virt_col[n] && !virt_col[n]->IsThreadSafe())
      return false;

  return true;
}

void GroupByWrapper::LockPack(int i, MIIterator &mit) {
  if (ColumnNotOmitted(i) && virt_col[i])
    virt_col[i]->LockSourcePacks(mit);
}

void GroupByWrapper::LockPackAlways(int i, MIIterator &mit) {
  if (virt_col[i])
    virt_col[i]->LockSourcePacks(mit);
}

void GroupByWrapper::ResetPackrow() {
  for (int i = 0; i < no_attr; i++) {
    pack_not_omitted[i] = true;  // reset information about packrow
  }
}

void DistinctWrapper::Initialize(int n_attr) {
  no_attr = n_attr;
  f.resize(no_attr);
  is_dist.resize(no_attr);
  gd_cache.resize(no_attr);
}

void DistinctWrapper::InitTuples(int64_t n_obj, const GroupTable &gt) {
  for (int i = 0; i < no_attr; i++)
    if (is_dist[i]) {
      // for now - init cache with a number of objects decreased (will cache on
      // disk if more is needed)
      gd_cache[i].SetNumOfObj(n_obj);
      DEBUG_ASSERT(f[i] == nullptr);
      f[i].reset(new Filter(n_obj, p_power));
      gd_cache[i].SetWidth(gt.GetCachedWidth(i));
    }
}

bool DistinctWrapper::AnyOmitted() {
  for (int i = 0; i < no_attr; i++)
    if (f[i] && !(f[i]->IsEmpty()))
      return true;
  return false;
}
}  // namespace core
}  // namespace Tianmu
