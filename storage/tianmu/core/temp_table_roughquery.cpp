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

/*
        This is a part of TempTable implementation concerned with the rough
   query execution
*/

#include "core/engine.h"
#include "core/mi_iterator.h"
#include "core/pack_orderer.h"
#include "core/temp_table.h"
#include "vc/single_column.h"

namespace Tianmu {
namespace core {

void TempTable::RoughMaterialize([[maybe_unused]] bool in_subq, ResultSender *sender, [[maybe_unused]] bool lazy) {
  MEASURE_FET("Descriptor::RoughMaterialize(...)");
  if (materialized)
    return;
  filter.PrepareRoughMultiIndex();
  RoughAggregate(sender);  // Always use RoughAggregate (table distinct, order
                           // by, having, etc. are ignored)
}

int64_t UpdateMin(int64_t old_min, int64_t v, bool double_val) {
  if (old_min == common::NULL_VALUE_64)
    return v;
  if (v == common::NULL_VALUE_64)
    return old_min;
  if (double_val) {
    if (*(double *)&old_min > *(double *)&v)
      return v;
  } else {
    if (old_min > v)
      return v;
  }
  return old_min;
}

int64_t UpdateMax(int64_t old_max, int64_t v, bool double_val) {
  if (old_max == common::NULL_VALUE_64)
    return v;
  if (v == common::NULL_VALUE_64)
    return old_max;
  if (double_val) {
    if (*(double *)&old_max < *(double *)&v)
      return v;
  } else {
    if (old_max < v)
      return v;
  }
  return old_max;
}

void TempTable::RoughAggregateMinMax(vcolumn::VirtualColumn *vc, int64_t &min_val, int64_t &max_val) {
  int dim = vc->GetDim();
  if (dim == -1) {
    min_val = vc->RoughMin();
    max_val = vc->RoughMax();
    return;
  }
  bool double_vals = vc->Type().IsFloat();
  MIIterator mit(filter.mind_, dim, true);
  while (mit.IsValid()) {
    if (filter.rough_mind_->GetPackStatus(dim, mit.GetCurPackrow(dim)) != common::RoughSetValue::RS_NONE &&
        vc->GetPackOntologicalStatus(mit) != PackOntologicalStatus::NULLS_ONLY) {
      int64_t v = vc->GetMinInt64(mit);
      if (v == common::NULL_VALUE_64)
        min_val = common::MINUS_INF_64;
      else
        min_val = UpdateMin(min_val, v, double_vals);
      v = vc->GetMaxInt64(mit);
      if (v == common::NULL_VALUE_64)
        max_val = common::PLUS_INF_64;
      else
        max_val = UpdateMax(max_val, v, double_vals);
    }
    mit.NextPackrow();
  }
  if (max_val < min_val)
    min_val = max_val = common::NULL_VALUE_64;
}

void TempTable::RoughAggregateCount(DimensionVector &dims, int64_t &min_val, int64_t &max_val, bool group_by_present) {
  for (int dim = 0; dim < dims.Size(); dim++)
    if (dims[dim]) {
      MIIterator mit(filter.mind_, dim, true);
      int64_t loc_min = 0;
      int64_t loc_max = 0;
      while (mit.IsValid()) {
        common::RoughSetValue res = filter.rough_mind_->GetPackStatus(dim, mit.GetCurPackrow(dim));
        if (res != common::RoughSetValue::RS_NONE) {
          loc_max += mit.GetPackSizeLeft();
          if (!group_by_present && res == common::RoughSetValue::RS_ALL)
            loc_min += mit.GetPackSizeLeft();
        }
        mit.NextPackrow();
      }
      if (min_val == common::NULL_VALUE_64)
        min_val = loc_min;
      else {
        min_val = SafeMultiplication(min_val, loc_min);
        if (min_val == common::NULL_VALUE_64)
          min_val = 0;
      }
      if (max_val == common::NULL_VALUE_64)
        max_val = loc_max;
      else {
        max_val = SafeMultiplication(max_val, loc_max);
        if (max_val == common::NULL_VALUE_64)
          max_val = common::PLUS_INF_64;
      }
    }
}

void TempTable::RoughAggregateSum(vcolumn::VirtualColumn *vc, int64_t &min_val, int64_t &max_val,
                                  std::vector<Attr *> &group_by_attrs, bool nulls_only, bool distinct_present) {
  int dim = vc->GetDim();
  bool double_vals = vc->Type().IsFloat();
  bool is_const = vc->IsConst();
  double min_val_d = (is_const ? 1 : 0);
  double max_val_d = (is_const ? 1 : 0);
  bool success = false;  // left as false for empty set
  bool empty_set = true;
  bool group_by_present = (group_by_attrs.size() > 0);
  if (!nulls_only && !is_const) {
    MIIterator mit(filter.mind_, dim, true);
    mit.Rewind();
    while (mit.IsValid()) {
      common::RoughSetValue res = filter.rough_mind_->GetPackStatus(dim, mit.GetCurPackrow(dim));
      bool no_groups_or_uniform = true;  // false if there is a nontrivial grouping (more than one group
                                         // possible)
      for (uint j = 0; j < group_by_attrs.size(); j++) {
        vcolumn::VirtualColumn *vc_gb = group_by_attrs[j]->term.vc;
        if (vc_gb == nullptr || vc_gb->GetNumOfNulls(mit) != 0 || vc_gb->GetMinInt64(mit) == common::NULL_VALUE_64 ||
            vc_gb->GetMinInt64(mit) != vc_gb->GetMaxInt64(mit))
          no_groups_or_uniform = false;  // leave it true only when we are sure the
                                         // grouping columns are uniform for this packrow
      }
      if (res != common::RoughSetValue::RS_NONE &&
          vc->GetPackOntologicalStatus(mit) != PackOntologicalStatus::NULLS_ONLY) {
        empty_set = false;
        success = true;
        bool nonnegative = false;
        int64_t v = vc->GetSum(mit, nonnegative);
        if (no_groups_or_uniform && res == common::RoughSetValue::RS_ALL && !distinct_present) {
          if (v == common::NULL_VALUE_64) {  // unknown sum
            success = false;
            break;
          }
          if (!group_by_present) {
            if (double_vals) {
              min_val_d += *(double *)&v;
              max_val_d += *(double *)&v;
            } else {
              min_val_d += v;
              max_val_d += v;
            }
          } else {  // grouping by on uniform pack
            // cannot approximate minimum (unknown distribution of groups in
            // other packs)
            if (double_vals) {
              max_val_d += *(double *)&v;
            } else {
              max_val_d += v;
            }
          }
        } else {
          if (nonnegative) {
            // nonnegative: minimum not changed, maximum limited by sum
            if (v == common::NULL_VALUE_64)
              v = vc->GetApproxSum(mit, nonnegative);
            if (double_vals)
              max_val_d += *(double *)&v;
            else
              max_val_d += v;
            if (v == common::NULL_VALUE_64) {
              success = false;
              break;
            }
          } else {
            // negative values possible: approximation by min/max
            v = vc->GetMinInt64(mit);
            if (v == common::MINUS_INF_64) {
              success = false;
              break;
            }
            if (v < 0) {
              if (double_vals)
                min_val_d += (*(double *)&v) * mit.GetPackSizeLeft();
              else
                min_val_d += double(v) * mit.GetPackSizeLeft();
            }  // else it is actually nonnegative
            v = vc->GetMaxInt64(mit);
            if (v == common::PLUS_INF_64) {
              success = false;
              break;
            }
            if (double_vals)
              max_val_d += (*(double *)&v) * mit.GetPackSizeLeft();
            else
              max_val_d += double(v) * mit.GetPackSizeLeft();
          }
        }
      }
      mit.NextPackrow();
    }
  }
  if (is_const) {
    int64_t min_count = common::NULL_VALUE_64;
    int64_t max_count = common::NULL_VALUE_64;
    DimensionVector other_dims(filter.mind_->NumOfDimensions());
    other_dims.SetAll();
    RoughAggregateCount(other_dims, min_count, max_count, group_by_present);
    MIIterator mit(filter.mind_, dim, true);
    mit.Rewind();
    int64_t val = vc->GetValueInt64(mit);
    if (double_vals) {
      min_val_d = *(double *)&val * min_count;
      max_val_d = *(double *)&val * max_count;
      min_val = *(int64_t *)&min_val_d;
      max_val = *(int64_t *)&max_val_d;
    } else {
      min_val = val * min_count;
      max_val = val * max_count;
    }
    if ((max_count == 0 && min_count == 0) || val == common::NULL_VALUE_64) {
      min_val = common::NULL_VALUE_64;
      max_val = common::NULL_VALUE_64;
    }
  } else if (success) {
    empty_set = false;
    if (filter.mind_->NumOfDimensions() > 1) {
      int64_t min_count = common::NULL_VALUE_64;
      int64_t max_count = common::NULL_VALUE_64;
      DimensionVector other_dims(filter.mind_->NumOfDimensions());
      other_dims.SetAll();
      other_dims[dim] = false;
      RoughAggregateCount(other_dims, min_count, max_count, group_by_present);
      min_val_d *= (min_val_d < 0 ? max_count : min_count);
      max_val_d *= (max_val_d < 0 ? min_count : max_count);
    }
    if (double_vals || (min_val_d > -9.223372037e+18 && max_val_d < 9.223372037e+18)) {  // overflow check
      if (double_vals) {
        min_val = *(int64_t *)&min_val_d;
        max_val = *(int64_t *)&max_val_d;
      } else {
        min_val = int64_t(min_val_d);
        max_val = int64_t(max_val_d);
      }
    }
  } else if (empty_set) {
    min_val = common::NULL_VALUE_64;
    max_val = common::NULL_VALUE_64;
  } else {
    min_val = common::MINUS_INF_64;  // +-INF
    max_val = common::PLUS_INF_64;
  }
}

bool IsTempTableColumn(vcolumn::VirtualColumn *vc) {
  vcolumn::SingleColumn *sc =
      ((vc && static_cast<int>(vc->IsSingleColumn())) ? static_cast<vcolumn::SingleColumn *>(vc) : nullptr);
  return (sc && sc->IsTempTableColumn());
}

void TempTable::RoughAggregate(ResultSender *sender) {
  MEASURE_FET("TempTable::RoughAggregate(...)");
  /*
        Assumptions:
        filter.mind_			- multiindex with nontrivial contents,
     although not necessarily updated by conditions filter.rough_mind_	- rough
     multiindex with more up-to-date contents than mind_, i.e. a packrow may
     exist in mind_, but be marked as common::RoughSetValue::RS_NONE in rough_mind_ To check a
     rough status of a packrow, use both mind_ and rough_mind_. The method does
     not change mind_ / rough_mind_.

        Interpretation of the result:
        Minimal and maximal possible value for a given column, if executed as
        exact. nullptr if not known (unable to determine).
  */

  // filter.Prepare();
  bool group_by_present = false;
  bool aggregation_present = false;
  for (uint i = 0; i < attrs.size(); i++) {
    if (attrs[i]->mode != common::ColOperation::LISTING && attrs[i]->mode != common::ColOperation::GROUP_BY &&
        attrs[i]->mode != common::ColOperation::DELAYED)
      aggregation_present = true;  // changing interpretation of result:
                                   // statistics of possible value in any group
    if (attrs[i]->mode == common::ColOperation::GROUP_BY)
      group_by_present = true;  // changing interpretation of result: statistics
                                // of possible value in any group
  }

  // Rough values for EXIST
  rough_is_empty = common::TRIBOOL_UNKNOWN;        // no_obj > 0 ? false - non-empty
                                                   // for sure, true - empty for sure
  if (!aggregation_present || group_by_present) {  // otherwise even empty multiindex may produce
                                                   // nonempty result - checked later
    rough_is_empty = false;
    for (int dim = 0; dim < filter.mind_->NumOfDimensions(); dim++) {
      bool local_empty = true;
      bool local_some = true;  // true if no pack is full
      for (int pack = 0; pack < filter.rough_mind_->NumOfPacks(dim); pack++) {
        common::RoughSetValue res = filter.rough_mind_->GetPackStatus(dim, pack);
        if (res != common::RoughSetValue::RS_NONE) {
          local_empty = false;
          if (rough_is_empty != false)
            break;
        }
        if (res == common::RoughSetValue::RS_ALL) {
          local_some = false;
          break;
        }
      }
      if (local_empty) {
        rough_is_empty = true;
        break;
      }
      if (local_some)
        rough_is_empty = common::TRIBOOL_UNKNOWN;  // cannot be false any more
    }
  }
  if (!group_by_present && aggregation_present)
    rough_is_empty = false;

  CalculatePageSize(2);  // 2 rows in result

  if (rough_is_empty == true || (mode.top && mode.param2 == 0)) {  // empty or "limit 0"
    no_obj = 0;
    materialized = true;
    if (sender)
      sender->Send(this);
    return;
  }

  // Rough sorting / limit
  if (!aggregation_present && !group_by_present && !mode.distinct && mode.top && mode.param2 > -1 &&
      filter.mind_->NumOfDimensions() == 1) {
    int64_t local_limit = mode.param1 + mode.param2;
    if (order_by.size() > 0) {
      vcolumn::VirtualColumn *vc;
      vc = order_by[0].vc;
      bool asc = (order_by[0].dir == 0);  // ascending sorting, if needed
      if (!vc->Type().IsString() && !vc->Type().Lookup() && vc->GetDim() == 0) {
        std::vector<PackOrderer> po(1);
        po[0].Init(vc,
                   (asc ? PackOrderer::OrderType::kMaxAsc
                        : PackOrderer::OrderType::kMinDesc),  // start with best packs to possibly
                                                              // roughly exclude others
                   filter.rough_mind_->GetRSValueTable(0));
        DimensionVector loc_dims(1);
        loc_dims[0] = true;
        MIIterator mit(filter.mind_, loc_dims, po);

        bool double_vals = vc->Type().IsFloat();
        int64_t cutoff_value = common::NULL_VALUE_64;
        int64_t certain_rows = 0;
        bool cutoff_is_null = false;  // true if all values up to limit are nullptr for ascending
        while (mit.IsValid()) {
          common::RoughSetValue res = filter.rough_mind_->GetPackStatus(0, mit.GetCurPackrow(0));
          if (res == common::RoughSetValue::RS_ALL) {
            // Algorithm for ascending:
            // - cutoff value is the maximum of the first full data pack which
            // hit the limit
            certain_rows += mit.GetPackSizeLeft();
            if (certain_rows >= local_limit) {
              cutoff_value = (asc ? vc->GetMaxInt64Exact(mit) : vc->GetMinInt64Exact(mit));
              if (asc && vc->GetNumOfNulls(mit) == mit.GetPackSizeLeft())
                cutoff_is_null = true;
              break;
            }
          }
          mit.NextPackrow();
        }
        if (cutoff_value != common::NULL_VALUE_64 || cutoff_is_null) {
          mit.Rewind();
          int64_t local_stat = common::NULL_VALUE_64;
          while (mit.IsValid()) {
            common::RoughSetValue res = filter.rough_mind_->GetPackStatus(0, mit.GetCurPackrow(0));
            if (res != common::RoughSetValue::RS_NONE) {
              bool omit = false;
              if (asc) {
                local_stat = vc->GetMinInt64(mit);  // omit if pack minimum is larger than cutoff
                if (!cutoff_is_null && local_stat != common::NULL_VALUE_64 &&
                    ((double_vals && *(double *)&local_stat > *(double *)&cutoff_value) ||
                     (!double_vals && local_stat > cutoff_value)))
                  omit = true;
                if (cutoff_is_null && vc->GetNumOfNulls(mit) == 0)
                  omit = true;
              } else {
                local_stat = vc->GetMaxInt64(mit);
                if (local_stat != common::NULL_VALUE_64 &&
                    ((double_vals && *(double *)&local_stat < *(double *)&cutoff_value) ||
                     (!double_vals && local_stat < cutoff_value)))
                  omit = true;
              }
              if (omit)
                filter.rough_mind_->SetPackStatus(0, mit.GetCurPackrow(0), common::RoughSetValue::RS_NONE);
            }
            mit.NextPackrow();
          }
        }
      }
    } else {
      int64_t certain_rows = 0;
      bool omit_the_rest = false;
      MIIterator mit(filter.mind_, filter.mind_->ValueOfPower());
      while (mit.IsValid()) {
        if (omit_the_rest) {
          filter.rough_mind_->SetPackStatus(0, mit.GetCurPackrow(0), common::RoughSetValue::RS_NONE);
        } else {
          common::RoughSetValue res = filter.rough_mind_->GetPackStatus(0, mit.GetCurPackrow(0));
          if (res == common::RoughSetValue::RS_ALL) {
            certain_rows += mit.GetPackSizeLeft();
            if (certain_rows >= local_limit)
              omit_the_rest = true;
          }
        }
        mit.NextPackrow();
      }
    }
  }

  // Rough values for columns
  for (uint i = 0; i < attrs.size(); i++) {
    bool value_set = false;
    attrs[i]->CreateBuffer(2);

    vcolumn::VirtualColumn *vc = attrs[i]->term.vc;
    bool nulls_only = vc ? (vc->GetLocalNullsOnly() || vc->IsRoughNullsOnly()) : false;
    types::BString vals;
    bool double_vals = (vc != nullptr && vc->Type().IsFloat());
    if (vc && vc->IsConst() && !vc->IsSubSelect() && attrs[i]->mode != common::ColOperation::SUM &&
        attrs[i]->mode != common::ColOperation::COUNT && attrs[i]->mode != common::ColOperation::BIT_XOR) {
      if (attrs[i]->mode == common::ColOperation::STD_POP || attrs[i]->mode == common::ColOperation::VAR_POP ||
          attrs[i]->mode == common::ColOperation::STD_SAMP || attrs[i]->mode == common::ColOperation::VAR_SAMP) {
        attrs[i]->SetValueInt64(0, 0);  // deviations for constants = 0
        attrs[i]->SetValueInt64(1, 0);
      } else {  // other rough values for constants: usually just these
                // constants
        MIIterator mit(filter.mind_, filter.mind_->ValueOfPower());
        if (vc->IsNull(mit)) {
          attrs[i]->SetNull(0);
          attrs[i]->SetNull(1);
        } else if (attrs[i]->mode == common::ColOperation::AVG) {
          int64_t val = vc->GetValueInt64(mit);
          val = vc->DecodeValueAsDouble(val);
          attrs[i]->SetValueInt64(0, val);
          attrs[i]->SetValueInt64(1, val);
        } else {
          switch (attrs[i]->TypeName()) {
            case common::ColumnType::STRING:
            case common::ColumnType::VARCHAR:
            case common::ColumnType::BIN:
            case common::ColumnType::BYTE:
            case common::ColumnType::VARBYTE:
            case common::ColumnType::LONGTEXT:
              vc->GetValueString(vals, mit);
              attrs[i]->SetValueString(0, vals);
              attrs[i]->SetValueString(1, vals);
              break;
            default:
              attrs[i]->SetValueInt64(0, vc->GetValueInt64(mit));
              attrs[i]->SetValueInt64(1, vc->GetValueInt64(mit));
              break;
          }
        }
      }
      value_set = true;
    } else {
      switch (attrs[i]->mode) {
        case common::ColOperation::LISTING:
        case common::ColOperation::GROUP_BY:  // Rough values of listed rows:
                                              // min and max of possible packs
        case common::ColOperation::AVG:       // easy implementation of AVG: between
                                              // min and max
          if (!vc->Type().IsString() && !vc->Type().Lookup()) {
            int64_t min_val = common::NULL_VALUE_64;
            int64_t max_val = common::NULL_VALUE_64;
            if (!nulls_only)
              RoughAggregateMinMax(vc, min_val, max_val);
            if (!double_vals && ATI::IsRealType(attrs[i]->TypeName()) && !nulls_only &&
                min_val != common::NULL_VALUE_64) {  // decimal column, double
                                                     // result (e.g. for AVG)
              min_val = vc->DecodeValueAsDouble(min_val);
              max_val = vc->DecodeValueAsDouble(max_val);
            }
            attrs[i]->SetValueInt64(0, min_val);
            attrs[i]->SetValueInt64(1, max_val);
            value_set = true;
          }
          break;
        case common::ColOperation::MIN:  // Rough min of MIN: minimum of all
                                         // possible packs
        case common::ColOperation::MAX:  // Rough min of MAX: maximum of
                                         // actual_max(relevant) and minimum of
                                         // min(suspect)
          // Rough max of MIN: minimum of actual_min(relevant) and maximum of
          // max(suspect)
          if (!vc->Type().IsString() && !vc->Type().Lookup() && vc->GetDim() != -1) {
            int dim = vc->GetDim();
            int64_t min_val = common::NULL_VALUE_64;
            int64_t max_val = common::NULL_VALUE_64;
            int64_t relevant_val = common::NULL_VALUE_64;
            bool is_min = (attrs[i]->mode == common::ColOperation::MIN);
            bool skip_counting = (IsTempTableColumn(vc) || SubqueryInFrom());
            if (!nulls_only) {
              MIIterator mit(filter.mind_, dim, true);
              while (mit.IsValid()) {
                common::RoughSetValue res = filter.rough_mind_->GetPackStatus(dim, mit.GetCurPackrow(dim));
                if (res != common::RoughSetValue::RS_NONE &&
                    vc->GetPackOntologicalStatus(mit) != PackOntologicalStatus::NULLS_ONLY) {
                  min_val = UpdateMin(min_val, vc->GetMinInt64(mit), double_vals);
                  max_val = UpdateMax(max_val, vc->GetMaxInt64(mit), double_vals);
                  if (!skip_counting && !group_by_present && res == common::RoughSetValue::RS_ALL) {
                    int64_t exact_val = is_min ? vc->GetMinInt64Exact(mit) : vc->GetMaxInt64Exact(mit);
                    if (exact_val != common::NULL_VALUE_64)
                      relevant_val = is_min ? UpdateMin(relevant_val, exact_val, double_vals)
                                            : UpdateMax(relevant_val, exact_val, double_vals);
                  }
                }
                mit.NextPackrow();
              }
              if (relevant_val != common::NULL_VALUE_64) {
                if (is_min)
                  max_val = UpdateMin(max_val, relevant_val,
                                      double_vals);  // take relevant_val, if smaller
                else
                  min_val = UpdateMax(min_val, relevant_val,
                                      double_vals);  // take relevant_val, if larger
              }
              if (max_val < min_val)
                min_val = max_val = common::NULL_VALUE_64;
            }
            attrs[i]->SetValueInt64(0, min_val);
            attrs[i]->SetValueInt64(1, max_val);
            value_set = true;
          }
          break;
        case common::ColOperation::COUNT: {
          int64_t min_val = common::NULL_VALUE_64;
          int64_t max_val = common::NULL_VALUE_64;
          DimensionVector dims(filter.mind_->NumOfDimensions());  // initialized as empty
          bool skip_counting = (IsTempTableColumn(vc) || SubqueryInFrom());
          if (vc && !attrs[i]->distinct && !skip_counting) {  // COUNT(a)
            int dim = vc->GetDim();
            if (dim != -1) {
              dims[dim] = true;
              MIIterator mit(filter.mind_, dim, true);
              min_val = 0;
              max_val = 0;
              if (!nulls_only) {
                while (mit.IsValid()) {
                  common::RoughSetValue res = filter.rough_mind_->GetPackStatus(dim, mit.GetCurPackrow(dim));
                  if (res != common::RoughSetValue::RS_NONE) {
                    max_val += mit.GetPackSizeLeft();
                    if (!group_by_present && res == common::RoughSetValue::RS_ALL) {
                      int64_t no_nulls = vc->GetNumOfNulls(mit);
                      if (no_nulls != common::NULL_VALUE_64) {
                        min_val += mit.GetPackSizeLeft() - no_nulls;
                        max_val -= no_nulls;
                      }
                    }
                  }
                  mit.NextPackrow();
                }
              }
            } else
              min_val = 0;
          } else if (vc && attrs[i]->distinct && !skip_counting) {  // COUNT(DISTINCT a)
            vc->MarkUsedDims(dims);
            MIIterator mit(filter.mind_, dims);
            min_val = 0;
            max_val = vc->GetApproxDistVals(
                false,
                filter.rough_mind_);  // Warning: mind_ used inside - may be more exact if rmind is also used

            // compare it with a rough COUNT(*)
            if (!nulls_only) {
              bool all_only = true;
              int64_t max_count_star = common::NULL_VALUE_64;
              int64_t min_count_star = 0;

              for (int dim = 0; dim < dims.Size(); dim++) {
                if (dims[dim]) {
                  MIIterator mit(filter.mind_, dim, true);
                  int64_t loc_max = 0;
                  while (mit.IsValid()) {
                    common::RoughSetValue res = filter.rough_mind_->GetPackStatus(dim, mit.GetCurPackrow(dim));
                    if (res != common::RoughSetValue::RS_NONE)
                      loc_max += mit.GetPackSizeLeft();

                    if (res == common::RoughSetValue::RS_ALL && !group_by_present)
                      min_count_star += mit.GetPackSizeLeft();

                    if (res != common::RoughSetValue::RS_ALL)
                      all_only = false;
                    mit.NextPackrow();
                  }

                  max_count_star =
                      (max_count_star == common::NULL_VALUE_64 ? loc_max : SafeMultiplication(max_count_star, loc_max));
                  if (max_count_star == common::NULL_VALUE_64)
                    max_count_star = common::PLUS_INF_64;

                  if (max_count_star > max_val)  // approx. distinct vals reached - stop execution
                    break;
                }
              }

              if (max_count_star < max_val)
                max_val = max_count_star;

              if (vc->IsDistinct() && dims.Size() == 1)
                min_val = min_count_star;

              if (all_only) {
                int64_t exact_dist = vc->GetExactDistVals();
                if (exact_dist != common::NULL_VALUE_64)
                  min_val = max_val = exact_dist;
              }
            } else
              max_val = 0;
          }

          // COUNT(*) or dimensions not covered by the above // all unused dimensions
          dims.Complement();

          if (!skip_counting)
            RoughAggregateCount(dims, min_val, max_val, group_by_present);
          else {
            min_val = 0;
            max_val = common::PLUS_INF_64;
          }

          attrs[i]->SetValueInt64(0, min_val);
          attrs[i]->SetValueInt64(1, max_val);
          value_set = true;
          break;
        }
        case common::ColOperation::SUM:
          // Rough min of SUM: positive only: sum of sums of relevant
          // Rough max of SUM: positive only: sum of sums of suspect
          if ((!vc->Type().IsString() && !vc->Type().Lookup() && vc->GetDim() != -1) || vc->IsConst()) {
            if (IsTempTableColumn(vc) || SubqueryInFrom()) {
              bool nonnegative = false;
              MIIterator mit(filter.mind_, vc->GetDim(), true);
              vc->GetSum(mit, nonnegative);
              attrs[i]->SetValueInt64(0, nonnegative ? 0 : common::MINUS_INF_64);  // +-INF
              attrs[i]->SetValueInt64(1, common::PLUS_INF_64);
              value_set = true;
              break;
            }
            int64_t min_val = common::NULL_VALUE_64;
            int64_t max_val = common::NULL_VALUE_64;
            std::vector<Attr *> group_by_attrs;
            for (uint j = 0; j < attrs.size(); j++)
              if (attrs[j]->mode == common::ColOperation::GROUP_BY)
                group_by_attrs.push_back(attrs[j]);
            RoughAggregateSum(vc, min_val, max_val, group_by_attrs, nulls_only, attrs[i]->distinct);
            if (min_val == common::NULL_VALUE_64 || max_val == common::NULL_VALUE_64) {
              attrs[i]->SetNull(0);  // nullptr as a result of empty set or non-computable sum
              attrs[i]->SetNull(1);
            } else {
              attrs[i]->SetValueInt64(0, min_val);
              attrs[i]->SetValueInt64(1, max_val);
            }
            value_set = true;
          }
          break;
        case common::ColOperation::BIT_AND:
          if (nulls_only) {
            attrs[i]->SetValueInt64(0, -1);  // unsigned 64-bit result
            attrs[i]->SetValueInt64(1, -1);
          } else {
            attrs[i]->SetValueInt64(0, 0);  // unsigned 64-bit result
            attrs[i]->SetValueInt64(1, -1);
          }
          value_set = true;
          break;
        case common::ColOperation::BIT_OR:
        case common::ColOperation::BIT_XOR:
          if (nulls_only) {
            attrs[i]->SetValueInt64(0, 0);  // unsigned 64-bit result
            attrs[i]->SetValueInt64(1, 0);
          } else {
            attrs[i]->SetValueInt64(0, 0);  // unsigned 64-bit result
            attrs[i]->SetValueInt64(1, -1);
          }
          value_set = true;
          break;
        case common::ColOperation::STD_POP:
        case common::ColOperation::STD_SAMP:
        case common::ColOperation::VAR_POP:
        case common::ColOperation::VAR_SAMP:
          if (!vc->Type().IsString() && !vc->Type().Lookup() && vc->GetDim() != -1) {
            int64_t min_val = common::NULL_VALUE_64;
            int64_t max_val = common::NULL_VALUE_64;
            if (!nulls_only) {
              RoughAggregateMinMax(vc, min_val, max_val);
              if (min_val != common::NULL_VALUE_64 && max_val != common::NULL_VALUE_64) {
                min_val = vc->DecodeValueAsDouble(min_val);  // decode to double
                max_val = vc->DecodeValueAsDouble(max_val);
                double diff = *(double *)(&max_val) - *(double *)(&min_val);
                if (attrs[i]->mode == common::ColOperation::VAR_POP || attrs[i]->mode == common::ColOperation::VAR_SAMP)
                  diff *= diff;
                attrs[i]->SetValueInt64(0, 0);
                attrs[i]->SetValueInt64(1, *(int64_t *)(&diff));  // May be approximated better. For
                                                                  // now Var <= difference^2
                value_set = true;
              } else {
                attrs[i]->SetValueInt64(0, common::NULL_VALUE_64);
                attrs[i]->SetValueInt64(1, common::NULL_VALUE_64);
                value_set = true;
              }
            } else {
              attrs[i]->SetValueInt64(0, common::NULL_VALUE_64);
              attrs[i]->SetValueInt64(1, common::NULL_VALUE_64);
              value_set = true;
            }
          }
          break;
        default:
          break;
      }
    }

    // Fill output buffers, if not filled yet
    if (!value_set) {
      if (ATI::IsStringType(attrs[i]->TypeName())) {
        attrs[i]->SetValueString(0, types::BString("*"));
        attrs[i]->SetValueString(1, types::BString("*"));
      } else {
        attrs[i]->SetMinusInf(0);
        attrs[i]->SetPlusInf(1);
      }
    }
  }
  no_obj = 2;
  materialized = true;
  if (sender)
    sender->Send(this);
}

void TempTable::RoughUnion(TempTable *t, ResultSender *sender) {
  if (!t) {
    this->RoughMaterialize(false);

    if (sender && !this->IsSent())
      sender->Send(this);

    return;
  }

  DEBUG_ASSERT(NumOfDisplaybleAttrs() == t->NumOfDisplaybleAttrs());

  if (NumOfDisplaybleAttrs() != t->NumOfDisplaybleAttrs())
    throw common::NotImplementedException("UNION of tables with different number of columns.");

  if (this->IsParametrized() || t->IsParametrized())
    throw common::NotImplementedException("Materialize: not implemented union of parameterized queries.");

  this->RoughMaterialize(false);
  t->RoughMaterialize(false);
  if (sender) {
    if (!this->IsSent())
      sender->Send(this);
    if (!t->IsSent())
      sender->Send(t);
    return;
  }

  for (uint i = 0; i < attrs.size(); i++)
    if (!attrs[i]->buffer)
      attrs[i]->CreateBuffer(2);

  int64_t pos = NumOfObj();
  no_obj += 2;
  for (uint i = 0; i < attrs.size(); i++) {
    if (IsDisplayAttr(i) && !ATI::IsStringType(attrs[i]->TypeName())) {
      int64_t v;
      if (GetColumnType(i).IsFloat()) {
        types::TianmuNum tn = (types::TianmuNum)t->GetValueObject(0, i);
        v = tn.ToReal().ValueInt();
      } else if (t->GetColumnType(i).IsFloat()) {
        types::TianmuNum tn = (types::TianmuNum)t->GetValueObject(0, i);
        v = (int64_t)((double)tn * types::PowOfTen(GetAttrScale(i)));
      } else {
        double multiplier = types::PowOfTen(GetAttrScale(i) - t->GetAttrScale(i));
        v = (int64_t)(t->GetTable64(0, i) * multiplier);
      }

      attrs[i]->SetValueInt64(pos, v);

      if (GetColumnType(i).IsFloat()) {
        types::TianmuNum tn = (types::TianmuNum)t->GetValueObject(1, i);
        v = tn.ToReal().ValueInt();
      } else if (t->GetColumnType(i).IsFloat()) {
        types::TianmuNum tn = (types::TianmuNum)t->GetValueObject(1, i);
        v = (int64_t)((double)tn * types::PowOfTen(GetAttrScale(i)));
      } else {
        double multiplier = types::PowOfTen(GetAttrScale(i) - t->GetAttrScale(i));
        v = (int64_t)(t->GetTable64(1, i) * multiplier);
      }
      attrs[i]->SetValueInt64(pos + 1, v);
    }
  }
}

}  // namespace core
}  // namespace Tianmu
