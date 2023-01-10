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

#include "condition_encoder.h"
#include "core/cq_term.h"
#include "core/descriptor.h"
#include "core/just_a_table.h"
#include "core/query.h"
#include "core/tianmu_attr.h"
#include "core/tianmu_table.h"
#include "core/value_set.h"
#include "vc/const_column.h"
#include "vc/expr_column.h"
#include "vc/in_set_column.h"
#include "vc/single_column.h"
#include "vc/subselect_column.h"

namespace Tianmu {
namespace core {
ConditionEncoder::ConditionEncoder(bool additional_nulls, uint32_t power)
    : additional_nulls(additional_nulls),
      in_type(ColumnType(common::ColumnType::UNK)),
      sharp(false),
      encoding_done(false),
      attr(nullptr),
      desc(nullptr),
      pack_power(power) {}

ConditionEncoder::~ConditionEncoder() {}

void ConditionEncoder::DoEncode() {
  if (desc->IsType_AttrAttr())
    return;  // No actual encoding needed, just mark it as encoded. Type
             // correspondence checked earlier.
  TransformWithRespectToNulls();
  if (!IsTransformationNeeded())
    return;

  DescriptorTransformation();
  if (!IsTransformationNeeded())
    return;

  if (ATI::IsStringType(AttrTypeName()))
    EncodeConditionOnStringColumn();
  else
    EncodeConditionOnNumerics();
}

void ConditionEncoder::operator()(Descriptor &desc) {
  MEASURE_FET("ConditionEncoder::operator()(...)");
  if (desc.encoded)
    return;

  auto tab = std::static_pointer_cast<TianmuTable>(desc.attr.vc->GetVarMap()[0].GetTabPtr());
  attr = tab->GetAttr(desc.attr.vc->GetVarMap()[0].col_ndx);
  attr->LoadPackInfo(current_txn_);
  in_type = attr->Type();
  this->desc = &desc;

  desc.encoded = true;
  DoEncode();
}

bool ConditionEncoder::IsTransformationNeeded() {
  return desc->encoded == true &&
         !(encoding_done || desc->op == common::Operator::O_IS_NULL || desc->op == common::Operator::O_NOT_NULL ||
           desc->op == common::Operator::O_FALSE || desc->op == common::Operator::O_TRUE);
}

void ConditionEncoder::DescriptorTransformation() {
  MEASURE_FET("ConditionEncoder::DescriptorTransformation(...)");
  if (desc->op == common::Operator::O_IN || desc->op == common::Operator::O_NOT_IN) {
    DEBUG_ASSERT(dynamic_cast<vcolumn::MultiValColumn *>(desc->val1.vc));
    return;
  }
  if (desc->op == common::Operator::O_EQ_ANY)
    desc->op = common::Operator::O_IN;

  if (desc->op == common::Operator::O_NOT_EQ_ALL)
    desc->op = common::Operator::O_NOT_IN;

  static MIIterator mit(nullptr, pack_power);
  if (desc->val1.IsNull() ||
      (!IsSetOperator(desc->op) && desc->val1.vc && desc->val1.vc->IsConst() && desc->val1.vc->IsNull(mit))) {
    desc->op = common::Operator::O_FALSE;
    desc->null_after_simplify = true;
  } else if (desc->op == common::Operator::O_BETWEEN &&
             (desc->val1.IsNull() || (desc->val1.vc->IsConst() && desc->val1.vc->IsNull(mit)) || desc->val2.IsNull() ||
              (desc->val2.vc->IsConst() && desc->val2.vc->IsNull(mit)))) {
    desc->op = common::Operator::O_FALSE;
    desc->null_after_simplify = true;
  } else if (desc->op == common::Operator::O_NOT_BETWEEN) {  // common::Operator::O_NOT_BETWEEN may be changed
                                                             // to inequalities in case of nulls
    bool first_null = (desc->val1.IsNull() || (desc->val1.vc->IsConst() && desc->val1.vc->IsNull(mit)));
    bool second_null = (desc->val2.IsNull() || (desc->val2.vc->IsConst() && desc->val2.vc->IsNull(mit)));
    if (first_null && second_null) {
      desc->op = common::Operator::O_FALSE;
      desc->null_after_simplify = true;
    } else if (first_null) {  // a NOT BETWEEN null AND 5  <=>  a > 5
      desc->val1 = desc->val2;
      desc->val2 = CQTerm();
      desc->op = common::Operator::O_MORE;
    } else if (second_null) {  // a NOT BETWEEN 5 AND null  <=>  a < 5
      desc->op = common::Operator::O_LESS;
    }
  }

  if (IsSetAllOperator(desc->op) && (desc->val1.vc)->IsMultival() &&
      static_cast<vcolumn::MultiValColumn &>(*desc->val1.vc).NumOfValues(mit) == 0)
    desc->op = common::Operator::O_TRUE;
  else {
    if (desc->op == common::Operator::O_EQ_ALL && (desc->val1.vc)->IsMultival()) {
      vcolumn::MultiValColumn &mvc = static_cast<vcolumn::MultiValColumn &>(*desc->val1.vc);
      PrepareValueSet(mvc);
      if (mvc.NumOfValues(mit) == 0)
        desc->op = common::Operator::O_TRUE;
      else if (mvc.AtLeastNoDistinctValues(mit, 2) == 1 && !mvc.ContainsNull(mit)) {
        desc->op = common::Operator::O_EQ;
        desc->val1 = CQTerm();
        desc->val1.vc = new vcolumn::ConstColumn(mvc.GetSetMin(mit), mvc.Type());
        desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
      } else
        desc->op = common::Operator::O_FALSE;
    }

    if (desc->op == common::Operator::O_NOT_EQ_ANY && (desc->val1.vc)->IsMultival()) {
      vcolumn::MultiValColumn &mvc = static_cast<vcolumn::MultiValColumn &>(*desc->val1.vc);
      PrepareValueSet(mvc);
      if (mvc.NumOfValues(mit) == 0) {
        desc->op = common::Operator::O_FALSE;
        return;
      }
      int no_distinct = int(mvc.AtLeastNoDistinctValues(mit, 2));
      if (no_distinct == 0)
        desc->op = common::Operator::O_FALSE;
      else if (no_distinct == 2) {
        desc->op = common::Operator::O_NOT_NULL;
        TransformWithRespectToNulls();
        return;
      } else {
        desc->op = common::Operator::O_NOT_EQ;
        desc->val1 = CQTerm();
        desc->val1.vc = new vcolumn::ConstColumn(mvc.GetSetMin(mit), mvc.Type());
        desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
      }
    }
    if (desc->op == common::Operator::O_FALSE || desc->op == common::Operator::O_TRUE)
      return;
    if (!IsSetOperator(desc->op) && (desc->val1.vc)->IsMultival()) {
      vcolumn::MultiValColumn &mvc = static_cast<vcolumn::MultiValColumn &>(*desc->val1.vc);
      vcolumn::VirtualColumn *vc = new vcolumn::ConstColumn(mvc.GetValue(mit), mvc.Type());
      desc->val1.vc = vc;
      desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
      desc->CoerceColumnType(desc->val1.vc);
    }
  }
}

void ConditionEncoder::TransformWithRespectToNulls() {
  MEASURE_FET("ConditionEncoder::TransformWithRespectToNulls(...)");

  if ((desc->op == common::Operator::O_IS_NULL || desc->op == common::Operator::O_NOT_NULL) &&
      dynamic_cast<vcolumn::ExpressionColumn *>(desc->attr.vc)) {
    desc->encoded = false;
    return;
  }

  bool nulls_only = desc->attr.vc->IsRoughNullsOnly();
  bool nulls_possible = (additional_nulls || desc->attr.vc->IsNullsPossible());

  if (desc->op == common::Operator::O_IS_NULL) {
    if (!nulls_possible || attr->NumOfObj() == 0)
      desc->op = common::Operator::O_FALSE;
    else if (nulls_only)
      desc->op = common::Operator::O_TRUE;
  } else if (desc->op == common::Operator::O_NOT_NULL) {
    if (!nulls_possible)
      desc->op = common::Operator::O_TRUE;
    else if (nulls_only)
      desc->op = common::Operator::O_FALSE;
  }

  if ((IsSetAllOperator(desc->op) || desc->op == common::Operator::O_NOT_IN) && desc->val1.vc &&
      desc->val1.vc->IsMultival()) {
    vcolumn::MultiValColumn *mvc = static_cast<vcolumn::MultiValColumn *>(desc->val1.vc);
    MIIterator mit(nullptr, pack_power);
    // Change to common::Operator::O_FALSE for non-subselect columns, or non-correlated
    // subselects (otherwise ContainsNulls needs feeding arguments)
    if (mvc->ContainsNull(mit)) {
      desc->op = common::Operator::O_FALSE;
      desc->null_after_simplify = true;
    }
  }
}

void ConditionEncoder::PrepareValueSet(vcolumn::MultiValColumn &mvc) { mvc.SetExpectedType(attr->Type()); }

void ConditionEncoder::EncodeConditionOnStringColumn() {
  DEBUG_ASSERT(ATI::IsStringType(AttrTypeName()));

  TextTransformation();
  if (!IsTransformationNeeded())
    return;

  if (desc->op == common::Operator::O_IN || desc->op == common::Operator::O_NOT_IN)
    TransformINs();
  else if (!attr->Type().Lookup())
    TransformOtherThanINsOnNotLookup();
  else
    TransformOtherThanINsOnNumerics();
}

void ConditionEncoder::EncodeConditionOnNumerics() {
  if (desc->op == common::Operator::O_IN || desc->op == common::Operator::O_NOT_IN)
    TransformINs();
  else
    TransformOtherThanINsOnNumerics();
}

void ConditionEncoder::TransformOtherThanINsOnNumerics() {
  MEASURE_FET("ConditionEncoder::TransformOtherThanINsOnNumerics(...)");
  int64_t v1 = 0;
  int64_t v2 = 0;
  bool v1_rounded = false, v2_rounded = false;
  static MIIterator mit(nullptr, pack_power);

  vcolumn::MultiValColumn *mvc = 0;
  if (desc->val1.vc && (desc->val1.vc)->IsMultival()) {
    mvc = static_cast<vcolumn::MultiValColumn *>(desc->val1.vc);
    PrepareValueSet(*mvc);  // else it was already done above
    // common::Operator::O_EQ_ANY = common::Operator::O_IN processed by other function,
    // common::Operator::O_NOT_EQ_ANY processed on higher level
    if (desc->op == common::Operator::O_LESS_ANY || desc->op == common::Operator::O_LESS_EQ_ANY ||
        desc->op == common::Operator::O_MORE_ALL || desc->op == common::Operator::O_MORE_EQ_ALL)
      v1 = attr->EncodeValue64(mvc->GetSetMax(mit).Get(),
                               v1_rounded);  // 1-level values
    else                                     //	ANY && MORE or ALL && LESS
      v1 = attr->EncodeValue64(mvc->GetSetMin(mit).Get(),
                               v1_rounded);  // 1-level values
  } else if (desc->val1.vc->IsConst()) {
    common::ErrorCode tianmu_err_code = common::ErrorCode::SUCCESS;
    v1 = attr->EncodeValue64(desc->val1.vc->GetValue(mit), v1_rounded,
                             &tianmu_err_code);  // 1-level values
    if (tianmu_err_code != common::ErrorCode::SUCCESS) {
      desc->encoded = false;
      throw common::DataTypeConversionException(common::ErrorCode::DATACONVERSION);
    }
  }

  if (desc->val2.vc && (desc->val2.vc)->IsMultival()) {
    mvc = static_cast<vcolumn::MultiValColumn *>(desc->val2.vc);
    PrepareValueSet(*mvc);
    // only for BETWEEN
    if (IsSetAnyOperator(desc->op))
      v2 = attr->EncodeValue64(mvc->GetSetMax(mit).Get(),
                               v2_rounded);  // 1-level values
    else
      //	ALL
      v2 = attr->EncodeValue64(mvc->GetSetMin(mit).Get(),
                               v2_rounded);  // 1-level values
  } else {
    common::ErrorCode tianmu_err_code = common::ErrorCode::SUCCESS;
    if (!desc->val2.IsNull() && desc->val2.vc && desc->val2.vc->IsConst()) {
      v2 = attr->EncodeValue64(desc->val2.vc->GetValue(mit), v2_rounded,
                               &tianmu_err_code);  // 1-level values
      if (tianmu_err_code != common::ErrorCode::SUCCESS) {
        desc->encoded = false;
        throw common::DataTypeConversionException(common::ErrorCode::DATACONVERSION);
      }
    } else
      v2 = common::NULL_VALUE_64;
  }

  if (v1_rounded) {
    if (ISTypeOfEqualOperator(desc->op)) {
      desc->op = common::Operator::O_FALSE;
      return;
    }

    if (ISTypeOfNotEqualOperator(desc->op)) {
      desc->op = common::Operator::O_NOT_NULL;
      return;
    }

    if (ISTypeOfLessOperator(desc->op) && v1 >= 0)
      desc->op = common::Operator::O_LESS_EQ;

    if (ISTypeOfLessEqualOperator(desc->op) && v1 < 0)
      desc->op = common::Operator::O_LESS;

    if (ISTypeOfMoreOperator(desc->op) && v1 < 0)
      desc->op = common::Operator::O_MORE_EQ;

    if (ISTypeOfMoreEqualOperator(desc->op) && v1 >= 0)
      desc->op = common::Operator::O_MORE;

    if ((desc->op == common::Operator::O_BETWEEN || desc->op == common::Operator::O_NOT_BETWEEN) && v1 >= 0)
      v1 += 1;
  }

  if (v2_rounded && (desc->op == common::Operator::O_BETWEEN || desc->op == common::Operator::O_NOT_BETWEEN) && v2 < 0)
    v2 -= 1;

  if (v1 == common::NULL_VALUE_64 || (desc->op == common::Operator::O_BETWEEN &&
                                      v2 == common::NULL_VALUE_64)) {  // any comparison with null values only
    desc->op = common::Operator::O_FALSE;
    return;
  }

  if (ISTypeOfEqualOperator(desc->op) || ISTypeOfNotEqualOperator(desc->op))
    v2 = v1;

  if (ISTypeOfLessOperator(desc->op)) {
    if (!ATI::IsRealType(AttrTypeName())) {
      if (v1 > common::MINUS_INF_64)
        v2 = v1 - 1;
      v1 = common::MINUS_INF_64;
    } else {
      if (*(double *)&v1 > common::MINUS_INF_DBL)
        v2 = (AttrTypeName() == common::ColumnType::REAL) ? DoubleMinusEpsilon(v1) : FloatMinusEpsilon(v1);
      v1 = *(int64_t *)&common::MINUS_INF_DBL;
    }
  }

  if (ISTypeOfMoreOperator(desc->op)) {
    if (!ATI::IsRealType(AttrTypeName())) {
      if (v1 != common::PLUS_INF_64)
        v1 += 1;
      v2 = common::PLUS_INF_64;
    } else {
      if (*(double *)&(v1) != common::PLUS_INF_DBL)
        v1 = (AttrTypeName() == common::ColumnType::REAL) ? DoublePlusEpsilon(v1) : FloatPlusEpsilon(v1);
      v2 = *(int64_t *)&common::PLUS_INF_DBL;
    }
  }

  if (ISTypeOfLessEqualOperator(desc->op)) {
    v2 = v1;
    if (!ATI::IsRealType(AttrTypeName()))
      v1 = common::MINUS_INF_64;
    else
      v1 = *(int64_t *)&common::MINUS_INF_DBL;
  }

  if (ISTypeOfMoreEqualOperator(desc->op)) {
    if (!ATI::IsRealType(AttrTypeName()))
      v2 = common::PLUS_INF_64;
    else
      v2 = *(int64_t *)&common::PLUS_INF_DBL;
  }

  desc->sharp = false;
  if (ISTypeOfNotEqualOperator(desc->op) || desc->op == common::Operator::O_NOT_BETWEEN)
    desc->op = common::Operator::O_NOT_BETWEEN;
  else
    desc->op = common::Operator::O_BETWEEN;

  desc->val1 = CQTerm();
  desc->val1.vc = new vcolumn::ConstColumn(
      ValueOrNull(types::TianmuNum(v1, attr->Type().GetScale(), ATI::IsRealType(AttrTypeName()), AttrTypeName())),
      attr->Type());
  desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);

  desc->val2 = CQTerm();
  desc->val2.vc = new vcolumn::ConstColumn(
      ValueOrNull(types::TianmuNum(v2, attr->Type().GetScale(), ATI::IsRealType(AttrTypeName()), AttrTypeName())),
      attr->Type());
  desc->val2.vc_id = desc->table->AddVirtColumn(desc->val2.vc);
}

void ConditionEncoder::TransformLIKEsPattern() {
  MEASURE_FET("ConditionEncoder::TransformLIKEsPattern(...)");
  static MIIterator const mit(nullptr, pack_power);
  types::BString pattern;
  desc->val1.vc->GetValueString(pattern, mit);
  uint min_len = 0;
  bool esc = false;
  for (uint i = 0; i < pattern.len_; i++) {
    if (pattern[i] != '%')
      min_len++;
    else
      esc = true;
  }

  if (min_len == 0) {
    if (esc) {
      if (desc->op == common::Operator::O_LIKE)
        desc->op = common::Operator::O_NOT_NULL;
      else
        desc->op = common::Operator::O_FALSE;
    } else {
      if (desc->op == common::Operator::O_LIKE)
        desc->op = common::Operator::O_EQ;
      else
        desc->op = common::Operator::O_NOT_EQ;
    }
  } else if (min_len > attr->Type().GetPrecision()) {
    if (desc->op == common::Operator::O_LIKE)
      desc->op = common::Operator::O_FALSE;
    else
      desc->op = common::Operator::O_NOT_NULL;
  } else if (attr->Type().Lookup())
    TransformLIKEsIntoINsOnLookup();
  else
    encoding_done = true;
}

void ConditionEncoder::TransformLIKEsIntoINsOnLookup() {
  MEASURE_FET("ConditionEncoder::TransformLIKEsIntoINsOnLookup(...)");
  DEBUG_ASSERT(attr->Type().Lookup());

  if (desc->op == common::Operator::O_LIKE)
    desc->op = common::Operator::O_IN;
  else
    desc->op = common::Operator::O_NOT_IN;

  ValueSet valset(desc->table->Getpackpower());
  static MIIterator mid(nullptr, pack_power);
  in_type = ColumnType(common::ColumnType::NUM);
  types::BString pattern;
  desc->val1.vc->GetValueString(pattern, mid);
  for (int i = 0; i < attr->Cardinality(); i++) {
    int res;
    if (types::RequiresUTFConversions(desc->GetCollation())) {
      types::BString s = attr->GetRealString(i);
      res = !common::wildcmp(desc->GetCollation(), s.val_, s.val_ + s.len_, pattern.val_, pattern.val_ + pattern.len_,
                             desc->like_esc, '_', '%');
    } else
      res = attr->GetRealString(i).Like(pattern, desc->like_esc);
    if (res)
      valset.Add64(i);
  }
  desc->val1.vc = new vcolumn::InSetColumn(in_type, nullptr, valset);
  desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
  if (static_cast<vcolumn::InSetColumn &>(*desc->val1.vc).IsEmpty(mid)) {
    if (desc->op == common::Operator::O_IN)
      desc->op = common::Operator::O_FALSE;
    else
      desc->op = common::Operator::O_NOT_NULL;
  }
}

void ConditionEncoder::TransformLIKEs() {
  MEASURE_FET("ConditionEncoder::TransformLIKEs(...)");
  TransformLIKEsPattern();
  if (!IsTransformationNeeded())
    return;

  if (attr->Type().Lookup() && (desc->op == common::Operator::O_LIKE || desc->op == common::Operator::O_NOT_LIKE))
    TransformLIKEsIntoINsOnLookup();
}

void ConditionEncoder::TransformINsOnLookup() {
  MEASURE_FET("ConditionEncoder::TransformINsOnLookup(...)");
  static MIIterator mid(nullptr, pack_power);
  ValueSet valset(desc->table->Getpackpower());
  types::BString s;
  for (int i = 0; i < attr->Cardinality(); i++) {
    s = attr->GetRealString(i);
    if ((static_cast<vcolumn::MultiValColumn *>(desc->val1.vc))->Contains(mid, s) == true)
      valset.Add64(i);
  }
  in_type = ColumnType(common::ColumnType::NUM);
  desc->val1.vc = new vcolumn::InSetColumn(in_type, nullptr, valset);
  desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
  if (static_cast<vcolumn::InSetColumn &>(*desc->val1.vc).IsEmpty(mid)) {
    if (desc->op == common::Operator::O_IN)
      desc->op = common::Operator::O_FALSE;
    else
      desc->op = common::Operator::O_NOT_NULL;
  }
}

void ConditionEncoder::TransformIntoINsOnLookup() {
  MEASURE_FET("ConditionEncoder::TransformIntoINsOnLookup(...)");
  static MIIterator mit(nullptr, pack_power);
  ValueSet vset_positive(desc->table->Getpackpower());
  ValueSet vset_negative(desc->table->Getpackpower());
  types::BString s, vs1, vs2;
  types::TianmuValueObject vo;
  if (desc->val1.vc->IsMultival() && static_cast<vcolumn::MultiValColumn &>(*desc->val1.vc).NumOfValues(mit) > 0) {
    if ((desc->op == common::Operator::O_LESS_ALL || desc->op == common::Operator::O_LESS_EQ_ALL) ||
        (desc->op == common::Operator::O_MORE_ANY || desc->op == common::Operator::O_MORE_EQ_ANY))
      vo = static_cast<vcolumn::MultiValColumn &>(*desc->val1.vc).GetSetMin(mit);
    else if ((desc->op == common::Operator::O_LESS_ANY || desc->op == common::Operator::O_LESS_EQ_ANY) ||
             (desc->op == common::Operator::O_MORE_ALL || desc->op == common::Operator::O_MORE_EQ_ALL))
      vo = static_cast<vcolumn::MultiValColumn &>(*desc->val1.vc).GetSetMax(mit);
    if (vo.Get())
      vs1 = vo.Get()->ToBString();
  } else if (desc->val1.vc->IsConst())
    desc->val1.vc->GetValueString(vs1, mit);

  if (desc->op == common::Operator::O_BETWEEN || desc->op == common::Operator::O_NOT_BETWEEN) {
    if (desc->val2.vc->IsMultival() && static_cast<vcolumn::MultiValColumn &>(*desc->val2.vc).NumOfValues(mit) > 0) {
      vo = static_cast<vcolumn::MultiValColumn &>(*desc->val2.vc).GetSetMin(mit);
      if (vo.Get())
        vs2 = vo.Get()->ToBString();
    } else if (desc->val2.vc->IsConst())
      desc->val2.vc->GetValueString(vs2, mit);
  }

  if (vs1.IsNull() && vs2.IsNull()) {
    desc->op = common::Operator::O_FALSE;
  } else {
    in_type = ColumnType(common::ColumnType::NUM);
    int cmp1 = 0, cmp2 = 0;
    int count1 = 0, count0 = 0;
    bool utf = types::RequiresUTFConversions(desc->GetCollation());
    bool single_value_search = (ISTypeOfEqualOperator(desc->op) && !utf);
    for (int i = 0; i < attr->Cardinality(); i++) {
      s = attr->GetRealString(i);
      if (utf)
        cmp1 = CollationStrCmp(desc->GetCollation(), s, vs1);
      else
        cmp1 = s.CompareWith(vs1);

      if (desc->op == common::Operator::O_BETWEEN || desc->op == common::Operator::O_NOT_BETWEEN) {
        if (utf)
          cmp2 = CollationStrCmp(desc->GetCollation(), s, vs2);
        else
          cmp2 = s.CompareWith(vs2);
      }

      if ((ISTypeOfEqualOperator(desc->op) && cmp1 == 0) || (ISTypeOfNotEqualOperator(desc->op) && cmp1 != 0) ||
          (ISTypeOfLessOperator(desc->op) && cmp1 < 0) || (ISTypeOfMoreOperator(desc->op) && cmp1 > 0) ||
          (ISTypeOfLessEqualOperator(desc->op) && cmp1 <= 0) || (ISTypeOfMoreEqualOperator(desc->op) && cmp1 >= 0) ||
          ((desc->op == common::Operator::O_BETWEEN) && cmp1 >= 0 && cmp2 <= 0) ||
          ((desc->op == common::Operator::O_NOT_BETWEEN) && (cmp1 < 0 || cmp2 > 0))) {
        if (count1 <= attr->Cardinality() / 2 + 1)
          vset_positive.Add64(i);
        count1++;
        if (single_value_search) {
          count0 += attr->Cardinality() - i - 1;
          break;
        }
      } else {
        if (!single_value_search && count0 <= attr->Cardinality() / 2 + 1)
          vset_negative.Add64(i);
        count0++;
      }
    }

    if (count1 <= count0) {
      desc->op = common::Operator::O_IN;
      desc->val1.vc = new vcolumn::InSetColumn(in_type, nullptr, vset_positive /* *vset.release()*/);
      desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
    } else {
      desc->op = common::Operator::O_NOT_IN;
      desc->val1.vc = new vcolumn::InSetColumn(in_type, nullptr, vset_negative /* *vset_n.release() */);
      desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
    }
  }
}

void ConditionEncoder::TextTransformation() {
  MEASURE_FET("ConditionEncoder::TextTransformation(...)");
  DEBUG_ASSERT(ATI::IsStringType(AttrTypeName()));

  if (desc->attr.vc) {
    vcolumn::ExpressionColumn *vcec = dynamic_cast<vcolumn::ExpressionColumn *>(desc->attr.vc);
    if (vcec && vcec->ExactlyOneLookup()) {
      LookupExpressionTransformation();
      return;
    }
  }
  if (desc->op == common::Operator::O_LIKE || desc->op == common::Operator::O_NOT_LIKE) {
    TransformLIKEs();
  } else if (attr->Type().Lookup()) {  // lookup - transform into IN (numbers)
    if (desc->op == common::Operator::O_IN || desc->op == common::Operator::O_NOT_IN)
      TransformINsOnLookup();
    else
      TransformIntoINsOnLookup();
  }
}

void ConditionEncoder::TransformINs() {
  MEASURE_FET("ConditionEncoder::TransformINs(...)");
  DEBUG_ASSERT(dynamic_cast<vcolumn::MultiValColumn *>(desc->val1.vc));

  static MIIterator mit(nullptr, pack_power);

  vcolumn::MultiValColumn &mvc = static_cast<vcolumn::MultiValColumn &>(*desc->val1.vc);
  mvc.SetExpectedType(in_type);

  int64_t no_dis_values = mvc.AtLeastNoDistinctValues(mit, 2);

  if (no_dis_values == 0) {
    if (mvc.ContainsNull(mit))
      desc->null_after_simplify = true;
    if (desc->op == common::Operator::O_IN)
      desc->op = common::Operator::O_FALSE;
    else if (!mvc.ContainsNull(mit))
      desc->op = common::Operator::O_NOT_NULL;
    else
      desc->op = common::Operator::O_FALSE;
  } else {
    if (no_dis_values == 1 && !mvc.ContainsNull(mit)) {
      if (attr->GetPackType() == common::PackType::INT && !attr->Type().Lookup()) {
        desc->val2 = CQTerm();
        desc->val2.vc =
            new vcolumn::ConstColumn(ValueOrNull(types::TianmuNum(attr->EncodeValue64(mvc.GetSetMin(mit), sharp),
                                                                  static_cast<short>(in_type.GetTypeName()))),
                                     in_type);
        desc->val2.vc_id = desc->table->AddVirtColumn(desc->val2.vc);
      } else {
        desc->val2 = CQTerm();
        desc->val2.vc = new vcolumn::ConstColumn(mvc.GetSetMin(mit), in_type);
        desc->val2.vc_id = desc->table->AddVirtColumn(desc->val2.vc);
      }

      if (sharp) {
        if (desc->op == common::Operator::O_IN)
          desc->op = common::Operator::O_FALSE;
        else
          desc->op = common::Operator::O_NOT_NULL;
      } else {
        desc->val1 = desc->val2;
        if (desc->op == common::Operator::O_IN)
          desc->op = common::Operator::O_BETWEEN;
        else if (!mvc.ContainsNull(mit))
          desc->op = common::Operator::O_NOT_BETWEEN;
        else
          desc->op = common::Operator::O_FALSE;
      }
    } else if (attr->GetPackType() == common::PackType::INT && !mvc.ContainsNull(mit)) {
      int64_t val_min, val_max;
      if (attr->Type().Lookup()) {
        val_min = int64_t((types::TianmuNum &)mvc.GetSetMin(mit));
        val_max = int64_t((types::TianmuNum &)mvc.GetSetMax(mit));
      } else {
        val_min = attr->EncodeValue64(mvc.GetSetMin(mit), sharp);
        val_max = attr->EncodeValue64(mvc.GetSetMax(mit), sharp);
      }
      int64_t span = val_max - val_min + 1;
      if (attr->Type().GetScale() < 1 && span > 0 && span < 65536  // otherwise AtLeast... will be too costly
          && span == mvc.AtLeastNoDistinctValues(mit, span + 1)) {
        desc->val2.vc = new vcolumn::ConstColumn(
            ValueOrNull(types::TianmuNum(val_max, attr->Type().GetScale(), ATI::IsRealType(in_type.GetTypeName()),
                                         in_type.GetTypeName())),
            in_type);
        desc->val2.vc_id = desc->table->AddVirtColumn(desc->val2.vc);
        desc->val1.vc = new vcolumn::ConstColumn(
            ValueOrNull(types::TianmuNum(val_min, attr->Type().GetScale(), ATI::IsRealType(in_type.GetTypeName()),
                                         in_type.GetTypeName())),
            in_type);
        desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
        if (desc->op == common::Operator::O_IN)
          desc->op = common::Operator::O_BETWEEN;
        else
          desc->op = common::Operator::O_NOT_BETWEEN;
      }
    }
  }
}

void ConditionEncoder::TransformOtherThanINsOnNotLookup() {
  MEASURE_FET("ConditionEncoder::TransformOtherThanINsOnNotLookup(...)");
  CQTerm v1 = desc->val1;
  CQTerm v2 = desc->val2;
  sharp = false;
  //(SOME, ALL)

  MIIterator mit(nullptr, pack_power);
  vcolumn::MultiValColumn *mvc = 0;
  if (v1.vc && v1.vc->IsMultival()) {
    mvc = static_cast<vcolumn::MultiValColumn *>(v1.vc);
    PrepareValueSet(*mvc);  // else it was already done above
    if (desc->op == common::Operator::O_LESS_ANY || desc->op == common::Operator::O_LESS_EQ_ANY ||
        desc->op == common::Operator::O_MORE_ALL || desc->op == common::Operator::O_MORE_EQ_ALL) {
      v1.vc = new vcolumn::ConstColumn(mvc->GetSetMax(mit), mvc->Type());
      v1.vc_id = desc->table->AddVirtColumn(v1.vc);
    } else {
      // ASSERT(desc->op != common::Operator::O_NOT_BETWEEN, "desc->op should not be
      // common::Operator::O_NOT_BETWEEN at this point!");
      v1.vc = new vcolumn::ConstColumn(mvc->GetSetMin(mit), mvc->Type());
      v1.vc_id = desc->table->AddVirtColumn(v1.vc);
    }
  }

  if (v2.vc && v2.vc->IsMultival()) {
    mvc = static_cast<vcolumn::MultiValColumn *>(v2.vc);
    // ASSERT(desc->op != common::Operator::O_NOT_BETWEEN, "desc->op should not be
    // common::Operator::O_NOT_BETWEEN at this point!");
    PrepareValueSet(*mvc);
    // only for BETWEEN
    if (IsSetAnyOperator(desc->op)) {
      v2.vc = new vcolumn::ConstColumn(mvc->GetSetMax(mit), mvc->Type());
      v2.vc_id = desc->table->AddVirtColumn(v2.vc);
    } else {  //	ALL
      v2.vc = new vcolumn::ConstColumn(mvc->GetSetMin(mit), mvc->Type());
      v2.vc_id = desc->table->AddVirtColumn(v2.vc);
    }
  }

  if ((v1.IsNull() || (v1.vc && v1.vc->IsMultival() && v1.vc->IsNull(mit))) &&
      (v2.IsNull() || (v2.vc && v2.vc->IsMultival() && v2.vc->IsNull(mit)))) {
    desc->null_after_simplify = true;
    desc->op = common::Operator::O_FALSE;
  } else {
    if (v1.vc && v1.vc->IsMultival() && !v1.vc->IsNull(mit))
      desc->CoerceColumnType(v1.vc);

    if (v2.vc && v2.vc->IsMultival() && !v2.vc->IsNull(mit))
      desc->CoerceColumnType(v2.vc);

    if (ISTypeOfEqualOperator(desc->op) || ISTypeOfNotEqualOperator(desc->op))
      v2 = v1;

    if (ISTypeOfLessOperator(desc->op) || ISTypeOfLessEqualOperator(desc->op)) {
      v2 = v1;
      v1 = CQTerm();
      v1.vc = new vcolumn::ConstColumn(ValueOrNull(), attr->Type());
      v1.vc_id = desc->table->AddVirtColumn(v1.vc);
    } else if (ISTypeOfMoreOperator(desc->op) || ISTypeOfMoreEqualOperator(desc->op)) {
      v2 = CQTerm();
      v2.vc = new vcolumn::ConstColumn(ValueOrNull(), attr->Type());
      v2.vc_id = desc->table->AddVirtColumn(v2.vc);
    }

    if (ISTypeOfLessOperator(desc->op) || ISTypeOfMoreOperator(desc->op))
      sharp = true;

    if (ISTypeOfNotEqualOperator(desc->op) || desc->op == common::Operator::O_NOT_BETWEEN)
      desc->op = common::Operator::O_NOT_BETWEEN;
    else
      desc->op = common::Operator::O_BETWEEN;  // common::Operator::O_IN, common::Operator::O_LIKE etc.
                                               // excluded earlier

    desc->sharp = sharp;
    desc->val1 = v1;
    desc->val2 = v2;
  }
}

void ConditionEncoder::EncodeIfPossible(Descriptor &desc, bool for_rough_query, bool additional_nulls) {
  MEASURE_FET("ConditionEncoder::EncodeIfPossible(...)");
  if (desc.done || desc.IsDelayed())
    return;
  if (desc.IsType_OrTree()) {
    desc.tree->root->EncodeIfPossible(for_rough_query, additional_nulls);
    desc.Simplify();
    return;
  }
  if (!desc.attr.vc || desc.attr.vc->GetDim() == -1)
    return;

  if (desc.IsType_In()) {
    return;
  }

  vcolumn::SingleColumn *vcsc =
      (static_cast<int>(desc.attr.vc->IsSingleColumn()) ? static_cast<vcolumn::SingleColumn *>(desc.attr.vc) : nullptr);

  bool encode_now = false;
  if (desc.IsType_AttrAttr() && IsSimpleEqualityOperator(desc.op) && vcsc) {
    // special case: simple operator on two compatible numerical columns
    vcolumn::SingleColumn *vcsc2 = nullptr;
    if (static_cast<int>(desc.val1.vc->IsSingleColumn()))
      vcsc2 = static_cast<vcolumn::SingleColumn *>(desc.val1.vc);
    if (vcsc2 == nullptr || vcsc->GetVarMap()[0].GetTabPtr()->TableType() != TType::TABLE ||
        vcsc2->GetVarMap()[0].GetTabPtr()->TableType() != TType::TABLE)
      return;
    if (vcsc->Type().IsString() || vcsc->Type().Lookup() || vcsc2->Type().IsString() ||
        vcsc2->Type().Lookup())  // excluding strings
      return;
    bool is_timestamp1 = (vcsc->Type().GetTypeName() == common::ColumnType::TIMESTAMP);
    bool is_timestamp2 = (vcsc2->Type().GetTypeName() == common::ColumnType::TIMESTAMP);
    // excluding timestamps compared with something else
    if (is_timestamp1 || (is_timestamp2 && !(is_timestamp1 && is_timestamp2)))
      return;

    encode_now = (vcsc->Type().IsDateTime() && vcsc2->Type().IsDateTime()) ||
                 (vcsc->Type().IsFloat() && vcsc2->Type().IsFloat()) ||
                 (vcsc->Type().IsFixed() && vcsc2->Type().IsFixed() &&
                  vcsc->Type().GetScale() == vcsc2->Type().GetScale());  // excluding floats
  }

  if (!encode_now) {
    vcolumn::ExpressionColumn *vcec = dynamic_cast<vcolumn::ExpressionColumn *>(desc.attr.vc);
    if (vcec == nullptr && (vcsc == nullptr || vcsc->GetVarMap()[0].GetTabPtr()->TableType() != TType::TABLE))
      return;
    if (vcec != nullptr) {
      encode_now =
          (vcec->ExactlyOneLookup() &&
           (desc.op == common::Operator::O_IS_NULL || desc.op == common::Operator::O_NOT_NULL ||
            (desc.val1.vc && desc.val1.vc->IsConst() && (desc.val2.vc == nullptr || desc.val2.vc->IsConst()))));
    } else {
      encode_now = (desc.IsType_AttrValOrAttrValVal() || desc.IsType_AttrMultiVal() ||
                    desc.op == common::Operator::O_IS_NULL || desc.op == common::Operator::O_NOT_NULL) &&
                   desc.attr.vc->GetVarMap()[0].GetTabPtr()->TableType() == TType::TABLE &&
                   (!for_rough_query || !desc.IsType_Subquery());
    }
  }
  if (!encode_now)
    return;
  // Encoding itself
  ConditionEncoder ce(additional_nulls, desc.table->Getpackpower());
  ce(desc);
  desc.Simplify();
}

void ConditionEncoder::LookupExpressionTransformation() {
  MEASURE_FET("ConditionEncoder::LookupExpressionTransformation(...)");
  vcolumn::ExpressionColumn *vcec = dynamic_cast<vcolumn::ExpressionColumn *>(desc->attr.vc);
  vcolumn::VirtualColumnBase::VarMap col_desc = vcec->GetLookupCoordinates();
  MILookupIterator mit;
  mit.Set(common::NULL_VALUE_64);
  desc->encoded = false;
  bool null_positive = desc->CheckCondition(mit);
  ValueSet valset(desc->table->Getpackpower());
  in_type = ColumnType(common::ColumnType::NUM);
  int code = 0;
  do {
    mit.Set(code);
    if (desc->CheckCondition(mit)) {
      if (mit.IsValid())
        valset.Add64(code);
    }
    code++;
  } while (mit.IsValid());

  if (!null_positive) {
    PhysicalColumn *col = col_desc.GetTabPtr()->GetColumn(col_desc.col_ndx);
    desc->attr.vc = new vcolumn::SingleColumn(col, vcec->GetMultiIndex(), col_desc.var_id.tab, col_desc.col_ndx,
                                              col_desc.GetTabPtr().get(), vcec->GetDim());
    desc->attr.vc_id = desc->table->AddVirtColumn(desc->attr.vc);
    desc->op = common::Operator::O_IN;
    desc->val1.vc = new vcolumn::InSetColumn(in_type, nullptr, valset);
    desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
    desc->encoded = true;
  } else if (valset.IsEmpty()) {
    PhysicalColumn *col = col_desc.GetTabPtr()->GetColumn(col_desc.col_ndx);
    desc->attr.vc = new vcolumn::SingleColumn(col, vcec->GetMultiIndex(), col_desc.var_id.tab, col_desc.col_ndx,
                                              col_desc.GetTabPtr().get(), vcec->GetDim());
    desc->attr.vc_id = desc->table->AddVirtColumn(desc->attr.vc);
    desc->op = common::Operator::O_IS_NULL;
    desc->encoded = true;
  } else {  // both nulls and not-nulls are positive - no single operator
            // possible
    // do not encode
    desc->encoded = false;
  }
}
}  // namespace core
}  // namespace Tianmu
