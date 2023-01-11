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

#include "expr_column.h"
#include "core/compiled_query.h"
#include "core/mysql_expression.h"
#include "core/tianmu_attr.h"

namespace Tianmu {
namespace vcolumn {
ExpressionColumn::ExpressionColumn(core::MysqlExpression *expr, core::TempTable *temp_table, int temp_table_alias,
                                   core::MultiIndex *multi_index)
    : VirtualColumn(core::ColumnType(), multi_index),
      expr_(expr),
      deterministic_(expr ? expr->IsDeterministic() : true) {
  const std::vector<core::JustATable *> *tables = &temp_table->GetTables();
  const std::vector<int> *aliases = &temp_table->GetAliases();

  if (expr_) {
    vars_ = expr_->GetVars();  // get all variables from complex term
    first_eval_ = true;
    // status = deterministic ? VC_EXPR : VC_EXPR_NONDET;
    // fill types for variables and create buffers for argument values
    int only_dim_number = -2;  // -2 = not used yet
    for (auto &v : vars_) {
      auto ndx_it = find(aliases->begin(), aliases->end(), v.tab);
      if (ndx_it != aliases->end()) {
        int ndx = int(distance(aliases->begin(), ndx_it));

        var_map_.push_back(VarMap(v, (*tables)[ndx], ndx));
        if (only_dim_number == -2 || only_dim_number == ndx)
          only_dim_number = ndx;
        else
          only_dim_number = -1;  // more than one

        var_types_[v] = (*tables)[ndx]->GetColumnType(var_map_[var_map_.size() - 1].col_ndx);
        var_buf_[v] = std::vector<core::MysqlExpression::value_or_null_info_t>();  // now empty, pointers
                                                                                   // inserted by SetBufs()
      } else if (v.tab == temp_table_alias) {
        var_map_.push_back(VarMap(v, temp_table, 0));
        if (only_dim_number == -2 || only_dim_number == 0)
          only_dim_number = 0;
        else
          only_dim_number = -1;  // more than one

        var_types_[v] = temp_table->GetColumnType(var_map_[var_map_.size() - 1].col_ndx);
        var_buf_[v] = std::vector<core::MysqlExpression::value_or_null_info_t>();  // now empty, pointers
                                                                                   // inserted by SetBufs()
      } else {
        // parameter
        // parameter type is not available here, must be set later (EvalType())
        params_.insert(v);
        //				param_buf[v].second = nullptr; //assigned
        // by
        // SetBufs()
      }
    }
    ct = core::ColumnType(expr_->EvalType(&var_types_));  // set the column type from expression result type
    expr->SetBufsOrParams(&var_buf_);
    //		expr->SetBufsOrParams(&param_buf);
    dim_ = (only_dim_number >= 0 ? only_dim_number : -1);

    // if (status == VC_EXPR && var_map_.size() == 0 )
    //	status = VC_CONST;
  } else {
    DEBUG_ASSERT(!"unexpected!!");
  }
}

ExpressionColumn::ExpressionColumn(const ExpressionColumn &ec)
    : VirtualColumn(ec),
      expr_(ec.expr_),
      vars_(ec.vars_),
      var_types_(ec.var_types_),
      var_buf_(ec.var_buf_),
      deterministic_(ec.deterministic_) {
  var_map_ = ec.var_map_;
}

void ExpressionColumn::SetParamTypes(core::MysqlExpression::TypOfVars *types) { expr_->EvalType(types); }

bool ExpressionColumn::FeedArguments(const core::MIIterator &mit) {
  bool diff = first_eval_;
  if (mit.Type() == core::MIIterator::MIIteratorType::MII_LOOKUP) {
    core::MILookupIterator *mit_lookup = dynamic_cast<core::MILookupIterator *>(const_cast<core::MIIterator *>(&mit));
    FeedLookupArguments(*mit_lookup);
    first_eval_ = false;
    return true;
  }
  for (auto &it : var_map_) {
    core::ValueOrNull v(it.just_a_table_ptr->GetComplexValue(mit[it.dim], it.col_ndx));
    v.MakeStringOwner();
    auto cache = var_buf_.find(it.var_id);
    DEBUG_ASSERT(cache != var_buf_.end());
    diff = diff || (v != cache->second.begin()->first);
    if (diff)
      for (auto &val_it : cache->second) *(val_it.second) = val_it.first = v;
  }
  first_eval_ = false;
  return (diff || !deterministic_);
}

int64_t ExpressionColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  if (FeedArguments(mit))
    last_val_ = expr_->Evaluate();
  if (last_val_->IsNull())
    return common::NULL_VALUE_64;
  return last_val_->Get64();
}

bool ExpressionColumn::IsNullImpl(const core::MIIterator &mit) {
  if (FeedArguments(mit))
    last_val_ = expr_->Evaluate();
  return last_val_->IsNull();
}

void ExpressionColumn::GetValueStringImpl(types::BString &s, const core::MIIterator &mit) {
  if (FeedArguments(mit))
    last_val_ = expr_->Evaluate();
  if (core::ATI::IsDateTimeType(TypeName())) {
    int64_t tmp;
    types::TianmuDateTime vd(last_val_->Get64(), TypeName());
    vd.ToInt64(tmp);
    last_val_->SetFixed(tmp);
  }
  last_val_->GetBString(s);
}

double ExpressionColumn::GetValueDoubleImpl(const core::MIIterator &mit) {
  double val = 0;
  if (FeedArguments(mit))
    last_val_ = expr_->Evaluate();
  if (last_val_->IsNull())
    val = NULL_VALUE_D;

  if (core::ATI::IsIntegerType(TypeName()))
    val = (double)last_val_->Get64();
  else if (core::ATI::IsFixedNumericType(TypeName()))
    val = ((double)last_val_->Get64()) / types::PowOfTen(ct.GetScale());
  else if (core::ATI::IsRealType(TypeName())) {
    val = last_val_->GetDouble();
  } else if (core::ATI::IsDateTimeType(TypeName())) {
    types::TianmuDateTime vd(last_val_->Get64(),
                             TypeName());  // 274886765314048  ->  2000-01-01
    int64_t vd_conv = 0;
    vd.ToInt64(vd_conv);  // 2000-01-01  ->  20000101
    val = (double)vd_conv;
  } else if (core::ATI::IsStringType(TypeName())) {
    auto str = last_val_->ToString();
    if (str)
      val = std::stod(*str);
  } else
    DEBUG_ASSERT(0 && "conversion to double not implemented");

  return val;
}

types::TianmuValueObject ExpressionColumn::GetValueImpl(const core::MIIterator &mit, bool lookup_to_num) {
  if (core::ATI::IsStringType((TypeName()))) {
    types::BString s;
    GetValueString(s, mit);
    return s;
  }
  if (core::ATI::IsIntegerType(TypeName()))
    return types::TianmuNum(GetValueInt64(mit), -1, false, TypeName());
  if (core::ATI::IsDateTimeType(TypeName()))
    return types::TianmuDateTime(GetValueInt64(mit), TypeName());
  if (core::ATI::IsRealType(TypeName()))
    return types::TianmuNum(GetValueInt64(mit), 0, true, TypeName());
  if (lookup_to_num || TypeName() == common::ColumnType::NUM || TypeName() == common::ColumnType::BIT)
    return types::TianmuNum(GetValueInt64(mit), Type().GetScale());
  DEBUG_ASSERT(!"Illegal execution path");
  return types::TianmuValueObject();
}

int64_t ExpressionColumn::GetSumImpl([[maybe_unused]] const core::MIIterator &mit, bool &nonnegative) {
  nonnegative = false;
  return common::NULL_VALUE_64;  // not implemented
}

int64_t ExpressionColumn::GetMinInt64Impl([[maybe_unused]] const core::MIIterator &mit) {
  return common::MINUS_INF_64;  // not implemented
}

int64_t ExpressionColumn::GetMaxInt64Impl([[maybe_unused]] const core::MIIterator &mit) {
  return common::PLUS_INF_64;  // not implemented
}

types::BString ExpressionColumn::GetMinStringImpl([[maybe_unused]] const core::MIIterator &mit) {
  return types::BString();  // not implemented
}

types::BString ExpressionColumn::GetMaxStringImpl([[maybe_unused]] const core::MIIterator &mit) {
  return types::BString();  // not implemented
}

int64_t ExpressionColumn::GetApproxDistValsImpl([[maybe_unused]] bool incl_nulls,
                                                [[maybe_unused]] core::RoughMultiIndex *rough_mind) {
  if (multi_index_->TooManyTuples())
    return common::PLUS_INF_64;
  return multi_index_->NumOfTuples();  // default
}

size_t ExpressionColumn::MaxStringSizeImpl()  // maximal byte string length in column
{
  return ct.GetPrecision();  // default
}

core::PackOntologicalStatus ExpressionColumn::GetPackOntologicalStatusImpl(const core::MIIterator &mit) {
  core::PackOntologicalStatus st =
      deterministic_ ? core::PackOntologicalStatus::UNIFORM : core::PackOntologicalStatus::NORMAL;  // will be used for

  // what about 0 arguments and null only?
  core::PackOntologicalStatus st_loc;
  for (auto &it : var_map_) {
    // cast to remove const as GetPackOntologicalStatus() is not const
    st_loc = ((core::PhysicalColumn *)it.GetTabPtr()->GetColumn(it.col_ndx))
                 ->GetPackOntologicalStatus(mit.GetCurPackrow(it.dim));
    if (st_loc != core::PackOntologicalStatus::UNIFORM && st_loc != core::PackOntologicalStatus::NULLS_ONLY)
      return core::PackOntologicalStatus::NORMAL;
  }

  return st;
}

void ExpressionColumn::EvaluatePackImpl([[maybe_unused]] core::MIUpdatingIterator &mit,
                                        [[maybe_unused]] core::Descriptor &d) {
  DEBUG_ASSERT(!"Common path shall be used in case of ExpressionColumn.");
}

int64_t ExpressionColumn::RoughMinImpl() {
  if (core::ATI::IsRealType(TypeName())) {
    double dmin = -(DBL_MAX);
    return *(int64_t *)(&dmin);
  }
  return common::MINUS_INF_64;
}

int64_t ExpressionColumn::RoughMaxImpl() {
  if (core::ATI::IsRealType(TypeName())) {
    double dmax = DBL_MAX;
    return *(int64_t *)(&dmax);
  }
  return common::PLUS_INF_64;
}

int64_t ExpressionColumn::GetNumOfNullsImpl(core::MIIterator const &, [[maybe_unused]] bool val_nulls_possible) {
  return common::NULL_VALUE_64;
}

bool ExpressionColumn::IsDistinctImpl() { return false; }

// the column is a deterministic expression on exactly one lookup column
bool ExpressionColumn::ExactlyOneLookup() {
  if (!deterministic_)
    return false;
  auto iter = var_map_.begin();
  if (iter == var_map_.end() || !iter->GetTabPtr()->GetColumnType(iter->col_ndx).Lookup())
    return false;  // not a lookup
  iter++;
  if (iter != var_map_.end())  // more than one column
    return false;
  return true;
}

VirtualColumnBase::VarMap ExpressionColumn::GetLookupCoordinates() {
  auto iter = var_map_.begin();
  return *iter;
}

void ExpressionColumn::FeedLookupArguments(core::MILookupIterator &mit) {
  auto iter = var_map_.begin();
  core::TianmuAttr *col = (core::TianmuAttr *)(iter->GetTabPtr()->GetColumn(iter->col_ndx));
  core::ValueOrNull v = types::BString();
  if (mit.IsValid() && mit[0] != common::NULL_VALUE_64 && mit[0] < col->Cardinality())
    v = col->DecodeValue_S(mit[0]);

  auto cache = var_buf_.find(iter->var_id);
  for (auto &val_it : cache->second) *(val_it.second) = val_it.first = v;

  if (mit.IsValid() && mit[0] != common::NULL_VALUE_64 && mit[0] >= col->Cardinality())
    mit.Invalidate();
}

void ExpressionColumn::LockSourcePacks(const core::MIIterator &mit) {
  for (auto &it : var_map_) it.just_a_table_ptr = it.GetTabPtr().get();
  VirtualColumn::LockSourcePacks(mit);
}
}  // namespace vcolumn
}  // namespace Tianmu
