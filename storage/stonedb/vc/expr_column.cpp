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
#include "core/rc_attr.h"

namespace stonedb {
namespace vcolumn {
ExpressionColumn::ExpressionColumn(core::MysqlExpression *expr, core::TempTable *temp_table, int temp_table_alias,
                                   core::MultiIndex *mind)
    : VirtualColumn(core::ColumnType(), mind), expr(expr), deterministic(expr ? expr->IsDeterministic() : true) {
  const std::vector<core::JustATable *> *tables = &temp_table->GetTables();
  const std::vector<int> *aliases = &temp_table->GetAliases();

  if (expr) {
    vars = expr->GetVars();  // get all variables from complex term
    first_eval = true;
    // status = deterministic ? VC_EXPR : VC_EXPR_NONDET;
    // fill types for variables and create buffers for argument values
    int only_dim_number = -2;  // -2 = not used yet
    for (auto &v : vars) {
      auto ndx_it = find(aliases->begin(), aliases->end(), v.tab);
      if (ndx_it != aliases->end()) {
        int ndx = int(distance(aliases->begin(), ndx_it));

        var_map.push_back(VarMap(v, (*tables)[ndx], ndx));
        if (only_dim_number == -2 || only_dim_number == ndx)
          only_dim_number = ndx;
        else
          only_dim_number = -1;  // more than one

        var_types[v] = (*tables)[ndx]->GetColumnType(var_map[var_map.size() - 1].col_ndx);
        var_buf[v] = std::vector<core::MysqlExpression::value_or_null_info_t>();  // now empty, pointers
                                                                                  // inserted by SetBufs()
      } else if (v.tab == temp_table_alias) {
        var_map.push_back(VarMap(v, temp_table, 0));
        if (only_dim_number == -2 || only_dim_number == 0)
          only_dim_number = 0;
        else
          only_dim_number = -1;  // more than one

        var_types[v] = temp_table->GetColumnType(var_map[var_map.size() - 1].col_ndx);
        var_buf[v] = std::vector<core::MysqlExpression::value_or_null_info_t>();  // now empty, pointers
                                                                                  // inserted by SetBufs()
      } else {
        // parameter
        // parameter type is not available here, must be set later (EvalType())
        params.insert(v);
        //				param_buf[v].second = NULL; //assigned
        // by
        // SetBufs()
      }
    }
    ct = core::ColumnType(expr->EvalType(&var_types));  // set the column type from expression result type
    expr->SetBufsOrParams(&var_buf);
    //		expr->SetBufsOrParams(&param_buf);
    dim = (only_dim_number >= 0 ? only_dim_number : -1);

    // if (status == VC_EXPR && var_map.size() == 0 )
    //	status = VC_CONST;
  } else {
    DEBUG_ASSERT(!"unexpected!!");
  }
}

ExpressionColumn::ExpressionColumn(const ExpressionColumn &ec)
    : VirtualColumn(ec),
      expr(ec.expr),
      vars(ec.vars),
      var_types(ec.var_types),
      var_buf(ec.var_buf),
      deterministic(ec.deterministic) {
  var_map = ec.var_map;
}

void ExpressionColumn::SetParamTypes(core::MysqlExpression::TypOfVars *types) { expr->EvalType(types); }

bool ExpressionColumn::FeedArguments(const core::MIIterator &mit) {
  bool diff = first_eval;
  if (mit.Type() == core::MIIterator::MIIteratorType::MII_LOOKUP) {
    core::MILookupIterator *mit_lookup = (core::MILookupIterator *)(&mit);
    FeedLookupArguments(*mit_lookup);
    first_eval = false;
    return true;
  }
  for (auto &it : var_map) {
    core::ValueOrNull v(it.tabp->GetComplexValue(mit[it.dim], it.col_ndx));
    v.MakeStringOwner();
    auto cache = var_buf.find(it.var);
    DEBUG_ASSERT(cache != var_buf.end());
    diff = diff || (v != cache->second.begin()->first);
    if (diff)
      for (auto &val_it : cache->second) *(val_it.second) = val_it.first = v;
  }
  first_eval = false;
  return (diff || !deterministic);
}

int64_t ExpressionColumn::DoGetValueInt64(const core::MIIterator &mit) {
  if (FeedArguments(mit)) last_val = expr->Evaluate();
  if (last_val->IsNull()) return common::NULL_VALUE_64;
  return last_val->Get64();
}

bool ExpressionColumn::DoIsNull(const core::MIIterator &mit) {
  if (FeedArguments(mit)) last_val = expr->Evaluate();
  return last_val->IsNull();
}

void ExpressionColumn::DoGetValueString(types::BString &s, const core::MIIterator &mit) {
  if (FeedArguments(mit)) last_val = expr->Evaluate();
  if (core::ATI::IsDateTimeType(TypeName())) {
    int64_t tmp;
    types::RCDateTime vd(last_val->Get64(), TypeName());
    vd.ToInt64(tmp);
    last_val->SetFixed(tmp);
  }
  last_val->GetBString(s);
}

double ExpressionColumn::DoGetValueDouble(const core::MIIterator &mit) {
  double val = 0;
  if (FeedArguments(mit)) last_val = expr->Evaluate();
  if (last_val->IsNull()) val = NULL_VALUE_D;
  if (core::ATI::IsIntegerType(TypeName()))
    val = (double)last_val->Get64();
  else if (core::ATI::IsFixedNumericType(TypeName()))
    val = ((double)last_val->Get64()) / types::PowOfTen(ct.GetScale());
  else if (core::ATI::IsRealType(TypeName())) {
    val = last_val->GetDouble();
  } else if (core::ATI::IsDateTimeType(TypeName())) {
    types::RCDateTime vd(last_val->Get64(),
                         TypeName());  // 274886765314048  ->  2000-01-01
    int64_t vd_conv = 0;
    vd.ToInt64(vd_conv);  // 2000-01-01  ->  20000101
    val = (double)vd_conv;
  } else if (core::ATI::IsStringType(TypeName())) {
    auto str = last_val->ToString();
    if (str) val = std::stod(*str);
  } else
    DEBUG_ASSERT(0 && "conversion to double not implemented");
  return val;
}

types::RCValueObject ExpressionColumn::DoGetValue(const core::MIIterator &mit, bool lookup_to_num) {
  if (core::ATI::IsStringType((TypeName()))) {
    types::BString s;
    GetValueString(s, mit);
    return s;
  }
  if (core::ATI::IsIntegerType(TypeName())) return types::RCNum(GetValueInt64(mit), -1, false, TypeName());
  if (core::ATI::IsDateTimeType(TypeName())) return types::RCDateTime(GetValueInt64(mit), TypeName());
  if (core::ATI::IsRealType(TypeName())) return types::RCNum(GetValueInt64(mit), 0, true, TypeName());
  if (lookup_to_num || TypeName() == common::CT::NUM) return types::RCNum(GetValueInt64(mit), Type().GetScale());
  DEBUG_ASSERT(!"Illegal execution path");
  return types::RCValueObject();
}

int64_t ExpressionColumn::DoGetSum([[maybe_unused]] const core::MIIterator &mit, bool &nonnegative) {
  nonnegative = false;
  return common::NULL_VALUE_64;  // not implemented
}

int64_t ExpressionColumn::DoGetMinInt64([[maybe_unused]] const core::MIIterator &mit) {
  return common::MINUS_INF_64;  // not implemented
}

int64_t ExpressionColumn::DoGetMaxInt64([[maybe_unused]] const core::MIIterator &mit) {
  return common::PLUS_INF_64;  // not implemented
}

types::BString ExpressionColumn::DoGetMinString([[maybe_unused]] const core::MIIterator &mit) {
  return types::BString();  // not implemented
}

types::BString ExpressionColumn::DoGetMaxString([[maybe_unused]] const core::MIIterator &mit) {
  return types::BString();  // not implemented
}

int64_t ExpressionColumn::DoGetApproxDistVals([[maybe_unused]] bool incl_nulls,
                                              [[maybe_unused]] core::RoughMultiIndex *rough_mind) {
  if (mind->TooManyTuples()) return common::PLUS_INF_64;
  return mind->NoTuples();  // default
}

size_t ExpressionColumn::DoMaxStringSize()  // maximal byte string length in column
{
  return ct.GetPrecision();  // default
}

core::PackOntologicalStatus ExpressionColumn::DoGetPackOntologicalStatus(const core::MIIterator &mit) {
  core::PackOntologicalStatus st =
      deterministic ? core::PackOntologicalStatus::UNIFORM : core::PackOntologicalStatus::NORMAL;  // will be used for
                                                                                                   // 0 arguments
  // what about 0 arguments and null only?
  core::PackOntologicalStatus st_loc;
  for (auto &it : var_map) {
    // cast to remove const as GetPackOntologicalStatus() is not const
    st_loc = ((core::PhysicalColumn *)it.GetTabPtr()->GetColumn(it.col_ndx))
                 ->GetPackOntologicalStatus(mit.GetCurPackrow(it.dim));
    if (st_loc != core::PackOntologicalStatus::UNIFORM && st_loc != core::PackOntologicalStatus::NULLS_ONLY)
      return core::PackOntologicalStatus::NORMAL;
    // if(NULLS_ONLY) and the expression may not be nontrivial on any null =>
    // NULLS_ONLY
  }
  return st;
}

void ExpressionColumn::DoEvaluatePack([[maybe_unused]] core::MIUpdatingIterator &mit,
                                      [[maybe_unused]] core::Descriptor &d) {
  DEBUG_ASSERT(!"Common path shall be used in case of ExpressionColumn.");
}

int64_t ExpressionColumn::DoRoughMin() {
  if (core::ATI::IsRealType(TypeName())) {
    double dmin = -(DBL_MAX);
    return *(int64_t *)(&dmin);
  }
  return common::MINUS_INF_64;
}

int64_t ExpressionColumn::DoRoughMax() {
  if (core::ATI::IsRealType(TypeName())) {
    double dmax = DBL_MAX;
    return *(int64_t *)(&dmax);
  }
  return common::PLUS_INF_64;
}

int64_t ExpressionColumn::DoGetNoNulls(core::MIIterator const &, [[maybe_unused]] bool val_nulls_possible) {
  return common::NULL_VALUE_64;
}

bool ExpressionColumn::DoIsDistinct() { return false; }

// the column is a deterministic expression on exactly one lookup column
bool ExpressionColumn::ExactlyOneLookup() {
  if (!deterministic) return false;
  auto iter = var_map.begin();
  if (iter == var_map.end() || !iter->GetTabPtr()->GetColumnType(iter->col_ndx).IsLookup())
    return false;  // not a lookup
  iter++;
  if (iter != var_map.end())  // more than one column
    return false;
  return true;
}

VirtualColumnBase::VarMap ExpressionColumn::GetLookupCoordinates() {
  auto iter = var_map.begin();
  return *iter;
}

void ExpressionColumn::FeedLookupArguments(core::MILookupIterator &mit) {
  auto iter = var_map.begin();
  core::RCAttr *col = (core::RCAttr *)(iter->GetTabPtr()->GetColumn(iter->col_ndx));
  core::ValueOrNull v = types::BString();
  if (mit.IsValid() && mit[0] != common::NULL_VALUE_64 && mit[0] < col->Cardinality()) v = col->DecodeValue_S(mit[0]);

  auto cache = var_buf.find(iter->var);
  for (auto &val_it : cache->second) *(val_it.second) = val_it.first = v;

  if (mit.IsValid() && mit[0] != common::NULL_VALUE_64 && mit[0] >= col->Cardinality()) mit.Invalidate();
}

void ExpressionColumn::LockSourcePacks(const core::MIIterator &mit) {
  for (auto &it : var_map) it.tabp = it.GetTabPtr().get();
  VirtualColumn::LockSourcePacks(mit);
}
}  // namespace vcolumn
}  // namespace stonedb
