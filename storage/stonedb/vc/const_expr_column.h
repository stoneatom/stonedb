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
#ifndef STONEDB_VC_CONST_EXPR_COLUMN_H_
#define STONEDB_VC_CONST_EXPR_COLUMN_H_
#pragma once

#include "core/mi_updating_iterator.h"
#include "core/pack_guardian.h"
#include "expr_column.h"

namespace stonedb {
namespace vcolumn {
/*! \brief A column defined by an expression (including a subquery) or
 * encapsulating a PhysicalColumn ConstExpressionColumn is associated with an
 * core::MultiIndex object and cannot exist without it. Values contained in
 * ConstExpressionColumn object may not exist physically, they can be computed
 * on demand.
 *
 */
class ConstExpressionColumn : public ExpressionColumn {
 public:
  /*! \brief Create a column that represent const value.
   *
   * \param val - value for const column.
   * \param ct - type of const column.
   */
  ConstExpressionColumn(core::MysqlExpression *expr, core::TempTable *temp_table, int temp_table_alias,
                        core::MultiIndex *mind)
      : ExpressionColumn(expr, temp_table, temp_table_alias, mind) {
    // status = VC_CONST;
    dim = -1;
    if (params.size() == 0) last_val = expr->Evaluate();
  }

  ConstExpressionColumn(core::MysqlExpression *expr, core::ColumnType forced_ct, core::TempTable *temp_table,
                        int temp_table_alias, core::MultiIndex *mind)
      : ExpressionColumn(expr, temp_table, temp_table_alias, mind) {
    // special case when naked column is a parameter
    // status = VC_CONST; Used for naked attributes as parameters
    dim = -1;
    //		if(params.size() == 0)
    //			last_val = expr->Evaluate();
    ct = forced_ct;
    ct = ct.RemovedLookup();
  }

  ConstExpressionColumn(ConstExpressionColumn const &cc) : ExpressionColumn(NULL, NULL, common::NULL_VALUE_32, NULL) {
    DEBUG_ASSERT(params.size() == 0 && "cannot copy expressions");
    last_val = cc.last_val;
    ct = cc.ct;
    first_eval = cc.first_eval;
  }

  ~ConstExpressionColumn() {}
  bool IsConst() const override { return true; }
  void RequestEval(const core::MIIterator &mit, const int tta) override;
  int64_t GetNotNullValueInt64([[maybe_unused]] const core::MIIterator &mit) override { return last_val->Get64(); }
  void GetNotNullValueString(types::BString &s, [[maybe_unused]] const core::MIIterator &mit) override {
    last_val->GetBString(s);
  }
  types::BString DecodeValue_S(int64_t code) override;  // lookup (physical) only
  int EncodeValue_S([[maybe_unused]] types::BString &v) override { return -1; }
  bool CanCopy() const override { return params.size() == 0; }
  bool IsThreadSafe() override { return params.size() == 0; }

 protected:
  int64_t DoGetValueInt64([[maybe_unused]] const core::MIIterator &mit) override {
    return last_val->IsNull() ? common::NULL_VALUE_64 : last_val->Get64();
  }
  bool DoIsNull([[maybe_unused]] const core::MIIterator &mit) override { return last_val->IsNull(); }
  void DoGetValueString(types::BString &s, [[maybe_unused]] const core::MIIterator &mit) override {
    last_val->GetBString(s);
  }
  double DoGetValueDouble(const core::MIIterator &mit) override;
  types::RCValueObject DoGetValue(const core::MIIterator &mit, bool lookup_to_num) override;
  int64_t DoGetMinInt64([[maybe_unused]] const core::MIIterator &mit) override {
    return last_val->IsNull() ? common::MINUS_INF_64 : last_val->Get64();
  }
  int64_t DoGetMaxInt64([[maybe_unused]] const core::MIIterator &mit) override {
    return last_val->IsNull() ? common::PLUS_INF_64 : last_val->Get64();
  }
  int64_t DoRoughMin() override {
    return (last_val->IsNull() || last_val->Get64() == common::NULL_VALUE_64) ? common::MINUS_INF_64
                                                                              : last_val->Get64();
  }
  int64_t DoRoughMax() override {
    return (last_val->IsNull() || last_val->Get64() == common::NULL_VALUE_64) ? common::PLUS_INF_64 : last_val->Get64();
  }
  types::BString DoGetMaxString(const core::MIIterator &mit) override;
  types::BString DoGetMinString(const core::MIIterator &mit) override;
  int64_t DoGetNoNulls(const core::MIIterator &mit, [[maybe_unused]] bool val_nulls_possible) override {
    return last_val->IsNull() ? mit.GetPackSizeLeft() : (mit.NullsPossibleInPack() ? common::NULL_VALUE_64 : 0);
  }
  bool DoRoughNullsOnly() const override { return last_val->IsNull(); }
  bool DoNullsPossible([[maybe_unused]] bool val_nulls_possible) override { return last_val->IsNull(); }
  int64_t DoGetSum(const core::MIIterator &mit, bool &nonnegative) override;
  bool DoIsDistinct() override {
    return (mind->TooManyTuples() || mind->NoTuples() > 1) ? false : (!last_val->IsNull());
  }
  int64_t DoGetApproxDistVals(bool incl_nulls, core::RoughMultiIndex *rough_mind) override;
  size_t DoMaxStringSize() override;  // maximal byte string length in column
  core::PackOntologicalStatus DoGetPackOntologicalStatus(const core::MIIterator &mit) override;
  common::RSValue DoRoughCheck(const core::MIIterator &it, core::Descriptor &d) override;
  void DoEvaluatePack(core::MIUpdatingIterator &mit, core::Descriptor &desc) override;
  // comparison of a const with a const should be simplified earlier
  virtual common::ErrorCode DoEvaluateOnIndex([[maybe_unused]] core::MIUpdatingIterator &mit, core::Descriptor &,
                                              [[maybe_unused]] int64_t limit) override {
    DEBUG_ASSERT(0);
    return common::ErrorCode::FAILED;
  }
};
}  // namespace vcolumn
}  // namespace stonedb

#endif  // STONEDB_VC_CONST_EXPR_COLUMN_H_
