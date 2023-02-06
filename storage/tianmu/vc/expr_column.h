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
#ifndef TIANMU_VC_EXPR_COLUMN_H_
#define TIANMU_VC_EXPR_COLUMN_H_
#pragma once

#include <mutex>

#include "core/mi_updating_iterator.h"
#include "core/pack_guardian.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
class TempTable;
}
namespace vcolumn {

/*! \brief A column defined by an expression (including a subquery) or
 * encapsulating a PhysicalColumn ExpressionColumn is associated with an
 * core::MultiIndex object and cannot exist without it. Values contained in
 * ExpressionColumn object may not exist physically, they can be computed on
 * demand.
 *
 */
class ExpressionColumn : public VirtualColumn {
 public:
  // the struct below is for precomputing relations between variables, tables
  // and dimensions

  /*! \brief Create a virtual column for given expression
   *
   * \param expr expression defining the column
   * \param temp_table core::TempTable for which the column is created
   * \param temp_table_alias alias of core::TempTable for which the column is
   * created \param multi_index the multiindex to which the ExpressionColumn is
   * attached. \e note: not sure if the connection to the core::MultiIndex is
   * necessary \param tables list of tables corresponding to dimensions in \e
   * multi_index, ExpressionColumn must access the tables to fetch expression arguments
   *
   * The constructor takes from \e expr a list of dimensions/tables providing
   * arguments for the expression. Therefore the list of multiindex dimensions
   * necessary for evaluation if computed from \e expr
   */
  ExpressionColumn(core::MysqlExpression *expr, core::TempTable *temp_table, int temp_table_alias,
                   core::MultiIndex *multi_index);
  ExpressionColumn(const ExpressionColumn &);
  virtual ~ExpressionColumn() = default;

  const core::MysqlExpression::tianmu_fields_cache_t &GetItems() const;
  void SetParamTypes(core::MysqlExpression::TypOfVars *types) override;
  bool IsConst() const override { return false; }
  bool IsDeterministic() override { return expr_->IsDeterministic(); }
  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override { return GetValueInt64Impl(mit); }
  void GetNotNullValueString(types::BString &s, const core::MIIterator &mit) override { GetValueStringImpl(s, mit); }
  core::MysqlExpression::StringType GetStringType() { return expr_->GetStringType(); }

  // Special functions for expressions on lookup
  virtual bool ExactlyOneLookup();  // the column is a deterministic expression
                                    // on exactly one lookup column, return the
                                    // coordinates of this column
  virtual VirtualColumnBase::VarMap GetLookupCoordinates();
  virtual void FeedLookupArguments(core::MILookupIterator &mit);
  void LockSourcePacks(const core::MIIterator &mit) override;
  void LockSourcePacks(const core::MIIterator &mit, int);

  Item *GetItem() override { return expr_->GetItem(); }

  /////////////// Data access //////////////////////
 protected:
  int64_t GetValueInt64Impl(const core::MIIterator &) override;
  bool IsNullImpl(const core::MIIterator &) override;
  void GetValueStringImpl(types::BString &, const core::MIIterator &) override;
  double GetValueDoubleImpl(const core::MIIterator &) override;
  types::TianmuValueObject GetValueImpl(const core::MIIterator &, bool) override;

  int64_t GetMinInt64Impl(const core::MIIterator &) override;
  int64_t GetMaxInt64Impl(const core::MIIterator &) override;
  int64_t RoughMinImpl() override;
  int64_t RoughMaxImpl() override;

  types::BString GetMaxStringImpl(const core::MIIterator &) override;
  types::BString GetMinStringImpl(const core::MIIterator &) override;
  int64_t GetNumOfNullsImpl(const core::MIIterator &, bool val_nulls_possible) override;

  bool IsRoughNullsOnlyImpl() const override { return false; }
  bool IsNullsPossibleImpl([[maybe_unused]] bool val_nulls_possible) override { return true; }
  int64_t GetSumImpl(const core::MIIterator &, bool &nonnegative) override;
  bool IsDistinctImpl() override;
  int64_t GetApproxDistValsImpl(bool incl_nulls, core::RoughMultiIndex *rough_mind) override;
  size_t MaxStringSizeImpl() override;  // maximal byte string length in column

  core::PackOntologicalStatus GetPackOntologicalStatusImpl(const core::MIIterator &) override;
  void EvaluatePackImpl(core::MIUpdatingIterator &mit, core::Descriptor &) override;
  virtual common::ErrorCode EvaluateOnIndexImpl([[maybe_unused]] core::MIUpdatingIterator &mit, core::Descriptor &,
                                                [[maybe_unused]] int64_t limit) override {
    TIANMU_ERROR("Common path shall be used in case of ExpressionColumn.");
    return common::ErrorCode::FAILED;
  }

  const core::MysqlExpression::tianmu_fields_cache_t &GetTIANMUItems() const override {
    return expr_->GetTIANMUItems();
  }
  core::MysqlExpression *expr_;  //!= nullptr if ExpressionColumn encapsulates an expression. Note - a
                                 //! constant is an expression

 private:
  /*! \brief Set variable buffers with values for a given iterator.
   *
   * \param mit - iterator that new variable values shall be based on.
   * \return true if new values are exactly the same as previous (already
   * existing.
   */
  bool FeedArguments(const core::MIIterator &mit);

  // if ExpressionColumn ExpressionColumn encapsulates an expression these sets
  // are used to interface with core::MysqlExpression
  core::MysqlExpression::SetOfVars vars_;
  core::MysqlExpression::TypOfVars var_types_;
  mutable core::MysqlExpression::var_buf_t var_buf_;

  //! value for a given row is always the same or not? e.g. currenttime() is not
  //! deterministic
  bool deterministic_;
};
}  // namespace vcolumn
}  // namespace Tianmu

#endif  // TIANMU_VC_EXPR_COLUMN_H_
