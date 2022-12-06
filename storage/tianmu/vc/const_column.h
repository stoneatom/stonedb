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
#ifndef TIANMU_VC_CONST_COLUMN_H_
#define TIANMU_VC_CONST_COLUMN_H_
#pragma once

#include "core/mi_updating_iterator.h"
#include "core/pack_guardian.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace vcolumn {
/*! \brief A column defined by an expression (including a subquery) or
 * encapsulating a PhysicalColumn ConstColumn is associated with an
 * core::MultiIndex object and cannot exist without it. Values contained in
 * ConstColumn object may not exist physically, they can be computed on demand.
 *
 */
class ConstColumn : public VirtualColumn {
 public:
  /*! \brief Create a column that represent const value.
   *
   * \param val - value for const column.
   * \param ct - type of const column.
   */
  ConstColumn(core::ValueOrNull const &val, core::ColumnType const &c, bool shift_to_UTC = false);
  ConstColumn(const types::TianmuValueObject &v, const core::ColumnType &c);
  ConstColumn(ConstColumn const &cc) : VirtualColumn(cc.ct, cc.multi_index_), value_or_null_(cc.value_or_null_) {}
  ~ConstColumn() {}
  bool IsConst() const override { return true; }
  const core::MysqlExpression::tianmu_fields_cache_t &GetTIANMUItems() const override {
    static core::MysqlExpression::tianmu_fields_cache_t const dummy;
    return dummy;
  }
  char *ToString(char p_buf[], size_t buf_ct) const override;
  int64_t GetNotNullValueInt64([[maybe_unused]] const core::MIIterator &mit) override { return value_or_null_.Get64(); }
  void GetNotNullValueString(types::BString &s, [[maybe_unused]] const core::MIIterator &mit) override {
    value_or_null_.GetBString(s);
  }
  bool IsThreadSafe() override { return true; }
  bool CanCopy() const override { return true; }

 protected:
  int64_t GetValueInt64Impl([[maybe_unused]] const core::MIIterator &mit) override {
    return value_or_null_.IsNull() ? common::NULL_VALUE_64 : value_or_null_.Get64();
  }
  bool IsNullImpl([[maybe_unused]] const core::MIIterator &mit) override { return value_or_null_.IsNull(); }
  void GetValueStringImpl(types::BString &s, const core::MIIterator &mit) override;
  double GetValueDoubleImpl(const core::MIIterator &mit) override;
  types::TianmuValueObject GetValueImpl(const core::MIIterator &mit, bool lookup_to_num) override;

  int64_t GetMinInt64Impl([[maybe_unused]] const core::MIIterator &mit) override {
    return value_or_null_.IsNull() || value_or_null_.IsString() ? common::MINUS_INF_64 : value_or_null_.Get64();
  }
  int64_t GetMaxInt64Impl([[maybe_unused]] const core::MIIterator &mit) override {
    return value_or_null_.IsNull() || value_or_null_.IsString() ? common::PLUS_INF_64 : value_or_null_.Get64();
  }
  int64_t RoughMinImpl() override {
    return value_or_null_.IsNull() || value_or_null_.IsString() ? common::MINUS_INF_64 : value_or_null_.Get64();
  }
  int64_t RoughMaxImpl() override {
    return value_or_null_.IsNull() || value_or_null_.IsString() ? common::PLUS_INF_64 : value_or_null_.Get64();
  }
  int64_t GetMinInt64ExactImpl(const core::MIIterator &) override {
    return value_or_null_.IsNull() ? common::NULL_VALUE_64 : value_or_null_.Get64();
  }
  int64_t GetMaxInt64ExactImpl(const core::MIIterator &) override {
    return value_or_null_.IsNull() ? common::NULL_VALUE_64 : value_or_null_.Get64();
  }

  types::BString GetMaxStringImpl(const core::MIIterator &mit) override;
  types::BString GetMinStringImpl(const core::MIIterator &mit) override;

  int64_t GetNumOfNullsImpl(const core::MIIterator &mit, [[maybe_unused]] bool val_nulls_possible) override {
    return value_or_null_.IsNull() ? mit.GetPackSizeLeft() : (mit.NullsPossibleInPack() ? common::NULL_VALUE_64 : 0);
  }

  bool IsRoughNullsOnlyImpl() const override { return value_or_null_.IsNull(); }
  bool IsNullsPossibleImpl([[maybe_unused]] bool val_nulls_possible) override { return value_or_null_.IsNull(); }

  int64_t GetSumImpl(const core::MIIterator &mit, bool &nonnegative) override;
  bool IsDistinctImpl() override { return false; }
  int64_t GetApproxDistValsImpl(bool incl_nulls, core::RoughMultiIndex *rough_mind) override;
  int64_t GetExactDistVals() override;

  size_t MaxStringSizeImpl() override;  // maximal byte string length in column
  core::PackOntologicalStatus GetPackOntologicalStatusImpl(const core::MIIterator &mit) override;
  void EvaluatePackImpl(core::MIUpdatingIterator &mit, core::Descriptor &desc) override;
  // comparison of a const with a const should be simplified earlier
  virtual common::ErrorCode EvaluateOnIndexImpl([[maybe_unused]] core::MIUpdatingIterator &mit, core::Descriptor &,
                                                [[maybe_unused]] int64_t limit) override {
    DEBUG_ASSERT(0);
    return common::ErrorCode::FAILED;
  }
  core::ValueOrNull value_or_null_;
};
}  // namespace vcolumn
}  // namespace Tianmu

#endif  // TIANMU_VC_CONST_COLUMN_H_
