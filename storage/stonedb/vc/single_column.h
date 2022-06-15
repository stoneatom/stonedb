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
#ifndef STONEDB_VC_SINGLE_COLUMN_H_
#define STONEDB_VC_SINGLE_COLUMN_H_
#pragma once

#include "core/mi_updating_iterator.h"
#include "core/pack_guardian.h"
#include "core/physical_column.h"
#include "vc/virtual_column.h"

namespace stonedb {
namespace vcolumn {
/*! \brief A column defined by an expression (including a subquery) or
 * encapsulating a PhysicalColumn SingleColumn is associated with an
 * core::MultiIndex object and cannot exist without it. Values contained in
 * SingleColumn object may not exist physically, they can be computed on demand.
 *
 */
class SingleColumn : public VirtualColumn {
 public:
  /*! \brief Create a virtual column for a given column
   *
   * \param col the column to be "wrapped" by VirtualColumn
   * \param _mind the multiindex to which the SingleColumn is attached.
   * \param alias the alias number of the source table
   * \param col_no number of the column in the table, negative if col of
   * core::TempTable \param source_table pointer to the table holding \endcode
   * col \param dim dimension number in the multiindex holding the column. \e
   * note: not sure if the pointer to the core::MultiIndex is necessary
   */
  SingleColumn(core::PhysicalColumn *col, core::MultiIndex *mind, int alias, int col_no, core::JustATable *source_table,
               int dim);
  SingleColumn(const SingleColumn &col);
  ~SingleColumn();
  virtual uint64_t ApproxAnswerSize(core::Descriptor &d) { return col->ApproxAnswerSize(d); }
  core::PhysicalColumn *GetPhysical() const { return col; }
  bool IsConst() const override { return false; }
  single_col_t IsSingleColumn() const override {
    return col->ColType() == core::PhysicalColumn::phys_col_t::ATTR ? single_col_t::SC_ATTR : single_col_t::SC_RCATTR;
  }
  char *ToString(char p_buf[], size_t buf_ct) const override;
  virtual void TranslateSourceColumns(std::map<core::PhysicalColumn *, core::PhysicalColumn *> &);
  void GetNotNullValueString(types::BString &s, const core::MIIterator &mit) override {
    col->GetNotNullValueString(mit[dim], s);
  }
  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override { return col->GetNotNullValueInt64(mit[dim]); }
  bool IsDistinctInTable() override { return col->IsDistinct(mind->GetFilter(dim)); }
  bool IsTempTableColumn() const { return col->ColType() == core::PhysicalColumn::phys_col_t::ATTR; }
  void GetTextStat(types::TextStat &s) override { col->GetTextStat(s, mind->GetFilter(dim)); }
  bool CanCopy() const override {
    return (col->ColType() != core::PhysicalColumn::phys_col_t::ATTR);
  }  // cannot copy core::TempTable, as it may be paged
  bool IsThreadSafe() override {
    return (col->ColType() != core::PhysicalColumn::phys_col_t::ATTR);
  }  // TODO: for ATTR check if not materialized and not buffered

 protected:
  int64_t DoGetValueInt64(const core::MIIterator &mit) override { return col->GetValueInt64(mit[dim]); }
  bool DoIsNull(const core::MIIterator &mit) override { return col->IsNull(mit[dim]); }
  void DoGetValueString(types::BString &s, const core::MIIterator &mit) override { col->GetValueString(mit[dim], s); }
  double DoGetValueDouble(const core::MIIterator &mit) override;
  types::RCValueObject DoGetValue(const core::MIIterator &mit, bool lookup_to_num) override;
  int64_t DoGetMinInt64(const core::MIIterator &mit) override;
  int64_t DoGetMaxInt64(const core::MIIterator &mit) override;
  int64_t DoGetMinInt64Exact(const core::MIIterator &mit) override;
  int64_t DoGetMaxInt64Exact(const core::MIIterator &mit) override;
  types::BString DoGetMinString(const core::MIIterator &) override;
  types::BString DoGetMaxString(const core::MIIterator &) override;
  int64_t DoRoughMin() override { return col->RoughMin(mind->GetFilter(dim)); }
  int64_t DoRoughMax() override { return col->RoughMax(mind->GetFilter(dim)); }
  double RoughSelectivity() override;

  int64_t DoGetNoNulls(const core::MIIterator &mit, bool val_nulls_possible) override {
    int64_t res;
    if (mit.GetCurPackrow(dim) >= 0 && !mit.NullsPossibleInPack(dim)) {  // if outer nulls possible - cannot
                                                                         // calculate precise result
      if (!val_nulls_possible) return 0;
      res = col->GetNoNulls(mit.GetCurPackrow(dim));
      if (res == 0 || mit.WholePack(dim)) return res;
    }
    return common::NULL_VALUE_64;
  }

  bool DoRoughNullsOnly() const override { return col->RoughNullsOnly(); }
  bool DoNullsPossible(bool val_nulls_possible) override {
    if (mind->NullsExist(dim)) return true;
    if (val_nulls_possible && col->GetNoNulls(-1) != 0) return true;
    return false;
  }
  int64_t DoGetSum(const core::MIIterator &mit, bool &nonnegative) override;
  int64_t DoGetApproxSum(const core::MIIterator &mit, bool &nonnegative) override;

  bool DoIsDistinct() override { return (col->IsDistinct(mind->GetFilter(dim)) && mind->CanBeDistinct(dim)); }
  int64_t DoGetApproxDistVals(bool incl_nulls, core::RoughMultiIndex *rough_mind) override;
  int64_t GetExactDistVals() override;
  size_t DoMaxStringSize() override;  // maximal byte string length in column
  core::PackOntologicalStatus DoGetPackOntologicalStatus(const core::MIIterator &mit) override;
  common::RSValue DoRoughCheck(const core::MIIterator &it, core::Descriptor &d) override;
  void DoEvaluatePack(core::MIUpdatingIterator &mit, core::Descriptor &desc) override;
  virtual common::ErrorCode DoEvaluateOnIndex(core::MIUpdatingIterator &mit, core::Descriptor &desc,
                                              int64_t limit) override {
    return col->EvaluateOnIndex(mit, dim, desc, limit);
  }
  bool TryToMerge(core::Descriptor &d1, core::Descriptor &d2) override;
  types::BString DecodeValue_S(int64_t code) override { return col->DecodeValue_S(code); }
  int EncodeValue_S(types::BString &v) override { return col->EncodeValue_S(v); }
  std::vector<int64_t> GetListOfDistinctValues(core::MIIterator const &mit) override;
  void DisplayAttrStats() override;
  const core::MysqlExpression::sdbfields_cache_t &GetSDBItems() const override {
    static core::MysqlExpression::sdbfields_cache_t const dummy;
    return dummy;
  }
  core::PhysicalColumn *col;  //!= NULL if SingleColumn encapsulates a single
                              //! column only (no expression)
                              //	this an easily accessible copy
                              // var_map[0].tab->GetColumn(var_map[0].col_ndx)
};
}  // namespace vcolumn
}  // namespace stonedb

#endif  // STONEDB_VC_SINGLE_COLUMN_H_
