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
#ifndef TIANMU_VC_VIRTUAL_COLUMN_BASE_H_
#define TIANMU_VC_VIRTUAL_COLUMN_BASE_H_
#pragma once

#include <vector>

#include "core/column.h"
#include "core/just_a_table.h"
#include "core/multi_index.h"
#include "core/mysql_expression.h"
#include "core/pack_guardian.h"
#include "types/text_stat.h"

namespace Tianmu {
namespace core {
class MIIterator;
class Descriptor;
class RoughMultiIndex;
class MIUpdatingIterator;
}  // namespace core

namespace vcolumn {
/*! \brief A column defined by an expression (including a subquery) or
 * encapsulating a PhysicalColumn VirtualColumn is associated with an
 * core::MultiIndex object and cannot exist without it. Values contained in
 * VirtualColumn object may not exist physically, they can be computed on
 * demand.
 *
 */

class VirtualColumnBase : public core::Column {
 public:
  struct VarMap {
    VarMap(core::VarID v, core::JustATable *t, int d)
        : var_id(v), col_ndx(v.col < 0 ? -v.col - 1 : v.col), dim(d), just_a_table(t->shared_from_this()) {}
    core::VarID var_id;  // variable identified by table alias and column number
    int col_ndx;         // column index (not negative)
    int dim;             // dimension in multiindex corresponding to the alias
    core::JustATable *just_a_table_ptr;
    std::shared_ptr<core::JustATable> GetTabPtr() const { return just_a_table.lock(); }
    bool operator<(VarMap const &v) const {
      return (var_id < v.var_id) || ((var_id == v.var_id) && (col_ndx < v.col_ndx)) ||
             ((var_id == v.var_id) && (col_ndx == v.col_ndx) && (GetTabPtr() < v.GetTabPtr())) ||
             ((var_id == v.var_id) && (col_ndx == v.col_ndx) && (GetTabPtr() == v.GetTabPtr()) && (dim < v.dim));
    }
    std::weak_ptr<core::JustATable> just_a_table;  // table corresponding to the alias
  };

  /*! \brief Create a virtual column for a given column type.
   *
   * \param col_type the column to be "wrapped" by VirtualColumn
   * \param multi_index the multiindex to which the VirtualColumn is attached.
   */
  VirtualColumnBase(core::ColumnType const &col_type, core::MultiIndex *multi_index);
  VirtualColumnBase(VirtualColumn const &vc);

  enum class single_col_t { SC_NOT = 0, SC_ATTR, SC_RCATTR };

  virtual ~VirtualColumnBase() {}

  /////////////// Data access //////////////////////

  /*! \brief Get a numeric value from a column
   *
   * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and
   * locked
   *
   * Use it for numeric columns only.
   * Old name: \b GetTable64
   *
   * \param mit points to a row in an Multiindex, requested column value comes
   * from this row \return numeric value from the column in level-1 encoding.
   * - Integers values can be used directly
   * - Decimals must be externally properly shifted
   * - Doubles must be cast e.g. *(double*)&
   */
  inline int64_t GetValueInt64(const core::MIIterator &mit) { return GetValueInt64Impl(mit); }
  virtual int64_t GetNotNullValueInt64(const core::MIIterator &mit) = 0;

  /*! \brief get Item
   *
   * \pre The ExpressionColumn implementation is meaningful
   * only if the column attribute is an Expressioncolumn implementation
   *
   */
  virtual Item *GetItem() { return nullptr; };

  /*! \brief Is the column value nullptr ?
   *
   * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and
   * locked
   *
   * \param mit points to a row in an Multiindex, requested column value comes
   * from this row \return \b true if column value is nullptr, \b false otherwise
   */
  inline bool IsNull(const core::MIIterator &mit) { return IsNullImpl(mit); }
  /*! \brief Get a non null-terminated String from a column
   *
   * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and
   * locked
   *
   * Can be used to retrieve binary data
   *
   * \param s a string object to be filled with a retrieved value
   * \param mit points to a row in an Multiindex, requested column value comes
   * from this row \param bin_as_hex in case of a binary column, present the
   * value as hexadecimal number
   */
  inline void GetValueString(types::BString &s, const core::MIIterator &mit) { GetValueStringImpl(s, mit); }
  virtual void GetNotNullValueString(types::BString &s, const core::MIIterator &mit) = 0;

  /*! \brief Get a double value from a column, possibly converting the original
   * value
   *
   * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and
   * locked
   *
   * \param mit points to a row in an Multiindex, requested column value comes
   * from this row
   */
  inline double GetValueDouble(const core::MIIterator &mit) { return (GetValueDoubleImpl(mit)); }
  /*! \brief Get a value from a colunm, whatever the column type is
   *
   * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and
   * locked
   *
   * \param mit points to a row in an Multiindex, requested column value comes
   * from this row \param lookup_to_num legacy meaning (?)
   */
  inline types::TianmuValueObject GetValue(const core::MIIterator &mit, bool lookup_to_num = false) {
    return (GetValueImpl(mit, lookup_to_num));
  }

  /////////////// Statistics //////////////////////

  /*! \brief Get rough minimum for packrow in level-1 encoding
   *
   * \param mit iterator pointing to a packrow, for which min value is requested
   * \return numeric value from the column in level-1 encoding. or
   * common::MINUS_INF_64 if no min available \return undefined for nulls only
   * or empty pack
   * - Integers values can be used directly
   * - Decimals must be externally properly shifted
   * - Doubles must be cast e.g. *(double*)&
   */
  int64_t GetMinInt64(const core::MIIterator &mit);

  /*! \brief Get minimum for packrow in level-1 encoding
   * 	\return min or common::NULL_VALUE_64 if min not known exactly (e.g. pack
   * not full)
   */
  inline int64_t GetMinInt64Exact(const core::MIIterator &mit) { return GetMinInt64ExactImpl(mit); }
  /*! \brief Get rough maximum for packrow in level-1 encoding
   *
   * Old name: GetMax64
   * \param mit iterator pointing to a packrow, for which max value is requested
   * \return numeric value from the column in level-1 encoding. or
   * common::PLUS_INF_64 if no max available \return undefined for nulls only or
   * empty pack
   * - Integers values can be used directly
   * - Decimals must be externally properly shifted
   * - Doubles must be cast e.g. *(double*)&
   */
  int64_t GetMaxInt64(const core::MIIterator &mit);

  /*! \brief Get maximum for packrow in level-1 encoding
   * 	\return max or common::NULL_VALUE_64 if max not known exactly (e.g. pack
   * not full)
   */
  inline int64_t GetMaxInt64Exact(const core::MIIterator &mit) { return GetMaxInt64ExactImpl(mit); }
  /*! \brief Get rough maximum for packrow in level-1 encoding
   *
   * \param mit iterator pointing to a packrow, for which max value is requested
   * \return string value from the column in level-1 encoding or
   * BString.IsNULL() if max not available
   */
  inline types::BString GetMaxString(const core::MIIterator &mit) { return GetMaxStringImpl(mit); }
  /*! \brief Get rough minimum for packrow in level-1 encoding
   *
   * \param mit iterator pointing to a packrow, for which min value is requested
   * \return string value from the column in level-1 encoding or
   * BString.IsNULL() if min not available
   */
  inline types::BString GetMinString(const core::MIIterator &mit) { return GetMinStringImpl(mit); }
  /*! \brief Get (global) rough minimum for numerical column in level-1 encoding
   *  For floating point types it is a double min encoded on int64_t
   *  Special values: common::MINUS_INF_64 is default (also for double),
   * undefined for nulls only or empty set
   */
  int64_t RoughMin();

  /*! \brief Get (global) rough maximum for numerical column in level-1 encoding
   *  For floating point types it is a double max encoded on int64_t
   *  Special values: common::PLUS_INF_64 is default (also for double),
   * undefined for nulls only or empty set
   */
  int64_t RoughMax();

  /*! \brief A coefficient of rough check selectivity for simple equality
   * conditions. \return Approximation of k/n, where k - no. of suspected data
   * packs, n - no. of all data packs. E.g. 1 means KNs are useless, 1/n means
   * only one pack is to be checked (like for "a = 100" for autoincrement).
   */
  virtual double RoughSelectivity() { return 1; }
  /*! \brief Update the given TextStat to be able to encode string values from
   * the column. Invalidate s if not possible.
   */
  virtual void GetTextStat(types::TextStat &s) { s.Invalidate(); }
  /*!
   * \brief Get number of NULLs in the packrow pointed by \e mit
   * \pre \e mit is located at the beginning of current packrow
   * \return number of nulls in the pack or common::NULL_VALUE_64 if not
   * available Note that int may be not enough, as core::MIIterator packrow may
   * be arbitrarily large.
   */
  inline int64_t GetNumOfNulls(const core::MIIterator &mit) { return GetNumOfNullsImpl(mit, vc_nulls_possible_); }
  /*!
   * \brief returns true -> column contains only NULLs for sure
   * \brief returns false -> it is sure that column contains only NULLs
   * If returns true, rough level Min/Max methods should not be used for this
   * column
   */
  inline bool IsRoughNullsOnly() const { return IsRoughNullsOnlyImpl(); }
  /*!
   * \brief Return true if it is possible that the column will return null
   * value, including nulls generated by multiindex null objects
   */
  inline bool IsNullsPossible() { return IsNullsPossibleImpl(vc_nulls_possible_); }
  /*! \brief Get the sum of values in the packrow pointed by \e mit
   * \pre \e mit is located at the beginning of current packrow
   * \return sum or common::NULL_VALUE_64 if sum not available (e.g. if the
   * packrow is not full), set nonnegative as true if the sum is based on
   * nonnegative values only (=> the sum is an upper approx. of a sum of a
   * subset)
   */
  inline int64_t GetSum(const core::MIIterator &mit, bool &nonnegative) { return GetSumImpl(mit, nonnegative); }
  /*! \brief Get an upper approximation of the sum of the whole packrow pointed
   * by \e mit (even if the iterator is not full for the pack) \pre \e mit is
   * located at the beginning of current packrow \return sum or
   * common::NULL_VALUE_64 if sum not available, set nonnegative as true if the
   * sum is based on nonnegative values only (=> the sum is an upper approx. of
   * a sum of a subset)
   */
  inline int64_t GetApproxSum(const core::MIIterator &mit, bool &nonnegative) {
    return GetApproxSumImpl(mit, nonnegative);
  }
  /*! \brief Return true if the virtual column contain only non-null distinct
   * values Depends on the current multiindex state, e.g. if there is no row
   * repetitions.
   */
  inline bool IsDistinct() { return IsDistinctImpl(); }
  /*! \brief Return true if the virtual column is based on single physical
   * column and contain distinct values within its table
   */
  virtual bool IsDistinctInTable() { return IsDistinct(); }
  /*! \brief Return the best possible upper approximation of distinct values,
   * incl. nulls (or not) Depends on the current multiindex state, e.g. KNs, if
   * there are nulls etc.
   */
  int64_t GetApproxDistVals(bool incl_nulls, core::RoughMultiIndex *rough_mind = nullptr);

  /*! \brief Return the exact number of distinct values of the whole column,
   * without nulls. If the exact number is unsure, return common::NULL_VALUE_64.
   */
  virtual int64_t GetExactDistVals() { return common::NULL_VALUE_64; }
  /*! \brief Return the upper approximation of text size the column may return.
   *  May depend on the current multiindex state, e.g. KNs.
   *  Used e.g. to prepare buffers for text values.
   *  maximal byte string length in column
   */
  inline size_t MaxStringSize() { return MaxStringSizeImpl(); }
  virtual types::BString DecodeValue_S([[maybe_unused]] int64_t code) {
    DEBUG_ASSERT(0);
    return types::BString();
  }  // lookup (physical) only
  virtual int EncodeValue_S([[maybe_unused]] types::BString &v) {
    DEBUG_ASSERT(0);
    return -1;
  }                                           // lookup (physical) only
  int64_t DecodeValueAsDouble(int64_t code);  // convert code to a double value
                                              // and return as binary-encoded
  virtual std::vector<int64_t> GetListOfDistinctValues([[maybe_unused]] core::MIIterator const &mit) {
    std::vector<int64_t> empty;
    return empty;
  }

  ////////////////////////////////////////////////////////////

  /*! Get list of variables which must be supplied as arguments the obtain a
   * value from this VirtualColumn It is a list of variables from underlying
   * core::MysqlExpression, or a the PhysicalColumn if VirtualColumn wraps a
   * PhysicalColumn
   *
   */
  virtual std::vector<VarMap> &GetVarMap() { return var_map_; }
  /*! Get list of parameters which values must be set before requesting any
   * value from this VirtualColumn It is a list of variables from underlying
   * core::MysqlExpression, which do not belong to tables from teh current query
   *
   */
  virtual const core::MysqlExpression::SetOfVars &GetParams() const { return params_; }
  //! \brief parameters were set, so reevaluation is necessary
  //! \brief for TIANMUConstExpressions, get arguments according to mit and evaluate
  //! the evaluation in IBConstExpression i done only if requested by
  //! core::TempTable identified by tta
  virtual void RequestEval([[maybe_unused]] const core::MIIterator &mit, [[maybe_unused]] const int tta) {
    first_eval_ = true;
  }
  //! \return true, if \e d2 is no longer needed
  virtual bool TryToMerge([[maybe_unused]] core::Descriptor &d1, [[maybe_unused]] core::Descriptor &d2) {
    return false;
  }  // default behaviour, should be overriden
  //! \brief Assign a value to a parameter
  //! It is up the to column user to set values of all the parameters before
  //! requesting any value from the column
  virtual void SetParamTypes([[maybe_unused]] core::MysqlExpression::TypOfVars *types) {}

  /*! Mark as true each dimension used as source for variables in this
   * VirtualColumn
   *
   * \param dims_usage caller creates this vector with the proper size (number
   * of dimensions in the multiindex)
   */
  virtual void MarkUsedDims(core::DimensionVector &dims_usage);

  /*! Check the condition \e desc on a packrow from \e multi_index pointed by \e mit
   *
   * Does not perform a rough check - does row by row comparison
   *
   * \pre Assumes that triviality of desc is checked beforehand.
   * Requires a new core::MultiIndex functionality:
   * Tell, that changes to the current packrow are finished
   * void core::MultiIndex::CommitPendingPackUpdate();
   * Apply all pending changes to the MutliIndex, so they become visible
   * void core::MultiIndex::CommitPendingUpdate();
   */
  void EvaluatePack(core::MIUpdatingIterator &mit, core::Descriptor &desc) { EvaluatePackImpl(mit, desc); }
  common::ErrorCode EvaluateOnIndex(core::MIUpdatingIterator &mit, core::Descriptor &desc, int64_t limit) {
    return EvaluateOnIndexImpl(mit, desc, limit);
  }
  //! check whether any value from the pack may meet the condition
  inline common::RoughSetValue RoughCheck(const core::MIIterator &it, core::Descriptor &d) {
    return RoughCheckImpl(it, d);
  }
  //! is a datapack pointed by \e mit NULLS_ONLY, UNIFORM, UNIFORM_AND_NULLS or
  //! NORMAL
  core::PackOntologicalStatus GetPackOntologicalStatus(const core::MIIterator &mit) {
    return GetPackOntologicalStatusImpl(mit);
  }
  /*! \brief Lock datapacks providing parameters for contained expression; must
   * be done before any data access. \param mit Multiindex iterator pointing to
   * a packrow to be locked Lock strategy is controlled by an internal
   * PackGuardian. Default strategy: unlocking the pack when the next one is
   * locked. \code // This is an example of use LockSourcePacks to list all
   * values of VirtualColumn: core::MIIterator it(current_multiindex);
   *       while(it.IsValid()) {
   *           if(it.PackrowStarted()) {
   *               virt_column.LockSourcePack(it);
   *           }
   *           cout << virt_column.GetValueInt64(it) << endl;
   *           ++it;
   *       }
   *       // may be unlocked manually, or just left until destructor does it
   */
  virtual void LockSourcePacks([[maybe_unused]] const core::MIIterator &mit) {}
  //! \brief Manually unlock all locked datapacks providing parameters for
  //! contained expression. Done also in destructor.
  virtual void UnlockSourcePacks() {}
  virtual void DisplayAttrStats() {}
  /*! \brief Returns true if access to column values does not depend on
   * MultiIndexIterator, false otherwise \return bool
   */
  virtual bool IsConst() const = 0;

  /*! \brief Returns not zero if the virtual column is an interface to a single
   * physical column, otherwise single_col_t::SC_ATTR or single_col_t::SC_RCATTR \return single_col_t
   */
  virtual single_col_t IsSingleColumn() const { return single_col_t::SC_NOT; }
  /*! \brief Returns true if the virtual column is a set (multival), false
   * otherwise \return bool
   */
  virtual bool IsMultival() const { return false; }
  /*! \brief Returns true if the virtual column is a subselect column, false
   * otherwise \return bool
   */
  virtual bool IsSubSelect() const { return false; }
  /*! \brief Returns true if the virtual column is a TypeCast column, false
   * otherwise \return bool
   */
  virtual bool IsTypeCastColumn() const { return false; }
  /*! \brief Returns true if the virtual column is an InSetColumn, false
   * otherwise \return bool
   */
  virtual bool IsInSet() const { return false; }
  /*! \brief Returns true if column values are deterministically set, false
   * otherwise \return bool note - not properly implemented in
   * MultivalueColumns.
   */
  virtual bool IsDeterministic() { return true; }
  /*! \brief Returns true if column has parameters that have to be set before
   * accessing its values, false otherwise \return bool
   */
  virtual bool IsParameterized() const { return params_.size() != 0; }
  /*! Is there a procedure to copy the column, so the copies can be used in
   * parallel
   *
   */
  virtual bool CanCopy() const { return false; }
  /*! Is there a procedure to copy the column, so the copies can be used in
   * parallel
   *
   */
  virtual VirtualColumn *Copy() {
    DEBUG_ASSERT(0 && "not implemented");
    return nullptr;
  }

  virtual bool IsThreadSafe() { return false; }
  /*! \brief Returns all dimensions (tables) involved by virtual column
   *
   */
  virtual std::set<int> GetDimensions();

  /*! \brief Returns the only dimension (table) involved by virtual column, or
   * -1
   *
   */
  virtual int GetDim() { return dim_; }
  core::Transaction *ConnInfo() { return conn_info_; }
  core::MultiIndex *GetMultiIndex() { return multi_index_; }
  /*! change the core::MultiIndex for which the column has been created, used in
   * subquery execution \param m - new core::MultiIndex, should be a copy of the
   * previously assigned one \param t - this copy of original
   * (compilation-produced) core::TempTable must be used by the column to take
   * values from
   */
  void SetMultiIndex(core::MultiIndex *m, std::shared_ptr<core::JustATable> t = std::shared_ptr<core::JustATable>());

  virtual int64_t NumOfTuples();

  //! Is it a CONST column without parameters? (i.e. fully defined by a value)
  bool IsFullConst() { return IsConst() && !IsParameterized(); }
  static bool IsConstExpression(core::MysqlExpression *expr, int temp_table_alias, const std::vector<int> *aliases);

  virtual const core::MysqlExpression::tianmu_fields_cache_t &GetTIANMUItems() const = 0;

  virtual char *ToString(char p_buf[], [[maybe_unused]] size_t buf_ct) const { return p_buf; }
  /*! Must be called in constructor, and every time the VirtualColumn looses its
   * history
   */
  void ResetLocalStatistics();

  /*! \brief Set local statistics for a virtual column. Use
   * common::NULL_VALUE_64 or common::MINUS_INF_64/common::PLUS_INF_64 to leave
   * min or max unchanged. Note that this function may only make the min/max
   * more narrow, i.e. settings wider than current min/max will be ignored. Use
   * ResetLocalStatistics() first to make the interval wider.
   */
  void SetLocalMinMax(int64_t loc_min, int64_t loc_max);

  /*! \brief Set local null information for a virtual column. Override the old
   * value.
   */
  void SetLocalNullsPossible(bool loc_nulls_possible) { vc_nulls_possible_ = loc_nulls_possible; }
  /*! \brief Set max. number of not null distinct values.
   *   Decreases the old value. To set it unconditionally, set
   * common::NULL_VALUE_64 first.
   */
  void SetLocalDistVals(int64_t loc_dist_vals) {
    if (vc_dist_vals_ == common::NULL_VALUE_64 || vc_dist_vals_ > loc_dist_vals)
      vc_dist_vals_ = loc_dist_vals;
  }
  void SetLocalNullsOnly(bool loc_nulls_only) { nulls_only_ = loc_nulls_only; }
  bool GetLocalNullsOnly() { return nulls_only_; }
  virtual std::vector<VirtualColumn *> GetChildren() const { return std::vector<VirtualColumn *>(); }

 protected:
  virtual int64_t GetValueInt64Impl(const core::MIIterator &) = 0;
  virtual bool IsNullImpl(const core::MIIterator &) = 0;
  virtual void GetValueStringImpl(types::BString &, const core::MIIterator &) = 0;
  virtual double GetValueDoubleImpl(const core::MIIterator &) = 0;
  virtual types::TianmuValueObject GetValueImpl(const core::MIIterator &, bool) = 0;

  virtual int64_t GetMinInt64Impl(const core::MIIterator &) = 0;
  virtual int64_t GetMinInt64ExactImpl(const core::MIIterator &) { return common::NULL_VALUE_64; }
  virtual int64_t GetMaxInt64ExactImpl(const core::MIIterator &) { return common::NULL_VALUE_64; }
  virtual int64_t GetMaxInt64Impl(const core::MIIterator &) = 0;

  virtual types::BString GetMinStringImpl(const core::MIIterator &) = 0;
  virtual types::BString GetMaxStringImpl(const core::MIIterator &) = 0;

  virtual int64_t RoughMinImpl() = 0;
  virtual int64_t RoughMaxImpl() = 0;
  virtual int64_t GetNumOfNullsImpl(const core::MIIterator &, bool val_nulls_possible) = 0;

  virtual bool IsRoughNullsOnlyImpl() const = 0;
  virtual bool IsNullsPossibleImpl(bool val_nulls_possible) = 0;
  virtual int64_t GetSumImpl(const core::MIIterator &, bool &nonnegative) = 0;
  virtual int64_t GetApproxSumImpl(const core::MIIterator &mit, bool &nonnegative);
  virtual bool IsDistinctImpl() = 0;
  virtual int64_t GetApproxDistValsImpl(bool incl_nulls, core::RoughMultiIndex *rough_mind) = 0;
  virtual size_t MaxStringSizeImpl() = 0;  // maximal byte string length in column

  virtual core::PackOntologicalStatus GetPackOntologicalStatusImpl(const core::MIIterator &) = 0;
  virtual common::RoughSetValue RoughCheckImpl(const core::MIIterator &, core::Descriptor &);
  virtual void EvaluatePackImpl(core::MIUpdatingIterator &mit, core::Descriptor &) = 0;
  virtual common::ErrorCode EvaluateOnIndexImpl(core::MIUpdatingIterator &mit, core::Descriptor &, int64_t limit) = 0;

 protected:
  core::MultiIndex *multi_index_;
  core::Transaction *conn_info_;
  std::vector<VarMap> var_map_;
  core::MysqlExpression::SetOfVars params_;  // parameters - columns from tables not
                                             // present in the local multiindex

  mutable std::shared_ptr<core::ValueOrNull> last_val_;
  //! for caching, the first time evaluation is obligatory e.g. because a
  //! parameter has been changed

  mutable bool first_eval_;

  int dim_;  // valid only for SingleColumn and for ExpressionColumn if based on
             // a single table only
  // then this an easily accessible copy of var_map_[0]._dim. Otherwise it should
  // be -1

  //////////// Local statistics (set by history) ///////////////
  int64_t vc_min_val_;  // int64_t for ints/dec, *(double*)& for floats
  int64_t vc_max_val_;
  bool vc_nulls_possible_;  // false => no nulls, except of outer join ones (to
                            // be checked in multiindex)
  int64_t vc_dist_vals_;    // upper approximation of non-null distinct vals, or
                            // common::NULL_VALUE_64 for no approximation
  bool nulls_only_;         // only nulls are present
};
}  // namespace vcolumn
}  // namespace Tianmu

#endif  // TIANMU_VC_VIRTUAL_COLUMN_BASE_H_
