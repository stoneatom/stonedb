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
#ifndef STONEDB_CORE_PHYSICAL_COLUMN_H_
#define STONEDB_CORE_PHYSICAL_COLUMN_H_
#pragma once

#include "core/column.h"
#include "core/descriptor.h"
#include "core/mysql_expression.h"
#include "types/text_stat.h"

namespace stonedb {
namespace core {

//! A column in a table. Values contained in it exist physically on disk and/or
//! in memory and are divided into datapacks
class PhysicalColumn : public Column {
 public:
  PhysicalColumn() : is_unique(false), is_unique_updated(false) {}
  PhysicalColumn(const PhysicalColumn &phc)
      : Column(phc), is_unique(phc.is_unique), is_unique_updated(phc.is_unique_updated) {}

  enum class phys_col_t { ATTR, RCATTR };

  /*! \brief Get a numeric value from a column
   *
   * \pre necessary datapacks are loaded and locked. TODO: Currently dps may be
   * not loaded
   *
   * Use it for numeric columns only.
   * Old name: \b GetTable64
   *
   * \param row identifies a row
   * \return numeric value from the column in level-1 encoding.
   * - Integers values can be used directly
   * - Decimals must be externally properly shifted
   * - Doubles must be cast e.g. *(double*)&
   */
  virtual int64_t GetValueInt64(int64_t row) const = 0;
  virtual int64_t GetNotNullValueInt64(int64_t row) const = 0;

  /*! \brief Is the column value NULL ?
   *
   * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and
   * locked
   *
   * \param row identifies a row
   * \return \b true if column value is NULL, \b false otherwise
   */
  virtual bool IsNull(int64_t row) const = 0;

  /*! \brief Get a numeric value from a column
   *
   * \pre necessary datapacks are loaded and locked
   *
   * \param row identifies a row
   * \return types::RCNum object - a representation of a numeric value
   */
  //	virtual const RCNum& GetValueNum(const uint64_t row) const
  //               {ASSERT(false, "not overridden"); static
  //               types::RCNum dummy(0.0); return dummy; };

  /*! \brief Get a date/time value from a column
   *
   * \pre necessary datapacks are loaded and locked
   *
   * \param row identifies a row
   * \return types::RCDateTime object - a representation of a date/time
   * value
   */
  //	virtual const RCDateTime& GetValueDateTime(const uint64_t row) const
  //               {ASSERT(false, "not overridden"); static
  //               types::RCDateTime dummy(0); return dummy;
  //               };

  /*! \brief Get a binary value from a column
   *
   * \pre necessary datapacks are loaded and locked
   *
   * \param row identifies a row
   * \param size the size of buffer, set to the number of copied bytes on return
   * \param buf buffer into which the bytes are copied
   */
  virtual void GetValueString(int64_t row, types::BString &s) = 0;
  virtual void GetNotNullValueString(int64_t row, types::BString &s) = 0;
  virtual types::RCValueObject GetValue(int64_t obj, bool lookup_to_num = false) = 0;

  // ToDO uncomment and solve cyclic includes
  //	virtual MysqlExpression::ValueOrNull GetComplexValue(const uint64_t
  // obj);

  /*! \brief Get a text value from a column
   *
   * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and
   * locked
   *
   * \param row is a row number in the column
   * \param size the size of buffer, set to the number of copied bytes on return
   * \param buf buffer into which the bytes are copied
   * \return number of characters put in the buffer
   */
  //	virtual int	GetValueText(uint64_t obj, char *val_buf) const = 0;

  /*! \brief Get rough minimum in level-1 encoding
   *
   * Old name: GetMin64
   * \param pack identifies a datapck, for which min value is requested
   * \return numeric value from the column in level-1 encoding.
   * - Integers values can be used directly
   * - Decimals must be externally properly shifted
   * - Doubles must be cast e.g. *(double*)&
   */
  virtual int64_t GetMinInt64(int pack) = 0;

  /*! \brief Get rough maximum in level-1 encoding
   *
   * Old name: GetMax64
   * \param pack identifies a datapck, for which min value is requested
   * \return numeric value from the column in level-1 encoding.
   * - Integers values can be used directly
   * - Decimals must be externally properly shifted
   * - Doubles must be cast e.g. *(double*)&
   */
  virtual int64_t GetMaxInt64(int pack) = 0;

  virtual types::BString GetMaxString(int pack) = 0;
  virtual types::BString GetMinString(int pack) = 0;
  //	virtual types::RCDateTime GetMaxDateTime(int pack) const
  //{ASSERT(false, "not overridden"); return
  // 0; };
  //	virtual types::RCDateTime GetMinDateTime(int pack) const
  //{ASSERT(false, "not overridden"); return
  // 0; };
  //	virtual types::RCNum GetMaxNum(int pack) const {ASSERT(false,
  //"not overridden"); return 0.0; }; 	virtual types::RCNum
  // GetMinNum(int pack) const {ASSERT(false, "not overridden"); return 0.0; };

  /*!
   * \brief Get number of NULLs in the datapack identified by \e pack
   * Special case: GetNoNulls(-1) gives a total number of nulls.
   * Value common::NULL_VALUE_64 means 'cannot determine'.
   */
  virtual int64_t GetNumOfNulls(int pack) = 0;

  /*!
   * \brief return true if sure that the column contains nulls only
   */
  virtual bool IsRoughNullsOnly() const = 0;

  //! \brief Get the sum of values in the datapack identified by \e pack
  virtual int64_t GetSum(int pack, bool &nonnegative) = 0;

  virtual std::vector<int64_t> GetListOfDistinctValuesInPack([[maybe_unused]] int pack) {
    std::vector<int64_t> empty;
    return empty;
  }

  //! provide the most probable approximation of number of objects matching the
  //! condition
  virtual uint64_t ApproxAnswerSize([[maybe_unused]] Descriptor &d) {
    ASSERT(false, "not overridden");
    return 0;
  }

  virtual uint64_t ApproxDistinctVals(bool incl_nulls, Filter *f, common::RSValue *rf,
                                      bool outer_nulls_possible) = 0;  // provide the best upper
                                                                       // approximation of number
                                                                       // of diff. values (incl.
                                                                       // null)

  virtual uint64_t ExactDistinctVals(Filter *f) = 0;  // provide the exact number of diff.
                                                      // non-null values, if possible, or
                                                      // common::NULL_VALUE_64

  virtual size_t MaxStringSize(Filter *f = NULL) = 0;  // maximal byte string length in column

  // Are all the values unique?
  bool IsUnique() const { return is_unique; }
  /*! \brief Sets the uniqueness status
   *
   * The status is changed in memory only
   * \param unique true if column has been verified to contain unique values
   */
  void SetUnique(bool unique) { is_unique = unique; }
  // Is Unique status updated (valid)?
  bool IsUniqueUpdated() const { return is_unique_updated; }
  void SetUniqueUpdated(bool updated) { is_unique_updated = updated; }
  //! shortcut utility function = IsUniqueUpdated && IsUnique
  common::RSValue IsDistinct() const {
    return (IsUniqueUpdated() ? (IsUnique() ? common::RSValue::RS_ALL : common::RSValue::RS_NONE)
                              : common::RSValue::RS_UNKNOWN);
  }
  virtual int64_t RoughMin(Filter *f = NULL,
                           common::RSValue *rf = NULL) = 0;  // for numerical: best
                                                             // rough approximation of
                                                             // min for a given filter
                                                             // (or global min if filter
                                                             // is NULL)
  virtual int64_t RoughMax(Filter *f = NULL,
                           common::RSValue *rf = NULL) = 0;  // for numerical: best
                                                             // rough approximation of
                                                             // max for a given filter
                                                             // (or global max if filter
                                                             // is NULL)
  virtual void GetTextStat(types::TextStat &s, [[maybe_unused]] Filter *f = NULL) { s.Invalidate(); }
  virtual double RoughSelectivity() { return 1; }
  /*! \brief Return true if the column (filtered) contain only non-null distinct
   * values
   */
  virtual bool IsDistinct(Filter *f) = 0;

  //! \brief Is the pack NULLS_ONLY, UNIFORM, NORMAL etc...
  virtual PackOntologicalStatus GetPackOntologicalStatus(int pack_no) = 0;

  /*! Check whether the value identified by \e row meets the condition \e d and
   * store the result in the filter \param  row row number \param f affected
   * Filter, associated with \e it \param d condition to be checked
   *
   * \pre requires a new Filter functionality:
   * Tell, that changes to the current packrow are finished
   * void Filter::SetDelayed();
   * void Filter::CommitPack();
   * Apply all pending changes to the Filter, so they become visible
   * void Filter::Commit();
   */
  // virtual void UpdateFilterDelayed(uint64_t row, Filter& f, Descriptor& d) =
  // 0;

  /*! Check whether the value identified by \e row meets the condition \e d and
   * store the result in the filter \param row row number \param f affected
   * Filter, associated with \e it \param d condition to be checked
   */
  // virtual void UpdateFilter(uint64_t row, Filter& f, Descriptor& d) = 0;

  /*! Check the condition \e desc on a packrow identified iterator by \e mit and
   * update it respectively \param mit - updating iterator representing a pack
   * \param dim - dimension in corresponding multiindex
   * \param desc - condition to be checked
   */
  virtual void EvaluatePack(MIUpdatingIterator &mit, int dim, Descriptor &desc) = 0;
  virtual common::ErrorCode EvaluateOnIndex(MIUpdatingIterator &mit, int dim, Descriptor &desc, int64_t limit) = 0;

  /*! Check the condition \e d on a datarow from \e mind identified by \e pack
   * \param row row number
   * \param d condition to be checked
   */
  // virtual bool CheckCondition(uint64_t row, Descriptor& d) = 0;

  //! check whether any value from the pack may meet the condition
  virtual common::RSValue RoughCheck(int pack, Descriptor &d, bool additional_nulls_possible) = 0;

  //! check whether any pair from two packs of two different attr/tables may
  //! meet the condition
  virtual common::RSValue RoughCheck(int pack1, int pack2, Descriptor &d) = 0;
  virtual common::RSValue RoughCheckBetween([[maybe_unused]] int pack, [[maybe_unused]] int64_t min,
                                            [[maybe_unused]] int64_t max) {
    return common::RSValue::RS_SOME;
  }
  virtual bool TryToMerge(Descriptor &d1, Descriptor &d2) = 0;

  virtual void DisplayAttrStats(Filter *f) = 0;
  virtual int AttrNo() const = 0;

  virtual ~PhysicalColumn() = default;

  /*! \brief For lookup (RCAttr) columns only: decode a lookup value
   * \return Text value encoded by a given code
   */
  virtual types::BString DecodeValue_S(int64_t code) = 0;
  virtual int EncodeValue_S(types::BString &v) = 0;
  virtual enum phys_col_t ColType() const = 0;

 private:
  bool is_unique;
  bool is_unique_updated;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_PHYSICAL_COLUMN_H_
