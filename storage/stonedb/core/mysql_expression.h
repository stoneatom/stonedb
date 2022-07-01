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
#ifndef STONEDB_CORE_MYSQL_EXPRESSION_H_
#define STONEDB_CORE_MYSQL_EXPRESSION_H_
#pragma once

#include <map>
#include <set>

#include "common/common_definitions.h"
#include "core/item_sdbfield.h"
#include "core/value_or_null.h"
#include "core/var_id.h"

namespace stonedb {
namespace core {
class Item_sdbfield;

// Wrapper for MySQL expression tree
class MysqlExpression {
 public:
  // map which tells how to replace Item_field's with Item_sdbfield
  using Item2VarID = std::map<Item *, VarID>;
  using TypeOfVars = std::map<VarID, DataType>;  // map of types of variables
  using sdbfields_cache_t = std::map<VarID, std::set<Item_sdbfield *>>;
  using value_or_null_info_t = std::pair<ValueOrNull, ValueOrNull *>;
  using var_buf_t = std::map<VarID, std::vector<value_or_null_info_t>>;
  using SetOfVars = std::set<VarID>;  // set of IDs of variables occuring in
  enum class StringType { STRING_NOTSTRING, STRING_TIME, STRING_NORMAL };

  MysqlExpression(Item *item, Item2VarID &item2varid);
  MysqlExpression(const MysqlExpression &) = delete;
  MysqlExpression &operator=(const MysqlExpression &) = delete;
  virtual ~MysqlExpression();

  static bool SanityAggregationCheck(Item *item, std::set<Item *> &aggregations, bool toplevel = true,
                                     bool *has_aggregation = nullptr);
  static bool HasAggregation(Item *item);

  virtual MysqlExpression::SetOfVars &GetVars();
  void SetBufsOrParams(var_buf_t *bufs);
  virtual DataType EvalType(TypeOfVars *tv = nullptr);
  StringType GetStringType();
  virtual std::shared_ptr<ValueOrNull> Evaluate();

  void RemoveUnusedVarID() { RemoveUnusedVarID(item); }
  Item *GetItem() { return item; }
  const sdbfields_cache_t &GetSDBItems() { return sdbfields_cache; }
  bool IsDeterministic() { return deterministic; }
  /*! \brief Tests if other MysqlExpression is same as this one.
   *
   * \param other - equality to this MysqlExpression is being questioned.
   * \return True iff this MysqlExpression and other MysqlExpression are
   * equivalent.
   */
  bool operator==(MysqlExpression const &other) const;

  static std::shared_ptr<ValueOrNull> ItemInt2ValueOrNull(Item *item);
  static std::shared_ptr<ValueOrNull> ItemReal2ValueOrNull(Item *item);
  static std::shared_ptr<ValueOrNull> ItemDecimal2ValueOrNull(Item *item, int dec_scale = -1);
  static std::shared_ptr<ValueOrNull> ItemString2ValueOrNull(Item *item, int max_str_len = -1,
                                                             common::CT a_type = common::CT::STRING);

  static int64_t &AsInt(int64_t &x) { return x; }
  static double &AsReal(int64_t &x) { return *(double *)&x; }
  static int64_t &AsValue(int64_t &x) { return x; }
  static int64_t &AsValue(double &x) { return *(int64_t *)&x; }
  static const int64_t &AsInt(const int64_t &x) { return x; }
  static const double &AsReal(const int64_t &x) { return *(double *)&x; }
  static const int64_t &AsValue(const int64_t &x) { return x; }
  static const int64_t &AsValue(const double &x) { return *(int64_t *)&x; }

 private:
  DataType type;  // type of result of the expression

  static bool HandledResultType(Item *item);
  static bool HandledFieldType(Item_result type);
  Item_sdbfield *GetSdbfieldItem(Item_field *);

  enum class TransformDirection { FORWARD, BACKWARD };

  //! Returns the root of transformed tree. New root may be different than the
  //! old one!
  Item *TransformTree(Item *root, TransformDirection dir);

  //! Checks whether an operation on MySQL decimal returned an error.
  //! If so, throws Exception.
  static void CheckDecimalError(int err);
  void RemoveUnusedVarID(Item *root);

  Item *item;
  Item_result mysql_type;
  uint decimal_precision, decimal_scale;
  Item2VarID *item2varid;
  sdbfields_cache_t sdbfields_cache;
  SetOfVars vars;  // variable IDs in the expression, same as in sdbfields_cache;
                   // filled in TransformTree
  bool deterministic;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_MYSQL_EXPRESSION_H_
