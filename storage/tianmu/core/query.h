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
#ifndef TIANMU_CORE_QUERY_H_
#define TIANMU_CORE_QUERY_H_
#pragma once

#include "core/column_type.h"
#include "core/item_tianmu_field.h"
#include "core/joiner.h"
#include "core/mysql_expression.h"
#include "handler/ha_my_tianmu.h"

namespace Tianmu {
namespace core {

using Tianmu::DBHandler::QueryRouteTo;

class CompiledQuery;
class MysqlExpression;
class ResultSender;
class JustATable;
class TianmuTable;
class Transaction;

enum class TableStatus {
  TABLE_SEEN_INVOLVED = 0,
  TABLE_YET_UNSEEN_INVOLVED = 1,
  TABLE_UNKONWN_ERROR = -1,
};

class Query final {
 public:
  enum class WrapStatus { SUCCESS, FAILURE, OPTIMIZE };

 public:
  Query() = delete;
  Query(Transaction *conn_info) : m_conn(conn_info) {}
  ~Query();

  void AddTable(std::shared_ptr<TianmuTable> tab) { t.push_back(tab); }
  /*! \brief Remove the given TianmuTable from the list of tables used in a query
   */
  void RemoveFromManagedList(const std::shared_ptr<TianmuTable> tab);

  int NumOfTabs() const { return t.size(); };
  std::shared_ptr<TianmuTable> Table(size_t table_num) const {
    ASSERT(table_num < t.size(), "table_num out of range: " + std::to_string(table_num));
    return t[table_num];
  }

  void LockPackInfoForUse();
  void UnlockPackInfoFromUse();

  void SetRoughQuery(bool set_rough) { rough_query = set_rough; }
  bool IsRoughQuery() { return rough_query; }
  QueryRouteTo Compile(CompiledQuery *compiled_query, SELECT_LEX *selects_list, SELECT_LEX *last_distinct,
                       TabID *res_tab = nullptr, bool ignore_limit = false, Item *left_expr_for_subselect = nullptr,
                       common::Operator *oper_for_subselect = nullptr, bool ignore_minmax = false,
                       bool for_subq_in_where = false);
  TempTable *Preexecute(CompiledQuery &qu, ResultSender *sender, bool display_now = true);
  QueryRouteTo BuildConditions(Item *conds, CondID &cond_id, CompiledQuery *cq, const TabID &tmp_table,
                               CondType filter_type, bool is_zero_result = false,
                               JoinType join_type = JoinType::JO_INNER, bool can_cond_push = false);

  std::multimap<std::string, std::pair<int, TABLE *>> table_alias2index_ptr;

 private:
  CompiledQuery *cq = nullptr;
  std::vector<std::pair<TabID, bool>> subqueries_in_where;
  using TabIDColAlias = std::pair<int, std::string>;
  std::map<TabIDColAlias, int> field_alias2num;
  std::map<std::string, unsigned> path2num;

  // all expression based virtual columns for a given table
  std::multimap<TabID, std::pair<int, MysqlExpression *>> tab_id2expression;
  std::multimap<TabID, std::pair<int, std::pair<std::vector<int>, AttrID>>> tab_id2inset;

  std::vector<MysqlExpression *> gc_expressions;
  std::multimap<std::pair<int, int>, std::pair<int, int>> phys2virt;
  std::multimap<TabID, std::pair<int, TabID>> tab_id2subselect;
  std::map<Item_tianmufield *, int> tianmuitems_cur_var_ids;

  // table aliases - sometimes point to TempTables (maybe to the same one), sometimes to TIanmuTables
  std::vector<std::shared_ptr<JustATable>> ta;

  std::vector<std::shared_ptr<TianmuTable>> t;

  bool rough_query = false;  // set as true to enable rough execution

  bool FieldUnmysterify(Item *item, TabID &tab, AttrID &col);
  QueryRouteTo FieldUnmysterify(Item *item, const char *&database_name, const char *&table_name,
                                const char *&table_alias, const char *&table_path, const TABLE *&table_ptr,
                                const char *&field_name, const char *&field_alias);

  const char *GetTableName(Item_field *ifield);
  TableStatus PrefixCheck(Item *conds);

  /*! \brief Checks if exists virtual column defined by physical column:
   * \param tmp_table - id of TempTable for which VC is searched for
   * \param tab - table id
   * \param col - column id
   * \return table id and number of virtual column if it exists or
   * common::NULL_VALUE_32 otherwise
   */
  std::pair<int, int> VirtualColumnAlreadyExists(const TabID &tmp_table, const TabID &tab, const AttrID &col);

  /*! \brief Checks if exists virtual column defined by expression:
   * \param tmp_table - id of table for which column is supposed to be created
   * \param expression
   * \return number of virtual column if it exists or common::NULL_VALUE_32
   * otherwise
   */
  int VirtualColumnAlreadyExists(const TabID &tmp_table, MysqlExpression *expression);

  /*! \brief Checks if exists virtual column defined by subquery
   * \param tmp_table - id of table for which column is supposed to be created
   * \param subquery - id of subquery
   * \return number of virtual column if it exists or common::NULL_VALUE_32
   * otherwise
   */
  int VirtualColumnAlreadyExists(const TabID &tmp_table, const TabID &subquery);

  int VirtualColumnAlreadyExists(const TabID &tmp_table, const std::vector<int> &vcs, const AttrID &at);

  QueryRouteTo Item2CQTerm(Item *an_arg, CQTerm &term, const TabID &tmp_table, CondType filter_type,
                           bool negative = false, Item *left_expr_for_subselect = nullptr,
                           common::Operator *oper_for_subselect = nullptr, const TabID &base_table = TabID());

  // int FilterNotSubselect(Item *conds, const TabID& tmp_table, FilterType
  // filter_type, FilterID *and_me_filter = 0);

  /*! \brief Create filter from field or function that has no condition attached.
   * \param conds - condition (a field or function).
   * \param tmp_table - required for Item2CQTerm.
   * \param filter_type - type of filter context (WHERE, HAVING).
   * \param and_me_filter - ?
   * \return filter number (non-negative) or error indication (negative)
   */
  CondID ConditionNumberFromNaked(Item *conds, const TabID &tmp_table, CondType filter_type, CondID *and_me_filter,
                                  bool is_or_subtree = false, bool can_cond_push = false);
  CondID ConditionNumberFromMultipleEquality(Item_equal *conds, const TabID &tmp_table, CondType filter_type,
                                             CondID *and_me_filter = 0, bool is_or_subtree = false,
                                             bool can_cond_push = false);
  CondID ConditionNumberFromComparison(Item *conds, const TabID &tmp_table, CondType filter_type,
                                       CondID *and_me_filter = 0, bool is_or_subtree = false, bool negative = false,
                                       bool can_cond_push = false);

  /*! \brief Checks type of operator involved in condition
   * \param conds - conditions
   * \param op - output param.: type of operator
   * \param negative - input information about negation of condition
   * \param like_esc - output param.: escape character for LIKE operator
   * \return void
   */
  void ExtractOperatorType(Item *&conds, common::Operator &op, bool &negative, char &like_esc);

  /*! \brief Adds ANY modifier to an operator
   * \param op - operator
   * \return void
   */
  static void MarkWithAny(common::Operator &op);

  /*! \brief Adds ALL modifier to an operator
   * \param op - operator
   * \return void
   */
  static void MarkWithAll(common::Operator &op);

  CondID ConditionNumber(Item *conds, const TabID &tmp_table, CondType filter_type, CondID *and_me_filter = 0,
                         bool is_or_subtree = false, bool can_cond_push = false);
  QueryRouteTo BuildCondsIfPossible(Item *conds, CondID &cond_id, const TabID &tmp_table, JoinType join_type);

 public:
  /*! \brief Removes ALL/ANY modifier from an operator
   * \param op - operator
   * \return void
   */
  static void UnmarkAllAny(common::Operator &op);

  /*! \brief Checks if operator is of all/any type
   * \param op - operator
   * \return true if yes
   */
  static bool IsAllAny(common::Operator &op);

  /*! \brief Checks if item has a aggregation as one of its arguments.
   * \param item
   * \return true if yes
   */
  static bool HasAggregation(Item *item);

  /*! \brief Checks if an aggregation represents COUNT(*)
   * \param item_sum - aggregation item
   * \return true if yes
   */
  static bool IsCountStar(Item *item_sum);

  /*! \brief Checks if an item is of type FieldItem
   * \param item
   * \return true if yes
   */
  static bool IsFieldItem(Item *item);

  bool IsDeterministic(Item *item);

  /*! \brief Checks if an item is of type Item_sum_func
   * \param item
   * \return true if yes
   */
  static bool IsAggregationItem(Item *item);

  /*! \brief Checks if an item represents a constant
   * \param item
   * \return true if yes
   */
  static bool IsConstItem(Item *item);

  /*! \brief Returns true if an item is aggregation over field item
   * \param item
   * \return true if yes
   */
  static bool IsAggregationOverFieldItem(Item *item);

  /*! \brief Checks if given join contains left or right outer component.
   * \param join - join to be checked
   * \return bool - true if contains left or right outer join
   */
  static bool IsLOJ(List<TABLE_LIST> *join);

  const std::string GetItemName(Item *item);

  static void GetPrecisionScale(Item *item, int &precision, int &scale, bool max_scale = true);

 private:
  /*! \brief Creates AddColumn step in compilation by creating, if does not
   * exist, Virtual Column based on PhysicalColumn \param item - what Item
   * \param tmp_table - for which TempTable
   * \param oper - defines type of added column
   * \param distinct - flag for AddColumn operation
   * \param group_by - indicates if it is column for group by query
   * \return column number
   */
  int AddColumnForPhysColumn(Item *item, const TabID &tmp_table, const TabID &base_table,
                             const common::ColOperation oper, const bool distinct, bool group_by,
                             const char *alias = nullptr);

  /*! \brief Creates AddColumn step in compilation by creating, if does not exist, Virtual Column based on expression
   * \param mysql_expression - pointer to expression
   * \param tmp_table - for which TempTable
   * \param alias - defines alias of added column
   * \param oper - defines type of added column
   * \param distinct - flag for AddColumn operation
   * \param group_by - indicates if it is column for group by query
   * \return column number
   */
  int AddColumnForMysqlExpression(MysqlExpression *mysql_expression, const TabID &tmp_table, const char *alias,
                                  const common::ColOperation oper, const bool distinct, bool group_by = false);

  /*! \brief Computes identifier of a column created by AddColumn operation
   * \param vc - for which virtual column
   * \param tmp_table -  for which TempTable
   * \param oper - type of column
   * \param distinct - modifier of AddColumn
   * \return column number if it exists or common::NULL_VALUE_32 otherwise
   */
  int GetAddColumnId(const AttrID &vc, const TabID &tmp_table, const common::ColOperation oper, const bool distinct);

  /*! \brief Changes type of AddColumn step in compilation from LISTING to GROUP_BY
   * \param tmp_table - for which TempTable
   * \param attr - for which column
   */
  void CQChangeAddColumnLIST2GROUP_BY(const TabID &tmp_table, int attr);

  /*! \brief Creates MysqlExpression object that wraps expression tree of MySQL not containing aggregations.
   * All Item_field items are substituted with Item_bhfield items
   * \param item - root of MySQL expression tree to be wrapped
   * \param tmp_table - alias of TempTable which is checked if it contains aggregation fields (should not)
   * \param expr - expression to store the result tree
   * \param in_aggregation - the expression is used in WHERE (true) or elsewhere (having, select list = false)
   * \return WrapStatus::SUCCESS on success, WrapStatus::OPTIMIZE if an aggregation was encountered, WrapStatus::FAILURE
   * if there was any problem with wrapping, e.g., not acceptable type of expression
   */
  WrapStatus WrapMysqlExpression(Item *item, const TabID &tmp_table, MysqlExpression *&expr, bool in_where,
                                 bool aggr_used);
  //
  //	/*! \brief Creates MysqlExpression object that wraps full expression tree of MySQL. All Item_field items are
  //	 * substituted with Item_bhfield items. In case of aggregation it is substituted with its whole subtree by a
  // single
  //	 * Item_bhfield, however, there is an AddColumn step added to compilation with this aggregation and Item_bhfield
  // refers to the
  //	 * column created by this step.
  //	 * \param item - root of MySQL expression tree to be wrapped
  //	 * \param tmp_table - alias of TempTable to which AddColumn is added in case of aggregation
  //	 * \param expr - expression to store the result tree
  //	 * \param is_const_or_aggr - optional pointer to bool variable that is set to true if expression has no
  // variables or
  //	 * it contains at least one aggregation (false otherwise)
  //	 * \return WrapStatus::SUCCESS on success, WrapStatus::FAILURE if there was any problem with wrapping,
  //	 *  e.g., not acceptable type of expression
  //	 */
  //	WrapStatus::wrap_status_t WrapMysqlExpressionWithAggregations(Item *item, const TabID& tmp_table,
  // MysqlExpression*& expr, bool* is_const_or_aggr = NULL);

  /*! \brief Generates AddColumn compilation steps for every field on SELECT list
   * \param fields - list of fields
   * \param tmp_table - alias of TempTable for which AddColumn steps are added
   * \param group_by_clause - indicates whether it is group by query
   * \param ignore_minmax - indicates if field of typy Min/Max should be transformed to LISTING
   * \return returns RETURN_QUERY_TO_MYSQL_ROUTE in case of any problem and RCBASE_QUERY_ROUTE otherwise
   */
  QueryRouteTo AddFields(List<Item> &fields, const TabID &tmp_table, TabID const &base_table,
                         const bool group_by_clause, int &num_of_added_fields, bool ignore_minmax, bool &aggr_used);

  QueryRouteTo AddSemiJoinFiled(List<Item> &fields, List<TABLE_LIST> &join, const TabID &tmp_table);

  /*! \brief Generates AddColumn compilation steps for every field on GROUP BY list
   * \param fields - pointer to GROUP BY fields
   * \param tmp_table - alias of TempTable for which AddColumn steps are added
   * \return returns RETURN_QUERY_TO_MYSQL_ROUTE in case of any problem and RCBASE_QUERY_ROUTE otherwise
   */
  QueryRouteTo AddGroupByFields(ORDER *group_by, const TabID &tmp_table, const TabID &base_table);

  //! is this item representing a column local to the temp table (not a parameter)
  bool IsLocalColumn(Item *item, const TabID &tmp_table);
  QueryRouteTo AddOrderByFields(ORDER *order_by, TabID const &tmp_table, TabID const &base_table,
                                int const group_by_clause);
  QueryRouteTo AddGlobalOrderByFields(SQL_I_List<ORDER> *global_order, const TabID &tmp_table, int max_col);

  /*! \brief AddJoins for every field on SELECT join list
   * \param join - list of joins
   * \param use_tmp_when_no_join - When join_list has no elements and field has sp, tmp table is used and de-duplicated
   * \return returns RETURN_QUERY_TO_MYSQL_ROUTE in case of any problem and RCBASE_QUERY_ROUTE otherwise
   */
  QueryRouteTo AddJoins(List<TABLE_LIST> &join, TabID &tmp_table, std::vector<TabID> &left_tables,
                        std::vector<TabID> &right_tables, bool in_subquery, bool &first_table, bool for_subq = false,
                        bool use_tmp_when_no_join = false);
  static bool ClearSubselectTransformation(common::Operator &oper_for_subselect, Item *&field_for_subselect,
                                           Item *&conds, Item *&having, Item *&cond_removed,
                                           List<Item> *&list_to_reinsert, Item *left_expr_for_subselect);

  /*!
   * \brief Are the variables constant (i.e. they are parameters) in the context of the query represented by TempTable t
   */
  bool IsConstExpr(MysqlExpression::SetOfVars &vars, const TabID &t);

  Transaction *m_conn;

  // void EvalCQTerm(bool cplx, CQTerm& term, MysqlExpression::BufOfVars bufs, CQTerm& t2, DataType eval_type,
  // std::map<int,ValueOrNull> & columns); void PrepareCQTerm(CQTerm& term, bool& cplx_val2, MysqlExpression::SetOfVars&
  // val2_vars, std::vector<int>& aliases, std::map<int,ValueOrNull>& columns, DataType& attr_eval_type);

  static JoinType GetJoinTypeAndCheckExpr(uint outer_join, Item *on_expr);

  void DisplayJoinResults(MultiIndex &mind, DimensionVector &all_involved_dims, JoinAlgType cur_join_type,
                          bool is_outer, int conditions_used);

  /*! \brief Checks if condition represented by \e it is a negation of other condition
   * \param it - item
   * \param is_there_not - output param.: true if negation was found
   * \return Item* - returns pointer to argument of negation or original item if there was no negation
   */
  Item *FindOutAboutNot(Item *it, bool &is_there_not);

  vcolumn::VirtualColumn *CreateColumnFromExpression(std::vector<MysqlExpression *> const &exprs, TempTable *temp_table,
                                                     int temp_table_alias, MultiIndex *mind);

  bool IsParameterFromWhere(const TabID &params_table);
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_QUERY_H_
