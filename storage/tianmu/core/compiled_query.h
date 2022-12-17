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
#ifndef TIANMU_CORE_COMPILED_QUERY_H_
#define TIANMU_CORE_COMPILED_QUERY_H_
#pragma once

#include <vector>

#include "common/common_definitions.h"
#include "core/cq_term.h"
#include "core/just_a_table.h"
#include "core/temp_table.h"

namespace Tianmu {
namespace core {
/*
        CompiledQuery - for storing execution plan of a query (sequence of
   primitive operations) and output data definition
*/
class MysqlExpression;

class CompiledQuery final {
 public:
  // Definition of one step
  enum class StepType {
    TABLE_ALIAS,
    TMP_TABLE,
    CREATE_CONDS,
    AND_F,
    OR_F,
    AND_DESC,
    OR_DESC,
    T_MODE,
    JOIN_T,
    LEFT_JOIN_ON,
    INNER_JOIN_ON,
    ADD_CONDS,
    APPLY_CONDS,
    ADD_COLUMN,
    ADD_ORDER,
    UNION,
    RESULT,
    STEP_ERROR,
    CREATE_VC
  };

  class CQStep {
   public:
    StepType type;
    TabID t1;
    TabID t2;
    TabID t3;
    AttrID a1, a2;
    CondID c1, c2, c3;
    CQTerm e1, e2, e3;
    common::Operator op;  // predicate: common::Operator::O_EQ, common::Operator::O_LESS etc.
    common::ExtraOperation ex_op;
    TMParameter tmpar;  // Table Mode Parameter
    JoinType jt;
    common::ColOperation cop;
    char *alias;
    std::vector<MysqlExpression *> mysql_expr;
    std::vector<int> virt_cols;
    std::vector<TabID> tables1;
    std::vector<TabID> tables2;
    int64_t n1, n2;  // additional parameter (e.g. descending order, TOP n,
                     // LIMIT n1..n2)
    SI si;

    CQStep()
        : type(StepType::TABLE_ALIAS),
          t1(common::NULL_VALUE_32),
          t2(common::NULL_VALUE_32),
          t3(common::NULL_VALUE_32),
          a1(common::NULL_VALUE_32),
          a2(common::NULL_VALUE_32),
          c1(common::NULL_VALUE_32),
          c2(common::NULL_VALUE_32),
          c3(common::NULL_VALUE_32),
          e1(),
          e2(),
          e3(),
          op(common::Operator::O_EQ),
          ex_op(common::ExtraOperation::EX_DO_NOTHING),
          tmpar(TMParameter::TM_DISTINCT),
          jt(JoinType::JO_INNER),
          cop(common::ColOperation::LISTING),
          alias(nullptr),
          n1(common::NULL_VALUE_64),
          n2(common::NULL_VALUE_64),
          si(){};

    CQStep(const CQStep &);

    ~CQStep() { delete[] alias; }
    CQStep &operator=(const CQStep &);
    /*! \brief Swap contents with another instance of CQStep.
     * \param s - another instance of CQStep.
     */
    void swap(CQStep &s);
    void Print(Query *query);  // display the step
   private:
    // if x is NullValue returns 0 otherwise x
    int N(int x) const {
      if (x == common::NULL_VALUE_32)
        return 0;
      else
        return x;
    }
  };
  // Initialization

  CompiledQuery();
  CompiledQuery(CompiledQuery const &);
  CompiledQuery &operator=(CompiledQuery const &) = delete;
  ~CompiledQuery() = default;

  // Add a new step to the execution plan

  void TableAlias(TabID &t_out, const TabID &n, const char *tab_name = nullptr, int id = -1);
  void TmpTable(TabID &t_out, const TabID &t1, bool for_subq = false);
  void CreateConds(CondID &c_out, const TabID &t1, CQTerm e1, common::Operator pr, CQTerm e2, CQTerm e3 = CQTerm(),
                   bool is_or_subtree = false, char like_esc = '\\', bool can_cond_push = false);
  void CreateConds(CondID &c_out, const TabID &t1, const CondID &c1, bool is_or_subtree = false,
                   bool can_cond_push = false);
  void And(const CondID &c1, const TabID &t, const CondID &c2);
  void Or(const CondID &c1, const TabID &t, const CondID &c2);
  void And(const CondID &c1, const TabID &t, CQTerm e1, common::Operator pr, CQTerm e2 = CQTerm(),
           CQTerm e3 = CQTerm());
  void Or(const CondID &c1, const TabID &t, CQTerm e1, common::Operator pr, CQTerm e2 = CQTerm(), CQTerm e3 = CQTerm());
  void Mode(const TabID &t1, TMParameter mode, int64_t n1 = 0, int64_t n2 = 0);
  void Join(const TabID &t1, const TabID &t2);
  void LeftJoinOn(const TabID &temp_table, std::vector<TabID> &left_tables, std::vector<TabID> &right_tables,
                  const CondID &cond_id);
  void InnerJoinOn(const TabID &temp_table, std::vector<TabID> &left_tables, std::vector<TabID> &right_tables,
                   const CondID &cond_id);
  void AddConds(const TabID &t1, const CondID &c1, CondType cond_type);
  void ApplyConds(const TabID &t1);
  void AddColumn(AttrID &a_out, const TabID &t1, CQTerm e1, common::ColOperation op, char const alias[] = 0,
                 bool distinct = false, SI *si = nullptr);

  /*! \brief Create compilation step CREATE_VC for mysql expression
   * \param a_out - id of created virtual column
   * \param t1 - id of TempTable for which virtual column is created
   * \param expr - the expression to be contained in the created column
   * \param src_table - src_table == t1 if expression works on aggregation
   * results, == common::NULL_VALUE_32 otherwise \return void
   */
  void CreateVirtualColumn(AttrID &a_out, const TabID &t1, MysqlExpression *expr,
                           const TabID &src_table = TabID(common::NULL_VALUE_32));

  /*! \brief Create compilation step CREATE_VC for subquery
   * \param a_out - id of created virtual column
   * \param t1 - id of TempTable for which virtual column is created
   * \param subquery - id of TempTable representing subquery
   * \param on_result - indicator of which MultiIndex should be passed to
   * Virtual column: one representing source data of t1 (\e false) or one
   * representing output columns of t1 (\e true), e.g. in case of subquery in
   * HAVING \return void
   */
  void CreateVirtualColumn(AttrID &a_out, const TabID &t1, const TabID &subquery, bool on_result);
  void CreateVirtualColumn(AttrID &a_out, const TabID &t1, std::vector<int> &vcs, const AttrID &vc_id);
  void CreateVirtualColumn(int &a_out, const TabID &t1, const TabID &table_alias, const AttrID &col_number);
  void Add_Order(const TabID &t1, const AttrID &a1,
                 int d = 0);  // d=1 for descending
  void Add_Order(const TabID &p_t, ptrdiff_t p_c) {
    int a_c(static_cast<int>(p_c));
    if (p_c == a_c)
      this->Add_Order(p_t, AttrID(-abs(a_c)), a_c < 0);
  }
  void Union(TabID &t_out, const TabID &t2, const TabID &t3, int all = 0);
  void Result(const TabID &t1);

  // Informations

  int NumOfTabs() { return no_tabs; }
  int NumOfAttrs(const TabID &tab) { return no_attrs[-tab.n - 1]; }
  int NumOfConds() { return no_conds; }
  int NumOfVirtualColumns(const TabID &tt) {
    if (no_virt_cols.find(tt) != no_virt_cols.end())
      return no_virt_cols[tt];
    return 0;
  }
  void Print(Query *);  // display the CompiledQuery

  int NumOfSteps() { return (int)steps.size(); }
  CQStep &Step(int i) { return steps[i]; }
  bool CountColumnOnly(const TabID &table);

  /*! \brief verify is given table alias is used in the query defined by a temp
   * table \param tab_id alias of a searched table \param tmp_table alias of the
   * temp table to be searched through returns true if tab_id == tmp_table or
   * tab_id is one of the source tables used in tmp_table
   */
  bool ExistsInTempTable(const TabID &tab_id, const TabID &tmp_table);

  /*! \brief Returns set of all dimensions used by output columns (ADD_COLUMN
   * step) for a given TempTable. \param table_id - id of given TempTable \param
   * ta - vector of pointers to JustATables necessary to get access to virtual
   * column inside relevant TempTable \return set<int>
   */
  std::set<int> GetUsedDims(const TabID &table_id, std::vector<std::shared_ptr<JustATable>> &ta);

  /*! \brief Finds the first TempTable in compilation steps that contains given
   * table as source table \param tab_id - id of search table \param tmp_table -
   * id of TempTable to start search from \param is_group_by - flag set to true
   * if searched table (one that is returned) represents GROUP BY query \return
   * returns id of TempTable containing tab_id as source table
   */
  TabID FindSourceOfParameter(const TabID &tab_id, const TabID &tmp_table, bool &is_group_by);

  /*! \brief Finds if TempTable represents query with GroupBy clause
   * \param tab_id - TempTable to be checked
   * \return true if it is GroupBy query, false otherwise
   */
  bool IsGroupByQuery(const TabID &tab_id);
  bool IsResultTable(const TabID &t);
  bool IsOrderedBy(const TabID &t);
  int64_t FindLimit(int table);  // return LIMIT value for table, or -1
  bool FindDistinct(int table);  // return true if there is DISTINCT flag for this query
  bool NoAggregationOrderingAndDistinct(int table);
  std::pair<int64_t, int64_t> GetGlobalLimit();

  int GetNumOfDimens(const TabID &tab_id);
  TabID GetTableOfCond(const CondID &cond_id);

  void BuildTableIDStepsMap();
  void AddGroupByStep(CQStep s) { steps_group_by_cols.push_back(s); }

 private:
  // NOTE: new objects' IDs (e.g. while declaring new aliases, filters etc.) are
  // reserved and obtained by these functions.
  TabID NextTabID() {
    no_tabs++;
    no_attrs.push_back(0);
    return TabID(-no_tabs);
  }
  AttrID NextVCID(const TabID &tt) { return AttrID(no_virt_cols[tt]++); }
  AttrID NextAttrID(const TabID &tab) {
    no_attrs[-tab.n - 1]++;
    return AttrID(-no_attrs[-tab.n - 1]);
  }
  CondID NextCondID() { return CondID(no_conds++); }  // IDs of tables and attributes start with -1 and decrease;
  /*! \brief Checks if given TabID represents TempTable or just some table alias
   * \param t - TabID of table to be checked
   * \return true if t is an alias of TempTable, false otherwise
   */
  bool IsTempTable(const TabID &t);
  int FindRootTempTable(int x);

  // IDs of params and filters start with 0 and increase.
  int no_tabs;   // counters: which IDs are not in use?
  int no_conds;  // counters: which IDs are not in use?

  std::map<TabID, int> no_virt_cols;

  std::vector<int> no_attrs;  // repository of attr_ids for each table
  std::vector<CQStep> steps;  // repository of steps to be executed

  // Extra containers to store specific steps (tmp_tables, group by, tabid2steps
  // map) so that search can be faster than looking them in the original
  // container (steps).
  std::vector<CQStep> steps_tmp_tables;
  std::vector<CQStep> steps_group_by_cols;
  std::multimap<TabID, CQStep> TabIDSteps;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_COMPILED_QUERY_H_
