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

#include <algorithm>

#include "common/mysql_gate.h"
#include "core/compilation_tools.h"
#include "core/compiled_query.h"
#include "core/engine.h"
#include "core/mysql_expression.h"
#include "core/query.h"
#include "core/transaction.h"

namespace Tianmu {
namespace core {
QueryRouteTo TableUnmysterify(TABLE_LIST *tab, const char *&database_name, const char *&table_name,
                              const char *&table_alias, const char *&table_path) {
  ASSERT_MYSQL_STRING(tab->table->s->db);
  ASSERT_MYSQL_STRING(tab->table->s->table_name);
  ASSERT_MYSQL_STRING(tab->table->s->path);

  database_name = tab->table->s->db.str;
  if (tab->referencing_view)
    table_name = tab->referencing_view->table_name;
  else
    table_name = tab->table->s->table_name.str;
  table_alias = tab->alias;
  table_path = tab->table->s->path.str;

  return QueryRouteTo::kToTianmu;
}

QueryRouteTo JudgeErrors(SELECT_LEX *sl) {
  if (!sl->join) {
    return QueryRouteTo::kToMySQL;
  }

  /* gone with mysql5.6
      if(sl->join && sl->join->procedure) {
              my_message(ER_SYNTAX_ERROR, "Tianmu specific error: No PROCEDURE
     syntax supported", MYF(0)); throw ReturnMeToMySQLWithError();
      }
  */

  if (sl->offset_limit)
    if (sl->offset_limit->type() != Item::INT_ITEM /*|| sl->offset_limit->val_int()*/) {
      my_message(ER_SYNTAX_ERROR, "Tianmu specific error: Only numerical OFFSET supported", MYF(0));
      throw ReturnMeToMySQLWithError();
    }

  if (sl->select_limit)
    if (sl->select_limit->type() != Item::INT_ITEM) {
      my_message(ER_SYNTAX_ERROR, "Tianmu specific error: Only numerical LIMIT supported", MYF(0));
      throw ReturnMeToMySQLWithError();
    }
  if (sl->olap == ROLLUP_TYPE) {
    /*my_message(ER_SYNTAX_ERROR, "Tianmu specific error: WITH ROLLUP not
     supported", MYF(0)); throw ReturnMeToMySQLWithError();*/
    return QueryRouteTo::kToMySQL;
  }

  return QueryRouteTo::kToTianmu;
}

void SetLimit(SELECT_LEX *sl, SELECT_LEX *gsl, int64_t &offset_value, int64_t &limit_value) {
  if (sl->select_limit && (!gsl || sl->select_limit != gsl->select_limit)) {
    limit_value = sl->select_limit->val_int();
    if (limit_value == UINT_MAX) { /* this value seems to be sometimes
                                      automatically set by MYSQL to UINT_MAX*/
      limit_value = -1;            // no limit set
      offset_value = -1;
    };
  };

  if (limit_value)
    offset_value = 0;

  if (sl->offset_limit && (!gsl || sl->offset_limit != gsl->offset_limit))
    offset_value = sl->offset_limit->val_int();
}

// Used in Query::Compile() to break compilation in the middle and make cleanup
// before returning
class CompilationError {};

QueryRouteTo Query::FieldUnmysterify(Item *item, const char *&database_name, const char *&table_name,
                                     const char *&table_alias, const char *&table_path, const TABLE *&table_ptr,
                                     const char *&field_name, const char *&field_alias) {
  table_alias = EMPTY_TABLE_CONST_INDICATOR;
  database_name = nullptr;
  table_name = nullptr;
  table_path = nullptr;
  table_ptr = nullptr;
  field_name = nullptr;
  field_alias = nullptr;

  item = UnRef(item);

  Item_field *ifield;
  switch (static_cast<int>(item->type())) {
    case static_cast<int>(Item_tianmufield::enumTIANMUFiledItem::TIANMUFIELD_ITEM):
      ifield = dynamic_cast<Item_tianmufield *>(item)->OriginalItem();
      if (IsAggregationItem(ifield)) {
        Item_sum *is = (Item_sum *)ifield;
        if (is->get_arg_count() > 1)
          return QueryRouteTo::kToMySQL;
        Item *tmp_item = UnRef(is->get_arg(0));
        if (tmp_item->type() == Item::FIELD_ITEM)
          ifield = (Item_field *)tmp_item;
        else if (static_cast<int>(tmp_item->type()) ==
                 static_cast<int>(Item_tianmufield::enumTIANMUFiledItem::TIANMUFIELD_ITEM))
          ifield = dynamic_cast<Item_tianmufield *>(tmp_item)->OriginalItem();
        else {
          return QueryRouteTo::kToMySQL;
        }
      }
      break;
    case Item::FIELD_ITEM:  // regular select
      ifield = (Item_field *)item;
      break;

    case Item::SUM_FUNC_ITEM: {  // min(k), max(k), count(), avg(k), sum
      Item_sum *is = (Item_sum *)item;
      if (is->get_arg_count() > 1) {
        return QueryRouteTo::kToMySQL;
      }
      Item *tmp_item = UnRef(is->get_arg(0));
      if (tmp_item->type() == Item::FIELD_ITEM)
        ifield = (Item_field *)tmp_item;
      else if (static_cast<int>(tmp_item->type()) ==
               static_cast<int>(Item_tianmufield::enumTIANMUFiledItem::TIANMUFIELD_ITEM)) /* *CAUTION* comparision of
                                                                enumerators from different
                                                                enums */
        ifield = dynamic_cast<Item_tianmufield *>(tmp_item)->OriginalItem();
      else
        return QueryRouteTo::kToMySQL;
      break;
    }
    case Item::FUNC_ITEM:  // complex expressions
      // if(WrapMysqlExpression(item, &not_a_table_column) ==
      // WrapStatus::SUCCESS)
      return QueryRouteTo::kToTianmu;
      return QueryRouteTo::kToMySQL;
    default:
      // if(WrapMysqlExpression(item, &not_a_table_column) ==
      // WrapStatus::SUCCESS)
      return QueryRouteTo::kToTianmu;
      return QueryRouteTo::kToMySQL;
  };

  /*
   * By MW. Related to bug 1073.
   *
   * For views, 'table_alias' must be created as a concatenation
   * of original view(s) name(s) and the table name
   * - currently it's just the table name, which leads to ambiguity and errors
   * when the same table is used in another place in the query.
   * Note that there can be several nested views.
   *
   * To retrieve name(s) of the view(s) from which the current 'ifield' comes
   * you may try the following expression:
   *
   *     ifield->cached_table->table_list->belong_to_view->alias  or
   * ...->table_name or
   * ifield->cached_table->table_list->referencing_view->alias  or
   * ...->table_name
   *
   * Here, 'belong_to_view' and 'referencing_view' are different
   * if there are nested views.
   *
   * Probably ifield->cached_table could be also used to find
   * 'database_name', 'table_name' and 'table_path'
   * in a simpler way than currently.
   */

  Field *f = ifield->result_field;

  ASSERT_MYSQL_STRING(f->table->s->db);
  ASSERT_MYSQL_STRING(f->table->s->table_name);
  ASSERT_MYSQL_STRING(f->table->s->path);

  table_ptr = f->table;
  table_alias = ifield->table_name;
  database_name = f->table->s->db.str;
  table_name = GetTableName(ifield);
  table_path = f->table->s->path.str;
  field_name = f->field_name;
  field_alias = ifield->item_name.ptr();

  return QueryRouteTo::kToTianmu;
}

bool Query::FieldUnmysterify(Item *item, TabID &tab, AttrID &col) {
  Item_field *ifield;
  if (item->type() == Item_tianmufield::get_tianmuitem_type()) {
    ifield = dynamic_cast<Item_tianmufield *>(item)->OriginalItem();
    if (IsAggregationItem(ifield)) {
      Item_sum *is = (Item_sum *)ifield;
      if (is->get_arg_count() > 1)
        return false;
      Item *tmp_item = UnRef(is->get_arg(0));
      if (tmp_item->type() == Item::FIELD_ITEM)
        ifield = (Item_field *)tmp_item;
      else if (tmp_item->type() == Item_tianmufield::get_tianmuitem_type())
        ifield = dynamic_cast<Item_tianmufield *>(tmp_item)->OriginalItem();
      else if (tmp_item->type() == Item::FUNC_ITEM) {
        Item_tianmufield *tianmui = dynamic_cast<Item_tianmufield *>(item);
        tab.n = tianmui->varID[0].tab;
        col.n = tianmui->varID[0].tab;
        return true;
      } else
        return false;
    }
  } else if (item->type() == Item::SUM_FUNC_ITEM) {  // min(k), max(k), count(), avg(k), sum(),
                                                     // group_concat()
    Item_sum *is = (Item_sum *)item;
    if (is->get_arg_count() > 1) {
      int dir = 0;
      if (((Item_sum *)item)->sum_func() == Item_sum::GROUP_CONCAT_FUNC) {
        dir = ((Item_func_group_concat *)item)->direction();
      }

      // only pass 1 group 1 order by case, which is the only case Tianmu
      // supported
      if (dir == 0 || is->get_arg_count() != 2)
        return false;
    }
    Item *tmp_item = UnRef(is->get_arg(0));
    if (tmp_item->type() == Item::FIELD_ITEM)
      ifield = (Item_field *)tmp_item;
    else if (tmp_item->type() == Item_tianmufield::get_tianmuitem_type())
      ifield = dynamic_cast<Item_tianmufield *>(tmp_item)->OriginalItem();
    else
      return false;
  } else if (item->type() == Item::FIELD_ITEM)
    ifield = (Item_field *)item;
  else
    return false;

  if (!ifield->table_name) {
    return false;
    // union results have no name, but refer to the temp table
    // if(field_alias2num.find(TabIDColAlias((*table_alias2index)[ifield->table_name],
    // ifield->item_name.ptr())) == field_alias2num.end())
    //	return false;
    // col.n =
    // field_alias2num[TabIDColAlias((*table_alias2index)[ifield->table_name],
    // ifield->item_name.ptr())];
    // tab.n = common::NULL_VALUE_32;
    // return true;
  }
  const char *table_name = GetTableName(ifield);
  std::string ext_alias =
      std::string(table_name ? table_name : "") + std::string(":") + std::string(ifield->table_name);
  auto it = table_alias2index_ptr.lower_bound(ext_alias);
  auto it_end = table_alias2index_ptr.upper_bound(ext_alias);
  if (it == table_alias2index_ptr.end())
    return false;
  for (; it != it_end; it++) {
    TABLE *mysql_table = it->second.second;
    tab = TabID(it->second.first);
    if (ifield->field->table != mysql_table)
      continue;

    // FIXME: is this correct?
    if (!mysql_table->pos_in_table_list->is_view_or_derived()) {
      // Physical table in FROM - TianmuTable
      int field_num;
      for (field_num = 0; mysql_table->field[field_num]; field_num++)
        if (mysql_table->field[field_num]->field_name == ifield->result_field->field_name)
          break;
      if (!mysql_table->field[field_num])
        continue;
      col = AttrID(field_num);
      return true;
    } else {
      // subselect in FROM - TempTable
      if (field_alias2num.find(TabIDColAlias(tab.n, ifield->result_field->field_name)) == field_alias2num.end())
        continue;
      col.n = field_alias2num[TabIDColAlias(tab.n, ifield->result_field->field_name)];
      return true;
    }
  }
  return false;
}

QueryRouteTo Query::AddJoins(List<TABLE_LIST> &join, TabID &tmp_table, std::vector<TabID> &left_tables,
                             std::vector<TabID> &right_tables, bool in_subquery, bool &first_table /*= true*/,
                             bool for_subq_in_where /*false*/, bool use_tmp_when_no_join /*false*/) {
  if (!join.elements) {
    // Use use_tmp_when_no_join when AddJoins
    // The caller decides that the scenario is When join_list has no elements and field has sp
    if (use_tmp_when_no_join) {
      // The index of the subscript [-1] indicates
      // that the first physical table is used as the temporary table.
      // The subscript value operation in
      // TMP_TABLE is required in combination with Query::Preexecute.
      TabID tab(-1);
      left_tables.push_back(tab);
      cq->TmpTable(tmp_table, tab, for_subq_in_where);
      return QueryRouteTo::kToTianmu;
    }

    // no tables in table list in this select
    return QueryRouteTo::kToMySQL;
  }

  // on first call first_table = true. It indicates if it is the first table to
  // be added is_left is true iff it is nested left join which needs to be
  // flatten (all tables regardless of their join type need to be left-joined)
  TABLE_LIST *join_ptr;
  List_iterator<TABLE_LIST> li(join);
  std::vector<TABLE_LIST *> reversed;

  // if the table list was empty altogether, we wouldn't even enter
  // Compilation(...) it must be sth. like `select 1 from t1 union select 2` and
  // we are in the second select in the union

  reversed.reserve(join.elements);
  while ((join_ptr = li++) != nullptr) reversed.push_back(join_ptr);
  size_t size = reversed.size();
  for (unsigned int i = 0; i < size; i++) {
    join_ptr = reversed[size - i - 1];
    if (join_ptr->nested_join) {
      std::vector<TabID> local_left, local_right;
      if (QueryRouteTo::kToMySQL == AddJoins(join_ptr->nested_join->join_list, tmp_table, local_left, local_right,
                                             in_subquery, first_table, for_subq_in_where))
        return QueryRouteTo::kToMySQL;
      JoinType join_type = GetJoinTypeAndCheckExpr(join_ptr->outer_join, join_ptr->join_cond());
      CondID cond_id;
      if (QueryRouteTo::kToMySQL == BuildCondsIfPossible(join_ptr->join_cond(), cond_id, tmp_table, join_type))
        return QueryRouteTo::kToMySQL;
      left_tables.insert(left_tables.end(), right_tables.begin(), right_tables.end());
      local_left.insert(local_left.end(), local_right.begin(), local_right.end());
      if (join_ptr->outer_join)
        right_tables = local_left;
      else
        left_tables.insert(left_tables.end(), local_left.begin(), local_left.end());
      if (join_ptr->join_cond() && join_ptr->outer_join) {
        cq->LeftJoinOn(tmp_table, left_tables, right_tables, cond_id);
        left_tables.insert(left_tables.end(), right_tables.begin(), right_tables.end());
        right_tables.clear();
      } else if (join_ptr->join_cond() && !join_ptr->outer_join)
        cq->InnerJoinOn(tmp_table, left_tables, right_tables, cond_id);
    } else {
      DEBUG_ASSERT(join_ptr->table && "We require that the table is defined if it is not a nested join");
      const char *database_name = 0;
      const char *table_name = 0;
      const char *table_alias = 0;
      const char *table_path = 0;
      TabID tab(0);
      if (join_ptr->is_view_or_derived()) {
        if (QueryRouteTo::kToMySQL ==
            Compile(cq, join_ptr->derived_unit()->first_select(), join_ptr->derived_unit()->union_distinct, &tab))
          return QueryRouteTo::kToMySQL;
        table_alias = join_ptr->alias;
      } else {
        if (QueryRouteTo::kToMySQL == TableUnmysterify(join_ptr, database_name, table_name, table_alias, table_path))
          return QueryRouteTo::kToMySQL;
        int tab_num = path2num[table_path];  // number of a table on a list in
                                             // `this` QUERY object
        int id = t[tab_num]->GetID();
        cq->TableAlias(tab, TabID(tab_num), table_name, id);
      }
      std::string ext_alias = std::string(table_name ? table_name : "") + std::string(":") + std::string(table_alias);
      table_alias2index_ptr.insert(std::make_pair(ext_alias, std::make_pair(tab.n, join_ptr->table)));
      if (first_table) {
        left_tables.push_back(tab);
        DEBUG_ASSERT(!join_ptr->join_cond() &&
                     "It is not possible to join the first table with the LEFT "
                     "direction");
        cq->TmpTable(tmp_table, tab, for_subq_in_where);
        first_table = false;
      } else {
        cq->Join(tmp_table, tab);
        JoinType join_type = GetJoinTypeAndCheckExpr(join_ptr->outer_join, join_ptr->join_cond());
        // if(join_type == JoinType::JO_LEFT && join_ptr->join_cond() &&
        // dynamic_cast<Item_cond_or*>(join_ptr->join_cond()))
        //	return QueryRouteTo::kToMySQL;
        CondID cond_id;
        if (QueryRouteTo::kToMySQL == BuildCondsIfPossible(join_ptr->join_cond(), cond_id, tmp_table, join_type))
          return QueryRouteTo::kToMySQL;
        if (join_ptr->join_cond() && join_ptr->outer_join) {
          right_tables.push_back(tab);
          cq->LeftJoinOn(tmp_table, left_tables, right_tables, cond_id);
          left_tables.push_back(tab);
          right_tables.clear();
        } else if (join_ptr->join_cond() && !join_ptr->outer_join) {
          right_tables.push_back(tab);
          cq->InnerJoinOn(tmp_table, left_tables, right_tables, cond_id);
          left_tables.push_back(tab);
          right_tables.clear();
        } else
          left_tables.push_back(tab);
        // if(join_ptr->on_expr)
        //	cq->SetLOJOuterDim(tmp_table, tab, i);
      }
    }
  }
  return QueryRouteTo::kToTianmu;
}

QueryRouteTo Query::AddFields(List<Item> &fields, TabID const &tmp_table, TabID const &base_table,
                              bool const group_by_clause, int &num_of_added_fields, bool ignore_minmax,
                              bool &aggregation_used) {
  List_iterator_fast<Item> li(fields);
  Item *item;
  int added = 0;
  item = li++;
  while (item) {
    WrapStatus ws;
    common::ColOperation oper;
    bool distinct;
    if (QueryRouteTo::kToMySQL == OperationUnmysterify(item, oper, distinct, group_by_clause))
      return QueryRouteTo::kToMySQL;

    if (IsAggregationItem(item))
      aggregation_used = true;

    // in case of transformed subquery sometimes we need to revert back
    // transformation to MIN/MAX
    if (ignore_minmax && (oper == common::ColOperation::MIN || oper == common::ColOperation::MAX))
      oper = common::ColOperation::LISTING;

    // select PHYSICAL COLUMN or AGGREGATION over PHYSICAL COLUMN
    if ((IsFieldItem(item) || IsAggregationOverFieldItem(item)) &&
        (IsLocalColumn(item, tmp_table) || (!base_table.IsNullID() && IsLocalColumn(item, base_table))))
      AddColumnForPhysColumn(item, tmp_table, base_table, oper, distinct, false, item->item_name.ptr());
    // REF to FIELD_ITEM
    else if (item->type() == Item::REF_ITEM) {
      item = UnRef(item);
      continue;
    }
    // if ((UnRef(item)->type() == Item_tianmufield::enumTIANMUFiledItem::TIANMUFIELD_ITEM ||
    //      UnRef(item)->type() == Item_tianmufield::FIELD_ITEM) &&
    //     IsLocalColumn(UnRef(item), tmp_table))
    //   AddColumnForPhysColumn(UnRef(item), tmp_table, oper, distinct, false, false);
    // else {
    //   //
    // }
    else if (IsAggregationItem(item) && (((Item_sum *)item)->get_arg(0))->type() == Item::REF_ITEM &&
             (UnRef(((Item_sum *)item)->get_arg(0))->type() == Item_tianmufield::get_tianmuitem_type() ||
              (UnRef(((Item_sum *)item)->get_arg(0))->type() == Item_tianmufield::FIELD_ITEM)) &&
             IsLocalColumn(UnRef(((Item_sum *)item)->get_arg(0)), tmp_table))
      // AGGR on REF to FIELD_ITEM
      AddColumnForPhysColumn(UnRef(((Item_sum *)item)->get_arg(0)), tmp_table, TabID(), oper, distinct, false,
                             item->item_name.ptr());
    else if (IsAggregationItem(item)) {
      // select AGGREGATION over EXPRESSION
      Item_sum *item_sum = (Item_sum *)item;
      if (item_sum->get_arg_count() > 1 || HasAggregation(item_sum->get_arg(0)))
        return QueryRouteTo::kToMySQL;
      if (IsCountStar(item_sum)) {  // count(*) doesn't need any virtual column
        AttrID at;
        cq->AddColumn(at, tmp_table, CQTerm(), oper, item_sum->item_name.ptr(), false);
        field_alias2num[TabIDColAlias(tmp_table.n, item_sum->item_name.ptr())] = at.n;
      } else {
        MysqlExpression *expr;
        ws = WrapMysqlExpression(item_sum->get_arg(0), tmp_table, expr, false, false);
        if (ws == WrapStatus::FAILURE)
          return QueryRouteTo::kToMySQL;
        AddColumnForMysqlExpression(expr, tmp_table,
                                    ignore_minmax ? item_sum->get_arg(0)->item_name.ptr() : item_sum->item_name.ptr(),
                                    oper, distinct);
      }
    } else if (item->type() == Item::SUBSELECT_ITEM) {
      CQTerm term;
      AttrID at;
      if (Item2CQTerm(item, term, tmp_table,
                      /*group_by_clause ? HAVING_FILTER :*/ CondType::WHERE_COND) == QueryRouteTo::kToMySQL)
        return QueryRouteTo::kToMySQL;
      cq->AddColumn(at, tmp_table, term, common::ColOperation::LISTING, item->item_name.ptr(), distinct);
      field_alias2num[TabIDColAlias(tmp_table.n, item->item_name.ptr())] = at.n;
    } else {
      // select EXPRESSION
      if (HasAggregation(item)) {
        oper = common::ColOperation::DELAYED;
        aggregation_used = true;
      }
      MysqlExpression *expr(nullptr);
      ws = WrapMysqlExpression(item, tmp_table, expr, false, oper == common::ColOperation::DELAYED);
      if (ws == WrapStatus::FAILURE)
        return QueryRouteTo::kToMySQL;
      if (!item->item_name.ptr()) {
        Item_func_conv_charset *item_conv = dynamic_cast<Item_func_conv_charset *>(item);
        if (item_conv) {
          Item **ifunc_args = item_conv->arguments();
          AddColumnForMysqlExpression(expr, tmp_table, ifunc_args[0]->item_name.ptr(), oper, distinct);
        } else {
          AddColumnForMysqlExpression(expr, tmp_table, item->item_name.ptr(), oper, distinct);
        }
      } else
        AddColumnForMysqlExpression(expr, tmp_table, item->item_name.ptr(), oper, distinct);
    }
    added++;
    item = li++;
  }
  num_of_added_fields = added;
  return QueryRouteTo::kToTianmu;
}

// todo(dfx): handle more query scenarios
QueryRouteTo Query::AddSemiJoinFiled(List<Item> &fields, List<TABLE_LIST> &join, const TabID &tmp_table) {
  List_iterator_fast<Item> field_li(fields);
  Item *item;
  item = field_li++;
  while (item) {
    WrapStatus ws;
    common::ColOperation oper;
    bool distinct;
    if (QueryRouteTo::kToMySQL == OperationUnmysterify(item, oper, distinct, 0))
      return QueryRouteTo::kToMySQL;
    if (!IsFieldItem(item)) {
      item = field_li++;
      continue;
    }
    AddColumnForPhysColumn(item, tmp_table, TabID(), oper, distinct, false, item->item_name.ptr());
    item = field_li++;
  }

  TABLE_LIST *join_ptr;
  List_iterator<TABLE_LIST> li(join);
  std::vector<TABLE_LIST *> reversed;
  while ((join_ptr = li++) != nullptr) {
    reversed.push_back(join_ptr);
  }
  size_t size = reversed.size();
  for (unsigned int i = 0; i < size; i++) {
    join_ptr = reversed[size - i - 1];
    if (join_ptr->nested_join) {
      List_iterator_fast<Item> outer_field_li(join_ptr->nested_join->sj_outer_exprs);
      Item *outer_item;
      outer_item = outer_field_li++;
      while (outer_item) {
        WrapStatus ws;
        common::ColOperation oper;
        bool distinct;
        if (QueryRouteTo::kToMySQL == OperationUnmysterify(outer_item, oper, distinct, 0))
          return QueryRouteTo::kToMySQL;
        if (!IsFieldItem(outer_item)) {
          outer_item = outer_field_li++;
          continue;
        }
        AddColumnForPhysColumn(outer_item, tmp_table, TabID(), oper, distinct, false, outer_item->item_name.ptr());
        outer_item = outer_field_li++;
      }
    }
  }
  return QueryRouteTo::kToTianmu;
}

QueryRouteTo Query::AddGroupByFields(ORDER *group_by, const TabID &tmp_table, const TabID &base_table) {
  for (; group_by; group_by = group_by->next) {
    if (group_by->direction != ORDER::ORDER_ASC) {
      my_message(ER_SYNTAX_ERROR,
                 "Tianmu specific error: Using DESC after GROUP BY clause not "
                 "allowed. Use "
                 "ORDER BY to order the result",
                 MYF(0));
      throw ReturnMeToMySQLWithError();
    }

    Item *item = *(group_by->item);
    item = UnRef(item);
    // group by PHYSICAL COLUMN
    if ((IsFieldItem(item) || (IsAggregationItem(item) && IsFieldItem(((Item_sum *)item)->get_arg(0)))) &&
        (IsLocalColumn(item, tmp_table) || (!base_table.IsNullID() && IsLocalColumn(item, base_table)))) {
      AddColumnForPhysColumn(item, tmp_table, base_table, common::ColOperation::GROUP_BY, false, true);
    } else if (item->type() == Item::SUBSELECT_ITEM) {
      CQTerm term;
      AttrID at;
      if (Item2CQTerm(item, term, tmp_table, CondType::WHERE_COND) == QueryRouteTo::kToMySQL)
        return QueryRouteTo::kToMySQL;
      cq->AddColumn(at, tmp_table, term, common::ColOperation::GROUP_BY, 0);
      //			field_alias2num[TabIDColAlias(tmp_table.n,
      // item->item_name.ptr())] =
      // at.n;
    } else {  // group by COMPLEX EXPRESSION
      MysqlExpression *expr = 0;
      if (WrapStatus::FAILURE == WrapMysqlExpression(item, tmp_table, expr, true, true))
        return QueryRouteTo::kToMySQL;
      AddColumnForMysqlExpression(expr, tmp_table, item->item_name.ptr(), common::ColOperation::GROUP_BY, false, true);
    }
  }
  return QueryRouteTo::kToTianmu;
}

QueryRouteTo Query::AddOrderByFields(ORDER *order_by, TabID const &tmp_table, TabID const &base_table,
                                     int const group_by_clause) {
  for (; order_by; order_by = order_by->next) {
    std::pair<int, int> vc;
    Item *item = *(order_by->item);
    CQTerm my_term;
    QueryRouteTo result{QueryRouteTo::kToMySQL};
    // at first we need to check if we don't have non-deterministic expression
    // (e.g., rand()) in such case we should order by output column in TempTable
    if (!IsFieldItem(item) && !IsAggregationItem(item) && !IsDeterministic(item) &&
        item->type() != Item::SUBSELECT_ITEM) {
      MysqlExpression *expr = nullptr;
      WrapStatus ws = WrapMysqlExpression(item, tmp_table, expr, false, false);
      if (ws == WrapStatus::FAILURE)
        return QueryRouteTo::kToMySQL;
      DEBUG_ASSERT(!expr->IsDeterministic());
      int col_num = AddColumnForMysqlExpression(expr, tmp_table, nullptr, common::ColOperation::LISTING, false, true);
      vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, AttrID(-col_num - 1));
      if (vc.first == common::NULL_VALUE_32) {
        vc.first = tmp_table.n;
        cq->CreateVirtualColumn(vc.second, tmp_table, tmp_table, AttrID(col_num));
        phys2virt.insert(std::make_pair(std::make_pair(tmp_table.n, -col_num - 1), vc));
      }
      cq->Add_Order(tmp_table, AttrID(vc.second), (order_by->direction != ORDER::ORDER_ASC));
      continue;
    }
    if (group_by_clause) {
      if (item->type() == Item::FUNC_ITEM) {
        MysqlExpression *expr = nullptr;
        bool delayed = false;
        if (HasAggregation(item)) {
          delayed = true;
        }

        WrapStatus ws = WrapMysqlExpression(item, tmp_table, expr, false, delayed);
        if (ws == WrapStatus::FAILURE)
          return QueryRouteTo::kToMySQL;
        DEBUG_ASSERT(expr->IsDeterministic());
        int col_num = AddColumnForMysqlExpression(
            expr, tmp_table, nullptr, delayed ? common::ColOperation::DELAYED : common::ColOperation::LISTING, false,
            true);
        vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, AttrID(-col_num - 1));
        if (vc.first == common::NULL_VALUE_32) {
          vc.first = tmp_table.n;
          cq->CreateVirtualColumn(vc.second, tmp_table, tmp_table, AttrID(col_num));
          phys2virt.insert(std::make_pair(std::make_pair(tmp_table.n, -col_num - 1), vc));
        }
        cq->Add_Order(tmp_table, AttrID(vc.second), (order_by->direction != ORDER::ORDER_ASC));
        continue;
        // we can reuse transformation done in case of HAVING
        // result = Item2CQTerm(item, my_term, tmp_table, CondType::HAVING_COND);
      } else {
        AttrID at;
        result = Item2CQTerm(item, my_term, tmp_table, CondType::HAVING_COND, false, nullptr, nullptr, base_table);
        if (item->type() == Item::SUBSELECT_ITEM) {
          // create a materialized column with subsel results for the ordering
          cq->AddColumn(at, tmp_table, my_term, common::ColOperation::DELAYED, nullptr, false);
          vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, at);
          if (vc.first == common::NULL_VALUE_32) {
            vc.first = tmp_table.n;
            cq->CreateVirtualColumn(vc.second, tmp_table, tmp_table, at);
            phys2virt.insert(std::make_pair(std::pair<int, int>(tmp_table.n, at.n), vc));
          }
        } else  // a naked column
          vc.second = my_term.vc_id;
        // cq->Add_Order(tmp_table, AttrID(vc.second), !(order_by->asc));
      }

    } else {
      result = Item2CQTerm(item, my_term, tmp_table, CondType::WHERE_COND);
      vc.second = my_term.vc_id;
    }
    if (result != QueryRouteTo::kToTianmu)
      return QueryRouteTo::kToMySQL;
    cq->Add_Order(tmp_table, AttrID(vc.second), order_by->direction != ORDER::ORDER_ASC);
  }
  return QueryRouteTo::kToTianmu;
}

QueryRouteTo Query::AddGlobalOrderByFields(SQL_I_List<ORDER> *global_order, const TabID &tmp_table, int max_col) {
  if (!global_order)
    return QueryRouteTo::kToTianmu;

  ORDER *order_by;
  for (uint i = 0; i < global_order->elements; i++) {
    order_by = (i == 0 ? (ORDER *)(global_order->first) : order_by->next);
    // the way to traverse 'global_order' list maybe is not very orthodox, but
    // it works

    if (order_by == nullptr)
      return QueryRouteTo::kToMySQL;

    int col_num = common::NULL_VALUE_32;
    if ((*(order_by->item))->type() == Item::INT_ITEM) {
      col_num = int((*(order_by->item))->val_int());
      if (col_num < 1 || col_num > max_col)
        return QueryRouteTo::kToMySQL;
      col_num--;
      col_num = -col_num - 1;  // make it negative as are columns in TempTable
    } else {
      Item *item = *(order_by->item);
      if (!item->item_name.ptr())
        return QueryRouteTo::kToMySQL;
      bool found = false;
      for (auto &it : field_alias2num) {
        if (tmp_table.n == it.first.first && strcasecmp(it.first.second.c_str(), item->item_name.ptr()) == 0) {
          col_num = it.second;
          found = true;
          break;
        }
      }
      if (!found)
        return QueryRouteTo::kToMySQL;
    }
    int attr;
    cq->CreateVirtualColumn(attr, tmp_table, tmp_table, AttrID(col_num));
    phys2virt.insert(std::make_pair(std::make_pair(tmp_table.n, col_num), std::make_pair(tmp_table.n, attr)));
    cq->Add_Order(tmp_table, AttrID(attr), order_by->direction != ORDER::ORDER_ASC);
  }

  return QueryRouteTo::kToTianmu;
}

Query::WrapStatus Query::WrapMysqlExpression(Item *item, const TabID &tmp_table, MysqlExpression *&expr, bool in_where,
                                             bool aggr_used) {
  // Check if the expression doesn't contain any strange items that we don't
  // want to see. By the way, collect references to all Item_field objects.
  std::set<Item *> ifields;
  MysqlExpression::Item2VarID item2varid;
  if (!MysqlExpression::SanityAggregationCheck(item, ifields))
    return WrapStatus::FAILURE;

  // this large "if" can be removed to use common code, but many small "ifs"
  // must be created then
  if (in_where) {
    // create a map:  [Item_field pointer] -> VarID
    for (auto &it : ifields) {
      if (IsAggregationItem(it)) {
        // a few checkings for aggregations
        Item_sum *aggregation = (Item_sum *)it;
        if (aggregation->get_arg_count() > 1)
          return WrapStatus::FAILURE;
        if (IsCountStar(aggregation))  // count(*) doesn't need any virtual column
          return WrapStatus::FAILURE;
      }
      AttrID col, at;
      TabID tab;
      // find [tab] and [col] which identify column in TIANMU
      if (!FieldUnmysterify(it, tab, col))
        return WrapStatus::FAILURE;
      if (!cq->ExistsInTempTable(tab, tmp_table)) {
        bool is_group_by;
        TabID params_table = cq->FindSourceOfParameter(tab, tmp_table, is_group_by);
        common::ColOperation oper;
        bool distinct;
        if (QueryRouteTo::kToMySQL == OperationUnmysterify(it, oper, distinct, true))
          return WrapStatus::FAILURE;
        if (is_group_by && !IsParameterFromWhere(params_table)) {
          col.n = AddColumnForPhysColumn(it, params_table, TabID(), oper, distinct, true);
          item2varid[it] = VarID(params_table.n, col.n);
        } else
          item2varid[it] = VarID(tab.n, col.n);
      } else {
        // aggregation in WHERE not possible unless it is a parameter
        DEBUG_ASSERT(!IsAggregationItem(it));
        item2varid[it] = VarID(tab.n, col.n);
      }
    }
  } else {  // !in_where
    WrapStatus ws;
    AttrID at, vc;
    for (auto &it : ifields) {
      if (IsAggregationItem(it)) {
        Item_sum *aggregation = (Item_sum *)it;
        if (aggregation->get_arg_count() > 1)
          return WrapStatus::FAILURE;

        if (IsCountStar(aggregation)) {  // count(*) doesn't need any virtual column
          at.n = GetAddColumnId(AttrID(common::NULL_VALUE_32), tmp_table, common::ColOperation::COUNT, false);
          if (at.n == common::NULL_VALUE_32)  // doesn't exist yet
            cq->AddColumn(at, tmp_table, CQTerm(), common::ColOperation::COUNT, nullptr, false);
        } else {
          common::ColOperation oper;
          bool distinct;
          if (QueryRouteTo::kToMySQL == OperationUnmysterify(aggregation, oper, distinct, true))
            return WrapStatus::FAILURE;
          AttrID col;
          TabID tab;
          if (IsFieldItem(aggregation->get_arg(0)) && FieldUnmysterify(aggregation, tab, col) &&
              cq->ExistsInTempTable(tab, tmp_table)) {
            // PHYSICAL COLUMN
            at.n = AddColumnForPhysColumn(aggregation->get_arg(0), tmp_table, TabID(), oper, distinct, true);
          } else {
            // EXPRESSION
            ws = WrapMysqlExpression(aggregation->get_arg(0), tmp_table, expr, in_where, false);
            if (ws == WrapStatus::FAILURE)
              return ws;
            at.n = AddColumnForMysqlExpression(expr, tmp_table, aggregation->item_name.ptr(), oper, distinct, true);
          }
        }
        item2varid[it] = VarID(tmp_table.n, at.n);
      } else if (IsFieldItem(it)) {
        AttrID col;
        TabID tab;
        if (!FieldUnmysterify(it, tab, col))
          return WrapStatus::FAILURE;
        if (!cq->ExistsInTempTable(tab, tmp_table)) {
          bool is_group_by;
          TabID params_table = cq->FindSourceOfParameter(tab, tmp_table, is_group_by);
          common::ColOperation oper;
          bool distinct;
          if (QueryRouteTo::kToMySQL == OperationUnmysterify(it, oper, distinct, true))
            return WrapStatus::FAILURE;
          if (is_group_by && !IsParameterFromWhere(params_table)) {
            col.n = AddColumnForPhysColumn(it, params_table, TabID(), oper, distinct, true);
            item2varid[it] = VarID(params_table.n, col.n);
          } else
            item2varid[it] = VarID(tab.n, col.n);
        } else if (aggr_used) {
          common::ColOperation oper;
          bool distinct;
          if (QueryRouteTo::kToMySQL == OperationUnmysterify(it, oper, distinct, true))
            return WrapStatus::FAILURE;
          at.n = AddColumnForPhysColumn(it, tmp_table, TabID(), oper, distinct, true);
          item2varid[it] = VarID(tmp_table.n, at.n);
        } else {
          item2varid[it] = VarID(tab.n, col.n);
        }
      } else
        DEBUG_ASSERT(0);  // unknown item type?
    }
  }
  gc_expressions.push_back(expr = new MysqlExpression(item, item2varid));
  return WrapStatus::SUCCESS;
}

int Query::AddColumnForPhysColumn(Item *item, const TabID &tmp_table, TabID const &base_table,
                                  const common::ColOperation oper, const bool distinct, bool group_by,
                                  const char *alias) {
  std::pair<int, int> vc;
  AttrID col, at;
  TabID tab;
  if (!FieldUnmysterify(item, tab, col))
    return common::NULL_VALUE_32;
  if (tab.n == common::NULL_VALUE_32)
    tab = tmp_table;  // table name not contained in item - must be the result
                      // temp_table

  if (base_table.IsNullID()) {
    DEBUG_ASSERT(cq->ExistsInTempTable(tab, tmp_table));
    if (item->type() == Item_tianmufield::get_tianmuitem_type() &&
        IsAggregationItem(dynamic_cast<Item_tianmufield *>(item)->OriginalItem())) {
      return ((Item_tianmufield *)item)->varID[0].col;
    }
    vc = VirtualColumnAlreadyExists(tmp_table, tab, col);
    if (vc.first == common::NULL_VALUE_32) {
      vc.first = tmp_table.n;
      cq->CreateVirtualColumn(vc.second, tmp_table, tab, col);
      phys2virt.insert(std::make_pair(std::make_pair(tab.n, col.n), vc));
    } else {
      int attr = GetAddColumnId(AttrID(vc.second), tmp_table, oper, distinct);
      if (attr != common::NULL_VALUE_32) {
        if (group_by)  // do not add column - not needed duplicate
          return attr;
        // vc.n = col_to_vc[attr];
      } else if (group_by && oper == common::ColOperation::GROUP_BY &&
                 (attr = GetAddColumnId(AttrID(vc.second), tmp_table, common::ColOperation::LISTING, distinct)) !=
                     common::NULL_VALUE_32) {
        // modify existing column
        CQChangeAddColumnLIST2GROUP_BY(tmp_table, attr);
        return attr;
      } else if (group_by && oper == common::ColOperation::LISTING &&
                 (attr = GetAddColumnId(AttrID(vc.second), tmp_table, common::ColOperation::GROUP_BY, distinct)) !=
                     common::NULL_VALUE_32) {
        // don;t add unnecessary column to select list
        return attr;
      }
    }
  } else {
    std::pair<int, int> phys_vc = VirtualColumnAlreadyExists(base_table, tab, col);
    if (phys_vc.first == common::NULL_VALUE_32) {
      return common::NULL_VALUE_32;
    }
    vc = VirtualColumnAlreadyExists(tmp_table, TabID(phys_vc.first), AttrID(phys_vc.first));
    if (vc.first == common::NULL_VALUE_32) {
      vc.first = tmp_table.n;
      int at_id = field_alias2num[TabIDColAlias(base_table.n, item->item_name.ptr())];
      cq->CreateVirtualColumn(vc.second, tmp_table, TabID(phys_vc.first), AttrID(at_id));
      phys2virt.insert(std::make_pair(std::make_pair(phys_vc.first, at_id), vc));
    } else {
      int attr = GetAddColumnId(AttrID(vc.second), tmp_table, oper, distinct);
      if (attr != common::NULL_VALUE_32) {
        if (group_by)  // do not add column - not needed duplicate
          return attr;
      } else if (group_by && oper == common::ColOperation::GROUP_BY &&
                 (attr = GetAddColumnId(AttrID(vc.second), tmp_table, common::ColOperation::LISTING, distinct)) !=
                     common::NULL_VALUE_32) {
        CQChangeAddColumnLIST2GROUP_BY(tmp_table, attr);
        return attr;
      } else if (group_by && oper == common::ColOperation::LISTING &&
                 (attr = GetAddColumnId(AttrID(vc.second), tmp_table, common::ColOperation::GROUP_BY, distinct)) !=
                     common::NULL_VALUE_32) {
        return attr;
      }
    }
  }

  if (!item->item_name.ptr() && item->type() == Item::SUM_FUNC_ITEM) {
    cq->AddColumn(at, tmp_table, CQTerm(vc.second), oper,
                  group_by ? nullptr : ((Item_field *)(((Item_sum *)item)->get_arg(0)))->item_name.ptr(), distinct);
  } else {
    if (item->type() == Item::SUM_FUNC_ITEM && ((Item_sum *)item)->sum_func() == Item_sum::GROUP_CONCAT_FUNC) {
      // pass the seprator to construct the special instruction
      char *ptr = ((Item_func_group_concat *)item)->get_separator()->c_ptr();
      SI si;
      si.separator.assign(ptr, std::strlen(ptr));
      si.order = ((Item_func_group_concat *)item)->direction();
      cq->AddColumn(at, tmp_table, CQTerm(vc.second), oper, group_by ? nullptr : item->item_name.ptr(), distinct, &si);
    } else {
      cq->AddColumn(at, tmp_table, CQTerm(vc.second), oper, group_by ? nullptr : item->item_name.ptr(), distinct);
    }
  }
  if (!group_by && item->item_name.ptr())
    field_alias2num[TabIDColAlias(tmp_table.n, alias ? alias : item->item_name.ptr())] = at.n;
  return at.n;
}

int Query::AddColumnForMysqlExpression(MysqlExpression *mysql_expression, const TabID &tmp_table, const char *alias,
                                       const common::ColOperation oper, const bool distinct,
                                       bool group_by /*= false*/) {
  AttrID at, vc;
  vc.n = VirtualColumnAlreadyExists(tmp_table, mysql_expression);
  if (vc.n == common::NULL_VALUE_32) {
    cq->CreateVirtualColumn(vc, tmp_table, mysql_expression,
                            (oper == common::ColOperation::DELAYED ? tmp_table : TabID(common::NULL_VALUE_32)));
    tab_id2expression.insert(std::make_pair(tmp_table, std::make_pair(vc.n, mysql_expression)));
  } else {
    mysql_expression->RemoveUnusedVarID();
    int attr = GetAddColumnId(vc, tmp_table, oper, distinct);
    if (attr != common::NULL_VALUE_32) {
      if (group_by)  // do not add column - not needed duplicate
        return attr;
      // vc.n = col_to_vc[attr];
    } else if (group_by && oper == common::ColOperation::GROUP_BY &&
               (attr = GetAddColumnId(vc, tmp_table, common::ColOperation::LISTING, distinct)) !=
                   common::NULL_VALUE_32) {
      // modify existing column
      CQChangeAddColumnLIST2GROUP_BY(tmp_table, attr);
      return attr;
    } else if (group_by && oper == common::ColOperation::LISTING &&
               (attr = GetAddColumnId(vc, tmp_table, common::ColOperation::GROUP_BY, distinct)) !=
                   common::NULL_VALUE_32) {
      // don;t add unnecessary column to select list
      return attr;
    }
  }

  // if (parametrized)
  //	cq->AddColumn(at, tmp_table, CQTerm(vc.n), DELAYED, group_by ? nullptr :
  // alias, distinct);
  // else
  cq->AddColumn(at, tmp_table, CQTerm(vc.n), oper, group_by ? nullptr : alias, distinct);
  if (!group_by && alias)
    field_alias2num[TabIDColAlias(tmp_table.n, alias)] = at.n;
  return at.n;
}

bool Query::IsLocalColumn(Item *item, const TabID &tmp_table) {
  DEBUG_ASSERT(IsFieldItem(item) || IsAggregationItem(item));
  AttrID col;
  TabID tab;
  if (!FieldUnmysterify(item, tab, col))
    return false;
  return cq->ExistsInTempTable(tab, tmp_table);
}

QueryRouteTo Query::Compile(CompiledQuery *compiled_query, SELECT_LEX *selects_list, SELECT_LEX *last_distinct,
                            TabID *res_tab, bool ignore_limit, Item *left_expr_for_subselect,
                            common::Operator *oper_for_subselect, bool ignore_minmax, bool for_subq_in_where) {
  MEASURE_FET("Query::Compile(...)");
  // at this point all tables are in RCBase engine, so we can proceed with the
  // query

  /*Item_func
   |
   --Item_int_func  <-  arguments are kept in an array accessible through arguments()
   |
   --Item_bool_func
   |   |
   |   ---Item_cond  <- arguments are kept in a list accessible through argument_list()
   |   |     |
   |   |     ---Item_cond_and   <- when negated OR of negated items is created
   |   |     |
   |   |     ---Item_cond_or    <- when negated AND of negated items is created
   |   |     |
   |   |     ---Item_cond_xor
   |   |
   |   ---Item_equal  <- arguments are kept in a list accessible through argument_list()
   |   |                 + const_item (accessible through get_const() )
   |   |           (multiple equality)
   |   |
   |   ---Item_func_not
   |   |            (???)
   |   |
   |   ---Item func_isnull     <- when negated IS NOT NULL is created
   |
   --Item_func_opt_neg  <-  arguments are kept in an array accessible through arguments(), if negated
   |   |                     this information is kept additionally (in a field named 'negated')
   |   |
   |   |
   |   ---Item_func_in
   |   |
   |   |
   |   ---Item_func_between
   |
   |
   --Item_bool_func2
   |
   |
   ---Item_bool_rowready_func2  <-arguments are kept in an array accessible through arguments(), if negated
   |                          an object of a corresponding class is created
   |                           (e.q. ~Item_func_lt => Item_func_ge)
   |
   ----Item_func_eq
   |
   |
   ----Item_func_ne
   |
   |
   ----Item_func_ge
   |
   |
   ----Item_func_le
   |
   |
   ----Item_func_gt
   |
   |
   ----Item_func_lt
   |
   |
   ----Item_func_equal   <- This is mystery so far
   There are 3 equality functions:
   Item_equal -> multiple equality (many fields and optional additional constant value)
   Item_func_equal -> ???
   Item_func_eq -> pairwise equality
   */

  bool union_all = (last_distinct == nullptr);
  TabID prev_result;

  SQL_I_List<ORDER> *global_order = nullptr;
  int col_count = 0;
  int64_t global_limit_value = -1;
  int64_t global_offset_value = -1;

  // local copy of current cq, to be restored on exit
  CompiledQuery *saved_cq = cq;
  cq = compiled_query;

  if ((selects_list->join) &&
      (selects_list != selects_list->join->unit->global_parameters())) {  // only in case of unions this is set
    SetLimit(selects_list->join->unit->global_parameters(), 0, global_offset_value, (int64_t &)global_limit_value);
    global_order = &(selects_list->join->unit->global_parameters()->order_list);
  }

  for (SELECT_LEX *sl = selects_list; sl; sl = sl->next_select()) {
    int64_t limit_value = -1;
    int64_t offset_value = -1;
    /*
      Increase the identification of whether to create a JOIN object,
      which is used to release the JOIN object later.
      See #669 for the problems solved.
    */
    bool ifNewJoinForTianmu = false;
    if (!sl->join) {
      sl->add_active_options(SELECT_NO_UNLOCK);
      JOIN *join = new JOIN(sl->master_unit()->thd, sl);

      if (!join) {
        sl->cleanup(0);
        return QueryRouteTo::kToTianmu;
      }
      ifNewJoinForTianmu = true;
      sl->set_join(join);
    }

    if (QueryRouteTo::kToMySQL == JudgeErrors(sl))
      return QueryRouteTo::kToMySQL;

    SetLimit(sl, sl == selects_list ? 0 : sl->join->unit->global_parameters(), offset_value, limit_value);
    List<Item> *fields = &sl->fields_list;

    Item *conds = (ifNewJoinForTianmu || !sl->join->where_cond) ? sl->where_cond() : sl->join->where_cond;

    ORDER *order = sl->order_list.first;

    // if (order) global_order = 0;   //we want to zero global order (which
    // seems to be always present) if we find a local order by clause
    //  The above is not necessary since global_order is set only in case of
    //  real UNIONs

    ORDER *group = sl->group_list.first;
    Item *having = sl->having_cond();
    List<TABLE_LIST> *join_list = sl->join_list;
    bool zero_result = sl->join->zero_result_cause != nullptr;

    // The exists subquery determines whether a value exists during the query optimization phase
    // result is not set to zero only when a matching value is found in the query optimization phase
    // When a field has an index, the optimization phase scans the table through the index
    // The primary key implementation of the current column storage engine
    // has a problem with the primary key index to scan the table for data
    // Remove the following temporary practices after primary key indexing is complete
    if (zero_result) {
      if (Item::Type::SUBSELECT_ITEM == (conds->type())) {
        zero_result = false;
      } else {
        Item_cond *item_cond = dynamic_cast<Item_cond *>(conds);
        if (item_cond) {
          List_iterator_fast<Item> li(*item_cond->argument_list());
          Item *item;
          while ((item = li++)) {
            if (item && Item::Type::SUBSELECT_ITEM == (item->type())) {
              zero_result = false;
              break;
            }
          }
        }
      }
    }

    // When join_list has no elements and field has sp, tmp table is used and de-duplicated
    // Use use_tmp_when_no_join when AddJoins
    bool use_tmp_when_no_join = false;
    if (!join_list->elements) {
      List_iterator_fast<Item> li(*fields);
      for (Item *item = li++; item; item = li++) {
        if ((item->type() == Item::Type::FUNC_ITEM) &&
            (down_cast<Item_func *>(item)->functype() == Item_func::Functype::FUNC_SP) && (!sl->is_distinct())) {
          sl->add_active_options(SELECT_DISTINCT);
          sl->join->select_distinct = TRUE;
          use_tmp_when_no_join = true;
          break;
        }
      }
    }

    Item *field_for_subselect;
    Item *cond_to_reinsert = nullptr;
    List<Item> *list_to_reinsert = nullptr;

    TabID tmp_table;
    try {
      // partial optimization of LOJ conditions, JOIN::optimize(part=3)
      // necessary due to already done basic transformation of conditions
      // see comments in sql_select.cc:JOIN::optimize()
      if (IsLOJ(join_list))
        sl->join->optimize(OptimizePhase::Finish_LOJ_Transform);

      if (left_expr_for_subselect)
        if (!ClearSubselectTransformation(*oper_for_subselect, field_for_subselect, conds, having, cond_to_reinsert,
                                          list_to_reinsert, left_expr_for_subselect))
          throw CompilationError();

      if (having && !group)  // we cannot handle the case of a having without a group by
        throw CompilationError();

      // handle table list
      TABLE_LIST *tables = sl->leaf_tables ? sl->leaf_tables : (TABLE_LIST *)sl->table_list.first;
      for (TABLE_LIST *table_ptr = tables; table_ptr; table_ptr = table_ptr->next_leaf) {
        if (!table_ptr->is_view_or_derived()) {
          if (!Engine::IsTianmuTable(table_ptr->table))
            throw CompilationError();
          std::string path = TablePath(table_ptr);
          if (path2num.find(path) == path2num.end()) {
            path2num[path] = NumOfTabs();
            AddTable(m_conn->GetTableByPath(path));
            TIANMU_LOG(LogCtl_Level::DEBUG, "add query table: %s", path.c_str());
          }
        }
      }

      // handle join & join cond
      std::vector<TabID> left_tables, right_tables;
      bool first_table = true;
      if (QueryRouteTo::kToMySQL == AddJoins(*join_list, tmp_table, left_tables, right_tables,
                                             (res_tab != nullptr && res_tab->n != 0), first_table, for_subq_in_where,
                                             use_tmp_when_no_join))
        throw CompilationError();

      // handle fields
      List<Item> field_list_for_subselect;
      if (left_expr_for_subselect && field_for_subselect) {
        field_list_for_subselect.push_back(field_for_subselect);
        fields = &field_list_for_subselect;
      }
      bool aggr_used = false;
      if (sl->has_sj_nests && group != nullptr) {
        // handle semi-join fields (use on group by)
        if (QueryRouteTo::kToMySQL == AddSemiJoinFiled(*fields, *join_list, tmp_table))
          throw CompilationError();
      } else {
        // handle normal fields
        if (QueryRouteTo::kToMySQL ==
            AddFields(*fields, tmp_table, TabID(), group != nullptr, col_count, ignore_minmax, aggr_used))
          throw CompilationError();
        if (QueryRouteTo::kToMySQL == AddGroupByFields(group, tmp_table, TabID()))
          throw CompilationError();
        bool group_by_clause = group != nullptr || sl->join->select_distinct || aggr_used || sl->has_sj_nests;
        if (QueryRouteTo::kToMySQL == AddOrderByFields(order, tmp_table, TabID(), group_by_clause))
          throw CompilationError();
      }

      // handle where cond
      CondID cond_id;
      if (QueryRouteTo::kToMySQL == BuildConditions(conds, cond_id, cq, tmp_table, CondType::WHERE_COND, zero_result))
        throw CompilationError();

      cq->AddConds(tmp_table, cond_id, CondType::WHERE_COND);

      // handle having cond
      cond_id = CondID();
      if (QueryRouteTo::kToMySQL == BuildConditions(having, cond_id, cq, tmp_table, CondType::HAVING_COND))
        throw CompilationError();

      cq->AddConds(tmp_table, cond_id, CondType::HAVING_COND);
      cq->ApplyConds(tmp_table);

      // handle group by & order by after semi-join
      if (sl->has_sj_nests) {
        if (group != nullptr) {
          cq->Mode(tmp_table, TMParameter::TM_DISTINCT);
          TabID new_tmp_table;
          cq->TmpTable(new_tmp_table, tmp_table, false);
          if (QueryRouteTo::kToMySQL ==
              AddFields(*fields, new_tmp_table, tmp_table, group != nullptr, col_count, ignore_minmax, aggr_used))
            throw CompilationError();
          if (QueryRouteTo::kToMySQL == AddGroupByFields(group, new_tmp_table, tmp_table))
            throw CompilationError();
          if (QueryRouteTo::kToMySQL == AddOrderByFields(order, new_tmp_table, tmp_table,
                                                         group != nullptr || sl->join->select_distinct || aggr_used))
            throw CompilationError();
          tmp_table = new_tmp_table;
        } else {
          cq->Mode(tmp_table, TMParameter::TM_DISTINCT);
        }
      }
    } catch (...) {
      // restore original values of class fields (necessary if this method is
      // called recursively)
      cq = saved_cq;
      if (cond_to_reinsert && list_to_reinsert)
        list_to_reinsert->push_back(cond_to_reinsert);
      if (ifNewJoinForTianmu)
        sl->cleanup(true);
      return QueryRouteTo::kToMySQL;
    }

    if (sl->join->select_distinct)
      cq->Mode(tmp_table, TMParameter::TM_DISTINCT);
    if (!ignore_limit && limit_value >= 0 && !sl->has_sj_nests)
      cq->Mode(tmp_table, TMParameter::TM_TOP, offset_value, limit_value);

    if (sl == selects_list) {
      prev_result = tmp_table;
      if (global_order && !selects_list->next_select()) {  // trivial union with one select and
                                                           // ext. order by
        tmp_table = TabID();
        cq->Union(prev_result, prev_result, tmp_table, true);
      }
    } else
      cq->Union(prev_result, prev_result, tmp_table, union_all);
    if (sl == last_distinct)
      union_all = true;
    if (cond_to_reinsert && list_to_reinsert)
      list_to_reinsert->push_back(cond_to_reinsert);
    if (ifNewJoinForTianmu)
      sl->cleanup(true);
  }

  cq->BuildTableIDStepsMap();

  if (QueryRouteTo::kToMySQL == AddGlobalOrderByFields(global_order, prev_result, col_count))
    return QueryRouteTo::kToMySQL;

  if (!ignore_limit && global_limit_value >= 0)
    cq->Mode(prev_result, TMParameter::TM_TOP, global_offset_value, global_limit_value);

  if (res_tab != nullptr)
    *res_tab = prev_result;
  else
    cq->Result(prev_result);
  cq = saved_cq;
  return QueryRouteTo::kToTianmu;
}

JoinType Query::GetJoinTypeAndCheckExpr(uint outer_join, Item *on_expr) {
  if (outer_join)
    ASSERT(on_expr != 0, "on_expr shouldn't be null when outer_join != 0");

  JoinType join_type;

  if ((outer_join & JOIN_TYPE_LEFT) && (outer_join & JOIN_TYPE_RIGHT))
    join_type = JoinType::JO_FULL;
  else if (outer_join & JOIN_TYPE_LEFT)
    join_type = JoinType::JO_LEFT;
  else if (outer_join & JOIN_TYPE_RIGHT)
    join_type = JoinType::JO_RIGHT;
  else
    join_type = JoinType::JO_INNER;

  return join_type;
}

bool Query::IsLOJ(List<TABLE_LIST> *join) {
  TABLE_LIST *join_ptr{nullptr};
  List_iterator<TABLE_LIST> li(*join);
  while ((join_ptr = li++)) {
    JoinType join_type = GetJoinTypeAndCheckExpr(join_ptr->outer_join, join_ptr->join_cond());
    if (join_ptr->join_cond() && (join_type == JoinType::JO_LEFT || join_type == JoinType::JO_RIGHT))
      return true;
  }
  return false;
}
}  // namespace core
}  // namespace Tianmu
