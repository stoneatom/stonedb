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

#include "query.h"

#include "common/common_definitions.h"
#include "compiled_query.h"
#include "core/compilation_tools.h"
#include "core/cq_term.h"
#include "core/engine.h"
#include "core/mysql_expression.h"
#include "core/parameterized_filter.h"
#include "core/rough_multi_index.h"
#include "core/temp_table.h"
#include "core/transaction.h"
#include "core/value_set.h"
#include "vc/const_column.h"
#include "vc/const_expr_column.h"
#include "vc/expr_column.h"
#include "vc/in_set_column.h"
#include "vc/single_column.h"
#include "vc/subselect_column.h"
#include "vc/type_cast_column.h"

namespace stonedb {
namespace core {
Query::~Query() {
  for (auto it : gc_expressions) delete it;
}

void Query::RemoveFromManagedList(const std::shared_ptr<RCTable> tab) {
  ta.erase(std::remove(ta.begin(), ta.end(), tab), ta.end());
}

void Query::LockPackInfoForUse() {
  for (auto &it : t) it->LockPackInfoForUse();
};

void Query::UnlockPackInfoFromUse() {
  for (auto &it : t) it->UnlockPackInfoFromUse();
};

bool Query::IsCountStar(Item *item_sum) {
  Item_sum_count *is = dynamic_cast<Item_sum_count *>(item_sum);
  if (is)
    return (is->sum_func() == Item_sum::COUNT_FUNC) &&
           ((is->get_arg(0)->type() == Item::INT_ITEM) || is->const_item() ||
            ((is->get_arg(0)->type() == Item::FUNC_ITEM) && (is->get_arg(0)->const_item())));
  else
    return false;
}

bool Query::IsAggregationItem(Item *item) { return item->type() == Item::SUM_FUNC_ITEM; }

bool Query::IsDeterministic(Item *item) {
  switch (static_cast<int>(item->type())) {
    case Item::FUNC_ITEM: {
      Item_func *ifunc = static_cast<Item_func *>(item);

      if ((dynamic_cast<Item_func_rand *>(ifunc) || dynamic_cast<Item_func_last_insert_id *>(ifunc) ||
                 dynamic_cast<Item_func_get_system_var *>(ifunc) || dynamic_cast<Item_func_is_free_lock *>(ifunc) ||
                 dynamic_cast<Item_func_is_used_lock *>(ifunc) || dynamic_cast<Item_func_row_count *>(ifunc) ||
                 dynamic_cast<Item_func_sp *>(ifunc)
                 /*  // disputable functions start here - should they be nondeterministic?
                                                        || dynamic_cast<Item_func_weekday*>(ifunc)
                                                        || dynamic_cast<Item_func_unix_timestamp*>(ifunc)
                                                        || dynamic_cast<Item_func_time_to_sec*>(ifunc) ||dynamic_cast<Item_date*>(ifunc)
                                                        || dynamic_cast<Item_func_curtime*>(ifunc) ||dynamic_cast<Item_func_now*>(ifunc)
                                                        || dynamic_cast<Item_func_microsecond*>(ifunc) ||dynamic_cast<Item_func_last_day*>(ifunc)
                     */  // end of disputable functions
                 ))
        return false;

      bool is_determ = true;
      for (uint i = 0; i < ifunc->argument_count(); i++)
        is_determ = is_determ && IsDeterministic(ifunc->arguments()[i]);
      return is_determ;
    }

    case Item::COND_ITEM: {
      Item_cond *cond = static_cast<Item_cond *>(item);
      List_iterator<Item> li(*cond->argument_list());
      Item *arg;
      bool is_determ = true;
      while ((arg = li++)) is_determ = is_determ && IsDeterministic(arg);
      return is_determ;
    }

    case Item::REF_ITEM: {
      Item_ref *iref = dynamic_cast<Item_ref *>(item);
      Item *arg = *(iref->ref);
      return IsDeterministic(arg);
    }

    default:
      return true;
  }
}

bool Query::HasAggregation(Item *item) {
  bool has = false;
  Item *i = UnRef(item);
  if (i->type() == Item::SUM_FUNC_ITEM)
    has = true;
  else if (i->type() == Item::FUNC_ITEM) {
    Item_func *f = static_cast<Item_func *>(i);
    int const arg_count(f->argument_count());
    for (int arg = 0; (arg < arg_count) && !has; ++arg) has = HasAggregation(f->arguments()[arg]);
  } else if (i->type() == Item::COND_ITEM) {
    Item_cond *c = static_cast<Item_cond *>(i);
    List<Item> *args = c->argument_list();
    List_iterator_fast<Item> li(*args);
    Item *item;
    while (!has && (item = li++)) has = HasAggregation(item);
  } else if (i->type() == Item_sdbfield::get_sdbitem_type())
    if (static_cast<Item_sdbfield *>(i)->IsAggregation()) has = true;
  return has;
}

int Query::VirtualColumnAlreadyExists(const TabID &tmp_table, MysqlExpression *expression) {
  int exists = common::NULL_VALUE_32;
  for (auto it = tab_id2expression.lower_bound(tmp_table), end = tab_id2expression.upper_bound(tmp_table); it != end;
       ++it) {
    if (*(it->second.second) == *expression) {
      exists = it->second.first;
      break;
    }
  }
  return exists;
}

int Query::VirtualColumnAlreadyExists(const TabID &tmp_table, const TabID &subselect) {
  int exists = common::NULL_VALUE_32;
  for (auto it = tab_id2subselect.lower_bound(tmp_table), end = tab_id2subselect.upper_bound(tmp_table); it != end;
       ++it) {
    if (it->second.second == subselect) {
      exists = it->second.first;
      break;
    }
  }
  return exists;
}

int Query::VirtualColumnAlreadyExists(const TabID &tmp_table, const std::vector<int> &vcs, const AttrID &at) {
  int exists = common::NULL_VALUE_32;
  for (auto it = tab_id2inset.lower_bound(tmp_table), end = tab_id2inset.upper_bound(tmp_table); it != end; ++it) {
    if (it->second.second.second.n == at.n) {
      std::set<int> s1, s2;
      s1.insert(it->second.second.first.begin(), it->second.second.first.end());
      s2.insert(vcs.begin(), vcs.end());
      if (s1 == s2) {
        exists = it->second.first;
        break;
      }
    }
  }
  return exists;
}

std::pair<int, int> Query::VirtualColumnAlreadyExists(const TabID &tmp_table, const TabID &tab, const AttrID &at) {
  for (auto it = phys2virt.lower_bound(std::make_pair(tab.n, at.n)),
            end = phys2virt.upper_bound(std::make_pair(tab.n, at.n));
       it != end; ++it) {
    if (it->second.first == tmp_table.n) {
      return it->second;
    }
  }
  return std::make_pair(common::NULL_VALUE_32, common::NULL_VALUE_32);
}

bool Query::IsFieldItem(Item *item) {
  return (item->type() == Item::FIELD_ITEM || item->type() == Item_sdbfield::get_sdbitem_type());
}

bool Query::IsAggregationOverFieldItem(Item *item) {
  return IsAggregationItem(item) && IsFieldItem(((Item_sum *)item)->get_arg(0));
}

bool Query::IsConstItem(Item *item) {
  bool res = item->type() == Item::INT_ITEM || item->type() == Item::NULL_ITEM || item->type() == Item::INT_ITEM ||
             item->type() == Item::VARBIN_ITEM || item->type() == Item::STRING_ITEM ||
             item->type() == Item::DECIMAL_ITEM || item->type() == Item::REAL_ITEM;
  return res;
}

const std::string Query::GetItemName(Item *item) {
  //  static const char* nameOf[] = {
  //          "FIELD_ITEM", "FUNC_ITEM", "SUM_FUNC_ITEM", "STRING_ITEM",
  //          "INT_ITEM", "REAL_ITEM", "NULL_ITEM", "VARBIN_ITEM",
  //          "COPY_STR_ITEM", "FIELD_AVG_ITEM", "DEFAULT_VALUE_ITEM",
  //          "PROC_ITEM", "COND_ITEM", "REF_ITEM", "FIELD_STD_ITEM",
  //          "FIELD_VARIANCE_ITEM", "INSERT_VALUE_ITEM", "SUBSELECT_ITEM",
  //          "ROW_ITEM", "CACHE_ITEM", "TYPE_HOLDER", "PARAM_ITEM",
  //          "TRIGGER_FIELD_ITEM", "DECIMAL_ITEM", "XPATH_NODESET",
  //          "XPATH_NODESET_CMP", "VIEW_FIXER_ITEM" };
  static const char *sumNameOf[] = {"COUNT_FUNC",       "COUNT_DISTINCT_FUNC", "SUM_FUNC",     "SUM_DISTINCT_FUNC",
                                    "AVG_FUNC",         "AVG_DISTINCT_FUNC",   "MIN_FUNC",     "MAX_FUNC",
                                    "STD_FUNC",         "VARIANCE_FUNC",       "SUM_BIT_FUNC", "UDF_SUM_FUNC",
                                    "GROUP_CONCAT_FUNC"};
  char buf[256] = {0};
  switch (static_cast<int>(item->type())) {
    case Item::FUNC_ITEM: {
      Item_func *func = static_cast<Item_func *>(item);
      return func->func_name();
    }
    case Item::COND_ITEM: {
      Item_cond *cond = static_cast<Item_cond *>(item);
      return cond->func_name();
    }
    case Item::SUM_FUNC_ITEM: {
      Item_sum *sum_func = static_cast<Item_sum *>(item);
      uint index = sum_func->sum_func();
      if (index >= sizeof(sumNameOf) / sizeof(*sumNameOf))
        return "UNKNOWN SUM_FUNC_ITEM";
      else
        return sumNameOf[index];
    }
    case Item::REF_ITEM: {
      Item_ref *ref = static_cast<Item_ref *>(item);
      Item *real = ref->real_item();
      if (ref != real) return GetItemName(real);
      return "REF_ITEM";
    }
    case Item::NULL_ITEM:
      return "NULL";
    case Item::INT_ITEM: {
      Item_int_with_ref *int_ref = dynamic_cast<Item_int_with_ref *>(item);
      String s(buf, 256, NULL);
      if (!int_ref) {
        String *ps = item->val_str(&s);
        return ps ? ps->c_ptr_safe() : "NULL";
      }
      // else item is an instance of Item_int_with_ref, not Item_int
      return GetItemName(int_ref->real_item());
    }
    case Item::STRING_ITEM: {
      String s(buf, 256, NULL);
      String *ps = item->val_str(&s);
      return ps ? ps->c_ptr_safe() : "NULL";
    }
    case Item::SUBSELECT_ITEM:
      return "SUBSELECT";
    case Item::REAL_ITEM:
      return "REAL";
    case Item::DECIMAL_ITEM:
      return "DECIMAL";
    case static_cast<int>(Item_sdbfield::enumSDBFiledItem::SDBFIELD_ITEM):
      Item_sdbfield *sdb = static_cast<Item_sdbfield *>(item);
      size_t cur_var_id = sdbitems_cur_var_ids[sdb]++;
      if (cur_var_id >= sdb->varID.size()) cur_var_id = 0;
      std::sprintf(buf, "SDB_FIELD(T:%d,A:%d)", sdb->varID[cur_var_id].tab, sdb->varID[cur_var_id].col);
      return buf;
  }
  return "UNKNOWN";
}

int Query::GetAddColumnId(const AttrID &vc, const TabID &tmp_table, const common::ColOperation oper,
                          const bool distinct) {
  for (int i = 0; i < cq->NoSteps(); i++) {
    CompiledQuery::CQStep *step = &cq->Step(i);
    if (step->type == CompiledQuery::StepType::ADD_COLUMN && step->t1 == tmp_table && step->e1.vc_id == vc.n &&
        step->cop == oper && step->n1 == (distinct ? 1 : 0)) {
      return step->a1.n;
    }
  }
  return common::NULL_VALUE_32;
}

void Query::CQChangeAddColumnLIST2GROUP_BY(const TabID &tmp_table, int attr) {
  for (int i = 0; i < cq->NoSteps(); i++) {
    CompiledQuery::CQStep *step = &cq->Step(i);
    if (step->type == CompiledQuery::StepType::ADD_COLUMN && step->t1 == tmp_table && step->a1.n == attr &&
        step->cop == common::ColOperation::LISTING) {
      step->cop = common::ColOperation::GROUP_BY;
      cq->AddGroupByStep(*step);
      return;
    }
  }
}

void Query::MarkWithAny(common::Operator &op) {
  switch (op) {
    case common::Operator::O_EQ:
      op = common::Operator::O_EQ_ANY;
      break;
    case common::Operator::O_NOT_EQ:
      op = common::Operator::O_NOT_EQ_ANY;
      break;
    case common::Operator::O_LESS:
      op = common::Operator::O_LESS_ANY;
      break;
    case common::Operator::O_MORE:
      op = common::Operator::O_MORE_ANY;
      break;
    case common::Operator::O_LESS_EQ:
      op = common::Operator::O_LESS_EQ_ANY;
      break;
    case common::Operator::O_MORE_EQ:
      op = common::Operator::O_MORE_EQ_ANY;
      break;
    default:
      // ANY can't be added to any other operator
      break;
  }
}

void Query::MarkWithAll(common::Operator &op) {
  switch (op) {
    case common::Operator::O_EQ:
      op = common::Operator::O_EQ_ALL;
      break;
    case common::Operator::O_NOT_EQ:
      op = common::Operator::O_NOT_EQ_ALL;
      break;
    case common::Operator::O_LESS:
      op = common::Operator::O_LESS_ALL;
      break;
    case common::Operator::O_MORE:
      op = common::Operator::O_MORE_ALL;
      break;
    case common::Operator::O_LESS_EQ:
      op = common::Operator::O_LESS_EQ_ALL;
      break;
    case common::Operator::O_MORE_EQ:
      op = common::Operator::O_MORE_EQ_ALL;
      break;
    default:
      // ALL can't be added to any other operator
      break;
  }
}

bool Query::IsAllAny(common::Operator &op) {
  return (op == common::Operator::O_EQ_ALL || op == common::Operator::O_EQ_ANY ||
          op == common::Operator::O_NOT_EQ_ALL || op == common::Operator::O_NOT_EQ_ANY ||
          op == common::Operator::O_LESS_ALL || op == common::Operator::O_LESS_ANY ||
          op == common::Operator::O_MORE_ALL || op == common::Operator::O_MORE_ANY ||
          op == common::Operator::O_LESS_EQ_ALL || op == common::Operator::O_LESS_EQ_ANY ||
          op == common::Operator::O_MORE_EQ_ALL || op == common::Operator::O_MORE_EQ_ANY);
}

void Query::UnmarkAllAny(common::Operator &op) {
  switch (op) {
    case common::Operator::O_EQ_ALL:
    case common::Operator::O_EQ_ANY:
      op = common::Operator::O_EQ;
      break;
    case common::Operator::O_NOT_EQ_ALL:
    case common::Operator::O_NOT_EQ_ANY:
      op = common::Operator::O_NOT_EQ;
      break;
    case common::Operator::O_LESS_ALL:
    case common::Operator::O_LESS_ANY:
      op = common::Operator::O_LESS;
      break;
    case common::Operator::O_MORE_ALL:
    case common::Operator::O_MORE_ANY:
      op = common::Operator::O_MORE;
      break;
    case common::Operator::O_LESS_EQ_ALL:
    case common::Operator::O_LESS_EQ_ANY:
      op = common::Operator::O_LESS_EQ;
      break;
    case common::Operator::O_MORE_EQ_ALL:
    case common::Operator::O_MORE_EQ_ANY:
      op = common::Operator::O_MORE_EQ;
      break;
    default:
      // ALL/ANY can't be removed from any other operator
      break;
  }
}

void Query::ExtractOperatorType(Item *&conds, common::Operator &op, bool &negative, char &like_esc) {
  bool is_there_not;
  like_esc = '\\';
  conds = UnRef(conds);
  conds = FindOutAboutNot(conds, is_there_not);  // it UnRefs the conds if there is need

  Item_func *cond_func = (Item_func *)conds;
  switch (cond_func->functype()) {
    case Item_func::BETWEEN:
      op = is_there_not ? common::Operator::O_NOT_BETWEEN : common::Operator::O_BETWEEN;
      break;
    case Item_func::LIKE_FUNC:
      op = is_there_not ? common::Operator::O_NOT_LIKE : common::Operator::O_LIKE;
      like_esc = ((Item_func_like *)cond_func)->escape;
      break;
    case Item_func::ISNULL_FUNC:
      op = common::Operator::O_IS_NULL;
      break;
    case Item_func::ISNOTNULL_FUNC:
      op = common::Operator::O_NOT_NULL;
      break;
    case Item_func::IN_FUNC:
      op = is_there_not ? common::Operator::O_NOT_IN : common::Operator::O_IN;
      break;
    case Item_func::EQ_FUNC:  // =
      op = negative ? common::Operator::O_NOT_EQ : common::Operator::O_EQ;
      break;
    case Item_func::NE_FUNC:  // <>
      op = negative ? common::Operator::O_EQ : common::Operator::O_NOT_EQ;
      break;
    case Item_func::LE_FUNC:  // <=
      op = negative ? common::Operator::O_MORE : common::Operator::O_LESS_EQ;
      break;
    case Item_func::GE_FUNC:  // >=
      op = negative ? common::Operator::O_LESS : common::Operator::O_MORE_EQ;
      break;
    case Item_func::GT_FUNC:  // >
      op = negative ? common::Operator::O_LESS_EQ : common::Operator::O_MORE;
      break;
    case Item_func::LT_FUNC:  // <
      op = negative ? common::Operator::O_MORE_EQ : common::Operator::O_LESS;
      break;
    case Item_func::MULT_EQUAL_FUNC:
      op = common::Operator::O_MULT_EQUAL_FUNC;
      break;
    case Item_func::NOT_FUNC:
      op = common::Operator::O_NOT_FUNC;
      break;
    case Item_func::NOT_ALL_FUNC: {
      Item_func *cond_func = (Item_func *)conds;
      negative = dynamic_cast<Item_func_nop_all *>(cond_func) == NULL;
      ExtractOperatorType(cond_func->arguments()[0], op, negative, like_esc);
      if (dynamic_cast<Item_func_nop_all *>(cond_func))
        MarkWithAny(op);
      else if (dynamic_cast<Item_func_not_all *>(cond_func))
        MarkWithAll(op);
      conds = cond_func->arguments()[0];
      break;
    }
    case Item_func::UNKNOWN_FUNC:
      op = common::Operator::O_UNKNOWN_FUNC;
      break;
    default:
      op = common::Operator::O_ERROR;  // unknown function type
      break;
  }
}

vcolumn::VirtualColumn *Query::CreateColumnFromExpression(std::vector<MysqlExpression *> const &exprs,
                                                          TempTable *temp_table, int temp_table_alias,
                                                          MultiIndex *mind) {
  DEBUG_ASSERT(exprs.size() > 0);
  vcolumn::VirtualColumn *vc = NULL;
  if (exprs.size() == 1) {
    bool is_const_expr =
        vcolumn::VirtualColumn::IsConstExpression(exprs[0], temp_table_alias, &temp_table->GetAliases());
    if (exprs[0]->IsDeterministic() && (exprs[0]->GetVars().size() == 0)) {
      ColumnType type(exprs[0]->EvalType());
      vc = new vcolumn::ConstColumn(*(exprs[0]->Evaluate()), type, true);
    } else if (is_const_expr && exprs[0]->IsDeterministic()) {
      if (IsFieldItem(exprs[0]->GetItem())) {
        // a special case when a naked column is a parameter
        // without this column type would be a seen by mysql, not STONEDB.
        // e.g. timestamp would be string 19
        TabID tab;
        AttrID col;
        tab.n = exprs[0]->GetVars().begin()->tab;
        col.n = exprs[0]->GetVars().begin()->col;
        col.n = col.n < 0 ? -col.n - 1 : col.n;
        ColumnType ct = ta[-tab.n - 1]->GetColumnType(col.n);
        vc = new vcolumn::ConstExpressionColumn(exprs[0], ct, temp_table, temp_table_alias, mind);
      } else
        vc = new vcolumn::ConstExpressionColumn(exprs[0], temp_table, temp_table_alias, mind);
    } else {
      if (rccontrol.isOn()) {
        if (static_cast<int>(exprs[0]->GetItem()->type()) == Item::FUNC_ITEM) {
          Item_func *ifunc = static_cast<Item_func *>(exprs[0]->GetItem());
          rccontrol.lock(mind->m_conn->GetThreadID())
              << "Unoptimized expression near '" << ifunc->func_name() << "'" << system::unlock;
        }
      }
      vc = new vcolumn::ExpressionColumn(exprs[0], temp_table, temp_table_alias, mind);
      if (static_cast<vcolumn::ExpressionColumn *>(vc)->GetStringType() == MysqlExpression::StringType::STRING_TIME &&
          vc->TypeName() != common::CT::TIME) {  // common::CT::TIME is already as int64_t
        vcolumn::TypeCastColumn *tcc = new vcolumn::String2DateTimeCastColumn(vc, ColumnType(common::CT::TIME));
        temp_table->AddVirtColumn(vc);
        vc = tcc;
      }
    }
  } else {
    DEBUG_ASSERT(0);
  }
  MysqlExpression::SetOfVars params = vc->GetParams();
  MysqlExpression::TypeOfVars types;
  for (auto &iter : params) {
    types[iter] = ta[-iter.tab - 1]->GetColumnType(iter.col < 0 ? -iter.col - 1 : iter.col);
  }
  vc->SetParamTypes(&types);
  return vc;
}

bool Query::IsConstExpr(MysqlExpression::SetOfVars &sv, const TabID &t) {
  bool res = false;
  for (auto &iter : sv) {
    res |= cq->ExistsInTempTable(TabID(iter.tab), t);
  }
  return !res;
}

bool Query::IsParameterFromWhere(const TabID &params_table) {
  for (auto &it : subqueries_in_where) {
    if (it.first == params_table) return it.second;
  }
  DEBUG_ASSERT(!"Subquery not properly placed on compilation stack");
  return true;
}

const char *Query::GetTableName(Item_field *ifield) {
  char *table_name = NULL;
  if (ifield->cached_table && !ifield->cached_table->view && !ifield->cached_table->derived)
    if (ifield->cached_table->referencing_view)
      table_name = ifield->cached_table->referencing_view->table_name;
    else
      table_name = ifield->cached_table->table_name;
  else if (ifield->result_field->table && ifield->result_field->table->s->table_category != TABLE_CATEGORY_TEMPORARY)
    table_name = ifield->result_field->table->s->table_name.str;
  return table_name;
}

void Query::GetPrecisionScale(Item *item, int &precision, int &scale, bool max_scale) {
  precision = item->decimal_precision();
  scale = item->decimals;
  // if(precision > 19) {
  //  int integers = (precision - scale);
  //  precision = 19;
  //  scale = integers > 19 ? 0 :  19 - integers;
  //}
  if (scale > 18) {
    precision -= (scale - 18);
    scale = 18;
  }
  if (precision > 18) precision = 18;

  Item_func *item_func = dynamic_cast<Item_func *>(item);
  if (max_scale && precision < 18 && item_func && std::strcmp(item_func->func_name(), "/") == 0) {
    scale += 18 - precision;
    if (scale > 15) scale = 15;
    precision = 18;
  }
}

TempTable *Query::Preexecute(CompiledQuery &qu, ResultSender *sender, [[maybe_unused]] bool display_now) {
  if (STONEDB_LOGCHECK(LogCtl_Level::DEBUG)) {
    qu.Print(this);
  }
  std::vector<Condition *> conds(qu.NoConds());

  TempTable *output_table = NULL;  // NOTE: this pointer will be returned by the function

  ta.resize(qu.NoTabs());
  auto global_limits = qu.GetGlobalLimit();

  cq = &qu;
  // Execution itself
  for (int i = 0; i < qu.NoSteps(); i++) {
    CompiledQuery::CQStep step = qu.Step(i);
    std::shared_ptr<JustATable> t1_ptr, t2_ptr, t3_ptr;

    if (step.t1.n != common::NULL_VALUE_32) {
      if (step.t1.n >= 0)
        t1_ptr = Table(step.t1.n);  // normal table
      else {
        t1_ptr = ta[-step.t1.n - 1];  // TempTable
      }
    }
    if (step.t2.n != common::NULL_VALUE_32) {
      if (step.t2.n >= 0)
        t2_ptr = Table(step.t2.n);  // normal table
      else {
        t2_ptr = ta[-step.t2.n - 1];  // TempTable
      }
    }
    if (step.t3.n != common::NULL_VALUE_32) {
      if (step.t3.n >= 0)
        t3_ptr = Table(step.t3.n);  // normal table
      else {
        t3_ptr = ta[-step.t3.n - 1];  // TempTable
      }
    }
    // Some technical information
    if (step.alias && std::strcmp(step.alias, "roughstats") == 0) {
      // magical word (passed as table alias) to display statistics
      ((TempTable *)ta[-step.t1.n - 1].get())->DisplayRSI();
    }

    if (step.alias && std::strcmp(step.alias, "roughattrstats") == 0) {
      // magical word (passed as table alias) to display attr. statistics
      m_conn->SetDisplayAttrStats();
    }

    // Implementation of steps
    try {
      switch (step.type) {
        case CompiledQuery::StepType::TABLE_ALIAS:
          ta[-step.t1.n - 1] = t2_ptr;
          break;
        case CompiledQuery::StepType::TMP_TABLE:
          DEBUG_ASSERT(step.t1.n < 0);
          ta[-step.t1.n - 1] = step.n1
                                   ? TempTable::Create(ta[-step.tables1[0].n - 1].get(), step.tables1[0].n, this, true)
                                   : TempTable::Create(ta[-step.tables1[0].n - 1].get(), step.tables1[0].n, this);
          ((TempTable *)ta[-step.t1.n - 1].get())->ReserveVirtColumns(qu.NoVirtualColumns(step.t1));
          break;
        case CompiledQuery::StepType::CREATE_CONDS:
          DEBUG_ASSERT(step.t1.n < 0);
          step.e1.vc = (step.e1.vc_id != common::NULL_VALUE_32)
                           ? ((TempTable *)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e1.vc_id)
                           : NULL;
          step.e2.vc = (step.e2.vc_id != common::NULL_VALUE_32)
                           ? ((TempTable *)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e2.vc_id)
                           : NULL;
          step.e3.vc = (step.e3.vc_id != common::NULL_VALUE_32)
                           ? ((TempTable *)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e3.vc_id)
                           : NULL;
          if (step.n1 != static_cast<int64_t>(CondType::OR_SUBTREE)) {  // on result = false
            conds[step.c1.n] = new Condition();
            if (step.c2.IsNull()) {
              conds[step.c1.n]->AddDescriptor(
                  step.e1, step.op, step.e2, step.e3, (TempTable *)ta[-step.t1.n - 1].get(), qu.GetNoDims(step.t1),
                  (step.op == common::Operator::O_LIKE || step.op == common::Operator::O_NOT_LIKE) ? char(step.n2)
                                                                                                   : '\\');
            } else {
              DEBUG_ASSERT(conds[step.c2.n]->IsType_Tree());
              conds[step.c1.n]->AddDescriptor(static_cast<SingleTreeCondition *>(conds[step.c2.n])->GetTree(),
                                              (TempTable *)ta[-step.t1.n - 1].get(), qu.GetNoDims(step.t1));
            }
          } else {  // on result = true
            if (step.c2.IsNull())
              conds[step.c1.n] =
                  new SingleTreeCondition(step.e1, step.op, step.e2, step.e3, (TempTable *)ta[-step.t1.n - 1].get(),
                                          qu.GetNoDims(step.t1), char(step.n2));
            else {
              DEBUG_ASSERT(conds[step.c2.n]->IsType_Tree());
              conds[step.c1.n] = new Condition();
              conds[step.c1.n]->AddDescriptor(((SingleTreeCondition *)conds[step.c2.n])->GetTree(),
                                              (TempTable *)ta[-step.t1.n - 1].get(), qu.GetNoDims(step.t1));
            }
          }
          break;
        case CompiledQuery::StepType::AND_F:
        case CompiledQuery::StepType::OR_F:
          if (!conds[step.c2.n]->IsType_Tree()) {
            ASSERT(step.type == CompiledQuery::StepType::AND_F);
            auto cond2 = conds[step.c2.n];
            for (size_t i = 0; i < cond2->Size(); i++) {
              auto &desc = (*cond2)[i];
              if (conds[step.c1.n]->IsType_Tree()) {
                TempTable *temptb = (TempTable *)ta[-qu.GetTableOfCond(step.c2).n - 1].get();
                int no_dims = qu.GetNoDims(qu.GetTableOfCond(step.c2));
                if (desc.op == common::Operator::O_OR_TREE) {
                  static_cast<SingleTreeCondition *>(conds[step.c1.n])
                      ->AddTree(common::LogicalOperator::O_AND, desc.tree, no_dims);
                } else {
                  static_cast<SingleTreeCondition *>(conds[step.c1.n])
                      ->AddDescriptor(common::LogicalOperator::O_AND, desc.attr, desc.op, desc.val1, desc.val2, temptb,
                                      no_dims, desc.like_esc);
                }
              } else {
                conds[step.c1.n]->AddDescriptor(desc);
              }
            }
          } else if (conds[step.c1.n]->IsType_Tree()) {  // on result = false
            DEBUG_ASSERT(conds[step.c2.n]->IsType_Tree());
            common::LogicalOperator lop = (step.type == CompiledQuery::StepType::AND_F ? common::LogicalOperator::O_AND
                                                                                       : common::LogicalOperator::O_OR);
            static_cast<SingleTreeCondition *>(conds[step.c1.n])
                ->AddTree(lop, static_cast<SingleTreeCondition *>(conds[step.c2.n])->GetTree(), qu.GetNoDims(step.t1));
          } else {
            DEBUG_ASSERT(conds[step.c2.n]->IsType_Tree());
            conds[step.c1.n]->AddDescriptor(static_cast<SingleTreeCondition *>(conds[step.c2.n])->GetTree(),
                                            (TempTable *)ta[-qu.GetTableOfCond(step.c1).n - 1].get(),
                                            qu.GetNoDims(qu.GetTableOfCond(step.c1)));
          }
          break;
        case CompiledQuery::StepType::OR_DESC:
        case CompiledQuery::StepType::AND_DESC: {
          common::LogicalOperator lop =
              (step.type == CompiledQuery::StepType::AND_DESC ? common::LogicalOperator::O_AND
                                                              : common::LogicalOperator::O_OR);
          step.e1.vc = (step.e1.vc_id != common::NULL_VALUE_32)
                           ? ((TempTable *)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e1.vc_id)
                           : NULL;
          step.e2.vc = (step.e2.vc_id != common::NULL_VALUE_32)
                           ? ((TempTable *)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e2.vc_id)
                           : NULL;
          step.e3.vc = (step.e3.vc_id != common::NULL_VALUE_32)
                           ? ((TempTable *)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e3.vc_id)
                           : NULL;
          if (!conds[step.c1.n]->IsType_Tree()) {
            DEBUG_ASSERT(conds[step.c1.n]);
            conds[step.c1.n]->AddDescriptor(
                step.e1, step.op, step.e2, step.e3, (TempTable *)ta[-step.t1.n - 1].get(), qu.GetNoDims(step.t1),
                (step.op == common::Operator::O_LIKE || step.op == common::Operator::O_NOT_LIKE) ? char(step.n2)
                                                                                                 : '\\');
          } else
            static_cast<SingleTreeCondition *>(conds[step.c1.n])
                ->AddDescriptor(lop, step.e1, step.op, step.e2, step.e3, (TempTable *)ta[-step.t1.n - 1].get(),
                                qu.GetNoDims(step.t1),
                                (step.op == common::Operator::O_LIKE || step.op == common::Operator::O_NOT_LIKE)
                                    ? char(step.n2)
                                    : '\\');
          break;
        }
        case CompiledQuery::StepType::T_MODE:
          DEBUG_ASSERT(step.t1.n < 0 && ta[-step.t1.n - 1]->TableType() == TType::TEMP_TABLE);
          ((TempTable *)ta[-step.t1.n - 1].get())->SetMode(step.tmpar, step.n1, step.n2);
          break;
        case CompiledQuery::StepType::JOIN_T:
          DEBUG_ASSERT(step.t1.n < 0 && ta[-step.t1.n - 1]->TableType() == TType::TEMP_TABLE);
          ((TempTable *)ta[-step.t1.n - 1].get())->JoinT(t2_ptr.get(), step.t2.n, step.jt);
          break;
        case CompiledQuery::StepType::ADD_CONDS: {
          DEBUG_ASSERT(step.t1.n < 0 && ta[-step.t1.n - 1]->TableType() == TType::TEMP_TABLE);
          if (step.c1.n == common::NULL_VALUE_32) break;
          if (step.n1 != static_cast<int64_t>(CondType::HAVING_COND)) conds[step.c1.n]->Simplify();
          ((TempTable *)ta[-step.t1.n - 1].get())->AddConds(conds[step.c1.n], (CondType)step.n1);
          break;
        }
        case CompiledQuery::StepType::LEFT_JOIN_ON: {
          DEBUG_ASSERT(step.t1.n < 0 && ta[-step.t1.n - 1]->TableType() == TType::TEMP_TABLE);
          if (step.c1.n == common::NULL_VALUE_32) break;
          ((TempTable *)ta[-step.t1.n - 1].get())->AddLeftConds(conds[step.c1.n], step.tables1, step.tables2);
          break;
        }
        case CompiledQuery::StepType::INNER_JOIN_ON: {
          DEBUG_ASSERT(step.t1.n < 0 && ta[-step.t1.n - 1]->TableType() == TType::TEMP_TABLE);
          if (step.c1.n == common::NULL_VALUE_32) break;
          ((TempTable *)ta[-step.t1.n - 1].get())->AddInnerConds(conds[step.c1.n], step.tables1);
          break;
        }
        case CompiledQuery::StepType::APPLY_CONDS: {
          int64_t cur_limit = -1;
          if (qu.FindDistinct(step.t1.n))
            ((TempTable *)ta[-step.t1.n - 1].get())->SetMode(TMParameter::TM_DISTINCT, 0, 0);
          if (qu.NoAggregationOrderingAndDistinct(step.t1.n)) cur_limit = qu.FindLimit(step.t1.n);

          if (cur_limit != -1 && ((TempTable *)ta[-step.t1.n - 1].get())->GetFilterP()->NoParameterizedDescs())
            cur_limit = -1;

          ParameterizedFilter *filter = ((TempTable *)ta[-step.t1.n - 1].get())->GetFilterP();
          std::set<int> used_dims = qu.GetUsedDims(step.t1, ta);

          // no need any more to check WHERE for not used dims
          bool is_simple_filter = true;  // qu.IsSimpleFilter(step.c1);
          if (used_dims.size() == 1 && used_dims.find(common::NULL_VALUE_32) != used_dims.end())
            is_simple_filter = false;
          for (int i = 0; i < filter->mind->NoDimensions(); i++) {
            if (used_dims.find(i) == used_dims.end() && is_simple_filter)
              filter->mind->ResetUsedInOutput(i);
            else
              filter->mind->SetUsedInOutput(i);
          }

          if (IsRoughQuery()) {
            ((TempTable *)ta[-step.t1.n - 1].get())->GetFilterP()->RoughUpdateParamFilter();
          } else
            ((TempTable *)ta[-step.t1.n - 1].get())
                ->GetFilterP()
                ->UpdateMultiIndex(qu.CountColumnOnly(step.t1), cur_limit);
          break;
        }
        case CompiledQuery::StepType::ADD_COLUMN: {
          DEBUG_ASSERT(step.t1.n < 0 && ta[-step.t1.n - 1]->TableType() == TType::TEMP_TABLE);
          CQTerm e(step.e1);
          if (e.vc_id != common::NULL_VALUE_32)
            e.vc =
                ((TempTable *)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e1.vc_id);  // vc must have been created
          step.a1.n = ((TempTable *)ta[-step.t1.n - 1].get())
                          ->AddColumn(e, step.cop, step.alias, step.n1 ? true : false, step.si);
          break;
        }
        case CompiledQuery::StepType::CREATE_VC: {
          DEBUG_ASSERT(step.t1.n < 0 && ta[-step.t1.n - 1]->TableType() == TType::TEMP_TABLE);
          TempTable *t = (TempTable *)ta[-step.t1.n - 1].get();

          DEBUG_ASSERT(t);
          if (step.mysql_expr.size() > 0) {
            // vcolumn::VirtualColumn for Expression
            DEBUG_ASSERT(step.mysql_expr.size() == 1);
            MultiIndex *mind = (step.t2.n == step.t1.n) ? t->GetOutputMultiIndexP() : t->GetMultiIndexP();
            int c = ((TempTable *)ta[-step.t1.n - 1].get())
                        ->AddVirtColumn(CreateColumnFromExpression(step.mysql_expr, t, step.t1.n, mind), step.a1.n);
            ASSERT(c == step.a1.n, "AddVirtColumn failed");
          } else if (step.virt_cols.size() > 0) {
            // vcolumn::VirtualColumn for IN
            ColumnType ct;
            if (step.a2.n != common::NULL_VALUE_32)
              ct = ((TempTable *)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.a2.n)->Type();
            std::vector<vcolumn::VirtualColumn *> vcs;
            for (uint i = 0; i < step.virt_cols.size(); i++)
              vcs.push_back(((TempTable *)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.virt_cols[i]));
            int c = ((TempTable *)ta[-step.t1.n - 1].get())
                        ->AddVirtColumn(new vcolumn::InSetColumn(ct, t->GetMultiIndexP(), vcs), step.a1.n);
            ASSERT(c == step.a1.n, "AddVirtColumn failed");
          } else if (step.a2.n != common::NULL_VALUE_32) {
            // vcolumn::VirtualColumn for PhysicalColumn
            JustATable *t_src = ta[-step.t2.n - 1].get();
            PhysicalColumn *phc;
            MultiIndex *mind = (step.t2.n == step.t1.n) ? t->GetOutputMultiIndexP() : t->GetMultiIndexP();
            int dim = (step.t2.n == step.t1.n) ? 0 : t->GetDimension(step.t2);
            phc = (PhysicalColumn *)t_src->GetColumn(step.a2.n >= 0 ? step.a2.n : -step.a2.n - 1);
            int c = ((TempTable *)ta[-step.t1.n - 1].get())
                        ->AddVirtColumn(
                            new vcolumn::SingleColumn(phc, mind, step.t2.n, step.a2.n, ta[-step.t2.n - 1].get(), dim),
                            step.a1.n);
            ASSERT(c == step.a1.n, "AddVirtColumn failed");
          } else {
            // vcolumn::VirtualColumn for Subquery
            DEBUG_ASSERT(ta[-step.t2.n - 1]->TableType() == TType::TEMP_TABLE);
            int c =
                ((TempTable *)ta[-step.t1.n - 1].get())
                    ->AddVirtColumn(new vcolumn::SubSelectColumn(
                                        dynamic_cast<TempTable *>(ta[-step.t2.n - 1].get()),
                                        step.n1 == 1 ? t->GetOutputMultiIndexP() : t->GetMultiIndexP(), t, step.t1.n),
                                    step.a1.n);
            ASSERT(c == step.a1.n, "AddVirtColumn failed");
          }
          break;
        }
        case CompiledQuery::StepType::ADD_ORDER: {
          DEBUG_ASSERT(step.t1.n < 0 && ta[-step.t1.n - 1]->TableType() == TType::TEMP_TABLE && step.n1 >= 0 &&
                       step.n1 < 2);
          DEBUG_ASSERT(step.a1.n >= 0 && step.a1.n < qu.NoVirtualColumns(step.t1));
          TempTable *loc_t = (TempTable *)ta[-step.t1.n - 1].get();
          loc_t->AddOrder(loc_t->GetVirtualColumn(step.a1.n),
                          (int)step.n1);  // step.n1 = 0 for asc, 1 for desc
          break;
        }
        case CompiledQuery::StepType::UNION:
          DEBUG_ASSERT(step.t1.n < 0 && step.t2.n < 0 && step.t3.n < 0);
          DEBUG_ASSERT(ta[-step.t2.n - 1]->TableType() == TType::TEMP_TABLE &&
                       (step.t3.n == common::NULL_VALUE_32 || ta[-step.t3.n - 1]->TableType() == TType::TEMP_TABLE));
          if (step.t1.n != step.t2.n)
            ta[-step.t1.n - 1] = TempTable::Create(*(TempTable *)ta[-step.t2.n - 1].get(), false);
          if (IsRoughQuery()) {
            if (step.t3.n == common::NULL_VALUE_32)
              ((TempTable *)ta[-step.t1.n - 1].get())->RoughUnion(NULL, qu.IsResultTable(step.t1) ? sender : NULL);
            else
              ((TempTable *)ta[-step.t1.n - 1].get())
                  ->RoughUnion((TempTable *)ta[-step.t3.n - 1].get(), qu.IsResultTable(step.t1) ? sender : NULL);
          } else if (qu.IsResultTable(step.t1) && !qu.IsOrderedBy(step.t1) && step.n1)
            ((TempTable *)ta[-step.t1.n - 1].get())
                ->Union((TempTable *)ta[-step.t3.n - 1].get(), (int)step.n1, sender, global_limits.first,
                        global_limits.second);
          else {
            if (step.t3.n == common::NULL_VALUE_32)
              ((TempTable *)ta[-step.t1.n - 1].get())->Union(NULL, (int)step.n1);
            else {
              ((TempTable *)ta[-step.t1.n - 1].get())->Union((TempTable *)ta[-step.t3.n - 1].get(), (int)step.n1);
              ta[-step.t3.n - 1].reset();
            }
          }
          break;
        case CompiledQuery::StepType::RESULT:
          DEBUG_ASSERT(step.t1.n < 0 && static_cast<size_t>(-step.t1.n - 1) < ta.size() &&
                       ta[-step.t1.n - 1]->TableType() == TType::TEMP_TABLE);
          output_table = (TempTable *)ta[-step.t1.n - 1].get();
          break;
        case CompiledQuery::StepType::STEP_ERROR:
          rccontrol.lock(m_conn->GetThreadID()) << "ERROR in step " << step.alias << system::unlock;
          break;
        default:
          rccontrol.lock(m_conn->GetThreadID())
              << "ERROR: unsupported type of CQStep (" << static_cast<int>(step.type) << ")" << system::unlock;
      }
    } catch (...) {
      for (auto &c : conds) delete c;
      throw;
    }
  }

  for (auto &c : conds) delete c;

  // NOTE: output_table is sent out of this function and should be managed
  // elsewhere. before deleting all TempTables but output_table those have to be
  // detected there are used by output_table

  return output_table;
}

int Query::Item2CQTerm(Item *an_arg, CQTerm &term, const TabID &tmp_table, CondType filter_type, bool negative,
                       Item *left_expr_for_subselect, common::Operator *oper_for_subselect) {
  an_arg = UnRef(an_arg);
  if (an_arg->type() == Item::SUBSELECT_ITEM) {
    Item_subselect *item_subs = dynamic_cast<Item_subselect *>(an_arg);
    DEBUG_ASSERT(item_subs && "The cast to (Item_subselect*) was unsuccessful");

    bool ignore_limit = false;
    if (dynamic_cast<Item_maxmin_subselect *>(item_subs) != NULL ||
        dynamic_cast<Item_in_subselect *>(item_subs) != NULL)
      ignore_limit = true;
    st_select_lex_unit *select_unit = item_subs->unit;

    // needs to check if we can relay on subquery transformation to min/max
    bool ignore_minmax = (dynamic_cast<Item_maxmin_subselect *>(item_subs) == NULL &&
                          dynamic_cast<Item_in_subselect *>(item_subs) == NULL && negative &&
                          item_subs->substype() == Item_subselect::SINGLEROW_SUBS);
    subqueries_in_where.emplace_back(tmp_table,
                                     item_subs->place() != IN_HAVING && filter_type != CondType::HAVING_COND);

    // we need to make a copy of global map with table aliases so that subquery
    // contains aliases of outer queries and itself but not "parallel"
    // subqueries. Once subquery is compiled we can get rid of its aliases since
    // they are not needed any longer and stay with aliases of outer query only
    auto outer_map_copy = table_alias2index_ptr;
    TabID subselect;
    int res = Compile(cq, select_unit->first_select(), select_unit->union_distinct, &subselect, ignore_limit,
                      left_expr_for_subselect, oper_for_subselect, ignore_minmax, true);
    // restore outer query aliases
    table_alias2index_ptr = outer_map_copy;

    subqueries_in_where.pop_back();
    if (res == RCBASE_QUERY_ROUTE) {
      AttrID vc;
      vc.n = VirtualColumnAlreadyExists(tmp_table, subselect);
      if (vc.n == common::NULL_VALUE_32) {
        cq->CreateVirtualColumn(vc, tmp_table, subselect, filter_type == CondType::HAVING_COND ? true : false);
        tab_id2subselect.insert(std::make_pair(tmp_table, std::make_pair(vc.n, subselect)));
      }
      if (oper_for_subselect) {
        if (dynamic_cast<Item_maxmin_subselect *>(item_subs) != NULL ||
            dynamic_cast<Item_in_subselect *>(item_subs) != NULL) {
          if (negative) {
            MarkWithAll(*oper_for_subselect);
            if (dynamic_cast<Item_in_subselect *>(item_subs) != NULL && *oper_for_subselect == common::Operator::O_IN)
              *oper_for_subselect = common::Operator::O_EQ_ALL;
          } else {
            MarkWithAny(*oper_for_subselect);
            if (dynamic_cast<Item_allany_subselect *>(item_subs) != NULL &&
                dynamic_cast<Item_allany_subselect *>(item_subs)->all == 1)
              *oper_for_subselect = common::Operator::O_EQ_ALL;
          }
        } else {
          if (negative) {
            // if(item_subs->substype() != Item_subselect::SINGLEROW_SUBS)
            //      return RETURN_QUERY_TO_MYSQL_ROUTE;
            MarkWithAll(*oper_for_subselect);
          } else
            UnmarkAllAny(*oper_for_subselect);
        }
      }
      term = CQTerm(vc.n);
    }
    return res;
  }

  if (filter_type == CondType::HAVING_COND) {
    common::ColOperation oper;
    bool distinct;
    if (!OperationUnmysterify(an_arg, oper, distinct,
                              true))  // is_having_clause may be true only in
                                      // case group by clause was present
      return RETURN_QUERY_TO_MYSQL_ROUTE;

    AttrID col, vc;
    TabID tab;
    if ((IsFieldItem(an_arg) || IsAggregationOverFieldItem(an_arg)) && !FieldUnmysterify(an_arg, tab, col))
      return RETURN_QUERY_TO_MYSQL_ROUTE;
    if (IsAggregationItem(an_arg) && HasAggregation(((Item_sum *)an_arg)->get_arg(0)))
      return RETURN_QUERY_TO_MYSQL_ROUTE;
    if ((IsFieldItem(an_arg) || IsAggregationOverFieldItem(an_arg)) && cq->ExistsInTempTable(tab, tmp_table)) {
      int col_num = AddColumnForPhysColumn(an_arg, tmp_table, oper, distinct, true);
      auto phys_vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, AttrID(-col_num - 1));
      if (phys_vc.first == common::NULL_VALUE_32) {
        phys_vc.first = tmp_table.n;
        cq->CreateVirtualColumn(phys_vc.second, tmp_table, tmp_table, AttrID(col_num));
        phys2virt.insert(std::make_pair(std::pair<int, int>(tmp_table.n, -col_num - 1), phys_vc));
      }
      vc.n = phys_vc.second;
    } else if (IsCountStar(an_arg)) {
      AttrID at;
      at.n = GetAddColumnId(AttrID(common::NULL_VALUE_32), tmp_table, common::ColOperation::COUNT, false);
      if (at.n == common::NULL_VALUE_32)  // doesn't exist yet
        cq->AddColumn(at, tmp_table, CQTerm(), common::ColOperation::COUNT, NULL, false);
      auto phys_vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, at);
      if (phys_vc.first == common::NULL_VALUE_32) {
        phys_vc.first = tmp_table.n;
        cq->CreateVirtualColumn(phys_vc.second, tmp_table, tmp_table, at);
        phys2virt.insert(std::make_pair(std::pair<int, int>(tmp_table.n, at.n), phys_vc));
      }
      vc.n = phys_vc.second;
    } else if (an_arg->type() == Item::VARBIN_ITEM) {
      String str;
      an_arg->val_str(&str);  // sets null_value
      if (!an_arg->null_value) {
        if (an_arg->max_length <= 8) {
          Item *int_item = new Item_int((ulonglong)an_arg->val_int());
          MysqlExpression *mysql_expression = NULL;
          MysqlExpression::Item2VarID item2varid;
          gc_expressions.push_back(mysql_expression = new MysqlExpression(int_item, item2varid));
          vc.n = VirtualColumnAlreadyExists(tmp_table, mysql_expression);
          if (vc.n == common::NULL_VALUE_32) {
            cq->CreateVirtualColumn(vc, tmp_table, mysql_expression);
            tab_id2expression.insert(std::make_pair(tmp_table, std::make_pair(vc.n, mysql_expression)));
          }
        } else
          return RETURN_QUERY_TO_MYSQL_ROUTE;  // too large binary to be treated
                                               // as BIGINT
      } else {
        return RETURN_QUERY_TO_MYSQL_ROUTE;
      }
    } else {
      MysqlExpression *expr;
      MysqlExpression::SetOfVars vars;
      if (WrapMysqlExpression(an_arg, tmp_table, expr, false, true) == WrapStatus::FAILURE)
        return RETURN_QUERY_TO_MYSQL_ROUTE;
      if (IsConstExpr(expr->GetVars(), tmp_table)) {
        vc.n = VirtualColumnAlreadyExists(tmp_table, expr);
        if (vc.n == common::NULL_VALUE_32) {
          cq->CreateVirtualColumn(vc, tmp_table, expr, tmp_table);
          tab_id2expression.insert(std::make_pair(tmp_table, std::make_pair(vc.n, expr)));
        }
      } else if (IsAggregationItem(an_arg)) {
        DEBUG_ASSERT(expr->GetItem()->type() == Item_sdbfield::get_sdbitem_type());
        int col_num =
            ((Item_sdbfield *)expr->GetItem())->varID[((Item_sdbfield *)expr->GetItem())->varID.size() - 1].col;
        auto phys_vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, AttrID(-col_num - 1));
        if (phys_vc.first == common::NULL_VALUE_32) {
          phys_vc.first = tmp_table.n;
          cq->CreateVirtualColumn(phys_vc.second, tmp_table, tmp_table, AttrID(col_num));
          phys2virt.insert(std::make_pair(std::pair<int, int>(tmp_table.n, -col_num - 1), phys_vc));
        }
        vc.n = phys_vc.second;
      } else {
        //                              int col_num =
        //                              AddColumnForMysqlExpression(expr,
        //                              tmp_table,
        // NULL, DELAYED, distinct, true);
        vc.n = VirtualColumnAlreadyExists(tmp_table, expr);
        if (vc.n == common::NULL_VALUE_32) {
          cq->CreateVirtualColumn(vc, tmp_table, expr, tmp_table);
          tab_id2expression.insert(std::make_pair(tmp_table, std::make_pair(vc.n, expr)));
        }
      }
    }
    term = CQTerm(vc.n);
    return RCBASE_QUERY_ROUTE;
  } else {
    // WHERE FILTER

    AttrID vc;
    AttrID col;
    TabID tab;
    if (IsFieldItem(an_arg) && !FieldUnmysterify(an_arg, tab, col)) return RETURN_QUERY_TO_MYSQL_ROUTE;
    if (IsFieldItem(an_arg) && cq->ExistsInTempTable(tab, tmp_table)) {
      auto phys_vc = VirtualColumnAlreadyExists(tmp_table, tab, col);
      if (phys_vc.first == common::NULL_VALUE_32) {
        phys_vc.first = tmp_table.n;
        cq->CreateVirtualColumn(phys_vc.second, tmp_table, tab, col);
        phys2virt.insert(std::make_pair(std::pair<int, int>(tab.n, col.n), phys_vc));
      }
      vc.n = phys_vc.second;
    } else if (an_arg->type() == Item::VARBIN_ITEM) {
      String str;
      an_arg->val_str(&str);  // sets null_value
      if (!an_arg->null_value) {
        if (an_arg->max_length <= 8) {
          Item *int_item = new Item_int((ulonglong)an_arg->val_int());
          MysqlExpression *mysql_expression = NULL;
          MysqlExpression::Item2VarID item2varid;
          gc_expressions.push_back(mysql_expression = new MysqlExpression(int_item, item2varid));
          vc.n = VirtualColumnAlreadyExists(tmp_table, mysql_expression);
          if (vc.n == common::NULL_VALUE_32) {
            cq->CreateVirtualColumn(vc, tmp_table, mysql_expression);
            tab_id2expression.insert(std::make_pair(tmp_table, std::make_pair(vc.n, mysql_expression)));
          }
        } else
          return RETURN_QUERY_TO_MYSQL_ROUTE;  // too large binary to be treated
                                               // as BIGINT
      } else {
        return RETURN_QUERY_TO_MYSQL_ROUTE;
      }
    } else {
      MysqlExpression *expr;
      WrapStatus ws = WrapMysqlExpression(an_arg, tmp_table, expr, true, false);
      if (ws != WrapStatus::SUCCESS) return RETURN_QUERY_TO_MYSQL_ROUTE;
      vc.n = VirtualColumnAlreadyExists(tmp_table, expr);
      if (vc.n == common::NULL_VALUE_32) {
        cq->CreateVirtualColumn(vc, tmp_table, expr);
        tab_id2expression.insert(std::make_pair(tmp_table, std::make_pair(vc.n, expr)));
      }
    }
    term = CQTerm(vc.n);
    return RCBASE_QUERY_ROUTE;
  }
  return RETURN_QUERY_TO_MYSQL_ROUTE;
}

CondID Query::ConditionNumberFromMultipleEquality(Item_equal *conds, const TabID &tmp_table, CondType filter_type,
                                                  CondID *and_me_filter, bool is_or_subtree) {
  Item_equal_iterator li(*conds);

  CQTerm zero_term, first_term, next_term;
  Item_field *ifield;
  Item *const_item = conds->get_const();
  if (const_item) {
    if (!Item2CQTerm(const_item, zero_term, tmp_table, filter_type)) return CondID(-1);
  } else {
    ifield = li++;
    if (!Item2CQTerm(ifield, zero_term, tmp_table, filter_type)) return CondID(-1);
  }
  ifield = li++;
  if (!Item2CQTerm(ifield, first_term, tmp_table, filter_type)) return CondID(-1);
  CondID filter;
  if (!and_me_filter)
    cq->CreateConds(filter, tmp_table, first_term, common::Operator::O_EQ, zero_term, CQTerm(),
                    is_or_subtree || filter_type == CondType::HAVING_COND);
  else {
    if (is_or_subtree)
      cq->Or(*and_me_filter, tmp_table, first_term, common::Operator::O_EQ, zero_term);
    else
      cq->And(*and_me_filter, tmp_table, first_term, common::Operator::O_EQ, zero_term);
  }
  while ((ifield = li++) != nullptr) {
    if (!Item2CQTerm(ifield, next_term, tmp_table, filter_type)) return CondID(-1);
    if (!and_me_filter) {
      if (is_or_subtree)
        cq->Or(filter, tmp_table, next_term, common::Operator::O_EQ, zero_term);
      else
        cq->And(filter, tmp_table, next_term, common::Operator::O_EQ, zero_term);
    } else {
      if (is_or_subtree)
        cq->Or(*and_me_filter, tmp_table, next_term, common::Operator::O_EQ, zero_term);
      else
        cq->And(*and_me_filter, tmp_table, next_term, common::Operator::O_EQ, zero_term);
    }
  }
  if (and_me_filter) filter = *and_me_filter;
  return filter;
}

Item *Query::FindOutAboutNot(Item *it, bool &is_there_not) {
  is_there_not = false;

  /*First we try to take care of IN and BETWEEN*/
  Item_func_opt_neg *it_maybe_neg = dynamic_cast<Item_func_opt_neg *>(it);
  if (it_maybe_neg) {
    is_there_not = it_maybe_neg->negated;
    return it;
  }

  /*Then we remove negation in case of LIKE (or sth. else)*/
  if (((Item_func *)it)->functype() == Item_func::NOT_FUNC) {
    Item *arg = UnRef(((Item_func *)it)->arguments()[0]);
    // OK, in case of subselects (and possibly other stuff) NOT is handled by
    // other statements, So we narrow the choice of cases in which we handle NOT
    // down to LIKE
    if (arg->type() == Item::FUNC_ITEM && ((Item_func *)arg)->functype() == Item_func::LIKE_FUNC) {
      is_there_not = true;
      return arg;
    }
  }
  return it;
}

CondID Query::ConditionNumberFromComparison(Item *conds, const TabID &tmp_table, CondType filter_type,
                                            CondID *and_me_filter, bool is_or_subtree, bool negative) {
  CondID filter;
  common::Operator op; /*{    common::Operator::O_EQ, common::Operator::O_NOT_EQ, common::Operator::O_LESS,
                     common::Operator::O_MORE, common::Operator::O_LESS_EQ, common::Operator::O_MORE_EQ,
                     common::Operator::O_IS_NULL, common::Operator::O_NOT_NULL, common::Operator::O_BETWEEN,
                     common::Operator::O_LIKE, common::Operator::O_IN, common::Operator::O_ESCAPE etc...};*/
  char like_esc;
  Item_in_optimizer *in_opt = NULL;  // set if IN expression with subselect

  ExtractOperatorType(conds, op, negative, like_esc);
  Item_func *cond_func = (Item_func *)conds;
  if (op == common::Operator::O_MULT_EQUAL_FUNC)
    return ConditionNumberFromMultipleEquality((Item_equal *)conds, tmp_table, filter_type, and_me_filter,
                                               is_or_subtree);
  else if (op == common::Operator::O_NOT_FUNC) {
    if (cond_func->arg_count != 1 || dynamic_cast<Item_in_optimizer *>(cond_func->arguments()[0]) == NULL)
      return CondID(-2);
    return ConditionNumberFromComparison(cond_func->arguments()[0], tmp_table, filter_type, and_me_filter,
                                         is_or_subtree, true);
  } else if (op == common::Operator::O_NOT_ALL_FUNC) {
    if (cond_func->arg_count != 1) return CondID(-1);
    return ConditionNumberFromComparison(cond_func->arguments()[0], tmp_table, filter_type, and_me_filter,
                                         is_or_subtree, dynamic_cast<Item_func_nop_all *>(cond_func) == NULL);
  } else if (op == common::Operator::O_UNKNOWN_FUNC) {
    in_opt = dynamic_cast<Item_in_optimizer *>(cond_func);
    if (in_opt == NULL || cond_func->arg_count != 2 || in_opt->arguments()[0]->cols() != 1) return CondID(-2);
    op = common::Operator::O_IN;
  } else if (op == common::Operator::O_ERROR)
    return CondID(-2);  // unknown function type

  if ((cond_func->arg_count > 3 && op != common::Operator::O_IN && op != common::Operator::O_NOT_IN))
    return CondID(-1);  // argument count error

  CQTerm terms[3];
  std::vector<MysqlExpression *> exprs;
  std::vector<int> vcs;

  Item **args = cond_func->arguments();
  for (uint i = 0; i < cond_func->arg_count; i++) {
    Item *an_arg = UnRef(args[i]);
    if ((op == common::Operator::O_IN || op == common::Operator::O_NOT_IN) && i > 0) {
      if (i == 1 && in_opt) {
        if (!Item2CQTerm(an_arg, terms[i], tmp_table, filter_type, negative, *in_opt->get_cache(), &op))
          return CondID(-1);
        if (negative) switch (op) {
            case common::Operator::O_EQ:
              op = common::Operator::O_NOT_EQ;
              break;
            case common::Operator::O_EQ_ALL:
              op = common::Operator::O_NOT_IN;
              break;
            case common::Operator::O_EQ_ANY:
              op = common::Operator::O_NOT_EQ_ANY;
              break;
            case common::Operator::O_NOT_EQ:
              op = common::Operator::O_EQ;
              break;
            case common::Operator::O_NOT_EQ_ALL:
              op = common::Operator::O_EQ_ALL;
              break;
            case common::Operator::O_NOT_EQ_ANY:
              op = common::Operator::O_EQ_ANY;
              break;
            case common::Operator::O_LESS_EQ:
              op = common::Operator::O_MORE;
              break;
            case common::Operator::O_LESS_EQ_ALL:
              op = common::Operator::O_MORE_ALL;
              break;
            case common::Operator::O_LESS_EQ_ANY:
              op = common::Operator::O_MORE_ANY;
              break;
            case common::Operator::O_MORE_EQ:
              op = common::Operator::O_LESS;
              break;
            case common::Operator::O_MORE_EQ_ALL:
              op = common::Operator::O_LESS_ALL;
              break;
            case common::Operator::O_MORE_EQ_ANY:
              op = common::Operator::O_LESS_ANY;
              break;
            case common::Operator::O_MORE:
              op = common::Operator::O_LESS_EQ;
              break;
            case common::Operator::O_MORE_ALL:
              op = common::Operator::O_LESS_EQ_ALL;
              break;
            case common::Operator::O_MORE_ANY:
              op = common::Operator::O_LESS_EQ_ANY;
              break;
            case common::Operator::O_LESS:
              op = common::Operator::O_MORE_EQ;
              break;
            case common::Operator::O_LESS_ALL:
              op = common::Operator::O_MORE_EQ_ALL;
              break;
            case common::Operator::O_LESS_ANY:
              op = common::Operator::O_MORE_EQ_ANY;
              break;
            case common::Operator::O_LIKE:
              op = common::Operator::O_NOT_LIKE;
              break;
            case common::Operator::O_IN:
              op = common::Operator::O_NOT_IN;
              break;
            case common::Operator::O_NOT_LIKE:
              op = common::Operator::O_LIKE;
              break;
            case common::Operator::O_NOT_IN:
              op = common::Operator::O_IN;
              break;
            default:
              return CondID(-1);
          }
      } else {
        CQTerm t;
        if (!Item2CQTerm(an_arg, t, tmp_table, filter_type, an_arg->type() == Item::SUBSELECT_ITEM ? negative : false,
                         NULL, &op))
          return CondID(-1);
        vcs.push_back(t.vc_id);
      }
    } else {
      if (!Item2CQTerm(an_arg, terms[i], tmp_table, filter_type,
                       an_arg->type() == Item::SUBSELECT_ITEM ? negative : false, NULL, &op))
        return CondID(-1);
      if ((op == common::Operator::O_LIKE || op == common::Operator::O_NOT_LIKE) &&
          !(an_arg->field_type() == MYSQL_TYPE_VARCHAR || an_arg->field_type() == MYSQL_TYPE_STRING ||
            an_arg->field_type() == MYSQL_TYPE_VAR_STRING || an_arg->field_type() == MYSQL_TYPE_BLOB)) {
        return CondID(-1);  // Argument of LIKE is not a string, return to MySQL.
      }
    }
  }

  if ((op == common::Operator::O_IN || op == common::Operator::O_NOT_IN) && !in_opt) {
    AttrID vc;
    vc.n = VirtualColumnAlreadyExists(tmp_table, vcs, AttrID(terms[0].vc_id));
    if (vc.n == common::NULL_VALUE_32) {
      cq->CreateVirtualColumn(vc, tmp_table, vcs, AttrID(terms[0].vc_id));
      tab_id2inset.insert(std::make_pair(tmp_table, std::make_pair(vc.n, std::make_pair(vcs, AttrID(terms[0].vc_id)))));
    }
    terms[1] = CQTerm(vc.n);
  }

  if (!and_me_filter)
    cq->CreateConds(filter, tmp_table, terms[0], op, terms[1], terms[2],
                    is_or_subtree || filter_type == CondType::HAVING_COND, like_esc);
  else {
    if (is_or_subtree)
      cq->Or(*and_me_filter, tmp_table, terms[0], op, terms[1], terms[2]);
    else
      cq->And(*and_me_filter, tmp_table, terms[0], op, terms[1], terms[2]);
    filter = *and_me_filter;
  }
  return filter;
}

CondID Query::ConditionNumberFromNaked(Item *conds, const TabID &tmp_table, CondType filter_type, CondID *and_me_filter,
                                       bool is_or_subtree) {
  CondID filter;
  CQTerm naked_col;
  if (!Item2CQTerm(conds, naked_col, tmp_table, filter_type,
                   conds->type() == Item::SUBSELECT_ITEM ? (and_me_filter != NULL) : false))
    return CondID(-1);

  bool is_string = conds->result_type() == STRING_RESULT;
  MysqlExpression::Item2VarID item2varid;
  AttrID vc;
  Item *zero_item;
  if (is_string)
    zero_item = new Item_empty_string("", 0, conds->collation.collation);
  else
    zero_item = new Item_int((longlong)0);

  MysqlExpression *mysql_expression = NULL;
  gc_expressions.push_back(mysql_expression = new MysqlExpression(zero_item, item2varid));
  // TODO: where mysql_expression & zero_item is destroyed ???
  vc.n = VirtualColumnAlreadyExists(tmp_table, mysql_expression);
  if (vc.n == common::NULL_VALUE_32) {
    cq->CreateVirtualColumn(vc, tmp_table, mysql_expression);
    tab_id2expression.insert(std::make_pair(tmp_table, std::make_pair(vc.n, mysql_expression)));
  }
  if (!and_me_filter)
    cq->CreateConds(filter, tmp_table, naked_col, common::Operator::O_NOT_EQ, CQTerm(vc.n), CQTerm(),
                    is_or_subtree || filter_type == CondType::HAVING_COND);
  else {
    if (is_or_subtree)
      cq->Or(*and_me_filter, tmp_table, naked_col, common::Operator::O_NOT_EQ, CQTerm(vc.n), CQTerm());
    else
      cq->And(*and_me_filter, tmp_table, naked_col, common::Operator::O_NOT_EQ, CQTerm(vc.n), CQTerm());
    filter = *and_me_filter;
  }
  return filter;
}

struct ItemFieldCompare {
  bool operator()(Item_field *const &f1, Item_field *const &f2) const { return f1->field < f2->field; }
};

CondID Query::ConditionNumber(Item *conds, const TabID &tmp_table, CondType filter_type, CondID *and_me_filter,
                              bool is_or_subtree) {
  // we know, that conds != 0
  // returns -1 on error
  //        >=0 is a created filter number
  conds = UnRef(conds);
  Item::Type cond_type = conds->type();
  CondID cond_id;
  if (cond_type == Item::COND_ITEM) {
    Item_cond *cond_cond = (Item_cond *)conds;
    Item_func::Functype func_type = cond_cond->functype();
    switch (func_type) {
      case Item_func::COND_AND_FUNC: {
        List_iterator_fast<Item> li(*(cond_cond->argument_list()));
        Item *item;
        std::unique_ptr<CondID> and_cond;
        while ((item = li++) != nullptr) {
          CondID res = ConditionNumber(item, tmp_table, filter_type, and_cond.get(),
                                       // if there is no and_cond the filter has to be created and info
                                       // about tree/no tree like descriptor has to be passed recursively
                                       // down once filter is created we pass 'false' to indicate AND
                                       and_cond.get() ? false : is_or_subtree);
          if (res.IsInvalid()) return res;
          if (!and_cond.get()) and_cond = std::unique_ptr<CondID>(new CondID(res.n));
        }
        cond_id.n = and_cond->n;
        if (and_me_filter && is_or_subtree)
          cq->Or(*and_me_filter, tmp_table, cond_id);
        else if (and_me_filter && !is_or_subtree)
          cq->And(*and_me_filter, tmp_table, cond_id);
        break;
      }
      case Item_func::COND_OR_FUNC: {
        List_iterator_fast<Item> li(*(cond_cond->argument_list()));
        Item *item;
        // rewriting a=1 or a=2 or b=3 or c=4 into a IN (1,2) or b IN (3,4)
        // step1: collect all constants for every column
        // a -> 1,2
        // b -> 3,4
        std::map<Item_field *, std::set<int>, ItemFieldCompare> value_map;
        std::map<Item *, bool> is_transformed;
        std::unique_ptr<CondID> or_cond;
        while ((item = li++) != nullptr) {
          is_transformed[item] = false;
          Item_func_eq *item_func = dynamic_cast<Item_func_eq *>(item);
          if (!item_func) continue;
          Item **args = item_func->arguments();
          DEBUG_ASSERT(item_func->arg_count == 2);
          Item *first_arg = UnRef(args[0]);
          Item *sec_arg = UnRef(args[1]);
          if (IsConstItem(first_arg) && IsFieldItem(sec_arg)) std::swap(first_arg, sec_arg);
          if (IsFieldItem(first_arg) && IsConstItem(sec_arg)) {
            is_transformed[item] = true;
            CQTerm t;
            if (!Item2CQTerm(sec_arg, t, tmp_table, filter_type)) return CondID(-1);
            value_map[static_cast<Item_field *>(first_arg)].insert(t.vc_id);
          }
        }
        bool create_or_subtree = is_or_subtree;
        li.rewind();
        // generate non-transformed conditions
        while ((item = li++) != nullptr) {
          if (is_transformed[item] == true) continue;
          create_or_subtree = true;
          CondID res = ConditionNumber(item, tmp_table, filter_type, or_cond.get(), true /*CondType::OR_SUBTREE*/);
          if (res.IsInvalid()) return res;
          if (!or_cond) or_cond.reset(new CondID(res.n));
        }
        create_or_subtree = create_or_subtree || (value_map.size() > 1);
        // generate re-written INs
        // one IN for every element of value_map
        for (auto &it : value_map) {
          CQTerm terms[2];
          if (!Item2CQTerm(it.first, terms[0], tmp_table, filter_type)) return CondID(-1);
          AttrID vc;
          std::vector<int> vcv(it.second.begin(), it.second.end());
          CondID c_id;
          if (vcv.size() > 1) {
            cq->CreateVirtualColumn(vc, tmp_table, vcv, AttrID(terms[0].vc_id));
            terms[1] = CQTerm(vc.n);
            if (!or_cond) {
              if (and_me_filter && !create_or_subtree) {
                cq->And(*and_me_filter, tmp_table, terms[0], common::Operator::O_IN, terms[1], CQTerm());
                c_id = *and_me_filter;
                and_me_filter = NULL;
              } else
                cq->CreateConds(c_id, tmp_table, terms[0], common::Operator::O_IN, terms[1], CQTerm(),
                                create_or_subtree || filter_type == CondType::HAVING_COND);
              or_cond.reset(new CondID(c_id.n));
            } else {
              cq->Or(*or_cond, tmp_table, terms[0], common::Operator::O_IN, terms[1], CQTerm());
              c_id = *or_cond;
            }
          } else {
            terms[1] = CQTerm(vcv[0]);
            if (!or_cond) {
              if (and_me_filter && !create_or_subtree) {
                cq->And(*and_me_filter, tmp_table, terms[0], common::Operator::O_EQ, terms[1], CQTerm());
                c_id = *and_me_filter;
                and_me_filter = NULL;
              } else
                cq->CreateConds(c_id, tmp_table, terms[0], common::Operator::O_EQ, terms[1], CQTerm(),
                                create_or_subtree || filter_type == CondType::HAVING_COND);
              or_cond.reset(new CondID(c_id.n));
            } else {
              cq->Or(*or_cond, tmp_table, terms[0], common::Operator::O_EQ, terms[1], CQTerm());
              c_id = *or_cond;
            }
          }
          if (c_id.IsInvalid()) {
            return c_id;
          }
        }
        cond_id.n = or_cond->n;
        if (and_me_filter && !is_or_subtree)
          cq->And(*and_me_filter, tmp_table, cond_id);
        else if (and_me_filter && is_or_subtree)
          cq->Or(*and_me_filter, tmp_table, cond_id);
        else if (filter_type != CondType::HAVING_COND && create_or_subtree && !is_or_subtree)
          cq->CreateConds(cond_id, tmp_table, cond_id, create_or_subtree || filter_type == CondType::HAVING_COND);
        break;
      }
      case Item_func::XOR_FUNC:  // we don't handle xor as yet
      default:
        return CondID(-1);  // unknown function type
    }                       // end switch()
  } else if (cond_type == Item::FUNC_ITEM) {
    Item_func *cond_func = (Item_func *)conds;
    Item_func::Functype func_type = cond_func->functype();
    Item *arg = NULL;
    if (cond_func->arg_count == 1) arg = cond_func->arguments()[0];
    if (func_type == Item_func::NOT_FUNC && arg != NULL && arg->type() == Item::SUBSELECT_ITEM &&
        ((Item_subselect *)arg)->substype() == Item_subselect::EXISTS_SUBS) {
      CQTerm term;
      if (!Item2CQTerm(arg, term, tmp_table, filter_type)) return CondID(-1);
      if (!and_me_filter)
        cq->CreateConds(cond_id, tmp_table, term, common::Operator::O_NOT_EXISTS, CQTerm(), CQTerm(),
                        is_or_subtree || filter_type == CondType::HAVING_COND);
      else {
        if (is_or_subtree)
          cq->Or(*and_me_filter, tmp_table, term, common::Operator::O_NOT_EXISTS, CQTerm(), CQTerm());
        else
          cq->And(*and_me_filter, tmp_table, term, common::Operator::O_NOT_EXISTS, CQTerm(), CQTerm());
        cond_id = *and_me_filter;
      }
    } else if (func_type == Item_func::XOR_FUNC) {
      return CondID(-1);
    } else {
      CondID val = ConditionNumberFromComparison(cond_func, tmp_table, filter_type, and_me_filter, is_or_subtree);
      if (val.n == -2) val = ConditionNumberFromNaked(conds, tmp_table, filter_type, and_me_filter, is_or_subtree);
      return val;
    }
  } else if (cond_type == Item::SUBSELECT_ITEM &&
             ((Item_subselect *)conds)->substype() == Item_subselect::EXISTS_SUBS) {
    CQTerm term;
    if (!Item2CQTerm(conds, term, tmp_table, filter_type)) return CondID(-1);
    if (!and_me_filter) {
      cq->CreateConds(cond_id, tmp_table, term, common::Operator::O_EXISTS, CQTerm(), CQTerm(),
                      is_or_subtree || filter_type == CondType::HAVING_COND);
    } else {
      if (is_or_subtree)
        cq->Or(*and_me_filter, tmp_table, term, common::Operator::O_EXISTS, CQTerm(), CQTerm());
      else
        cq->And(*and_me_filter, tmp_table, term, common::Operator::O_EXISTS, CQTerm(), CQTerm());
      cond_id = *and_me_filter;
    }
  } else if (cond_type == Item::FIELD_ITEM || cond_type == Item::SUM_FUNC_ITEM || cond_type == Item::SUBSELECT_ITEM ||
             cond_type == Item::INT_ITEM || cond_type == Item::STRING_ITEM || cond_type == Item::NULL_ITEM ||
             cond_type == Item::REAL_ITEM || cond_type == Item::DECIMAL_ITEM) {
    return ConditionNumberFromNaked(conds, tmp_table, filter_type, and_me_filter, is_or_subtree);
  }
  return cond_id;
}

int Query::BuildConditions(Item *conds, CondID &cond_id, CompiledQuery *cq, const TabID &tmp_table,
                           CondType filter_type, bool is_zero_result, [[maybe_unused]] JoinType join_type) {
  conds = UnRef(conds);
  PrintItemTree("BuildFiler(), item tree passed in 'conds':", conds);
  if (is_zero_result) {
    CondID fi;
    cq->CreateConds(fi, tmp_table, CQTerm(), common::Operator::O_FALSE, CQTerm(), CQTerm(), false);
    cond_id = fi;
    return RCBASE_QUERY_ROUTE;
  }
  if (!conds) return RCBASE_QUERY_ROUTE;  // No Conditions - no filters. OK

  // keep local copies of class fields to be changed
  CompiledQuery *saved_cq = this->cq;

  // copy method arguments to class fields
  this->cq = cq;

  CondID res = ConditionNumber(conds, tmp_table, filter_type);
  if (res.IsInvalid()) return RETURN_QUERY_TO_MYSQL_ROUTE;

  if (filter_type == CondType::HAVING_COND) {
    cq->CreateConds(res, tmp_table, res, false);
  }

  // restore original values of class fields (may be necessary if this method is
  // called recursively)
  this->cq = saved_cq;
  if (res.IsInvalid()) return RETURN_QUERY_TO_MYSQL_ROUTE;
  cond_id = res;
  return RCBASE_QUERY_ROUTE;
}

bool Query::ClearSubselectTransformation(common::Operator &oper_for_subselect, Item *&field_for_subselect, Item *&conds,
                                         Item *&having, Item *&cond_to_reinsert, List<Item> *&list_to_reinsert,
                                         Item *left_expr_for_subselect) {
  cond_to_reinsert = NULL;
  list_to_reinsert = NULL;
  Item *cond_removed;
  if (having &&
      (having->type() == Item::COND_ITEM ||
       (having->type() == Item::FUNC_ITEM && ((Item_func *)having)->functype() != Item_func::ISNOTNULLTEST_FUNC &&
        (((Item_func *)having)->functype() != Item_func::TRIG_COND_FUNC ||
         ((Item_func *)having)->arguments()[0]->type() != Item::FUNC_ITEM ||
         ((Item_func *)((Item_func *)having)->arguments()[0])->functype() != Item_func::ISNOTNULLTEST_FUNC)))) {
    if (having->type() == Item::COND_ITEM) {
      Item_cond *having_cond = (Item_cond *)having;
      // if the condition is a complex formula it must be AND
      if (having_cond->functype() != Item_func::COND_AND_FUNC) return false;
      // the extra condition is in the last argument
      if (having_cond->argument_list()->elements < 2) return false;
      List_iterator<Item> li(*(having_cond->argument_list()));
      while (li++ != NULL) cond_to_reinsert = *li.ref();
      li.rewind();
      while (*li.ref() != cond_to_reinsert) li++;
      li.remove();
      list_to_reinsert = having_cond->argument_list();
      cond_removed = cond_to_reinsert;
    } else {
      // if no complex boolean formula the original condition was empty
      cond_removed = having;
      having = NULL;
    }
    cond_removed = UnRef(cond_removed);
    // check if the extra condition was wrapped into trigger
    if (cond_removed->type() == Item::FUNC_ITEM &&
        ((Item_func *)cond_removed)->functype() == Item_func::TRIG_COND_FUNC) {
      cond_removed = ((Item_func *)cond_removed)->arguments()[0];
      cond_removed = UnRef(cond_removed);
    }
    // check if the extra condition is a comparison
    if (cond_removed->type() != Item::FUNC_ITEM || ((Item_func *)cond_removed)->arg_count != 2) return false;
    // the right side of equality is the field of the original subselect
    if (dynamic_cast<Item_ref_null_helper *>(((Item_func *)cond_removed)->arguments()[1]) == NULL) return false;
    field_for_subselect = NULL;
  } else if (!having || (having->type() == Item::FUNC_ITEM &&
                         (((Item_func *)having)->functype() == Item_func::ISNOTNULLTEST_FUNC ||
                          ((Item_func *)having)->functype() == Item_func::TRIG_COND_FUNC))) {
    if (!conds) return false;
    if (conds->type() == Item::COND_ITEM && ((Item_cond *)conds)->functype() == Item_func::COND_AND_FUNC) {
      // if the condition is a conjunctive formula
      // the extra condition should be in the last argument
      if (((Item_cond *)conds)->argument_list()->elements < 2) return false;
      List_iterator<Item> li(*(((Item_cond *)conds)->argument_list()));
      while (li++ != NULL) cond_to_reinsert = *li.ref();
      li.rewind();
      while (*li.ref() != cond_to_reinsert) li++;
      li.remove();
      list_to_reinsert = ((Item_cond *)conds)->argument_list();
      cond_removed = cond_to_reinsert;
    } else {
      // if no conjunctive formula the original condition was empty
      cond_removed = conds;
      conds = NULL;
    }
    if (cond_removed->type() == Item::FUNC_ITEM &&
        ((Item_func *)cond_removed)->functype() == Item_func::TRIG_COND_FUNC) {
      // Condition was wrapped into trigger
      cond_removed = (Item_cond *)((Item_func *)cond_removed)->arguments()[0];
    }
    if (cond_removed->type() == Item::COND_ITEM && ((Item_func *)cond_removed)->functype() == Item_func::COND_OR_FUNC) {
      // if the subselect field could have null values
      // equality condition was OR-ed with IS NULL condition
      Item_cond *cond_cond = (Item_cond *)cond_removed;
      List_iterator_fast<Item> li(*(cond_cond->argument_list()));
      cond_removed = li++;
      if (cond_removed == NULL) return false;
      if (li++ == NULL) return false;
      if (li++ != NULL) return false;
      // the original having was empty
      having = NULL;
    }
    // check if the extra condition is a comparison
    if (cond_removed->type() != Item::FUNC_ITEM || ((Item_func *)cond_removed)->arg_count != 2) return false;
    // the right side of equality is the field of the original subselect
    field_for_subselect = ((Item_func *)cond_removed)->arguments()[1];
  } else
    return false;
  // the left side of equality should be the left side of the original
  // expression with subselect
  Item *left_ref = ((Item_func *)cond_removed)->arguments()[0];
  if (dynamic_cast<Item_int_with_ref *>(left_ref) != NULL) left_ref = ((Item_int_with_ref *)left_ref)->real_item();
  if (left_ref->type() != Item::REF_ITEM || ((Item_ref *)left_ref)->ref_type() != Item_ref::DIRECT_REF ||
      ((Item_ref *)left_ref)->real_item() != left_expr_for_subselect)
    return false;
  // set the operation type
  switch (((Item_func *)cond_removed)->functype()) {
    case Item_func::EQ_FUNC:
      oper_for_subselect = common::Operator::O_IN; /*common::Operator::common::Operator::O_IN;*/
      break;
    case Item_func::NE_FUNC:
      oper_for_subselect = common::Operator::O_NOT_EQ;
      break;
    case Item_func::LT_FUNC:
      oper_for_subselect = common::Operator::O_LESS;
      break;
    case Item_func::LE_FUNC:
      oper_for_subselect = common::Operator::O_LESS_EQ;
      break;
    case Item_func::GT_FUNC:
      oper_for_subselect = common::Operator::O_MORE;
      break;
    case Item_func::GE_FUNC:
      oper_for_subselect = common::Operator::O_MORE_EQ;
      break;
    default:
      return false;
  }
  return true;
}

#define TABLE_YET_UNSEEN_INVOLVED 2
// used for verification if all tables involved in a condition were already seen
// used only in assert macro
int Query::PrefixCheck(Item *conds) {
  conds = UnRef(conds);
  if (!conds) return RCBASE_QUERY_ROUTE;
  Item::Type cond_type = conds->type();
  CondID filter;
  switch (cond_type) {
    case Item::COND_ITEM: {
      Item_cond *cond_cond = (Item_cond *)conds;
      Item_func::Functype func_type = cond_cond->functype();
      switch (func_type) {
        case Item_func::COND_AND_FUNC:
        case Item_func::COND_OR_FUNC: {
          //                  List_iterator_fast<Item_equal>
          // list_equal(((Item_cond_and*)cond_cond)->cond_equal.current_level);
          List_iterator_fast<Item> li(*(cond_cond->argument_list()));
          Item *item;
          while ((item = li++) /*|| (item = list_equal++)*/) {
            int ret = PrefixCheck(item);
            if (!ret) return RETURN_QUERY_TO_MYSQL_ROUTE;
            if (ret == TABLE_YET_UNSEEN_INVOLVED) return TABLE_YET_UNSEEN_INVOLVED;
            // for RETURN_TO_RCBASE_ROUTE the next item is evaluated
          }
          break;
        }
        case Item_func::XOR_FUNC:  // we don't handle xor as yet
        default:
          return -1;  // unknown function type
      }
      break;
    }
    case Item::FUNC_ITEM: {
      Item_func *cond_func = (Item_func *)conds;
      Item_func::Functype func_type = cond_func->functype();
      switch (func_type) {
        case Item_func::BETWEEN:
        case Item_func::LIKE_FUNC:
        case Item_func::ISNULL_FUNC:
        case Item_func::ISNOTNULL_FUNC:
        case Item_func::IN_FUNC:
        case Item_func::EQ_FUNC:    // =
        case Item_func::NE_FUNC:    // <>
        case Item_func::LE_FUNC:    // <=
        case Item_func::GE_FUNC:    // >=
        case Item_func::GT_FUNC:    // >
        case Item_func::LT_FUNC: {  // <
          for (int i = 0; (unsigned)i < cond_func->arg_count; i++) {
            Item **args = cond_func->arguments();
            Item *an_arg = UnRef(args[i]);
            int ret = PrefixCheck(an_arg);
            if (!ret) return RETURN_QUERY_TO_MYSQL_ROUTE;
            if (ret == TABLE_YET_UNSEEN_INVOLVED) return TABLE_YET_UNSEEN_INVOLVED;
          }
          break;
        }
        case Item_func::MULT_EQUAL_FUNC: {
          Item_equal_iterator li(*(Item_equal *)conds);
          Item_field *ifield;
          while ((ifield = li++) != nullptr) {
            int ret = PrefixCheck(ifield);
            if (!ret) return RETURN_QUERY_TO_MYSQL_ROUTE;
            if (ret == TABLE_YET_UNSEEN_INVOLVED) return TABLE_YET_UNSEEN_INVOLVED;
          }
          break;
        }
        default:
          return RETURN_QUERY_TO_MYSQL_ROUTE;  // unknown function type
      }
      break;
    }
    case Item::FIELD_ITEM:       // regular select
    case Item::SUM_FUNC_ITEM: {  // min(k), max(k), count(), avg(k), sum
      const char *field_alias = NULL;
      const char *table_alias = NULL;
      const char *database_name = NULL;
      const char *table_name = NULL;
      const char *table_path = NULL;
      const TABLE *table_ptr = NULL;
      const char *field_name = NULL;
      if (!FieldUnmysterify(conds, database_name, table_name, table_alias, table_path, table_ptr, field_name,
                            field_alias))
        return RETURN_QUERY_TO_MYSQL_ROUTE;
      ASSERT(std::strcmp(table_alias, EMPTY_TABLE_CONST_INDICATOR), "unexpected table alias");
      std::string ext_alias = std::string(table_name ? table_name : "") + std::string(":") + std::string(table_alias);
      if (table_alias2index_ptr.lower_bound(ext_alias) == table_alias2index_ptr.end())
        return TABLE_YET_UNSEEN_INVOLVED;
      else
        return RCBASE_QUERY_ROUTE;
      break;
    }
    default:
      // hmmm.... ?
      break;
  }
  return RCBASE_QUERY_ROUTE;
}

int Query::BuildCondsIfPossible(Item *conds, CondID &cond_id, const TabID &tmp_table, JoinType join_type) {
  conds = UnRef(conds);
  if (conds) {
    CondType filter_type =
        (join_type == JoinType::JO_LEFT
             ? CondType::ON_LEFT_FILTER
             : (join_type == JoinType::JO_RIGHT ? CondType::ON_RIGHT_FILTER : CondType::ON_INNER_FILTER));
    // in case of Right join MySQL changes order of tables. Right must be
    // switched back to left!
    if (filter_type == CondType::ON_RIGHT_FILTER) filter_type = CondType::ON_LEFT_FILTER;
    DEBUG_ASSERT(PrefixCheck(conds) != TABLE_YET_UNSEEN_INVOLVED &&
                 "Table not yet seen was involved in this condition");

    bool zero_result = conds->type() == Item::INT_ITEM && !conds->val_bool();
    if (!BuildConditions(conds, cond_id, cq, tmp_table, filter_type, zero_result, join_type))
      return RETURN_QUERY_TO_MYSQL_ROUTE;
    conds = 0;
  }
  return RCBASE_QUERY_ROUTE;
}
}  // namespace core
}  // namespace stonedb
