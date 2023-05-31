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

#include "compiled_query.h"
#include "core/mysql_expression.h"
#include "core/query.h"
#include "vc/multi_value_column.h"

namespace Tianmu {
namespace core {
CompiledQuery::CompiledQuery() {
  no_tabs = 0;
  no_conds = 0;
}

CompiledQuery::CompiledQuery(CompiledQuery const &cq) {
  steps = cq.steps;
  no_tabs = cq.no_tabs;
  no_attrs = cq.no_attrs;
  no_conds = cq.no_conds;
  no_virt_cols = cq.no_virt_cols;
  steps_tmp_tables = cq.steps_tmp_tables;
  steps_group_by_cols = cq.steps_group_by_cols;
  TabIDSteps = cq.TabIDSteps;
}

CompiledQuery::CQStep::CQStep(const CompiledQuery::CQStep &s)
    : type(s.type),
      t1(s.t1),
      t2(s.t2),
      t3(s.t3),
      a1(s.a1),
      a2(s.a2),
      c1(s.c1),
      c2(s.c2),
      c3(s.c3),
      e1(s.e1),
      e2(s.e2),
      e3(s.e3),
      op(s.op),
      ex_op(s.ex_op),
      tmpar(s.tmpar),
      jt(s.jt),
      cop(s.cop),
      alias(nullptr),
      mysql_expr(s.mysql_expr),
      virt_cols(s.virt_cols),
      tables1(s.tables1),
      tables2(s.tables2),
      n1(s.n1),
      n2(s.n2),
      si(s.si) {
  if (s.alias) {
    size_t alias_ct(std::strlen(s.alias) + 1);
    alias = new char[alias_ct];
    std::strcpy(alias, s.alias);
  } else
    alias = nullptr;
}

CompiledQuery::CQStep &CompiledQuery::CQStep::operator=(const CompiledQuery::CQStep &s) {
  if (&s != this) {
    CQStep tmp_step(s);
    swap(tmp_step);
  }
  return *this;
}
void CompiledQuery::CQStep::swap(CQStep &s) {
  if (&s != this) {
    std::swap(type, s.type);
    std::swap(t1, s.t1);
    std::swap(t2, s.t2);
    std::swap(t3, s.t3);
    std::swap(a1, s.a1);
    std::swap(a2, s.a2);
    std::swap(c1, s.c1);
    std::swap(c2, s.c2);
    std::swap(c3, s.c3);
    std::swap(e1, s.e1);
    std::swap(e2, s.e2);
    std::swap(e3, s.e3);
    std::swap(op, s.op);
    std::swap(ex_op, s.ex_op);
    std::swap(tmpar, s.tmpar);
    std::swap(jt, s.jt);
    std::swap(cop, s.cop);
    std::swap(n1, s.n1);
    std::swap(n2, s.n2);
    std::swap(mysql_expr, s.mysql_expr);
    std::swap(virt_cols, s.virt_cols);
    std::swap(tables1, s.tables1);
    std::swap(tables2, s.tables2);
    std::swap(alias, s.alias);
    std::swap(si, s.si);
  }
}

void CompiledQuery::CQStep::Print(Query *query) {
  char buf[512] = "";
  char b1[100], b2[100], b3[100];
  b1[0] = b2[0] = b3[0] = '\0';

  char b_op[20];
  switch (op) {
    case common::Operator::O_EQ:
      std::strcpy(b_op, "=");
      break;
    case common::Operator::O_EQ_ALL:
      std::strcpy(b_op, "=ALL");
      break;
    case common::Operator::O_EQ_ANY:
      std::strcpy(b_op, "=ANY");
      break;
    case common::Operator::O_NOT_EQ:
      std::strcpy(b_op, "<>");
      break;
    case common::Operator::O_NOT_EQ_ALL:
      std::strcpy(b_op, "<>ALL");
      break;
    case common::Operator::O_NOT_EQ_ANY:
      std::strcpy(b_op, "<>ANY");
      break;
    case common::Operator::O_LESS:
      std::strcpy(b_op, "<");
      break;
    case common::Operator::O_LESS_ALL:
      std::strcpy(b_op, "<ALL");
      break;
    case common::Operator::O_LESS_ANY:
      std::strcpy(b_op, "<ANY");
      break;
    case common::Operator::O_MORE:
      std::strcpy(b_op, ">");
      break;
    case common::Operator::O_MORE_ALL:
      std::strcpy(b_op, ">ALL");
      break;
    case common::Operator::O_MORE_ANY:
      std::strcpy(b_op, ">ANY");
      break;
    case common::Operator::O_LESS_EQ:
      std::strcpy(b_op, "<=");
      break;
    case common::Operator::O_LESS_EQ_ALL:
      std::strcpy(b_op, "<=ALL");
      break;
    case common::Operator::O_LESS_EQ_ANY:
      std::strcpy(b_op, "<=ANY");
      break;
    case common::Operator::O_MORE_EQ:
      std::strcpy(b_op, ">=");
      break;
    case common::Operator::O_MORE_EQ_ALL:
      std::strcpy(b_op, ">=ALL");
      break;
    case common::Operator::O_MORE_EQ_ANY:
      std::strcpy(b_op, ">=ANY");
      break;
    case common::Operator::O_IS_NULL:
      std::strcpy(b_op, "IS nullptr");
      break;
    case common::Operator::O_NOT_NULL:
      std::strcpy(b_op, "IS NOT nullptr");
      break;
    case common::Operator::O_BETWEEN:
      std::strcpy(b_op, "BETWEEN");
      break;
    case common::Operator::O_IN:
      std::strcpy(b_op, "IN");
      break;
    case common::Operator::O_LIKE:
      std::strcpy(b_op, "LIKE");
      break;
    case common::Operator::O_ESCAPE:
      std::strcpy(b_op, "ESCAPE");
      break;
    case common::Operator::O_EXISTS:
      std::strcpy(b_op, "EXISTS");
      break;
    case common::Operator::O_NOT_LIKE:
      std::strcpy(b_op, "NOT LIKE");
      break;
    case common::Operator::O_NOT_BETWEEN:
      std::strcpy(b_op, "NOT BETWEEN");
      break;
    case common::Operator::O_NOT_IN:
      std::strcpy(b_op, "NOT IN");
      break;
    case common::Operator::O_NOT_EXISTS:
      std::strcpy(b_op, "NOT EXISTS");
      break;
    case common::Operator::O_FALSE:
      std::strcpy(b_op, "FALSE");
      break;
    case common::Operator::O_TRUE:
      std::strcpy(b_op, "TRUE");
      break;
    default:
      std::strcpy(b_op, "?");
  }

  char b_cop[20];  // enum common::ColOperation {LIST, COUNT, COUNT_DISTINCT,
                   // SUM, MIN, MAX, AVG};

  switch (cop) {
    case common::ColOperation::LISTING:
      std::strcpy(b_cop, "LIST");
      break;
    case common::ColOperation::COUNT:
      std::strcpy(b_cop, "COUNT");
      break;
    case common::ColOperation::SUM:
      std::strcpy(b_cop, "SUM");
      break;
    case common::ColOperation::MIN:
      std::strcpy(b_cop, "MIN");
      break;
    case common::ColOperation::MAX:
      std::strcpy(b_cop, "MAX");
      break;
    case common::ColOperation::AVG:
      std::strcpy(b_cop, "AVG");
      break;
    case common::ColOperation::STD_POP:
      std::strcpy(b_cop, "STD_POP");
      break;
    case common::ColOperation::STD_SAMP:
      std::strcpy(b_cop, "STD_SAMP");
      break;
    case common::ColOperation::VAR_POP:
      std::strcpy(b_cop, "VAR_POP");
      break;
    case common::ColOperation::VAR_SAMP:
      std::strcpy(b_cop, "VAR_SAMP");
      break;
    case common::ColOperation::BIT_AND:
      std::strcpy(b_cop, "BIT_AND");
      break;
    case common::ColOperation::BIT_OR:
      std::strcpy(b_cop, "BIT_OR");
      break;
    case common::ColOperation::BIT_XOR:
      std::strcpy(b_cop, "BIT_XOR");
      break;
    case common::ColOperation::GROUP_CONCAT:
      std::strcpy(b_cop, "GROUP_CONCAT");
      break;
    case common::ColOperation::GROUP_BY:
      std::strcpy(b_cop, "GROUP_BY");
      break;
    case common::ColOperation::DELAYED:
      std::strcpy(b_cop, "DELAYED");
      break;
    default:
      std::strcpy(b_cop, "[no name yet]");
  }

  char b_tmpar[20];  // enum TMParameter	{	TMParameter::TM_DISTINCT, TMParameter::TM_TOP,
                     // TMParameter::TM_EXISTS, TM_COUNT };
                     // // Table Mode

  switch (tmpar) {
    case TMParameter::TM_DISTINCT:
      std::strcpy(b_tmpar, "DISTINCT");
      break;
    case TMParameter::TM_TOP:
      std::strcpy(b_tmpar, "LIMIT");
      break;
    case TMParameter::TM_EXISTS:
      std::strcpy(b_tmpar, "EXISTS");
      break;
    default:
      std::strcpy(b_tmpar, "???");
  }

  char b_jt[20];  // enum JoinType		{	JoinType::JO_INNER, JoinType::JO_LEFT,
                  // JoinType::JO_RIGHT, JoinType::JO_FULL
                  // };

  switch (jt) {
    case JoinType::JO_INNER:
      std::strcpy(b_jt, "INNER");
      break;
    case JoinType::JO_LEFT:
      std::strcpy(b_jt, "LEFT");
      break;
    case JoinType::JO_RIGHT:
      std::strcpy(b_jt, "RIGHT");
      break;
    case JoinType::JO_FULL:
      std::strcpy(b_jt, "FULL");
      break;
    default:
      std::strcpy(b_jt, "????");
  }

  switch (type) {
    case StepType::TABLE_ALIAS:
      if (alias)
        std::sprintf(buf, "T:%d = TABLE_ALIAS(T:%d,\"%s\")", N(t1.n), N(t2.n), alias);
      else
        std::sprintf(buf, "T:%d = TABLE_ALIAS(T:%d)", N(t1.n), N(t2.n));
      break;
    case StepType::CREATE_CONDS:
      if (c2.IsNull()) {
        std::sprintf(buf, "C:%d = CREATE_%sCONDS(T:%d,%s,%s,%s,%s)", N(c1.n),
                     n1 == static_cast<int64_t>(CondType::OR_SUBTREE) ? "TREE_" : "", N(t1.n),
                     e1.ToString(b1, _countof(b1), t1.n), b_op, e2.ToString(b2, _countof(b2), t1.n),
                     e3.ToString(b3, _countof(b3), t1.n)
                     // n1 == 0 ? "WHERE" : (n1 == 1 ? "HAVING" : (n1 == 2 ? "ON
                     // INNER" : (n1==3 ? "ON LEFT" : "ON RIGHT"))), n2
        );
      } else
        std::sprintf(buf, "C:%d = CREATE_CONDS(T:%d, C:%d)", N(c1.n), N(t1.n), N(c2.n));
      break;
    case StepType::AND_F:
      std::sprintf(buf, "C:%d.AND(C:%d)", N(c1.n), N(c2.n));
      break;
    case StepType::OR_F:
      std::sprintf(buf, "C:%d.OR(C:%d)", N(c1.n), N(c2.n));
      break;
    // case NOT_F:
    //	std::sprintf(buf,"F:%d = NOT(F:%d)",N(f2.n),N(f1.n));
    //	break;
    // case COPY_F:
    //	std::sprintf(buf,"C:%d = COPY(C:%d)", N(c2.n), N(c1.n));
    //	break;
    case StepType::AND_DESC:
      std::sprintf(buf, "C:%d.AND(%s,%s,%s,%s)", N(c1.n), e1.ToString(b1, _countof(b1), t1.n), b_op,
                   e2.ToString(b2, _countof(b2), t1.n), e3.ToString(b3, _countof(b3), t1.n));
      break;
    case StepType::OR_DESC:
      std::sprintf(buf, "C:%d.OR(%s,%s,%s,%s)", N(c1.n), e1.ToString(b1, _countof(b1), t1.n), b_op,
                   e2.ToString(b2, _countof(b2), t1.n), e3.ToString(b3, _countof(b3), t1.n));
      break;
    case StepType::TMP_TABLE: {
      std::sprintf(buf, "T:%d = TMP_TABLE(", N(t1.n));
      unsigned int i = 0;
      for (; i < tables1.size() - 1; i++) std::sprintf(buf + std::strlen(buf), "T:%d,", N(tables1[i].n));
      std::sprintf(buf + std::strlen(buf), "T:%u)", N(tables1[i].n));
      break;
    }
    case StepType::CREATE_VC:
      if (mysql_expr.size() == 1) {
        char s1[200];
        std::strncpy(s1, query->GetItemName(mysql_expr[0]->GetItem()).c_str(), 199);
        s1[199] = '\0';
        std::sprintf(buf, "VC:%d.%d = CREATE_VC(T:%d,EXPR(\"%s\"))", N(t1.n), N(a1.n), N(t1.n), s1);
      } else if (mysql_expr.size() > 1) {
        char s1[200], s2[200];
        std::strncpy(s1, query->GetItemName(mysql_expr[0]->GetItem()).c_str(), 199);
        std::strncpy(s2, query->GetItemName(mysql_expr[mysql_expr.size() - 1]->GetItem()).c_str(), 199);
        s1[199] = '\0';
        s2[199] = '\0';
        std::sprintf(buf, "VC:%d.%d = CREATE_VC(T:%d,EXPR([%s,..,%s](%ld items)))", N(t1.n), N(a1.n), N(t1.n), s1, s2,
                     mysql_expr.size());
      } else if (virt_cols.size() > 1) {
        std::sprintf(buf, "VC:%d.%d = CREATE_VC(T:%d,VIRT_COLS([%d,..,%d](%ld items)))", N(t1.n), N(a1.n), N(t1.n),
                     virt_cols[0], virt_cols[virt_cols.size() - 1], virt_cols.size());
      } else if (a2.n != common::NULL_VALUE_32)
        std::sprintf(buf, "VC:%d.%d = CREATE_VC(T:%d,PHYS_COL(T:%d,A:%d))", N(t1.n), N(a1.n), N(t1.n), N(t2.n),
                     N(a2.n));
      else
        std::sprintf(buf, "VC:%d.%d = CREATE_VC(T:%d,SUBQUERY(T:%d))", N(t1.n), N(a1.n), N(t1.n), N(t2.n));
      break;
    case StepType::T_MODE:
      std::sprintf(buf, "T:%d.MODE(%s,%ld,%ld)", N(t1.n), b_tmpar, n1, n2);
      break;
    case StepType::JOIN_T:
      // This step exists but should not be displayed
      return;
    case StepType::LEFT_JOIN_ON: {
      std::sprintf(buf, "T:%d.LEFT_JOIN_ON({", N(t1.n));
      int i = 0;
      for (; i < (int)tables1.size() - 1; i++) std::sprintf(buf + std::strlen(buf), "T:%d,", N(tables1[i].n));
      std::sprintf(buf + std::strlen(buf), "T:%d},{", N(tables1[i].n));
      i = 0;
      for (; i < (int)tables2.size() - 1; i++) std::sprintf(buf + std::strlen(buf), "T:%d,", N(tables2[i].n));
      std::sprintf(buf + std::strlen(buf), "T:%d},C:%d)", N(tables2[i].n), N(c1.n));
      break;
    }
    case StepType::INNER_JOIN_ON: {
      std::sprintf(buf, "T:%d.INNER_JOIN_ON({", N(t1.n));
      unsigned int i = 0;
      for (; i < tables1.size() - 1; i++) std::sprintf(buf + std::strlen(buf), "T:%d,", N(tables1[i].n));
      std::sprintf(buf + std::strlen(buf), "T:%d},C:%d)", N(tables1[i].n), N(c1.n));
      break;
    }
    case StepType::ADD_CONDS:
      std::sprintf(buf, "T:%d.ADD_CONDS(C:%d,%s)", N(t1.n), N(c1.n), n1 == 0 ? "WHERE" : (n1 == 1 ? "HAVING" : "?!?"));
      break;
    case StepType::APPLY_CONDS:
      std::sprintf(buf, "T:%d.APPLY_CONDS()", N(t1.n));
      break;
    case StepType::ADD_COLUMN:
      std::sprintf(buf, "A:%d = T:%d.ADD_COLUMN(%s,%s,\"%s\",\"%s\")", N(a1.n), N(t1.n),
                   e1.ToString(b1, _countof(b1), t1.n), b_cop, (alias) ? alias : "null", n1 ? "DISTINCT" : "ALL");
      break;
    case StepType::ADD_ORDER:
      std::sprintf(buf, "T:%d.ADD_ORDER(VC:%d.%d,%s)", N(t1.n), N(t1.n), N(a1.n), n1 ? "DESC" : "ASC");
      break;
    case StepType::UNION:
      std::sprintf(buf, "T:%d = UNION(T:%d,T:%d,%ld)", N(t1.n), N(t2.n), N(t3.n), n1);
      break;
    case StepType::RESULT:
      std::sprintf(buf, "RESULT(T:%d)", N(t1.n));
      break;
    default:
      std::sprintf(buf, "Unsupported type of CQStep: %d", type);
  }
  TIANMU_LOG(LogCtl_Level::DEBUG, "%s", buf);
}

void CompiledQuery::TableAlias(TabID &t_out, const TabID &n, const char *name, [[maybe_unused]] int id) {
  CompiledQuery::CQStep s;
  s.type = StepType::TABLE_ALIAS;
  s.t1 = t_out = NextTabID();
  s.t2 = n;
  if (name) {
    s.alias = new char[std::strlen(name) + 1];
    std::strcpy(s.alias, name);
  }
  steps.push_back(s);
}

void CompiledQuery::TmpTable(TabID &t_out, const TabID &t1, bool for_subq_in_where) {
  CompiledQuery::CQStep s;
  if (for_subq_in_where)
    s.n1 = 1;
  else
    s.n1 = 0;
  DEBUG_ASSERT(t1.n < 0 && NumOfTabs() > 0);
  s.type = StepType::TMP_TABLE;
  s.t1 = t_out = NextTabID();  // was s.t2!!!
  s.tables1.push_back(t1);
  steps_tmp_tables.push_back(s);
  steps.push_back(s);
}

void CompiledQuery::CreateConds(CondID &c_out, const TabID &t1, CQTerm e1, common::Operator pr, CQTerm e2, CQTerm e3,
                                bool is_or_subtree, char like_esc, bool is_cond_push) {
  CompiledQuery::CQStep s;
  s.type = StepType::CREATE_CONDS;
  s.c1 = c_out = NextCondID();
  s.t1 = t1;
  s.e1 = e1;
  s.op = pr;
  s.e2 = e2;
  s.e3 = e3;
  s.n1 = is_or_subtree ? static_cast<int64_t>(CondType::OR_SUBTREE) : 0;
  s.n2 = like_esc;
  s.ex_op = is_cond_push ? common::ExtraOperation::EX_COND_PUSH : common::ExtraOperation::EX_DO_NOTHING;
  steps.push_back(s);
}

void CompiledQuery::CreateConds(CondID &c_out, const TabID &t1, const CondID &c1, bool is_or_subtree,
                                bool is_cond_push) {
  CompiledQuery::CQStep s;
  s.type = StepType::CREATE_CONDS;
  s.c2 = c1;
  s.c1 = c_out = NextCondID();
  s.t1 = t1;
  s.n1 = is_or_subtree ? static_cast<int64_t>(CondType::OR_SUBTREE) : 0;
  s.ex_op = is_cond_push ? common::ExtraOperation::EX_COND_PUSH : common::ExtraOperation::EX_DO_NOTHING;
  steps.push_back(s);
}

void CompiledQuery::And(const CondID &c1, const TabID &t, const CondID &c2) {
  if (c1.IsNull()) {
    return;
  }
  CompiledQuery::CQStep s;
  s.type = StepType::AND_F;
  s.c1 = c1;
  s.t1 = t;
  s.c2 = c2;
  steps.push_back(s);
}

void CompiledQuery::Or(const CondID &c1, const TabID &t, const CondID &c2) {
  if (c1.IsNull()) {
    return;
  }
  CompiledQuery::CQStep s;
  s.type = StepType::OR_F;
  s.c1 = c1;
  s.t1 = t;
  s.c2 = c2;
  steps.push_back(s);
}

void CompiledQuery::And(const CondID &c1, const TabID &t, CQTerm e1, common::Operator pr, CQTerm e2, CQTerm e3) {
  CompiledQuery::CQStep s;
  s.type = StepType::AND_DESC;
  s.t1 = t;
  s.c1 = c1;
  s.e1 = e1;
  s.op = pr;
  s.e2 = e2;
  s.e3 = e3;
  steps.push_back(s);
}

void CompiledQuery::Or(const CondID &c1, const TabID &t, CQTerm e1, common::Operator pr, CQTerm e2, CQTerm e3) {
  CompiledQuery::CQStep s;
  s.type = StepType::OR_DESC;
  s.t1 = t;
  s.c1 = c1;
  s.e1 = e1;
  s.op = pr;
  s.e2 = e2;
  s.e3 = e3;
  steps.push_back(s);
}

void CompiledQuery::Mode(const TabID &t1, TMParameter mode, int64_t n1, int64_t n2) {
  CompiledQuery::CQStep s;
  if (s.t1.n >= 0) {
    size_t const alias_ct(100);
    s.type = StepType::STEP_ERROR;
    s.alias = new char[alias_ct];
    std::strcpy(s.alias, "T_MODE: can't be applied to TianmuTable");
    return;
  }
  s.type = StepType::T_MODE;
  s.t1 = t1;
  s.tmpar = mode;
  s.n1 = n1;
  s.n2 = n2;
  steps.push_back(s);
}

void CompiledQuery::Join(const TabID &t1, const TabID &t2) {
  for (auto &step : steps)
    if (step.type == StepType::TMP_TABLE && step.t1 == t1) {
      step.tables1.push_back(t2);
      for (auto &it : steps_tmp_tables) {
        if (it.t1 == t1) {
          it.tables1.push_back(t2);
          break;
        }
      }
      break;
    }
  CompiledQuery::CQStep s;
  s.type = StepType::JOIN_T;
  s.t1 = t1;
  s.t2 = t2;
  steps.push_back(s);
}

void CompiledQuery::LeftJoinOn(const TabID &temp_table, std::vector<TabID> &left_tables,
                               std::vector<TabID> &right_tables, const CondID &cond_id) {
  CompiledQuery::CQStep s;
  s.type = StepType::LEFT_JOIN_ON;
  s.t1 = temp_table;
  s.c1 = cond_id;
  s.tables1 = left_tables;
  s.tables2 = right_tables;
  steps.push_back(s);
}

void CompiledQuery::InnerJoinOn(const TabID &temp_table, std::vector<TabID> &left_tables,
                                std::vector<TabID> &right_tables, const CondID &cond_id) {
  CompiledQuery::CQStep s;
  s.type = StepType::INNER_JOIN_ON;
  s.t1 = temp_table;
  s.c1 = cond_id;
  s.tables1 = left_tables;
  s.tables1.insert(s.tables1.end(), right_tables.begin(), right_tables.end());
  steps.push_back(s);
}

void CompiledQuery::AddConds(const TabID &t1, const CondID &c1, CondType cond_type) {
  if (c1.IsNull())
    return;
  CompiledQuery::CQStep s;
  s.type = StepType::ADD_CONDS;
  s.t1 = t1;
  s.c1 = c1;
  s.n1 = static_cast<int64_t>(cond_type);
  steps.push_back(s);
}

void CompiledQuery::ApplyConds(const TabID &t1) {
  CompiledQuery::CQStep s;
  s.type = StepType::APPLY_CONDS;
  s.t1 = t1;
  steps.push_back(s);
}

void CompiledQuery::AddColumn(AttrID &a_out, const TabID &t1, CQTerm e1, common::ColOperation op, char const alias[],
                              bool distinct, SI *si) {
  DEBUG_ASSERT(t1.n < 0 && NumOfTabs() > 0);
  CompiledQuery::CQStep s;
  s.type = StepType::ADD_COLUMN;
  s.a1 = a_out = NextAttrID(t1);
  s.t1 = t1;
  s.e1 = e1;
  s.cop = op;
  if (op == common::ColOperation::GROUP_CONCAT && si != nullptr)
    s.si = *si;
  if (alias) {
    size_t const alias_ct(std::strlen(alias) + 1);
    s.alias = new char[alias_ct];
    std::strcpy(s.alias, alias);
  } else
    s.alias = nullptr;
  s.n1 = distinct ? 1 : 0;
  steps.push_back(s);
  if (op == common::ColOperation::GROUP_BY)
    steps_group_by_cols.push_back(s);
}

void CompiledQuery::CreateVirtualColumn(AttrID &a_out, const TabID &t1, MysqlExpression *expr, const TabID &src_tab) {
  DEBUG_ASSERT(t1.n < 0 && NumOfTabs() > 0);
  CompiledQuery::CQStep s;
  s.type = StepType::CREATE_VC;
  s.a1 = a_out = NextVCID(t1);
  s.t1 = t1;
  s.t2 = src_tab;
  s.mysql_expr.push_back(expr);
  steps.push_back(s);
}

void CompiledQuery::CreateVirtualColumn(AttrID &a_out, const TabID &t1, const TabID &subquery, bool on_result) {
  DEBUG_ASSERT(t1.n < 0 && NumOfTabs() > 0);
  CompiledQuery::CQStep s;
  s.type = StepType::CREATE_VC;
  s.a1 = a_out = NextVCID(t1);
  s.t1 = t1;
  s.t2 = subquery;
  s.n1 = on_result ? 1 : 0;
  steps.push_back(s);
}

void CompiledQuery::CreateVirtualColumn(AttrID &a_out, const TabID &t1, std::vector<int> &vcs, const AttrID &vc_id) {
  DEBUG_ASSERT(t1.n < 0 && NumOfTabs() > 0);
  CompiledQuery::CQStep s;
  s.type = StepType::CREATE_VC;
  s.a1 = a_out = NextVCID(t1);
  s.a2 = vc_id;
  s.t1 = t1;
  s.virt_cols = vcs;
  steps.push_back(s);
}

void CompiledQuery::CreateVirtualColumn(int &a_out, const TabID &t1, const TabID &table_alias,
                                        const AttrID &col_number) {
  DEBUG_ASSERT(t1.n < 0 && NumOfTabs() > 0);
  CompiledQuery::CQStep s;
  s.type = StepType::CREATE_VC;
  s.a1 = NextVCID(t1);
  a_out = s.a1.n;
  s.a2 = col_number;
  s.t1 = t1;
  s.t2 = table_alias;
  steps.push_back(s);
}

void CompiledQuery::Add_Order(const TabID &t1, const AttrID &vc,
                              int d)  // d=1 for descending
{
  CompiledQuery::CQStep s;
  s.type = StepType::ADD_ORDER;
  s.t1 = t1;
  s.a1 = vc;
  s.n1 = d;
  steps.push_back(s);
}

void CompiledQuery::Union(TabID &t_out, const TabID &t2, const TabID &t3, int all) {
  CompiledQuery::CQStep s;
  s.type = StepType::UNION;
  if (t_out.n != t2.n)
    s.t1 = t_out = NextTabID();
  else
    s.t1 = t2;
  s.t2 = t2;
  s.t3 = t3;
  s.n1 = all;
  steps.push_back(s);
}

void CompiledQuery::Result(const TabID &t1) {
  CompiledQuery::CQStep s;
  s.type = StepType::RESULT;
  s.t1 = t1;
  steps.push_back(s);
}

void CompiledQuery::Print(Query *query) {
  for (size_t i = 0; i < steps.size(); i++) steps[i].Print(query);
}

bool CompiledQuery::CountColumnOnly(const TabID &table) {
  CompiledQuery::CQStep step;
  bool count_only = false;
  for (int i = 0; i < NumOfSteps(); i++) {
    step = Step(i);
    if (step.type == CompiledQuery::StepType::ADD_COLUMN && step.t1 == table &&
        step.cop == common::ColOperation::COUNT && step.e1.IsNull())
      count_only = true;
    if (step.type == CompiledQuery::StepType::ADD_COLUMN && step.t1 == table &&
        (step.cop != common::ColOperation::COUNT || (step.cop == common::ColOperation::COUNT && !step.e1.IsNull()))) {
      count_only = false;
      break;
    }
  }
  return count_only;
}

bool CompiledQuery::NoAggregationOrderingAndDistinct(int table) {
  CompiledQuery::CQStep step;
  for (int i = 0; i < NumOfSteps(); i++) {
    step = Step(i);
    if (step.type == CompiledQuery::StepType::ADD_ORDER && step.t1.n == table)
      return false;  // exclude ordering
    if (step.type == CompiledQuery::StepType::ADD_COLUMN && step.t1.n == table &&
        step.cop != common::ColOperation::LISTING)
      return false;  // exclude all kinds of aggregations
    if (step.type == CompiledQuery::StepType::T_MODE && step.t1.n == table && step.tmpar == TMParameter::TM_DISTINCT)
      return false;  // exclude DISTINCT
  }
  return true;
}

int64_t CompiledQuery::FindLimit(int table) {
  CompiledQuery::CQStep step;
  for (int i = 0; i < NumOfSteps(); i++) {
    step = Step(i);
    if (step.type == CompiledQuery::StepType::T_MODE && step.t1.n == table && step.tmpar == TMParameter::TM_TOP)
      return step.n1 + step.n2;  // n1 - omitted, n2 - displayed, i.e. either
                                 // ...LIMIT n2; or  ...LIMIT n1, n2;
  }
  return -1;
}

bool CompiledQuery::FindDistinct(int table) {
  CompiledQuery::CQStep step;
  for (int i = 0; i < NumOfSteps(); i++) {
    step = Step(i);
    if (step.type == CompiledQuery::StepType::T_MODE && step.t1.n == table && step.tmpar == TMParameter::TM_DISTINCT)
      return true;
  }
  return false;
}

std::set<int> CompiledQuery::GetUsedDims(const TabID &table_id, std::vector<std::shared_ptr<JustATable>> &ta) {
  std::set<int> result;
  auto itsteps = TabIDSteps.equal_range(table_id);
  for (auto it = itsteps.first; it != itsteps.second; ++it) {
    CompiledQuery::CQStep step = it->second;
    if (step.type == CompiledQuery::StepType::ADD_COLUMN && step.t1 == table_id &&
        step.e1.vc_id != common::NULL_VALUE_32) {
      vcolumn::VirtualColumn *vc = ((TempTable *)ta[-table_id.n - 1].get())->GetVirtualColumn(step.e1.vc_id);
      if (vc) {
        auto local = vc->GetDimensions();
        result.insert(local.begin(), local.end());
      } else {
        // VC does not exist since it is created in latter compilation step
        // temporary solution is to skip optimization
        result.clear();
        result.insert(common::NULL_VALUE_32);
        return result;
      }
    } else {
      bool is_group_by = IsGroupByQuery(table_id);
      if (!is_group_by && step.type == CompiledQuery::StepType::ADD_ORDER && step.t1 == table_id &&
          step.a1.n != common::NULL_VALUE_32) {
        vcolumn::VirtualColumn *vc = ((TempTable *)ta[-table_id.n - 1].get())->GetVirtualColumn(step.a1.n);
        if (vc) {
          auto local = vc->GetDimensions();
          result.insert(local.begin(), local.end());
        } else {
          // VC does not exist since it is created in latter compilation step
          // temporary solution is to skip optimization
          result.clear();
          result.insert(common::NULL_VALUE_32);
          return result;
        }
      }
    }
  }
  return result;
}

void CompiledQuery::BuildTableIDStepsMap() {
  CompiledQuery::CQStep step;
  for (int i = 0; i < NumOfSteps(); i++) {
    step = Step(i);
    TabIDSteps.emplace(step.t1, step);
  }
}

bool CompiledQuery::IsGroupByQuery(const TabID &tab_id) {
  for (auto &i : steps_group_by_cols) {
    if (i.t1 == tab_id)
      return true;
  }
  return false;
}

bool CompiledQuery::ExistsInTempTable(const TabID &tab_id, const TabID &tmp_table) {
  CompiledQuery::CQStep step;
  if (tab_id == tmp_table)
    return true;
  for (auto &i : steps_tmp_tables) {
    if (i.t1 == tmp_table) {
      step = i;
      for (auto &j : step.tables1)
        if (j == tab_id)
          return true;
    }
  }
  return false;
}

TabID CompiledQuery::FindSourceOfParameter(const TabID &tab_id, const TabID &tmp_table, bool &is_group_by) {
  DEBUG_ASSERT(tmp_table.n < 0);
  is_group_by = false;
  for (int x = tmp_table.n + 1; x < 0; x++) {
    if (IsTempTable(TabID(x)) && ExistsInTempTable(tab_id, TabID(x))) {
      x = FindRootTempTable(x);
      is_group_by = IsGroupByQuery(TabID(x));
      return TabID(x);
    }
  }
  DEBUG_ASSERT(!"Invalid parameter");
  return TabID(0);
}

bool CompiledQuery::IsTempTable(const TabID &t) {
  for (int i = 0; i < NumOfSteps(); i++) {
    if (Step(i).type == CompiledQuery::StepType::TMP_TABLE && Step(i).t1 == t)
      return true;
  }
  return false;
}

int CompiledQuery::FindRootTempTable(int tab_id) {
  DEBUG_ASSERT(tab_id < 0);
  for (int x = tab_id + 1; x < 0; x++) {
    if (IsTempTable(TabID(x)) && ExistsInTempTable(TabID(tab_id), TabID(x)))
      return FindRootTempTable(x);
  }
  return tab_id;
}

bool CompiledQuery::IsResultTable(const TabID &t) {
  for (int i = 0; i < NumOfSteps(); i++) {
    if (Step(i).type == CompiledQuery::StepType::RESULT && Step(i).t1 == t) {
      return true;
    }
  }
  return false;
}

bool CompiledQuery::IsOrderedBy(const TabID &t) {
  for (int i = 0; i < NumOfSteps(); i++) {
    if (Step(i).type == CompiledQuery::StepType::ADD_ORDER && Step(i).t1 == t) {
      return true;
    }
  }
  return false;
}

std::pair<int64_t, int64_t> CompiledQuery::GetGlobalLimit() {
  // global limit is the step just before RESULT and for the same table as
  // RESULT

  int i;
  for (i = 0; i < NumOfSteps(); i++) {
    if (Step(i).type == CompiledQuery::StepType::RESULT) {
      break;
    }
  }
  DEBUG_ASSERT(i < NumOfSteps());
  TabID res = Step(i).t1;
  if (i > 0 && Step(i - 1).type == CompiledQuery::StepType::T_MODE && Step(i - 1).t1 == res) {
    return std::pair<int64_t, int64_t>(Step(i - 1).n1, Step(i - 1).n2);
  }
  return std::pair<int64_t, int64_t>(0, -1);
}

int CompiledQuery::GetNumOfDimens(const TabID &tab_id) {
  for (int i = 0; i < NumOfSteps(); i++) {
    if (Step(i).type == CompiledQuery::StepType::TMP_TABLE && Step(i).t1 == tab_id) {
      return int(Step(i).tables1.size());
    }
  }
  return -1;
}

TabID CompiledQuery::GetTableOfCond(const CondID &cond_id) {
  for (int i = 0; i < NumOfSteps(); i++) {
    if (Step(i).type == CompiledQuery::StepType::CREATE_CONDS && Step(i).c1 == cond_id)
      return Step(i).t1;
  }
  return TabID();
}
}  // namespace core
}  // namespace Tianmu
