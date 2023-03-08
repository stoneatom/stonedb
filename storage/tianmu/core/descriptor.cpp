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

#include "descriptor.h"

#include "core/compiled_query.h"
#include "core/condition_encoder.h"
#include "core/parameterized_filter.h"
#include "core/query.h"
#include "core/query_operator.h"
#include "core/rough_value.h"
#include "core/transaction.h"
#include "vc/const_column.h"
#include "vc/const_expr_column.h"
#include "vc/expr_column.h"
#include "vc/in_set_column.h"
#include "vc/single_column.h"
#include "vc/subselect_column.h"
#include "vc/type_cast_column.h"

namespace Tianmu {
namespace core {

inline bool IsVirtualColumnEqual(vcolumn::VirtualColumn *vc_left, vcolumn::VirtualColumn *vc_right) {
  if (vc_left && vc_left->IsTypeCastColumn() && vc_right && vc_right->IsTypeCastColumn()) {
    return vc_left->GetChildren() == vc_right->GetChildren();
  }
  return vc_left == vc_right;
}

inline bool IsDescriptorCQTermEqual(const CQTerm &cq_left, const CQTerm &cs_right) {
  return cq_left.type == cs_right.type && cq_left.vc_id == cs_right.vc_id &&
         IsVirtualColumnEqual(cq_left.vc, cs_right.vc);
}

Descriptor::Descriptor()
    : lop(common::LogicalOperator::O_AND),
      sharp(false),
      encoded(false),
      done(false),
      evaluation(0),
      delayed(false),
      table(nullptr),
      tree(nullptr),
      left_dims(0),
      right_dims(0),
      rv(common::RoughSetValue::RS_UNKNOWN),
      like_esc('\\'),
      desc_t(DescriptorJoinType::DT_NOT_KNOWN_YET),
      collation(DTCollation()),
      null_after_simplify(false) {
  op = common::Operator::O_UNKNOWN_FUNC;
}

Descriptor::Descriptor(TempTable *t, int no_dims)  // no_dims is a destination number of dimensions
                                                   // (multiindex may be smaller at this point)
    : op(common::Operator::O_OR_TREE),
      lop(common::LogicalOperator::O_AND),
      sharp(false),
      encoded(false),
      done(false),
      evaluation(0),
      delayed(false),
      table(t),
      tree(nullptr),
      left_dims(no_dims),
      right_dims(no_dims),
      rv(common::RoughSetValue::RS_UNKNOWN),
      like_esc('\\'),
      desc_t(DescriptorJoinType::DT_NOT_KNOWN_YET),
      collation(DTCollation()),
      null_after_simplify(false) {
  DEBUG_ASSERT(table);
}

Descriptor::~Descriptor() { delete tree; }

Descriptor::Descriptor(const Descriptor &desc) {
  attr = desc.attr;
  op = desc.op;
  lop = desc.lop;
  val1 = desc.val1;
  val2 = desc.val2;
  sharp = desc.sharp;
  encoded = desc.encoded;
  done = desc.done;
  evaluation = desc.evaluation;
  desc_t = desc.desc_t;
  delayed = desc.delayed;
  table = desc.table;
  collation = desc.collation;
  tree = nullptr;
  if (desc.tree)
    tree = new DescTree(*desc.tree);
  left_dims = desc.left_dims;
  right_dims = desc.right_dims;
  rv = desc.rv;
  like_esc = desc.like_esc;
  null_after_simplify = desc.null_after_simplify;
}

Descriptor::Descriptor(CQTerm e1, common::Operator pr, CQTerm e2, CQTerm e3, TempTable *t, int no_dims,
                       char like_escape)
    : lop(common::LogicalOperator::O_AND),
      sharp(false),
      encoded(false),
      done(false),
      evaluation(0),
      delayed(false),
      table(t),
      tree(nullptr),
      left_dims(no_dims),
      right_dims(no_dims),
      rv(common::RoughSetValue::RS_UNKNOWN),
      desc_t(DescriptorJoinType::DT_NOT_KNOWN_YET),
      collation(DTCollation()),
      null_after_simplify(false) {
  op = pr;
  attr = e1;
  val1 = e2;
  val2 = e3;
  like_esc = like_escape, CalculateJoinType();
}

Descriptor::Descriptor(DescTree *sec_tree, TempTable *t, int no_dims)
    : op(common::Operator::O_OR_TREE),
      lop(common::LogicalOperator::O_AND),
      sharp(false),
      encoded(false),
      done(false),
      evaluation(0),
      delayed(false),
      table(t),
      left_dims(no_dims),
      right_dims(no_dims),
      rv(common::RoughSetValue::RS_UNKNOWN),
      like_esc('\\'),
      desc_t(DescriptorJoinType::DT_NOT_KNOWN_YET),
      collation(DTCollation()),
      null_after_simplify(false) {
  tree = nullptr;
  if (sec_tree)
    tree = new DescTree(*sec_tree);
  CalculateJoinType();
}

Descriptor::Descriptor(TempTable *t, vcolumn::VirtualColumn *v1, common::Operator pr, vcolumn::VirtualColumn *v2,
                       vcolumn::VirtualColumn *v3)
    : lop(common::LogicalOperator::O_AND),
      sharp(false),
      encoded(false),
      done(false),
      evaluation(0),
      delayed(false),
      table(t),
      tree(nullptr),
      left_dims(0),
      right_dims(0),
      rv(common::RoughSetValue::RS_UNKNOWN),
      like_esc('\\'),
      desc_t(DescriptorJoinType::DT_NOT_KNOWN_YET),
      collation(DTCollation()),
      null_after_simplify(false) {
  attr = CQTerm();
  val1 = CQTerm();
  val2 = CQTerm();
  attr.vc = v1;
  val1.vc = v2;
  val2.vc = v3;
  op = pr;
  CalculateJoinType();
}

void Descriptor::swap(Descriptor &d) {
  std::swap(op, d.op);
  std::swap(lop, d.lop);
  std::swap(attr, d.attr);
  std::swap(val1, d.val1);
  std::swap(val2, d.val2);
  std::swap(sharp, d.sharp);
  std::swap(encoded, d.encoded);
  std::swap(done, d.done);
  std::swap(evaluation, d.evaluation);
  std::swap(delayed, d.delayed);
  std::swap(rv, d.rv);
  std::swap(like_esc, d.like_esc);
  std::swap(table, d.table);
  std::swap(tree, d.tree);
  std::swap(left_dims, d.left_dims);
  std::swap(right_dims, d.right_dims);
  std::swap(desc_t, d.desc_t);
  std::swap(collation, d.collation);
  std::swap(null_after_simplify, d.null_after_simplify);
}

Descriptor &Descriptor::operator=(const Descriptor &d) {
  if (&d == this)
    return *this;
  op = d.op;
  lop = d.lop;
  attr = d.attr;
  val1 = d.val1;
  val2 = d.val2;
  sharp = d.sharp;
  encoded = d.encoded;
  done = d.done;
  evaluation = d.evaluation;
  delayed = d.delayed;
  rv = d.rv;
  like_esc = d.like_esc;
  table = d.table;
  delete tree;
  tree = nullptr;
  if (d.tree)
    tree = new DescTree(*d.tree);
  left_dims = d.left_dims;
  right_dims = d.right_dims;
  desc_t = d.desc_t;
  collation = d.collation;
  null_after_simplify = d.null_after_simplify;
  return *this;
}

int Descriptor::operator==(const Descriptor &sec) const {
  return (attr == sec.attr && op == sec.op && lop == sec.lop && IsDescriptorCQTermEqual(val1, sec.val1) &&
          IsDescriptorCQTermEqual(val2, sec.val2) && sharp == sec.sharp && encoded == sec.encoded &&
          delayed == sec.delayed && like_esc == sec.like_esc && table == sec.table &&
          left_dims == sec.left_dims &&    // check: dims order in vectors can be different
          right_dims == sec.right_dims &&  // check: dims order in vectors can be different
          collation.collation == sec.collation.collation && collation.derivation == sec.collation.derivation &&
          null_after_simplify == sec.null_after_simplify);
}

bool Descriptor::operator<=(const Descriptor &sec) const {
  if (*this == sec)
    return true;
  MIIterator dummy_mit;
  if (attr == sec.attr && (!val1.vc || val1.vc->IsConst()) && (!val2.vc || val2.vc->IsConst()) &&
      (!sec.val1.vc || sec.val1.vc->IsConst()) && (!sec.val2.vc || sec.val2.vc->IsConst())) {
    switch (op) {
      case common::Operator::O_EQ:
        if (sec.op == common::Operator::O_BETWEEN && val1.vc->GetValue(dummy_mit) >= sec.val1.vc->GetValue(dummy_mit) &&
            val1.vc->GetValue(dummy_mit) <= sec.val2.vc->GetValue(dummy_mit))
          return true;
        if (sec.op == common::Operator::O_IN &&
            static_cast<vcolumn::MultiValColumn *>(sec.val1.vc)
                    ->Contains(dummy_mit, *val1.vc->GetValue(dummy_mit).Get()) == true)
          return true;
        if ((sec.op == common::Operator::O_LESS || sec.op == common::Operator::O_LESS_EQ ||
             sec.op == common::Operator::O_MORE || sec.op == common::Operator::O_MORE_EQ ||
             sec.op == common::Operator::O_EQ) &&
            types::TianmuValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), sec.op,
                                              '\\'))
          return true;
        break;
      case common::Operator::O_LESS_EQ:
        if ((sec.op == common::Operator::O_LESS || sec.op == common::Operator::O_LESS_EQ) &&
            types::TianmuValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), sec.op,
                                              '\\'))
          return true;
        break;
      case common::Operator::O_MORE_EQ:
        if ((sec.op == common::Operator::O_MORE || sec.op == common::Operator::O_MORE_EQ) &&
            types::TianmuValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), sec.op,
                                              '\\'))
          return true;
        break;
      case common::Operator::O_LESS:
        if ((sec.op == common::Operator::O_LESS || sec.op == common::Operator::O_LESS_EQ) &&
            types::TianmuValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit),
                                              common::Operator::O_LESS_EQ, '\\'))
          return true;
        break;
      case common::Operator::O_MORE:
        if ((sec.op == common::Operator::O_MORE || sec.op == common::Operator::O_MORE_EQ) &&
            types::TianmuValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit),
                                              common::Operator::O_MORE_EQ, '\\'))
          return true;
        break;
      case common::Operator::O_BETWEEN:
        if (sec.op == common::Operator::O_BETWEEN && val1.vc->GetValue(dummy_mit) >= sec.val1.vc->GetValue(dummy_mit) &&
            val2.vc->GetValue(dummy_mit) <= sec.val2.vc->GetValue(dummy_mit))
          return true;
        if ((sec.op == common::Operator::O_LESS || sec.op == common::Operator::O_LESS_EQ) &&
            types::TianmuValueObject::compare(val2.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), sec.op,
                                              '\\'))
          return true;
        if ((sec.op == common::Operator::O_MORE || sec.op == common::Operator::O_MORE_EQ) &&
            types::TianmuValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), sec.op,
                                              '\\'))
          return true;
        break;
      case common::Operator::O_IN: {
        // vcolumn::MultiValColumn* mvc =
        // static_cast<vcolumn::MultiValColumn*>(val1.vc); if( sec.op ==
        // common::Operator::O_EQ && mvc->Contains(dummy_mit,
        // *sec.val1.vc->GetValue(dummy_mit).Get()) == true )
        //	return true;
        break;
      }
      default:
        break;
    }
  }
  return false;
}

bool Descriptor::operator<(const Descriptor &desc) const { return evaluation < desc.evaluation; }

bool Descriptor::EqualExceptOuter(const Descriptor &sec) {
  return (attr == sec.attr && op == sec.op && lop == sec.lop && val1 == sec.val1 && val2 == sec.val2 &&
          sharp == sec.sharp && like_esc == sec.like_esc && table == sec.table &&
          collation.collation == sec.collation.collation && collation.derivation == sec.collation.derivation &&
          null_after_simplify == sec.null_after_simplify);
}

namespace {
void SwitchOperator(common::Operator &op) {
  if (op == common::Operator::O_LESS)
    op = common::Operator::O_MORE;
  else if (op == common::Operator::O_MORE)
    op = common::Operator::O_LESS;
  else if (op == common::Operator::O_LESS_EQ)
    op = common::Operator::O_MORE_EQ;
  else if (op == common::Operator::O_MORE_EQ)
    op = common::Operator::O_LESS_EQ;
}
}  // namespace

void Descriptor::SwitchSides()  // change "a<b" into "b>a" etc; throw error if
                                // not possible (e.g. between)
{
  DEBUG_ASSERT(op == common::Operator::O_EQ || op == common::Operator::O_NOT_EQ || op == common::Operator::O_LESS ||
               op == common::Operator::O_MORE || op == common::Operator::O_LESS_EQ ||
               op == common::Operator::O_MORE_EQ);
  SwitchOperator(op);
  CQTerm p = attr;
  attr = val1;
  val1 = p;
}

bool Descriptor::IsType_AttrValOrAttrValVal() const  // true if "phys column op val or column between val and val or "
{
  // or "phys_column IS nullptr/NOT nullptr"
  if (attr.vc == nullptr || !static_cast<int>(attr.vc->IsSingleColumn()))
    return false;
  return ((val1.vc && val1.vc->IsConst()) ||
          (op == common::Operator::O_IS_NULL || op == common::Operator::O_NOT_NULL)) &&
         (!val2.vc || (val2.vc && val2.vc->IsConst()));
}

bool Descriptor::IsType_AttrMultiVal() const {
  return op != common::Operator::O_BETWEEN && op != common::Operator::O_NOT_BETWEEN && attr.vc &&
         static_cast<int>(attr.vc->IsSingleColumn()) && val1.vc && val1.vc->IsMultival() && val1.vc->IsConst();
}

bool Descriptor::IsType_Subquery() {
  return ((attr.vc && attr.vc->IsSubSelect()) || (val1.vc && val1.vc->IsSubSelect()) ||
          (val2.vc && val2.vc->IsSubSelect()));
}

bool Descriptor::IsType_JoinSimple() const  // true if more than one table involved
{
  DEBUG_ASSERT(desc_t != DescriptorJoinType::DT_NOT_KNOWN_YET);
  return desc_t == DescriptorJoinType::DT_SIMPLE_JOIN;
}

bool Descriptor::IsType_AttrAttr() const  // true if "column op column" from one table
{
  if (attr.vc == nullptr || val1.vc == nullptr || !static_cast<int>(attr.vc->IsSingleColumn()) ||
      !static_cast<int>(val1.vc->IsSingleColumn()) || val2.vc || IsType_Join())
    return false;

  return true;
}

bool Descriptor::IsType_TIANMUExpression() const  // only columns, constants and TIANMUExpressions
{
  if (attr.vc == nullptr)
    return false;
  if ((static_cast<int>(attr.vc->IsSingleColumn()) || attr.vc->IsConst()) &&
      (val1.vc == nullptr || static_cast<int>(val1.vc->IsSingleColumn()) || val1.vc->IsConst()) &&
      (val2.vc == nullptr || static_cast<int>(val2.vc->IsSingleColumn()) || val2.vc->IsConst()))
    return true;
  return false;
}

void Descriptor::CalculateJoinType() {
  if (IsType_OrTree()) {
    DimensionVector used_dims(right_dims.Size());  // the most current number of dimensions
    tree->DimensionUsed(used_dims);
    if (used_dims.NoDimsUsed() > 1) {
      desc_t = DescriptorJoinType::DT_COMPLEX_JOIN;
      left_dims = used_dims;
    } else
      desc_t = DescriptorJoinType::DT_NON_JOIN;
    return;
  }
  std::set<int> tables_a, tables_v1, tables_v2, tables_all;
  if (attr.vc) {
    tables_a = attr.vc->GetDimensions();
    tables_all.insert(tables_a.begin(), tables_a.end());
  }
  if (val1.vc) {
    tables_v1 = val1.vc->GetDimensions();
    tables_all.insert(tables_v1.begin(), tables_v1.end());
  }
  if (val2.vc) {
    tables_v2 = val2.vc->GetDimensions();
    tables_all.insert(tables_v2.begin(), tables_v2.end());
  }
  if (tables_all.size() <= 1)
    desc_t = DescriptorJoinType::DT_NON_JOIN;
  else if (tables_a.size() > 1 || tables_v1.size() > 1 || tables_v2.size() > 1 ||
           // 1 BETWEEN a1 AND b1
           (tables_a.size() == 0 && (op == common::Operator::O_BETWEEN || op == common::Operator::O_NOT_BETWEEN)))
    desc_t = DescriptorJoinType::DT_COMPLEX_JOIN;
  else
    desc_t = DescriptorJoinType::DT_SIMPLE_JOIN;
}

bool Descriptor::IsType_JoinComplex() const {
  // Make sure to use CalculateJoinType() before
  DEBUG_ASSERT(desc_t != DescriptorJoinType::DT_NOT_KNOWN_YET);
  return desc_t == DescriptorJoinType::DT_COMPLEX_JOIN;
}

common::RoughSetValue Descriptor::EvaluateRoughlyPack(const MIIterator &mit) {
  if (IsType_OrTree())
    return tree->root->EvaluateRoughlyPack(mit);
  common::RoughSetValue r = common::RoughSetValue::RS_SOME;
  if (attr.vc /*&& !attr.vc->IsConst()*/)
    r = attr.vc->RoughCheck(mit, *this);
  if (rv == common::RoughSetValue::RS_UNKNOWN)
    rv = r;
  else if (rv == common::RoughSetValue::RS_NONE && r != common::RoughSetValue::RS_NONE)
    rv = common::RoughSetValue::RS_SOME;
  else if (rv == common::RoughSetValue::RS_ALL && r != common::RoughSetValue::RS_ALL)
    rv = common::RoughSetValue::RS_SOME;
  return r;
}

void Descriptor::Simplify(bool in_having) {
  MEASURE_FET("Descriptor::Simplify(...)");
  static MIIterator const mit(nullptr, table->Getpackpower());
  if (op == common::Operator::O_FALSE || op == common::Operator::O_TRUE)
    return;

  if (IsType_OrTree()) {
    common::Tribool res = tree->Simplify(in_having);
    if (res == true)
      op = common::Operator::O_TRUE;
    else if (res == false)
      op = common::Operator::O_FALSE;
    else if (!tree->root->desc.IsType_OrTree()) {
      Descriptor new_desc = tree->root->desc;
      new_desc.left_dims = left_dims;
      new_desc.right_dims = right_dims;
      *this = new_desc;
    }
    return;
  }

  bool res = false;

  if (attr.vc && attr.vc->IsConst() && val1.vc && !val1.vc->IsConst() &&
      (op == common::Operator::O_EQ || op == common::Operator::O_NOT_EQ || op == common::Operator::O_LESS ||
       op == common::Operator::O_LESS_EQ || op == common::Operator::O_MORE || op == common::Operator::O_MORE_EQ)) {
    SwitchSides();
  }
  if (Query::IsAllAny(op) && dynamic_cast<vcolumn::MultiValColumn *>(val1.vc) == nullptr)
    Query::UnmarkAllAny(op);
  if ((attr.vc && (!attr.vc->IsConst() || (in_having && attr.vc->IsParameterized()))) ||
      (val1.vc && (!val1.vc->IsConst() || (in_having && val1.vc->IsParameterized()))) ||
      (val2.vc && (!val2.vc->IsConst() || (in_having && val2.vc->IsParameterized())))) {
    return;
  }

  // from here attr, val1 and val2 (if exists) are constants

  vcolumn::VirtualColumn *acc = attr.vc;
  vcolumn::VirtualColumn *v1cc = val1.vc;
  vcolumn::VirtualColumn *v2cc = val2.vc;

  if (op == common::Operator::O_BETWEEN && (v1cc->IsNull(mit) || v2cc->IsNull(mit))) {
    op = common::Operator::O_FALSE;
    null_after_simplify = true;
    return;
  } else if (op == common::Operator::O_NOT_BETWEEN && (v1cc->IsNull(mit) || v2cc->IsNull(mit))) {
    if (v1cc->IsNull(mit) && v2cc->IsNull(mit)) {
      op = common::Operator::O_FALSE;
      null_after_simplify = true;
      return;
    }
    if (v1cc->IsNull(mit)) {  // a not between null and x  ==>  a > x
      op = common::Operator::O_MORE;
      val1 = val2;
      v1cc = v2cc;
    } else  // a not between x and null  ==>  a < x
      op = common::Operator::O_LESS;
  }
  if (IsSetOperator(op)) {
    null_after_simplify = IsNull_Set(mit, op);
    res = CheckSetCondition(mit, op);
    op = res ? common::Operator::O_TRUE : common::Operator::O_FALSE;
    return;
  }

  switch (op) {
    case common::Operator::O_IS_NULL:
      res = acc->IsNull(mit) ? true : false;
      break;
    case common::Operator::O_NOT_EXISTS:
    case common::Operator::O_EXISTS: {
      vcolumn::MultiValColumn *mvc = static_cast<vcolumn::MultiValColumn *>(attr.vc);
      res = mvc->CheckExists(mit);
      if (op == common::Operator::O_NOT_EXISTS)
        res = !res;
      break;
    }
    case common::Operator::O_NOT_NULL:
      res = acc->IsNull(mit) ? false : true;
      break;
    case common::Operator::O_BETWEEN:
    case common::Operator::O_NOT_BETWEEN: {
      DEBUG_ASSERT(sharp == false);
      if (acc->IsNull(mit)) {
        null_after_simplify = true;
        res = false;
      } else if (ATI::IsTxtType(acc->TypeName()) || ATI::IsTxtType(v1cc->TypeName()) ||
                 ATI::IsTxtType(v2cc->TypeName())) {
        types::BString s1, s2, s3;
        acc->GetValueString(s1, mit);
        v1cc->GetValueString(s2, mit);
        v2cc->GetValueString(s3, mit);
        res = (s1.CompareWith(s2) >= 0 && s1.CompareWith(s3) <= 0);
      } else {
        types::TianmuValueObject rv1 = acc->GetValue(mit);
        types::TianmuValueObject rv2 = v1cc->GetValue(mit);
        types::TianmuValueObject rv3 = v2cc->GetValue(mit);
        res = types::TianmuValueObject::compare(rv1, rv2, common::Operator::O_MORE_EQ, '\\') &&
              types::TianmuValueObject::compare(rv1, rv3, common::Operator::O_LESS_EQ, '\\');
      }

      if (op == common::Operator::O_NOT_BETWEEN && !acc->IsNull(mit))
        res = !res;
      break;
    }
    default: {
      types::TianmuValueObject rv1 = acc->GetValue(mit);
      types::TianmuValueObject rv2 = v1cc->GetValue(mit);
      res = types::TianmuValueObject::compare(rv1, rv2, op, like_esc);
      if (res == false && (rv1.IsNull() || rv2.IsNull()))
        null_after_simplify = true;
    }
  }
  op = res ? common::Operator::O_TRUE : common::Operator::O_FALSE;
  return;
}

/**
Appends as string representation of this descriptor's operator
and data values to a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
*/
char *Descriptor::ToString(char buffer[], size_t buffer_size) {
  /*
  Only the first few of the several steps needed to completely
  refactor this code were needed to resolve the valgrind issue.  The
  next steps in refactoring this code would be done via 'Replace
  Conditional with Polymorphism'.
  i.e. Create the operator objects with a common appendToString()
  member overriden to function as one of the groups below and
  by moving append*ToString() calls into the QueryOperator
  base/derived classes (the operator_object parameter will
  not be needed, etc).
  One last note, the common::Operator classes would need to be the objects
  that are sentient of the relationships with the objects around them.
  */
  std::unique_ptr<const QueryOperator> operator_object(CreateQueryOperator(op));

  switch (operator_object->GetType()) {
    case common::Operator::O_ESCAPE:
    case common::Operator::O_EXISTS:
    case common::Operator::O_NOT_EXISTS: {
      size_t offset = std::strlen(buffer);
      AppendString(buffer, buffer_size, "(exists expr.)", 14, offset);
      break;
    }
    case common::Operator::O_OR_TREE: {
      size_t offset = std::strlen(buffer);
      AppendString(buffer, buffer_size, "(OR expr.)", 10, offset);
      // if(tree) {
      //	if(!tree->root->left)
      //		tree->root->desc.ToString(buffer, buffer_size);
      //	else {
      //		tree->root->left->desc.ToString(buffer, buffer_size);
      //		tree->root->right->desc.ToString(buffer, buffer_size);
      //	}
      //}
      break;
    }
    case common::Operator::O_FALSE:
    case common::Operator::O_TRUE:
      AppendConstantToString(buffer, buffer_size, operator_object.get());
      break;

    case common::Operator::O_IS_NULL:
    case common::Operator::O_NOT_NULL:
      AppendUnaryOperatorToString(buffer, buffer_size, operator_object.get());
      break;

    case common::Operator::O_EQ:
    case common::Operator::O_EQ_ALL:
    case common::Operator::O_EQ_ANY:
    case common::Operator::O_LESS:
    case common::Operator::O_LESS_ALL:
    case common::Operator::O_LESS_ANY:
    case common::Operator::O_MORE:
    case common::Operator::O_MORE_ALL:
    case common::Operator::O_MORE_ANY:
    case common::Operator::O_LESS_EQ:
    case common::Operator::O_LESS_EQ_ALL:
    case common::Operator::O_LESS_EQ_ANY:
    case common::Operator::O_MORE_EQ:
    case common::Operator::O_MORE_EQ_ALL:
    case common::Operator::O_MORE_EQ_ANY:
    case common::Operator::O_NOT_EQ:
    case common::Operator::O_NOT_EQ_ALL:
    case common::Operator::O_NOT_EQ_ANY:
    case common::Operator::O_IN:
    case common::Operator::O_NOT_IN:
    case common::Operator::O_LIKE:
    case common::Operator::O_NOT_LIKE:
      AppendBinaryOperatorToString(buffer, buffer_size, operator_object.get());
      break;

    case common::Operator::O_BETWEEN:
    case common::Operator::O_NOT_BETWEEN:
      AppendTernaryOperatorToString(buffer, buffer_size, operator_object.get());
      break;
    default:
      break;
  }
  if (!IsInner()) {
    std::sprintf(buffer + std::strlen(buffer), " (outer");
    for (int i = 0; i < right_dims.Size(); ++i)
      if (right_dims[i])
        std::sprintf(buffer + std::strlen(buffer), " %d", i);
    std::sprintf(buffer + std::strlen(buffer), ")");
  }
  return buffer;
}

/**
Creates a query operator of the requested type.

Interim implementation until an operator class hierarchy
is put in place.  This call is only a stop-gap measure
to that effect.

\param The common::Operator enumeration of the desired type of query operator.
*/
const QueryOperator *Descriptor::CreateQueryOperator(common::Operator type) const {
  const char *string_rep[static_cast<int>(common::Operator::OPERATOR_ENUM_COUNT)] = {
      "=",               // common::Operator::O_EQ
      "=ALL",            // common::Operator::O_EQ_ALL
      "=ANY",            // common::Operator::O_EQ_ANY
      "<>",              // common::Operator::O_NOT_EQ
      "<>ALL",           // common::Operator::O_NOT_EQ_ALL
      "<>ANY",           // common::Operator::O_NOT_EQ_ANY
      "<",               // common::Operator::O_LESS
      "<ALL",            // common::Operator::O_LESS_ALL
      "<ANY",            // common::Operator::O_LESS_ANY
      ">",               // common::Operator::O_MORE
      ">ALL",            // common::Operator::O_MORE_ALL
      ">ANY",            // common::Operator::O_MORE_ANY
      "<=",              // common::Operator::O_LESS_EQ
      "<=ALL",           // common::Operator::O_LESS_EQ_ALL
      "<=ANY",           // common::Operator::O_LESS_EQ_ANY
      ">=",              // common::Operator::O_MORE_EQ
      ">=ALL",           // common::Operator::O_MORE_EQ_ALL
      ">=ANY",           // common::Operator::O_MORE_EQ_ANY
      "IS nullptr",      // common::Operator::O_IS_NULL
      "IS NOT nullptr",  // common::Operator::O_NOT_NULL
      "BET.",            // common::Operator::O_BETWEEN
      "NOT BET.",        // common::Operator::O_NOT_BETWEEN
      "LIKE",            // common::Operator::O_LIKE
      "NOT LIKE",        // common::Operator::O_NOT_LIKE
      "IN",              // common::Operator::O_IN
      "NOT IN",          // common::Operator::O_NOT_IN
      "EXISTS",          // common::Operator::O_EXISTS
      "NOT EXISTS",      // common::Operator::O_NOT_EXISTS
      "FALSE",           // common::Operator::O_FALSE
      "TRUE",            // common::Operator::O_TRUE
      "ESCAPE",          // common::Operator::O_ESCAPE
      "OR TREE"          // common::Operator::O_OR_TREE
  };

  return new QueryOperator(type, string_rep[static_cast<int>(type)]);
}

/**
Simple string concatenation into a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
\param The string to concatenate.
\param The length of the string to be concatenated.
\param The destination offset within buffer for the concatenation.
*/
void Descriptor::AppendString(char *buffer, size_t buffer_size, const char *str, size_t string_length,
                              size_t offset) const {
  if ((offset + string_length) > buffer_size) {
    throw common::InternalException("Bounds Check in Descriptor::AppendString");
  }
  std::strcpy(buffer + offset, str);
}

/**
Appends a string representation of a constant operator into a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
\param The QueryOperator object managed by this Descriptor object.
*/
void Descriptor::AppendConstantToString(char buffer[], size_t size, const QueryOperator *operator_object) const {
  const std::string &constant = operator_object->AsString();

  size_t offset = std::strlen(buffer);

  AppendString(buffer, size, " ", 1, offset++);
  AppendString(buffer, size, constant.data(), constant.length(), offset);
}

/**
Appends a string representation of a unary operator (attr op)
into a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
\param The QueryOperator object managed by this Descriptor object.
*/
void Descriptor::AppendUnaryOperatorToString(char buffer[], size_t size, const QueryOperator *operator_object) const {
  attr.ToString(buffer, size, 0);
  AppendConstantToString(buffer, size, operator_object);
}

/**
Appends a string representation of a binary operator (attr op val1)
into a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
\param The QueryOperator object managed by this Descriptor object.
*/
void Descriptor::AppendBinaryOperatorToString(char buffer[], size_t size, const QueryOperator *operator_object) const {
  AppendUnaryOperatorToString(buffer, size, operator_object);
  AppendString(buffer, size, " ", 1, std::strlen(buffer));
  val1.ToString(buffer, size, 0);
}

/**
Appends a string representation of a special-case ternary
operator into a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
\param The QueryOperator object managed by this Descriptor object.
*/
void Descriptor::AppendTernaryOperatorToString(char buffer[], size_t size, const QueryOperator *operator_object) const {
  AppendBinaryOperatorToString(buffer, size, operator_object);
  size_t offset = std::strlen(buffer);
  AppendString(buffer, size, " AND ", 5, offset);
  val2.ToString(buffer, size, 0);
  if (sharp) {
    offset = std::strlen(buffer);
    AppendString(buffer, size, " (sharp)", 8, offset);
  }
}

void Descriptor::DimensionUsed(DimensionVector &dims) {
  if (tree)
    tree->DimensionUsed(dims);
  if (attr.vc)
    attr.vc->MarkUsedDims(dims);
  if (val1.vc)
    val1.vc->MarkUsedDims(dims);
  if (val2.vc)
    val2.vc->MarkUsedDims(dims);
}

void Descriptor::LockSourcePacks(const MIIterator &mit) {
  if (tree)
    tree->root->PrepareToLock(0);
  if (attr.vc)
    attr.vc->LockSourcePacks(mit);
  if (val1.vc)
    val1.vc->LockSourcePacks(mit);
  if (val2.vc)
    val2.vc->LockSourcePacks(mit);
}

void Descriptor::LockSourcePacks(const MIIterator &mit, [[maybe_unused]] int th_no) { LockSourcePacks(mit); }

void Descriptor::UnlockSourcePacks() {
  if (tree)
    tree->root->UnlockSourcePacks();
  if (attr.vc)
    attr.vc->UnlockSourcePacks();
  if (val1.vc)
    val1.vc->UnlockSourcePacks();
  if (val2.vc)
    val2.vc->UnlockSourcePacks();
}

void Descriptor::EvaluatePackImpl(MIUpdatingIterator &mit) {
  MEASURE_FET("Descriptor::EvaluatePackImpl(...)");

  // Check if we can delegate evaluation of descriptor to physical column
  if (encoded)
    attr.vc->EvaluatePack(mit, *this);
  else if (IsType_OrTree() && (GetParallelSize() == 0)) {
    // single thread Prepare rough values to be stored inside the tree
    tree->root->ClearRoughValues();
    tree->root->EvaluateRoughlyPack(mit);
    tree->root->EvaluatePack(mit);
  } else if (IsType_OrTree() && GetParallelSize()) {
    // muti thread for IsType_OrTree; single reserved
    tree->root->MClearRoughValues(mit.GetTaskId());
    tree->root->MEvaluateRoughlyPack(mit, mit.GetTaskId());
    tree->root->MEvaluatePack(mit, mit.GetTaskId());
  } else if (types::RequiresUTFConversions(collation)) {
    std::scoped_lock guard(mtx);
    while (mit.IsValid()) {
      if (CheckCondition_UTF(mit) == false)
        mit.ResetCurrent();
      ++mit;
      if (mit.PackrowStarted())
        break;
    }
  } else {
    if (IsType_Subquery() && op != common::Operator::O_OR_TREE) {
      // pack based optimization of corr. subq. by using RoughQuery
      common::Tribool res = RoughCheckSubselectCondition(mit, SubSelectOptimizationType::PACK_BASED);
      if (res == false)
        mit.ResetCurrentPack();
      else if (res == common::TRIBOOL_UNKNOWN) {
        // int true_c = 0, false_c = 0, unkn_c = 0;
        while (mit.IsValid()) {
          // row based optimization of corr. subq. by using RoughQuery
          res = RoughCheckSubselectCondition(mit, SubSelectOptimizationType::ROW_BASED);
          // if(res == false)
          //	false_c++;
          // else if(res == true)
          //	true_c++;
          // else
          //	unkn_c++;
          if (res == false)
            mit.ResetCurrent();
          else if (res == common::TRIBOOL_UNKNOWN && CheckCondition(mit) == false)
            mit.ResetCurrent();
          ++mit;
          if (mit.PackrowStarted())
            break;
        }
        // cout << "# of skipped subqueries: " << true_c << "/" << false_c <<
        // "/" << unkn_c
        // << " -> " << (true_c + false_c) << " / " << (true_c + false_c +
        // unkn_c) << endl;
      }
    } else {
      std::scoped_lock guard(mtx);
      while (mit.IsValid()) {
        if (CheckCondition(mit) == false)
          mit.ResetCurrent();
        ++mit;
        if (mit.PackrowStarted())
          break;
      }
    }
  }
}

void Descriptor::EvaluatePack(MIUpdatingIterator &mit) {
  MEASURE_FET("Descriptor::EvaluatePack(...)");
  if (GetParallelSize() == 0)
    LockSourcePacks(mit);
  else
    MLockSourcePacks(mit, mit.GetTaskId());
  EvaluatePackImpl(mit);
}

void Descriptor::UpdateVCStatistics()  // Apply all the information from
                                       // constants etc. to involved VC
{
  MEASURE_FET("Descriptor::UpdateVCStatistics(...)");
  if (attr.vc == nullptr)
    return;
  if (op == common::Operator::O_IS_NULL) {
    attr.vc->SetLocalNullsOnly(true);
    return;
  }
  attr.vc->SetLocalNullsPossible(false);
  if ((attr.vc->Type().IsNumeric() || attr.vc->Type().Lookup()) &&
      encoded) {  // if not encoded, we may have an incompatible comparison here
                  // (e.g. double between int and int)
    int64_t v1 = common::NULL_VALUE_64;
    int64_t v2 = common::NULL_VALUE_64;
    if (op == common::Operator::O_BETWEEN) {
      if (val1.vc)
        v1 = val1.vc->RoughMin();
      if (val2.vc)
        v2 = val2.vc->RoughMax();
    } else if (op == common::Operator::O_EQ) {
      if (val1.vc) {
        v1 = val1.vc->RoughMin();
        v2 = val1.vc->RoughMax();
        val1.vc->SetLocalMinMax(v1, v2);  // apply to both sides
      }
    } else if (op == common::Operator::O_LESS || op == common::Operator::O_LESS_EQ) {
      if (val1.vc)
        v2 = val1.vc->RoughMax();
    } else if (op == common::Operator::O_MORE || op == common::Operator::O_MORE_EQ) {
      if (val1.vc)
        v1 = val1.vc->RoughMin();
    }
    int v1_scale = val1.vc ? val1.vc->Type().GetScale() : 0;
    int v2_scale = val2.vc ? val2.vc->Type().GetScale() : v1_scale;
    types::TianmuNum v1_conv(v1, v1_scale);
    types::TianmuNum v2_conv(v2, v2_scale);
    if (v1 != common::NULL_VALUE_64 && v1 != common::PLUS_INF_64 && v1 != common::MINUS_INF_64)
      v1_conv = v1_conv.ToDecimal(attr.vc->Type().GetScale());
    if (v2 != common::NULL_VALUE_64 && v2 != common::PLUS_INF_64 && v2 != common::MINUS_INF_64)
      v2_conv = v2_conv.ToDecimal(attr.vc->Type().GetScale());
    attr.vc->SetLocalMinMax(v1_conv.ValueInt(), v2_conv.ValueInt());
  }
  if (op == common::Operator::O_IN && val1.vc->IsConst()) {
    vcolumn::MultiValColumn *mv_vc = static_cast<vcolumn::MultiValColumn *>(val1.vc);
    MIDummyIterator mit(1);
    int64_t v = mv_vc->NumOfValues(mit);
    attr.vc->SetLocalDistVals(v);
  }
  // TODO: all other info, e.g.:
  //       - min/max from sets
  //       - max_len from string equalities and LIKE
  //       - second column stats for attr-attr inequalities
}

bool Descriptor::CheckCondition_UTF(const MIIterator &mit) {
  MEASURE_FET("Descriptor::CheckCondition_UTF(...)");
  if (op == common::Operator::O_TRUE)
    return true;
  else if (op == common::Operator::O_FALSE)
    return false;

  bool result = true;

  if (encoded && attr.vc->Type().Lookup())  // encoded to numerical comparison
    return CheckCondition(mit);

  // Assumption: LockSourcePack externally done.
  if (op == common::Operator::O_EQ) {  // fast track for the most common operator
    DEBUG_ASSERT(attr.vc && val1.vc && types::RequiresUTFConversions(collation));
    if (attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
      return false;
    types::BString s1, s2;
    attr.vc->GetNotNullValueString(s1, mit);
    val1.vc->GetNotNullValueString(s2, mit);
    return CollationStrCmp(collation, s1, s2) == 0;
  } else if (op == common::Operator::O_NOT_NULL) {
    if (attr.vc->IsNull(mit))
      return false;
  } else if (op == common::Operator::O_IS_NULL) {
    if (!attr.vc->IsNull(mit))
      return false;
  } else if (op == common::Operator::O_EXISTS || op == common::Operator::O_NOT_EXISTS) {
    DEBUG_ASSERT(dynamic_cast<vcolumn::SubSelectColumn *>(attr.vc));
    vcolumn::SubSelectColumn *sub = static_cast<vcolumn::SubSelectColumn *>(attr.vc);
    bool is_nonempty = sub->CheckExists(mit);
    if ((op == common::Operator::O_EXISTS && !is_nonempty) || (op == common::Operator::O_NOT_EXISTS && is_nonempty))
      return false;
  } else if (op == common::Operator::O_BETWEEN || op == common::Operator::O_NOT_BETWEEN) {
    if (attr.vc->IsNull(mit))
      return false;
    // need to consider three value logic
    types::BString s1, s2, s3;
    attr.vc->GetNotNullValueString(s1, mit);
    val1.vc->GetValueString(s2, mit);
    val2.vc->GetValueString(s3, mit);
    bool attr_ge_val1 = (sharp ? CollationStrCmp(collation, s1, s2) > 0 : CollationStrCmp(collation, s1, s2) >= 0);
    bool attr_le_val2 = (sharp ? CollationStrCmp(collation, s1, s3) < 0 : CollationStrCmp(collation, s1, s3) <= 0);
    common::Tribool val1_res, val2_res;
    if (encoded) {  // Rare case: for encoded conditions treat nullptr as +/- inf.
      val1_res = val1.vc->IsNull(mit) ? true : common::Tribool(attr_ge_val1);
      val2_res = val2.vc->IsNull(mit) ? true : common::Tribool(attr_le_val2);
    } else {
      val1_res = val1.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN : common::Tribool(attr_ge_val1);
      val2_res = val2.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN : common::Tribool(attr_le_val2);
    }
    if (op == common::Operator::O_BETWEEN) {
      if (val1_res != true || val2_res != true)
        return false;
    } else {
      if (val1_res != false && val2_res != false)
        return false;
    }
  } else if (IsSetOperator(op)) {
    DEBUG_ASSERT(attr.vc && dynamic_cast<vcolumn::MultiValColumn *>(val1.vc));
    return CheckSetCondition_UTF(mit, op);
  } else if (op == common::Operator::O_LIKE || op == common::Operator::O_NOT_LIKE) {
    if (attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
      return false;
    types::BString v, pattern;
    attr.vc->GetNotNullValueString(v, mit);
    val1.vc->GetNotNullValueString(pattern, mit);
    int x = common::wildcmp(collation, v.val_, v.val_ + v.len_, pattern.val_, pattern.val_ + pattern.len_, like_esc,
                            '_', '%');
    result = (x == 0 ? true : false);
    if (op == common::Operator::O_LIKE)
      return result;
    else
      return !result;
  } else if (IsType_OrTree()) {
    DEBUG_ASSERT(tree);
    return tree->root->CheckCondition((MIIterator &)mit);
  } else {  // all other logical operators: >, >=, <, <=
    DEBUG_ASSERT(attr.vc && val1.vc);
    if (attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
      return false;
    types::BString s1, s2;
    attr.vc->GetNotNullValueString(s1, mit);
    val1.vc->GetNotNullValueString(s2, mit);
    if (!CollationStrCmp(collation, s1, s2, op))
      return false;
  }
  return result;
}

bool Descriptor::CheckCondition(const MIIterator &mit) {
  MEASURE_FET("Descriptor::CheckCondition(...)");
  if (op == common::Operator::O_TRUE)
    return true;
  else if (op == common::Operator::O_FALSE)
    return false;

  bool result = true;

  // Assumption: LockSourcePacks externally done.
  if (op == common::Operator::O_EQ) {  // fast track for the most common operator
    DEBUG_ASSERT(attr.vc && val1.vc);
    // nulls checked in operator ==
    if (!(attr.vc->GetValue(mit) == val1.vc->GetValue(mit)))
      return false;
  } else if (op == common::Operator::O_NOT_NULL) {
    if (attr.vc->IsNull(mit))
      return false;
  } else if (op == common::Operator::O_IS_NULL) {
    if (!attr.vc->IsNull(mit))
      return false;
  } else if (op == common::Operator::O_EXISTS || op == common::Operator::O_NOT_EXISTS) {
    DEBUG_ASSERT(dynamic_cast<vcolumn::SubSelectColumn *>(attr.vc));
    vcolumn::SubSelectColumn *sub = static_cast<vcolumn::SubSelectColumn *>(attr.vc);
    bool is_nonempty = sub->CheckExists(mit);
    if ((op == common::Operator::O_EXISTS && !is_nonempty) || (op == common::Operator::O_NOT_EXISTS && is_nonempty))
      return false;
  } else if (op == common::Operator::O_BETWEEN || op == common::Operator::O_NOT_BETWEEN) {
    if (attr.vc->IsNull(mit))
      return false;
    // need to consider three value logic
    common::Tribool val1_res, val2_res;
    if (encoded) {
      if (attr.vc->Type().IsString() && !attr.vc->Type().Lookup()) {
        types::BString val, v1, v2;
        attr.vc->GetNotNullValueString(val, mit);
        val1.vc->GetValueString(v1, mit);
        val2.vc->GetValueString(v2, mit);
        val1_res = v1.IsNull() ? true : (sharp ? common::Tribool(val > v1) : common::Tribool(val >= v1));
        val2_res = v2.IsNull() ? true : (sharp ? common::Tribool(val < v2) : common::Tribool(val <= v2));
      } else if (!attr.vc->Type().IsFloat()) {
        int64_t val = attr.vc->GetNotNullValueInt64(mit);
        val1_res =
            val1.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN : common::Tribool(val >= val1.vc->GetNotNullValueInt64(mit));
        val2_res =
            val2.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN : common::Tribool(val <= val2.vc->GetNotNullValueInt64(mit));
      } else {  // Rare case: for encoded conditions treat nullptr as +/- inf.
        types::TianmuValueObject tianmu_value_obj1 = attr.vc->GetValue(mit, false);
        val1_res = val1.vc->IsNull(mit) ? true
                                        : (sharp ? common::Tribool(tianmu_value_obj1 > val1.vc->GetValue(mit, false))
                                                 : common::Tribool(tianmu_value_obj1 >= val1.vc->GetValue(mit, false)));
        val2_res = val2.vc->IsNull(mit) ? true
                                        : (sharp ? common::Tribool(tianmu_value_obj1 < val2.vc->GetValue(mit, false))
                                                 : common::Tribool(tianmu_value_obj1 <= val2.vc->GetValue(mit, false)));
      }
    } else {
      types::TianmuValueObject tianmu_value_obj1 = attr.vc->GetValue(mit, false);
      val1_res = val1.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN
                                      : common::Tribool(tianmu_value_obj1 >= val1.vc->GetValue(mit, false));
      val2_res = val2.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN
                                      : common::Tribool(tianmu_value_obj1 <= val2.vc->GetValue(mit, false));
    }
    if (op == common::Operator::O_BETWEEN) {
      if (val1_res != true || val2_res != true)
        return false;
    } else {
      if (val1_res != false && val2_res != false)
        return false;
    }
  } else if (IsSetOperator(op)) {
    DEBUG_ASSERT(attr.vc && dynamic_cast<vcolumn::MultiValColumn *>(val1.vc));
    return CheckSetCondition(mit, op);
  } else if (IsType_OrTree()) {
    DEBUG_ASSERT(tree);
    return tree->root->CheckCondition((MIIterator &)mit);
  } else {  // all other logical operators: >, >=, <, <=
    DEBUG_ASSERT(attr.vc && val1.vc);
    if (attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
      return false;
    if (!types::TianmuValueObject::compare(attr.vc->GetValue(mit), val1.vc->GetValue(mit), op, like_esc))
      return false;
  }
  return result;
}

bool Descriptor::IsNull(const MIIterator &mit) {
  MEASURE_FET("Descriptor::IsNull(...)");
  if (null_after_simplify)
    return true;
  if (op == common::Operator::O_TRUE || op == common::Operator::O_FALSE)
    return false;

  // Assumption: LockSourcePacks externally done.
  if (op == common::Operator::O_EQ) {
    DEBUG_ASSERT(attr.vc && val1.vc);
    if (attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
      return true;
  } else if (op == common::Operator::O_NOT_NULL || op == common::Operator::O_IS_NULL) {
    return false;
  } else if (op == common::Operator::O_EXISTS || op == common::Operator::O_NOT_EXISTS) {
    return false;
  } else if (op == common::Operator::O_BETWEEN || op == common::Operator::O_NOT_BETWEEN) {
    if (attr.vc->IsNull(mit))
      return true;
    // need to consider three value logic
    common::Tribool val1_res, val2_res;
    if (encoded) {
      if (attr.vc->Type().IsString() && !attr.vc->Type().Lookup()) {
        types::BString val, v1, v2;
        attr.vc->GetNotNullValueString(val, mit);
        val1.vc->GetValueString(v1, mit);
        val2.vc->GetValueString(v2, mit);
        val1_res = v1.IsNull() ? true : (sharp ? common::Tribool(val > v1) : common::Tribool(val >= v1));
        val2_res = v2.IsNull() ? true : (sharp ? common::Tribool(val < v2) : common::Tribool(val <= v2));
      } else if (!attr.vc->Type().IsFloat()) {
        int64_t val = attr.vc->GetNotNullValueInt64(mit);
        val1_res =
            val1.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN : common::Tribool(val >= val1.vc->GetNotNullValueInt64(mit));
        val2_res =
            val2.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN : common::Tribool(val <= val2.vc->GetNotNullValueInt64(mit));
      } else {  // Rare case: for encoded conditions treat nullptr as +/- inf.
        types::TianmuValueObject tianmu_value_obj1 = attr.vc->GetValue(mit, false);
        val1_res = val1.vc->IsNull(mit) ? true
                                        : (sharp ? common::Tribool(tianmu_value_obj1 > val1.vc->GetValue(mit, false))
                                                 : common::Tribool(tianmu_value_obj1 >= val1.vc->GetValue(mit, false)));
        val2_res = val2.vc->IsNull(mit) ? true
                                        : (sharp ? common::Tribool(tianmu_value_obj1 < val2.vc->GetValue(mit, false))
                                                 : common::Tribool(tianmu_value_obj1 <= val2.vc->GetValue(mit, false)));
      }
    } else {
      types::TianmuValueObject tianmu_value_obj1 = attr.vc->GetValue(mit, false);
      val1_res = val1.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN
                                      : common::Tribool(tianmu_value_obj1 >= val1.vc->GetValue(mit, false));
      val2_res = val2.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN
                                      : common::Tribool(tianmu_value_obj1 <= val2.vc->GetValue(mit, false));
    }
    if (common::Tribool::And(val1_res, val2_res) == common::TRIBOOL_UNKNOWN)
      return true;
  } else if (IsSetOperator(op)) {
    DEBUG_ASSERT(attr.vc && dynamic_cast<vcolumn::MultiValColumn *>(val1.vc));
    return IsNull_Set(mit, op);
  } else if (IsType_OrTree()) {
    DEBUG_ASSERT(tree);
    return tree->root->CheckCondition((MIIterator &)mit);
  } else {  // all other logical operators: >, >=, <, <=
    DEBUG_ASSERT(attr.vc && val1.vc);
    if (attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
      return true;
  }
  return false;
}

common::Tribool Descriptor::RoughCheckSubselectCondition(MIIterator &mit, SubSelectOptimizationType sot) {
  if (sot == SubSelectOptimizationType::PACK_BASED)
    return common::TRIBOOL_UNKNOWN;  // not implemented
  MEASURE_FET("Descriptor::RoughCheckSubselectCondition(...)");
  if (op == common::Operator::O_TRUE)
    return true;
  else if (op == common::Operator::O_FALSE)
    return false;

  common::ColumnType attr_t = attr.vc ? attr.vc->TypeName() : common::ColumnType();
  common::ColumnType val1_t = val1.vc ? val1.vc->TypeName() : common::ColumnType();
  common::ColumnType val2_t = val2.vc ? val2.vc->TypeName() : common::ColumnType();
  if ((attr.vc && ATI::IsStringType(attr_t)) || (val1.vc && ATI::IsStringType(val1_t)) ||
      (val2.vc && ATI::IsStringType(val2_t)))
    return common::TRIBOOL_UNKNOWN;

  if (op == common::Operator::O_EXISTS || op == common::Operator::O_NOT_EXISTS) {
    DEBUG_ASSERT(dynamic_cast<vcolumn::SubSelectColumn *>(attr.vc));
    vcolumn::SubSelectColumn *sub = static_cast<vcolumn::SubSelectColumn *>(attr.vc);
    common::Tribool is_empty = sub->RoughIsEmpty(mit, sot);
    if ((op == common::Operator::O_EXISTS && is_empty == true) ||
        (op == common::Operator::O_NOT_EXISTS && is_empty == false))
      return false;
    else if ((op == common::Operator::O_EXISTS && is_empty == false) ||
             (op == common::Operator::O_NOT_EXISTS && is_empty == true))
      return true;
    return common::TRIBOOL_UNKNOWN;
  } else if (IsSetOperator(op)) {
    DEBUG_ASSERT(attr.vc && dynamic_cast<vcolumn::MultiValColumn *>(val1.vc));
    return RoughCheckSetSubSelectCondition(mit, op, sot);
  }

  if (op == common::Operator::O_BETWEEN || op == common::Operator::O_NOT_BETWEEN) {
    // TODO: to be implemented
    // if(attr.vc->IsNull(mit))
    //	return false;
    // types::TianmuValueObject tianmu_value_obj1 = attr.vc->GetValue(mit, false);
    // need to consider three value logic
    // common::Tribool val1_res = val1.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN
    // : common::Tribool(tianmu_value_obj1 >= val1.vc->GetValue(mit, false));
    // common::Tribool val2_res = val2.vc->IsNull(mit) ? common::TRIBOOL_UNKNOWN
    // : common::Tribool(tianmu_value_obj1 <= val2.vc->GetValue(mit, false)); if(op ==
    // common::Operator::O_BETWEEN) {
    //	if(val1_res != true || val2_res != true)
    //		return false;
    //} else {
    //	if(val1_res != false && val2_res != false)
    //		return false;
    //}
    return common::TRIBOOL_UNKNOWN;
  }

  if (val1.vc->IsSubSelect() && attr.vc->IsSubSelect())
    return common::TRIBOOL_UNKNOWN;  // trivial, can be implemented better

  DEBUG_ASSERT(dynamic_cast<vcolumn::SubSelectColumn *>(attr.vc) || dynamic_cast<vcolumn::SubSelectColumn *>(val1.vc));
  vcolumn::SubSelectColumn *sub;
  vcolumn::VirtualColumn *val;
  if (attr.vc->IsSubSelect()) {
    sub = static_cast<vcolumn::SubSelectColumn *>(attr.vc);
    val = val1.vc;
  } else {
    sub = static_cast<vcolumn::SubSelectColumn *>(val1.vc);
    val = attr.vc;
  }
  if (sub->IsMaterialized())
    return common::TRIBOOL_UNKNOWN;

  RoughValue rv = sub->RoughGetValue(mit, sot);
  std::shared_ptr<types::TianmuDataType> rv_min, rv_max;
  if (sub->Type().IsDateTime()) {
    rv_min.reset(new types::TianmuDateTime(rv.GetMin(), sub->TypeName()));
    rv_max.reset(new types::TianmuDateTime(rv.GetMax(), sub->TypeName()));
  } else {
    rv_min.reset(new types::TianmuNum(rv.GetMin(), sub->Type().GetScale(), sub->Type().IsFloat(), sub->TypeName()));
    rv_max.reset(new types::TianmuNum(rv.GetMax(), sub->Type().GetScale(), sub->Type().IsFloat(), sub->TypeName()));
  }
  types::TianmuValueObject v = val->GetValue(mit);
  DEBUG_ASSERT(attr.vc);
  // NULLs are checked within operators
  if (op == common::Operator::O_EQ) {
    if (v < *rv_min || v > *rv_max)
      return false;
    // else if(v == rv_min && v == rv_max)
    //	return true;
  } else if (op == common::Operator::O_NOT_EQ) {
    if (v == *rv_min && v == *rv_max)
      return false;
    // else if(v < rv_min || v > rv_max)
    //	return true;
  } else if (op == common::Operator::O_MORE_EQ) {
    if (v < *rv_min)
      return false;
    // else if(v >= rv_max)
    //	return true;
  } else if (op == common::Operator::O_MORE) {
    if (v <= *rv_min)
      return false;
    // else if(v > rv_max)
    //	return true;
  } else if (op == common::Operator::O_LESS_EQ) {
    if (v > *rv_max)
      return false;
    // else if(v <= rv_min)
    //	return true;
  } else if (op == common::Operator::O_LESS) {
    if (v >= *rv_max)
      return false;
    // else if(v < rv_min)
    //	return true;
  }
  return common::TRIBOOL_UNKNOWN;
}

bool Descriptor::CheckSetCondition_UTF(const MIIterator &mit, common::Operator op) {
  DEBUG_ASSERT(IsSetOperator(op));
  bool result = true;
  vcolumn::MultiValColumn *mvc = static_cast<vcolumn::MultiValColumn *>(val1.vc);
  types::BString s1;
  attr.vc->GetValueString(s1, mit);
  types::TianmuValueObject aggr;
  switch (op) {
    case common::Operator::O_EQ_ALL:
      for (vcolumn::MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit); it != end; ++it) {
        types::BString s2 = it->GetString();
        // ConvertToBinaryForm(it->GetString(), buf_val1, buf_val1_len,
        // collation.collation, false);
        if (s1.IsNull() || it->IsNull() || CollationStrCmp(collation, s1, s2) != 0)
          return false;
      }
      break;
    case common::Operator::O_IN:
    case common::Operator::O_EQ_ANY:
      if (s1.IsNull())
        result = false;
      else
        result = (mvc->Contains(mit, s1) == true);
      break;
    case common::Operator::O_NOT_IN:
    case common::Operator::O_NOT_EQ_ALL: {
      if ((s1.IsNull() && mvc->NumOfValues(mit) != 0)) {
        result = false;
        break;
      }
      common::Tribool res = mvc->Contains(mit, s1);
      res = !res;
      result = (res == true);
      break;
    }
    case common::Operator::O_NOT_EQ_ANY:
      result = false;
      if (!s1.IsNull()) {
        for (vcolumn::MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit); it != end; ++it) {
          // ConvertToBinaryForm(it->GetString(), buf_val1, buf_val1_len,
          // collation.collation, false);
          if (!it->IsNull() && CollationStrCmp(collation, s1, it->GetString()) != 0)
            return true;
        }
      }
      break;
    case common::Operator::O_LESS_ALL:
    case common::Operator::O_LESS_EQ_ALL:
      Query::UnmarkAllAny(op);
      aggr = mvc->GetSetMin(mit);
      if (mvc->NumOfValues(mit) == 0)
        result = true;  // op ALL (empty_set) is TRUE
      else if (s1.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit) ||
               !CollationStrCmp(collation, s1, aggr.ToBString(), op))
        result = false;
      break;
    case common::Operator::O_MORE_ANY:
    case common::Operator::O_MORE_EQ_ANY:
      Query::UnmarkAllAny(op);
      aggr = mvc->GetSetMin(mit);
      if (mvc->NumOfValues(mit) == 0)
        result = false;  // op ANY (empty_set) is FALSE
      else if (s1.IsNull() || aggr.IsNull() || !CollationStrCmp(collation, s1, aggr.ToBString(), op))
        result = false;
      break;
    case common::Operator::O_LESS_ANY:
    case common::Operator::O_LESS_EQ_ANY:
      Query::UnmarkAllAny(op);
      aggr = mvc->GetSetMax(mit);
      if (mvc->NumOfValues(mit) == 0)
        result = false;  // op ANY (empty_set) is FALSE
      else if (s1.IsNull() || aggr.IsNull() || !CollationStrCmp(collation, s1, aggr.ToBString(), op))
        result = false;
      break;
    case common::Operator::O_MORE_ALL:
    case common::Operator::O_MORE_EQ_ALL:
      Query::UnmarkAllAny(op);
      aggr = mvc->GetSetMax(mit);
      if (mvc->NumOfValues(mit) == 0)
        result = true;  // op ALL (empty_set) is TRUE
      else if (s1.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit) ||
               !CollationStrCmp(collation, s1, aggr.ToBString(), op))
        result = false;
      break;
    default:
      break;
  }
  return result;
}

bool Descriptor::CheckSetCondition(const MIIterator &mit, common::Operator op) {
  MEASURE_FET("Descriptor::CheckSetCondition(...)");
  DEBUG_ASSERT(IsSetOperator(op));
  bool result = true;
  vcolumn::MultiValColumn *mvc = static_cast<vcolumn::MultiValColumn *>(val1.vc);
  if (encoded) {
    DEBUG_ASSERT(op == common::Operator::O_IN || op == common::Operator::O_NOT_IN);
    if (attr.vc->IsNull(mit)) {
      if (op == common::Operator::O_IN)
        return false;
      if (mvc->NumOfValues(mit) != 0)
        return false;
      return true;
    }
    common::Tribool res;
    if (attr.vc->Type().IsString() && !attr.vc->Type().Lookup()) {
      types::BString val;
      attr.vc->GetNotNullValueString(val, mit);
      res = mvc->ContainsString(mit, val);
    } else {
      int64_t val = attr.vc->GetNotNullValueInt64(mit);
      res = mvc->Contains64(mit, val);
    }
    if (op == common::Operator::O_NOT_IN)
      res = !res;
    return (res == true);
  }
  types::TianmuValueObject val = attr.vc->GetValue(mit);
  types::TianmuValueObject aggr;
  switch (op) {
    case common::Operator::O_EQ_ALL:
      for (vcolumn::MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit); it != end; ++it) {
        if (val.IsNull() || it->IsNull() ||
            !types::TianmuValueObject::compare(val, it->GetValue(), common::Operator::O_EQ, '\\'))
          return false;
      }
      break;
    case common::Operator::O_IN:
    case common::Operator::O_EQ_ANY:
      if (val.IsNull())
        result = false;
      else
        result = (mvc->Contains(mit, *val) == true);
      break;
    case common::Operator::O_NOT_IN:
    case common::Operator::O_NOT_EQ_ALL: {
      if ((val.IsNull() && mvc->NumOfValues(mit) != 0)) {
        result = false;
        break;
      }
      common::Tribool res = mvc->Contains(mit, *val);
      res = !res;
      result = (res == true);
      break;
    }
    case common::Operator::O_NOT_EQ_ANY:
      result = false;
      if (!val.IsNull()) {
        for (vcolumn::MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit); it != end; ++it) {
          if (!it->IsNull() && types::TianmuValueObject::compare(val, it->GetValue(), common::Operator::O_NOT_EQ, '\\'))
            return true;
        }
      }
      break;
    case common::Operator::O_LESS_ALL:
    case common::Operator::O_LESS_EQ_ALL:
      Query::UnmarkAllAny(op);
      aggr = mvc->GetSetMin(mit);
      if (mvc->NumOfValues(mit) == 0)
        result = true;  // op ALL (empty_set) is TRUE
      else if (val.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit) ||
               !types::TianmuValueObject::compare(val, aggr, op, '\\'))
        result = false;
      break;
    case common::Operator::O_MORE_ANY:
    case common::Operator::O_MORE_EQ_ANY:
      Query::UnmarkAllAny(op);
      aggr = mvc->GetSetMin(mit);
      if (mvc->NumOfValues(mit) == 0)
        result = false;  // op ANY (empty_set) is FALSE
      else if (val.IsNull() || aggr.IsNull() || !types::TianmuValueObject::compare(val, aggr, op, '\\'))
        result = false;
      break;
    case common::Operator::O_LESS_ANY:
    case common::Operator::O_LESS_EQ_ANY:
      Query::UnmarkAllAny(op);
      aggr = mvc->GetSetMax(mit);
      if (mvc->NumOfValues(mit) == 0)
        result = false;  // op ANY (empty_set) is FALSE
      else if (val.IsNull() || aggr.IsNull() || !types::TianmuValueObject::compare(val, aggr, op, '\\'))
        result = false;
      break;
    case common::Operator::O_MORE_ALL:
    case common::Operator::O_MORE_EQ_ALL:
      Query::UnmarkAllAny(op);
      aggr = mvc->GetSetMax(mit);
      if (mvc->NumOfValues(mit) == 0)
        result = true;  // op ALL (empty_set) is TRUE
      else if (val.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit) ||
               !types::TianmuValueObject::compare(val, aggr, op, '\\'))
        result = false;
      break;
    default:
      DEBUG_ASSERT(0 && "unexpected operator");
      break;
  }
  return result;
}

bool Descriptor::IsNull_Set(const MIIterator &mit, common::Operator op) {
  MEASURE_FET("Descriptor::CheckSetCondition(...)");
  DEBUG_ASSERT(IsSetOperator(op));
  vcolumn::MultiValColumn *mvc = static_cast<vcolumn::MultiValColumn *>(val1.vc);
  if (encoded) {
    DEBUG_ASSERT(op == common::Operator::O_IN || op == common::Operator::O_NOT_IN);
    if (attr.vc->IsNull(mit))
      return true;
    common::Tribool res;
    if (attr.vc->Type().IsString() && !attr.vc->Type().Lookup()) {
      types::BString val;
      attr.vc->GetNotNullValueString(val, mit);
      res = mvc->ContainsString(mit, val);
    } else {
      int64_t val = attr.vc->GetNotNullValueInt64(mit);
      res = mvc->Contains64(mit, val);
    }
    return (res == common::TRIBOOL_UNKNOWN);
  }
  types::TianmuValueObject val = attr.vc->GetValue(mit);
  types::TianmuValueObject aggr;
  switch (op) {
    case common::Operator::O_EQ_ALL:
    case common::Operator::O_NOT_EQ_ALL:
      if (val.IsNull() || mvc->ContainsNull(mit))
        return true;
      break;
    case common::Operator::O_IN:
    case common::Operator::O_EQ_ANY:
    case common::Operator::O_NOT_IN:
    case common::Operator::O_NOT_EQ_ANY:
      if (val.IsNull())
        return true;
      return (mvc->Contains(mit, *val) == common::TRIBOOL_UNKNOWN);
    case common::Operator::O_LESS_ALL:
    case common::Operator::O_LESS_EQ_ALL:
      aggr = mvc->GetSetMin(mit);
      if (val.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit))
        return true;
      break;
    case common::Operator::O_MORE_ANY:
    case common::Operator::O_MORE_EQ_ANY:
      aggr = mvc->GetSetMin(mit);
      if (val.IsNull() || aggr.IsNull())
        return true;
      break;
    case common::Operator::O_LESS_ANY:
    case common::Operator::O_LESS_EQ_ANY:
      aggr = mvc->GetSetMax(mit);
      if (val.IsNull() || aggr.IsNull())
        return true;
      break;
    case common::Operator::O_MORE_ALL:
    case common::Operator::O_MORE_EQ_ALL:
      aggr = mvc->GetSetMax(mit);
      if (val.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit))
        return true;
      break;
    default:
      DEBUG_ASSERT(0 && "unexpected operator");
      break;
  }
  return false;
}

common::Tribool Descriptor::RoughCheckSetSubSelectCondition(const MIIterator &mit, common::Operator op,
                                                            SubSelectOptimizationType sot) {
  MEASURE_FET("Descriptor::RoughCheckSetSubselectCondition(...)");
  DEBUG_ASSERT(IsSetOperator(op));
  DEBUG_ASSERT(val1.vc->IsSubSelect());
  vcolumn::SubSelectColumn *sub = static_cast<vcolumn::SubSelectColumn *>(val1.vc);
  if (sub->IsMaterialized())
    return common::TRIBOOL_UNKNOWN;

  RoughValue rv = sub->RoughGetValue(mit, sot);
  std::shared_ptr<types::TianmuDataType> rv_min, rv_max;
  if (sub->Type().IsDateTime()) {
    rv_min.reset(new types::TianmuDateTime(rv.GetMin(), sub->TypeName()));
    rv_max.reset(new types::TianmuDateTime(rv.GetMax(), sub->TypeName()));
  } else {
    rv_min.reset(new types::TianmuNum(rv.GetMin(), sub->Type().GetScale(), sub->Type().IsFloat(), sub->TypeName()));
    rv_max.reset(new types::TianmuNum(rv.GetMax(), sub->Type().GetScale(), sub->Type().IsFloat(), sub->TypeName()));
  }
  types::TianmuValueObject v = attr.vc->GetValue(mit);
  common::Tribool rough_is_empty = sub->RoughIsEmpty(mit, sot);

  switch (op) {
    case common::Operator::O_EQ_ALL:
      if (rough_is_empty == true)
        return true;  // op ALL (empty_set) is TRUE
      else if (rough_is_empty == false &&
               /*v.IsNull() ||*/ (v < *rv_min || v > *rv_max))
        return false;
      // else if(v == rv_min && v == rv_max)
      //	return true;
      break;
    case common::Operator::O_IN:
    case common::Operator::O_EQ_ANY:
      if (rough_is_empty == true)
        return false;  // // op ANY (empty_set) is FALSE
      else if (rough_is_empty == false &&
               /*v.IsNull() ||*/ (v < *rv_min || v > *rv_max))
        return false;
      // else if(v == rv_min && v == rv_max)
      //	return true;
      break;
    // case common::Operator::O_NOT_IN:
    // case common::Operator::O_NOT_EQ_ALL: {
    //	if((val.IsNull() && mvc->NumOfValues(mit) != 0)) {
    //		result = false;
    //		break;
    //	}
    //	common::Tribool res = mvc->Contains(mit, *val);
    //	res = !res;
    //	result = (res == true);
    //	break;
    //				   }
    // case common::Operator::O_NOT_EQ_ANY:
    //	result = false;
    //	if(!val.IsNull()) {
    //		for(vcolumn::MultiValColumn::Iterator it = mvc->begin(mit), end
    //= mvc->end(mit); it !=
    // end; ++it) {
    //			if(!it->IsNull() &&
    // types::TianmuValueObject::compare(val, it->GetValue(),
    // common::Operator::O_NOT_EQ))
    //				return true;
    //		}
    //	}
    //	break;
    case common::Operator::O_LESS_ALL:
    case common::Operator::O_LESS_EQ_ALL:
      Query::UnmarkAllAny(op);
      if (rough_is_empty == true)
        return true;  // op ALL (empty_set) is TRUE
      else if (rough_is_empty == false && !types::TianmuValueObject::compare(v, *rv_max, op, '\\'))
        return false;
      break;
    case common::Operator::O_MORE_ANY:
    case common::Operator::O_MORE_EQ_ANY:
      Query::UnmarkAllAny(op);
      if (rough_is_empty == true)
        return false;  // op ANY (empty_set) is FALSE
      else if (rough_is_empty == false && !types::TianmuValueObject::compare(v, *rv_min, op, '\\'))
        return false;
      break;
    case common::Operator::O_LESS_ANY:
    case common::Operator::O_LESS_EQ_ANY:
      Query::UnmarkAllAny(op);
      if (rough_is_empty == true)
        return false;  // op ANY (empty_set) is FALSE
      else if (rough_is_empty == false && !types::TianmuValueObject::compare(v, *rv_max, op, '\\'))
        return false;
      break;
    case common::Operator::O_MORE_ALL:
    case common::Operator::O_MORE_EQ_ALL:
      Query::UnmarkAllAny(op);
      if (rough_is_empty == true)
        return true;  // op ALL (empty_set) is TRUE
      else if (rough_is_empty == false && !types::TianmuValueObject::compare(v, *rv_min, op, '\\'))
        return false;
      break;
    default:
      DEBUG_ASSERT("unexpected operator");
      break;
  }
  return common::TRIBOOL_UNKNOWN;
}

void Descriptor::CoerceColumnType(vcolumn::VirtualColumn *&for_typecast) {
  vcolumn::VirtualColumn *vc = attr.vc;
  vcolumn::TypeCastColumn *tcc = nullptr;
  bool tstamp = false;
  if (ATI::IsNumericType(vc->TypeName())) {
    if (ATI::IsTxtType(for_typecast->TypeName()))
      tcc = new vcolumn::String2NumCastColumn(for_typecast, vc->Type());
    else if (ATI::IsDateTimeType(for_typecast->TypeName()))
      tcc = new vcolumn::DateTime2NumCastColumn(for_typecast, vc->Type());
  } else if (ATI::IsDateTimeType(vc->TypeName())) {
    if (ATI::IsTxtType(for_typecast->TypeName()))
      tcc = new vcolumn::String2DateTimeCastColumn(for_typecast, vc->Type());
    else if (ATI::IsNumericType(for_typecast->TypeName()))
      tcc = new vcolumn::Num2DateTimeCastColumn(for_typecast, vc->Type());
    else if (vc->TypeName() == common::ColumnType::TIMESTAMP &&
             !(ATI::IsDateTimeType(for_typecast->TypeName()))) {  // for_typecast->TypeName() !=
                                                                  // common::CT::TIMESTAMP	{
      tcc = new vcolumn::TimeZoneConversionCastColumn(vc);
      tstamp = true;
    }
  } else if (ATI::IsTxtType(vc->TypeName())) {
    if (ATI::IsDateTimeType(for_typecast->TypeName()))
      tcc = new vcolumn::DateTime2VarcharCastColumn(for_typecast, vc->Type());
    else if (ATI::IsNumericType(for_typecast->TypeName()))
      tcc = new vcolumn::Num2VarcharCastColumn(for_typecast, vc->Type());
  } else
    return;

  if (tcc) {
    if (tianmu_control_.isOn()) {
      tianmu_control_.lock(current_txn_->GetThreadID())
          << "Type conversion for VC:"
          << (for_typecast == val1.vc ? val1.vc_id : for_typecast == val2.vc ? val2.vc_id : val1.vc_id)
          << system::unlock;
    }

    table->AddVirtColumn(tcc);
    if (!tstamp)
      for_typecast = tcc;
    else
      attr.vc = tcc;
  }
}

void Descriptor::CoerceColumnTypes() {
  if (val1.vc) {
    if (val1.vc->IsMultival()) {
      vcolumn::MultiValColumn *mvc = static_cast<vcolumn::MultiValColumn *>(val1.vc);
      if (val1.vc->IsInSet()) {
        vcolumn::InSetColumn *isc = new vcolumn::InSetColumn(*static_cast<vcolumn::InSetColumn *>(val1.vc));
        for (uint i = 0; i < isc->columns.size(); i++) CoerceColumnType(isc->columns[i]);
        table->AddVirtColumn(isc);
        val1.vc = mvc = isc;
      }
      mvc->SetExpectedType(attr.vc->Type());
    } else
      CoerceColumnType(val1.vc);
  }
  if (val2.vc)
    CoerceColumnType(val2.vc);
  CoerceCollation();
}

bool Descriptor::NullMayBeTrue()  // true, if the descriptor may give nontrivial
                                  // answer if any of involved dimension is null
{
  if (op == common::Operator::O_IS_NULL || op == common::Operator::O_NOT_NULL)
    return true;
  if (IsType_OrTree())
    return tree->NullMayBeTrue();

  // t1.a not between t2.b and t1.b
  if (op == common::Operator::O_NOT_BETWEEN)
    return true;
  // TODO: more precise conditions
  // Examples:
  //    (a is null) = 1
  //    b = ifnull(a, 0)
  //    c > (case when a is null then 10 else 20)
  //    d = (select count(*) from t where t.x = a)
  //    e IN (1, 2, a)
  //    f < ALL (1, 2, a)

  // For now, a simplistic version: any complex case is true.
  if (attr.vc && !static_cast<int>(attr.vc->IsSingleColumn()) && !attr.vc->IsConst())
    return true;
  if (val1.vc && !static_cast<int>(val1.vc->IsSingleColumn()) && !val1.vc->IsConst())
    return true;
  if (val2.vc && !static_cast<int>(val2.vc->IsSingleColumn()) && !val2.vc->IsConst())
    return true;
  return false;
}

bool Descriptor::IsParameterized() const {
  return ((attr.vc && attr.vc->IsParameterized()) || (val1.vc && val1.vc->IsParameterized()) ||
          (val2.vc && val2.vc->IsParameterized()) || (tree && tree->IsParameterized()));
}

bool Descriptor::IsDeterministic() const {
  bool det = true;
  if (attr.vc)
    det = det && attr.vc->IsDeterministic();
  if (val1.vc)
    det = det && val1.vc->IsDeterministic();
  if (val2.vc)
    det = det && val2.vc->IsDeterministic();
  return det;
}

bool Descriptor::WithoutAttrs() {
  if (IsType_OrTree())
    return tree->WithoutAttrs();
  else
    return (!attr.vc || attr.vc->IsSingleColumn() != vcolumn::VirtualColumn::single_col_t::SC_ATTR) &&
           (!val1.vc || val1.vc->IsSingleColumn() != vcolumn::VirtualColumn::single_col_t::SC_ATTR) &&
           (!val2.vc || val2.vc->IsSingleColumn() != vcolumn::VirtualColumn::single_col_t::SC_ATTR);
}

bool Descriptor::WithoutTypeCast() {
  if (IsType_OrTree())
    return tree->WithoutTypeCast();
  else
    return (!attr.vc || !attr.vc->IsTypeCastColumn()) && (!val1.vc || !val1.vc->IsTypeCastColumn()) &&
           (!val2.vc || !val2.vc->IsTypeCastColumn());
}

bool Descriptor::IsTIANMUItemsEmpty() {
  if (IsType_OrTree())
    return tree->IsTIANMUItemsEmpty();
  else
    return (!attr.vc || attr.vc->GetTIANMUItems().empty()) && (!val1.vc || val1.vc->GetTIANMUItems().empty()) &&
           (!val2.vc || val2.vc->GetTIANMUItems().empty());
}

void Descriptor::CoerceCollation() {
  collation = attr.vc && attr.vc->Type().IsString() ? attr.vc->GetCollation() : DTCollation();
  if (val1.vc && val1.vc->Type().IsString())
    collation = types::ResolveCollation(collation, val1.vc->GetCollation());
  if (val2.vc && val2.vc->Type().IsString())
    collation = types::ResolveCollation(collation, val2.vc->GetCollation());
}

void Descriptor::ClearRoughValues() {
  if (IsType_OrTree())
    tree->root->ClearRoughValues();
  else
    rv = common::RoughSetValue::RS_UNKNOWN;
}

void Descriptor::RoughAccumulate(MIIterator &mit) {
  DEBUG_ASSERT(IsType_OrTree());
  tree->RoughAccumulate(mit);
}

void Descriptor::SimplifyAfterRoughAccumulate() {
  if (IsType_OrTree()) {
    if (tree->UseRoughAccumulated())
      Simplify(false);
  } else {
    if (rv == common::RoughSetValue::RS_NONE)
      op = common::Operator::O_FALSE;
    else if (rv == common::RoughSetValue::RS_ALL)
      op = common::Operator::O_TRUE;
  }
}

// TODO: now only for EvaluatePack_InString_UTF; others future
bool Descriptor::CopyDesCond(MIUpdatingIterator &mit) {
  common::PackType pack_type;
  if (IsType_AttrValOrAttrValVal()) {
    common::ColumnType column_type = attr.vc->Type().GetTypeName();
    bool is_lookup = attr.vc->Type().Lookup();
    if (((column_type == common::ColumnType::VARCHAR || column_type == common::ColumnType::STRING) && is_lookup) ||
        ATI::IsNumericType(column_type) || ATI::IsDateTimeType(column_type))
      pack_type = common::PackType::INT;
    else
      pack_type = common::PackType::STR;

    if ((op == common::Operator::O_IN || op == common::Operator::O_NOT_IN)) {
      vcolumn::MultiValColumn *multival_column = static_cast<vcolumn::MultiValColumn *>(val1.vc);
      if (pack_type == common::PackType::STR) {
        val1.cond_value.clear();
        return multival_column->CopyCond(mit, val1.cond_value, collation);
      } else if (pack_type == common::PackType::INT) {
        if (val1.cond_numvalue != nullptr)
          val1.cond_numvalue.reset();
        return multival_column->CopyCond(mit, val1.cond_numvalue, collation);
      }
    }
  }
  return false;
}
// prepare data before thread start
void Descriptor::PrepareValueSet(MIIterator &mit) {
  if (IsType_OrTree())
    tree->root->PrepareValueSet(mit);
  else {
    vcolumn::InSetColumn *set_column = dynamic_cast<vcolumn::InSetColumn *>(val1.vc);
    if (set_column != nullptr)
      set_column->PrepareValueSet(mit);
  }
}

bool Descriptor::CheckTmpInTerm(const CQTerm &t) const {
  if (!t.vc)
    return false;

  for (auto &var_map : t.vc->GetVarMap()) {
    if (var_map.GetTabPtr()->TableType() == TType::TEMP_TABLE) {
      return true;
    }
  }
  return false;
}

bool Descriptor::ExsitTmpTable() const {
  return (CheckTmpInTerm(attr)) || (CheckTmpInTerm(val1)) || (CheckTmpInTerm(val2));
}

bool Descriptor::IsleftIndexSearch() const {
  if (!tianmu_sysvar_index_search || IsType_OrTree() || IsType_Between())
    return false;
  if (IsType_AttrValOrAttrValVal() && encoded) {
    auto col = static_cast<vcolumn::SingleColumn *>(attr.vc);
    if (table && table->NumOfTables() == 1 && table->GetTableP(0)->TableType() == TType::TABLE &&
        col->GetPhysical()->ColType() == PhysicalColumn::phys_col_t::kTianmuAttr) {
      auto path = (static_cast<TianmuTable *>(table->GetTableP(0))->Path());
      auto indextab = ha_tianmu_engine_->GetTableIndex(path.replace_extension().string());
      uint32_t colid = static_cast<TianmuAttr *>(col->GetPhysical())->ColId();

      if (indextab) {
        std::vector<uint> keycols = indextab->KeyCols();
        if (keycols.size() > 0 && colid == keycols[0])
          return true;
      }
    }
  }
  return false;
}

common::ErrorCode Descriptor::EvaluateOnIndex(MIUpdatingIterator &mit, int64_t limit) {
  if (IsleftIndexSearch()) {
    return attr.vc->EvaluateOnIndex(mit, *this, limit);
  }
  return common::ErrorCode::FAILED;
}
void DescTreeNode::PrepareValueSet(MIIterator &mit) {
  if (left && right) {
    left->PrepareValueSet(mit);
    right->PrepareValueSet(mit);
  } else
    desc.PrepareValueSet(mit);
}

bool IsSetOperator(common::Operator op) {
  return IsSetAnyOperator(op) || IsSetAllOperator(op) || op == common::Operator::O_IN ||
         op == common::Operator::O_NOT_IN;
}

bool IsSetAllOperator(common::Operator op) {
  return op == common::Operator::O_EQ_ALL || op == common::Operator::O_NOT_EQ_ALL ||
         op == common::Operator::O_LESS_ALL || op == common::Operator::O_MORE_ALL ||
         op == common::Operator::O_LESS_EQ_ALL || op == common::Operator::O_MORE_EQ_ALL;
}

bool IsSimpleEqualityOperator(common::Operator op) {
  return op == common::Operator::O_EQ || op == common::Operator::O_NOT_EQ || op == common::Operator::O_LESS ||
         op == common::Operator::O_MORE || op == common::Operator::O_LESS_EQ || op == common::Operator::O_MORE_EQ;
}

bool IsSetAnyOperator(common::Operator op) {
  return op == common::Operator::O_EQ_ANY || op == common::Operator::O_NOT_EQ_ANY ||
         op == common::Operator::O_LESS_ANY || op == common::Operator::O_MORE_ANY ||
         op == common::Operator::O_LESS_EQ_ANY || op == common::Operator::O_MORE_EQ_ANY;
}

bool ISTypeOfEqualOperator(common::Operator op) {
  return op == common::Operator::O_EQ || op == common::Operator::O_EQ_ALL || op == common::Operator::O_EQ_ANY;
}

bool ISTypeOfNotEqualOperator(common::Operator op) {
  return op == common::Operator::O_NOT_EQ || op == common::Operator::O_NOT_EQ_ALL ||
         op == common::Operator::O_NOT_EQ_ANY;
}

bool ISTypeOfLessOperator(common::Operator op) {
  return op == common::Operator::O_LESS || op == common::Operator::O_LESS_ALL || op == common::Operator::O_LESS_ANY;
}

bool ISTypeOfLessEqualOperator(common::Operator op) {
  return op == common::Operator::O_LESS_EQ || op == common::Operator::O_LESS_EQ_ALL ||
         op == common::Operator::O_LESS_EQ_ANY;
}

bool ISTypeOfMoreOperator(common::Operator op) {
  return op == common::Operator::O_MORE || op == common::Operator::O_MORE_ALL || op == common::Operator::O_MORE_ANY;
}

bool ISTypeOfMoreEqualOperator(common::Operator op) {
  return op == common::Operator::O_MORE_EQ || op == common::Operator::O_MORE_EQ_ALL ||
         op == common::Operator::O_MORE_EQ_ANY;
}

DescTree::DescTree(CQTerm e1, common::Operator op, CQTerm e2, CQTerm e3, TempTable *t, int no_dims, char like_esc) {
  curr = root = new DescTreeNode(e1, op, e2, e3, t, no_dims, like_esc);
}

DescTree::DescTree(DescTree &t) { curr = root = Copy(t.root); }

DescTreeNode *DescTree::Copy(DescTreeNode *node) {
  if (!node)
    return nullptr;
  DescTreeNode *res = new DescTreeNode(*node);
  if (node->left) {
    res->left = Copy(node->left);
    res->left->parent = res;
  }
  if (node->right) {
    res->right = Copy(node->right);
    res->right->parent = res;
  }
  return res;
}

// void DescTree::Release(DescTreeNode * &node)
//{
//	if(node && node->left)
//		Release(node->left);
//	if(node && node->right)
//		Release(node->right);
//	delete node;
//	node = nullptr;
//}

// make _lop the root, make current tree the left child, make descriptor the
// right child
void DescTree::AddDescriptor(common::LogicalOperator lop, CQTerm e1, common::Operator op, CQTerm e2, CQTerm e3,
                             TempTable *t, int no_dims, char like_esc) {
  curr = new DescTreeNode(lop, t, no_dims);
  curr->left = root;
  curr->left->parent = curr;
  curr->right = new DescTreeNode(e1, op, e2, e3, t, no_dims, like_esc);
  curr->right->parent = curr;
  root = curr;
}

// make _lop the root, make current tree the left child, make tree the right
// child
void DescTree::AddTree(common::LogicalOperator lop, DescTree *tree, int no_dims) {
  if (tree->root) {
    curr = new DescTreeNode(lop, tree->root->desc.table, no_dims);
    curr->left = root;
    root->parent = curr;
    curr->right = tree->root;
    tree->root->parent = curr;
    root = curr;
    tree->root = nullptr;
    tree->curr = nullptr;
  }
}

void DescTree::Display() { Display(root); }

void DescTree::Display(DescTreeNode *node) {
  if (node == nullptr)
    return;
  if (node->left)
    Display(node->left);
  if (node->right)
    Display(node->right);
  std::cout << "------------------------" << std::endl;
  if (node->left)
    std::cout << (static_cast<int>(node->desc.lop) ? "OR" : "AND") << std::endl;
  else {
    char buf[50];
    std::cout << node->desc.attr.ToString(buf, 0) << std::endl;
    std::cout << "........" << std::endl;
    if (static_cast<int>(node->desc.op))
      std::cout << static_cast<int>(node->desc.op) << std::endl;
    else
      std::cout << "=" << std::endl;
    std::cout << "........" << std::endl;
    std::cout << node->desc.val1.ToString(buf, 0) << std::endl;
  }
  std::cout << "------------------------" << std::endl;
}

bool DescTree::IsParameterized() { return root->IsParameterized(); }

Descriptor DescTree::ExtractDescriptor() {
  std::vector<std::pair<int, Descriptor>> desc_counts;
  root->CollectDescriptor(desc_counts);
  sort(desc_counts.begin(), desc_counts.end());
  for (int i = int(desc_counts.size() - 1); i >= 0; i--) {
    Descriptor &desc = desc_counts[i].second;
    if (root->CanBeExtracted(desc)) {
      root->ExtractDescriptor(desc, root);
      return desc;
    }
  }
  return Descriptor();
}

void DescTree::MakeSingleColsPrivate(std::vector<vcolumn::VirtualColumn *> &virt_cols) {
  root->MakeSingleColsPrivate(virt_cols);
}

bool DescriptorEqual(std::pair<int, Descriptor> const &d1, std::pair<int, Descriptor> const &d2) {
  return (d1.second == d2.second);
}

DescTreeNode::~DescTreeNode() {
  if (left)
    delete left;
  if (right)
    delete right;
}

common::Tribool DescTreeNode::Simplify(DescTreeNode *&root, bool in_having) {
  common::Tribool left_res, right_res;
  if (left)
    left_res = left->Simplify(root, in_having);
  if (right)
    right_res = right->Simplify(root, in_having);
  if (desc.op == common::Operator::O_OR_TREE) {
    common::Tribool res = (desc.lop == common::LogicalOperator::O_AND ? common::Tribool::And(left_res, right_res)
                                                                      : common::Tribool::Or(left_res, right_res));
    if (res == true) {
      desc.op = common::Operator::O_TRUE;
      delete left;
      left = nullptr;
      delete right;
      right = nullptr;
    } else if (res == false) {
      desc.op = common::Operator::O_FALSE;
      delete left;
      left = nullptr;
      delete right;
      right = nullptr;
    } else if (!left->left && !right->right && desc.lop == common::LogicalOperator::O_AND) {
      bool merged = ParameterizedFilter::TryToMerge(left->desc, right->desc);
      if (merged) {
        delete right;
        right = nullptr;
        res = ReplaceNode(this, left, root);
      }
    } else if (desc.lop == common::LogicalOperator::O_OR) {
      if (left->desc.op == common::Operator::O_FALSE) {
        delete left;
        left = nullptr;
        res = ReplaceNode(this, right, root);
      } else if (left->desc.op == common::Operator::O_TRUE) {
        delete right;
        right = nullptr;
        res = ReplaceNode(this, left, root);
      } else if (right->desc.op == common::Operator::O_FALSE) {
        delete right;
        right = nullptr;
        res = ReplaceNode(this, left, root);
      } else if (right->desc.op == common::Operator::O_TRUE) {
        delete left;
        left = nullptr;
        res = ReplaceNode(this, right, root);
      }
    }

    return res;
  }
  desc.Simplify(in_having);
  return desc.op == common::Operator::O_TRUE ? true
                                             : (desc.op == common::Operator::O_FALSE ? false : common::TRIBOOL_UNKNOWN);
}

common::RoughSetValue DescTreeNode::EvaluateRoughlyPack(const MIIterator &mit) {
  if (desc.op == common::Operator::O_OR_TREE) {
    common::RoughSetValue left_res = left->EvaluateRoughlyPack(mit);
    common::RoughSetValue right_res = right->EvaluateRoughlyPack(mit);
    common::RoughSetValue r;
    if (desc.lop == common::LogicalOperator::O_AND)
      r = And(left_res, right_res);
    else
      r = Or(left_res, right_res);
    if (desc.rv == common::RoughSetValue::RS_UNKNOWN)
      desc.rv = r;
    else if (desc.rv == common::RoughSetValue::RS_NONE && r != common::RoughSetValue::RS_NONE)
      desc.rv = common::RoughSetValue::RS_SOME;
    else if (desc.rv == common::RoughSetValue::RS_ALL && r != common::RoughSetValue::RS_ALL)
      desc.rv = common::RoughSetValue::RS_SOME;
    return r;
  }
  return desc.EvaluateRoughlyPack(mit);
}

bool DescTreeNode::CheckCondition(MIIterator &mit) {
  if (left) {             // i.e., not a leaf
    DEBUG_ASSERT(right);  // if left is not empty so should be right
    if (desc.lop == common::LogicalOperator::O_AND) {
      if (!left->CheckCondition(mit))
        return false;
      if (!right->CheckCondition(mit))
        return false;
      return true;
    } else {
      if (left->CheckCondition(mit))
        return true;
      if (right->CheckCondition(mit))
        return true;
      return false;
    }
  } else {  // i.e., a leaf
    if (locked >= 0) {
      desc.LockSourcePacks(mit, locked);
      locked = -1;
    }
    if (types::RequiresUTFConversions(desc.GetCollation())) {
      return desc.CheckCondition_UTF(mit);
    } else
      return desc.CheckCondition(mit);
  }
}
/*
bool DescTreeNode::IsNull(MIIterator &mit)
{
    if (left) {         // i.e., not a leaf
        DEBUG_ASSERT(right);  // if left is not empty so should be right
        bool left_res = left->CheckCondition(mit);
        bool right_res = right->CheckCondition(mit);
        if (desc.lop == common::LogicalOperator::O_AND) {
            if (left_res == true && right_res == false && right->IsNull(mit))
                return true;
            if (left_res == false && right_res == true && left->IsNull(mit))
                return true;
            return false;
        } else {
            if (left_res == false && right_res == false && (right->IsNull(mit)
|| left->IsNull(mit))) return true; return false;
        }
    } else {  // i.e., a leaf
        if (locked >= 0) {
            desc.LockSourcePacks(mit, locked);
            locked = -1;
        }
        return desc.IsNull(mit);
    }
}
*/

void DescTreeNode::EvaluatePack(MIUpdatingIterator &mit) {
  int single_dim = mit.SingleFilterDim();
  // general case:
  if (single_dim == -1) {
    while (mit.IsValid()) {
      if (CheckCondition(mit) == false)
        mit.ResetCurrent();
      ++mit;
      if (mit.PackrowStarted())
        break;
    }
    return;
  }

  // optimized case
  if (left) {             // i.e., not a leaf
    DEBUG_ASSERT(right);  // if left is not empty so should be right
    if (desc.lop == common::LogicalOperator::O_AND) {
      if (left->desc.rv == common::RoughSetValue::RS_NONE || right->desc.rv == common::RoughSetValue::RS_NONE) {
        mit.ResetCurrentPack();
        mit.NextPackrow();
        return;
      }
      if (left->desc.rv == common::RoughSetValue::RS_ALL && right->desc.rv == common::RoughSetValue::RS_ALL) {
        mit.NextPackrow();
        return;
      }
      int pack_start = mit.GetCurPackrow(single_dim);
      if (left->desc.rv != common::RoughSetValue::RS_ALL && mit.IsValid())
        left->EvaluatePack(mit);
      if (right->desc.rv != common::RoughSetValue::RS_ALL && mit.RewindToPack(pack_start) &&
          mit.IsValid())  // otherwise the pack is already empty
        right->EvaluatePack(mit);
      return;
    } else {
      if (left->desc.rv == common::RoughSetValue::RS_NONE && right->desc.rv == common::RoughSetValue::RS_NONE) {
        mit.ResetCurrentPack();
        mit.NextPackrow();
        return;
      }
      if (left->desc.rv == common::RoughSetValue::RS_ALL || right->desc.rv == common::RoughSetValue::RS_ALL) {
        mit.NextPackrow();
        return;
      }
      if (left->desc.rv == common::RoughSetValue::RS_NONE)
        right->EvaluatePack(mit);
      else if (right->desc.rv == common::RoughSetValue::RS_NONE)
        left->EvaluatePack(mit);
      else {
        int pack_start = mit.GetCurPackrow(single_dim);
        std::unique_ptr<Filter> f(mit.NewPackFilter(pack_start));
        left->EvaluatePack(mit);
        if (mit.SwapPackFilter(pack_start,
                               f.get())) {  // return true if the pack in the
                                            // current iterator is not full
          mit.RewindToPack(pack_start);
          right->EvaluatePack(mit);
          mit.OrPackFilter(pack_start, f.get());
        }
      }
      return;
    }
  } else {  // i.e., a leaf
    if (locked >= 0) {
      desc.LockSourcePacks(mit, locked);
      locked = -1;
    }
    desc.EvaluatePackImpl(mit);
  }
}

void DescTreeNode::PrepareToLock(int locked_by) {
  if (left) {  // i.e., not a leaf
    left->PrepareToLock(locked_by);
    right->PrepareToLock(locked_by);
  } else  // i.e., a leaf
    locked = locked_by;
}

void DescTreeNode::UnlockSourcePacks() {
  if (left) {  // i.e., not a leaf
    left->UnlockSourcePacks();
    right->UnlockSourcePacks();
  } else {  // i.e., a leaf
    locked = 0;
    desc.UnlockSourcePacks();
  }
}

bool DescTreeNode::IsParameterized() {
  bool is_parameterized = desc.IsParameterized();
  if (left)
    is_parameterized = is_parameterized || left->IsParameterized();
  if (right)
    is_parameterized = is_parameterized || right->IsParameterized();
  return is_parameterized;
}

void DescTreeNode::DimensionUsed(DimensionVector &dims) {
  desc.DimensionUsed(dims);
  if (left)
    left->DimensionUsed(dims);
  if (right)
    right->DimensionUsed(dims);
}

bool DescTreeNode::NullMayBeTrue() {
  // TODO: revisit the logics below
  if (left && right) {
    if (left->NullMayBeTrue() || right->NullMayBeTrue())
      return true;
    // special case: (a1 = 3) OR true
    // return false only if both sides of OR contain all involved dims
    if (desc.lop == common::LogicalOperator::O_OR) {
      DimensionVector dims1(desc.right_dims.Size());
      DimensionVector dims2(desc.right_dims.Size());
      left->DimensionUsed(dims1);
      right->DimensionUsed(dims2);
      if (!(dims1 == dims2))
        return true;
    }
    return false;
  }
  return desc.NullMayBeTrue();
}

void DescTreeNode::EncodeIfPossible(bool for_rough_query, bool additional_nulls) {
  if (left && right) {
    left->EncodeIfPossible(for_rough_query, additional_nulls);
    right->EncodeIfPossible(for_rough_query, additional_nulls);
  } else
    ConditionEncoder::EncodeIfPossible(desc, for_rough_query, additional_nulls);
}

double DescTreeNode::EvaluateConditionWeight(ParameterizedFilter *p, bool for_or) {
  if (left && right) {
    bool or_here = (desc.lop == common::LogicalOperator::O_OR);  // and: smaller result first; or: bigger result first
    double e1 = left->EvaluateConditionWeight(p, or_here);
    double e2 = right->EvaluateConditionWeight(p, or_here);
    if (e1 > e2) {
      DescTreeNode *pp = left;
      left = right;
      right = pp;
    }
    return e1 + e2 + 1;  // +1 is a penalty of logical operation
  } else
    return p->EvaluateConditionNonJoinWeight(desc, for_or);
}

void DescTreeNode::CollectDescriptor(std::vector<std::pair<int, Descriptor>> &desc_counts) {
  if (left && right) {
    left->CollectDescriptor(desc_counts);
    right->CollectDescriptor(desc_counts);
  } else {
    DEBUG_ASSERT(!left && !right);
    IncreaseDescriptorCount(desc_counts);
  }
}

bool DescTreeNode::CanBeExtracted(Descriptor &searched_desc) {
  if (left && right) {
    if (desc.lop == common::LogicalOperator::O_AND) {
      if (left->CanBeExtracted(searched_desc))
        return true;
      return right->CanBeExtracted(searched_desc);
    } else {
      DEBUG_ASSERT(desc.lop == common::LogicalOperator::O_OR);
      return (left->CanBeExtracted(searched_desc) && right->CanBeExtracted(searched_desc));
    }
  } else {
    return ((parent && parent->desc.lop == common::LogicalOperator::O_AND && desc <= searched_desc) ||
            (parent && parent->desc.lop == common::LogicalOperator::O_OR && desc == searched_desc));
  }
}

// bool DescTreeNode::IsWidestRange(Descriptor& searched_desc)
//{
//	if(left && right) {
//		if(desc.lop == common::LogicalOperator::O_AND) {
//			if(left->IsWidestRange(searched_desc))
//				return true;
//			return right->IsWidestRange(searched_desc);
//		} else {
//			DEBUG_ASSERT(desc.lop == common::LogicalOperator::O_OR);
//			return (left->IsWidestRange(searched_desc) &&
// right->IsWidestRange(searched_desc));
//		}
//	} else {
//		return (desc < searched_desc);
//	}
//}

void DescTreeNode::ExtractDescriptor(Descriptor &searched_desc, DescTreeNode *&root) {
  if (left && left->left) {
    DEBUG_ASSERT(left->right);
    left->ExtractDescriptor(searched_desc, root);
  } else {
    DEBUG_ASSERT(left && right);
    if (/*(desc.lop == common::LogicalOperator::O_AND && left->desc <= searched_desc) ||
                    (desc.lop == common::LogicalOperator::O_OR && */
        left->desc == searched_desc /*)*/) {
      delete left;
      left = nullptr;
      DescTreeNode *old_right = right;
      bool parent_is_or = (desc.lop == common::LogicalOperator::O_OR);
      ReplaceNode(this, right, root);
      // if there is no parent and there is single right node
      // it means that the whole tree should be empty
      // and extracted descriptor should replace the tree
      // we make this tree trivial with one common::Operator::O_TRUE node
      if (parent_is_or) {
        old_right->desc = Descriptor();
        old_right->desc.op = common::Operator::O_TRUE;
      } else if (old_right->right) {
        old_right->ExtractDescriptor(searched_desc, root);
      }
      return;
    }
  }

  if (right && right->left) {
    DEBUG_ASSERT(right->right);
    right->ExtractDescriptor(searched_desc, root);
  } else {
    DEBUG_ASSERT(left && right);
    if (/*(desc.lop == common::LogicalOperator::O_AND && right->desc <= searched_desc) ||
                    (desc.lop == common::LogicalOperator::O_OR &&*/
        right->desc == searched_desc /*)*/) {
      delete right;
      right = nullptr;
      DescTreeNode *old_left = left;
      bool parent_is_or = (desc.lop == common::LogicalOperator::O_OR);
      ReplaceNode(this, left, root);
      // if there is no parent and there is single left node
      // it means that the whole tree should be empty
      // and extracted descriptor should replace the tree
      // we make this tree trivial with one common::Operator::O_TRUE node
      if (parent_is_or) {
        old_left->desc = Descriptor();
        old_left->desc.op = common::Operator::O_TRUE;
      } else if (old_left->left) {
        old_left->ExtractDescriptor(searched_desc, root);
      }
      return;
    }
  }
}

common::Tribool DescTreeNode::ReplaceNode(DescTreeNode *src, DescTreeNode *dst, DescTreeNode *&root) {
  dst->parent = src->parent;
  if (src->parent) {
    if (src->parent->left == src)
      src->parent->left = dst;
    else
      src->parent->right = dst;
  } else
    root = dst;
  if (src->left == dst || src->right == dst) {
    // src's children are reused - prevent deleting them
    src->left = nullptr;
    src->right = nullptr;
  }
  delete src;
  if (dst->desc.op == common::Operator::O_FALSE)
    return false;
  if (dst->desc.op == common::Operator::O_TRUE)
    return true;
  return common::TRIBOOL_UNKNOWN;
}

void DescTreeNode::RoughAccumulate(MIIterator &mit) {
  if (left && right) {
    left->RoughAccumulate(mit);
    right->RoughAccumulate(mit);
  } else {
    if (desc.rv == common::RoughSetValue::RS_SOME)
      return;
    desc.EvaluateRoughlyPack(mit);  // updating desc.rv inside
  }
}

bool DescTreeNode::UseRoughAccumulated() {
  if (left && right) {
    bool res1 = left->UseRoughAccumulated();
    bool res2 = right->UseRoughAccumulated();
    return (res1 || res2);
  } else {
    bool res = false;
    if (desc.rv == common::RoughSetValue::RS_NONE) {
      desc.op = common::Operator::O_FALSE;
      res = true;
    } else if (desc.rv == common::RoughSetValue::RS_ALL) {
      desc.op = common::Operator::O_TRUE;
      res = true;
    }
    return res;
  }
}

void DescTreeNode::ClearRoughValues() {
  if (left && right) {
    left->ClearRoughValues();
    right->ClearRoughValues();
  } else
    desc.ClearRoughValues();
}

void DescTreeNode::MakeSingleColsPrivate(std::vector<vcolumn::VirtualColumn *> &virt_cols) {
  if (left && right) {
    left->MakeSingleColsPrivate(virt_cols);
    right->MakeSingleColsPrivate(virt_cols);
  } else {
    if (desc.attr.vc && static_cast<int>(desc.attr.vc->IsSingleColumn())) {
      size_t i = 0;
      for (; i < virt_cols.size(); i++)
        if (virt_cols[i] == desc.attr.vc)
          break;
      DEBUG_ASSERT(i < virt_cols.size());
      desc.attr.vc = CreateVCCopy(desc.attr.vc);
      desc.attr.is_vc_owner = true;
      virt_cols[i] = desc.attr.vc;
    }
    if (desc.val1.vc && static_cast<int>(desc.val1.vc->IsSingleColumn())) {
      size_t i = 0;
      for (; i < virt_cols.size(); i++)
        if (virt_cols[i] == desc.val1.vc)
          break;
      DEBUG_ASSERT(i < virt_cols.size());
      desc.val1.vc = CreateVCCopy(desc.val1.vc);
      desc.val1.is_vc_owner = true;
      virt_cols[i] = desc.val1.vc;
    }
    if (desc.val2.vc && static_cast<int>(desc.val2.vc->IsSingleColumn())) {
      size_t i = 0;
      for (; i < virt_cols.size(); i++)
        if (virt_cols[i] == desc.val2.vc)
          break;
      DEBUG_ASSERT(i < virt_cols.size());
      desc.val2.vc = CreateVCCopy(desc.val2.vc);
      desc.val2.is_vc_owner = true;
      virt_cols[i] = desc.val2.vc;
    }
  }
}

bool DescTreeNode::IsTIANMUItemsEmpty() {
  if (left && right)
    return left->IsTIANMUItemsEmpty() && right->IsTIANMUItemsEmpty();
  else
    return desc.IsTIANMUItemsEmpty();
}

bool DescTreeNode::WithoutAttrs() {
  if (left && right)
    return left->WithoutAttrs() && right->WithoutAttrs();
  else
    return desc.WithoutAttrs();
}

bool DescTreeNode::WithoutTypeCast() {
  if (left && right)
    return left->WithoutTypeCast() && right->WithoutTypeCast();
  else
    return desc.WithoutTypeCast();
}

void DescTreeNode::IncreaseDescriptorCount(std::vector<std::pair<int, Descriptor>> &desc_counts) {
  auto p = std::find_if(desc_counts.begin(), desc_counts.end(),
                        std::bind(&DescriptorEqual, std::make_pair(0, std::ref(desc)), std::placeholders::_1));
  if (p != desc_counts.end())
    p->first++;
  else
    desc_counts.push_back(std::make_pair(1, desc));
}

// DescTree fun for Mutithread; Single above reserved

void Descriptor::MClearRoughValues(int taskid) {
  if (IsType_OrTree())
    tree->root->MClearRoughValues(taskid);
  else
    rvs[taskid] = common::RoughSetValue::RS_UNKNOWN;
}

void DescTreeNode::MClearRoughValues(int taskid) {
  if (left && right) {
    left->MClearRoughValues(taskid);
    right->MClearRoughValues(taskid);
  } else
    desc.MClearRoughValues(taskid);
}

common::RoughSetValue Descriptor::MEvaluateRoughlyPack(const MIIterator &mit, int taskid) {
  if (IsType_OrTree())
    return tree->root->MEvaluateRoughlyPack(mit, taskid);
  common::RoughSetValue r = common::RoughSetValue::RS_SOME;
  if (attr.vc /*&& !attr.vc->IsConst()*/)
    r = attr.vc->RoughCheck(mit, *this);
  if (rvs[taskid] == common::RoughSetValue::RS_UNKNOWN)
    rvs[taskid] = r;
  else if (rvs[taskid] == common::RoughSetValue::RS_NONE && r != common::RoughSetValue::RS_NONE)
    rvs[taskid] = common::RoughSetValue::RS_SOME;
  else if (rvs[taskid] == common::RoughSetValue::RS_ALL && r != common::RoughSetValue::RS_ALL)
    rvs[taskid] = common::RoughSetValue::RS_SOME;
  return r;
}

common::RoughSetValue DescTreeNode::MEvaluateRoughlyPack(const MIIterator &mit, int taskid) {
  if (desc.op == common::Operator::O_OR_TREE) {
    common::RoughSetValue left_res = left->MEvaluateRoughlyPack(mit, taskid);
    common::RoughSetValue right_res = right->MEvaluateRoughlyPack(mit, taskid);
    common::RoughSetValue r;
    if (desc.lop == common::LogicalOperator::O_AND)
      r = And(left_res, right_res);
    else
      r = Or(left_res, right_res);
    if (desc.rvs[taskid] == common::RoughSetValue::RS_UNKNOWN)
      desc.rvs[taskid] = r;
    else if (desc.rvs[taskid] == common::RoughSetValue::RS_NONE && r != common::RoughSetValue::RS_NONE)
      desc.rvs[taskid] = common::RoughSetValue::RS_SOME;
    else if (desc.rvs[taskid] == common::RoughSetValue::RS_ALL && r != common::RoughSetValue::RS_ALL)
      desc.rvs[taskid] = common::RoughSetValue::RS_SOME;
    return r;
  }
  return desc.MEvaluateRoughlyPack(mit, taskid);
}

void Descriptor::MLockSourcePacks(const MIIterator &mit, int taskid) {
  if (tree)
    tree->root->MPrepareToLock(0, taskid);
  if (attr.vc)
    attr.vc->LockSourcePacks(mit);
  if (val1.vc)
    val1.vc->LockSourcePacks(mit);
  if (val2.vc)
    val2.vc->LockSourcePacks(mit);
}

void DescTreeNode::MPrepareToLock(int locked_by, int taskid) {
  locks[taskid] = locked_by;
  if (left) {  // i.e., not a leaf
    left->MPrepareToLock(locked_by, taskid);
    right->MPrepareToLock(locked_by, taskid);
  }
}

void Descriptor::InitRvs(int value) {
  parallsize = value;
  for (int i = 0; i < parallsize + 1; i++) {
    rvs.push_back(common::RoughSetValue::RS_UNKNOWN);
  }
}

void DescTreeNode::InitRvs(int value) {
  desc.InitRvs(value);
  if (left && right) {
    left->InitRvs(value);
    right->InitRvs(value);
  }
}

void DescTreeNode::InitLocks(int value) {
  locks_size = value;
  for (int i = 0; i < locks_size + 1; i++) {
    locks.push_back(0);
  }
  if (left && right) {
    left->InitLocks(value);
    right->InitLocks(value);
  }
}

void Descriptor::InitParallel(int value, MIIterator &mit) {
  parallsize = value;
  for (int i = 0; i < parallsize + 1; i++) {
    rvs.push_back(common::RoughSetValue::RS_UNKNOWN);
  }
  if (encoded) {
    PrepareValueSet(mit);
  } else if (IsType_OrTree()) {
    // muti thread for IsType_OrTree
    tree->root->InitRvs(value);
    tree->root->InitLocks(value);
    tree->root->PrepareValueSet(mit);
  }
}

void DescTreeNode::MEvaluatePack(MIUpdatingIterator &mit, int taskid) {
  int single_dim = mit.SingleFilterDim();
  // general case:
  if (single_dim == -1) {
    while (mit.IsValid()) {
      if (CheckCondition(mit) == false)
        mit.ResetCurrent();
      ++mit;
      if (mit.PackrowStarted())
        break;
    }
    return;
  }

  // optimized case
  if (left) {             // i.e., not a leaf
    DEBUG_ASSERT(right);  // if left is not empty so should be right
    if (desc.lop == common::LogicalOperator::O_AND) {
      if (left->desc.rvs[taskid] == common::RoughSetValue::RS_NONE ||
          right->desc.rvs[taskid] == common::RoughSetValue::RS_NONE) {
        mit.ResetCurrentPack();
        mit.NextPackrow();
        return;
      }
      if (left->desc.rvs[taskid] == common::RoughSetValue::RS_ALL &&
          right->desc.rvs[taskid] == common::RoughSetValue::RS_ALL) {
        mit.NextPackrow();
        return;
      }
      int pack_start = mit.GetCurPackrow(single_dim);
      if (left->desc.rvs[taskid] != common::RoughSetValue::RS_ALL && mit.IsValid())
        left->MEvaluatePack(mit, taskid);
      if (right->desc.rvs[taskid] != common::RoughSetValue::RS_ALL && mit.RewindToPack(pack_start) &&
          mit.IsValid())  // otherwise the pack is already empty
        right->MEvaluatePack(mit, taskid);
      return;
    } else {
      if (left->desc.rvs[taskid] == common::RoughSetValue::RS_NONE &&
          right->desc.rvs[taskid] == common::RoughSetValue::RS_NONE) {
        mit.ResetCurrentPack();
        mit.NextPackrow();
        return;
      }
      if (left->desc.rvs[taskid] == common::RoughSetValue::RS_ALL ||
          right->desc.rvs[taskid] == common::RoughSetValue::RS_ALL) {
        mit.NextPackrow();
        return;
      }
      if (left->desc.rvs[taskid] == common::RoughSetValue::RS_NONE)
        right->MEvaluatePack(mit, taskid);
      else if (right->desc.rvs[taskid] == common::RoughSetValue::RS_NONE)
        left->MEvaluatePack(mit, taskid);
      else {
        int pack_start = mit.GetCurPackrow(single_dim);
        std::unique_ptr<Filter> f(mit.NewPackFilter(pack_start));
        left->MEvaluatePack(mit, taskid);
        if (mit.SwapPackFilter(pack_start,
                               f.get())) {  // return true if the pack in the
                                            // current iterator is not full
          mit.RewindToPack(pack_start);
          right->MEvaluatePack(mit, taskid);
          mit.OrPackFilter(pack_start, f.get());
        }
      }
      return;
    }
  } else {  // i.e., a leaf
    if (locks[taskid] >= 0) {
      desc.LockSourcePacks(mit, locks[taskid]);
      locks[taskid] = -1;
    }
    desc.EvaluatePackImpl(mit);
  }
}

}  // namespace core
}  // namespace Tianmu
