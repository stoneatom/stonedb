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

#include "core/filter.h"
#include "core/query.h"
#include "core/temp_table.h"

#include "vc/const_column.h"
#include "vc/const_expr_column.h"
#include "vc/in_set_column.h"
#include "vc/single_column.h"
#include "vc/subselect_column.h"
#include "vc/type_cast_column.h"

namespace Tianmu {
namespace core {

TempTable::TempTable(JustATable *t, int alias, Query *q)
    : mem_scale_(-1), filter_(t->Getpackpower()), output_multi_index_(t->Getpackpower()), m_conn_(current_txn_) {
  p_power_ = t->Getpackpower();
  filter_.table = this;
  src_tables_.push_back(t);
  aliases_.push_back(alias);

  if (t->TableType() == TType::TEMP_TABLE) {
    has_temp_table_ = true;
    if (q->IsRoughQuery())
      ((TempTable *)t)->RoughMaterialize(false, nullptr, true);
    else
      ((TempTable *)t)->Materialize(false, nullptr, false);
    filter_.mind->AddDimension_cross(t->NumOfObj());
  } else {
    filter_.mind->AddDimension_cross(t->NumOfObj());
  }

  if (filter_.mind->TooManyTuples())
    num_of_obj_ = common::NULL_VALUE_64;  // a big, improper number, which we hope to
                                          // be changed after conditions are applied
  else
    num_of_obj_ = filter_.mind->NumOfTuples();

  num_of_columns_ = 0;
  num_of_global_virtual_columns_ = 0;

  lazy_ = false;
  num_of_materialized_ = 0;
  is_sent_ = false;
  rough_is_empty_ = common::TRIBOOL_UNKNOWN;
}

void TempTable::JoinT(JustATable *t, int alias, JoinType jt) {
  if (jt != JoinType::JO_INNER)
    throw common::NotImplementedException("left/right/outer join is not implemented.");

  src_tables_.push_back(t);
  aliases_.push_back(alias);

  if (t->TableType() == TType::TEMP_TABLE) {
    has_temp_table_ = true;
    ((TempTable *)t)->Materialize();
    filter_.mind->AddDimension_cross(t->NumOfObj());
  } else
    filter_.mind->AddDimension_cross(t->NumOfObj());

  join_types_.push_back(jt);

  if (filter_.mind->TooManyTuples())
    num_of_obj_ = common::NULL_VALUE_64;  // a big, improper number, which we hope to
                                          // be changed after conditions are applied
  else
    num_of_obj_ = filter_.mind->NumOfTuples();
}

}  // namespace core

vcolumn::VirtualColumn *CreateVCCopy(vcolumn::VirtualColumn *vc) {
  if (static_cast<int>(vc->IsSingleColumn())) {
    return new vcolumn::SingleColumn(*static_cast<const vcolumn::SingleColumn *>(vc));
  } else if (dynamic_cast<vcolumn::ExpressionColumn *>(vc)) {
    return new vcolumn::ExpressionColumn(*static_cast<const vcolumn::ExpressionColumn *>(vc));
  } else if (dynamic_cast<vcolumn::ConstExpressionColumn *>(vc)) {
    return new vcolumn::ConstExpressionColumn(*static_cast<const vcolumn::ConstExpressionColumn *>(vc));
  } else if (dynamic_cast<vcolumn::ConstColumn *>(vc)) {
    return new vcolumn::ConstColumn(*static_cast<const vcolumn::ConstColumn *>(vc));
  } else if (vc->IsInSet()) {
    return new vcolumn::InSetColumn(*static_cast<const vcolumn::InSetColumn *>(vc));
  } else if (vc->IsSubSelect()) {
    return new vcolumn::SubSelectColumn(*static_cast<const vcolumn::SubSelectColumn *>(vc));
  } else if (dynamic_cast<vcolumn::TypeCastColumn *>(vc))
    return new vcolumn::TypeCastColumn(*static_cast<const vcolumn::TypeCastColumn *>(vc));
  else {
    DEBUG_ASSERT(0 && "Cannot copy VC");
    return vc;
  }
}

}  // namespace Tianmu
