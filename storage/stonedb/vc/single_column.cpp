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

#include "single_column.h"

#include "core/compiled_query.h"
#include "core/mysql_expression.h"
#include "core/rc_attr.h"

namespace stonedb {
namespace vcolumn {
SingleColumn::SingleColumn(core::PhysicalColumn *col, core::MultiIndex *mind, int alias, int col_no,
                           core::JustATable *source_table, int d)
    : VirtualColumn(col->Type(), mind), col(col) {
  dim = d;
  VarMap vm(core::VarID(alias, col_no), source_table, dim);
  var_map.push_back(vm);
}

SingleColumn::SingleColumn(const SingleColumn &sc) : VirtualColumn(sc), col(sc.col) {
  dim = sc.dim;
  var_map = sc.var_map;
}

SingleColumn::~SingleColumn() {}

void SingleColumn::TranslateSourceColumns(std::map<core::PhysicalColumn *, core::PhysicalColumn *> &attr_translation) {
  if (attr_translation.find(col) != attr_translation.end()) col = attr_translation[col];
}

double SingleColumn::DoGetValueDouble(const core::MIIterator &mit) {
  double val = 0;
  if (col->IsNull(mit[dim])) {
    return NULL_VALUE_D;
  } else if (core::ATI::IsIntegerType(TypeName())) {
    val = double(col->GetValueInt64(mit[dim]));
  } else if (core::ATI::IsFixedNumericType(TypeName())) {
    int64_t v = col->GetValueInt64(mit[dim]);
    val = ((double)v) / types::PowOfTen(ct.GetScale());
  } else if (core::ATI::IsRealType(TypeName())) {
    union {
      double d;
      int64_t i;
    } u;
    u.i = col->GetValueInt64(mit[dim]);
    val = u.d;
  } else if (core::ATI::IsDateTimeType(TypeName())) {
    int64_t v = col->GetValueInt64(mit[dim]);
    types::RCDateTime vd(v, TypeName());  // 274886765314048  ->  2000-01-01
    int64_t vd_conv = 0;
    vd.ToInt64(vd_conv);  // 2000-01-01  ->  20000101
    val = (double)vd_conv;
  } else if (core::ATI::IsStringType(TypeName())) {
    types::BString vrcbs;
    col->GetValueString(mit[dim], vrcbs);
    auto vs = std::make_unique<char[]>(vrcbs.len + 1);
    std::memcpy(vs.get(), vrcbs.GetDataBytesPointer(), vrcbs.len);
    vs[vrcbs.len] = '\0';
    val = std::atof(vs.get());
  } else
    DEBUG_ASSERT(0 && "conversion to double not implemented");
  return val;
}

types::RCValueObject SingleColumn::DoGetValue(const core::MIIterator &mit, bool lookup_to_num) {
  return col->GetValue(mit[dim], lookup_to_num);
}

int64_t SingleColumn::DoGetSum(const core::MIIterator &mit, bool &nonnegative) {
  // DEBUG_ASSERT(!core::ATI::IsStringType(TypeName()));
  if (mit.WholePack(dim) && mit.GetCurPackrow(dim) >= 0) return col->GetSum(mit.GetCurPackrow(dim), nonnegative);
  nonnegative = false;
  return common::NULL_VALUE_64;
}

int64_t SingleColumn::DoGetApproxSum(const core::MIIterator &mit, bool &nonnegative) {
  // DEBUG_ASSERT(!core::ATI::IsStringType(TypeName()));
  if (mit.GetCurPackrow(dim) >= 0) return col->GetSum(mit.GetCurPackrow(dim), nonnegative);
  nonnegative = false;
  return common::NULL_VALUE_64;
}

int64_t SingleColumn::DoGetMinInt64(const core::MIIterator &mit) {
  if (mit.GetCurPackrow(dim) >= 0 && !(col->Type().IsString() && !col->Type().IsLookup()))
    return col->GetMinInt64(mit.GetCurPackrow(dim));
  else
    return common::MINUS_INF_64;
}

int64_t SingleColumn::DoGetMaxInt64(const core::MIIterator &mit) {
  if (mit.GetCurPackrow(dim) >= 0 && !(col->Type().IsString() && !col->Type().IsLookup()))
    return col->GetMaxInt64(mit.GetCurPackrow(dim));
  else
    return common::PLUS_INF_64;
}

int64_t SingleColumn::DoGetMinInt64Exact(const core::MIIterator &mit) {
  if (mit.GetCurPackrow(dim) >= 0 && mit.WholePack(dim) && !(col->Type().IsString() && !col->Type().IsLookup())) {
    int64_t val = col->GetMinInt64(mit.GetCurPackrow(dim));
    if (val != common::MINUS_INF_64) return val;
  }
  return common::NULL_VALUE_64;
}

int64_t SingleColumn::DoGetMaxInt64Exact(const core::MIIterator &mit) {
  if (mit.GetCurPackrow(dim) >= 0 && mit.WholePack(dim) && !(col->Type().IsString() && !col->Type().IsLookup())) {
    int64_t val = col->GetMaxInt64(mit.GetCurPackrow(dim));
    if (val != common::PLUS_INF_64) return val;
  }
  return common::NULL_VALUE_64;
}

types::BString SingleColumn::DoGetMinString(const core::MIIterator &mit) {
  return col->GetMinString(mit.GetCurPackrow(dim));
}

types::BString SingleColumn::DoGetMaxString(const core::MIIterator &mit) {
  return col->GetMaxString(mit.GetCurPackrow(dim));
}

int64_t SingleColumn::DoGetApproxDistVals(bool incl_nulls, core::RoughMultiIndex *rough_mind) {
  if (rough_mind)
    return col->ApproxDistinctVals(incl_nulls, mind->GetFilter(dim), rough_mind->GetRSValueTable(dim), true);
  return col->ApproxDistinctVals(incl_nulls, mind->GetFilter(dim), NULL, mind->NullsExist(dim));
}

int64_t SingleColumn::GetExactDistVals() { return col->ExactDistinctVals(mind->GetFilter(dim)); }

double SingleColumn::RoughSelectivity() { return col->RoughSelectivity(); }

std::vector<int64_t> SingleColumn::GetListOfDistinctValues(core::MIIterator const &mit) {
  int pack = mit.GetCurPackrow(dim);
  if (pack < 0 ||
      (!mit.WholePack(dim) && col->GetPackOntologicalStatus(pack) != core::PackOntologicalStatus::UNIFORM)) {
    std::vector<int64_t> empty;
    return empty;
  }
  return col->GetListOfDistinctValuesInPack(pack);
}

bool SingleColumn::TryToMerge(core::Descriptor &d1, core::Descriptor &d2) { return col->TryToMerge(d1, d2); }

size_t SingleColumn::DoMaxStringSize()  // maximal byte string length in column
{
  return col->MaxStringSize(mind->NoDimensions() == 0 ? NULL : mind->GetFilter(dim));
}

core::PackOntologicalStatus SingleColumn::DoGetPackOntologicalStatus(const core::MIIterator &mit) {
  return col->GetPackOntologicalStatus(mit.GetCurPackrow(dim));
}

void SingleColumn::DoEvaluatePack(core::MIUpdatingIterator &mit, core::Descriptor &desc) {
  col->EvaluatePack(mit, dim, desc);
}

common::RSValue SingleColumn::DoRoughCheck(const core::MIIterator &mit, core::Descriptor &d) {
  if (mit.GetCurPackrow(dim) >= 0) {
    // check whether isn't it a join
    SingleColumn *sc = NULL;
    if (d.val1.vc && static_cast<int>(d.val1.vc->IsSingleColumn())) sc = static_cast<SingleColumn *>(d.val1.vc);
    if (sc && sc->dim != dim)  // Pack2Pack rough check
      return col->RoughCheck(mit.GetCurPackrow(dim), mit.GetCurPackrow(sc->dim), d);
    else  // One-dim rough check
      return col->RoughCheck(mit.GetCurPackrow(dim), d, mit.NullsPossibleInPack(dim));
  } else
    return common::RSValue::RS_SOME;
}

void SingleColumn::DisplayAttrStats() { col->DisplayAttrStats(mind->GetFilter(dim)); }

char *SingleColumn::ToString(char p_buf[], size_t buf_ct) const {
  int attr_no = col->AttrNo();
  if (attr_no > -1)
    std::snprintf(p_buf, buf_ct, "t%da%d", dim, attr_no);
  else
    std::snprintf(p_buf, buf_ct, "t%da*", dim);
  return p_buf;
}
}  // namespace vcolumn
}  // namespace stonedb
