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

SingleColumn::SingleColumn(core::PhysicalColumn *col, core::MultiIndex *multi_idx_, int alias, int col_num,
                           core::JustATable *source_table, int d)
    : VirtualColumn(col->Type(), multi_idx_), col_(col) {
  dimension_ = d;
  VarMap vm(core::VarID(alias, col_num), source_table, dimension_);
  var_map_.push_back(vm);
}

SingleColumn::SingleColumn(const SingleColumn &sc) : VirtualColumn(sc), col_(sc.col_) {
  dimension_ = sc.dimension_;
  var_map_ = sc.var_map_;
}

SingleColumn::~SingleColumn() {}

void SingleColumn::TranslateSourceColumns(std::map<core::PhysicalColumn *, core::PhysicalColumn *> &attr_translation) {
  if (attr_translation.find(col_) != attr_translation.end()) col_ = attr_translation[col_];
}

double SingleColumn::GetValueDoubleImpl(const core::MIIterator &mit) {
  double val = 0;
  if (col_->IsNull(mit[dimension_])) {
    return NULL_VALUE_D;
  } else if (core::ATI::IsIntegerType(TypeName())) {
    val = double(col_->GetValueInt64(mit[dimension_]));
  } else if (core::ATI::IsFixedNumericType(TypeName())) {
    int64_t v = col_->GetValueInt64(mit[dimension_]);
    val = (static_cast<double>(v)) / types::PowOfTen(ct.GetScale());
  } else if (core::ATI::IsRealType(TypeName())) {
    union {
      double d;
      int64_t i;
    } u;
    u.i = col_->GetValueInt64(mit[dimension_]);
    val = u.d;
  } else if (core::ATI::IsDateTimeType(TypeName())) {
    int64_t v = col_->GetValueInt64(mit[dimension_]);
    types::RCDateTime vd(v, TypeName());  // 274886765314048  ->  2000-01-01
    int64_t vd_conv = 0;
    vd.ToInt64(vd_conv);  // 2000-01-01  ->  20000101
    val = static_cast<double> (vd_conv);
  } else if (core::ATI::IsStringType(TypeName())) {
    types::BString vrcbs;
    col_->GetValueString(mit[dimension_], vrcbs);
    auto vs = std::make_unique<char[]>(vrcbs.len + 1);
    std::memcpy(vs.get(), vrcbs.GetDataBytesPointer(), vrcbs.len);
    vs[vrcbs.len] = '\0';
    val = std::atof(vs.get());
  } else
    DEBUG_ASSERT(0 && "conversion to double not implemented");
  return val;
}

types::RCValueObject SingleColumn::GetValueImpl(const core::MIIterator &mit, bool lookup_to_num) {
  return col_->GetValue(mit[dimension_], lookup_to_num);
}

int64_t SingleColumn::GetSumImpl(const core::MIIterator &mit, bool &nonnegative) {
  // DEBUG_ASSERT(!core::ATI::IsStringType(TypeName()));
  if (mit.WholePack(dimension_) && mit.GetCurPackrow(dimension_) >= 0) return col_->GetSum(mit.GetCurPackrow(dimension_), nonnegative);
  nonnegative = false;
  return common::NULL_VALUE_64;
}

int64_t SingleColumn::GetApproxSumImpl(const core::MIIterator &mit, bool &nonnegative) {
  // DEBUG_ASSERT(!core::ATI::IsStringType(TypeName()));
  if (mit.GetCurPackrow(dimension_) >= 0) return col_->GetSum(mit.GetCurPackrow(dimension_), nonnegative);
  nonnegative = false;
  return common::NULL_VALUE_64;
}

int64_t SingleColumn::GetMinInt64Impl(const core::MIIterator &mit) {
  if (mit.GetCurPackrow(dimension_) >= 0 && !(col_->Type().IsString() && !col_->Type().IsLookup()))
    return col_->GetMinInt64(mit.GetCurPackrow(dimension_));
  else
    return common::MINUS_INF_64;
}

int64_t SingleColumn::GetMaxInt64Impl(const core::MIIterator &mit) {
  if (mit.GetCurPackrow(dimension_) >= 0 && !(col_->Type().IsString() && !col_->Type().IsLookup()))
    return col_->GetMaxInt64(mit.GetCurPackrow(dimension_));
  else
    return common::PLUS_INF_64;
}

int64_t SingleColumn::GetMinInt64ExactImpl(const core::MIIterator &mit) {
  if (mit.GetCurPackrow(dimension_) >= 0 && mit.WholePack(dimension_) && !(col_->Type().IsString() && !col_->Type().IsLookup())) {
    int64_t val = col_->GetMinInt64(mit.GetCurPackrow(dimension_));
    if (val != common::MINUS_INF_64) return val;
  }
  return common::NULL_VALUE_64;
}

int64_t SingleColumn::GetMaxInt64ExactImpl(const core::MIIterator &mit) {
  if (mit.GetCurPackrow(dimension_) >= 0 && mit.WholePack(dimension_) && !(col_->Type().IsString() && !col_->Type().IsLookup())) {
    int64_t val = col_->GetMaxInt64(mit.GetCurPackrow(dimension_));
    if (val != common::PLUS_INF_64) return val;
  }
  return common::NULL_VALUE_64;
}

types::BString SingleColumn::GetMinStringImpl(const core::MIIterator &mit) {
  return col_->GetMinString(mit.GetCurPackrow(dimension_));
}

types::BString SingleColumn::GetMaxStringImpl(const core::MIIterator &mit) {
  return col_->GetMaxString(mit.GetCurPackrow(dimension_));
}

int64_t SingleColumn::GetApproxDistValsImpl(bool incl_nulls, core::RoughMultiIndex *rough_mind) {
  if (rough_mind)
    return col_->ApproxDistinctVals(incl_nulls, multi_idx_->GetFilter(dimension_), rough_mind->GetRSValueTable(dimension_), true);
  return col_->ApproxDistinctVals(incl_nulls, multi_idx_->GetFilter(dimension_), NULL, multi_idx_->NullsExist(dimension_));
}

int64_t SingleColumn::GetExactDistVals() { return col_->ExactDistinctVals(multi_idx_->GetFilter(dimension_)); }

double SingleColumn::RoughSelectivity() { return col_->RoughSelectivity(); }

std::vector<int64_t> SingleColumn::GetListOfDistinctValues(core::MIIterator const &mit) {
  int pack = mit.GetCurPackrow(dimension_);
  if (pack < 0 ||
      (!mit.WholePack(dimension_) && col_->GetPackOntologicalStatus(pack) != core::PackOntologicalStatus::UNIFORM)) {
    std::vector<int64_t> empty;
    return empty;
  }
  return col_->GetListOfDistinctValuesInPack(pack);
}

bool SingleColumn::TryToMerge(core::Descriptor &d1, core::Descriptor &d2) { return col_->TryToMerge(d1, d2); }

size_t SingleColumn::MaxStringSizeImpl()  // maximal byte string length in column
{
  return col_->MaxStringSize(multi_idx_->NoDimensions() == 0 ? NULL : multi_idx_->GetFilter(dimension_));
}

core::PackOntologicalStatus SingleColumn::GetPackOntologicalStatusImpl(const core::MIIterator &mit) {
  return col_->GetPackOntologicalStatus(mit.GetCurPackrow(dimension_));
}

void SingleColumn::EvaluatePackImpl(core::MIUpdatingIterator &mit, core::Descriptor &desc) {
  col_->EvaluatePack(mit, dimension_, desc);
}

common::RSValue SingleColumn::RoughCheckImpl(const core::MIIterator &mit, core::Descriptor &d) {
  if (mit.GetCurPackrow(dimension_) >= 0) {
    // check whether isn't it a join
    SingleColumn *sc = NULL;
    if (d.val1.vc && static_cast<int>(d.val1.vc->IsSingleColumn())) sc = static_cast<SingleColumn *>(d.val1.vc);
    if (sc && sc->dimension_ != dimension_)  // Pack2Pack rough check
      return col_->RoughCheck(mit.GetCurPackrow(dimension_), mit.GetCurPackrow(sc->dimension_), d);
    else  // One-dimension_ rough check
      return col_->RoughCheck(mit.GetCurPackrow(dimension_), d, mit.NullsPossibleInPack(dimension_));
  } else
    return common::RSValue::RS_SOME;
}

void SingleColumn::DisplayAttrStats() { col_->DisplayAttrStats(multi_idx_->GetFilter(dimension_)); }

char *SingleColumn::ToString(char p_buf[], size_t buf_ct) const {
  int attr_no = col_->AttrNo();
  if (attr_no > -1)
    std::snprintf(p_buf, buf_ct, "t%da%d", dimension_, attr_no);
  else
    std::snprintf(p_buf, buf_ct, "t%da*", dimension_);
  return p_buf;
}

}  // namespace vcolumn
}  // namespace stonedb
