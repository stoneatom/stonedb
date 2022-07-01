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

#include "const_column.h"

#include "core/compiled_query.h"
#include "core/mysql_expression.h"
#include "core/rc_attr.h"
#include "core/transaction.h"

namespace stonedb {
namespace vcolumn {

ConstColumn::ConstColumn(core::ValueOrNull const &val, core::ColumnType const &c, bool shift_to_UTC)
    : VirtualColumn(c, NULL), value_(val) {
  dimension_ = -1;
  if (ct.IsString()) ct.SetPrecision(c.GetPrecision());

  if (c.GetTypeName() == common::CT::TIMESTAMP && shift_to_UTC) {
    types::RCDateTime rcdt(val.Get64(), common::CT::TIMESTAMP);
    // needs to convert value_ to UTC
    MYSQL_TIME myt;
    std::memset(&myt, 0, sizeof(MYSQL_TIME));
    myt.year = rcdt.Year();
    myt.month = rcdt.Month();
    myt.day = rcdt.Day();
    myt.hour = rcdt.Hour();
    myt.minute = rcdt.Minute();
    myt.second = rcdt.Second();
    myt.second_part = rcdt.MicroSecond();
    myt.time_type = MYSQL_TIMESTAMP_DATETIME;
    if (!common::IsTimeStampZero(myt)) {
      my_bool myb;
      // convert local time to UTC seconds from beg. of EPOCHE
      my_time_t secs_utc = current_tx->Thd()->variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
      // UTC seconds converted to UTC TIME
      common::GMTSec2GMTTime(&myt, secs_utc);
      myt.second_part = rcdt.MicroSecond();
    }
    rcdt = types::RCDateTime(myt, common::CT::TIMESTAMP);
    value_.SetFixed(rcdt.GetInt64());
  }
}

ConstColumn::ConstColumn(const types::RCValueObject &v, const core::ColumnType &c) : VirtualColumn(c, NULL), value_() {
  dimension_ = -1;
  if (c.IsString()) {
    value_ = core::ValueOrNull(v.ToBString());
    ct.SetPrecision(value_.StrLen());
  } else if (c.IsNumeric() && !c.IsDateTime()) {
    if (v.GetValueType() == types::ValueTypeEnum::NUMERIC_TYPE)
      value_ = core::ValueOrNull(static_cast<types::RCNum &>(v));
    else if (v.GetValueType() == types::ValueTypeEnum::STRING_TYPE) {
      types::RCNum rcn;
      if (c.IsFloat())
        types::RCNum::ParseReal(v.ToBString(), rcn, c.GetTypeName());
      else
        types::RCNum::ParseNum(v.ToBString(), rcn);
      value_ = rcn;
    } else if (v.GetValueType() == types::ValueTypeEnum::NULL_TYPE)
      value_ = core::ValueOrNull();
    else
      throw common::DataTypeConversionException(common::ErrorCode::DATACONVERSION);
  } else {
    DEBUG_ASSERT(v.GetValueType() == types::ValueTypeEnum::DATE_TIME_TYPE);
    value_ = core::ValueOrNull(static_cast<types::RCDateTime &>(v));
  }
}

double ConstColumn::GetValueDoubleImpl([[maybe_unused]] const core::MIIterator &mit) {
  DEBUG_ASSERT(core::ATI::IsNumericType(TypeName()));
  double val = 0;
  if (value_.IsNull())
    val = NULL_VALUE_D;
  else if (core::ATI::IsIntegerType(TypeName()))
    val = (double)value_.Get64();
  else if (core::ATI::IsFixedNumericType(TypeName()))
    val = ((double)value_.Get64()) / types::PowOfTen(ct.GetScale());
  else if (core::ATI::IsRealType(TypeName())) {
    union {
      double d;
      int64_t i;
    } u;
    u.i = value_.Get64();
    val = u.d;
  } else if (core::ATI::IsDateTimeType(TypeName())) {
    types::RCDateTime vd(value_.Get64(),
                         TypeName());  // 274886765314048  -> 2000-01-01
    int64_t vd_conv = 0;
    vd.ToInt64(vd_conv);  // 2000-01-01  ->  20000101
    val = (double)vd_conv;
  } else if (core::ATI::IsStringType(TypeName())) {
    auto vs = value_.ToString();
    if (vs) val = std::stod(*vs);
  } else
    DEBUG_ASSERT(0 && "conversion to double not implemented");
  return val;
}

types::RCValueObject ConstColumn::GetValueImpl([[maybe_unused]] const core::MIIterator &mit, bool lookup_to_num) {
  if (value_.IsNull()) return types::RCValueObject();

  if (core::ATI::IsStringType((TypeName()))) {
    types::BString s;
    value_.GetBString(s);
    return s;
  }
  if (core::ATI::IsIntegerType(TypeName())) return types::RCNum(value_.Get64(), -1, false, TypeName());
  if (core::ATI::IsDateTimeType(TypeName())) return types::RCDateTime(value_.Get64(), TypeName());
  if (core::ATI::IsRealType(TypeName())) return types::RCNum(value_.Get64(), 0, true, TypeName());
  if (lookup_to_num || TypeName() == common::CT::NUM) return types::RCNum((int64_t)value_.Get64(), Type().GetScale());
  DEBUG_ASSERT(!"Illegal execution path");
  return types::RCValueObject();
}

void ConstColumn::GetValueStringImpl(types::BString &s, const core::MIIterator &mit) { s = GetValue(mit).ToBString(); }

int64_t ConstColumn::GetSumImpl(const core::MIIterator &mit, bool &nonnegative) {
  DEBUG_ASSERT(!core::ATI::IsStringType(TypeName()));
  nonnegative = true;
  if (value_.IsNull())
    return common::NULL_VALUE_64;  // note that this is a bit ambiguous: the
                                   // same is for sum of nulls and for "not
                                   // implemented"
  if (core::ATI::IsRealType(TypeName())) {
    double res = value_.GetDouble() * mit.GetPackSizeLeft();
    return *(int64_t *)&res;
  }
  return (value_.Get64() * mit.GetPackSizeLeft());
}

types::BString ConstColumn::GetMinStringImpl([[maybe_unused]] const core::MIIterator &mit) {
  types::BString s;
  value_.GetBString(s);
  return s;
}

types::BString ConstColumn::GetMaxStringImpl([[maybe_unused]] const core::MIIterator &mit) {
  types::BString s;
  value_.GetBString(s);
  return s;
}

int64_t ConstColumn::GetApproxDistValsImpl([[maybe_unused]] bool incl_nulls,
                                           [[maybe_unused]] core::RoughMultiIndex *rough_mind) {
  return 1;
}

int64_t ConstColumn::GetExactDistVals() { return (value_.IsNull() ? 0 : 1); }

size_t ConstColumn::MaxStringSizeImpl()  // maximal byte string length in column
{
  return ct.GetDisplaySize();
}

core::PackOntologicalStatus ConstColumn::GetPackOntologicalStatusImpl([[maybe_unused]] const core::MIIterator &mit) {
  if (value_.IsNull()) return core::PackOntologicalStatus::NULLS_ONLY;
  return core::PackOntologicalStatus::UNIFORM;
}

void ConstColumn::EvaluatePackImpl([[maybe_unused]] core::MIUpdatingIterator &mit,
                                   [[maybe_unused]] core::Descriptor &desc) {
  DEBUG_ASSERT(0);  // comparison of a const with a const should be simplified earlier
}

char *ConstColumn::ToString(char p_buf[], size_t buf_ct) const {
  if (value_.IsNull() || value_.Get64() == common::NULL_VALUE_64)
    std::snprintf(p_buf, buf_ct, "<null>");
  else if (value_.Get64() == common::PLUS_INF_64)
    std::snprintf(p_buf, buf_ct, "+inf");
  else if (value_.Get64() == common::MINUS_INF_64)
    std::snprintf(p_buf, buf_ct, "-inf");
  else if (ct.IsInt())
    std::snprintf(p_buf, buf_ct, "%ld", value_.Get64());
  else if (ct.IsFixed())
    std::snprintf(p_buf, buf_ct, "%g", value_.Get64() / types::PowOfTen(ct.GetScale()));
  else if (ct.IsFloat())
    std::snprintf(p_buf, buf_ct, "%g", value_.GetDouble());
  else if (ct.IsString()) {
    types::BString val;
    value_.GetBString(val);
    std::snprintf(p_buf, buf_ct - 2, "\"%.*s", (int)(val.len < buf_ct - 4 ? val.len : buf_ct - 4),
                  val.GetDataBytesPointer());
    std::strcat(p_buf, "\"");
  }

  return p_buf;
}

}  // namespace vcolumn
}  // namespace stonedb
