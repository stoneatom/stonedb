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
#include "core/tianmu_attr.h"
#include "core/transaction.h"

namespace Tianmu {
namespace vcolumn {

ConstColumn::ConstColumn(core::ValueOrNull const &val, core::ColumnType const &c, bool shift_to_UTC)
    : VirtualColumn(c, nullptr), value_or_null_(val) {
  dim_ = -1;
  if (ct.IsString())
    ct.SetPrecision(c.GetPrecision());
  if (c.GetTypeName() == common::ColumnType::TIMESTAMP && shift_to_UTC) {
    types::TianmuDateTime tianmu_dt(val.Get64(), common::ColumnType::TIMESTAMP);
    // needs to convert value_or_null_ to UTC
    MYSQL_TIME myt;
    std::memset(&myt, 0, sizeof(MYSQL_TIME));
    myt.year = tianmu_dt.Year();
    myt.month = tianmu_dt.Month();
    myt.day = tianmu_dt.Day();
    myt.hour = tianmu_dt.Hour();
    myt.minute = tianmu_dt.Minute();
    myt.second = tianmu_dt.Second();
    myt.second_part = tianmu_dt.MicroSecond();
    myt.time_type = MYSQL_TIMESTAMP_DATETIME;
    if (!common::IsTimeStampZero(myt)) {
      my_bool myb;
      // convert local time to UTC seconds from beg. of EPOCHE
      my_time_t secs_utc = current_txn_->Thd()->variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
      // UTC seconds converted to UTC TIME
      common::GMTSec2GMTTime(&myt, secs_utc);
      myt.second_part = tianmu_dt.MicroSecond();
    }
    tianmu_dt = types::TianmuDateTime(myt, common::ColumnType::TIMESTAMP);
    value_or_null_.SetFixed(tianmu_dt.GetInt64());
  }
}

ConstColumn::ConstColumn(const types::TianmuValueObject &v, const core::ColumnType &c)
    : VirtualColumn(c, nullptr), value_or_null_() {
  dim_ = -1;
  if (c.IsString()) {
    value_or_null_ = core::ValueOrNull(v.ToBString());
    ct.SetPrecision(value_or_null_.StrLen());
  } else if (c.IsNumeric() && !c.IsDateTime()) {
    if (v.GetValueType() == types::ValueTypeEnum::NUMERIC_TYPE)
      value_or_null_ = core::ValueOrNull(static_cast<types::TianmuNum &>(v));
    else if (v.GetValueType() == types::ValueTypeEnum::STRING_TYPE) {
      types::TianmuNum tianmu_n;
      if (c.IsFloat())
        types::TianmuNum::ParseReal(v.ToBString(), tianmu_n, c.GetTypeName());
      else
        types::TianmuNum::ParseNum(v.ToBString(), tianmu_n);
      value_or_null_ = tianmu_n;
    } else if (v.GetValueType() == types::ValueTypeEnum::NULL_TYPE)
      value_or_null_ = core::ValueOrNull();
    else
      throw common::DataTypeConversionException(common::ErrorCode::DATACONVERSION);
  } else {
    DEBUG_ASSERT(v.GetValueType() == types::ValueTypeEnum::DATE_TIME_TYPE);
    // TODO: if it is non-date-time a proper conversion should be done here
    value_or_null_ = core::ValueOrNull(static_cast<types::TianmuDateTime &>(v));
  }
}

double ConstColumn::GetValueDoubleImpl([[maybe_unused]] const core::MIIterator &mit) {
  DEBUG_ASSERT(core::ATI::IsNumericType(TypeName()));
  double val = 0;
  if (value_or_null_.IsNull())
    val = NULL_VALUE_D;
  else if (core::ATI::IsIntegerType(TypeName()))
    val = (double)value_or_null_.Get64();
  else if (core::ATI::IsFixedNumericType(TypeName()))
    val = ((double)value_or_null_.Get64()) / types::PowOfTen(ct.GetScale());
  else if (core::ATI::IsRealType(TypeName())) {
    union {
      double d;
      int64_t i;
    } u;
    u.i = value_or_null_.Get64();
    val = u.d;
  } else if (core::ATI::IsDateTimeType(TypeName())) {
    types::TianmuDateTime vd(value_or_null_.Get64(),
                             TypeName());  // 274886765314048  -> 2000-01-01
    int64_t vd_conv = 0;
    vd.ToInt64(vd_conv);  // 2000-01-01  ->  20000101
    val = (double)vd_conv;
  } else if (core::ATI::IsStringType(TypeName())) {
    auto vs = value_or_null_.ToString();
    if (vs)
      val = std::stod(*vs);
  } else
    DEBUG_ASSERT(0 && "conversion to double not implemented");
  return val;
}

types::TianmuValueObject ConstColumn::GetValueImpl([[maybe_unused]] const core::MIIterator &mit, bool lookup_to_num) {
  if (value_or_null_.IsNull())
    return types::TianmuValueObject();

  if (core::ATI::IsStringType((TypeName()))) {
    types::BString s;
    value_or_null_.GetBString(s);
    return s;
  }
  if (core::ATI::IsIntegerType(TypeName()))
    return types::TianmuNum(value_or_null_.Get64(), -1, false, TypeName());
  if (core::ATI::IsDateTimeType(TypeName()))
    return types::TianmuDateTime(value_or_null_.Get64(), TypeName());
  if (core::ATI::IsRealType(TypeName()))
    return types::TianmuNum(value_or_null_.Get64(), 0, true, TypeName());
  if (lookup_to_num || TypeName() == common::ColumnType::NUM || TypeName() == common::ColumnType::BIT)
    return types::TianmuNum((int64_t)value_or_null_.Get64(), Type().GetScale());
  DEBUG_ASSERT(!"Illegal execution path");
  return types::TianmuValueObject();
}

void ConstColumn::GetValueStringImpl(types::BString &s, const core::MIIterator &mit) { s = GetValue(mit).ToBString(); }

int64_t ConstColumn::GetSumImpl(const core::MIIterator &mit, bool &nonnegative) {
  DEBUG_ASSERT(!core::ATI::IsStringType(TypeName()));
  nonnegative = true;
  if (value_or_null_.IsNull())
    return common::NULL_VALUE_64;  // note that this is a bit ambiguous: the
                                   // same is for sum of nulls and for "not
                                   // implemented"
  if (core::ATI::IsRealType(TypeName())) {
    double res = value_or_null_.GetDouble() * mit.GetPackSizeLeft();
    return *(int64_t *)&res;
  }
  return (value_or_null_.Get64() * mit.GetPackSizeLeft());
}

types::BString ConstColumn::GetMinStringImpl([[maybe_unused]] const core::MIIterator &mit) {
  types::BString s;
  value_or_null_.GetBString(s);
  return s;
}

types::BString ConstColumn::GetMaxStringImpl([[maybe_unused]] const core::MIIterator &mit) {
  types::BString s;
  value_or_null_.GetBString(s);
  return s;
}

int64_t ConstColumn::GetApproxDistValsImpl([[maybe_unused]] bool incl_nulls,
                                           [[maybe_unused]] core::RoughMultiIndex *rough_mind) {
  return 1;
}

int64_t ConstColumn::GetExactDistVals() { return (value_or_null_.IsNull() ? 0 : 1); }

size_t ConstColumn::MaxStringSizeImpl()  // maximal byte string length in column
{
  return ct.GetDisplaySize();
}

core::PackOntologicalStatus ConstColumn::GetPackOntologicalStatusImpl([[maybe_unused]] const core::MIIterator &mit) {
  if (value_or_null_.IsNull())
    return core::PackOntologicalStatus::NULLS_ONLY;
  return core::PackOntologicalStatus::UNIFORM;
}

void ConstColumn::EvaluatePackImpl([[maybe_unused]] core::MIUpdatingIterator &mit,
                                   [[maybe_unused]] core::Descriptor &desc) {
  DEBUG_ASSERT(0);  // comparison of a const with a const should be simplified earlier
}

char *ConstColumn::ToString(char p_buf[], size_t buf_ct) const {
  if (value_or_null_.IsNull() || value_or_null_.Get64() == common::NULL_VALUE_64)
    std::snprintf(p_buf, buf_ct, "<null>");
  else if (value_or_null_.Get64() == common::PLUS_INF_64)
    std::snprintf(p_buf, buf_ct, "+inf");
  else if (value_or_null_.Get64() == common::MINUS_INF_64)
    std::snprintf(p_buf, buf_ct, "-inf");
  else if (ct.IsInt())
    std::snprintf(p_buf, buf_ct, "%ld", value_or_null_.Get64());
  else if (ct.IsFixed())
    std::snprintf(p_buf, buf_ct, "%g", value_or_null_.Get64() / types::PowOfTen(ct.GetScale()));
  else if (ct.IsFloat())
    std::snprintf(p_buf, buf_ct, "%g", value_or_null_.GetDouble());
  else if (ct.IsString()) {
    types::BString val;
    value_or_null_.GetBString(val);
    std::snprintf(p_buf, buf_ct - 2, "\"%.*s", (int)(val.len_ < buf_ct - 4 ? val.len_ : buf_ct - 4),
                  val.GetDataBytesPointer());
    std::strcat(p_buf, "\"");
  }
  return p_buf;
}

}  // namespace vcolumn
}  // namespace Tianmu
