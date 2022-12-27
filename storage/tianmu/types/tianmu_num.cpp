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

#include "tianmu_num.h"

#include <cmath>

#include "common/assert.h"
#include "core/tools.h"
#include "system/txt_utils.h"
#include "types/tianmu_data_types.h"
#include "types/value_parser4txt.h"

namespace Tianmu {
namespace types {

TianmuNum::TianmuNum(common::ColumnType attrt)
    : value_(0), scale_(0), is_double_(false), is_dot_(false), attr_type_(attrt) {}

TianmuNum::TianmuNum(int64_t value, short scale, bool is_double, common::ColumnType attrt) {
  Assign(value, scale, is_double, attrt);
}

TianmuNum::TianmuNum(double value)
    : value_(*(int64_t *)&value), scale_(0), is_double_(true), is_dot_(false), attr_type_(common::ColumnType::REAL) {
  null_ = (value_ == NULL_VALUE_D ? true : false);
}

TianmuNum::TianmuNum(const TianmuNum &tianmu_n)
    : ValueBasic<TianmuNum>(tianmu_n),
      value_(tianmu_n.value_),
      scale_(tianmu_n.scale_),
      is_double_(tianmu_n.is_double_),
      is_dot_(tianmu_n.is_dot_),
      attr_type_(tianmu_n.attr_type_) {
  null_ = tianmu_n.null_;
}

TianmuNum::~TianmuNum() {}

TianmuNum &TianmuNum::Assign(int64_t value, short scale, bool is_double, common::ColumnType attrt) {
  this->value_ = value;
  this->scale_ = scale;
  this->is_double_ = is_double;
  this->attr_type_ = attrt;

  if (scale != -1 &&
      !is_double_) {  // check if construct decimal, the UNK is used on temp_table.cpp: GetValueString(..)
    if ((scale != 0 && attrt != common::ColumnType::BIT) || attrt == common::ColumnType::UNK) {
      is_dot_ = true;
      this->attr_type_ = common::ColumnType::NUM;
    }
  }
  if (scale <= -1 && !is_double_)
    scale_ = 0;
  if (is_double_) {
    if (!(this->attr_type_ == common::ColumnType::REAL || this->attr_type_ == common::ColumnType::FLOAT))
      this->attr_type_ = common::ColumnType::REAL;
    this->is_dot_ = false;
    scale_ = 0;
    null_ = (value_ == *(int64_t *)&NULL_VALUE_D ? true : false);
  } else
    null_ = (value_ == common::NULL_VALUE_64 ? true : false);
  return *this;
}

TianmuNum &TianmuNum::Assign(double value) {
  this->value_ = *(int64_t *)&value;
  this->scale_ = 0;
  this->is_double_ = true;
  this->is_dot_ = false;
  this->attr_type_ = common::ColumnType::REAL;
  common::double_int_t v(value_);
  null_ = (v.i == common::NULL_VALUE_64 ? true : false);
  return *this;
}

common::ErrorCode TianmuNum::Parse(const BString &tianmu_s, TianmuNum &tianmu_n, common::ColumnType at) {
  return ValueParserForText::Parse(tianmu_s, tianmu_n, at);
}

common::ErrorCode TianmuNum::ParseReal(const BString &tianmu_s, TianmuNum &tianmu_n, common::ColumnType at) {
  return ValueParserForText::ParseReal(tianmu_s, tianmu_n, at);
}

common::ErrorCode TianmuNum::ParseNum(const BString &tianmu_s, TianmuNum &tianmu_n, short scale) {
  return ValueParserForText::ParseNum(tianmu_s, tianmu_n, scale);
}

TianmuNum &TianmuNum::operator=(const TianmuNum &tianmu_n) {
  value_ = tianmu_n.value_;
  is_double_ = tianmu_n.is_double_;
  scale_ = tianmu_n.scale_;
  null_ = tianmu_n.null_;
  attr_type_ = tianmu_n.attr_type_;
  return *this;
}

TianmuNum &TianmuNum::operator=(const TianmuDataType &tianmu_dt) {
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    *this = (TianmuNum &)tianmu_dt;
  else {
    TianmuNum tianmu_num1;
    if (common::IsError(TianmuNum::Parse(tianmu_dt.ToBString(), tianmu_num1, this->attr_type_))) {
      *this = tianmu_num1;
    } else {
      TIANMU_ERROR("Unsupported assign operation!");
      null_ = true;
    }
  }
  return *this;
}

common::ColumnType TianmuNum::Type() const { return attr_type_; }

bool TianmuNum::IsDecimal(ushort scale) const {
  if (core::ATI::IsIntegerType(this->attr_type_) || attr_type_ == common::ColumnType::BIT) {
    return GetDecIntLen() <= (MAX_DEC_PRECISION - scale);
  } else if (attr_type_ == common::ColumnType::NUM) {
    if (this->GetDecFractLen() <= scale)
      return true;
    if (scale_ > scale)
      return value_ % (int64_t)Uint64PowOfTen(scale_ - scale) == 0;
    return true;
  } else {
    double f = GetFractPart();
    return f == 0.0 || (fabs(f) >= (1.0 / (double)Uint64PowOfTen(scale)));
  }
  return false;
}

bool TianmuNum::IsInt() const {
  if (!is_double_) {
    if ((value_ % (int64_t)Uint64PowOfTen(scale_)) != 0) {
      return false;
    }
    return true;
  }
  return false;
}

TianmuNum TianmuNum::ToDecimal(int scale) const {
  int64_t tmpv = 0;
  short tmpp = 0;
  int sign = 1;

  if (is_double_) {
    double intpart(0);
    double fracpart(modf(*(double *)&value_, &intpart));

    if (intpart < 0 || fracpart < 0) {
      sign = -1;
      intpart = fabs(intpart);
      fracpart = fabs(fracpart);
    }

    if (scale == -1) {
      if (intpart >= Uint64PowOfTen(MAX_DEC_PRECISION)) {
        if (sign != 1)
          scale = 0;
      } else {
        int l = 0;
        if (intpart != 0)
          l = (int)floor(log10(intpart)) + 1;
        scale = MAX_DEC_PRECISION - l;
      }
    }

    if (intpart >= Uint64PowOfTen(MAX_DEC_PRECISION)) {
      tmpv = (Uint64PowOfTen(MAX_DEC_PRECISION) - 1);
    } else
      tmpv = (int64_t)intpart * Uint64PowOfTen(scale) + std::llround(fracpart * Uint64PowOfTen(scale));

    tmpp = scale;
  } else {
    tmpv = this->value_;
    tmpp = this->scale_;
    if (scale != -1) {
      if (tmpp > scale)
        tmpv /= (int64_t)Uint64PowOfTen(tmpp - scale);
      else
        tmpv *= (int64_t)Uint64PowOfTen(scale - tmpp);
      tmpp = scale;
    }
  }
  return TianmuNum(tmpv * sign, tmpp);
}

TianmuNum TianmuNum::ToReal() const {
  if (core::ATI::IsRealType(attr_type_)) {
    return TianmuNum(*(double *)&value_);
  }
  return TianmuNum((double)((double)(this->value_ / PowOfTen(scale_))));
}

TianmuNum TianmuNum::ToInt() const { return GetIntPart(); }

static char *Text(int64_t value, char buf[], int scale) {
  bool sign = true;
  if (value < 0) {
    sign = false;
    value *= -1;
  }
  longlong2str(value, buf, 10);
  int l = (int)std::strlen(buf);
  std::memset(buf + l + 1, ' ', 21 - l);
  int pos = 21;
  int i = 0;
  for (i = l; i >= 0; i--) {
    if (scale != 0 && pos + scale == 20) {
      buf[pos--] = '.';
      i++;
    } else {
      buf[pos--] = buf[i];
      buf[i] = ' ';
    }
  }

  if (scale >= l) {
    buf[20 - scale] = '.';
    buf[20 - scale - 1] = '0';
    i = 20 - scale + 1;
    while (buf[i] == ' ') buf[i++] = '0';
  }
  pos = 0;
  while (buf[pos] == ' ') pos++;
  if (!sign)
    buf[--pos] = '-';
  return buf + pos;
}

BString TianmuNum::ToBString() const {
  if (!IsNull()) {
    static int const SIZE(24);
    char buf[SIZE];
    if (core::ATI::IsRealType(attr_type_)) {
      gcvt(*(double *)&value_, 15, buf);
      size_t s = std::strlen(buf);
      if (s && buf[s - 1] == '.')
        buf[s - 1] = 0;
    } else if (core::ATI::IsIntegerType(attr_type_))
      std::sprintf(buf, "%ld", value_);
    else {
      return BString(Text(value_, buf, scale_), 0, true);  // here include num & bit
    }
    return BString(buf, std::strlen(buf), true);
  }
  return BString();
}

TianmuNum::operator double() const {
  return (core::ATI::IsRealType(Type()) || Type() == common::ColumnType::FLOAT) ? *(double *)&value_
                                                                                : GetIntPart() + GetFractPart();
}

bool TianmuNum::operator==(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return (compare((TianmuNum &)tianmu_dt) == 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return (compare((TianmuDateTime &)tianmu_dt) == 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return (tianmu_dt == this->ToBString());
  TIANMU_ERROR("Bad cast inside TianmuNum");
  return false;
}

bool TianmuNum::operator!=(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return (compare((TianmuNum &)tianmu_dt) != 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return (compare((TianmuDateTime &)tianmu_dt) != 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return (tianmu_dt != this->ToBString());
  TIANMU_ERROR("Bad cast inside TianmuNum");
  return false;
}

bool TianmuNum::operator<(const TianmuDataType &tianmu_dt) const {
  if (IsNull() || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return (compare((TianmuNum &)tianmu_dt) < 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return (compare((TianmuDateTime &)tianmu_dt) < 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return (this->ToBString() < tianmu_dt);
  TIANMU_ERROR("Bad cast inside TianmuNum");
  return false;
}

bool TianmuNum::operator>(const TianmuDataType &tianmu_dt) const {
  if (IsNull() || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return (compare((TianmuNum &)tianmu_dt) > 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return (compare((TianmuDateTime &)tianmu_dt) > 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return (this->ToBString() > tianmu_dt);
  TIANMU_ERROR("Bad cast inside TianmuNum");
  return false;
}

bool TianmuNum::operator<=(const TianmuDataType &tianmu_dt) const {
  if (IsNull() || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return (compare((TianmuNum &)tianmu_dt) <= 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return (compare((TianmuDateTime &)tianmu_dt) <= 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return (this->ToBString() <= tianmu_dt);
  TIANMU_ERROR("Bad cast inside TianmuNum");
  return false;
}

bool TianmuNum::operator>=(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return (compare((TianmuNum &)tianmu_dt) >= 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return (compare((TianmuDateTime &)tianmu_dt) >= 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return (this->ToBString() >= tianmu_dt);
  TIANMU_ERROR("Bad cast inside TianmuNum");
  return false;
}

TianmuNum &TianmuNum::operator-=(const TianmuNum &tianmu_n) {
  DEBUG_ASSERT(!null_);
  if (tianmu_n.IsNull() || tianmu_n.IsNull())
    return *this;
  if (IsReal() || tianmu_n.IsReal()) {
    if (IsReal() && tianmu_n.IsReal())
      *(double *)&value_ -= *(double *)&tianmu_n.value_;
    else {
      if (IsReal())
        *this -= tianmu_n.ToReal();
      else
        *this -= tianmu_n.ToDecimal();
    }
  } else {
    if (scale_ < tianmu_n.scale_) {
      value_ = ((int64_t)(value_ * PowOfTen(tianmu_n.scale_ - scale_)) - tianmu_n.value_);
      scale_ = tianmu_n.scale_;
    } else {
      value_ -= (int64_t)(tianmu_n.value_ * PowOfTen(scale_ - tianmu_n.scale_));
    }
  }
  return *this;
}

TianmuNum &TianmuNum::operator+=(const TianmuNum &tianmu_n) {
  DEBUG_ASSERT(!null_);
  if (tianmu_n.IsNull() || tianmu_n.IsNull())
    return *this;
  if (IsReal() || tianmu_n.IsReal()) {
    if (IsReal() && tianmu_n.IsReal())
      *(double *)&value_ -= *(double *)&tianmu_n.value_;
    else {
      if (IsReal())
        *this += tianmu_n.ToReal();
      else
        *this += tianmu_n.ToDecimal();
    }
  } else {
    if (scale_ < tianmu_n.scale_) {
      value_ = ((int64_t)(value_ * PowOfTen(tianmu_n.scale_ - scale_)) + tianmu_n.value_);
      scale_ = tianmu_n.scale_;
    } else {
      value_ += (int64_t)(tianmu_n.value_ * PowOfTen(scale_ - tianmu_n.scale_));
    }
  }
  return *this;
}

TianmuNum &TianmuNum::operator*=(const TianmuNum &tianmu_n) {
  DEBUG_ASSERT(!null_);
  if (tianmu_n.IsNull() || tianmu_n.IsNull())
    return *this;
  if (IsReal() || tianmu_n.IsReal()) {
    if (IsReal() && tianmu_n.IsReal())
      *(double *)&value_ -= *(double *)&tianmu_n.value_;
    else {
      if (IsReal())
        *this /= tianmu_n.ToReal();
      else
        *this /= tianmu_n.ToDecimal();
    }
  } else {
    value_ *= tianmu_n.value_;
    scale_ += tianmu_n.scale_;
  }
  return *this;
}

void fcvt(char *buf, double val, int digits, int *dec, int *sign) {
  static int const fmtlen = 10;
  char format[fmtlen + 1];
  std::snprintf(format, fmtlen + 1, "%%.%df", digits);
  std::snprintf(buf, digits, format, val);
  int len(std::strlen(buf));
  (*sign) = (buf[0] == '-') ? 1 : 0;
  (*sign) && std::memmove(buf, buf + 1, len);
  char *pbuf(buf);
  ::strsep(&pbuf, ".,");
  if (pbuf) {
    (*dec) = pbuf - buf - 1;
    std::memmove(pbuf - 1, pbuf, std::strlen(pbuf) + 1);
  }
  return;
}

TianmuNum &TianmuNum::operator/=(const TianmuNum &tianmu_n) {
  DEBUG_ASSERT(!null_);
  if (tianmu_n.IsNull() || tianmu_n.IsNull())
    return *this;
  if (IsReal() || tianmu_n.IsReal()) {
    if (IsReal() && tianmu_n.IsReal())
      *(double *)&value_ -= *(double *)&tianmu_n.value_;
    else {
      if (IsReal())
        *this /= tianmu_n.ToReal();
      else
        *this /= tianmu_n.ToDecimal();
    }
  } else {
    double tmv = ((double)(value_ / tianmu_n.value_) / PowOfTen(scale_ - tianmu_n.scale_));
    int decimal = 0;
    int sign;
    static int const MAX_NUM_DIGITS = 21 + 1;
    char buf[MAX_NUM_DIGITS - 1];
    fcvt(buf, tmv, 18, &decimal, &sign);
    buf[18] = 0;
    ptrdiff_t lz = std::strlen(buf) - 1;
    while (lz >= 0 && buf[lz--] == '0')
      ;
    buf[lz + 2] = 0;
    value_ = std::strtoll(buf, nullptr, 10) * (sign == 1 ? -1 : 1);
    scale_ = (short)((lz + 2) - decimal);
  }
  return *this;
}

TianmuNum TianmuNum::operator-(const TianmuNum &tianmu_n) const {
  TianmuNum res(*this);
  return res -= tianmu_n;
}

TianmuNum TianmuNum::operator+(const TianmuNum &tianmu_n) const {
  TianmuNum res(*this);
  return res += tianmu_n;
}

TianmuNum TianmuNum::operator*(const TianmuNum &tianmu_n) const {
  TianmuNum res(*this);
  return res *= tianmu_n;
}

TianmuNum TianmuNum::operator/(const TianmuNum &tianmu_n) const {
  TianmuNum res(*this);
  return res /= tianmu_n;
}

uint TianmuNum::GetHashCode() const { return uint(GetIntPart() * 1040021); }

int TianmuNum::compare(const TianmuNum &tianmu_n) const {
  if (IsNull() || tianmu_n.IsNull())
    return false;
  if (IsReal() || tianmu_n.IsReal()) {
    if (IsReal() && tianmu_n.IsReal())
      return (*(double *)&value_ > *(double *)&tianmu_n.value_
                  ? 1
                  : (*(double *)&value_ == *(double *)&tianmu_n.value_ ? 0 : -1));
    else {
      if (IsReal())
        return (*this > tianmu_n.ToReal() ? 1 : (*this == tianmu_n.ToReal() ? 0 : -1));
      else
        return (this->ToReal() > tianmu_n ? 1 : (this->ToReal() == tianmu_n ? 0 : -1));
    }
  } else {
    if (scale_ != tianmu_n.scale_) {
      if (value_ < 0 && tianmu_n.value_ >= 0)
        return -1;
      if (value_ >= 0 && tianmu_n.value_ < 0)
        return 1;
      if (scale_ < tianmu_n.scale_) {
        int64_t power_of_ten = (int64_t)PowOfTen(tianmu_n.scale_ - scale_);
        int64_t tmpv = (int64_t)(tianmu_n.value_ / power_of_ten);
        if (value_ > tmpv)
          return 1;
        if (value_ < tmpv || tianmu_n.value_ % power_of_ten > 0)
          return -1;
        if (tianmu_n.value_ % power_of_ten < 0)
          return 1;
        return 0;
      } else {
        int64_t power_of_ten = (int64_t)PowOfTen(scale_ - tianmu_n.scale_);
        int64_t tmpv = (int64_t)(value_ / power_of_ten);
        if (tmpv < tianmu_n.value_)
          return -1;
        if (tmpv > tianmu_n.value_ || value_ % power_of_ten > 0)
          return 1;
        if (value_ % power_of_ten < 0)
          return -1;
        return 0;
      }
    } else
      return (value_ > tianmu_n.value_ ? 1 : (value_ == tianmu_n.value_ ? 0 : -1));
  }
}

int TianmuNum::compare(const TianmuDateTime &tianmu_dt) const {
  int64_t tmp;
  tianmu_dt.ToInt64(tmp);
  return int(GetIntPart() - tmp);
}

double TianmuNum::GetIntPartAsDouble() const {
  if (is_double_) {
    double integer;
    modf(*(double *)&value_, &integer);
    return integer;
  } else {
    return (double)(value_ / (int64_t)Uint64PowOfTen(scale_));
  }
}

double TianmuNum::GetFractPart() const {
  if (is_double_) {
    double fract, integer;
    fract = modf(*(double *)&value_, &integer);
    return fract;
  } else {
    double tmpv = ((double)value_ / Uint64PowOfTen(scale_));
    double fract, integer;
    fract = modf(tmpv, &integer);
    return fract;
  }
}

short TianmuNum::GetDecStrLen() const {
  if (IsNull())
    return 0;
  if (is_double_)
    return 18;
  short res = scale_;
  int64_t tmpi = value_ / (int64_t)PowOfTen(scale_);
  while (tmpi != 0) {
    tmpi /= 10;
    res++;
  }
  return res;
}

short TianmuNum::GetDecIntLen() const {
  if (IsNull())
    return 0;
  short res = 0;
  int64_t tmpi = 0;
  if (is_double_)
    tmpi = GetIntPart();
  else {
    tmpi = value_ / (int64_t)PowOfTen(scale_);
  }
  while (tmpi != 0) {
    tmpi /= 10;
    res++;
  }
  return res;
}

short TianmuNum::GetDecFractLen() const {
  if (IsNull())
    return 0;
  return scale_;
}

void TianmuNum::Negate() {
  if (IsNull())
    return;
  if (is_double_)
    *(double *)&value_ = *(double *)&value_ * -1;
  else
    value_ *= -1;
}

}  // namespace types
}  // namespace Tianmu
