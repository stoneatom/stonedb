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

#include "types/rc_data_types.h"

#include "core/engine.h"
#include "core/tools.h"
#include "core/transaction.h"
#include "system/rc_system.h"
#include "types/value_parser4txt.h"

namespace Tianmu {
namespace types {

static_assert(sizeof(DT) == 8);

RCDateTime::RCDateTime(int64_t v, common::ColumnType at) : at_(at) {
  null_ = (v == common::NULL_VALUE_64);
  if (!null_) {
    *(int64_t *)&dt_ = v;
    if (at == common::ColumnType::DATE) {
      dt_.second = 0;
      dt_.minute = 0;
      dt_.hour = 0;
    } else if (at == common::ColumnType::TIME) {
      dt_.day = 0;
      dt_.month = 0;
      dt_.year = 0;
    }
  }
}

RCDateTime::RCDateTime(short year) {
  at_ = common::ColumnType::YEAR;
  null_ = false;
  if (year == common::NULL_VALUE_SH)
    null_ = true;
  else {
    dt_.year = std::abs(year);
  }
}

RCDateTime::RCDateTime(short yh, short mm, short ds, common::ColumnType at) : at_(at) {
  null_ = false;
  if (at == common::ColumnType::DATE) {
    dt_.day = std::abs(ds);
    dt_.month = std::abs(mm);
    dt_.year = std::abs(yh);
  } else if (at == common::ColumnType::TIME) {
    dt_.second = std::abs(ds);
    dt_.minute = std::abs(mm);
    dt_.time_hour = std::abs(yh);
    if (yh < 0 || mm < 0 || ds < 0)
      dt_.neg = 1;
  } else
    TIANMU_ERROR("type not supported");
}

RCDateTime::RCDateTime(short year, short month, short day, short hour, short minute, short second,
                       common::ColumnType at)
    : at_(at) {
  ASSERT(at == common::ColumnType::DATETIME || at == common::ColumnType::TIMESTAMP,
         "should be 'at == common::ColumnType::DATETIME || at == common::ColumnType::TIMESTAMP'");
  null_ = false;
  dt_.year = std::abs(year);
  dt_.month = std::abs(month);
  dt_.day = std::abs(day);
  dt_.hour = std::abs(hour);
  dt_.minute = std::abs(minute);
  dt_.second = std::abs(second);
}

RCDateTime::RCDateTime(const MYSQL_TIME &myt, common::ColumnType at) {
  ASSERT(at == common::ColumnType::DATETIME || at == common::ColumnType::TIMESTAMP,
         "should be 'at == common::ColumnType::DATETIME || at == common::ColumnType::TIMESTAMP'");
  null_ = false;

  dt_.year = myt.year;
  dt_.month = myt.month;
  dt_.day = myt.day;
  dt_.hour = myt.hour;
  dt_.minute = myt.minute;
  dt_.second = myt.second;
  dt_.microsecond = myt.second_part;
  dt_.neg = myt.neg;
}

RCDateTime::RCDateTime(RCNum &rcn, common::ColumnType at) : at_(at) {
  null_ = rcn.null_;
  if (!null_) {
    if (core::ATI::IsRealType(rcn.Type()))
      throw common::DataTypeConversionException(common::TianmuError(common::ErrorCode::DATACONVERSION));
    if (rcn.Type() == common::ColumnType::NUM && rcn.Scale() > 0)
      throw common::DataTypeConversionException(common::TianmuError(common::ErrorCode::DATACONVERSION));
    if (Parse((int64_t)rcn, *this, at) != common::ErrorCode::SUCCESS)
      throw common::DataTypeConversionException(common::TianmuError(common::ErrorCode::DATACONVERSION));
  }
}

RCDateTime::RCDateTime(const RCDateTime &rcdt) : ValueBasic<RCDateTime>(rcdt) { *this = rcdt; }

RCDateTime::~RCDateTime() {}

RCDateTime &RCDateTime::operator=(const RCDateTime &rcv) {
  *(int64_t *)&dt_ = *reinterpret_cast<int64_t *>(const_cast<DT *>(&rcv.dt_));
  this->at_ = rcv.at_;
  this->null_ = rcv.null_;
  return *this;
}

RCDateTime &RCDateTime::operator=(const RCDataType &rcv) {
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    *this = dynamic_cast<RCDateTime &>(const_cast<RCDataType &>(rcv));
  else {
    TIANMU_ERROR("bad cast");
    null_ = true;
  }
  return *this;
}

RCDateTime &RCDateTime::Assign(int64_t v, common::ColumnType at) {
  this->at_ = at;
  null_ = (v == common::NULL_VALUE_64);
  if (null_)
    *(int64_t *)&dt_ = 0;
  else
    *(int64_t *)&dt_ = v;
  return *this;
}

int64_t RCDateTime::GetInt64() const {
  if (null_)
    return common::NULL_VALUE_64;
  return *reinterpret_cast<int64_t *>(const_cast<DT *>(&dt_));
}

bool RCDateTime::ToInt64(int64_t &value) const {
  if (!IsNull()) {
    if (at_ == common::ColumnType::YEAR) {
      value = (int)dt_.year;
      return true;
    } else if (at_ == common::ColumnType::DATE) {
      value = Year() * 10000 + Month() * 100 + Day();
      return true;
    } else if (at_ == common::ColumnType::TIME) {
      value = dt_.time_hour * 10000 + Minute() * 100 + Second();
      if (dt_.Neg())
        value = -value;
      return true;
    } else {
      value = Year() * 10000 + Month() * 100 + Day();
      value *= 1000000;
      value += Hour() * 10000 + Minute() * 100 + Second();
      return true;
    }
  } else
    value = common::NULL_VALUE_64;
  return false;
}

bool RCDateTime::IsZero() const { return *this == GetSpecialValue(Type()); }

BString RCDateTime::ToBString() const {
  if (!IsNull()) {
    BString rcs(0, 30, true);
    char *buf = rcs.val_;
    if (dt_.Neg())
      *buf++ = '-';
    if (at_ == common::ColumnType::YEAR) {
      std::sprintf(buf, "%04d", (int)std::abs(Year()));
    } else if (at_ == common::ColumnType::DATE) {
      std::sprintf(buf, "%04d-%02d-%02d", (int)std::abs(Year()), (int)std::abs(Month()), (int)std::abs(Day()));
    } else if (at_ == common::ColumnType::TIME) {
      std::sprintf(buf, "%02d:%02d:%02d", (int)dt_.time_hour, (int)Minute(), (int)Second());
    } else if (at_ == common::ColumnType::DATETIME || at_ == common::ColumnType::TIMESTAMP) {
      std::sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d.%06d", (int)std::abs(Year()), (int)std::abs(Month()),
                   (int)std::abs(Day()), (int)Hour(), (int)Minute(), (int)Second(), (int)MicroSecond());
    } else
      TIANMU_ERROR("type not supported");
    rcs.len_ = (uint)std::strlen(rcs.val_);
    return rcs;
  }
  return BString();
}

common::ErrorCode RCDateTime::Parse(const BString &rcs, RCDateTime &rcv, common::ColumnType at) {
  return ValueParserForText::ParseDateTime(rcs, rcv, at);
}

common::ErrorCode RCDateTime::Parse(const int64_t &v, RCDateTime &rcv, common::ColumnType at, int precision) {
  int64_t tmp_v = v < 0 ? -v : v;
  int sign = 1;
  if (v < 0)
    sign = -1;

  rcv.at_ = at;
  if (v == common::NULL_VALUE_64) {
    rcv.null_ = true;
    return common::ErrorCode::SUCCESS;
  } else
    rcv.null_ = false;

  if (at == common::ColumnType::YEAR) {
    uint vv = (uint)v;
    vv = ToCorrectYear(vv, at, (precision >= 0 && precision < 4));
    if (IsCorrectTIANMUYear((short)vv)) {
      rcv.dt_.year = (short)vv;
      return common::ErrorCode::SUCCESS;
    }
  } else if (at == common::ColumnType::DATE) {
    if (!CanBeDay(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt_.day = tmp_v % 100;
    tmp_v /= 100;
    if (!CanBeMonth(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt_.month = tmp_v % 100;
    tmp_v /= 100;
    uint vv = uint(tmp_v);
    vv = ToCorrectYear(vv, at);
    if (!CanBeYear(vv)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt_.year = vv;
    if (sign == 1 && IsCorrectTIANMUDate(short(rcv.dt_.year), short(rcv.dt_.month), short(rcv.dt_.day)))
      return common::ErrorCode::SUCCESS;
  } else if (at == common::ColumnType::TIME) {
    if (!CanBeSecond(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt_.second = tmp_v % 100;
    tmp_v /= 100;
    if (!CanBeMinute(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt_.minute = tmp_v % 100;
    tmp_v /= 100;

    if ((tmp_v * sign) > RC_TIME_MAX.Hour()) {
      rcv = RC_TIME_MAX;
      return common::ErrorCode::OUT_OF_RANGE;
    } else if ((tmp_v * sign) < -RC_TIME_MIN.Hour()) {
      rcv = RC_TIME_MIN;
      return common::ErrorCode::OUT_OF_RANGE;
    }

    rcv.dt_.hour = tmp_v;

    if (IsCorrectTIANMUTime(short(rcv.dt_.hour * sign), short(rcv.dt_.minute * sign), short(rcv.dt_.second * sign))) {
      if (sign == -1)
        rcv.dt_.neg = 1;
      return common::ErrorCode::SUCCESS;
    } else {
      rcv = RC_TIME_SPEC;
      return common::ErrorCode::VALUE_TRUNCATED;
    }
  } else if (at == common::ColumnType::DATETIME || at == common::ColumnType::TIMESTAMP) {
    if (v > 245959) {
      if (!CanBeSecond(tmp_v % 100)) {
        rcv = GetSpecialValue(at);
        return common::ErrorCode::OUT_OF_RANGE;
      }
      rcv.dt_.second = tmp_v % 100;
      tmp_v /= 100;
      if (!CanBeMinute(tmp_v % 100)) {
        rcv = GetSpecialValue(at);
        return common::ErrorCode::OUT_OF_RANGE;
      }
      rcv.dt_.minute = tmp_v % 100;
      tmp_v /= 100;
      if (!CanBeHour(tmp_v % 100)) {
        rcv = GetSpecialValue(at);
        return common::ErrorCode::OUT_OF_RANGE;
      }
      rcv.dt_.hour = tmp_v % 100;
      tmp_v /= 100;
    }
    if (!CanBeDay(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt_.day = tmp_v % 100;
    tmp_v /= 100;
    if (!CanBeMonth(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt_.month = tmp_v % 100;
    tmp_v /= 100;
    if (!CanBeYear(tmp_v)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt_.year = RCDateTime::ToCorrectYear((uint)tmp_v, at);
    if (sign == 1 && at == common::ColumnType::DATETIME &&
        IsCorrectTIANMUDatetime(rcv.dt_.year, rcv.dt_.month, rcv.dt_.day, rcv.dt_.hour, rcv.dt_.minute, rcv.dt_.second))
      return common::ErrorCode::SUCCESS;
    if (sign == 1 && at == common::ColumnType::TIMESTAMP &&
        IsCorrectTIANMUTimestamp(short(rcv.dt_.year), short(rcv.dt_.month), short(rcv.dt_.day), short(rcv.dt_.hour),
                                 short(rcv.dt_.minute), short(rcv.dt_.second)))
      return common::ErrorCode::SUCCESS;
  } else
    TIANMU_ERROR("type not supported");

  rcv = GetSpecialValue(at);
  return common::ErrorCode::OUT_OF_RANGE;
}

bool RCDateTime::CanBeYear(int64_t year) {
  if (year >= 0 && year <= 9999)
    return true;
  return false;
}

bool RCDateTime::CanBeMonth(int64_t month) {
  if (month >= 1 && month <= 12)
    return true;
  return false;
}

bool RCDateTime::CanBeDay(int64_t day) {
  if (day >= 1 && day <= 31)
    return true;
  return false;
}

bool RCDateTime::CanBeHour(int64_t hour) {
  if (hour >= 0 && hour <= 23)
    return true;
  return false;
}

bool RCDateTime::CanBeMinute(int64_t minute) {
  if (minute >= 0 && minute <= 59)
    return true;
  return false;
}

bool RCDateTime::CanBeSecond(int64_t second) { return RCDateTime::CanBeMinute(second); }

bool RCDateTime::CanBeDate(int64_t year, int64_t month, int64_t day) {
  if (year == RC_DATE_SPEC.Year() && month == RC_DATE_SPEC.Month() && day == RC_DATE_SPEC.Day())
    return true;
  if (CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth((ushort)year, (ushort)month))))
    return true;
  return false;
}

bool RCDateTime::CanBeTime(int64_t hour, int64_t minute, int64_t second) {
  if (hour == RC_TIME_SPEC.Hour() && minute == RC_TIME_SPEC.Minute() && second == RC_TIME_SPEC.Second())
    return true;
  if (hour >= -838 && hour <= 838 && CanBeMinute(minute) && CanBeSecond(second))
    return true;
  return false;
}

bool RCDateTime::CanBeTimestamp(int64_t year, int64_t month, int64_t day, int64_t hour, int64_t minute,
                                int64_t second) {
  if (year == RC_TIMESTAMP_SPEC.Year() && month == RC_TIMESTAMP_SPEC.Month() && day == RC_TIMESTAMP_SPEC.Day() &&
      hour == RC_TIMESTAMP_SPEC.Hour() && minute == RC_TIMESTAMP_SPEC.Minute() && second == RC_TIMESTAMP_SPEC.Second())
    return true;
  if (CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth((ushort)year, (ushort)month))) &&
      CanBeHour(hour) && CanBeMinute(minute) && CanBeSecond(second))
    return true;
  return false;
}

bool RCDateTime::CanBeDatetime(int64_t year, int64_t month, int64_t day, int64_t hour, int64_t minute, int64_t second) {
  if (year == RC_DATETIME_SPEC.Year() && month == RC_DATETIME_SPEC.Month() && day == RC_DATETIME_SPEC.Day() &&
      hour == RC_DATETIME_SPEC.Hour() && minute == RC_DATETIME_SPEC.Minute() && second == RC_DATETIME_SPEC.Second())
    return true;
  if (CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth((ushort)year, (ushort)month))) &&
      CanBeHour(hour) && CanBeMinute(minute) && CanBeSecond(second))
    return true;
  return false;
}

bool RCDateTime::IsCorrectTIANMUYear(short year) {
  return year == RC_YEAR_SPEC.Year() || (CanBeYear(year) && (year >= RC_YEAR_MIN.Year() && year <= RC_YEAR_MAX.Year()));
}

bool RCDateTime::IsCorrectTIANMUDate(short year, short month, short day) {
  if (year == RC_DATE_SPEC.Year() && month == RC_DATE_SPEC.Month() && day == RC_DATE_SPEC.Day())
    return true;
  if (CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth(year, month)))) {
    if (((year >= RC_DATE_MIN.Year() && month >= RC_DATE_MIN.Month() && day >= RC_DATE_MIN.Day()) &&
         (year <= RC_DATE_MAX.Year() && month <= RC_DATE_MAX.Month() && day <= RC_DATE_MAX.Day())))
      return true;
  }
  return false;
}

bool RCDateTime::IsCorrectTIANMUTime(short hour, short minute, short second) {
  if (hour == RC_TIME_SPEC.Hour() && minute == RC_TIME_SPEC.Minute() && second == RC_TIME_SPEC.Second())
    return true;
  bool haspositive = false;
  bool hasnegative = false;
  if (hour < 0 || minute < 0 || second < 0)
    hasnegative = true;
  if (hour > 0 || minute > 0 || second > 0)
    haspositive = true;

  if (hasnegative == haspositive && (hour != 0 || minute != 0 || second != 0))
    return false;

  if (hour >= -RC_TIME_MIN.Hour() && hour <= RC_TIME_MAX.Hour() && (CanBeMinute(minute) || CanBeMinute(-minute)) &&
      (CanBeSecond(second) || CanBeSecond(-second)))
    return true;
  return false;
}

bool RCDateTime::IsCorrectTIANMUTimestamp(short year, short month, short day, short hour, short minute, short second) {
  if (year == RC_TIMESTAMP_SPEC.Year() && month == RC_TIMESTAMP_SPEC.Month() && day == RC_TIMESTAMP_SPEC.Day() &&
      hour == RC_TIMESTAMP_SPEC.Hour() && minute == RC_TIMESTAMP_SPEC.Minute() && second == RC_TIMESTAMP_SPEC.Second())
    return true;
  if (CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth(year, month))) && CanBeHour(hour) &&
      CanBeMinute(minute) && CanBeSecond(second)) {
    RCDateTime rcdt(year, month, day, hour, minute, second, common::ColumnType::TIMESTAMP);
    if (rcdt >= RC_TIMESTAMP_MIN && rcdt <= RC_TIMESTAMP_MAX)
      return true;
  }
  return false;
}

bool RCDateTime::IsCorrectTIANMUDatetime(short year, short month, short day, short hour, short minute, short second) {
  if (year == RC_DATETIME_SPEC.Year() && month == RC_DATETIME_SPEC.Month() && day == RC_DATETIME_SPEC.Day() &&
      hour == RC_DATETIME_SPEC.Hour() && minute == RC_DATETIME_SPEC.Minute() && second == RC_DATETIME_SPEC.Second())
    return true;
  if (CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth(year, month))) && CanBeHour(hour) &&
      CanBeMinute(minute) && CanBeSecond(second)) {
    if ((year >= RC_DATETIME_MIN.Year() && month >= RC_DATETIME_MIN.Month() && day >= RC_DATETIME_MIN.Day() &&
         hour >= RC_DATETIME_MIN.Hour() && minute >= RC_DATETIME_MIN.Minute() && second >= RC_DATETIME_MIN.Second()) &&
        (year <= RC_DATETIME_MAX.Year() && month <= RC_DATETIME_MAX.Month() && day <= RC_DATETIME_MAX.Day() &&
         hour <= RC_DATETIME_MAX.Hour() && minute <= RC_DATETIME_MAX.Minute() && second <= RC_DATETIME_MAX.Second()))
      return true;
  }
  return false;
}

bool RCDateTime::IsLeapYear(short year) {
  if (year == 0)
    return false;
  if (!CanBeYear(year))
    throw common::DataTypeConversionException(common::ErrorCode::DATACONVERSION);
  return ((year & 3) == 0 && year % 100) || ((year & 3) == 0 && year % 400 == 0);
}

ushort RCDateTime::NoDaysInMonth(short year, ushort month) {
  static const ushort no_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  if (!CanBeYear(year) || !CanBeMonth(month))
    throw common::DataTypeConversionException(common::ErrorCode::DATACONVERSION);
  if (month == 2 && IsLeapYear(year))
    return 29;
  return no_days[month - 1];
}

short RCDateTime::ToCorrectYear(uint v, common::ColumnType at, bool is_year_2 /*= false*/) {
  switch (at) {
    case common::ColumnType::YEAR:
      if (v == 0 && is_year_2)  // 0 for year(2) corresponds to 2000
        return 2000;
      if (v <= 0 || v > 2155)
        return v;
      else if (v < 100) {
        if (v <= 69)
          return v + 2000;
        return v + 1900;
      }
      return (short)v;
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
      if (v < 100) {
        if (v <= 69)
          return v + 2000;
        return v + 1900;
      }
      return (short)v;
    default:
      TIANMU_ERROR("type not supported");
  }
  return 0;  // to avoid errors in release version
}

RCDateTime RCDateTime::GetSpecialValue(common::ColumnType at) {
  switch (at) {
    case common::ColumnType::YEAR:
      return RC_YEAR_SPEC;
    case common::ColumnType::TIME:
      return RC_TIME_SPEC;
    case common::ColumnType::DATE:
      return RC_DATE_SPEC;
    case common::ColumnType::DATETIME:
      return RC_DATETIME_SPEC;
    case common::ColumnType::TIMESTAMP:
      return RC_TIMESTAMP_SPEC;
    default:
      TIANMU_ERROR("type not supported");
  }
  return RC_DATETIME_SPEC;  // to avoid errors in release version
}

RCDateTime RCDateTime::GetCurrent() {
  time_t const curr = time(0);
  tm const tmt = *localtime(&curr);
  RCDateTime rcdt(tmt.tm_year + 1900, tmt.tm_mon + 1, tmt.tm_mday, tmt.tm_hour, tmt.tm_min, tmt.tm_sec,
                  common::ColumnType::DATETIME);
  return rcdt;
}

bool RCDateTime::operator==(const RCDataType &rcv) const {
  if (!AreComparable(at_, rcv.Type()) || IsNull() || rcv.IsNull())
    return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare(dynamic_cast<RCDateTime &>(const_cast<RCDataType &>(rcv))) == 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) {
    return compare(dynamic_cast<RCNum &>(const_cast<RCDataType &>(rcv))) == 0;
  }
  return false;
}

bool RCDateTime::operator<(const RCDataType &rcv) const {
  if (!AreComparable(at_, rcv.Type()) || IsNull() || rcv.IsNull())
    return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare(dynamic_cast<RCDateTime &>(const_cast<RCDataType &>(rcv))) < 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return compare(dynamic_cast<RCNum &>(const_cast<RCDataType &>(rcv))) < 0;
  return false;
}

bool RCDateTime::operator>(const RCDataType &rcv) const {
  if (!AreComparable(at_, rcv.Type()) || IsNull() || rcv.IsNull())
    return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare(dynamic_cast<RCDateTime &>(const_cast<RCDataType &>(rcv))) > 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return compare(dynamic_cast<RCNum &>(const_cast<RCDataType &>(rcv))) > 0;
  return false;
}

bool RCDateTime::operator>=(const RCDataType &rcv) const {
  if (!AreComparable(at_, rcv.Type()) || IsNull() || rcv.IsNull())
    return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare(dynamic_cast<RCDateTime &>(const_cast<RCDataType &>(rcv))) >= 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return compare(dynamic_cast<RCNum &>(const_cast<RCDataType &>(rcv))) >= 0;
  return false;
}

bool RCDateTime::operator<=(const RCDataType &rcv) const {
  if (!AreComparable(at_, rcv.Type()) || IsNull() || rcv.IsNull())
    return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare(dynamic_cast<RCDateTime &>(const_cast<RCDataType &>(rcv))) <= 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return compare(dynamic_cast<RCNum &>(const_cast<RCDataType &>(rcv))) <= 0;
  return false;
}

bool RCDateTime::operator!=(const RCDataType &rcv) const {
  if (!AreComparable(at_, rcv.Type()) || IsNull() || rcv.IsNull())
    return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare(dynamic_cast<RCDateTime &>(const_cast<RCDataType &>(rcv))) != 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return compare(dynamic_cast<RCNum &>(const_cast<RCDataType &>(rcv))) != 0;
  return false;
}

int64_t RCDateTime::operator-(const RCDateTime &sec) const {
  if (at_ != common::ColumnType::DATE || sec.at_ != common::ColumnType::DATE || IsNull() || sec.IsNull())
    return common::NULL_VALUE_64;
  int64_t result = 0;  // span in days for [sec., ..., this]
  bool notless_than_sec = (this->operator>(sec));
  if (notless_than_sec) {
    if (dt_.year == sec.dt_.year) {
      if (dt_.month == sec.dt_.month) {
        result = dt_.day - sec.dt_.day;
      } else {
        for (unsigned int i = sec.dt_.month + 1; i < dt_.month; i++) result += NoDaysInMonth(dt_.year, i);
        result += NoDaysInMonth(sec.dt_.year, sec.dt_.month) - sec.dt_.day + 1;
        result += dt_.day - 1;
      }
    } else {
      for (int i = int(sec.dt_.year) + 1; i < dt_.year; i++) result += (IsLeapYear(i) ? 366 : 365);
      for (int i = int(sec.dt_.month) + 1; i <= 12; i++) result += NoDaysInMonth(sec.dt_.year, i);
      for (unsigned int i = 1; i < dt_.month; i++) result += NoDaysInMonth(dt_.year, i);
      result += NoDaysInMonth(sec.dt_.year, sec.dt_.month) - sec.dt_.day + 1;
      result += dt_.day - 1;
    }
  } else {
    if (dt_.year == sec.dt_.year) {
      if (dt_.month == sec.dt_.month) {
        result = sec.dt_.day - dt_.day;
      } else {
        for (int i = int(dt_.month) + 1; i < sec.dt_.month; i++) result += NoDaysInMonth(sec.dt_.year, i);
        result += NoDaysInMonth(dt_.year, dt_.month) - dt_.day + 1;
        result += sec.dt_.day - 1;
      }
    } else {
      for (unsigned int i = (dt_.year) + 1; i < sec.dt_.year; i++) result += (IsLeapYear(i) ? 366 : 365);
      for (int i = int(dt_.month) + 1; i <= 12; i++) result += NoDaysInMonth(dt_.year, i);
      for (unsigned int i = 1; i < sec.dt_.month; i++) result += NoDaysInMonth(sec.dt_.year, i);
      result += NoDaysInMonth(dt_.year, dt_.month) - dt_.day + 1;
      result += sec.dt_.day - 1;
    }
  }

  return notless_than_sec ? result : -result;
}

common::ColumnType RCDateTime::Type() const { return at_; }

uint RCDateTime::GetHashCode() const {
  uint64_t v = *reinterpret_cast<uint64_t *>(const_cast<DT *>(&dt_));
  return (uint)(v >> 32) + (uint)(v) /*+ *(short*)&tz*/;
}

int RCDateTime::compare(const RCDateTime &rcv) const {
  int64_t v1 = *reinterpret_cast<int64_t *>(const_cast<DT *>(&dt_));
  int64_t v2 = *reinterpret_cast<int64_t *>(const_cast<DT *>(&rcv.dt_));
  return (v1 < v2 ? -1 : (v1 > v2 ? 1 : 0));
}

int RCDateTime::compare(const RCNum &rcv) const {
  if (IsNull() || rcv.IsNull())
    return false;
  int64_t tmp;
  ToInt64(tmp);
  return int(tmp - (const_cast<RCNum &>(rcv)).GetIntPart());
}

void RCDateTime::AdjustTimezone(RCDateTime &dt) {
  // timezone conversion
  if (!dt.IsZero()) {
    auto thd = current_txn_->Thd();
    MYSQL_TIME t;
    t.year = dt.Year();
    t.month = dt.Month();
    t.day = dt.Day();
    t.hour = dt.Hour();
    t.minute = dt.Minute();
    t.second = dt.Second();
    t.second_part = dt.MicroSecond();
    t.time_type = MYSQL_TIMESTAMP_DATETIME;
    time_t secs = 0;
    short sign, minutes;
    core::Engine::ComputeTimeZoneDiffInMinutes(thd, sign, minutes);
    if (minutes == common::NULL_VALUE_SH) {
      // System time zone
      // time in system time zone is converted into UTC and expressed as seconds
      // since EPOCHE
      MYSQL_TIME time_tmp;
      thd->variables.time_zone->gmt_sec_to_TIME(
          &time_tmp, tianmu_sec_since_epoch(t.year, t.month, t.day, t.hour, t.minute, t.second));
      secs = tianmu_sec_since_epoch(time_tmp.year, time_tmp.month, time_tmp.day, time_tmp.hour, time_tmp.minute,
                                    time_tmp.second);

    } else {
      // time in client time zone is converted into UTC and expressed as seconds
      // since EPOCHE
      secs = tianmu_sec_since_epoch(t.year, t.month, t.day, t.hour, t.minute, t.second) - sign * minutes * 60;
    }
    // UTC seconds converted to UTC struct tm
    struct tm utc_t;
    gmtime_r(&secs, &utc_t);
    // UTC time stored on server
    dt = RCDateTime((utc_t.tm_year + 1900) % 10000, utc_t.tm_mon + 1, utc_t.tm_mday, utc_t.tm_hour, utc_t.tm_min,
                    utc_t.tm_sec, common::ColumnType::TIMESTAMP);
    dt.dt_.microsecond = t.second_part;
  }
}

}  // namespace types
}  // namespace Tianmu
