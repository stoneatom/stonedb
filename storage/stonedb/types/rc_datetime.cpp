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

namespace stonedb {
namespace types {
static_assert(sizeof(DT) == 8);

RCDateTime::RCDateTime(int64_t v, common::CT at) : at(at) {
  null = (v == common::NULL_VALUE_64);
  if (!null) {
    *(int64_t *)&dt = v;
    if (at == common::CT::DATE) {
      dt.second = 0;
      dt.minute = 0;
      dt.hour = 0;
    } else if (at == common::CT::TIME) {
      dt.day = 0;
      dt.month = 0;
      dt.year = 0;
    }
  }
}

RCDateTime::RCDateTime(short year) {
  at = common::CT::YEAR;
  null = false;
  if (year == common::NULL_VALUE_SH)
    null = true;
  else {
    dt.year = std::abs(year);
  }
}

RCDateTime::RCDateTime(short yh, short mm, short ds, common::CT at) : at(at) {
  null = false;
  if (at == common::CT::DATE) {
    dt.day = std::abs(ds);
    dt.month = std::abs(mm);
    dt.year = std::abs(yh);
  } else if (at == common::CT::TIME) {
    dt.second = std::abs(ds);
    dt.minute = std::abs(mm);
    dt.time_hour = std::abs(yh);
    if (yh < 0 || mm < 0 || ds < 0) dt.neg = 1;
  } else
    STONEDB_ERROR("type not supported");
}

RCDateTime::RCDateTime(short year, short month, short day, short hour, short minute, short second, common::CT at)
    : at(at) {
  ASSERT(at == common::CT::DATETIME || at == common::CT::TIMESTAMP,
         "should be 'at == common::CT::DATETIME || at == common::CT::TIMESTAMP'");
  null = false;
  dt.year = std::abs(year);
  dt.month = std::abs(month);
  dt.day = std::abs(day);
  dt.hour = std::abs(hour);
  dt.minute = std::abs(minute);
  dt.second = std::abs(second);
}

RCDateTime::RCDateTime(const MYSQL_TIME &myt, common::CT at) {
  ASSERT(at == common::CT::DATETIME || at == common::CT::TIMESTAMP,
         "should be 'at == common::CT::DATETIME || at == common::CT::TIMESTAMP'");
  null = false;

  dt.year = myt.year;
  dt.month = myt.month;
  dt.day = myt.day;
  dt.hour = myt.hour;
  dt.minute = myt.minute;
  dt.second = myt.second;
  dt.microsecond = myt.second_part;
  dt.neg = myt.neg;
}

RCDateTime::RCDateTime(RCNum &rcn, common::CT at) : at(at) {
  null = rcn.null;
  if (!null) {
    if (core::ATI::IsRealType(rcn.Type()))
      throw common::DataTypeConversionException(common::SDBError(common::ErrorCode::DATACONVERSION));
    if (rcn.Type() == common::CT::NUM && rcn.Scale() > 0)
      throw common::DataTypeConversionException(common::SDBError(common::ErrorCode::DATACONVERSION));
    if (Parse((int64_t)rcn, *this, at) != common::ErrorCode::SUCCESS)
      throw common::DataTypeConversionException(common::SDBError(common::ErrorCode::DATACONVERSION));
  }
}

RCDateTime::RCDateTime(const RCDateTime &rcdt) : ValueBasic<RCDateTime>(rcdt) { *this = rcdt; }

RCDateTime::~RCDateTime() {}

RCDateTime &RCDateTime::operator=(const RCDateTime &rcv) {
  *(int64_t *)&dt = *(int64_t *)&rcv.dt;
  this->at = rcv.at;
  this->null = rcv.null;
  return *this;
}

RCDateTime &RCDateTime::operator=(const RCDataType &rcv) {
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    *this = (RCDateTime &)rcv;
  else {
    STONEDB_ERROR("bad cast");
    null = true;
  }
  return *this;
}

RCDateTime &RCDateTime::Assign(int64_t v, common::CT at) {
  this->at = at;
  null = (v == common::NULL_VALUE_64);
  if (null)
    *(int64_t *)&dt = 0;
  else
    *(int64_t *)&dt = v;
  return *this;
}

int64_t RCDateTime::GetInt64() const {
  if (null) return common::NULL_VALUE_64;
  return *(int64_t *)&dt;
}

bool RCDateTime::ToInt64(int64_t &value) const {
  if (!IsNull()) {
    if (at == common::CT::YEAR) {
      value = (int)dt.year;
      return true;
    } else if (at == common::CT::DATE) {
      value = Year() * 10000 + Month() * 100 + Day();
      return true;
    } else if (at == common::CT::TIME) {
      value = dt.time_hour * 10000 + Minute() * 100 + Second();
      if (dt.Neg()) value = -value;
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
    char *buf = rcs.val;
    if (dt.Neg()) *buf++ = '-';
    if (at == common::CT::YEAR) {
      std::sprintf(buf, "%04d", (int)std::abs(Year()));
    } else if (at == common::CT::DATE) {
      std::sprintf(buf, "%04d-%02d-%02d", (int)std::abs(Year()), (int)std::abs(Month()), (int)std::abs(Day()));
    } else if (at == common::CT::TIME) {
      std::sprintf(buf, "%02d:%02d:%02d", (int)dt.time_hour, (int)Minute(), (int)Second());
    } else if (at == common::CT::DATETIME || at == common::CT::TIMESTAMP) {
      std::sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d.%06d", (int)std::abs(Year()), (int)std::abs(Month()),
                   (int)std::abs(Day()), (int)Hour(), (int)Minute(), (int)Second(), (int)MicroSecond());
    } else
      STONEDB_ERROR("type not supported");
    rcs.len = (uint)std::strlen(rcs.val);
    return rcs;
  }
  return BString();
}

common::ErrorCode RCDateTime::Parse(const BString &rcs, RCDateTime &rcv, common::CT at) {
  return ValueParserForText::ParseDateTime(rcs, rcv, at);
}

common::ErrorCode RCDateTime::Parse(const int64_t &v, RCDateTime &rcv, common::CT at, int precision) {
  int64_t tmp_v = v < 0 ? -v : v;
  int sign = 1;
  if (v < 0) sign = -1;

  rcv.at = at;
  if (v == common::NULL_VALUE_64) {
    rcv.null = true;
    return common::ErrorCode::SUCCESS;
  } else
    rcv.null = false;

  if (at == common::CT::YEAR) {
    uint vv = (uint)v;
    vv = ToCorrectYear(vv, at, (precision >= 0 && precision < 4));
    if (IsCorrectSDBYear((short)vv)) {
      rcv.dt.year = (short)vv;
      return common::ErrorCode::SUCCESS;
    }
  } else if (at == common::CT::DATE) {
    if (!CanBeDay(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt.day = tmp_v % 100;
    tmp_v /= 100;
    if (!CanBeMonth(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt.month = tmp_v % 100;
    tmp_v /= 100;
    uint vv = uint(tmp_v);
    vv = ToCorrectYear(vv, at);
    if (!CanBeYear(vv)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt.year = vv;
    if (sign == 1 && IsCorrectSDBDate(short(rcv.dt.year), short(rcv.dt.month), short(rcv.dt.day)))
      return common::ErrorCode::SUCCESS;
  } else if (at == common::CT::TIME) {
    if (!CanBeSecond(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt.second = tmp_v % 100;
    tmp_v /= 100;
    if (!CanBeMinute(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt.minute = tmp_v % 100;
    tmp_v /= 100;

    if ((tmp_v * sign) > RC_TIME_MAX.Hour()) {
      rcv = RC_TIME_MAX;
      return common::ErrorCode::OUT_OF_RANGE;
    } else if ((tmp_v * sign) < -RC_TIME_MIN.Hour()) {
      rcv = RC_TIME_MIN;
      return common::ErrorCode::OUT_OF_RANGE;
    }

    rcv.dt.hour = tmp_v;

    if (IsCorrectSDBTime(short(rcv.dt.hour * sign), short(rcv.dt.minute * sign), short(rcv.dt.second * sign))) {
      if (sign == -1) rcv.dt.neg = 1;
      return common::ErrorCode::SUCCESS;
    } else {
      rcv = RC_TIME_SPEC;
      return common::ErrorCode::VALUE_TRUNCATED;
    }
  } else if (at == common::CT::DATETIME || at == common::CT::TIMESTAMP) {
    if (v > 245959) {
      if (!CanBeSecond(tmp_v % 100)) {
        rcv = GetSpecialValue(at);
        return common::ErrorCode::OUT_OF_RANGE;
      }
      rcv.dt.second = tmp_v % 100;
      tmp_v /= 100;
      if (!CanBeMinute(tmp_v % 100)) {
        rcv = GetSpecialValue(at);
        return common::ErrorCode::OUT_OF_RANGE;
      }
      rcv.dt.minute = tmp_v % 100;
      tmp_v /= 100;
      if (!CanBeHour(tmp_v % 100)) {
        rcv = GetSpecialValue(at);
        return common::ErrorCode::OUT_OF_RANGE;
      }
      rcv.dt.hour = tmp_v % 100;
      tmp_v /= 100;
    }
    if (!CanBeDay(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt.day = tmp_v % 100;
    tmp_v /= 100;
    if (!CanBeMonth(tmp_v % 100)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt.month = tmp_v % 100;
    tmp_v /= 100;
    if (!CanBeYear(tmp_v)) {
      rcv = GetSpecialValue(at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
    rcv.dt.year = RCDateTime::ToCorrectYear((uint)tmp_v, at);
    if (sign == 1 && at == common::CT::DATETIME &&
        IsCorrectSDBDatetime(rcv.dt.year, rcv.dt.month, rcv.dt.day, rcv.dt.hour, rcv.dt.minute, rcv.dt.second))
      return common::ErrorCode::SUCCESS;
    if (sign == 1 && at == common::CT::TIMESTAMP &&
        IsCorrectSDBTimestamp(short(rcv.dt.year), short(rcv.dt.month), short(rcv.dt.day), short(rcv.dt.hour),
                              short(rcv.dt.minute), short(rcv.dt.second)))
      return common::ErrorCode::SUCCESS;
  } else
    STONEDB_ERROR("type not supported");

  rcv = GetSpecialValue(at);
  return common::ErrorCode::OUT_OF_RANGE;
}

bool RCDateTime::CanBeYear(int64_t year) {
  if (year >= 0 && year <= 9999) return true;
  return false;
}

bool RCDateTime::CanBeMonth(int64_t month) {
  if (month >= 1 && month <= 12) return true;
  return false;
}

bool RCDateTime::CanBeDay(int64_t day) {
  if (day >= 1 && day <= 31) return true;
  return false;
}

bool RCDateTime::CanBeHour(int64_t hour) {
  if (hour >= 0 && hour <= 23) return true;
  return false;
}

bool RCDateTime::CanBeMinute(int64_t minute) {
  if (minute >= 0 && minute <= 59) return true;
  return false;
}

bool RCDateTime::CanBeSecond(int64_t second) { return RCDateTime::CanBeMinute(second); }

bool RCDateTime::CanBeDate(int64_t year, int64_t month, int64_t day) {
  if (year == RC_DATE_SPEC.Year() && month == RC_DATE_SPEC.Month() && day == RC_DATE_SPEC.Day()) return true;
  if (CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth((ushort)year, (ushort)month))))
    return true;
  return false;
}

bool RCDateTime::CanBeTime(int64_t hour, int64_t minute, int64_t second) {
  if (hour == RC_TIME_SPEC.Hour() && minute == RC_TIME_SPEC.Minute() && second == RC_TIME_SPEC.Second()) return true;
  if (hour >= -838 && hour <= 838 && CanBeMinute(minute) && CanBeSecond(second)) return true;
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

bool RCDateTime::IsCorrectSDBYear(short year) {
  return year == RC_YEAR_SPEC.Year() || (CanBeYear(year) && (year >= RC_YEAR_MIN.Year() && year <= RC_YEAR_MAX.Year()));
}

bool RCDateTime::IsCorrectSDBDate(short year, short month, short day) {
  if (year == RC_DATE_SPEC.Year() && month == RC_DATE_SPEC.Month() && day == RC_DATE_SPEC.Day()) return true;
  if (CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth(year, month)))) {
    if (((year >= RC_DATE_MIN.Year() && month >= RC_DATE_MIN.Month() && day >= RC_DATE_MIN.Day()) &&
         (year <= RC_DATE_MAX.Year() && month <= RC_DATE_MAX.Month() && day <= RC_DATE_MAX.Day())))
      return true;
  }
  return false;
}

bool RCDateTime::IsCorrectSDBTime(short hour, short minute, short second) {
  if (hour == RC_TIME_SPEC.Hour() && minute == RC_TIME_SPEC.Minute() && second == RC_TIME_SPEC.Second()) return true;
  bool haspositive = false;
  bool hasnegative = false;
  if (hour < 0 || minute < 0 || second < 0) hasnegative = true;
  if (hour > 0 || minute > 0 || second > 0) haspositive = true;

  if (hasnegative == haspositive && (hour != 0 || minute != 0 || second != 0)) return false;

  if (hour >= -RC_TIME_MIN.Hour() && hour <= RC_TIME_MAX.Hour() && (CanBeMinute(minute) || CanBeMinute(-minute)) &&
      (CanBeSecond(second) || CanBeSecond(-second)))
    return true;
  return false;
}

bool RCDateTime::IsCorrectSDBTimestamp(short year, short month, short day, short hour, short minute, short second) {
  if (year == RC_TIMESTAMP_SPEC.Year() && month == RC_TIMESTAMP_SPEC.Month() && day == RC_TIMESTAMP_SPEC.Day() &&
      hour == RC_TIMESTAMP_SPEC.Hour() && minute == RC_TIMESTAMP_SPEC.Minute() && second == RC_TIMESTAMP_SPEC.Second())
    return true;
  if (CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth(year, month))) && CanBeHour(hour) &&
      CanBeMinute(minute) && CanBeSecond(second)) {
    RCDateTime rcdt(year, month, day, hour, minute, second, common::CT::TIMESTAMP);
    if (rcdt >= RC_TIMESTAMP_MIN && rcdt <= RC_TIMESTAMP_MAX) return true;
  }
  return false;
}

bool RCDateTime::IsCorrectSDBDatetime(short year, short month, short day, short hour, short minute, short second) {
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
  if (year == 0) return false;
  if (!CanBeYear(year)) throw common::DataTypeConversionException(common::ErrorCode::DATACONVERSION);
  return ((year & 3) == 0 && year % 100) || ((year & 3) == 0 && year % 400 == 0);
}

ushort RCDateTime::NoDaysInMonth(short year, ushort month) {
  static const ushort no_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  if (!CanBeYear(year) || !CanBeMonth(month))
    throw common::DataTypeConversionException(common::ErrorCode::DATACONVERSION);
  if (month == 2 && IsLeapYear(year)) return 29;
  return no_days[month - 1];
}

short RCDateTime::ToCorrectYear(uint v, common::CT at, bool is_year_2 /*= false*/) {
  switch (at) {
    case common::CT::YEAR:
      if (v == 0 && is_year_2)  // 0 for year(2) corresponds to 2000
        return 2000;
      if (v <= 0 || v > 2155)
        return v;
      else if (v < 100) {
        if (v <= 69) return v + 2000;
        return v + 1900;
      }
      return (short)v;
    case common::CT::DATE:
    case common::CT::DATETIME:
    case common::CT::TIMESTAMP:
      if (v < 100) {
        if (v <= 69) return v + 2000;
        return v + 1900;
      }
      return (short)v;
    default:
      STONEDB_ERROR("type not supported");
  }
  return 0;  // to avoid errors in release version
}

RCDateTime RCDateTime::GetSpecialValue(common::CT at) {
  switch (at) {
    case common::CT::YEAR:
      return RC_YEAR_SPEC;
    case common::CT::TIME:
      return RC_TIME_SPEC;
    case common::CT::DATE:
      return RC_DATE_SPEC;
    case common::CT::DATETIME:
      return RC_DATETIME_SPEC;
    case common::CT::TIMESTAMP:
      return RC_TIMESTAMP_SPEC;
    default:
      STONEDB_ERROR("type not supported");
  }
  return RC_DATETIME_SPEC;  // to avoid errors in release version
}

RCDateTime RCDateTime::GetCurrent() {
  time_t const curr = time(0);
  tm const tmt = *localtime(&curr);
  RCDateTime rcdt(tmt.tm_year + 1900, tmt.tm_mon + 1, tmt.tm_mday, tmt.tm_hour, tmt.tm_min, tmt.tm_sec,
                  common::CT::DATETIME);
  return rcdt;
}

bool RCDateTime::operator==(const RCDataType &rcv) const {
  if (!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull()) return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare((RCDateTime &)rcv) == 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) {
    return compare((RCNum &)rcv) == 0;
  }
  return false;
}

bool RCDateTime::operator<(const RCDataType &rcv) const {
  if (!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull()) return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare((RCDateTime &)rcv) < 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return compare((RCNum &)rcv) < 0;
  return false;
}

bool RCDateTime::operator>(const RCDataType &rcv) const {
  if (!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull()) return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare((RCDateTime &)rcv) > 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return compare((RCNum &)rcv) > 0;
  return false;
}

bool RCDateTime::operator>=(const RCDataType &rcv) const {
  if (!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull()) return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare((RCDateTime &)rcv) >= 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return compare((RCNum &)rcv) >= 0;
  return false;
}

bool RCDateTime::operator<=(const RCDataType &rcv) const {
  if (!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull()) return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare((RCDateTime &)rcv) <= 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return compare((RCNum &)rcv) <= 0;
  return false;
}

bool RCDateTime::operator!=(const RCDataType &rcv) const {
  if (!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull()) return false;
  if (rcv.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return compare((RCDateTime &)rcv) != 0;
  else if (rcv.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return compare((RCNum &)rcv) != 0;
  return false;
}

int64_t RCDateTime::operator-(const RCDateTime &sec) const {
  if (at != common::CT::DATE || sec.at != common::CT::DATE || IsNull() || sec.IsNull()) return common::NULL_VALUE_64;
  int64_t result = 0;  // span in days for [sec., ..., this]
  bool notless_than_sec = (this->operator>(sec));
  if (notless_than_sec) {
    if (dt.year == sec.dt.year) {
      if (dt.month == sec.dt.month) {
        result = dt.day - sec.dt.day;
      } else {
        for (unsigned int i = sec.dt.month + 1; i < dt.month; i++) result += NoDaysInMonth(dt.year, i);
        result += NoDaysInMonth(sec.dt.year, sec.dt.month) - sec.dt.day + 1;
        result += dt.day - 1;
      }
    } else {
      for (int i = int(sec.dt.year) + 1; i < dt.year; i++) result += (IsLeapYear(i) ? 366 : 365);
      for (int i = int(sec.dt.month) + 1; i <= 12; i++) result += NoDaysInMonth(sec.dt.year, i);
      for (unsigned int i = 1; i < dt.month; i++) result += NoDaysInMonth(dt.year, i);
      result += NoDaysInMonth(sec.dt.year, sec.dt.month) - sec.dt.day + 1;
      result += dt.day - 1;
    }
  } else {
    if (dt.year == sec.dt.year) {
      if (dt.month == sec.dt.month) {
        result = sec.dt.day - dt.day;
      } else {
        for (int i = int(dt.month) + 1; i < sec.dt.month; i++) result += NoDaysInMonth(sec.dt.year, i);
        result += NoDaysInMonth(dt.year, dt.month) - dt.day + 1;
        result += sec.dt.day - 1;
      }
    } else {
      for (unsigned int i = (dt.year) + 1; i < sec.dt.year; i++) result += (IsLeapYear(i) ? 366 : 365);
      for (int i = int(dt.month) + 1; i <= 12; i++) result += NoDaysInMonth(dt.year, i);
      for (unsigned int i = 1; i < sec.dt.month; i++) result += NoDaysInMonth(sec.dt.year, i);
      result += NoDaysInMonth(dt.year, dt.month) - dt.day + 1;
      result += sec.dt.day - 1;
    }
  }

  return notless_than_sec ? result : -result;
}

common::CT RCDateTime::Type() const { return at; }

uint RCDateTime::GetHashCode() const {
  uint64_t v = *(uint64_t *)&dt;
  return (uint)(v >> 32) + (uint)(v) /*+ *(short*)&tz*/;
}

int RCDateTime::compare(const RCDateTime &rcv) const {
  int64_t v1 = *(int64_t *)&dt;
  int64_t v2 = *(int64_t *)&rcv.dt;
  return (v1 < v2 ? -1 : (v1 > v2 ? 1 : 0));
}

int RCDateTime::compare(const RCNum &rcv) const {
  if (IsNull() || rcv.IsNull()) return false;
  int64_t tmp;
  ToInt64(tmp);
  return int(tmp - ((RCNum &)rcv).GetIntPart());
}

void RCDateTime::AdjustTimezone(RCDateTime &dt) {
  // timezone conversion
  if (!dt.IsZero()) {
    auto thd = current_tx->Thd();
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
          &time_tmp, stonedb_sec_since_epoch(t.year, t.month, t.day, t.hour, t.minute, t.second));
      secs = stonedb_sec_since_epoch(time_tmp.year, time_tmp.month, time_tmp.day, time_tmp.hour, time_tmp.minute,
                                     time_tmp.second);

    } else {
      // time in client time zone is converted into UTC and expressed as seconds
      // since EPOCHE
      secs = stonedb_sec_since_epoch(t.year, t.month, t.day, t.hour, t.minute, t.second) - sign * minutes * 60;
    }
    // UTC seconds converted to UTC struct tm
    struct tm utc_t;
    gmtime_r(&secs, &utc_t);
    // UTC time stored on server
    dt = RCDateTime((utc_t.tm_year + 1900) % 10000, utc_t.tm_mon + 1, utc_t.tm_mday, utc_t.tm_hour, utc_t.tm_min,
                    utc_t.tm_sec, common::CT::TIMESTAMP);
    dt.dt.microsecond = t.second_part;
  }
}
}  // namespace types
}  // namespace stonedb
