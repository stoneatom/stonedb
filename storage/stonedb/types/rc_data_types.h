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
#ifndef STONEDB_TYPES_RC_DATA_TYPES_H_
#define STONEDB_TYPES_RC_DATA_TYPES_H_
#pragma once

#include <cstring>

#include "common/assert.h"
#include "common/common_definitions.h"
#include "common/exception.h"
#include "core/bin_tools.h"
#include "core/rc_attr_typeinfo.h"
#include "system/txt_utils.h"

namespace stonedb {
namespace types {
class BString;
class RCNum;

bool AreComparable(common::CT att1, common::CT att2);

//  TYPE            range
// -------------------------------------------
//  DATE          	'1000-01-01' to '9999-12-31'
//  DATETIME      	'1000-01-01 00:00:00.000000' to '9999-12-31
//  23:59:59.999999' TIMESTAMP     	'1970-01-01 00:00:01.000000' to
//  '2038-01-19 03:14:07.999999'. TIME          	'-838:59:59.000000' to
//  '838:59:59.000000'. YEAR            1901 to 2155

union DT {
  //  63    MSB <---------                     --------------> LSB    0
  //   |                                                              |
  //   v                                                              v
  //   xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  //   ????++++++++++++++----+++++-----++++++------++++++++++++++++++++
  //   4      14          4    5    5     6     6        20
  //  pad     Y           M    D    H     m     s        us
  //   ---------------------+++++++++++
  //            signed time hour (10bit)
  struct {
    uint32_t microsecond : 20;
    uint32_t second : 6;
    uint32_t minute : 6;
    union {
      uint32_t time_hour : 10;  // -838 to 838
      struct {
        uint32_t hour : 5;
        uint32_t day : 5;
        uint32_t month : 4;
        uint32_t year : 14;
        uint32_t padding : 3;
        uint32_t neg : 1;
      };
    };
  };
  uint64_t val = 0;

  DT() {}
  DT(const MYSQL_TIME &my_time) {
    year = my_time.year;
    month = my_time.month;
    day = my_time.day;
    hour = my_time.hour;
    minute = my_time.minute;
    second = my_time.second;
    microsecond = my_time.second_part;
    neg = my_time.neg;
    if (my_time.time_type == MYSQL_TIMESTAMP_TIME) time_hour = my_time.hour;
  }
  bool Neg() const { return neg == 1; }
  void Store(MYSQL_TIME *my_time, enum_mysql_timestamp_type t) {
    my_time->year = year;
    my_time->month = month;
    my_time->day = day;
    my_time->hour = hour;
    my_time->minute = minute;
    my_time->second = second;
    my_time->second_part = microsecond;
    my_time->neg = neg;
    my_time->time_type = t;

    if (t == MYSQL_TIMESTAMP_TIME) my_time->hour = time_hour;
  }
  /////////////////////////////////////////////////////////////////////////////
  // util functions
  static int64_t DateSortEncoding(int64_t v) {
    if (v != common::MINUS_INF_64 && v != common::PLUS_INF_64) return (v >> 37);  // omit sec, min, hour
    return v;
  }

  static int64_t DateSortDecoding(int64_t v) {
    if (v != common::MINUS_INF_64 && v != common::PLUS_INF_64) return (v << 37);  // omit sec, min, hour
    return v;
  }

  static int64_t YearSortEncoding(int64_t v) {
    if (v != common::MINUS_INF_64 && v != common::PLUS_INF_64) return (v >> 46);  // omit sec, min, hour, day, month
    return v;
  }

  static int64_t YearSortDecoding(int64_t v) {
    if (v != common::MINUS_INF_64 && v != common::PLUS_INF_64) return (v << 46);  // omit sec, min, hour, day, month
    return v;
  }
};

enum class ValueTypeEnum { NULL_TYPE, DATE_TIME_TYPE, NUMERIC_TYPE, STRING_TYPE };

class RCDataType {
 public:
  RCDataType() : null(true) {}
  virtual ~RCDataType();

 public:
  virtual std::unique_ptr<RCDataType> Clone() const = 0;
  virtual BString ToBString() const = 0;
  virtual common::CT Type() const = 0;
  virtual ValueTypeEnum GetValueType() const = 0;

  bool AreComperable(const RCDataType &) const;
  bool compare(const RCDataType &rcdt, common::Operator op, char like_esc) const;

  virtual RCDataType &operator=(const RCDataType &rcn) = 0;
  virtual bool operator==(const RCDataType &rcdt) const = 0;
  virtual bool operator<(const RCDataType &rcdt) const = 0;
  virtual bool operator>(const RCDataType &rcdt) const = 0;
  virtual bool operator>=(const RCDataType &rcdt) const = 0;
  virtual bool operator<=(const RCDataType &rcdt) const = 0;
  virtual bool operator!=(const RCDataType &rcdt) const = 0;

  bool IsNull() const { return null; }
  virtual uint GetHashCode() const = 0;

  virtual char *GetDataBytesPointer() const = 0;

  void SetToNull() { null = true; }
  bool null;

 public:
  static ValueTypeEnum GetValueType(common::CT attr_type);
  static bool ToDecimal(const RCDataType &in, int scale, RCNum &out);
  static bool ToInt(const RCDataType &in, RCNum &out);
  static bool ToReal(const RCDataType &in, RCNum &out);

  static bool AreComperable(const RCDataType &rcdt1, const RCDataType &rcdt2);
  static bool compare(const RCDataType &rcdt1, const RCDataType &rcdt2, common::Operator op, char like_esc);
};

template <typename T>
class ValueBasic : public RCDataType {
 public:
  ValueTypeEnum GetValueType() const override { return T::value_type; }
  std::unique_ptr<RCDataType> Clone() const override { return std::unique_ptr<RCDataType>(new T((T &)*this)); };
  static T null_value;
  static T &NullValue() { return T::null_value; }
  using RCDataType::operator=;
};

template <typename T>
T ValueBasic<T>::null_value;

using CondArray = std::vector<BString>;

class BString : public ValueBasic<BString> {
  friend std::ostream &operator<<(std::ostream &out, const BString &rcbs);
  friend bool operator!=(const BString &rcbs1, const BString &rcbs2);

 public:
  BString();
  BString(const char *val, size_t len = 0, bool materialize = false);
  // len == -1 or -2  => the length is stored on the first 2 or 4 (respectively,
  // ushort / int) bytes of val. len == 0  => the length is a result of
  // std::strlen(val), i.e. val is 0-terminated zero-term = true  => this is a
  // non-null empty string, or a longer zero-terminated string
  BString(const BString &rcbs);
  ~BString();

  BString &operator=(const BString &rcbs);
  BString &operator=(const RCDataType &rcn) override;
  void PersistentCopy(const BString &rcbs);  // like "=", but makes this persistent

  static bool Parse(BString &in, BString &out);
  common::CT Type() const override;

  void PutString(char *&dest, ushort len, bool move_ptr = true) const;
  void PutVarchar(char *&dest, uchar prefixlen, bool move_ptr) const;
  void MakePersistent();
  bool IsPersistent() const { return persistent; }
  bool IsEmpty() const;        // return true if this is 0 len string, if this is null
                               // this function will return false
  bool IsNullOrEmpty() const;  // return true if this is null or this is 0 len string
  std::string ToString() const;
  BString ToBString() const override { return *this; }
  char *GetDataBytesPointer() const override { return val + pos; }
  char *begin() const { return GetDataBytesPointer(); }
  char *end() const { return begin() + len; }
  BString &operator+=(ushort pos);
  BString &operator-=(ushort pos);

  bool operator==(const RCDataType &rcn) const override;
  bool operator<(const RCDataType &rcn) const override;
  bool operator>(const RCDataType &rcn) const override;
  bool operator>=(const RCDataType &rcn) const override;
  bool operator<=(const RCDataType &rcn) const override;
  bool operator!=(const RCDataType &rcn) const override;
  bool operator==(const BString &rcs) const;

  size_t RoundUpTo8Bytes(const DTCollation &dt) const;
  void CopyTo(void *dest, size_t count) const;

  // this is fast for string literal
  bool Equals(const char *s, uint l) const {
    if (l != len) return false;
    return std::memcmp(s, val, l) == 0;
  }

  int CompareWith(const BString &rcbs2) const {
    int l = std::min(len, rcbs2.len);

    if (l == 0) {
      if (len == 0 && rcbs2.len == 0) return 0;

      if (len == 0) return -1;

      return 1;
    }

    if (len != rcbs2.len) {
      int ret = std::memcmp(val + pos, rcbs2.val + rcbs2.pos, l);
      if (ret == 0) {
        if (len < rcbs2.len) return -1;
        return 1;
      }
      return ret;
    }

    // equal length
    return std::memcmp(val + pos, rcbs2.val + rcbs2.pos, l);
  }

  // Wildcards: "_" is any character, "%" is 0 or more characters
  bool Like(const BString &pattern, char escape_character);

  bool GreaterEqThanMin(const void *txt_min);
  bool LessEqThanMax(const void *txt_max);
  bool GreaterEqThanMinUTF(const void *txt_min, DTCollation col, bool use_full_len = false);
  bool LessEqThanMaxUTF(const void *txt_max, DTCollation col, bool use_full_len = false);

  uint GetHashCode() const override;
  size_t size() const { return len; }
  char &operator[](size_t pos) const;

  char *val;
  uint len;
  uint pos;

 private:
  bool persistent;

 public:
  const static ValueTypeEnum value_type = ValueTypeEnum::STRING_TYPE;
};

class RCDateTime : public ValueBasic<RCDateTime> {
  friend class ValueParserForText;

 public:
  RCDateTime(int64_t dt, common::CT at);
  RCDateTime(short year = common::NULL_VALUE_SH);
  RCDateTime(short year, short month, short day, short hour, short minute, short second,
             common::CT at);                                // DataTime , Timestamp
  RCDateTime(short yh, short mm, short ds, common::CT at);  // Date or Time
  RCDateTime(const RCDateTime &rcdt);
  RCDateTime(const MYSQL_TIME &myt, common::CT at);

  RCDateTime(RCNum &rcn, common::CT at);
  ~RCDateTime();

 public:
  RCDateTime &operator=(const RCDateTime &rcdt);
  RCDateTime &operator=(const RCDataType &rcdt) override;
  RCDateTime &Assign(int64_t v, common::CT at);

  void Store(MYSQL_TIME *my_time, enum_mysql_timestamp_type t) { dt.Store(my_time, t); }
  bool IsZero() const;
  int64_t GetInt64() const;
  bool GetInt64(int64_t &value) const {
    value = GetInt64();
    return true;
  };

  /** Convert RCDateTime to 64 bit integer in the following format:
   * YEAR: 				YYYY
   * TIME:				(+/-)HHH:MM:SS
   * Date: 				YYYYMMDD
   * DATETIME/TIMESTAM:	YYYYMMDDHHMMSS
   * \param value result of the conversion
   * \return false if it is NULL, true otherwise
   */
  bool ToInt64(int64_t &value) const;
  char *GetDataBytesPointer() const override { return (char *)&dt; }
  BString ToBString() const override;
  common::CT Type() const override;
  uint GetHashCode() const override;

  bool operator==(const RCDataType &rcdt) const override;
  bool operator<(const RCDataType &rcdt) const override;
  bool operator>(const RCDataType &rcdt) const override;
  bool operator>=(const RCDataType &rcdt) const override;
  bool operator<=(const RCDataType &rcdt) const override;
  bool operator!=(const RCDataType &rcdt) const override;
  int64_t operator-(const RCDateTime &sec) const;  // difference in days, only for common::CT::DATE

  short Year() const { return dt.year; }
  short Month() const { return dt.month; }
  short Day() const { return dt.day; }
  short Hour() const {
    if (at != common::CT::TIME) return dt.hour;
    return dt.time_hour;
  }
  short Minute() const { return dt.minute; }
  short Second() const { return dt.second; }
  int MicroSecond() const { return dt.microsecond; }

 private:
  DT dt{};
  common::CT at;

 private:
  int compare(const RCDateTime &rcdt) const;
  int compare(const RCNum &rcdt) const;

 public:
  static void AdjustTimezone(RCDateTime &dt);
  static common::ErrorCode Parse(const BString &, RCDateTime &, common::CT);
  static common::ErrorCode Parse(const int64_t &, RCDateTime &, common::CT, int precision = -1);

  static bool CanBeYear(int64_t year);
  static bool CanBeMonth(int64_t month);
  static bool CanBeDay(int64_t day);
  static bool CanBeHour(int64_t hour);
  static bool CanBeMinute(int64_t minute);
  static bool CanBeSecond(int64_t second);
  static bool CanBeDate(int64_t year, int64_t month, int64_t day);
  static bool CanBeTime(int64_t hour, int64_t minute, int64_t second);
  static bool CanBeTimestamp(int64_t year, int64_t month, int64_t day, int64_t hour, int64_t minute, int64_t second);
  static bool CanBeDatetime(int64_t year, int64_t month, int64_t day, int64_t hour, int64_t minute, int64_t second);

  static bool IsLeapYear(short year);
  static ushort NoDaysInMonth(short year, ushort month);

  static bool IsCorrectSDBYear(short year);
  static bool IsCorrectSDBDate(short year, short month, short day);
  static bool IsCorrectSDBTime(short hour, short minute, short second);
  static bool IsCorrectSDBTimestamp(short year, short month, short day, short hour, short minute, short second);
  static bool IsCorrectSDBDatetime(short year, short month, short day, short hour, short minute, short second);

  static short ToCorrectYear(uint v, common::CT at, bool is_year_2 = false);
  static RCDateTime GetSpecialValue(common::CT at);
  static RCDateTime GetCurrent();

 public:
  const static ValueTypeEnum value_type = ValueTypeEnum::DATE_TIME_TYPE;
};

class RCValueObject {
 public:
  RCValueObject();
  RCValueObject(const RCValueObject &rcvo);
  RCValueObject(const RCDataType &rcvo);

  ~RCValueObject();
  RCValueObject &operator=(const RCValueObject &rcvo);

  bool compare(const RCValueObject &rcvo, common::Operator op, char like_esc) const;

  bool operator==(const RCValueObject &rcvo) const;
  bool operator<(const RCValueObject &rcvo) const;
  bool operator>(const RCValueObject &rcvo) const;
  bool operator>=(const RCValueObject &rcvo) const;
  bool operator<=(const RCValueObject &rcvo) const;
  bool operator!=(const RCValueObject &rcvo) const;

  bool operator==(const RCDataType &rcdt) const;
  bool operator<(const RCDataType &rcdt) const;
  bool operator>(const RCDataType &rcdt) const;
  bool operator>=(const RCDataType &rcdt) const;
  bool operator<=(const RCDataType &rcdt) const;
  bool operator!=(const RCDataType &rcdt) const;

  bool IsNull() const;

  common::CT Type() const { return value.get() ? value->Type() : common::CT::UNK; }
  ValueTypeEnum GetValueType() const { return value.get() ? value->GetValueType() : ValueTypeEnum::NULL_TYPE; }
  BString ToBString() const;
  // operator RCDataType*()		{ return value.get(); }
  RCDataType *Get() const { return value.get(); }
  RCDataType &operator*() const;

  // RCDataType& operator*() const	{ DEBUG_ASSERT(value.get()); return
  // *value; }

  operator RCNum &() const;
  // operator BString&() const;
  operator RCDateTime &() const;
  uint GetHashCode() const;
  char *GetDataBytesPointer() const { return value->GetDataBytesPointer(); }

 private:
  inline void construct(const RCDataType &rcdt);

 protected:
  std::unique_ptr<RCDataType> value;

 public:
  static bool compare(const RCValueObject &rcvo1, const RCValueObject &rcvo2, common::Operator op, char like_esc);
};

template <class T>
class rc_hash_compare {
 private:
  using Key = T;

 public:
  size_t operator()(const Key k) const { return k->GetHashCode() & 1048575; };
  bool operator()(const Key &k1, const Key &k2) const {
    if (dynamic_cast<RCNum *>(k1)) {
      if (dynamic_cast<RCNum *>(k2)) return *k1 == *k2;
    } else if (AreComparable(k1->Type(), k2->Type()))
      return *k1 == *k2;
    return false;
  };
};

/*! \brief Converts BString according to a given collation to the binary form
 * that can be used by std::memcmp \param src - source BString \param dst -
 * destination BString \param charset - character set to be used
 */
static inline void ConvertToBinaryForm(const BString &src, BString &dst, DTCollation coll) {
  if (!src.IsNull()) {
    coll.collation->coll->strnxfrm(coll.collation, (uchar *)dst.val, dst.len, dst.len, (uchar *)(src.val), src.len,
                                   MY_STRXFRM_PAD_WITH_SPACE);
    dst.null = false;
  } else {
    dst.null = true;
  }
}

static int inline CollationStrCmp(DTCollation coll, const BString &s1, const BString &s2) {
  return coll.collation->coll->strnncoll(coll.collation, (const uchar *)s1.val, s1.len, (const uchar *)s2.val, s2.len,
                                         0);
}

static bool inline CollationStrCmp(DTCollation coll, const BString &s1, const BString &s2, common::Operator op) {
  int res =
      coll.collation->coll->strnncoll(coll.collation, (const uchar *)s1.val, s1.len, (const uchar *)s2.val, s2.len, 0);
  switch (op) {
    case common::Operator::O_EQ:
      return (res == 0);
    case common::Operator::O_NOT_EQ:
      return (res != 0);
    case common::Operator::O_MORE:
      return (res > 0);
    case common::Operator::O_MORE_EQ:
      return (res >= 0);
    case common::Operator::O_LESS:
      return (res < 0);
    case common::Operator::O_LESS_EQ:
      return (res <= 0);
    default:
      ASSERT(false, "OPERATOR NOT IMPLEMENTED");
      return false;
  }
}

static uint inline CollationBufLen(DTCollation coll, int max_str_size) {
  return coll.collation->coll->strnxfrmlen(coll.collation, max_str_size * coll.collation->mbmaxlen);
}

static bool conv_required_table[] = {
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1,  //
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 10-
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 20-
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 30-
    1, 1, 1, 0, 1, 1, 1, 0, 1, 1,  // 40-
    0, 1, 1, 0, 1, 1, 1, 1, 0, 1,  // 50
    1, 1, 1, 0, 0, 0, 0, 0, 0, 0,  // 60
    0, 0, 0, 0, 0, 0, 1, 0, 0, 0,  // 70
    0, 0, 0, 1, 0, 0, 0, 0, 1, 0,  // 80
    1, 1, 1, 0, 1, 1, 0, 1, 1, 1,  // 90
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 100
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 110
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 120
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 130
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 140
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 150
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 160
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 170
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 180
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 190
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 200
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 210
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 220
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 230
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  // 240
    1, 1, 1, 1, 1, 1               // 250
};

static inline bool RequiresUTFConversions(const DTCollation &coll) {
  DEBUG_ASSERT(coll.collation->number < 256);
  return (conv_required_table[coll.collation->number]);
}

static inline bool IsUnicode(DTCollation coll) {
  return (std::strcmp(coll.collation->csname, "utf8") == 0 || std::strcmp(coll.collation->csname, "ucs2") == 0);
}

static inline bool IsBinary(DTCollation coll) { return (std::strcmp(coll.collation->csname, "binary") == 0); }

static inline bool IsBin(DTCollation coll) { return (std::strstr(coll.collation->csname, "_bin") == 0); }

static inline bool IsCaseInsensitive(const DTCollation &coll) {
  return (std::strstr(coll.collation->name, "_ci") != 0);
}

inline DTCollation ResolveCollation(DTCollation first, DTCollation sec) {
  if (sec.collation != first.collation && sec.derivation <= first.derivation) {
    if ((IsUnicode(first) && !IsUnicode(sec)) || (IsBinary(first) && !IsBinary(sec))) return first;
    if (sec.derivation < first.derivation || (IsUnicode(sec) && !IsUnicode(first)) ||
        (IsBinary(sec) && !IsBinary(first)))
      return sec;
    if (std::strcmp(sec.collation->csname, first.collation->csname) == 0) {
      if (IsBin(first)) return first;
      if (IsBin(sec)) return sec;
    }
    DEBUG_ASSERT(!"Error: Incompatible collations!");
  }
  return first;
}

static inline double PowOfTen(int exponent) { return std::pow((double)10, exponent); }

static inline uint64_t Uint64PowOfTen(short exponent) {
  DEBUG_ASSERT(exponent >= 0 && exponent < 20);

  static uint64_t v[] = {1ULL,
                         10ULL,
                         100ULL,
                         1000ULL,
                         10000ULL,
                         100000ULL,
                         1000000ULL,
                         10000000ULL,
                         100000000ULL,
                         1000000000ULL,
                         10000000000ULL,
                         100000000000ULL,
                         1000000000000ULL,
                         10000000000000ULL,
                         100000000000000ULL,
                         1000000000000000ULL,
                         10000000000000000ULL,
                         100000000000000000ULL,
                         1000000000000000000ULL,
                         10000000000000000000ULL};

  if (exponent >= 0 && exponent < 20)
    return v[exponent];
  else
    return (uint64_t)PowOfTen(exponent);
}

static inline int64_t Int64PowOfTen(short exponent) { return int64_t(Uint64PowOfTen(exponent)); }

static inline uint64_t Uint64PowOfTenMultiply5(short exponent) {
  DEBUG_ASSERT(exponent >= 0 && exponent < 19);

  static uint64_t v[] = {5ULL,
                         50ULL,
                         500ULL,
                         5000ULL,
                         50000ULL,
                         500000ULL,
                         5000000ULL,
                         50000000ULL,
                         500000000ULL,
                         5000000000ULL,
                         50000000000ULL,
                         500000000000ULL,
                         5000000000000ULL,
                         50000000000000ULL,
                         500000000000000ULL,
                         5000000000000000ULL,
                         50000000000000000ULL,
                         500000000000000000ULL,
                         5000000000000000000ULL};
  if (exponent >= 0 && exponent < 19) return v[exponent];
  return (uint64_t)PowOfTen(exponent) * 5;
}

const static RCDateTime RC_YEAR_MIN(1901);
const static RCDateTime RC_YEAR_MAX(2155);
const static RCDateTime RC_YEAR_SPEC(0);

const static RCDateTime RC_TIME_MIN(-838, 59, 59, common::CT::TIME);
const static RCDateTime RC_TIME_MAX(838, 59, 59, common::CT::TIME);
const static RCDateTime RC_TIME_SPEC(0, common::CT::TIME);

const static RCDateTime RC_DATE_MIN(100, 1, 1, common::CT::DATE);
const static RCDateTime RC_DATE_MAX(9999, 12, 31, common::CT::DATE);
const static RCDateTime RC_DATE_SPEC(0, common::CT::DATE);

const static RCDateTime RC_DATETIME_MIN(100, 1, 1, 0, 0, 0, common::CT::DATETIME);
const static RCDateTime RC_DATETIME_MAX(9999, 12, 31, 23, 59, 59, common::CT::DATETIME);
const static RCDateTime RC_DATETIME_SPEC(0, common::CT::DATETIME);

const static RCDateTime RC_TIMESTAMP_MIN(1970, 01, 01, 00, 00, 00, common::CT::TIMESTAMP);
const static RCDateTime RC_TIMESTAMP_MAX(2038, 01, 01, 00, 59, 59, common::CT::TIMESTAMP);
const static RCDateTime RC_TIMESTAMP_SPEC(0, common::CT::TIMESTAMP);
}  // namespace types
}  // namespace stonedb

#endif  // STONEDB_TYPES_RC_DATA_TYPES_H_
