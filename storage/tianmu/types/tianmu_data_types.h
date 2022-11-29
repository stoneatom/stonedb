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
#ifndef TIANMU_TYPES_DATA_TYPES_H_
#define TIANMU_TYPES_DATA_TYPES_H_
#pragma once

#include <cstring>
#include <regex>

#include "common/assert.h"
#include "common/common_definitions.h"
#include "common/exception.h"
#include "core/bin_tools.h"
#include "core/tianmu_attr_typeinfo.h"
#include "system/txt_utils.h"

namespace Tianmu {
namespace types {

class BString;
class TianmuNum;

bool AreComparable(common::ColumnType att1, common::ColumnType att2);

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
    if (my_time.time_type == MYSQL_TIMESTAMP_TIME)
      time_hour = my_time.hour;
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

    if (t == MYSQL_TIMESTAMP_TIME)
      my_time->hour = time_hour;
  }

  // util functions
  static int64_t DateSortEncoding(int64_t v) {
    if (v != common::MINUS_INF_64 && v != common::PLUS_INF_64)
      return (v >> 37);  // omit sec, min, hour
    return v;
  }

  static int64_t DateSortDecoding(int64_t v) {
    if (v != common::MINUS_INF_64 && v != common::PLUS_INF_64)
      return (v << 37);  // omit sec, min, hour
    return v;
  }

  static int64_t YearSortEncoding(int64_t v) {
    if (v != common::MINUS_INF_64 && v != common::PLUS_INF_64)
      return (v >> 46);  // omit sec, min, hour, day, month
    return v;
  }

  static int64_t YearSortDecoding(int64_t v) {
    if (v != common::MINUS_INF_64 && v != common::PLUS_INF_64)
      return (v << 46);  // omit sec, min, hour, day, month
    return v;
  }
};

enum class ValueTypeEnum { NULL_TYPE, DATE_TIME_TYPE, NUMERIC_TYPE, STRING_TYPE };

class TianmuDataType {
 public:
  TianmuDataType() : null_(true) {}
  constexpr TianmuDataType(const TianmuDataType &) = default;
  virtual ~TianmuDataType();

 public:
  virtual std::unique_ptr<TianmuDataType> Clone() const = 0;
  virtual BString ToBString() const = 0;
  virtual common::ColumnType Type() const = 0;
  virtual ValueTypeEnum GetValueType() const = 0;

  bool AreComperable(const TianmuDataType &) const;
  bool compare(const TianmuDataType &tianmu_dt, common::Operator op, char like_esc) const;

  virtual TianmuDataType &operator=(const TianmuDataType &tianmu_n) = 0;
  virtual bool operator==(const TianmuDataType &tianmu_dt) const = 0;
  virtual bool operator<(const TianmuDataType &tianmu_dt) const = 0;
  virtual bool operator>(const TianmuDataType &tianmu_dt) const = 0;
  virtual bool operator>=(const TianmuDataType &tianmu_dt) const = 0;
  virtual bool operator<=(const TianmuDataType &tianmu_dt) const = 0;
  virtual bool operator!=(const TianmuDataType &tianmu_dt) const = 0;

  bool IsNull() const { return null_; }
  virtual uint GetHashCode() const = 0;

  virtual char *GetDataBytesPointer() const = 0;

  void SetToNull() { null_ = true; }
  bool null_;

 public:
  static ValueTypeEnum GetValueType(common::ColumnType attr_type);
  static bool ToDecimal(const TianmuDataType &in, int scale, TianmuNum &out);
  static bool ToInt(const TianmuDataType &in, TianmuNum &out);
  static bool ToReal(const TianmuDataType &in, TianmuNum &out);

  static bool AreComperable(const TianmuDataType &tianmu_dt1, const TianmuDataType &tianmu_dt2);
  static bool compare(const TianmuDataType &tianmu_dt1, const TianmuDataType &tianmu_dt2, common::Operator op,
                      char like_esc);
};

template <typename T>
class ValueBasic : public TianmuDataType {
 public:
  constexpr ValueBasic(const ValueBasic &) = default;
  ValueBasic() = default;
  ValueTypeEnum GetValueType() const override { return T::value_type_; }
  std::unique_ptr<TianmuDataType> Clone() const override { return std::unique_ptr<TianmuDataType>(new T((T &)*this)); };
  static T null_value_;
  static T &NullValue() { return T::null_value_; }
  using TianmuDataType::operator=;
};

template <typename T>
T ValueBasic<T>::null_value_;

using CondArray = std::vector<BString>;

class BString : public ValueBasic<BString> {
  friend std::ostream &operator<<(std::ostream &out, const BString &tianmu_s);
  friend bool operator!=(const BString &tianmu_s1, const BString &tianmu_s2);

 public:
  BString();
  BString(const char *val, size_t len = 0, bool materialize = false);
  // len_ == -1 or -2  => the length is stored on the first 2 or 4 (respectively,
  // ushort / int) bytes of val_. len_ == 0  => the length is a result of
  // std::strlen(val_), i.e. val_ is 0-terminated zero-term = true  => this is a
  // non-null_ empty string, or a longer zero-terminated string
  BString(const BString &tianmu_s);
  ~BString();

  BString &operator=(const BString &tianmu_s);
  BString &operator=(const TianmuDataType &tianmu_n) override;
  void PersistentCopy(const BString &tianmu_s);  // like "=", but makes this persistent_

  static bool Parse(BString &in, BString &out);
  common::ColumnType Type() const override;

  void PutString(char *&dest, ushort len, bool move_ptr = true) const;
  void PutVarchar(char *&dest, uchar prefixlen, bool move_ptr) const;
  void MakePersistent();
  bool IsPersistent() const { return persistent_; }
  bool IsEmpty() const;        // return true if this is 0 len_ string, if this is null_
                               // this function will return false
  bool IsNullOrEmpty() const;  // return true if this is null_ or this is 0 len_ string
  std::string ToString() const;
  BString ToBString() const override { return *this; }
  char *GetDataBytesPointer() const override { return val_ + pos_; }
  char *begin() const { return GetDataBytesPointer(); }
  char *end() const { return begin() + len_; }
  BString &operator+=(ushort pos);
  BString &operator-=(ushort pos);

  bool operator==(const TianmuDataType &tianmu_n) const override;
  bool operator<(const TianmuDataType &tianmu_n) const override;
  bool operator>(const TianmuDataType &tianmu_n) const override;
  bool operator>=(const TianmuDataType &tianmu_n) const override;
  bool operator<=(const TianmuDataType &tianmu_n) const override;
  bool operator!=(const TianmuDataType &tianmu_n) const override;
  bool operator==(const BString &tianmu_s) const;

  size_t RoundUpTo8Bytes(const DTCollation &dt) const;
  void CopyTo(void *dest, size_t count) const;

  // this is fast for string literal
  bool Equals(const char *s, uint l) const {
    if (l != len_)
      return false;
    return std::memcmp(s, val_, l) == 0;
  }

  int CompareWith(const BString &tianmu_s2) const {
    int l = std::min(len_, tianmu_s2.len_);

    if (l == 0) {
      if (len_ == 0 && tianmu_s2.len_ == 0)
        return 0;

      if (len_ == 0)
        return -1;

      return 1;
    }

    if (len_ != tianmu_s2.len_) {
      int ret = std::memcmp(val_ + pos_, tianmu_s2.val_ + tianmu_s2.pos_, l);
      if (ret == 0) {
        if (len_ < tianmu_s2.len_)
          return -1;
        return 1;
      }
      return ret;
    }

    // equal length
    return std::memcmp(val_ + pos_, tianmu_s2.val_ + tianmu_s2.pos_, l);
  }

  bool IsDigital() {
    static std::regex const re{R"([-+]?((\.\d+)|(\d+\.)|(\d+))\d*([eE][-+]?\d+)?)"};
    return std::regex_match(ToString(), re);
  }

  bool IsDigitalZero() const { return (Equals("0", 1) || Equals("+0", 2) || Equals("-0", 2)); }

  // Wildcards: "_" is any character, "%" is 0 or more characters
  bool Like(const BString &pattern, char escape_character);

  bool GreaterEqThanMin(const void *txt_min);
  bool LessEqThanMax(const void *txt_max);
  bool GreaterEqThanMinUTF(const void *txt_min, DTCollation col, bool use_full_len = false);
  bool LessEqThanMaxUTF(const void *txt_max, DTCollation col, bool use_full_len = false);

  uint GetHashCode() const override;
  size_t size() const { return len_; }
  char &operator[](size_t pos) const;

  char *val_;
  uint len_;
  uint pos_;

 private:
  bool persistent_;

 public:
  const static ValueTypeEnum value_type_ = ValueTypeEnum::STRING_TYPE;
};

class TianmuDateTime : public ValueBasic<TianmuDateTime> {
  friend class ValueParserForText;

 public:
  TianmuDateTime(int64_t dt, common::ColumnType at);
  TianmuDateTime(short year = common::NULL_VALUE_SH);
  TianmuDateTime(short year, short month, short day, short hour, short minute, short second,
                 common::ColumnType at);  // DataTime , Timestamp
  TianmuDateTime(short year, short month, short day, short hour, short minute, short second, uint microsecond,
                 common::ColumnType at);                                // DataTime , Timestamp
  TianmuDateTime(short yh, short mm, short ds, common::ColumnType at);  // Date or Time
  TianmuDateTime(const TianmuDateTime &tianmu_dt);
  TianmuDateTime(const MYSQL_TIME &myt, common::ColumnType at);

  TianmuDateTime(TianmuNum &tianmu_n, common::ColumnType at);
  ~TianmuDateTime();

 public:
  TianmuDateTime &operator=(const TianmuDateTime &tianmu_dt);
  TianmuDateTime &operator=(const TianmuDataType &tianmu_dt) override;
  TianmuDateTime &Assign(int64_t v, common::ColumnType at);

  void Store(MYSQL_TIME *my_time, enum_mysql_timestamp_type t) { dt_.Store(my_time, t); }
  bool IsZero() const;
  int64_t GetInt64() const;
  bool GetInt64(int64_t &value) const {
    value = GetInt64();
    return true;
  };

  /** Convert TianmuDateTime to 64 bit integer in the following format:
   * YEAR: 				YYYY
   * TIME:				(+/-)HHH:MM:SS
   * Date: 				YYYYMMDD
   * DATETIME/TIMESTAM:	YYYYMMDDHHMMSS
   * \param value result of the conversion
   * \return false if it is nullptr, true otherwise
   */
  bool ToInt64(int64_t &value) const;
  char *GetDataBytesPointer() const override { return (char *)&dt_; }
  BString ToBString() const override;
  common::ColumnType Type() const override;
  uint GetHashCode() const override;

  bool operator==(const TianmuDataType &tianmu_dt) const override;
  bool operator<(const TianmuDataType &tianmu_dt) const override;
  bool operator>(const TianmuDataType &tianmu_dt) const override;
  bool operator>=(const TianmuDataType &tianmu_dt) const override;
  bool operator<=(const TianmuDataType &tianmu_dt) const override;
  bool operator!=(const TianmuDataType &tianmu_dt) const override;
  int64_t operator-(const TianmuDateTime &sec) const;  // difference in days, only for common::CT::DATE

  short Year() const { return dt_.year; }
  short Month() const { return dt_.month; }
  short Day() const { return dt_.day; }
  short Hour() const {
    if (at_ != common::ColumnType::TIME)
      return dt_.hour;
    return dt_.time_hour;
  }
  short Minute() const { return dt_.minute; }
  short Second() const { return dt_.second; }
  int MicroSecond() const { return dt_.microsecond; }

 private:
  DT dt_{};
  common::ColumnType at_;

 private:
  int compare(const TianmuDateTime &tianmu_dt) const;
  int compare(const TianmuNum &tianmu_dt) const;

 public:
  static void AdjustTimezone(TianmuDateTime &dt);
  static common::ErrorCode Parse(const BString &, TianmuDateTime &, common::ColumnType);
  static common::ErrorCode Parse(const int64_t &, TianmuDateTime &, common::ColumnType, int precision = -1);

  static bool CanBeYear(int64_t year);
  static bool CanBeMonth(int64_t month);
  static bool CanBeDay(int64_t day);
  static bool CanBeHour(int64_t hour);
  static bool CanBeMinute(int64_t minute);
  static bool CanBeSecond(int64_t second);
  static bool CanBeMicroSecond(int64_t microsecond);
  static bool CanBeDate(int64_t year, int64_t month, int64_t day);
  static bool CanBeTime(int64_t hour, int64_t minute, int64_t second);
  static bool CanBeTimestamp(int64_t year, int64_t month, int64_t day, int64_t hour, int64_t minute, int64_t second);
  static bool CanBeDatetime(int64_t year, int64_t month, int64_t day, int64_t hour, int64_t minute, int64_t second);

  static bool IsLeapYear(short year);
  static ushort NoDaysInMonth(short year, ushort month);

  static bool IsCorrectTianmuYear(short year);
  static bool IsCorrectTianmuDate(short year, short month, short day);
  static bool IsCorrectTianmuTime(short hour, short minute, short second);
  static bool IsCorrectTianmuTimestamp(short year, short month, short day, short hour, short minute, short second);
  static bool IsCorrectTianmuDatetime(short year, short month, short day, short hour, short minute, short second);
  static bool IsZeroTianmuDate(short year, short month, short day);
  static short ToCorrectYear(uint v, common::ColumnType at, bool is_year_2 = false);
  static TianmuDateTime GetSpecialValue(common::ColumnType at);
  static TianmuDateTime GetCurrent();

 public:
  const static ValueTypeEnum value_type_ = ValueTypeEnum::DATE_TIME_TYPE;
};

class TianmuValueObject {
 public:
  TianmuValueObject();
  TianmuValueObject(const TianmuValueObject &tianmu_value_obj);
  TianmuValueObject(const TianmuDataType &tianmu_value_obj);

  ~TianmuValueObject();
  TianmuValueObject &operator=(const TianmuValueObject &tianmu_value_obj);

  bool compare(const TianmuValueObject &tianmu_value_obj, common::Operator op, char like_esc) const;

  bool operator==(const TianmuValueObject &tianmu_value_obj) const;
  bool operator<(const TianmuValueObject &tianmu_value_obj) const;
  bool operator>(const TianmuValueObject &tianmu_value_obj) const;
  bool operator>=(const TianmuValueObject &tianmu_value_obj) const;
  bool operator<=(const TianmuValueObject &tianmu_value_obj) const;
  bool operator!=(const TianmuValueObject &tianmu_value_obj) const;

  bool operator==(const TianmuDataType &tianmu_dt) const;
  bool operator<(const TianmuDataType &tianmu_dt) const;
  bool operator>(const TianmuDataType &tianmu_dt) const;
  bool operator>=(const TianmuDataType &tianmu_dt) const;
  bool operator<=(const TianmuDataType &tianmu_dt) const;
  bool operator!=(const TianmuDataType &tianmu_dt) const;

  bool IsNull() const;

  common::ColumnType Type() const { return value_.get() ? value_->Type() : common::ColumnType::UNK; }
  ValueTypeEnum GetValueType() const { return value_.get() ? value_->GetValueType() : ValueTypeEnum::NULL_TYPE; }
  BString ToBString() const;
  // operator TianmuDataType*()		{ return value_.get(); }
  TianmuDataType *Get() const { return value_.get(); }
  TianmuDataType &operator*() const;

  // TianmuDataType& operator*() const	{ DEBUG_ASSERT(value_.get()); return
  // *value_; }

  operator TianmuNum &() const;
  // operator BString&() const;
  operator TianmuDateTime &() const;
  uint GetHashCode() const;
  char *GetDataBytesPointer() const { return value_->GetDataBytesPointer(); }

 private:
  inline void construct(const TianmuDataType &tianmu_dt);

 protected:
  std::unique_ptr<TianmuDataType> value_;

 public:
  static bool compare(const TianmuValueObject &tianmu_value_obj1, const TianmuValueObject &tianmu_value_obj2,
                      common::Operator op, char like_esc);
};

template <class T>
class tianmu_hash_compare {
 private:
  using Key = T;

 public:
  size_t operator()(const Key k) const { return k->GetHashCode() & 1048575; };
  bool operator()(const Key &k1, const Key &k2) const {
    if (dynamic_cast<TianmuNum *>(k1)) {
      if (dynamic_cast<TianmuNum *>(k2))
        return *k1 == *k2;
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
    coll.collation->coll->strnxfrm(coll.collation, (uchar *)dst.val_, dst.len_, dst.len_, (uchar *)(src.val_), src.len_,
                                   MY_STRXFRM_PAD_WITH_SPACE);
    dst.null_ = false;
  } else {
    dst.null_ = true;
  }
}

static int inline CollationStrCmp(DTCollation coll, const BString &s1, const BString &s2) {
  return coll.collation->coll->strnncoll(coll.collation, (const uchar *)s1.val_, s1.len_, (const uchar *)s2.val_,
                                         s2.len_, 0);
}

static int inline CollationRealCmp(DTCollation coll, const BString &s1, const BString &s2) {
  int not_used = 0;
  char *end_not_used = nullptr;
  double d1 = coll.collation->cset->strntod(coll.collation, const_cast<char *>(s1.GetDataBytesPointer()), s1.len_,
                                            &end_not_used, &not_used);
  double d2 = coll.collation->cset->strntod(coll.collation, const_cast<char *>(s2.GetDataBytesPointer()), s2.len_,
                                            &end_not_used, &not_used);
  return (d1 < d2) ? -1 : ((d1 == d2) ? 0 : 1);
}

static bool inline CollationStrCmp(DTCollation coll, const BString &s1, const BString &s2, common::Operator op) {
  int res = coll.collation->coll->strnncoll(coll.collation, (const uchar *)s1.val_, s1.len_, (const uchar *)s2.val_,
                                            s2.len_, 0);
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
    if ((IsUnicode(first) && !IsUnicode(sec)) || (IsBinary(first) && !IsBinary(sec)))
      return first;
    if (sec.derivation < first.derivation || (IsUnicode(sec) && !IsUnicode(first)) ||
        (IsBinary(sec) && !IsBinary(first)))
      return sec;
    if (std::strcmp(sec.collation->csname, first.collation->csname) == 0) {
      if (IsBin(first))
        return first;
      if (IsBin(sec))
        return sec;
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
  if (exponent >= 0 && exponent < 19)
    return v[exponent];
  return (uint64_t)PowOfTen(exponent) * 5;
}

const static TianmuDateTime kTianmuYearMin(1901);
const static TianmuDateTime kTianmuYearMax(2155);
const static TianmuDateTime kTianmuYearSpec(0);

const static TianmuDateTime kTianmuTimeMin(-838, 59, 59, common::ColumnType::TIME);
const static TianmuDateTime kTianmuTimeMax(838, 59, 59, common::ColumnType::TIME);
const static TianmuDateTime kTianmuTimeSpec(0, common::ColumnType::TIME);

const static TianmuDateTime kTianmuDateMin(100, 1, 1, common::ColumnType::DATE);
const static TianmuDateTime kTianmuDateMax(9999, 12, 31, common::ColumnType::DATE);
const static TianmuDateTime kTianmuDateSpec(0, common::ColumnType::DATE);

const static TianmuDateTime kTianmuDatetimeMin(100, 1, 1, 0, 0, 0, common::ColumnType::DATETIME);
const static TianmuDateTime kTianmuDatetimeMax(9999, 12, 31, 23, 59, 59, common::ColumnType::DATETIME);
const static TianmuDateTime kTianmuDatetimeSpec(0, common::ColumnType::DATETIME);

const static TianmuDateTime kTianmuTimestampMin(1970, 01, 01, 00, 00, 00, common::ColumnType::TIMESTAMP);
const static TianmuDateTime kTianmuTimestampMax(2038, 01, 01, 00, 59, 59, common::ColumnType::TIMESTAMP);
const static TianmuDateTime kTianmuTimestampSpec(0, common::ColumnType::TIMESTAMP);

}  // namespace types
}  // namespace Tianmu

#endif  // TIANMU_TYPES_DATA_TYPES_H_
