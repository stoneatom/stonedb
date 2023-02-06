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
#ifndef TIANMU_TYPES_NUM_H_
#define TIANMU_TYPES_NUM_H_
#pragma once

#include "types/tianmu_data_types.h"

namespace Tianmu {
namespace types {

class BString;

class TianmuNum : public ValueBasic<TianmuNum> {
  friend class ValueParserForText;
  friend class Engine;

 public:
  TianmuNum(common::ColumnType attrt = common::ColumnType::NUM);
  TianmuNum(int64_t value, short scale = -1, bool dbl = false, common::ColumnType attrt = common::ColumnType::UNK);
  TianmuNum(double value);
  TianmuNum(const TianmuNum &);
  ~TianmuNum();

  TianmuNum &Assign(int64_t value, short scale = -1, bool dbl = false,
                    common::ColumnType attrt = common::ColumnType::UNK);
  TianmuNum &Assign(double value);

  static common::ErrorCode Parse(const BString &tianmu_s, TianmuNum &tianmu_n,
                                 common::ColumnType at = common::ColumnType::UNK);
  static common::ErrorCode ParseReal(const BString &, TianmuNum &, common::ColumnType at);
  static common::ErrorCode ParseNum(const BString &, TianmuNum &, short scale = -1);

  TianmuNum &operator=(const TianmuNum &tianmu_n);
  TianmuNum &operator=(const TianmuDataType &tianmu_dt) override;

  common::ColumnType Type() const override;

  bool operator==(const TianmuDataType &tianmu_dt) const override;
  bool operator<(const TianmuDataType &tianmu_dt) const override;
  bool operator>(const TianmuDataType &tianmu_dt) const override;
  bool operator>=(const TianmuDataType &tianmu_dt) const override;
  bool operator<=(const TianmuDataType &tianmu_dt) const override;
  bool operator!=(const TianmuDataType &tianmu_dt) const override;

  TianmuNum &operator-=(const TianmuNum &tianmu_n);
  TianmuNum &operator+=(const TianmuNum &tianmu_n);
  TianmuNum &operator*=(const TianmuNum &tianmu_n);
  TianmuNum &operator/=(const TianmuNum &tianmu_n);

  TianmuNum operator-(const TianmuNum &tianmu_n) const;
  TianmuNum operator+(const TianmuNum &tianmu_n) const;
  TianmuNum operator*(const TianmuNum &tianmu_n) const;
  TianmuNum operator/(const TianmuNum &tianmu_n) const;

  bool IsDecimal(ushort scale) const;
  bool IsReal() const { return is_double_; }
  bool IsInt() const;

  BString ToBString() const override;
  TianmuNum ToDecimal(int scale = -1) const;
  TianmuNum ToReal() const;
  TianmuNum ToInt() const;

  operator int64_t() const { return GetIntPart(); }
  operator double() const;
  operator float() const { return (float)(double)*this; }

  short Scale() const { return scale_; }
  int64_t ValueInt() const { return value_; }
  char *GetDataBytesPointer() const override { return (char *)&value_; }
  int64_t GetIntPart() const {
    return is_double_ ? (int64_t)GetIntPartAsDouble() : value_ / (int64_t)Uint64PowOfTen(scale_);
  }

  int64_t GetValueInt64() const { return value_; }
  double GetIntPartAsDouble() const;
  double GetFractPart() const;

  short GetDecStrLen() const;
  short GetDecIntLen() const;
  short GetDecFractLen() const;

  uint GetHashCode() const override;
  void Negate();

 private:
  int compare(const TianmuNum &tianmu_n) const;
  int compare(const TianmuDateTime &tianmu_n) const;

 private:
  static constexpr int MAX_DEC_PRECISION = 18;
  int64_t value_;
  ushort scale_;  // means 'scale' actually
  bool is_double_;
  bool is_dot_;
  common::ColumnType attr_type_;

 public:
  const static ValueTypeEnum value_type_ = ValueTypeEnum::NUMERIC_TYPE;
};

}  // namespace types
}  // namespace Tianmu

#endif  // TIANMU_TYPES_NUM_H_
