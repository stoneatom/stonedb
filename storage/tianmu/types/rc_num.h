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
#ifndef TIANMU_TYPES_RC_NUM_H_
#define TIANMU_TYPES_RC_NUM_H_
#pragma once

#include "types/rc_data_types.h"

namespace Tianmu {
namespace types {

class BString;

class RCNum : public ValueBasic<RCNum> {
  friend class ValueParserForText;
  friend class Engine;

 public:
  RCNum(common::ColumnType attrt = common::ColumnType::NUM);
  RCNum(int64_t value, short scale = -1, bool dbl = false, common::ColumnType attrt = common::ColumnType::UNK);
  RCNum(double value);
  RCNum(const RCNum &);
  ~RCNum();

  RCNum &Assign(int64_t value, short scale = -1, bool dbl = false, common::ColumnType attrt = common::ColumnType::UNK);
  RCNum &Assign(double value);

  static common::ErrorCode Parse(const BString &rcs, RCNum &rcn, common::ColumnType at = common::ColumnType::UNK);
  static common::ErrorCode ParseReal(const BString &, RCNum &, common::ColumnType at);
  static common::ErrorCode ParseNum(const BString &, RCNum &, short scale = -1);

  RCNum &operator=(const RCNum &rcn);
  RCNum &operator=(const RCDataType &rcdt) override;

  common::ColumnType Type() const override;

  bool operator==(const RCDataType &rcdt) const override;
  bool operator<(const RCDataType &rcdt) const override;
  bool operator>(const RCDataType &rcdt) const override;
  bool operator>=(const RCDataType &rcdt) const override;
  bool operator<=(const RCDataType &rcdt) const override;
  bool operator!=(const RCDataType &rcdt) const override;

  RCNum &operator-=(const RCNum &rcn);
  RCNum &operator+=(const RCNum &rcn);
  RCNum &operator*=(const RCNum &rcn);
  RCNum &operator/=(const RCNum &rcn);

  RCNum operator-(const RCNum &rcn) const;
  RCNum operator+(const RCNum &rcn) const;
  RCNum operator*(const RCNum &rcn) const;
  RCNum operator/(const RCNum &rcn) const;

  bool IsDecimal(ushort scale) const;
  bool IsReal() const { return is_double_; }
  bool IsInt() const;

  BString ToBString() const override;
  RCNum ToDecimal(int scale = -1) const;
  RCNum ToReal() const;
  RCNum ToInt() const;

  operator int64_t() const { return GetIntPart(); }
  operator double() const;
  operator float() const { return (float)(double)*this; }

  short Scale() const { return scale_; }
  int64_t ValueInt() const { return value_; }
  char *GetDataBytesPointer() const override { return reinterpret_cast<char *>(const_cast<int64_t *>(&value_)); }
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
  int compare(const RCNum &rcn) const;
  int compare(const RCDateTime &rcn) const;

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

#endif  // TIANMU_TYPES_RC_NUM_H_
