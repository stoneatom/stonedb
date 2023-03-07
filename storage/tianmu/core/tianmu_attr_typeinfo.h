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
#ifndef TIANMU_CORE_TIANMU_ATTR_TYPEINFO_H_
#define TIANMU_CORE_TIANMU_ATTR_TYPEINFO_H_
#pragma once

#include <bitset>

#include "common/common_definitions.h"

namespace Tianmu {
namespace types {
class TianmuDataType;
}  // namespace types
namespace core {
class ATI {
 public:
  static int TextSize(common::ColumnType attrt, uint precision, int scale, DTCollation col = DTCollation());

  static bool IsInteger32Type(common::ColumnType attr_type) {
    return attr_type == common::ColumnType::INT || attr_type == common::ColumnType::BYTEINT ||
           attr_type == common::ColumnType::SMALLINT || attr_type == common::ColumnType::MEDIUMINT;
  }

  static bool IsIntegerType(common::ColumnType attr_type) {
    return IsInteger32Type(attr_type) || attr_type == common::ColumnType::BIGINT;
  }
  static bool IsFixedNumericType(common::ColumnType attr_type) {
    return IsInteger32Type(attr_type) || attr_type == common::ColumnType::BIGINT ||
           attr_type == common::ColumnType::NUM || attr_type == common::ColumnType::BIT;
  }

  static bool IsRealType(common::ColumnType attr_type) {
    return attr_type == common::ColumnType::FLOAT || attr_type == common::ColumnType::REAL;
  }

  static bool IsBitType(common::ColumnType attr_type) { return attr_type == common::ColumnType::BIT; }

  static bool IsNumericType(common::ColumnType attr_type) {
    return IsIntegerType(attr_type) || attr_type == common::ColumnType::NUM || attr_type == common::ColumnType::FLOAT ||
           attr_type == common::ColumnType::REAL || attr_type == common::ColumnType::BIT;
  }

  static bool IsBinType(common::ColumnType attr_type) {
    return attr_type == common::ColumnType::BYTE || attr_type == common::ColumnType::VARBYTE ||
           attr_type == common::ColumnType::BIN;
  }

  static bool IsTxtType(common::ColumnType attr_type) {
    return attr_type == common::ColumnType::STRING || attr_type == common::ColumnType::VARCHAR ||
           attr_type == common::ColumnType::LONGTEXT;
  }

  static bool IsCharType(common::ColumnType attr_type) { return attr_type == common::ColumnType::STRING; }
  static bool IsStringType(common::ColumnType attr_type) {
    return attr_type == common::ColumnType::STRING || attr_type == common::ColumnType::VARCHAR ||
           attr_type == common::ColumnType::LONGTEXT || IsBinType(attr_type);
  }

  static bool IsDateTimeType(common::ColumnType attr_type) {
    return attr_type == common::ColumnType::DATE || attr_type == common::ColumnType::TIME ||
           attr_type == common::ColumnType::YEAR || attr_type == common::ColumnType::DATETIME ||
           attr_type == common::ColumnType::TIMESTAMP;
  }

  static bool IsDateTimeNType(common::ColumnType attr_type) {
    return attr_type == common::ColumnType::TIME_N || attr_type == common::ColumnType::DATETIME_N ||
           attr_type == common::ColumnType::TIMESTAMP_N;
  }
};

class AttributeTypeInfo {
 public:
  enum class enumATI {
    NOT_NULL = 0,
    AUTO_INC,
    BLOOM_FILTER,
  };

  AttributeTypeInfo(common::ColumnType attrt, bool not_null, uint precision = 0, ushort scale = 0,
                    bool auto_inc = false, DTCollation collation = DTCollation(),
                    common::PackFmt fmt = common::PackFmt::DEFAULT, bool filter = false,
                    std::string field_name = std::string(), bool unsigned_flag = false)
      : attrt_(attrt), fmt_(fmt), precision_(precision), scale_(scale), collation_(collation), field_name_(field_name) {
    unsigned_flag_ = unsigned_flag;
    flag_[static_cast<int>(enumATI::NOT_NULL)] = not_null;
    flag_[static_cast<int>(enumATI::BLOOM_FILTER)] = filter;
    flag_[static_cast<int>(enumATI::AUTO_INC)] = auto_inc;

    // lookup only applies to string type
    if (attrt != common::ColumnType::STRING && attrt != common::ColumnType::VARCHAR && Lookup())
      fmt = common::PackFmt::DEFAULT;
  }
  common::ColumnType Type() const { return attrt_; }
  common::PackType GetPackType() const {
    return ATI::IsDateTimeType(attrt_) || ATI::IsNumericType(attrt_) || Lookup() ? common::PackType::INT
                                                                                 : common::PackType::STR;
  }
  std::string GetFieldName() { return field_name_; }
  uint Precision() const { return precision_; }
  ushort Scale() const { return scale_; }
  uint CharLen() const { return precision_ / collation_.collation->mbmaxlen; }
  bool NotNull() const { return flag_[static_cast<int>(enumATI::NOT_NULL)]; }
  bool AutoInc() const { return flag_[static_cast<int>(enumATI::AUTO_INC)]; }
  void SetCollation(const DTCollation &collation) { this->collation_ = collation; }
  void SetCollation(CHARSET_INFO *charset_info) { this->collation_.set(charset_info); }
  DTCollation GetCollation() const { return collation_; }
  CHARSET_INFO *CharsetInfo() const { return const_cast<CHARSET_INFO *>(this->collation_.collation); }
  const types::TianmuDataType &ValuePrototype() const;
  common::PackFmt Fmt() const { return fmt_; }
  bool Lookup() const { return fmt_ == common::PackFmt::LOOKUP; }
  unsigned char Flag() const { return flag_.to_ulong(); }
  void SetFlag(unsigned char v) { flag_ = std::bitset<std::numeric_limits<unsigned char>::digits>(v); }
  bool GetUnsignedFlag() const { return unsigned_flag_; }

 private:
  common::ColumnType attrt_;
  common::PackFmt fmt_;
  uint precision_;
  int scale_;
  DTCollation collation_;
  std::string field_name_;
  bool unsigned_flag_ = false;

  std::bitset<std::numeric_limits<unsigned char>::digits> flag_;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_TIANMU_ATTR_TYPEINFO_H_
