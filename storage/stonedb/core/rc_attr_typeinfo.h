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
#ifndef STONEDB_CORE_RC_ATTR_TYPEINFO_H_
#define STONEDB_CORE_RC_ATTR_TYPEINFO_H_
#pragma once

#include <bitset>

#include "common/common_definitions.h"

namespace stonedb {
namespace types {
class RCDataType;
}  // namespace types
namespace core {
class ATI {
 public:
  static int TextSize(common::CT attrt, uint precision, int scale, DTCollation col = DTCollation());

  static bool IsInteger32Type(common::CT attr_type) {
    return attr_type == common::CT::INT || attr_type == common::CT::BYTEINT || attr_type == common::CT::SMALLINT ||
           attr_type == common::CT::MEDIUMINT;
  }

  static bool IsIntegerType(common::CT attr_type) {
    return IsInteger32Type(attr_type) || attr_type == common::CT::BIGINT;
  }
  static bool IsFixedNumericType(common::CT attr_type) {
    return IsInteger32Type(attr_type) || attr_type == common::CT::BIGINT || attr_type == common::CT::NUM;
  }

  static bool IsRealType(common::CT attr_type) {
    return attr_type == common::CT::FLOAT || attr_type == common::CT::REAL;
  }
  static bool IsNumericType(common::CT attr_type) {
    return IsInteger32Type(attr_type) || attr_type == common::CT::BIGINT || attr_type == common::CT::NUM ||
           attr_type == common::CT::FLOAT || attr_type == common::CT::REAL;
  }

  static bool IsBinType(common::CT attr_type) {
    return attr_type == common::CT::BYTE || attr_type == common::CT::VARBYTE || attr_type == common::CT::BIN;
  }

  static bool IsTxtType(common::CT attr_type) {
    return attr_type == common::CT::STRING || attr_type == common::CT::VARCHAR || attr_type == common::CT::LONGTEXT;
  }

  static bool IsCharType(common::CT attr_type) { return attr_type == common::CT::STRING; }
  static bool IsStringType(common::CT attr_type) {
    return attr_type == common::CT::STRING || attr_type == common::CT::VARCHAR || attr_type == common::CT::LONGTEXT ||
           IsBinType(attr_type);
  }

  static bool IsDateTimeType(common::CT attr_type) {
    return attr_type == common::CT::DATE || attr_type == common::CT::TIME || attr_type == common::CT::YEAR ||
           attr_type == common::CT::DATETIME || attr_type == common::CT::TIMESTAMP;
  }

  static bool IsDateTimeNType(common::CT attr_type) {
    return attr_type == common::CT::TIME_N || attr_type == common::CT::DATETIME_N ||
           attr_type == common::CT::TIMESTAMP_N;
  }
};

class AttributeTypeInfo {
 public:
  enum class enumATI {
    NOT_NULL = 0,
    AUTO_INC = 1,
    BLOOM_FILTER = 2,
  };

  AttributeTypeInfo(common::CT attrt, bool notnull, uint precision = 0, ushort scale = 0, bool auto_inc = false,
                    DTCollation collation = DTCollation(), common::PackFmt fmt = common::PackFmt::DEFAULT,
                    bool filter = false)
      : attrt(attrt), fmt(fmt), precision(precision), scale(scale), collation(collation) {
    flag[static_cast<int>(enumATI::NOT_NULL)] = notnull;
    flag[static_cast<int>(enumATI::BLOOM_FILTER)] = filter;
    flag[static_cast<int>(enumATI::AUTO_INC)] = auto_inc;

    // lookup only applies to string type
    if (attrt != common::CT::STRING && attrt != common::CT::VARCHAR && Lookup()) fmt = common::PackFmt::DEFAULT;
  }
  common::CT Type() const { return attrt; }
  common::PackType GetPackType() const {
    return ATI::IsDateTimeType(attrt) || ATI::IsNumericType(attrt) || Lookup() ? common::PackType::INT
                                                                               : common::PackType::STR;
  }
  uint Precision() const { return precision; }
  ushort Scale() const { return scale; }
  uint CharLen() const { return precision / collation.collation->mbmaxlen; }
  bool NotNull() const { return flag[static_cast<int>(enumATI::NOT_NULL)]; }
  bool AutoInc() const { return flag[static_cast<int>(enumATI::AUTO_INC)]; }
  void SetCollation(const DTCollation &collation) { this->collation = collation; }
  void SetCollation(CHARSET_INFO *charset_info) { this->collation.set(charset_info); }
  DTCollation GetCollation() const { return collation; }
  CHARSET_INFO *CharsetInfo() const { return const_cast<CHARSET_INFO *>(this->collation.collation); }
  const types::RCDataType &ValuePrototype() const;
  common::PackFmt Fmt() const { return fmt; }
  bool Lookup() const { return fmt == common::PackFmt::LOOKUP; }
  unsigned char Flag() const { return flag.to_ulong(); }
  void SetFlag(unsigned char v) { flag = std::bitset<std::numeric_limits<unsigned char>::digits>(v); }

 private:
  common::CT attrt;
  common::PackFmt fmt;
  uint precision;
  int scale;
  DTCollation collation;

  std::bitset<std::numeric_limits<unsigned char>::digits> flag;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_RC_ATTR_TYPEINFO_H_
