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
#ifndef TIANMU_TYPES_VALUE_PARSER4TXT_H_
#define TIANMU_TYPES_VALUE_PARSER4TXT_H_
#pragma once

#include "common/assert.h"
#include "common/exception.h"
#include "core/tianmu_attr_typeinfo.h"
#include "types/tianmu_data_types.h"

namespace Tianmu {
namespace types {
class ValueParserForText {
 public:
  ValueParserForText(){};
  virtual ~ValueParserForText(){};

  static auto GetParsingFuntion(const core::AttributeTypeInfo &at)
      -> std::function<common::ErrorCode(BString const &, int64_t &)> {
    switch (at.Type()) {
      case common::ColumnType::NUM:
        return std::bind<common::ErrorCode>(&ParseDecimal, std::placeholders::_1, std::placeholders::_2, at.Precision(),
                                            at.Scale());
      case common::ColumnType::REAL:
      case common::ColumnType::FLOAT:
      case common::ColumnType::BYTEINT:
      case common::ColumnType::SMALLINT:
      case common::ColumnType::MEDIUMINT:
      case common::ColumnType::INT:
        return std::bind<common::ErrorCode>(&ParseNumeric, std::placeholders::_1, std::placeholders::_2, at.Type(),
                                            at.GetUnsignedFlag());
      case common::ColumnType::BIGINT:
        return &ParseBigIntAdapter;
      case common::ColumnType::BIT:
        return &ParseBitAdapter;
      case common::ColumnType::DATE:
      case common::ColumnType::TIME:
      case common::ColumnType::YEAR:
      case common::ColumnType::DATETIME:
      case common::ColumnType::TIMESTAMP:
        return std::bind<common::ErrorCode>(&ParseDateTimeAdapter, std::placeholders::_1, std::placeholders::_2,
                                            at.Type());
      default:
        TIANMU_ERROR("type not supported:" + std::to_string(static_cast<unsigned char>(at.Type())));
        break;
    }
    return nullptr;
  }

  static common::ErrorCode ParseNumeric(BString const &tianmu_s, int64_t &out, common::ColumnType at,
                                        bool unsigned_flag);
  static common::ErrorCode ParseBigIntAdapter(const BString &tianmu_s, int64_t &out);
  static common::ErrorCode ParseBitAdapter(const BString &tianmu_s, int64_t &out);
  static common::ErrorCode ParseDecimal(BString const &tianmu_s, int64_t &out, short precision, short scale);
  static common::ErrorCode ParseDateTimeAdapter(BString const &tianmu_s, int64_t &out, common::ColumnType at);

  static common::ErrorCode Parse(const BString &tianmu_s, TianmuNum &tianmu_n, common::ColumnType at,
                                 bool unsigned_flag = false);
  static common::ErrorCode ParseNum(const BString &tianmu_s, TianmuNum &tianmu_n, short scale);
  static common::ErrorCode ParseBigInt(const BString &tianmu_s, TianmuNum &out);
  static common::ErrorCode ParseReal(const BString &tianmu_s, TianmuNum &tianmu_n, common::ColumnType at);
  static common::ErrorCode ParseDateTime(const BString &tianmu_s, TianmuDateTime &rcv, common::ColumnType at);

 private:
  static common::ErrorCode ParseDateTimeOrTimestamp(const BString &tianmu_s, TianmuDateTime &rcv,
                                                    common::ColumnType at);
  static common::ErrorCode ParseTime(const BString &tianmu_s, TianmuDateTime &rcv);
  static common::ErrorCode ParseDate(const BString &tianmu_s, TianmuDateTime &rcv);
  static common::ErrorCode ParseYear(const BString &tianmu_s, TianmuDateTime &rcv);
};

}  // namespace types
}  // namespace Tianmu

#endif  // TIANMU_TYPES_VALUE_PARSER4TXT_H_
