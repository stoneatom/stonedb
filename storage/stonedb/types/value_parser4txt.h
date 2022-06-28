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
#ifndef STONEDB_TYPES_VALUE_PARSER4TXT_H_
#define STONEDB_TYPES_VALUE_PARSER4TXT_H_
#pragma once

#include "common/assert.h"
#include "common/exception.h"
#include "core/rc_attr_typeinfo.h"
#include "types/rc_data_types.h"

namespace stonedb {
namespace types {
class ValueParserForText {
 public:
  ValueParserForText(){};
  virtual ~ValueParserForText(){};

  static auto GetParsingFuntion(const core::AttributeTypeInfo &at)
      -> std::function<common::ErrorCode(BString const &, int64_t &)> {
    switch (at.Type()) {
      case common::CT::NUM:
        return std::bind<common::ErrorCode>(&ParseDecimal, std::placeholders::_1, std::placeholders::_2, at.Precision(),
                                            at.Scale());
      case common::CT::REAL:
      case common::CT::FLOAT:
      case common::CT::BYTEINT:
      case common::CT::SMALLINT:
      case common::CT::MEDIUMINT:
      case common::CT::INT:
        return std::bind<common::ErrorCode>(&ParseNumeric, std::placeholders::_1, std::placeholders::_2, at.Type());
      case common::CT::BIGINT:
        return &ParseBigIntAdapter;
      case common::CT::DATE:
      case common::CT::TIME:
      case common::CT::YEAR:
      case common::CT::DATETIME:
      case common::CT::TIMESTAMP:
        return std::bind<common::ErrorCode>(&ParseDateTimeAdapter, std::placeholders::_1, std::placeholders::_2,
                                            at.Type());
      default:
        STONEDB_ERROR("type not supported:" + std::to_string(static_cast<unsigned char>(at.Type())));
        break;
    }
    return NULL;
  }

  static common::ErrorCode ParseNumeric(BString const &rcs, int64_t &out, common::CT at);
  static common::ErrorCode ParseBigIntAdapter(const BString &rcs, int64_t &out);
  static common::ErrorCode ParseDecimal(BString const &rcs, int64_t &out, short precision, short scale);
  static common::ErrorCode ParseDateTimeAdapter(BString const &rcs, int64_t &out, common::CT at);

  static common::ErrorCode Parse(const BString &rcs, RCNum &rcn, common::CT at);
  static common::ErrorCode ParseNum(const BString &rcs, RCNum &rcn, short scale);
  static common::ErrorCode ParseBigInt(const BString &rcs, RCNum &out);
  static common::ErrorCode ParseReal(const BString &rcbs, RCNum &rcn, common::CT at);
  static common::ErrorCode ParseDateTime(const BString &rcs, RCDateTime &rcv, common::CT at);

 private:
  static common::ErrorCode ParseDateTimeOrTimestamp(const BString &rcs, RCDateTime &rcv, common::CT at);
  static common::ErrorCode ParseTime(const BString &rcs, RCDateTime &rcv);
  static common::ErrorCode ParseDate(const BString &rcs, RCDateTime &rcv);
  static common::ErrorCode ParseYear(const BString &rcs, RCDateTime &rcv);
};
}  // namespace types
}  // namespace stonedb

#endif  // STONEDB_TYPES_VALUE_PARSER4TXT_H_
