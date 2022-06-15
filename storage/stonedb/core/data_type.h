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
#ifndef STONEDB_CORE_DATA_TYPE_H_
#define STONEDB_CORE_DATA_TYPE_H_
#pragma once

#include "common/common_definitions.h"

namespace stonedb {
namespace core {
struct ColumnType;

// Type of intermediate results of expression evaluation.
// ValueType::VT_FLOAT - floating-point real number (double)
// ValueType::VT_FIXED - fixed-point real (decimal) or integer, encoded as int64_t with
// base-10 scale ValueType::VT_STRING - pointer to character string, accompanied by string
// length ValueType::VT_DATETIME - date/time value encoded as int64_t, taken from
// types::RCDateTime representation
//               DataType::attrtype must be set to precisely denote which
//               date/time type is considered.

struct DataType final {
  enum class ValueType { VT_FLOAT, VT_FIXED, VT_STRING, VT_DATETIME, VT_NOTKNOWN = 255 };
  ValueType valtype;
  common::CT attrtype;  // storage type of SDB (only for source columns;
                        // otherwise common::CT::UNK)
  int fixscale;         // base-10 scale of ValueType::VT_FIXED (no. of decimal digits after
                        // comma)
  int64_t fixmax;       // maximum _absolute_ value possible (upper bound) of ValueType::VT_FIXED;
  // fixmax = -1  when upper bound is unknown or doesn't fit in int64_t;
  // precision of a decimal = QuickMath::precision10(fixmax)
  DTCollation collation;  // character set of ValueType::VT_STRING + coercibility
  int precision;

  DataType() {
    valtype = ValueType::VT_NOTKNOWN;
    attrtype = common::CT::UNK;
    fixscale = 0;
    fixmax = -1;
    collation = DTCollation();
    precision = -1;
  }
  DataType(common::CT atype, int prec = 0, int scale = 0, DTCollation collation = DTCollation());
  DataType &operator=(const ColumnType &ct);

  bool IsKnown() const { return valtype != ValueType::VT_NOTKNOWN; }
  bool IsFixed() const { return valtype == ValueType::VT_FIXED; }
  bool IsFloat() const { return valtype == ValueType::VT_FLOAT; }
  bool IsInt() const { return IsFixed() && (fixscale == 0); }
  bool IsString() const { return valtype == ValueType::VT_STRING; }
  bool IsDateTime() const { return valtype == ValueType::VT_DATETIME; }
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_DATA_TYPE_H_
