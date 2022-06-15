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

#include "data_type.h"

#include "common/assert.h"
#include "core/column_type.h"
#include "core/quick_math.h"

namespace stonedb {
namespace core {
#define MAX(a, b) ((a) > (b) ? (a) : (b))

DataType::DataType(common::CT atype, int prec, int scale, DTCollation collation) : precision(prec) {
  valtype = ValueType::VT_NOTKNOWN;
  attrtype = atype;
  fixscale = scale;
  fixmax = -1;
  this->collation = collation;

  switch (attrtype) {
    case common::CT::INT:
      valtype = ValueType::VT_FIXED;
      fixmax = MAX(std::numeric_limits<int>::max(), -SDB_INT_MIN);
      break;
    case common::CT::BIGINT:
      valtype = ValueType::VT_FIXED;
      fixmax = MAX(common::SDB_BIGINT_MAX, -common::SDB_BIGINT_MIN);
      break;
    case common::CT::MEDIUMINT:
      valtype = ValueType::VT_FIXED;
      fixmax = MAX(SDB_MEDIUMINT_MAX, -SDB_MEDIUMINT_MIN);
      break;
    case common::CT::SMALLINT:
      valtype = ValueType::VT_FIXED;
      fixmax = MAX(SDB_SMALLINT_MAX, -SDB_SMALLINT_MIN);
      break;
    case common::CT::BYTEINT:
      valtype = ValueType::VT_FIXED;
      fixmax = MAX(SDB_TINYINT_MAX, -SDB_TINYINT_MIN);
      break;

    case common::CT::NUM:
      DEBUG_ASSERT((prec > 0) && (prec <= 19) && (fixscale >= 0));
      if (prec == 19)
        fixmax = common::PLUS_INF_64;
      else
        fixmax = QuickMath::power10i(prec) - 1;
      valtype = ValueType::VT_FIXED;
      break;

    case common::CT::REAL:
    case common::CT::FLOAT:
      valtype = ValueType::VT_FLOAT;
      break;

    case common::CT::STRING:
    case common::CT::VARCHAR:
    case common::CT::BIN:
    case common::CT::BYTE:
    case common::CT::VARBYTE:
    case common::CT::LONGTEXT:
      valtype = ValueType::VT_STRING;
      break;

    case common::CT::DATETIME:
    case common::CT::TIMESTAMP:
    case common::CT::TIME:
    case common::CT::DATE:
    case common::CT::YEAR:
      valtype = ValueType::VT_DATETIME;
      break;

    case common::CT::DATETIME_N:
    case common::CT::TIMESTAMP_N:
    case common::CT::TIME_N:
        // NOT IMPLEMENTED YET
        ;
    default:
      // NOT IMPLEMENTED YET
      break;
  }
}

DataType &DataType::operator=(const ColumnType &ct) {
  *this = DataType();
  if (!ct.IsKnown()) return *this;

  *this = DataType(ct.GetTypeName(), ct.GetPrecision(), ct.GetScale(), ct.GetCollation());

  if (valtype == ValueType::VT_NOTKNOWN) {
    char s[128];
    std::sprintf(s, "ColumnType (common::CT #%d) is not convertible to a DataType", (int)ct.GetTypeName());
    throw common::Exception(s);
  }

  return *this;
}
}  // namespace core
}  // namespace stonedb
