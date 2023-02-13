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

namespace Tianmu {
namespace core {
#define MAX(a, b) ((a) > (b) ? (a) : (b))

// TODO: why not pass struct ColumnType& in column_type.h?
DataType::DataType(common::ColumnType atype, int prec, int scale, DTCollation collation, bool unsigned_flag)
    : precision(prec) {
  valtype = ValueType::VT_NOTKNOWN;
  attrtype = atype;
  fixscale = scale;
  fixmax = -1;
  this->collation = collation;
  unsigned_flag_ = unsigned_flag;

  // UINT_xxx from include/my_global.h
  switch (attrtype) {
    case common::ColumnType::INT:
      valtype = ValueType::VT_FIXED;
      fixmax = unsigned_flag ? UINT_MAX32 : MAX(std::numeric_limits<int>::max(), -TIANMU_INT_MIN);
      break;
    case common::ColumnType::BIGINT:
      valtype = ValueType::VT_FIXED;
      fixmax = unsigned_flag ? common::TIANMU_BIGINT_UNSIGNED_MAX
                             : MAX(common::TIANMU_BIGINT_MAX, -common::TIANMU_BIGINT_MIN);
      break;
    case common::ColumnType::MEDIUMINT:
      valtype = ValueType::VT_FIXED;
      fixmax = unsigned_flag ? UINT_MAX24 : MAX(TIANMU_MEDIUMINT_MAX, -TIANMU_MEDIUMINT_MIN);
      break;
    case common::ColumnType::SMALLINT:
      valtype = ValueType::VT_FIXED;
      fixmax = unsigned_flag ? UINT_MAX16 : MAX(TIANMU_SMALLINT_MAX, -TIANMU_SMALLINT_MIN);
      break;
    case common::ColumnType::BYTEINT:
      valtype = ValueType::VT_FIXED;
      fixmax = unsigned_flag ? UINT_MAX8 : MAX(TIANMU_TINYINT_MAX, -TIANMU_TINYINT_MIN);
      break;
    case common::ColumnType::BIT:
      valtype = ValueType::VT_FIXED;
      fixmax = MAX(common::TIANMU_BIGINT_MAX,
                   -common::TIANMU_BIGINT_MIN);  // TODO(fix max value with common::TIANMU_BIT_MAX_PREC)
      break;

    case common::ColumnType::NUM:
      DEBUG_ASSERT((prec > 0) && (prec <= 19) && (fixscale >= 0));
      if (prec == 19)
        fixmax = common::PLUS_INF_64;
      else
        fixmax = QuickMath::power10i(prec) - 1;
      valtype = ValueType::VT_FIXED;
      break;

    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      valtype = ValueType::VT_FLOAT;
      break;

    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
      valtype = ValueType::VT_STRING;
      break;

    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::YEAR:
      valtype = ValueType::VT_DATETIME;
      break;

    case common::ColumnType::DATETIME_N:
    case common::ColumnType::TIMESTAMP_N:
    case common::ColumnType::TIME_N:
        // NOT IMPLEMENTED YET
        ;
    default:
      // NOT IMPLEMENTED YET
      break;
  }
}

DataType &DataType::operator=(const ColumnType &ct) {
  *this = DataType();
  if (!ct.IsKnown())
    return *this;

  *this = DataType(ct.GetTypeName(), ct.GetPrecision(), ct.GetScale(), ct.GetCollation(), ct.GetUnsigned());

  if (valtype == ValueType::VT_NOTKNOWN) {
    char s[128];
    std::sprintf(s, "ColumnType (common::ColumnType #%d) is not convertible to a DataType", (int)ct.GetTypeName());
    throw common::Exception(s);
  }

  return *this;
}
}  // namespace core
}  // namespace Tianmu
