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

#include "txt_data_format.h"

#include "exporter/data_exporter.h"
#include "exporter/data_exporter_txt.h"
#include "loader/parsing_strategy.h"
#include "types/value_parser4txt.h"

namespace Tianmu {
namespace common {

TxtDataFormat::TxtDataFormat() : DataFormat("EDF::TRI_UNKNOWN", EDF::TRI_UNKNOWN) {}

TxtDataFormat::TxtDataFormat(std::string name, EDF edf) : DataFormat(name, edf) {}

std::unique_ptr<exporter::DataExporter> TxtDataFormat::CreateDataExporter(const system::IOParameters &iop) const {
  return std::unique_ptr<exporter::DataExporter>(new exporter::DEforTxt(iop));
}

//! Number of bytes taken by a value when written out,
//! or number of chars if collation not binary and ColumnType::STRING or ColumnType::VARCHAR
uint TxtDataFormat::StaticExtrnalSize(ColumnType attrt, uint precision, int scale, const DTCollation *col) {
  if (attrt == ColumnType::NUM)
    // because "-0.1" may be declared as DEC(1,1), so 18 decimal places may take
    // 21 characters, "-0.xxxxx...", "-", "0", "." need extra 3 bytes, 21 = 3 + 18.
    return 1 + precision + (scale ? 1 : 0) + (precision == (uint)scale ? 1 : 0);
  if (attrt == ColumnType::BIT)  // unsigned value.
    return precision;            // e.g.: if precison = 4, bit value max display will be "1010", one byte one digit.
  if (attrt == ColumnType::BYTE || attrt == ColumnType::VARBYTE || attrt == ColumnType::BIN)
    return precision * 2;
  if (attrt == ColumnType::TIME_N || attrt == ColumnType::DATETIME_N)
    return precision + scale + 1;
  if (attrt == ColumnType::TIME)
    return 10;
  if (attrt == ColumnType::DATETIME || attrt == ColumnType::TIMESTAMP)
    return 19;
  if (attrt == ColumnType::DATE)
    return 10;
  else if (attrt == ColumnType::INT)
    return 11;  // -2,147,483,648 ~ 2,147,483,647, 1(signed) + 10(max digits)
  else if (attrt == ColumnType::BIGINT)
    return 20;  // -9,223,372,036,854,775,808 ~ 9,223,372,036,854,775,807, 1(signed) + 19(max digits)
  else if (attrt == ColumnType::BYTEINT || attrt == ColumnType::YEAR)
    return 4;
  else if (attrt == ColumnType::SMALLINT)
    return 6;
  else if (attrt == ColumnType::MEDIUMINT)
    return 8;
  else if (attrt == ColumnType::REAL)
    return 23;
  else if (attrt == ColumnType::FLOAT)
    return 15;
  if (col == nullptr)
    return precision;
  else
    return precision / col->collation->mbmaxlen;  // at creation, the charlen was multiplicated by
                                                  // mbmaxlen. This is not always true, as the
                                                  // 'charlen' is character length, not in bytes. E.g
                                                  // in UTF8, a chinese character is stored in 3
                                                  // bytes, while its 'charlen' is 1 (Check max_length
                                                  // defination inside Item_string, which assiciates
                                                  // precision for details).
                                                  //
                                                  // Therefore, the return value might be wrong if
                                                  // precision is divide by mbmaxlen under the case a
                                                  // Chinese character is passed.
                                                  //
                                                  // TempFix2017-1-13(see MysqlExpression.cpp) is
                                                  // provided to work around the problem for a quick
                                                  // patch. A follow up should be provided to avoid
                                                  // deviding mbmaxlen under that case.
                                                  //
}

}  // namespace common
}  // namespace Tianmu
