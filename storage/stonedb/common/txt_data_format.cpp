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

namespace stonedb {
namespace common {

TxtDataFormat::TxtDataFormat() : DataFormat("EDF::TRI_UNKNOWN", EDF::TRI_UNKNOWN) {}

TxtDataFormat::TxtDataFormat(std::string name, EDF edf) : DataFormat(name, edf) {}

std::unique_ptr<exporter::DataExporter> TxtDataFormat::CreateDataExporter(const system::IOParameters &iop) const {
  return std::unique_ptr<exporter::DataExporter>(new exporter::DEforTxt(iop));
}

//! Number of bytes taken by a value when written out,
//! or number of chars if collation not binary and CT::STRING or CT::VARCHAR
uint TxtDataFormat::StaticExtrnalSize(CT attrt, uint precision, int scale, const DTCollation *col) {
  if (attrt == CT::NUM) return 1 + precision + (scale ? 1 : 0) + (precision == (uint)scale ? 1 : 0);
  // because "-0.1" may be declared as DEC(1,1), so 18 decimal places may take
  // 21 characters
  if (attrt == CT::BYTE || attrt == CT::VARBYTE || attrt == CT::BIN) return precision * 2;
  if (attrt == CT::TIME_N || attrt == CT::DATETIME_N) return precision + scale + 1;
  if (attrt == CT::TIME) return 10;
  if (attrt == CT::DATETIME || attrt == CT::TIMESTAMP) return 19;
  if (attrt == CT::DATE)
    return 10;
  else if (attrt == CT::INT)
    return 11;
  else if (attrt == CT::BIGINT)
    return 20;
  else if (attrt == CT::BYTEINT || attrt == CT::YEAR)
    return 4;
  else if (attrt == CT::SMALLINT)
    return 6;
  else if (attrt == CT::MEDIUMINT)
    return 8;
  else if (attrt == CT::REAL)
    return 23;
  else if (attrt == CT::FLOAT)
    return 15;
  if (col == NULL)
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
}  // namespace stonedb
