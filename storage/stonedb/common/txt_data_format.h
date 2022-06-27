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
#ifndef STONEDB_COMMON_TXT_DATA_FORMAT_H_
#define STONEDB_COMMON_TXT_DATA_FORMAT_H_
#pragma once

#include "common/data_format.h"

namespace stonedb {
namespace common {

class TxtDataFormat final : public DataFormat {
 public:
  TxtDataFormat();
  TxtDataFormat(std::string name, EDF edf);
  std::unique_ptr<exporter::DataExporter> CreateDataExporter(const system::IOParameters &iop) const override;

 public:
  static uint StaticExtrnalSize(CT attrt, uint precision, int scale, const DTCollation *col);
};

}  // namespace common
}  // namespace stonedb

#endif  // STONEDB_COMMON_TXT_DATA_FORMAT_H_
