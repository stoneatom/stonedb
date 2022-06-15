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
#ifndef STONEDB_COMMON_DATA_FORMAT_H_
#define STONEDB_COMMON_DATA_FORMAT_H_
#pragma once

#include <map>
#include <string>

#include "common/common_definitions.h"

namespace stonedb {

namespace system {
class IOParameters;
}  // namespace system

namespace exporter {
class DataExporter;
}  // namespace exporter

namespace common {

class DataFormat;
using DataFormatPtr = std::shared_ptr<DataFormat>;

// External Data Format
enum class EDF { TRI_UNKNOWN };

class DataFormat {
 protected:
  DataFormat(std::string name, EDF edf) : name(name), id(no_formats++), edf(edf) {}

 public:
  virtual ~DataFormat() = default;

 public:
  std::string GetName() const { return name; }
  int GetId() const { return id; }
  EDF GetEDF() const { return edf; }
  virtual std::unique_ptr<exporter::DataExporter> CreateDataExporter(const system::IOParameters &iop) const = 0;
  virtual bool CanExport() const { return true; }

 private:
  std::string name;
  int id;
  EDF edf;

 public:
  static DataFormatPtr GetDataFormat(const std::string &name);
  static DataFormatPtr GetDataFormat(int id);
  static DataFormatPtr GetDataFormat(EDF edf);
  static DataFormatPtr GetDataFormatbyEDF(EDF edf);
  static auto GetNoFormats() { return df_map.size(); }

 private:
  static int no_formats;
  static std::map<std::string, DataFormatPtr> df_map;
};

}  // namespace common
}  // namespace stonedb

#endif  // STONEDB_COMMON_DATA_FORMAT_H_
