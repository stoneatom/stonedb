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
#ifndef TIANMU_COMMON_DATA_FORMAT_H_
#define TIANMU_COMMON_DATA_FORMAT_H_
#pragma once

#include <map>
#include <string>

#include "common/common_definitions.h"

namespace Tianmu {

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
  DataFormat(std::string name, EDF edf) : name_(name), id_(no_formats_++), edf_(edf) {}

 public:
  virtual ~DataFormat() = default;

 public:
  std::string GetName() const { return name_; }
  int GetId() const { return id_; }
  EDF GetEDF() const { return edf_; }
  virtual std::unique_ptr<exporter::DataExporter> CreateDataExporter(const system::IOParameters &iop) const = 0;
  virtual bool CanExport() const { return true; }

 private:
  std::string name_;
  int id_;
  EDF edf_;

 public:
  static DataFormatPtr GetDataFormat(const std::string &name);
  static DataFormatPtr GetDataFormat(int id);
  static DataFormatPtr GetDataFormat(EDF edf);
  static DataFormatPtr GetDataFormatbyEDF(EDF edf);
  static auto GetNoFormats() { return df_map_.size(); }

 private:
  static int no_formats_;
  static std::map<std::string, DataFormatPtr> df_map_;
};

}  // namespace common
}  // namespace Tianmu

#endif  // TIANMU_COMMON_DATA_FORMAT_H_
