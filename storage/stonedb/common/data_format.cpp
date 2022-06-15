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

#include "common/data_format.h"

#include <algorithm>
#include <string>

#include <boost/algorithm/string.hpp>

#include "common/txt_data_format.h"

namespace stonedb {
namespace common {

int DataFormat::no_formats = 0;

std::map<std::string, DataFormatPtr> DataFormat::df_map = {{"EDF::TRI_UNKNOWN", std::make_shared<TxtDataFormat>()}};

DataFormatPtr DataFormat::GetDataFormat(const std::string &name) {
  auto it = df_map.find(boost::trim_copy(boost::to_upper_copy(name)));
  return it != df_map.end() ? it->second : DataFormatPtr();
}

DataFormatPtr DataFormat::GetDataFormat(int id) {
  auto it = std::find_if(df_map.begin(), df_map.end(), [id](const auto &p) -> bool { return p.second->GetId() == id; });
  return it != df_map.end() ? it->second : DataFormatPtr();
}

DataFormatPtr DataFormat::GetDataFormat(EDF edf) { return GetDataFormatbyEDF(edf); }

DataFormatPtr DataFormat::GetDataFormatbyEDF(EDF edf) {
  auto it =
      std::find_if(df_map.begin(), df_map.end(), [edf](const auto &p) -> bool { return p.second->GetEDF() == edf; });
  return it != df_map.end() ? it->second : DataFormatPtr();
}

}  // namespace common
}  // namespace stonedb
