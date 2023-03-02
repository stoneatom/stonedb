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

#include "mm/memory_statistics.h"

namespace Tianmu {

MemoryStatisticsOS *MemoryStatisticsOS::instance = nullptr;

void SplitString(const std::string &str, const char pattern, std::vector<std::string> &res) {
  std::stringstream input(str);
  std::string temp;
  while (getline(input, temp, pattern)) {
    res.push_back(temp);
  }
}

}  // namespace Tianmu
