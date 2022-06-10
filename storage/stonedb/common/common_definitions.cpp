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

#include "common_definitions.h"

#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>

namespace stonedb {
namespace common {

void PushWarning(THD *thd, Sql_condition::enum_warning_level level, uint code, const char *msg) {
  static std::mutex ibm;

  std::scoped_lock guard(ibm);
  push_warning(thd, level, code, msg);
}

std::string TX_ID::ToString() const {
  std::stringstream ss;
  ss << std::setfill('0') << std::setw(sizeof(v) * 2) << std::hex << v;
  return ss.str();
}

}  // namespace common
}  // namespace stonedb
