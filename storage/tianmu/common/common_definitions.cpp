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

namespace Tianmu {
namespace common {

void PushWarning(THD *thd, Sql_condition::enum_severity_level level, uint code, const char *msg) {
  static std::mutex ibm;

  std::scoped_lock guard(ibm);
  push_warning(thd, level, code, msg);
}

// Here for args int `type`, we do not use enum directly as it'll caused compiling failed for the dependent package.
void PushWarningIfOutOfRange(THD *thd, std::string col_name, int64_t v, int type, bool unsigned_flag) {
  // below `0` is for min unsigned value.
  switch (type) {
    case 1: {  // MYSQL_TYPE_TINY
      if (unsigned_flag && (static_cast<uint64_t>(v) > TIANMU_TINYINT_MAX)) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, 0, TIANMU_TINYINT_MAX, unsigned_flag, v).c_str());
      } else if (v > TIANMU_TINYINT_MAX || v < TIANMU_TINYINT_MIN) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, TIANMU_TINYINT_MIN, TIANMU_TINYINT_MAX, unsigned_flag, v).c_str());
      }
    } break;
    case 2: {  // MYSQL_TYPE_SHORT
      if (unsigned_flag && (static_cast<uint64_t>(v) > TIANMU_SMALLINT_MAX)) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, 0, TIANMU_SMALLINT_MAX, unsigned_flag, v).c_str());
      } else if (v > TIANMU_SMALLINT_MAX || v < TIANMU_SMALLINT_MIN) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, TIANMU_SMALLINT_MIN, TIANMU_SMALLINT_MAX, unsigned_flag, v).c_str());
      };
    } break;
    case 9: {  // MYSQL_TYPE_INT24
      if (unsigned_flag && (static_cast<uint64_t>(v) > TIANMU_MEDIUMINT_MAX)) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, 0, TIANMU_MEDIUMINT_MAX, unsigned_flag, v).c_str());
      } else if (v > TIANMU_MEDIUMINT_MAX || v < TIANMU_MEDIUMINT_MIN) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, TIANMU_MEDIUMINT_MIN, TIANMU_MEDIUMINT_MAX, unsigned_flag, v).c_str());
      }
    } break;
    case 3: {  // MYSQL_TYPE_LONG
      if (unsigned_flag && (static_cast<uint64_t>(v) > TIANMU_INT_MAX)) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, 0, TIANMU_INT_MAX, unsigned_flag, v).c_str());
      } else if (v > TIANMU_INT_MAX || v < TIANMU_INT_MIN) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, TIANMU_INT_MIN, TIANMU_INT_MAX, unsigned_flag, v).c_str());
      }
    } break;
    case 8: {  // MYSQL_TYPE_LONGLONG
      if (unsigned_flag && (static_cast<uint64_t>(v) > TIANMU_BIGINT_MAX)) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, 0, TIANMU_BIGINT_MAX, unsigned_flag, v).c_str());
      } else if (v > TIANMU_BIGINT_MAX || v < TIANMU_BIGINT_MIN) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, TIANMU_BIGINT_MIN, TIANMU_BIGINT_MAX, unsigned_flag, v).c_str());
      }
    } break;
    default:  // For type which is not integer, nothing to do
      break;
  }
}

// Msg: Out of range[min, max] for column 'col' value: 123
// Just for unsigned type check in tianmu engine.
std::string getErrMsg(std::string col_name, int64_t min, int64_t max, bool unsigned_flag, int64_t v) {
  std::string str = "Out of range[";
  str += std::to_string(min);
  str += ", ";
  str += std::to_string(max);
  str += "] for column '";
  str += col_name;
  str += "' value: ";
  str += unsigned_flag ? std::to_string(static_cast<uint64_t>(v)) : std::to_string(v);
  return str;
}

std::string TX_ID::ToString() const {
  std::stringstream ss;
  ss << std::setfill('0') << std::setw(sizeof(v) * 2) << std::hex << v;
  return ss.str();
}

}  // namespace common
}  // namespace Tianmu
