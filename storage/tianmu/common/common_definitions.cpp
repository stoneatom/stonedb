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

// min MYSQL_TYPE_LONG value in tianmu is -2147483647, -2147483648 is used for null_32, so here we do not test signed
// int min MYSQL_TYPE_LONGLONG value in tianmu is -9223372036854775806, -9223372036854775807 is used for null_64, so
// here we do not test signed int We use std::exception() but not common::Exceptions here as it will raise infos like
// "internal error in storage tianmu ..." on mysql client, this is not user friendly. Users has already get error msg
// from push_warning() func.
void PushWarningIfOutOfRange(THD *thd, std::string col_name, int64_t v, int type, bool unsigned_flag) {
  // below `0` is for min unsigned value.
  switch (type) {
    case MYSQL_TYPE_TINY: {
      if (unsigned_flag) {
        if (static_cast<uint64_t>(v) > UINT_MAX8) {
          PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                      getErrMsg(col_name, 0, UINT_MAX8, unsigned_flag, v).c_str());
          throw std::exception();
        }
      } else if (v > TIANMU_TINYINT_MAX || v < TIANMU_TINYINT_MIN) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, TIANMU_TINYINT_MIN, TIANMU_TINYINT_MAX, unsigned_flag, v).c_str());
        throw std::exception();
      }
    } break;
    case MYSQL_TYPE_SHORT: {
      if (unsigned_flag) {
        if (static_cast<uint64_t>(v) > UINT_MAX16) {
          PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                      getErrMsg(col_name, 0, UINT_MAX16, unsigned_flag, v).c_str());
          throw std::exception();
        }
      } else if (v > TIANMU_SMALLINT_MAX || v < TIANMU_SMALLINT_MIN) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, TIANMU_SMALLINT_MIN, TIANMU_SMALLINT_MAX, unsigned_flag, v).c_str());
        throw std::exception();
      };
    } break;
    case MYSQL_TYPE_INT24: {
      if (unsigned_flag) {
        if (static_cast<uint64_t>(v) > UINT_MAX24) {
          PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                      getErrMsg(col_name, 0, UINT_MAX24, unsigned_flag, v).c_str());
          throw std::exception();
        }
      } else if (v > TIANMU_MEDIUMINT_MAX || v < TIANMU_MEDIUMINT_MIN) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, TIANMU_MEDIUMINT_MIN, TIANMU_MEDIUMINT_MAX, unsigned_flag, v).c_str());
        throw std::exception();
      }
    } break;
    case MYSQL_TYPE_LONG: {
      if (unsigned_flag) {
        if (static_cast<uint64_t>(v) > UINT_MAX32) {
          PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                      getErrMsg(col_name, 0, UINT_MAX32, unsigned_flag, v).c_str());
          throw std::exception();
        }
      } else if (v > TIANMU_INT_MAX || v < TIANMU_INT_MIN) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, TIANMU_INT_MIN, TIANMU_INT_MAX, unsigned_flag, v).c_str());
        throw std::exception();
      }
    } break;
    case MYSQL_TYPE_LONGLONG: {
      if (unsigned_flag && (static_cast<uint64_t>(v) > TIANMU_BIGINT_MAX)) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, 0, TIANMU_BIGINT_MAX, unsigned_flag, v).c_str());
        throw std::exception();
      } else if (v > TIANMU_BIGINT_MAX || v < TIANMU_BIGINT_MIN) {
        PushWarning(thd, Sql_condition::SL_WARNING, ER_WARN_DATA_OUT_OF_RANGE,
                    getErrMsg(col_name, TIANMU_BIGINT_MIN, TIANMU_BIGINT_MAX, unsigned_flag, v).c_str());
        throw std::exception();
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
