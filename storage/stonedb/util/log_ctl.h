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
#ifndef STONEDB_UTIL_LOG_CTL_H_
#define STONEDB_UTIL_LOG_CTL_H_
#pragma once

#include <string>

namespace stonedb {

namespace logger {
enum class LogCtl_Level { DISABLED = 0, FATAL = 1, ERROR = 2, WARN = 3, INFO = 4, DEBUG = 5, TRACE = 6 };
}  // namespace logger
namespace utils {
// a class help to check debug switch and print debug msg
class LogCtl {
 public:
  static logger::LogCtl_Level GetSessionLevel();
  static logger::LogCtl_Level GetGlobalLevel();

  static bool LogEnabled(logger::LogCtl_Level level);

  static void LogMsg(logger::LogCtl_Level level, const char *format, ...);
  static void LogMsg(logger::LogCtl_Level level, const char *file, int line, const char *format, ...);
  static void LogMsg(logger::LogCtl_Level level, const std::string &msg);
  static void LogMsg(logger::LogCtl_Level level, const char *file, int line, const std::string &msg);
  static void LogMsg(logger::LogCtl_Level level, std::stringstream &msg_ss);
};
}  // namespace utils

constexpr size_t MAX_LOG_LEN = 4096;

// StoneDB logger API
#define STONEDB_LOG(_level, ...) stonedb::utils::LogCtl::LogMsg(logger::_level, __FILE__, __LINE__, ##__VA_ARGS__)

#define STONEDB_LOGCHECK(_level) (stonedb::utils::LogCtl::LogEnabled(logger::_level))

#define LOG_MSG(_level, msg) stonedb::utils::LogCtl::LogMsg(logger::_level, __FILE__, __LINE__, msg);

void log_setup(const std::string &log_file);
}  // namespace stonedb

#endif  // STONEDB_UTIL_LOG_CTL_H_
