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

#include "log_ctl.h"

#include "core/transaction.h"
#include "system/configuration.h"
#include "system/file_out.h"
#include "system/tianmu_system.h"

namespace Tianmu {

namespace logger {
// NOTICE: the order must be align with enum LogCtl_Level
static const char *level_str[] = {"", "LogCtl_Level::FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"};

static const char *get_level_str(LogCtl_Level level) { return level_str[static_cast<int>(level)]; }
}  // namespace logger

static system::Channel tianmulog(true);

void log_setup(const std::string &log_file) { tianmulog.addOutput(new system::FileOut(log_file)); }

namespace utils {
logger::LogCtl_Level LogCtl::GetSessionLevel() {
  if (current_txn_ == nullptr) {
    tianmulog << system::lock << "ERROR" << __PRETTY_FUNCTION__ << " current_txn_ invalid" << system::unlock;
    return logger::LogCtl_Level::DISABLED;
  }
  return static_cast<logger::LogCtl_Level>(current_txn_->DebugLevel());
}

logger::LogCtl_Level LogCtl::GetGlobalLevel() {
  return static_cast<logger::LogCtl_Level>(tianmu_sysvar_global_debug_level);
}

bool LogCtl::LogEnabled(logger::LogCtl_Level level) {
  // there is issue with TLS in thread pool so we disable session log level until
  // a fix is available.
  return tianmu_sysvar_global_debug_level >= static_cast<unsigned int>(level);
}

void LogCtl::LogMsg(logger::LogCtl_Level level, const char *file, int line, const char *format, ...) {
  // NOTICE: log msg will be truncate to MAX_LOG_LEN
  if (LogEnabled(level)) {
    va_list args;
    va_start(args, format);
    char buff[MAX_LOG_LEN];
    auto length = std::vsnprintf(buff, sizeof(buff), format, args);
    if (static_cast<size_t>(length) >= MAX_LOG_LEN)
      length = MAX_LOG_LEN - 1;
    buff[length] = '\0';
    tianmulog << system::lock << "[" << logger::get_level_str(level) << "] [" << basename(file) << ":" << line
              << "] MSG: " << buff << system::unlock;
    va_end(args);
  }
}

void LogCtl::LogMsg(logger::LogCtl_Level level, const std::string &msg) {
  if (LogEnabled(level)) {
    tianmulog << system::lock << logger::get_level_str(level) << " MSG: " << msg << system::unlock;
  }
}

void LogCtl::LogMsg(logger::LogCtl_Level level, const char *file, int line, const std::string &msg) {
  if (LogEnabled(level)) {
    tianmulog << system::lock << "[" << logger::get_level_str(level) << "] [" << basename(file) << ":" << line
              << "] MSG: " << msg << system::unlock;
  }
}

void LogCtl::LogMsg(logger::LogCtl_Level level, std::stringstream &msg_ss) {
  if (LogEnabled(level)) {
    tianmulog << system::lock << logger::get_level_str(level) << " MSG: " << msg_ss.str() << system::unlock;
    // NOTICE: will reset stringstream here
    msg_ss.str("");
  }
}
}  // namespace utils
}  // namespace Tianmu

void tianmu_log(enum loglevel mysql_level, const char *buffer, [[maybe_unused]] size_t length) {
  using namespace Tianmu;
  logger::LogCtl_Level level;

  // mapping mysql log level to tianmu log level
  switch (mysql_level) {
    case ERROR_LEVEL:
      level = logger::LogCtl_Level::ERROR;
      break;
    case WARNING_LEVEL:
      level = logger::LogCtl_Level::WARN;
      break;
    case INFORMATION_LEVEL:
      level = logger::LogCtl_Level::WARN;
      break;
    default:
      level = logger::LogCtl_Level::ERROR;
      break;
  }
  utils::LogCtl::LogMsg(level, "MYSQL", 0, buffer);
}
