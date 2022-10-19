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

#include "common/exception.h"

#include <cstring>

#include "util/fs.h"
#include "util/log_ctl.h"
#include "util/stack_trace.h"

namespace Tianmu {
namespace common {

Exception::Exception(std::string const &msg) : std::runtime_error(msg) {
  exception_msg_ = msg;
  stack_trace_ = "\n";
  stack_trace_ += "STACK TRACE BEGIN\n";

  std::vector<std::string> trace;
  utils::GetStackTrace(trace, 2);
  for (auto &it : trace) {
    stack_trace_ += "        ";
    stack_trace_ += it;
    stack_trace_ += "\n";
  }
  stack_trace_ += "STACK TRACE END\n";
  TIANMU_LOG(LogCtl_Level::WARN, "Exception: %s.\n%s", msg.c_str(), stack_trace_.c_str());
}

AssertException::AssertException(const char *cond, const char *file, int line, const std::string &msg)
    : Exception(std::string("assert failed on ") + cond + " at " + fs::path(file).filename().string() + ":" +
                std::to_string(line) + ", msg: [" + msg + "]") {}

}  // namespace common
}  // namespace Tianmu
