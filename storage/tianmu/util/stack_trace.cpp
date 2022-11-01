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

#include <cxxabi.h>
#include <execinfo.h>
#include <cstdlib>

#include "stack_trace.h"

namespace Tianmu {
namespace utils {
// this function may modify the string pointed by name!!!
static inline bool DemangelSymbol(char *name, std::string &result) {
  // return the original mangled name if having problem demangling it.
  result = name;

  char *p = name;

  while ((*p != '\0') && (*p != '(')) p++;

  if (*p == '\0')
    return false;

  char *begin = ++p;

  while ((*p != '\0') && (*p != '+')) p++;

  if (*p == '\0')
    return false;

  char *end = p++;
  *end = '\0';
  int status;
  char *func = abi::__cxa_demangle(begin, nullptr, nullptr, &status);
  if (!func)
    return false;

  *end = '+';
  *begin = '\0';
  result = name;
  result += func;
  result += end;
  std::free(func);
  return true;
}

bool GetStackTrace(std::vector<std::string> &v, int skip_level, bool demangle) {
  static const int SIZE = 100;

  void *buffer[SIZE];
  int n = backtrace(buffer, SIZE);

  char **str;

  str = backtrace_symbols(buffer, n);
  if (str == nullptr) {
    return false;
  }

  for (int i = skip_level; i < n; i++) {
    std::string s;
    if (demangle)
      DemangelSymbol(str[i], s);
    else
      s = str[i];
    v.push_back(s);
  }

  std::free(str);

  return true;
}
}  // namespace utils
}  // namespace Tianmu
