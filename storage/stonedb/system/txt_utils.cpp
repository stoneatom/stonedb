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

#include "txt_utils.h"

#include "common/assert.h"

namespace stonedb {
namespace system {
inline char Convert2Hex(int index) {
  static const char tab[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  return tab[index];
};

void Convert2Hex(const unsigned char *src, int src_size, char *dest, int dest_size, bool zero_term) {
  DEBUG_ASSERT(dest_size > 1 && "At least 2 bytes needed for hexadecimal representation");

  dest_size = std::min(src_size * 2 + 1, dest_size);
  if (zero_term) {
    if (dest_size % 2)
      dest_size--;
    else
      dest_size -= 2;
  }

  for (int i = 0; i < dest_size / 2; i++) {
    dest[2 * i] = Convert2Hex((src[i]) / 16);
    dest[2 * i + 1] = Convert2Hex((src[i]) % 16);
  };
  if (zero_term) dest[dest_size] = '\0';
};

bool EatDTSeparators(char *&ptr, int &len) {
  bool vs = false;
  while (len > 0 && CanBeDTSeparator(*ptr)) {
    len--;
    ptr++;
    vs = true;
  }
  return vs;
}

common::ErrorCode EatInt(char *&ptr, int &len, int &out_value) {
  out_value = 0;
  int64_t out_v_tmp = 0;
  common::ErrorCode rc = common::ErrorCode::FAILED;
  short sign = 1;

  if (len > 0 && *ptr == '+') {  // WINPORT extension
    ptr++;
  }

  if (len > 0 && *ptr == '-') {
    sign = -1;
    ptr++;
  }

  while (len > 0 && isdigit((unsigned char)*ptr)) {
    if (rc != common::ErrorCode::OUT_OF_RANGE) {
      out_v_tmp = out_v_tmp * 10 + ((ushort)*ptr - '0');
      if (out_v_tmp * sign > INT_MAX) {
        out_v_tmp = INT_MAX;
        rc = common::ErrorCode::OUT_OF_RANGE;
      } else if (out_v_tmp * sign < INT_MIN) {
        out_v_tmp = INT_MIN;
        rc = common::ErrorCode::OUT_OF_RANGE;
      } else
        rc = common::ErrorCode::SUCCESS;
    }
    len--;
    ptr++;
  }

  if (rc == common::ErrorCode::SUCCESS) out_v_tmp *= sign;
  out_value = (int)out_v_tmp;
  return rc;
}

common::ErrorCode EatUInt(char *&ptr, int &len, uint &out_value) {
  if (len > 0 && *ptr == '+') {
    ptr++;
    len--;
  }

  out_value = 0;
  uint out_v_tmp = 0;
  common::ErrorCode rc = common::ErrorCode::FAILED;
  while (len > 0 && isdigit((unsigned char)*ptr)) {
    out_v_tmp = out_value;
    out_value = out_value * 10 + ((ushort)*ptr - '0');
    if (rc != common::ErrorCode::OUT_OF_RANGE) {
      if (out_v_tmp > out_value)
        rc = common::ErrorCode::OUT_OF_RANGE;
      else
        rc = common::ErrorCode::SUCCESS;
    }
    len--;
    ptr++;
  }
  return rc;
}

common::ErrorCode EatUInt64(char *&ptr, int &len, uint64_t &out_value) {
  if (len > 0 && *ptr == '+') {
    ptr++;
    len--;
  }

  out_value = 0;
  uint64_t out_v_tmp = 0;
  common::ErrorCode rc = common::ErrorCode::FAILED;
  while (len > 0 && isdigit((unsigned char)*ptr)) {
    out_v_tmp = out_value;
    out_value = out_value * 10 + ((ushort)*ptr - '0');
    if (rc != common::ErrorCode::OUT_OF_RANGE) {
      if (out_v_tmp > out_value)
        rc = common::ErrorCode::OUT_OF_RANGE;
      else
        rc = common::ErrorCode::SUCCESS;
    }
    len--;
    ptr++;
  }
  return rc;
}

bool CanBeDTSeparator(char c) {
  if (c == '-' || c == ':' || c == '~' || c == '!' || c == '@' || c == '#' || c == '$' || c == '%' || c == '^' ||
      c == '&' || c == '*' || c == '(' || c == ')' || c == '{' || c == '}' || c == '[' || c == ']' || c == '/' ||
      c == '?' || c == '.' || c == ',' || c == '<' || c == '>' || c == '_' || c == '=' || c == '+' || c == '|' ||
      c == '`')
    return true;
  return false;
}
}  // namespace system
}  // namespace stonedb
