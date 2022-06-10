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

#include "types/text_stat.h"

namespace stonedb {
namespace types {
TextStat::TextStat() {
  encode_table.fill(255);
  len_table.fill(0);
  chars_found.fill(0);
}

bool TextStat::AddString(const BString &rcbs) {
  size_t len = rcbs.size();
  if (len > TAB_SIZE) {
    valid = false;
    return false;
  }
  for (size_t i = 0; i < len; i++) {
    if (rcbs[i] == 0) {
      valid = false;
      return false;
    }
    chars_found[256 * i + uchar(rcbs[i])] = 1;
  }
  if (len < TAB_SIZE) chars_found[256 * len] = 1;  // value of len n puts 0 on position n (starting with 0)
  if (len > max_string_size) max_string_size = len;
  return true;
}

bool TextStat::AddChar(uchar v, int pos)  // return false if out of range
{
  if (static_cast<size_t>(pos) >= TAB_SIZE || v == 0) {
    valid = false;
    return false;
  }
  chars_found[256 * pos + v] = 1;
  if (static_cast<size_t>(pos + 1) > max_string_size) max_string_size = pos + 1;
  return true;
}

void TextStat::AddLen(int pos)  // value of len n puts 0 on position n (starting with 0)
{
  if (static_cast<size_t>(pos) == TAB_SIZE) return;
  DEBUG_ASSERT(static_cast<size_t>(pos) < TAB_SIZE);
  chars_found[256 * pos] = 1;
  if (static_cast<size_t>(pos) > max_string_size) max_string_size = pos;
}

bool TextStat::CheckIfCreatePossible()  // return false if cannot create
                                        // encoding (too wide), do not actually
                                        // create anything
{
  if (chars_found_for_decoding) {
    valid = false;
    return false;
  }
  int total_len = 0;
  for (size_t pos = 0; pos < max_string_size; pos++) {
    int loc_len_table = 0;
    for (size_t i = 0; i < 256; i++) {
      if (chars_found[256 * pos + i] == 1) {
        if (loc_len_table == 255) {
          valid = false;
          return false;  // too many characters
        }
        loc_len_table++;
      }
    }
    if (loc_len_table > 1) total_len += core::GetBitLen(uint(loc_len_table) - 1);
    if (total_len > 63) {
      valid = false;
      return false;
    }
  }
  return true;
}

bool TextStat::CreateEncoding() {
  if (chars_found_for_decoding) {  // chars_found is already used as a decoding
                                   // structure - cannot create decoding twice
                                   // with a decoding in between
    valid = false;
    return false;
  }
  int total_len = 0;
  max_code = 0;
  for (size_t pos = 0; pos < max_string_size; pos++) {
    len_table[pos] = 0;
    for (int i = 0; i < 256; i++) {
      if (chars_found[256 * pos + i] == 1) {
        encode_table[i + 256 * pos] = len_table[pos];  // Encoding a string: Code(character c) =
                                                       // encode_table[c][pos]
        // Note: initial value of 255 means "illegal character", so we must not
        // allow to use such code value
        if (len_table[pos] == 255) {
          valid = false;
          return false;  // too many characters
        }
        len_table[pos]++;
      }
    }
    // translate lengths into binary sizes
    if (len_table[pos] == 1)
      len_table[pos] = 0;
    else if (len_table[pos] > 1) {
      uint max_charcode = len_table[pos] - 1;
      len_table[pos] = core::GetBitLen(max_charcode);
      max_code <<= len_table[pos];
      max_code += max_charcode;
    }
    total_len += len_table[pos];
    if (total_len > 63) {
      valid = false;
      return false;
    }
  }
  return true;
}

int64_t TextStat::Encode(const BString &rcbs,
                         bool round_up)  // round_up = true => fill the unused
                                         // characters by max codes
{
  int64_t res = 0;
  size_t len = rcbs.size();
  if (len > TAB_SIZE) return common::NULL_VALUE_64;
  int charcode;
  for (size_t i = 0; i < len; i++) {
    if (rcbs[i] == 0)  // special cases, for encoding DPNs
      charcode = 0;
    else if (uchar(rcbs[i]) == 255 && encode_table[256 * i + 255] == 255)
      charcode = (1 << len_table[i]) - 1;
    else {
      charcode = encode_table[256 * i + uchar(rcbs[i])];
      if (charcode == 255) return common::NULL_VALUE_64;
    }
    res <<= len_table[i];
    res += charcode;
  }
  if (!round_up) {
    if (len < max_string_size) {
      charcode = encode_table[256 * len];  // add 0 on the end
      if (charcode == 255) return common::NULL_VALUE_64;
      if (len_table[len] > 0) {
        res <<= len_table[len];
        res += charcode;
      }
    }
    for (size_t i = len + 1; i < max_string_size; i++)  // ignore the rest of string
      if (len_table[i] > 0) res <<= len_table[i];

  } else {
    for (size_t i = len; i < max_string_size; i++)  // fill the rest with maximal values
      if (len_table[i] > 0) {
        res <<= len_table[i];
        res += int((1 << len_table[i]) - 1);
      }
  }
  return res;
}

BString TextStat::Decode(int64_t code) {
  if (!chars_found_for_decoding) {
    // redefine chars_found as decoding dictionary
    chars_found_for_decoding = true;
    for (size_t i = 0; i < max_string_size; i++) {
      chars_found[256 * i] = 0;
      for (int j = 0; j < 256; j++)
        if (encode_table[j + 256 * i] != 255) chars_found[encode_table[j + 256 * i] + 256 * i] = j;
    }
  }
  int charcode;
  int len = max_string_size;
  static int const TMP_BUFFER_SIZE = 49;
  char buf_val[TMP_BUFFER_SIZE];
  DEBUG_ASSERT(max_string_size < TMP_BUFFER_SIZE);
  buf_val[max_string_size] = 0;
  for (int i = max_string_size - 1; i >= 0; i--) {
    charcode = 0;
    if (len_table[i] > 0) {
      charcode = int(code & int((1 << len_table[i]) - 1));  // take last len_table[i] bits
      code >>= len_table[i];
    }
    buf_val[i] = chars_found[charcode + 256 * i];
    if (buf_val[i] == 0)  // finding the first 0
      len = i;
  }
  return BString(buf_val, len, true);  // materialized copy of the value
}
}  // namespace types
}  // namespace stonedb
