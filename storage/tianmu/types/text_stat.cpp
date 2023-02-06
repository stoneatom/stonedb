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

namespace Tianmu {
namespace types {

TextStat::TextStat() {
  encode_table_.fill(255);
  len_table_.fill(0);
  chars_found_.fill(0);
}

bool TextStat::AddString(const BString &tianmu_s) {
  size_t len = tianmu_s.size();
  if (len > TAB_SIZE_) {
    valid_ = false;
    return false;
  }

  for (size_t i = 0; i < len; i++) {
    if (tianmu_s[i] == 0) {
      valid_ = false;
      return false;
    }
    chars_found_[256 * i + uchar(tianmu_s[i])] = 1;
  }

  if (len < TAB_SIZE_)
    chars_found_[256 * len] = 1;  // value of len n puts 0 on position n (starting with 0)
  if (len > max_string_size_)
    max_string_size_ = len;

  return true;
}

bool TextStat::AddChar(uchar v, int pos)  // return false if out of range
{
  if (static_cast<size_t>(pos) >= TAB_SIZE_ || v == 0) {
    valid_ = false;
    return false;
  }

  chars_found_[256 * pos + v] = 1;
  if (static_cast<size_t>(pos + 1) > max_string_size_)
    max_string_size_ = pos + 1;
  return true;
}

void TextStat::AddLen(int pos)  // value of len n puts 0 on position n (starting with 0)
{
  if (static_cast<size_t>(pos) == TAB_SIZE_)
    return;
  DEBUG_ASSERT(static_cast<size_t>(pos) < TAB_SIZE_);
  chars_found_[256 * pos] = 1;
  if (static_cast<size_t>(pos) > max_string_size_)
    max_string_size_ = pos;
}

// return false if cannot create encoding (too wide), do not actually create anything
bool TextStat::CheckIfCreatePossible() {
  if (chars_found_for_decoding_) {
    valid_ = false;
    return false;
  }

  int total_len = 0;
  for (size_t pos = 0; pos < max_string_size_; pos++) {
    int loc_len_table = 0;
    for (size_t i = 0; i < 256; i++) {
      if (chars_found_[256 * pos + i] == 1) {
        if (loc_len_table == 255) {
          valid_ = false;
          return false;  // too many characters
        }
        loc_len_table++;
      }
    }
    if (loc_len_table > 1)
      total_len += core::GetBitLen(uint(loc_len_table) - 1);
    if (total_len > 63) {
      valid_ = false;
      return false;
    }
  }
  return true;
}

// chars_found_ is already used as a decoding, structure - cannot create decoding twice with a decoding in between
bool TextStat::CreateEncoding() {
  if (chars_found_for_decoding_) {
    valid_ = false;
    return false;
  }

  int total_len = 0;
  max_code_ = 0;
  for (size_t pos = 0; pos < max_string_size_; pos++) {
    len_table_[pos] = 0;
    for (int i = 0; i < 256; i++) {
      if (chars_found_[256 * pos + i] == 1) {
        encode_table_[i + 256 * pos] = len_table_[pos];  // Encoding a string: Code(character c) =
                                                         // encode_table_[c][pos]
        // Note: initial value of 255 means "illegal character", so we must not
        // allow to use such code value
        if (len_table_[pos] == 255) {
          valid_ = false;
          return false;  // too many characters
        }
        len_table_[pos]++;
      }
    }

    // translate lengths into binary sizes
    if (len_table_[pos] == 1)
      len_table_[pos] = 0;
    else if (len_table_[pos] > 1) {
      uint max_charcode = len_table_[pos] - 1;
      len_table_[pos] = core::GetBitLen(max_charcode);
      max_code_ <<= len_table_[pos];
      max_code_ += max_charcode;
    }

    total_len += len_table_[pos];
    if (total_len > 63) {
      valid_ = false;
      return false;
    }
  }
  return true;
}

int64_t TextStat::Encode(const BString &tianmu_s,
                         bool round_up)  // round_up = true => fill the unused
                                         // characters by max codes
{
  int64_t res = 0;
  size_t len = tianmu_s.size();
  if (len > TAB_SIZE_)
    return common::NULL_VALUE_64;
  int charcode;
  for (size_t i = 0; i < len; i++) {
    if (tianmu_s[i] == 0)  // special cases, for encoding DPNs
      charcode = 0;
    else if (uchar(tianmu_s[i]) == 255 && encode_table_[256 * i + 255] == 255)
      charcode = (1 << len_table_[i]) - 1;
    else {
      charcode = encode_table_[256 * i + uchar(tianmu_s[i])];
      if (charcode == 255)
        return common::NULL_VALUE_64;
    }
    res <<= len_table_[i];
    res += charcode;
  }
  if (!round_up) {
    if (len < max_string_size_) {
      charcode = encode_table_[256 * len];  // add 0 on the end
      if (charcode == 255)
        return common::NULL_VALUE_64;
      if (len_table_[len] > 0) {
        res <<= len_table_[len];
        res += charcode;
      }
    }
    for (size_t i = len + 1; i < max_string_size_; i++)  // ignore the rest of string
      if (len_table_[i] > 0)
        res <<= len_table_[i];

  } else {
    for (size_t i = len; i < max_string_size_; i++)  // fill the rest with maximal values
      if (len_table_[i] > 0) {
        res <<= len_table_[i];
        res += int((1 << len_table_[i]) - 1);
      }
  }
  return res;
}

BString TextStat::Decode(int64_t code) {
  if (!chars_found_for_decoding_) {
    // redefine chars_found_ as decoding dictionary
    chars_found_for_decoding_ = true;
    for (size_t i = 0; i < max_string_size_; i++) {
      chars_found_[256 * i] = 0;
      for (int j = 0; j < 256; j++)
        if (encode_table_[j + 256 * i] != 255)
          chars_found_[encode_table_[j + 256 * i] + 256 * i] = j;
    }
  }

  int charcode;
  int len = max_string_size_;
  char buf_val[TMP_BUFFER_SIZE_] = {0};

  DEBUG_ASSERT(max_string_size_ < TMP_BUFFER_SIZE_);
  buf_val[max_string_size_] = 0;
  for (int i = max_string_size_ - 1; i >= 0; i--) {
    charcode = 0;
    if (len_table_[i] > 0) {
      charcode = int(code & int((1 << len_table_[i]) - 1));  // take last len_table_[i] bits
      code >>= len_table_[i];
    }
    buf_val[i] = chars_found_[charcode + 256 * i];
    if (buf_val[i] == 0)  // finding the first 0
      len = i;
  }
  return BString(buf_val, len, true);  // materialized copy of the value
}

}  // namespace types
}  // namespace Tianmu
