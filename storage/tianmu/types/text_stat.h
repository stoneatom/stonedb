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
#ifndef TIANMU_TYPES_TEXT_STAT_H_
#define TIANMU_TYPES_TEXT_STAT_H_
#pragma once

#include "types/tianmu_data_types.h"

namespace Tianmu {
namespace types {

class TextStat final {
  //////////////////////////////////////////////
  //
  // Encoding of strings into 63-bit positive integer (monotonically), if
  // possible. Limitations: the strings must not contain 0 (reserved value), the
  // max. width must not exceed 48, not all 255 values are present on a
  // position.
 public:
  TextStat();
  TextStat(const TextStat &sec) = default;
  ~TextStat() = default;

  bool AddString(const BString &tianmu_s);  // return false if cannot create encoding
  bool AddChar(uchar v, int pos);
  void AddLen(int pos);          // value of len n puts 0 on position n (starting with 0)
  bool CheckIfCreatePossible();  // return false if cannot create encoding (too
                                 // wide), do not actually create anything
  bool CreateEncoding();         // return false if cannot create encoding (too wide),
                                 // reset temporary statistics

  int64_t Encode(const BString &tianmu_s,
                 bool round_up = false);   // return common::NULL_VALUE_64 if not
                                           // encodable, round_up = true =>
                                           // fill the unused characters by
                                           // max codes
  int64_t MaxCode() { return max_code_; }  // return the maximal code which may occur
  BString Decode(int64_t code);

  bool IsValid() { return valid_; }
  void Invalidate() { valid_ = false; }

 private:
  static constexpr int TMP_BUFFER_SIZE_ = 49;

  static constexpr size_t TAB_SIZE_ = 48;

  // binary length of position n, value 0 means that this is a constant
  // character
  std::array<int, TAB_SIZE_> len_table_;

  // encode_table[c + 256 * n] is a character for code c on position n
  std::array<uchar, 256 * TAB_SIZE_> encode_table_;

  // a buffer for two purposes: collecting 0/1 info about chars found, or
  // collecting decoding into
  std::array<uchar, 256 * TAB_SIZE_> chars_found_;

  bool chars_found_for_decoding_{false};  // true, if chars_found is initialized for value decoding
  bool valid_{true};
  size_t max_string_size_{0};
  int64_t max_code_{0};
};

}  // namespace types
}  // namespace Tianmu

#endif  // TIANMU_TYPES_TEXT_STAT_H_
