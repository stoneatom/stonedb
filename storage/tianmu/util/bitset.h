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
#ifndef TIANMU_UTIL_BITSET_H_
#define TIANMU_UTIL_BITSET_H_
#pragma once

#include <string.h>
namespace Tianmu {
namespace utils {

class BitSet {
 public:
  BitSet() = default;
  BitSet(size_t sz, char *data = nullptr) : sz_(sz) {
    if (data) {
      ptr_ = data;
      allocated_ = false;
    } else {
      ptr_ = new char[(sz + NO_OF_BITS - 1) / NO_OF_BITS]();
      memset(ptr_, 0, (sz + NO_OF_BITS - 1) / NO_OF_BITS);
      allocated_ = true;
    }
  }

  ~BitSet() {
    if (allocated_) {
      delete[] ptr_;
      ptr_ = nullptr;
    }
  }

  void Init(size_t sz, char *data = nullptr) {
    sz_ = sz;
    if (data) {
      ptr_ = data;
      allocated_ = false;
    } else {
      ptr_ = new char[(sz + NO_OF_BITS - 1) / NO_OF_BITS]();
      memset(ptr_, 0, (sz + NO_OF_BITS - 1) / NO_OF_BITS);
      allocated_ = true;
    }
  }

  bool operator[](size_t pos) const { return ptr_[pos / NO_OF_BITS] & (1U << (pos % NO_OF_BITS)); }
  void set(size_t pos) { ptr_[pos / NO_OF_BITS] |= (1U << (pos % NO_OF_BITS)); }
  void reset(size_t pos) { ptr_[pos / NO_OF_BITS] &= ~(1U << (pos % NO_OF_BITS)); }
  const char *data() const { return ptr_; }
  size_t data_size() const { return (sz_ + NO_OF_BITS - 1) / NO_OF_BITS; }

 private:
  char *ptr_ = nullptr;
  size_t sz_;
  bool allocated_;
  static constexpr size_t NO_OF_BITS = 8;
};

}  // namespace utils
}  // namespace Tianmu

#endif  // TIANMU_UTIL_BITSET_H_
