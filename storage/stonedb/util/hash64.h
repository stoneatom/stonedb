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
#ifndef STONEDB_UTIL_HASH64_H_
#define STONEDB_UTIL_HASH64_H_
#pragma once

#include <array>
#include <vector>

namespace stonedb {
namespace utils {
// Easy hash set for 64-bit values
class Hash64 final {
  static constexpr int HASH64_STEPS_MASK = 31;

 private:
  int64_t FindAddress(int64_t n) {
    n ^= n << 17;
    n += n >> 8;
    n ^= n << 13;
    n += n >> 42;
    n ^= n << 33;
    n += n >> 25;
    return n & address_mask;
  }

  int64_t HashStep(int64_t n) {
    n ^= n << 14;
    n += n >> 23;
    n += n >> 33;
    return hash_step[n & HASH64_STEPS_MASK];
  }

 public:
  Hash64(int set_size) {  // do not exceed the set_size numbers of input values
    int size;
    for (size = 4; size < set_size + 2; size <<= 1)
      ;
    size <<= 1;  // at least twice as much as set_size, typically 3 times
    t.resize(size);
    address_mask = size - 1;
    zero_inserted = false;
    hash_capacity = set_size;
  }
  Hash64(Hash64 &sec) : t(sec.t), hash_step(sec.hash_step) {
    address_mask = sec.address_mask;
    zero_inserted = sec.zero_inserted;
  }

  ~Hash64() = default;

  bool Find(int64_t &n) {
    if (n == 0) return zero_inserted;
    int64_t i = FindAddress(n);
    int64_t tv = t[i];
    if (tv == n) return true;
    if (tv == 0) return false;
    int64_t s = HashStep(n);
    while (1) {
      i = (i + s) & address_mask;
      tv = t[i];
      if (tv == n) return true;
      if (tv == 0) return false;
    }
    return false;
  }

  void Insert(int64_t n) {
    if (n == 0) {
      zero_inserted = true;
      return;
    }
    int64_t i = FindAddress(n);
    if (t[i] == 0) {
      t[i] = n;
      return;
    }
    if (t[i] == n) return;
    int64_t s = HashStep(n);
    while (1) {
      i = (i + s) & address_mask;
      if (t[i] == 0) {
        t[i] = n;
        return;
      }
      if (t[i] == n) return;
    }
    return;
  }

  int capacity() { return hash_capacity; }

 private:
  int hash_capacity;
  int64_t address_mask;
  bool zero_inserted;

  std::vector<int64_t> t;

  std::array<int64_t, HASH64_STEPS_MASK + 1> hash_step{1,   5,   7,   11,  13,  17,  19,  23,  29,  31, 37,
                                                       41,  43,  47,  53,  59,  71,  73,  79,  83,  89, 97,
                                                       101, 103, 107, 109, 113, 127, 131, 137, 139, 149};
};
}  // namespace utils
}  // namespace stonedb

#endif  // STONEDB_UTIL_HASH64_H_
