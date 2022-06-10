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
#ifndef STONEDB_UTIL_CIRC_BUF_H_
#define STONEDB_UTIL_CIRC_BUF_H_
#pragma once

#include <vector>

namespace stonedb {
namespace utils {
template <class T>
class FixedSizeBuffer {
 public:
  FixedSizeBuffer(int size = 15) : size(size) {
    buf.resize(size);
    Reset();
  }
  void Put(const T &v) {
    DEBUG_ASSERT(elems < size);
    iin = (iin + 1) % size;
    buf[iin] = v;
    ++elems;
  }
  T &Get() {
    DEBUG_ASSERT(elems > 0);
    T &v = buf[iout];
    --elems;
    iout = (iout + 1) % size;
    return v;
  }
  T &GetLast() {
    DEBUG_ASSERT(elems > 0);
    return buf[iin];
  }
  T &Nth(int n) {
    DEBUG_ASSERT(elems >= n - 1);
    return buf[(iout + n) % size];
  }
  int Elems() { return elems; }
  bool Empty() { return elems == 0; }
  void Reset() {
    iin = size - 1;
    iout = 0;
    elems = 0;
  }

 private:
  std::vector<T> buf;
  int iin, iout;
  int elems;
  int size;
};
}  // namespace utils
}  // namespace stonedb

#endif  // STONEDB_UTIL_CIRC_BUF_H_
