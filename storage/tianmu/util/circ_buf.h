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
#ifndef TIANMU_UTIL_CIRC_BUF_H_
#define TIANMU_UTIL_CIRC_BUF_H_
#pragma once

#include <vector>

namespace Tianmu {
namespace utils {

template <class T>
class FixedSizeBuffer {
 public:
  FixedSizeBuffer(int size = 15) : size_(size) {
    buffer_.resize(size);
    Reset();
  }

  void Put(const T &v) {
    DEBUG_ASSERT(elems_ < size_);
    iin_ = (iin_ + 1) % size_;
    buffer_[iin_] = v;
    ++elems_;
  }

  T &Get() {
    DEBUG_ASSERT(elems_ > 0);
    T &v = buffer_[iout_];
    --elems_;
    iout_ = (iout_ + 1) % size_;
    return v;
  }

  T &GetLast() {
    DEBUG_ASSERT(elems_ > 0);
    return buffer_[iin_];
  }

  T &Nth(int n) {
    DEBUG_ASSERT(elems_ >= n - 1);
    return buffer_[(iout_ + n) % size_];
  }

  int Elems() { return elems_; }
  bool Empty() { return elems_ == 0; }
  void Reset() {
    iin_ = size_ - 1;
    iout_ = 0;
    elems_ = 0;
  }

 private:
  std::vector<T> buffer_;
  int iin_, iout_;
  int elems_;
  int size_;
};

}  // namespace utils
}  // namespace Tianmu

#endif  // TIANMU_UTIL_CIRC_BUF_H_
