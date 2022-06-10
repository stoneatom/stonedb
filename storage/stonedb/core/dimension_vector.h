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
#ifndef STONEDB_CORE_DIMENSION_VECTOR_H_
#define STONEDB_CORE_DIMENSION_VECTOR_H_
#pragma once

#include <vector>

#include "common/assert.h"

namespace stonedb {
namespace core {
class DimensionVector {
 public:
  DimensionVector() = default;
  DimensionVector(int no_dims) : v(no_dims){};
  DimensionVector(const DimensionVector &sec) = default;
  ~DimensionVector() = default;

  DimensionVector &operator=(const DimensionVector &sec) {
    if (&sec != this) {
      v = sec.v;
    }
    return *this;
  }

  bool operator==(const DimensionVector &d2) const {
    DEBUG_ASSERT(Size() == d2.Size());
    for (int i = 0; i < Size(); ++i)
      if (v[i] != d2.v[i]) return false;
    return true;
  }
  bool operator!=(const DimensionVector &d2) { return !operator==(d2); }

  std::vector<bool>::reference operator[](int i) { return v[i]; }

  bool Get(int i) const { return v[i]; }

  void Clean() { std::fill(v.begin(), v.end(), false); }
  void SetAll() { std::fill(v.begin(), v.end(), true); }
  void Complement() { v.flip(); }

  // true if any common dimension present
  bool Intersects(DimensionVector &sec) {
    DEBUG_ASSERT(v.size() == sec.v.size());
    for (size_t i = 0; i < v.size(); i++)
      if (v[i] && sec.v[i]) return true;
    return false;
  }

  // true if all dimensions from sec are present in *this
  bool Includes(DimensionVector &sec) {
    DEBUG_ASSERT(v.size() == sec.v.size());
    for (size_t i = 0; i < v.size(); i++)
      if (sec.v[i] && !v[i]) return false;
    return true;
  }
  // exclude from *this all dimensions present in sec
  void Minus(DimensionVector &sec) {
    DEBUG_ASSERT(v.size() == sec.v.size());
    for (size_t i = 0; i < v.size(); i++)
      if (sec.v[i]) v[i] = false;
  }
  // include in *this all dimensions present in sec
  void Plus(DimensionVector &sec) {
    DEBUG_ASSERT(v.size() == sec.v.size());
    for (size_t i = 0; i < v.size(); i++)
      if (sec.v[i]) v[i] = true;
  }

  int NoDimsUsed() const {
    int res = 0;
    for (size_t i = 0; i < v.size(); i++)
      if (v[i]) res++;
    return res;
  }
  // return the only existing dim, or -1 if more or less than one
  int GetOneDim() {
    int res = -1;
    for (size_t i = 0; i < v.size(); i++)
      if (v[i]) {
        if (res != -1) return -1;
        res = i;
      }
    return res;
  }
  bool IsEmpty() const {
    for (size_t i = 0; i < v.size(); i++)
      if (v[i]) return false;
    return true;
  }
  int Size() const { return v.size(); }  // return a number of all dimensions (present or not)

 private:
  std::vector<bool> v;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_DIMENSION_VECTOR_H_
