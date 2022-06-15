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

/* Definitions for compression methods */
#ifndef STONEDB_COMPRESS_DEFS_H_
#define STONEDB_COMPRESS_DEFS_H_
#pragma once

// Compression and decompression errors
enum class CprsErr {
  CPRS_SUCCESS = 0,
  CPRS_ERR_BUF = 1,   // buffer overflow error
  CPRS_ERR_PAR = 2,   // bad parameters
  CPRS_ERR_SUM = 3,   // wrong cumulative-sum table (for arithmetic coding)
  CPRS_ERR_VER = 4,   // incorrect version of compressed data
  CPRS_ERR_COR = 5,   // compressed data are corrupted
  CPRS_ERR_MEM = 6,   // memory allocation error
  CPRS_ERR_OTH = 100  // other error (unrecognized)
};

// Attribute types treated specially by compression routines
enum class CprsAttrType /*Enum*/ {
  CAT_OTHER = 0,
  CAT_DATA = 1,
  CAT_TIME = 2,
};

#ifdef _MAKESTAT_
#define IFSTAT(ins) ins
#pragma message("-----------------  Storing STATISTICS is on!!!  -----------------")
#else
#define IFSTAT(ins) ((void)0)
#endif

#include <iostream>

namespace stonedb {
namespace compress {

class _SHIFT_CHECK_ {
 public:
  unsigned long long v;
  explicit _SHIFT_CHECK_(unsigned long long a) : v(a) {}
};

template <class T>
inline T operator>>(T a, _SHIFT_CHECK_ b) {
  if (b.v >= sizeof(T) * 8) {
    return 0;
  }
  return a >> b.v;
}
template <class T>
inline T &operator>>=(T &a, _SHIFT_CHECK_ b) {
  if (b.v >= sizeof(T) * 8) {
    a = (T)0;
    return a;
  }
  return a >>= b.v;
}

template <class T>
inline T operator<<(T a, _SHIFT_CHECK_ b) {
  if (b.v >= sizeof(T) * 8) {
    return 0;
  }
  return a << b.v;
}
template <class T>
inline T &operator<<=(T &a, _SHIFT_CHECK_ b) {
  if (b.v >= sizeof(T) * 8) {
    a = (T)0;
    return a;
  }
  return a <<= b.v;
}

#ifndef _SHR_
#define _SHR_ >> (_SHIFT_CHECK_)(unsigned long long)
#endif
#ifndef _SHR_ASSIGN_
#define _SHR_ASSIGN_ >>= (_SHIFT_CHECK_)(unsigned long long)
#endif
#ifndef _SHL_
#define _SHL_ << (_SHIFT_CHECK_)(unsigned long long)
#endif
#ifndef _SHL_ASSIGN_
#define _SHL_ASSIGN_ <<= (_SHIFT_CHECK_)(unsigned long long)
#endif

}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_DEFS_H_
