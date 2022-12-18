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
#ifndef TIANMU_CORE_BIN_TOOLS_H_
#define TIANMU_CORE_BIN_TOOLS_H_
#pragma once

#include "common/common_definitions.h"

namespace Tianmu {
namespace core {
// Number of "1"-s in 32-bit value

int CalculateBinSum(unsigned int n);
int CalculateBinSize(unsigned int n);
int CalculateBinSize(uint64_t n);

static inline uint CalculateByteSize(uint64_t n)  // 0 -> 0, 1 -> 1, 255 -> 1, 256 -> 2 etc.
{
  uint res = 0;
  while (n != 0) {
    n >>= 8;
    res++;
  }
  return res;
}

// Additional tools // extern
int64_t FloatMinusEpsilon(int64_t v);      // modify the number to the nearest down (precision permitting)
int64_t FloatPlusEpsilon(int64_t v);       // modify the number to the nearest up (precision permitting)
int64_t FloatMinusEpsilon(int64_t v);      // modify the number to the nearest down (precision permitting)
int64_t FloatPlusEpsilon(int64_t v);       // modify the number to the nearest up (precision permitting)
int64_t DoubleMinusEpsilon(int64_t v);     // modify the number to the nearest down (precision permitting)
int64_t DoublePlusEpsilon(int64_t v);      // modify the number to the nearest up (precision permitting)
int64_t MonotonicDouble2Int64(int64_t d);  // encode double value (stored bitwise as int64_t) into
                                           // int64_t to prevent comparison directions
int64_t MonotonicInt642Double(int64_t d);  // reverse encoding done by MonotonicDouble2Int64

int64_t SafeMultiplication(int64_t x,
                           int64_t y);  // return a multiplication of two numbers or
                                        // common::NULL_VALUE_64 in case of overflow

uint HashValue(const void *buf,
               int n);  // CRC-32 checksum for a given memory buffer, n - number of bytes
#define HASH_FUNCTION_BYTE_SIZE 16
void HashMD5(unsigned char const *buf, int n, unsigned char *hash);

common::RoughSetValue Or(common::RoughSetValue f, common::RoughSetValue s);
common::RoughSetValue And(common::RoughSetValue f, common::RoughSetValue s);

inline bool IsDoubleNull(const double d) { return *(int64_t *)&d == common::NULL_VALUE_64; }

inline uint GetBitLen(uint x) {
#ifdef __x86_64__
  uint position;
  if (!x)
    return 0;
  asm("bsrl %1, %0" : "=r"(position) : "r"(x));
  return position + 1;
#else
#error "Unsupported system."
#endif
}

inline uint GetBitLen(uint64_t x) {
#ifdef __x86_64__
  uint64_t position;
  if (!x)
    return 0;
  asm("bsr %1, %0" : "=r"(position) : "r"(x));
  return (uint)position + 1;
#else
#error "Unsupported system."
#endif
}

inline uint GetBitLen(ushort x) { return GetBitLen((uint)x); }
inline uint GetBitLen(uchar x) { return GetBitLen((uint)x); }
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_BIN_TOOLS_H_
