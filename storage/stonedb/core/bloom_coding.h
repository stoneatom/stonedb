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
#ifndef STONEDB_CORE_BLOOM_CODING_H_
#define STONEDB_CORE_BLOOM_CODING_H_
#pragma once

#include <stdint.h>
#include <string.h>
#include <string>
#include "core/bloom_block.h"

namespace stonedb {
namespace core {
static const bool kLittleEndian = true;

// Standard Put... routines append to a string
extern void PutFixed32(std::string *dst, uint32_t value);
extern void PutFixed64(std::string *dst, uint64_t value);
extern void PutVarint32(std::string *dst, uint32_t value);
extern void PutVarint64(std::string *dst, uint64_t value);
extern void PutLengthPrefixedSlice(std::string *dst, const Slice &value);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
extern bool GetVarint32(Slice *input, uint32_t *value);
extern bool GetVarint64(Slice *input, uint64_t *value);
extern bool GetLengthPrefixedSlice(Slice *input, Slice *result);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// NULL on error.  These routines only look at bytes in the range
// [p..limit-1]
extern const char *GetVarint32Ptr(const char *p, const char *limit, uint32_t *v);
extern const char *GetVarint64Ptr(const char *p, const char *limit, uint64_t *v);

// Returns the length of the varint32 or varint64 encoding of "v"
extern int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written
extern void EncodeFixed32(char *dst, uint32_t value);
extern void EncodeFixed64(char *dst, uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
extern char *EncodeVarint32(char *dst, uint32_t value);
extern char *EncodeVarint64(char *dst, uint64_t value);

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

inline uint32_t DecodeFixed32(const char *ptr) {
  if (kLittleEndian) {
    // Load the raw bytes
    uint32_t result;
    std::memcpy(&result, ptr,
                sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0]))) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

inline uint64_t DecodeFixed64(const char *ptr) {
  if (kLittleEndian) {
    // Load the raw bytes
    uint64_t result;
    std::memcpy(&result, ptr,
                sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

// Internal routine for use by fallback path of GetVarint32Ptr
extern const char *GetVarint32PtrFallback(const char *p, const char *limit, uint32_t *value);
inline const char *GetVarint32Ptr(const char *p, const char *limit, uint32_t *value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char *>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_BLOOM_CODING_H_
