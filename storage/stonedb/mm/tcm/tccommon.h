// Copyright (c) 2008, Google Inc.
// All rights reserved.
//
// Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// ---
// Author: Sanjay Ghemawat <opensource@google.com>
//
// Common definitions for tcmalloc code.
#ifndef STONEDB_MM_TCCOMMON_H_
#define STONEDB_MM_TCCOMMON_H_
#pragma once

#include <common/assert.h>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace stonedb {
namespace mm {

// Type that can hold a page number
using PageID = uintptr_t;

// Type that can hold the length of a run of pages
using Length = uintptr_t;

//-------------------------------------------------------------------
// Configuration
//-------------------------------------------------------------------

// Not all possible combinations of the following parameters make
// sense.  In particular, if kMaxSize increases, you may have to
// increase kNumClasses as well.
static const size_t kPageShift = 12;
static const size_t kPageSize = 1 << kPageShift;
static const size_t kMaxSize = 8u * kPageSize;
static const size_t kAlignment = 8;
static const size_t kNumClasses = 61;

#define ADDR_TO_PAGEID(p) static_cast<PageID>((size_t)p >> kPageShift)

static const Length kMaxValidPages = (~static_cast<Length>(0)) >> kPageShift;

namespace tcm {
// Size-class information + mapping
class SizeMap {
 private:
  //-------------------------------------------------------------------
  // Mapping from size to size_class and vice versa
  //-------------------------------------------------------------------

  // Sizes <= 1024 have an alignment >= 8.  So for such sizes we have an
  // array indexed by ceil(size/8).  Sizes > 1024 have an alignment >= 128.
  // So for these larger sizes we have an array indexed by ceil(size/128).
  //
  // We flatten both logical arrays into one physical array and use
  // arithmetic to compute an appropriate index.  The constants used by
  // ClassIndex() were selected to make the flattening work.
  //
  // Examples:
  //   Size       Expression                      Index
  //   -------------------------------------------------------
  //   0          (0 + 7) / 8                     0
  //   1          (1 + 7) / 8                     1
  //   ...
  //   1024       (1024 + 7) / 8                  128
  //   1025       (1025 + 127 + (120<<7)) / 128   129
  //   ...
  //   32768      (32768 + 127 + (120<<7)) / 128  376
  static const int kMaxSmallSize = 1024;
  unsigned char class_array_[377];

  // Compute index of the class_array[] entry for a given size
  static inline int ClassIndex(int s) {
    ASSERT(0 <= s);
    ASSERT(s <= (int)kMaxSize);
    const bool big = (s > kMaxSmallSize);
    const int add_amount = big ? (127 + (120 << 7)) : 7;
    const int shift_amount = big ? 7 : 3;
    return (s + add_amount) >> shift_amount;
  }

  // Mapping from size class to max size storable in that class
  size_t class_to_size_[kNumClasses];

  // Mapping from size class to number of pages to allocate at a time
  size_t class_to_pages_[kNumClasses];

 public:
  // Constructor should do nothing since we rely on explicit Init()
  // call, which may or may not be called before the constructor runs.
  SizeMap() {
    for (unsigned int i = 0; i < kNumClasses; i++) {
      class_to_size_[i] = 0;
      class_to_pages_[i] = 0;
    }

    for (int i = 0; i < 377; i++) class_array_[i] = 0;
  }

  // Initialize the mapping arrays
  void Init();

  inline int SizeClass(int size) { return class_array_[ClassIndex(size)]; }

  // Get the byte-size for a specified class
  inline size_t ByteSizeForClass(size_t cl) { return class_to_size_[cl]; }

  // Mapping from size class to number of pages to allocate at a time
  inline size_t class_to_pages(size_t cl) { return class_to_pages_[cl]; }

  // Dump contents of the computed size map
  // void Dump(TCMalloc_Printer* out);
};

// Allocates "bytes" worth of memory and returns it.  Increments
// metadata_system_bytes appropriately.  May return NULL if allocation
// fails.  Requires pageheap_lock is held.
void *MetaDataAlloc(size_t bytes);

void *TCMalloc_SystemAlloc(size_t size, size_t *actual_size, size_t alignment);

}  // namespace tcm
}  // namespace mm
}  // namespace stonedb

#endif  // STONEDB_MM_TCCOMMON_H_
