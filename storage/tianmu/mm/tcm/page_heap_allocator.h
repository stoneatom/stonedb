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
#ifndef TIANMU_MM_PAGE_HEAP_ALLOCATOR_H_
#define TIANMU_MM_PAGE_HEAP_ALLOCATOR_H_
#pragma once

#include <cstdlib>
#include <list>

#include "mm/tcm/tccommon.h"

namespace Tianmu {
namespace mm {
namespace tcm {
// Simple allocator for objects of a specified type.  External locking
// is required before accessing one of these objects.
template <class T>
class PageHeapAllocator {
 public:
  ~PageHeapAllocator() {
    for (auto i : system_alloc_list) free(i);
  }
  // We use an explicit Init function because these variables are statically
  // allocated and their constructors might not have run by the time some
  // other static variable tries to allocate memory.
  void Init() {
    ASSERT(kAlignedSize <= (size_t)kAllocIncrement);
    free_area_ = nullptr;
    free_avail_ = 0;
    free_list_ = nullptr;
    // Reserve some space at the beginning to avoid fragmentation.
    Delete(New());
  }

  T *New() {
    // Consult free list
    void *result;
    if (free_list_ != nullptr) {
      result = free_list_;
      free_list_ = *(reinterpret_cast<void **>(result));
    } else {
      if (free_avail_ < kAlignedSize) {
        // Need more room
        // free_area_ = (char *)TCMalloc_SystemAlloc(kAllocIncrement, nullptr,
        // kPageSize);
        free_area_ = reinterpret_cast<char *>(MetaDataAlloc(kAllocIncrement));
        if (free_area_ == nullptr) {
          return nullptr;
        }
        system_alloc_list.push_back(free_area_);
        free_avail_ = kAllocIncrement;
      }
      result = free_area_;
      free_area_ += kAlignedSize;
      free_avail_ -= kAlignedSize;
    }
    return reinterpret_cast<T *>(result);
  }

  void Delete(T *p) {
    *(reinterpret_cast<void **>(p)) = free_list_;
    free_list_ = p;
  }

 private:
  // How much to allocate from system at a time
  static const int kAllocIncrement = 128 << 10;

  // Aligned size of T
  static const size_t kAlignedSize = (((sizeof(T) + kAlignment - 1) / kAlignment) * kAlignment);

  // Free area from which to carve new objects
  char *free_area_;
  size_t free_avail_;

  // Free list of already carved objects
  void *free_list_;

  std::list<void *> system_alloc_list;
};

}  // namespace tcm
}  // namespace mm
}  // namespace Tianmu

#endif  // TIANMU_MM_PAGE_HEAP_ALLOCATOR_H_
