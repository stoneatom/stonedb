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
#ifndef TIANMU_MM_SPAN_H_
#define TIANMU_MM_SPAN_H_
#pragma once

#include "mm/tcm/page_heap_allocator.h"
#include "mm/tcm/tccommon.h"

namespace Tianmu {
namespace mm {
namespace tcm {

// Information kept for a span (a contiguous run of pages).
struct Span {
  PageID start;                // Starting page number
  Length length;               // Number of pages in span
  Span *next;                  // Used when in link list
  Span *prev;                  // Used when in link list
  void *objects;               // Linked list of free objects
  unsigned int refcount : 16;  // Number of non-free objects
  unsigned int sizeclass : 8;  // Size-class for small objects (or 0)
  unsigned int location : 2;   // Is the span on a freelist, and if so, which?
  unsigned int sample : 1;     // Sampled object?
  unsigned int size;

#undef SPAN_HISTORY
#ifdef SPAN_HISTORY
  // For debugging, we can keep a log events per span
  int nexthistory;
  char history[64];
  int value[64];
#endif

  // What freelist the span is on: IN_USE if on none, or normal or returned
  enum class enumSpanType { IN_USE, ON_NORMAL_FREELIST, ON_RETURNED_FREELIST };
};

#ifdef SPAN_HISTORY
void Event(Span *span, char op, int v = 0);
#else
#define Event(s, o, v) ((void)0)
#endif

// Allocator/deallocator for spans
Span *NewSpan(PageID p, Length len);
void DeleteSpan(Span *span);

// -------------------------------------------------------------------------
// Doubly linked list of spans.
// -------------------------------------------------------------------------

// Initialize *list to an empty list.
void DLL_Init(Span *list);

// Remove 'span' from the linked list in which it resides, updating the
// pointers of adjacent Spans and setting span's next and prev to nullptr.
void DLL_Remove(Span *span);

// Return true iff "list" is empty.
inline bool DLL_IsEmpty(const Span *list) { return list->next == list; }

// Add span to the front of list.
void DLL_Prepend(Span *list, Span *span);

// Print the contents of the list to stderr.
#if 0  // This isn't used.
    void DLL_Print(const char *label, const Span *list);
#endif

extern PageHeapAllocator<Span> _span_allocator;

}  // namespace tcm
}  // namespace mm
}  // namespace Tianmu

#endif  // TIANMU_MM_SPAN_H_
