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

#include "span.h"

namespace Tianmu {
namespace mm {
namespace tcm {
PageHeapAllocator<Span> _span_allocator;

#ifdef SPAN_HISTORY
void Event(Span *span, char op, int v = 0) {
  span->history[span->nexthistory] = op;
  span->value[span->nexthistory] = v;
  span->nexthistory++;
  if (span->nexthistory == sizeof(span->history))
    span->nexthistory = 0;
}
#endif

Span *NewSpan(PageID p, Length len) {
  Span *result = _span_allocator.New();
  memset(result, 0, sizeof(*result));
  result->start = p;
  result->length = len;
#ifdef SPAN_HISTORY
  result->nexthistory = 0;
#endif
  return result;
}

void DeleteSpan(Span *span) {
#ifndef NDEBUG
  // In debug mode, trash the contents of deleted Spans
  memset(span, 0x3f, sizeof(*span));
#endif
  _span_allocator.Delete(span);
}

void DLL_Init(Span *list) {
  list->next = list;
  list->prev = list;
}

void DLL_Remove(Span *span) {
  span->prev->next = span->next;
  span->next->prev = span->prev;
  span->prev = nullptr;
  span->next = nullptr;
}

#if 0  // This isn't used.  If that changes, rewrite to use TCMalloc_Printer.
void DLL_Print(const char *label, const Span *list)
{
    MESSAGE("%-10s %p:", label, list);
    for (const Span *s = list->next; s != list; s = s->next) {
        MESSAGE(" <%p,%"PRIuPTR",%"PRIuPTR">", s, s->start, s->length);
    }
    MESSAGE("%s\n", "");  // %s is to get around a compiler error.
}
#endif

void DLL_Prepend(Span *list, Span *span) {
  ASSERT(span->next == nullptr);
  ASSERT(span->prev == nullptr);
  span->next = list->next;
  span->prev = list;
  list->next->prev = span;
  list->next = span;
}

}  // namespace tcm
}  // namespace mm
}  // namespace Tianmu
