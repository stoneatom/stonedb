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

#include "page_heap.h"

#include "mm/tcm/tccommon.h"

namespace Tianmu {
namespace mm {
namespace tcm {

PageHeap::PageHeap() : pagemap_(MetaDataAlloc), scavenge_counter_(0), release_index_(kMaxPages) {
  DLL_Init(&large_.normal);
  DLL_Init(&large_.returned);
  for (size_t i = 0; i < kMaxPages; i++) {
    DLL_Init(&free_[i].normal);
    DLL_Init(&free_[i].returned);
  }
}

Span *PageHeap::New(Length n) {
  ASSERT(Check());
  ASSERT(n > 0);

  // Find first size >= n that has a non-empty list
  for (Length s = n; s < kMaxPages; s++) {
    Span *ll = &free_[s].normal;
    // If we're lucky, ll is non-empty, meaning it has a suitable span.
    if (!DLL_IsEmpty(ll)) {
      ASSERT(ll->next->location == static_cast<unsigned int>(Span::enumSpanType::ON_NORMAL_FREELIST));
      return Carve(ll->next, n);
    }
    // Alternatively, maybe there's a usable returned span.
    ll = &free_[s].returned;
    if (!DLL_IsEmpty(ll)) {
      ASSERT(ll->next->location == static_cast<unsigned int>(Span::enumSpanType::ON_RETURNED_FREELIST));
      return Carve(ll->next, n);
    }
    // Still no luck, so keep looking in larger classes.
  }

  Span *result = AllocLarge(n);
  // if (result != nullptr) return result;

  // Grow the heap and try again
  // if (!GrowHeap(n)) {
  //  ASSERT(Check());
  //  return nullptr;
  //}
  return result;
}

Span *PageHeap::AllocLarge(Length n) {
  // find the best span (closest to n in size).
  // The following loops implements address-ordered best-fit.
  Span *best = nullptr;

  // Search through normal list
  for (Span *span = large_.normal.next; span != &large_.normal; span = span->next) {
    if (span->length >= n) {
      if ((best == nullptr) || (span->length < best->length) ||
          ((span->length == best->length) && (span->start < best->start))) {
        best = span;
        ASSERT(best->location == static_cast<unsigned int>(Span::enumSpanType::ON_NORMAL_FREELIST));
      }
    }
  }

  // Search through released list in case it has a better fit
  for (Span *span = large_.returned.next; span != &large_.returned; span = span->next) {
    if (span->length >= n) {
      if ((best == nullptr) || (span->length < best->length) ||
          ((span->length == best->length) && (span->start < best->start))) {
        best = span;
        ASSERT(best->location == static_cast<unsigned int>(Span::enumSpanType::ON_RETURNED_FREELIST));
      }
    }
  }

  return best == nullptr ? nullptr : Carve(best, n);
}

Span *PageHeap::Carve(Span *span, Length n) {
  ASSERT(n > 0);
  ASSERT(span->location != static_cast<unsigned int>(Span::enumSpanType::IN_USE));
  const int old_location = span->location;
  RemoveFromFreeList(span);
  span->location = static_cast<unsigned int>(Span::enumSpanType::IN_USE);
  Event(span, 'A', n);

  const int extra = int(span->length - n);
  ASSERT(extra >= 0);
  if (extra > 0) {
    Span *leftover = NewSpan(span->start + n, extra);
    leftover->location = old_location;
    Event(leftover, 'S', extra);
    RecordSpan(leftover);
    PrependToFreeList(leftover);  // Skip coalescing - no candidates possible
    span->length = n;
    pagemap_.set(span->start + n - 1, span);
  }
  ASSERT(Check());
  return span;
}

void PageHeap::Delete(Span *span) {
  ASSERT(Check());
  ASSERT(span->location == static_cast<unsigned int>(Span::enumSpanType::IN_USE));
  ASSERT(span->length > 0);
  ASSERT(GetDescriptor(span->start) == span);
  ASSERT(GetDescriptor(span->start + span->length - 1) == span);
  const Length n = span->length;
  span->sizeclass = 0;
  span->sample = 0;
  span->location = static_cast<unsigned int>(Span::enumSpanType::ON_NORMAL_FREELIST);
  Event(span, 'D', span->length);
  MergeIntoFreeList(span);  // Coalesces if possible
  IncrementalScavenge(n);
  ASSERT(Check());
}

void PageHeap::MergeIntoFreeList(Span *span) {
  ASSERT(span->location != static_cast<unsigned int>(Span::enumSpanType::IN_USE));

  // Coalesce -- we guarantee that "p" != 0, so no bounds checking
  // necessary.  We do not bother resetting the stale pagemap
  // entries for the pieces we are merging together because we only
  // care about the pagemap entries for the boundaries.
  //
  // Note that only similar spans are merged together.  For example,
  // we do not coalesce "returned" spans with "normal" spans.
  const PageID p = span->start;
  const Length n = span->length;
  Span *prev = GetDescriptor(p - 1);
  if (prev != nullptr && prev->location == span->location) {
    // Merge preceding span into this span
    ASSERT(prev->start + prev->length == p);
    const Length len = prev->length;
    RemoveFromFreeList(prev);
    DeleteSpan(prev);
    span->start -= len;
    span->length += len;
    pagemap_.set(span->start, span);
    Event(span, 'L', len);
  }
  Span *next = GetDescriptor(p + n);
  if (next != nullptr && next->location == span->location) {
    // Merge next span into this span
    ASSERT(next->start == p + n);
    const Length len = next->length;
    RemoveFromFreeList(next);
    DeleteSpan(next);
    span->length += len;
    pagemap_.set(span->start + span->length - 1, span);
    Event(span, 'R', len);
  }

  PrependToFreeList(span);
}

void PageHeap::PrependToFreeList(Span *span) {
  ASSERT(span->location != static_cast<unsigned int>(Span::enumSpanType::IN_USE));
  SpanList *list = (span->length < kMaxPages) ? &free_[span->length] : &large_;
  if (span->location == static_cast<unsigned int>(Span::enumSpanType::ON_NORMAL_FREELIST)) {
    stats_.free_bytes += (span->length << kPageShift);
    DLL_Prepend(&list->normal, span);
  } else {
    stats_.unmapped_bytes += (span->length << kPageShift);
    DLL_Prepend(&list->returned, span);
  }
}

void PageHeap::RemoveFromFreeList(Span *span) {
  ASSERT(span->location != static_cast<unsigned int>(Span::enumSpanType::IN_USE));
  if (span->location == static_cast<unsigned int>(Span::enumSpanType::ON_NORMAL_FREELIST)) {
    stats_.free_bytes -= (span->length << kPageShift);
  } else {
    stats_.unmapped_bytes -= (span->length << kPageShift);
  }
  DLL_Remove(span);
}

void PageHeap::IncrementalScavenge(Length n) {
  // Fast path; not yet time to release memory
  scavenge_counter_ -= n;
  if (scavenge_counter_ >= 0)
    return;  // Not yet time to scavenge

  //  const double rate = FLAGS_tcmalloc_release_rate;
  const double rate = 5.0;

  if (rate <= 1e-6) {
    // Tiny release rate means that releasing is disabled.
    scavenge_counter_ = kDefaultReleaseDelay;
    return;
  }

  Length released_pages = ReleaseAtLeastNPages(1);

  if (released_pages == 0) {
    // Nothing to scavenge, delay for a while.
    scavenge_counter_ = kDefaultReleaseDelay;
  } else {
    // Compute how long to wait until we return memory.
    // FLAGS_tcmalloc_release_rate==1 means wait for 1000 pages
    // after releasing one page.
    const double mult = 1000.0 / rate;
    double wait = mult * static_cast<double>(released_pages);
    if (wait > kMaxReleaseDelay) {
      // Avoid overflow and bound to reasonable range.
      wait = kMaxReleaseDelay;
    }
    scavenge_counter_ = static_cast<int64_t>(wait);
  }
}

Length PageHeap::ReleaseLastNormalSpan(SpanList *slist) {
  Span *s = slist->normal.prev;
  ASSERT(s->location == static_cast<unsigned int>(Span::enumSpanType::ON_NORMAL_FREELIST));
  RemoveFromFreeList(s);
  const Length n = s->length;
  // TCMalloc_SystemRelease(reinterpret_cast<void*>(s->start << kPageShift),
  //                       static_cast<size_t>(s->length << kPageShift));
  s->location = static_cast<unsigned int>(Span::enumSpanType::ON_RETURNED_FREELIST);
  MergeIntoFreeList(s);  // Coalesces if possible.
  return n;
}

Length PageHeap::ReleaseAtLeastNPages(Length num_pages) {
  Length released_pages = 0;
  Length prev_released_pages = -1;

  // Round robin through the lists of free spans, releasing the last
  // span in each list.  Stop after releasing at least num_pages.
  while (released_pages < num_pages) {
    if (released_pages == prev_released_pages) {
      // Last iteration of while loop made no progress.
      break;
    }
    prev_released_pages = released_pages;

    for (size_t i = 0; i < kMaxPages + 1 && released_pages < num_pages; i++, release_index_++) {
      if (release_index_ > kMaxPages)
        release_index_ = 0;
      SpanList *slist = (release_index_ == kMaxPages) ? &large_ : &free_[release_index_];
      if (!DLL_IsEmpty(&slist->normal)) {
        Length released_len = ReleaseLastNormalSpan(slist);
        released_pages += released_len;
      }
    }
  }
  return released_pages;
}

void PageHeap::RegisterSizeClass(Span *span, size_t sc) {
  // Associate span object with all interior pages as well
  ASSERT(span->location == static_cast<unsigned int>(Span::enumSpanType::IN_USE));
  ASSERT(GetDescriptor(span->start) == span);
  ASSERT(GetDescriptor(span->start + span->length - 1) == span);
  Event(span, 'C', sc);
  span->sizeclass = (uint)sc;
  for (Length i = 1; i < span->length - 1; i++) {
    pagemap_.set(span->start + i, span);
  }
}

bool PageHeap::GrowHeap(Length n) {
  ASSERT(kMaxPages >= kMinSystemAlloc);
  if (n > kMaxValidPages)
    return false;
  Length ask = (n > kMinSystemAlloc) ? n : static_cast<Length>(kMinSystemAlloc);
  size_t actual_size;
  void *ptr = nullptr;
  ptr = TCMalloc_SystemAlloc(ask << kPageShift, nullptr, kPageSize);

  if (ptr == nullptr) {
    if (n < ask) {
      // Try growing just "n" pages
      ask = n;
      ptr = TCMalloc_SystemAlloc(ask << kPageShift, nullptr, kPageSize);
    }
    if (ptr == nullptr)
      return false;
  }
  system_alloc_list.push_back(ptr);
  actual_size = ask << kPageShift;
  // ask = actual_size >> kPageShift;
  // RecordGrowth(ask << kPageShift);
  (void)actual_size;  // FIXME

  uint64_t old_system_bytes = stats_.system_bytes;
  stats_.system_bytes += (ask << kPageShift);
  const PageID p = reinterpret_cast<uintptr_t>(ptr) >> kPageShift;
  ASSERT(p > 0);

  // If we have already a lot of pages allocated, just pre allocate a bunch of
  // memory for the page map. This prevents fragmentation by pagemap metadata
  // when a program keeps allocating and freeing large blocks.

  if (old_system_bytes < kPageMapBigAllocationThreshold && stats_.system_bytes >= kPageMapBigAllocationThreshold) {
    pagemap_.PreallocateMoreMemory();
  }

  // Make sure pagemap_ has entries for all of the new pages.
  // Plus ensure one before and one after so coalescing code
  // does not need bounds-checking.
  if (pagemap_.Ensure(p - 1, ask + 2)) {
    // Pretend the new area is allocated and then Delete() it to cause
    // any necessary coalescing to occur.
    Span *span = NewSpan(p, ask);
    RecordSpan(span);
    Delete(span);
    ASSERT(Check());
    return true;
  } else {
    // We could not allocate memory within "pagemap_"
    // TODO: Once we can return memory to the system, return the new span
    return false;
  }
}

// Primarily used for HugePages
bool PageHeap::RegisterArea(void *ptr, Length pages) {
  const PageID p = reinterpret_cast<uintptr_t>(ptr) >> kPageShift;

  pagemap_.PreallocateMoreMemory();

  // Make sure pagemap_ has entries for all of the new pages.
  // Plus ensure one before and one after so coalescing code
  // does not need bounds-checking.
  if (pagemap_.Ensure(p - 1, pages + 2)) {
    // Pretend the new area is allocated and then Delete() it to cause
    // any necessary coalescing to occur.
    Span *span = NewSpan(p, pages);
    RecordSpan(span);
    Delete(span);
    ASSERT(Check());
    return true;
  } else {
    return false;
  }
}

bool PageHeap::Check() {
  ASSERT(free_[0].normal.next == &free_[0].normal);
  ASSERT(free_[0].returned.next == &free_[0].returned);
  return true;
}

#define CHECK_CONDITION(a) return false;

}  // namespace tcm
}  // namespace mm
}  // namespace Tianmu
