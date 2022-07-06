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
#ifndef STONEDB_MM_PAGE_HEAP_H_
#define STONEDB_MM_PAGE_HEAP_H_
#pragma once

#include <list>
#include "mm/tcm/pagemap.h"
#include "mm/tcm/span.h"
#include "mm/tcm/tccommon.h"

namespace stonedb {
namespace mm {
// change the namespace so if we ever try to link with the real tcmalloc we
// don't get strangeness
namespace tcm {
// -------------------------------------------------------------------------
// Map from page-id to per-page data
// -------------------------------------------------------------------------

// We use PageMap2<> for 32-bit and PageMap3<> for 64-bit machines.
// We also use a simple one-level cache for hot PageID-to-sizeclass mappings,
// because sometimes the sizeclass is all the information we need.

// Selector class -- general selector uses 3-level map
template <int BITS>
class MapSelector {
 public:
  using Type = TCMalloc_PageMap3<BITS - kPageShift>;
};

// -------------------------------------------------------------------------
// Page-level allocator
//  * Eager coalescing
//
// Heap for page-level allocation.  We allow allocating and freeing a
// contiguous runs of pages (called a "span").
// -------------------------------------------------------------------------

class PageHeap final {
 public:
  PageHeap();
  ~PageHeap() {
    for (auto i : system_alloc_list) free(i);
  }

  // Allocate a run of "n" pages.  Returns zero if out of memory.
  // Caller should not pass "n == 0" -- instead, n should have
  // been rounded up already.
  Span *New(Length n);

  // Delete the span "[p, p+n-1]".
  // REQUIRES: span was returned by earlier call to New() and
  //           has not yet been deleted.
  void Delete(Span *span);

  // Mark an allocated span as being used for small objects of the
  // specified size-class.
  // REQUIRES: span was returned by an earlier call to New()
  //           and has not yet been deleted.
  void RegisterSizeClass(Span *span, size_t sc);

  // Return the descriptor for the specified page.  Returns NULL if
  // this PageID was not allocated previously.
  inline Span *GetDescriptor(PageID p) const { return reinterpret_cast<Span *>(pagemap_.get(p)); }

  // Dump state to stderr
  // void Dump(TCMalloc_Printer* out);

  // If this page heap is managing a range with starting page # >= start,
  // store info about the range in *r and return true.  Else return false.
  // bool GetNextRange(PageID start, base::MallocRange* r);

  // Page heap statistics
  struct Stats {
    Stats() : system_bytes(0), free_bytes(0), unmapped_bytes(0) {}
    uint64_t system_bytes;    // Total bytes allocated from system
    uint64_t free_bytes;      // Total bytes on normal freelists
    uint64_t unmapped_bytes;  // Total bytes on returned freelists
  };
  inline Stats stats() const { return stats_; }

  bool Check();

  // Try to release at least num_pages for reuse by the OS.  Returns
  // the actual number of pages released, which may be less than
  // num_pages if there weren't enough pages to release. The result
  // may also be larger than num_pages since page_heap might decide to
  // release one large range instead of fragmenting it into two
  // smaller released and unreleased ranges.
  Length ReleaseAtLeastNPages(Length num_pages);

  bool GrowHeap(Length n);

  bool RegisterArea(void *ptr, Length pages);

 private:
  // Allocates a big block of memory for the pagemap once we reach more than
  // 128MB
  static const size_t kPageMapBigAllocationThreshold = 128 << 20;

  // Minimum number of pages to fetch from system at a time.  Must be
  // significantly bigger than kBlockSize to amortize system-call
  // overhead, and also to reduce external fragementation.  Also, we
  // should keep this value big because various incarnations of Linux
  // have small limits on the number of mmap() regions per
  // address-space.
  static const unsigned int kMinSystemAlloc = 1 << (20 - kPageShift);

  // For all span-lengths < kMaxPages we keep an exact-size list.
  // REQUIRED: kMaxPages >= kMinSystemAlloc;
  static const size_t kMaxPages = kMinSystemAlloc;

  // Never delay scavenging for more than the following number of
  // deallocated pages.  With 4K pages, this comes to 4GB of
  // deallocation.
  static const int kMaxReleaseDelay = 1 << 20;

  // If there is nothing to release, wait for so many pages before
  // scavenging again.  With 4K pages, this comes to 1GB of memory.
  static const int kDefaultReleaseDelay = 1 << 18;

  // Pick the appropriate map and cache types based on pointer size
  MapSelector<8 * sizeof(uintptr_t)>::Type pagemap_;

  // We segregate spans of a given size into two circular linked
  // lists: one for normal spans, and one for spans whose memory
  // has been returned to the system.
  struct SpanList {
    Span normal;
    Span returned;
  };

  // List of free spans of length >= kMaxPages
  SpanList large_;

  // Array mapping from span length to a doubly linked list of free spans
  SpanList free_[kMaxPages];

  // Statistics on system, free, and unmapped bytes
  Stats stats_;
  std::list<void *> system_alloc_list;

  // REQUIRES: span->length >= n
  // REQUIRES: span->location != enumSpanType::IN_USE
  // Remove span from its free list, and move any leftover part of
  // span into appropriate free lists.  Also update "span" to have
  // length exactly "n" and mark it as non-free so it can be returned
  // to the client.  After all that, decrease free_pages_ by n and
  // return span.
  Span *Carve(Span *span, Length n);

  void RecordSpan(Span *span) {
    pagemap_.set(span->start, span);
    if (span->length > 1) {
      pagemap_.set(span->start + span->length - 1, span);
    }
  }

  // Allocate a large span of length == n.  If successful, returns a
  // span of exactly the specified length.  Else, returns NULL.
  Span *AllocLarge(Length n);

  // Coalesce span with neighboring spans if possible, prepend to
  // appropriate free list, and adjust stats.
  void MergeIntoFreeList(Span *span);

  // Prepends span to appropriate free list, and adjusts stats.
  void PrependToFreeList(Span *span);

  // Removes span from its free list, and adjust stats.
  void RemoveFromFreeList(Span *span);

  // Incrementally release some memory to the system.
  // IncrementalScavenge(n) is called whenever n pages are freed.
  void IncrementalScavenge(Length n);

  // Release the last span on the normal portion of this list.
  // Return the length of that span.
  Length ReleaseLastNormalSpan(SpanList *slist);

  // Number of pages to deallocate before doing more scavenging
  int64_t scavenge_counter_;

  // Index of last free list where we released memory to the OS.
  unsigned int release_index_;
};

}  // namespace tcm
}  // namespace mm
}  // namespace stonedb

#endif  // STONEDB_MM_PAGE_HEAP_H_
