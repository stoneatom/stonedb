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
#include "tcm_heap_policy.h"

#include <algorithm>
#include <cstring>

#include "common/assert.h"
#include "core/tools.h"
#include "tcm/span.h"

namespace stonedb {
namespace mm {

TCMHeap::TCMHeap(size_t heap_size) : HeapPolicy(heap_size) {
  if (heap_size > 0) m_heap.GrowHeap((heap_size) >> kPageShift);
  m_sizemap.Init();
  for (size_t i = 0; i < kNumClasses; i++) m_freelist[i].Init();
}

void *TCMHeap::alloc(size_t size) {
  void *res;

  // make size a multiple of 16
  // size = size + 0xf & (~0xf);

  if (size > kMaxSize) {
    int pages = int(size >> kPageShift);
    tcm::Span *s = m_heap.New(pages + 1);
    if (s == NULL) return NULL;
    s->objects = NULL;
    s->next = NULL;
    s->prev = NULL;
    s->refcount = 1;
    s->size = uint(size);
    s->sizeclass = 0;
    m_heap.RegisterSizeClass(s, 0);
    return reinterpret_cast<void *>(s->start << kPageShift);
  }
  const size_t cl = size_t(m_sizemap.SizeClass((int)size));
  const size_t alloc_size = m_sizemap.ByteSizeForClass(cl);
  FreeList *list = &m_freelist[cl];
  if (list->empty()) {
    // Get a new span of pages (potentially large enough for multiple
    // allocations of this size)
    int pages = int(m_sizemap.class_to_pages(cl));
    tcm::Span *res = m_heap.New(pages);
    if (res == NULL) return NULL;

    m_heap.RegisterSizeClass(res, cl);

    // from CentralFreeList::Populate
    // initialize the span of pages into a "list" of smaller objects
    void **tail = &res->objects;
    char *ptr = reinterpret_cast<char *>(res->start << kPageShift);
    char *limit = ptr + (pages << kPageShift);
    int num = 0;
    while (ptr + alloc_size <= limit) {
      *tail = ptr;
      tail = reinterpret_cast<void **>(ptr);
      ptr += alloc_size;
      num++;
    }
    ASSERT(ptr <= limit);
    *tail = NULL;
    res->refcount = 0;
    list->PushRange(num, (void *)(res->start << kPageShift), tail);
  }
  if (list->empty()) return NULL;

  res = list->Pop();
  tcm::Span *s = m_heap.GetDescriptor(ADDR_TO_PAGEID(res));
  s->refcount++;
  return res;
}

size_t TCMHeap::getBlockSize(void *mh) {
  size_t result;
  tcm::Span *span = m_heap.GetDescriptor(ADDR_TO_PAGEID(mh));
  ASSERT(span != NULL);
  if (span->sizeclass == 0)
    result = span->size;
  else
    result = m_sizemap.ByteSizeForClass(span->sizeclass);

  ASSERT(result != 0, "Block size error");
  return result;
}

void TCMHeap::dealloc(void *mh) {
  // be an enabler for broken code
  if (mh == NULL) return;

  tcm::Span *span = m_heap.GetDescriptor(ADDR_TO_PAGEID(mh));
  ASSERT(span != NULL);
  span->refcount--;
  if (span->sizeclass == 0) {
    m_heap.Delete(span);
  } else {
    int sclass = span->sizeclass;
    if (span->refcount == 0) {
      m_freelist[sclass].RemoveRange(
          reinterpret_cast<void *>(span->start << kPageShift),
          reinterpret_cast<void *>((span->start << kPageShift) + (span->length * kPageSize) - 1));
      m_heap.Delete(span);
    } else {
      m_freelist[sclass].Push(mh);
    }
  }
}

void *TCMHeap::rc_realloc(void *mh, size_t size) {
  void *res = alloc(size);

  if (mh == NULL) return res;
  tcm::Span *span = m_heap.GetDescriptor(ADDR_TO_PAGEID(mh));
  if (span->sizeclass == 0) {
    std::memcpy(res, mh, std::min(span->length * kPageSize, size));
    dealloc(mh);
  } else {
    std::memcpy(res, mh, std::min(m_sizemap.ByteSizeForClass(span->sizeclass), size));
    dealloc(mh);
  }

  return res;
}

}  // namespace mm
}  // namespace stonedb
