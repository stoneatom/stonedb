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
#ifndef STONEDB_MM_TCM_HEAP_POLICY_H_
#define STONEDB_MM_TCM_HEAP_POLICY_H_
#pragma once

#include <map>
#include <string>

#include "mm/heap_policy.h"
#include "mm/memory_block.h"

#include "tcm/linked_list.h"
#include "tcm/page_heap.h"
#include "tcm/page_heap_allocator.h"
#include "tcm/tccommon.h"

namespace stonedb {
namespace mm {

class TCMHeap : public HeapPolicy {
 protected:
  tcm::PageHeap m_heap;
  tcm::SizeMap m_sizemap;

  // from tcmalloc::ThreadCache
  class FreeList {
   private:
    void *list_;       // Linked list of nodes
    uint32_t length_;  // Current length.

   public:
    void Init() {
      list_ = NULL;
      length_ = 0;
    }

    // Is list empty?
    bool empty() const { return list_ == NULL; }

    void Push(void *ptr) {
      tcm::SLL_Push(&list_, ptr);
      length_++;
    }

    void *Pop() {
      ASSERT(list_ != NULL);
      length_--;
      return tcm::SLL_Pop(&list_);
    }

    void PushRange(int N, void *start, void *end) {
      tcm::SLL_PushRange(&list_, start, end);
      length_ += N;
    }

    void RemoveRange(void *low, void *hi) {
      void *prev, *current;
      while (list_ <= hi && list_ >= low) {
        list_ = tcm::SLL_Next(list_);
      }
      current = list_;
      while (current != NULL) {
        prev = current;
        current = tcm::SLL_Next(current);
        while (current <= hi && current >= low) {
          tcm::SLL_SetNext(prev, tcm::SLL_Next(current));
          current = tcm::SLL_Next(current);
        }
      }
    }
  };

  FreeList m_freelist[kNumClasses];  // Array indexed by size-class

 public:
  TCMHeap(size_t hsize);
  virtual ~TCMHeap() = default;

  /*
      allocate memory block of size [size] and for data of type [type]
      type != BLOCK_TYPE::BLOCK_FREE
  */
  void *alloc(size_t size) override;
  void dealloc(void *mh) override;
  void *rc_realloc(void *mh, size_t size) override;
  size_t getBlockSize(void *mh) override;
};

}  // namespace mm
}  // namespace stonedb

#endif  // STONEDB_MM_TCM_HEAP_POLICY_H_
