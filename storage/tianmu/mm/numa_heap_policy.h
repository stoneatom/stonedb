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

#ifndef TIANMU_MM_NUMA_HEAP_POLICY_H_
#define TIANMU_MM_NUMA_HEAP_POLICY_H_
#pragma once

#include <unordered_map>
#include "mm/heap_policy.h"
#include "mm/tcm_heap_policy.h"

namespace Tianmu {
namespace mm {
#ifdef USE_NUMA

class NUMAHeap : public HeapPolicy {
 public:
  NUMAHeap(size_t);
  virtual ~NUMAHeap();

  /*
      allocate memory block of size [size] and for data of type [type]
      type != BLOCK_TYPE::BLOCK_FREE
  */
  void *alloc(size_t size);
  void dealloc(void *mh);
  void *rc_realloc(void *mh, size_t size);

  size_t getBlockSize(void *mh);

 private:
  std::unordered_map<int, TCMHeap *> m_nodeHeaps;
  std::unordered_map<void *, TCMHeap *> m_blockHeap;

  bool m_avail;
};

#else
using NUMAHeap = TCMHeap;
#endif
}  // namespace mm
}  // namespace Tianmu

#endif  // TIANMU_MM_NUMA_HEAP_POLICY_H_
