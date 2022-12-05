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
#ifndef TIANMU_MM_MEMORY_HANDLING_POLICY_H_
#define TIANMU_MM_MEMORY_HANDLING_POLICY_H_
#pragma once

#include <mutex>
#include <unordered_map>

#include "common/assert.h"
#include "mm/memory_block.h"
namespace Tianmu {
namespace core {
class DataCache;
}  // namespace core

namespace mm {
class TraceableObject;
class ReleaseStrategy;
class HeapPolicy;
// Systemwide memory responsibilities
//  -- alloc/dealloc on any heap by delegating
//  -- track objects
class MemoryHandling {
  friend class TraceableObject;

  using PtrHeapMap = std::unordered_map<void *, HeapPolicy *>;
  std::unordered_map<TraceableObject *, PtrHeapMap *> m_objs;

  HeapPolicy *m_main_heap, *m_huge_heap, *m_system, *m_large_temp;
  int main_heap_MB, comp_heap_MB;
  bool m_hard_limit;
  unsigned int m_release_count = 0;
  size_t m_release_total = 0;

  std::recursive_mutex m_mutex;
  std::recursive_mutex m_release_mutex;

  // status counters
  unsigned long m_alloc_blocks, m_alloc_objs, m_alloc_size, m_alloc_pack, m_alloc_temp, m_free_blocks,
      m_alloc_temp_size, m_alloc_pack_size, m_free_pack, m_free_temp, m_free_pack_size, m_free_temp_size, m_free_size;

  ReleaseStrategy *_releasePolicy = nullptr;

  void DumpObjs(std::ostream &out);

 public:
  MemoryHandling(size_t comp_heap_size, size_t uncomp_heap_size, std::string hugedir = "", core::DataCache *d = nullptr,
                 size_t hugesize = 0);
  virtual ~MemoryHandling();

  void *alloc(size_t size, BLOCK_TYPE type, TraceableObject *owner, bool nothrow = false);
  void dealloc(void *mh, TraceableObject *owner);
  void *rc_realloc(void *mh, size_t size, TraceableObject *owner, BLOCK_TYPE type);
  size_t rc_msize(void *mh,
                  TraceableObject *owner);  // returns size of a memory block
                                            // represented by [mh]

  void TrackAccess(TraceableObject *);
  void StopAccessTracking(TraceableObject *);

  void AssertNoLeak(TraceableObject *);
  bool ReleaseMemory(size_t, TraceableObject *untouchable);  // Release given amount of memory

  void ReportLeaks();
  void EnsureNoLeakedTraceableObject();
  void CompressPacks();

  unsigned long getAllocBlocks() { return m_alloc_blocks; }
  unsigned long getAllocObjs() { return m_alloc_objs; }
  unsigned long getAllocSize() { return m_alloc_size; }
  unsigned long getAllocPack() { return m_alloc_pack; }
  unsigned long getAllocTemp() { return m_alloc_temp; }
  unsigned long getFreeBlocks() { return m_free_blocks; }
  unsigned long getAllocTempSize() { return m_alloc_temp_size; }
  unsigned long getAllocPackSize() { return m_alloc_pack_size; }
  unsigned long getFreePacks() { return m_free_pack; }
  unsigned long getFreeTemp() { return m_free_temp; }
  unsigned long getFreePackSize() { return m_free_pack_size; }
  unsigned long getFreeTempSize() { return m_free_temp_size; }
  unsigned long getFreeSize() { return m_free_size; }
  unsigned long getReleaseCount() { return m_release_count; }
  unsigned long getReleaseTotal() { return m_release_total; }
  unsigned long long getReleaseCount1();
  unsigned long long getReleaseCount2();
  unsigned long long getReleaseCount3();
  unsigned long long getReleaseCount4();
  unsigned long long getReloaded();

  void HeapHistogram(std::ostream &);
};

}  // namespace mm
}  // namespace Tianmu

#endif  // TIANMU_MM_MEMORY_HANDLING_POLICY_H_
