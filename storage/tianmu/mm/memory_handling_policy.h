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
  std::unordered_map<TraceableObject *, PtrHeapMap *> m_objs_;

  HeapPolicy *m_main_heap_, *m_huge_heap_, *m_system_, *m_large_temp_;
  int main_heap_size_, comp_heap_size_;  // MB
  bool m_hard_limit_;
  int m_release_count_ = 0;
  size_t m_release_total_ = 0;

  std::recursive_mutex m_mutex_;
  std::recursive_mutex m_release_mutex_;

  // status counters
  unsigned long m_alloc_blocks_, m_alloc_objs_, m_alloc_size_, m_alloc_pack_, m_alloc_temp_, m_free_blocks_,
      m_alloc_temp_size_, m_alloc_pack_size_, m_free_pack_, m_free_temp_, m_free_pack_size_, m_free_temp_size_,
      m_free_size_;

  ReleaseStrategy *release_policy_ = nullptr;

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

  unsigned long getAllocBlocks() { return m_alloc_blocks_; }
  unsigned long getAllocObjs() { return m_alloc_objs_; }
  unsigned long getAllocSize() { return m_alloc_size_; }
  unsigned long getAllocPack() { return m_alloc_pack_; }
  unsigned long getAllocTemp() { return m_alloc_temp_; }
  unsigned long getFreeBlocks() { return m_free_blocks_; }
  unsigned long getAllocTempSize() { return m_alloc_temp_size_; }
  unsigned long getAllocPackSize() { return m_alloc_pack_size_; }
  unsigned long getFreePacks() { return m_free_pack_; }
  unsigned long getFreeTemp() { return m_free_temp_; }
  unsigned long getFreePackSize() { return m_free_pack_size_; }
  unsigned long getFreeTempSize() { return m_free_temp_size_; }
  unsigned long getFreeSize() { return m_free_size_; }
  unsigned long getReleaseCount() { return m_release_count_; }
  unsigned long getReleaseTotal() { return m_release_total_; }
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
