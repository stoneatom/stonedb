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

  unsigned long getAllocBlocks() { return alloc_blocks_; }
  unsigned long getAllocObjs() { return alloc_objs_; }
  unsigned long getAllocSize() { return alloc_size_; }
  unsigned long getAllocPack() { return alloc_pack_; }
  unsigned long getAllocTemp() { return alloc_temp_; }
  unsigned long getFreeBlocks() { return free_blocks_; }
  unsigned long getAllocTempSize() { return alloc_temp_size_; }
  unsigned long getAllocPackSize() { return alloc_pack_size_; }
  unsigned long getFreePacks() { return free_pack_; }
  unsigned long getFreeTemp() { return free_temp_; }
  unsigned long getFreePackSize() { return free_pack_size_; }
  unsigned long getFreeTempSize() { return free_temp_size_; }
  unsigned long getFreeSize() { return free_size_; }
  unsigned long getReleaseCount() { return release_count_; }
  unsigned long getReleaseTotal() { return release_total_; }
  unsigned long long getReleaseCount1();
  unsigned long long getReleaseCount2();
  unsigned long long getReleaseCount3();
  unsigned long long getReleaseCount4();
  unsigned long long getReloaded();

  void HeapHistogram(std::ostream &);

  void DumpObjs(std::ostream &out);

  using PtrHeapMap = std::unordered_map<void *, HeapPolicy *>;
  std::unordered_map<TraceableObject *, PtrHeapMap *> objs_;

  HeapPolicy *main_heap_ = nullptr;
  HeapPolicy *huge_heap_ = nullptr;
  HeapPolicy *system_ = nullptr;
  HeapPolicy *large_temp_ = nullptr;
  int main_heap_size_ = 0;
  int comp_heap_size_ = 0;  // MB
  bool hard_limit_ = false;
  int release_count_ = 0;
  size_t release_total_ = 0;

  std::recursive_mutex mutex_;
  std::recursive_mutex release_mutex_;

  // status counters
  unsigned long alloc_blocks_ = 0;
  unsigned long alloc_objs_ = 0;
  unsigned long alloc_size_ = 0;
  unsigned long alloc_pack_ = 0;
  unsigned long alloc_temp_ = 0;
  unsigned long free_blocks_ = 0;
  unsigned long alloc_temp_size_ = 0;
  unsigned long alloc_pack_size_ = 0;
  unsigned long free_pack_ = 0;
  unsigned long free_temp_ = 0;
  unsigned long free_pack_size_ = 0;
  unsigned long free_temp_size_ = 0;
  unsigned long free_size_ = 0;

  ReleaseStrategy *release_policy_ = nullptr;
};

}  // namespace mm
}  // namespace Tianmu

#endif  // TIANMU_MM_MEMORY_HANDLING_POLICY_H_
