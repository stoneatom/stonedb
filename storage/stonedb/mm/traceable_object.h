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
#ifndef STONEDB_MM_TRACEABLE_OBJECT_H_
#define STONEDB_MM_TRACEABLE_OBJECT_H_
#pragma once

#include <atomic>

#include "common/assert.h"
#include "core/tools.h"
#include "mm/memory_handling_policy.h"

namespace stonedb {

namespace core {
class TOCoordinate;
class DataCache;
class Pack;
}  // namespace core

class MemoryHandling;

namespace mm {

enum class TO_TYPE {
  TO_PACK = 0,
  TO_SORTER,
  TO_CACHEDBUFFER,
  TO_FILTER,
  TO_MULTIFILTER2,
  TO_INDEXTABLE,
  TO_RSINDEX,
  TO_TEMPORARY,
  TO_FTREE,
  TO_SPLICE,
  TO_INITIALIZER,
  TO_DEPENDENT,  // object is dependent from other one
  TO_REFERENCE,  // Logical reference to an object on disk
  TO_DATABLOCK
};

class TraceableAccounting;
class ReleaseTracker;

class TraceableObject {
  friend class core::DataCache;  // from core
  friend class TraceableAccounting;
  friend class ReleaseTracker;
  friend class ReleaseStrategy;

 public:
  using UniquePtr = std::unique_ptr<void, std::function<void(void *)>>;

  TraceableObject();
  TraceableObject(size_t, size_t, std::string = "", core::DataCache * = NULL, size_t = 0);
  TraceableObject(const TraceableObject &to);
  virtual ~TraceableObject();

  virtual TO_TYPE TraceableType() const = 0;

  void Lock();
  void Unlock();
  static void UnlockAndResetOrDeletePack(std::shared_ptr<core::Pack> &pack);
  static void UnlockAndResetPack(std::shared_ptr<core::Pack> &pack);

  static int MemorySettingsScale();             // Memory scale factors: 0 - <0.5GB, 1 -
                                                // <1.2GB, 2 - <2.5GB,
                                                // ..., 99 - higher than all these
  static int64_t MaxBufferSize(int coeff = 0);  // how much bytes may be used for typical large buffers
  // WARNING: potentially slow function (e.g. on Linux)
  static int64_t MaxBufferSizeForAggr(int64_t size = 0);  // as above but can recommend large buffers for large
                                                          // memory machines

  // Lock the object during destruction to prevent garbage collection
  void DestructionLock();

  bool IsLocked() const { return m_lock_count > 0; }
  bool LastLock() const { return m_lock_count == 1; }
  // Locking is used by RCAttr etc. for packs manipulation purposes
  short NoLocks() const { return m_lock_count; }
  void SetNoLocks(int n);

  void SetOwner(core::DataCache *new_owner) { owner = new_owner; }
  core::DataCache *GetOwner() { return owner; }
  static size_t GetFreeableSize() { return globalFreeable; }
  static size_t GetUnFreeableSize() { return globalUnFreeable; }
  // DataPacks can be prefetched but not used yet
  // this is a hint to memory release algorithm
  bool IsPrefetchUnused() { return m_preUnused; }
  void clearPrefetchUnused() { m_preUnused = false; }
  static MemoryHandling *Instance() {
    if (!m_MemHandling) {
      // m_MemHandling = new MemoryHandling<void*, BasicHeap<void*> >(COMP_SIZE,
      // UNCOMP_SIZE, COMPRESSED_HEAP_RELEASE, UNCOMPRESSED_HEAP_RELEASE);
      STONEDB_ERROR("Memory manager is not instantiated");
      return NULL;
    } else
      return m_MemHandling;
  }

  void TrackAccess() { Instance()->TrackAccess(this); }
  void StopAccessTracking() { Instance()->StopAccessTracking(this); }
  bool IsTracked() { return tracker != NULL; }
  virtual void Release() { STONEDB_ERROR("Release functionality not implemented for this object"); }
  core::TOCoordinate &GetCoordinate();

  size_t SizeAllocated() const { return m_sizeAllocated; }

 protected:
  // For release tracking purposes, used by ReleaseTracker and ReleaseStrategy
  TraceableObject *next, *prev;
  ReleaseTracker *tracker;

  UniquePtr alloc_ptr(size_t size, BLOCK_TYPE type, bool nothrow = false);

  void *alloc(size_t size, BLOCK_TYPE type, bool nothrow = false);
  void dealloc(void *ptr);
  void *rc_realloc(void *ptr, size_t size, BLOCK_TYPE type = BLOCK_TYPE::BLOCK_FIXED);
  size_t rc_msize(void *ptr);

  void deinitialize(bool detect_leaks);

  static std::recursive_mutex &GetLockingMutex() { return Instance()->m_release_mutex; }
  static MemoryHandling *m_MemHandling;

  static MemoryHandling *Instance(size_t comp_size, size_t uncomp_size, std::string hugedir = "",
                                  core::DataCache *d = NULL, size_t hugesize = 0) {
    if (!m_MemHandling) m_MemHandling = new MemoryHandling(comp_size, uncomp_size, hugedir, d, hugesize);
    return m_MemHandling;
  }

  bool m_preUnused;

  static std::atomic_size_t globalFreeable;
  static std::atomic_size_t globalUnFreeable;

  size_t m_sizeAllocated;

  core::DataCache *owner = nullptr;

  std::recursive_mutex &m_locking_mutex;
  core::TOCoordinate m_coord;

 private:
  short m_lock_count = 1;
  static int64_t MemScale2BufSizeLarge(int ms);
  static int64_t MemScale2BufSizeSmall(int ms);
};

}  // namespace mm
}  // namespace stonedb

#endif  // STONEDB_MM_TRACEABLE_OBJECT_H_
