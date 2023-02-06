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

#include "traceable_object.h"

#include "common/assert.h"
#include "core/engine.h"
#include "core/pack.h"
#include "core/tools.h"
#include "mm/release_tracker.h"

#include "system/fet.h"
#include "system/res_manager.h"
#include "system/tianmu_system.h"

namespace Tianmu {
namespace mm {

MemoryHandling *TraceableObject::m_MemHandling = nullptr;

std::atomic_size_t TraceableObject::globalFreeable;
std::atomic_size_t TraceableObject::globalUnFreeable;

//   static const char *TypeName(TO_TYPE tt)
//   {
//       static const char *arr[] = {
//           "TO_TYPE::TO_PACK",        "TO_TYPE::TO_SORTER",    "TO_TYPE::TO_CACHEDBUFFER", "TO_TYPE::TO_FILTER",
//           "TO_TYPE::TO_MULTIFILTER2", "TO_TYPE::TO_INDEXTABLE",  "TO_TYPE::TO_RSINDEX", "TO_TYPE::TO_TEMPORARY",
//           "TO_TYPE::TO_FTREE",     "TO_TYPE::TO_SPLICE", "TO_TYPE::TO_INITIALIZER", "TO_TYPE::TO_DEPENDENT",
//           "TO_TYPE::TO_REFERENCE",    "TO_TYPE::TO_DATABLOCK",
//
//       };
//       return arr[tt];
//   }

TraceableObject::UniquePtr TraceableObject::alloc_ptr(size_t size, BLOCK_TYPE type, bool nothrow) {
  return UniquePtr(alloc(size, type, nothrow), [this](void *ptr) { dealloc(ptr); });
}

void *TraceableObject::alloc(size_t size, BLOCK_TYPE type, bool nothrow) {
  void *addr = Instance()->alloc(size, type, this, nothrow);
  if (addr != nullptr) {
    size_t s = Instance()->rc_msize(addr, this);
    m_sizeAllocated += s;
    if (!IsLocked() && TraceableType() == TO_TYPE::TO_PACK)
      globalFreeable += s;
    else
      globalUnFreeable += s;
  }
  return addr;
}

void TraceableObject::dealloc(void *ptr) {
  size_t s;
  if (ptr == nullptr)
    return;
  s = Instance()->rc_msize(ptr, this);
  Instance()->dealloc(ptr, this);
  m_sizeAllocated -= s;
  if (!IsLocked() && TraceableType() == TO_TYPE::TO_PACK)
    globalFreeable -= s;
  else
    globalUnFreeable -= s;
}

void *TraceableObject::rc_realloc(void *ptr, size_t size, BLOCK_TYPE type) {
  if (ptr == nullptr)
    return alloc(size, type);

  size_t s1 = Instance()->rc_msize(ptr, this);
  void *addr = Instance()->rc_realloc(ptr, size, this, type);
  if (addr != nullptr) {
    size_t s = Instance()->rc_msize(addr, this);
    m_sizeAllocated += s;
    m_sizeAllocated -= s1;
    if (!IsLocked() && TraceableType() == TO_TYPE::TO_PACK) {
      globalFreeable += s;
      globalFreeable -= s1;
    } else {
      globalUnFreeable += s;
      globalUnFreeable -= s1;
    }
  }
  return addr;
}

size_t TraceableObject::rc_msize(void *ptr) { return Instance()->rc_msize(ptr, this); }

TraceableObject::TraceableObject()
    : next(nullptr),
      prev(nullptr),
      tracker(nullptr),
      m_preUnused(false),
      m_sizeAllocated(0),
      m_locking_mutex(Instance()->m_release_mutex) {}
TraceableObject::TraceableObject(size_t comp_size, size_t uncomp_size, std::string hugedir, core::DataCache *owner_,
                                 size_t hugesize)
    : next(nullptr),
      prev(nullptr),
      tracker(nullptr),
      m_preUnused(false),
      m_sizeAllocated(0),
      owner(owner_),
      m_locking_mutex(Instance(comp_size, uncomp_size, hugedir, owner_, hugesize)->m_release_mutex) {}

TraceableObject::TraceableObject(const TraceableObject &to)
    : next(nullptr),
      prev(nullptr),
      tracker(nullptr),
      m_preUnused(false),
      m_sizeAllocated(0),
      m_locking_mutex(Instance()->m_release_mutex),
      m_coord(to.m_coord) {}

TraceableObject::~TraceableObject() {
  // Instance()->AssertNoLeak(this);
  DEBUG_ASSERT(m_sizeAllocated == 0 /*, "TraceableObject size accounting"*/);
  if (IsTracked())
    StopAccessTracking();
}

void TraceableObject::Lock() {
  MEASURE_FET("TraceableObject::Lock");
  if (TraceableType() == TO_TYPE::TO_PACK) {
    std::scoped_lock locking_guard(m_locking_mutex);
    clearPrefetchUnused();
    if (!IsLocked()) {
      globalFreeable -= m_sizeAllocated;
      globalUnFreeable += m_sizeAllocated;
    }
    m_lock_count++;

    // Locking is done externally
    // if(!((Pack*) this)->IsEmpty() && IsCompressed()) Uncompress();
  } else {
    std::scoped_lock locking_guard(m_locking_mutex);
    m_lock_count++;
  }
  if (m_lock_count == 32766) {
    std::string message =
        "TraceableObject locked too many times. Object type: " + std::to_string((int)this->TraceableType());
    tianmu_control_ << system::lock << message << system::unlock;
    TIANMU_ERROR(message.c_str());
  }
}

void TraceableObject::SetNumOfLocks(int n) {
  if (TraceableType() == TO_TYPE::TO_PACK) {
    std::scoped_lock locking_guard(m_locking_mutex);
    clearPrefetchUnused();
    if (!IsLocked()) {
      globalFreeable -= m_sizeAllocated;
      globalUnFreeable += m_sizeAllocated;
    }
    m_lock_count = n;

    // Locking is done externally
    // if(n > 0 && !((Pack*) this)->IsEmpty() && IsCompressed()) Uncompress();
  } else {
    std::scoped_lock locking_guard(m_locking_mutex);
    m_lock_count = n;
  }
  if (m_lock_count == 32766) {
    std::string message =
        "TraceableObject locked too many times. Object type: " + std::to_string((int)this->TraceableType());
    tianmu_control_ << system::lock << message << system::unlock;
    TIANMU_ERROR(message.c_str());
  }
}

void TraceableObject::Unlock() {
  MEASURE_FET("TraceableObject::UnLock");
  std::scoped_lock locking_guard(m_locking_mutex);
  m_lock_count--;
  if (m_lock_count < 0) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Internal error: An object unlocked too many times in memory manager.");
    DEBUG_ASSERT(!"Internal error: An object unlocked too many times in memory manager.");
    m_lock_count = 0;
  }
  if (!IsLocked() && TraceableType() == TO_TYPE::TO_PACK) {
    globalUnFreeable -= m_sizeAllocated;
    globalFreeable += m_sizeAllocated;
  }
}

void TraceableObject::UnlockAndResetOrDeletePack(std::shared_ptr<core::Pack> &pack) {
  bool reset_outside = false;
  {
    std::scoped_lock locking_guard(GetLockingMutex());
    if (pack.use_count() > 1) {
      pack->Unlock();
      pack.reset();
    } else
      reset_outside = true;
  }
  if (reset_outside)
    pack.reset();
}

void TraceableObject::UnlockAndResetPack(std::shared_ptr<core::Pack> &pack) {
  std::scoped_lock locking_guard(GetLockingMutex());
  pack->Unlock();
  pack.reset();
}

void TraceableObject::DestructionLock() {
  std::scoped_lock locking_guard(m_locking_mutex);
  if (!IsLocked() && TraceableType() == TO_TYPE::TO_PACK) {
    globalUnFreeable += m_sizeAllocated;
    globalFreeable -= m_sizeAllocated;
  }
  m_lock_count++;
}

int TraceableObject::MemorySettingsScale() { return ha_tianmu_engine_->getResourceManager()->GetMemoryScale(); }

void TraceableObject::deinitialize(bool detect_leaks) {
  if (TraceableType() != TO_TYPE::TO_INITIALIZER) {
    TIANMU_ERROR("TraceableType() not equals 'TO_TYPE::TO_INITIALIZER'");
    return;
  }

  if (m_MemHandling) {  // Used only in MemoryManagerInitializer!
    if (detect_leaks)
      m_MemHandling->ReportLeaks();
    delete m_MemHandling;
  }
}

int64_t TraceableObject::MemScale2BufSizeLarge(int ms)

{
  DEBUG_ASSERT(ms > 2);
  size_t max_total_size = (ms - 2) * 512_MB;
  int sc16 = ms - 5;
  // additionally add progressively more mem for each additional 16GB of free
  // mem : +2GB for 16 Gb free, +3GB for 32Gb free, +6GB if 48GB free
  auto to_add = 2_GB;
  // single user:
  //	int64_t to_add = 8_GB;
  for (; sc16 > 0; sc16--) {
    max_total_size += to_add;
    if (to_add < 12_GB)
      to_add += 1_GB;
  }
  return max_total_size;
}

int64_t TraceableObject::MemScale2BufSizeSmall(int mem_scale) {
  int64_t max_total_size;
  if (mem_scale < 0)  // low settings with negative coefficient
    max_total_size = 32_MB;
  else if (mem_scale == 0)  // low settings (not more than 0.5 GB)
    max_total_size = 64_MB;
  else if (mem_scale <= 2)  // normal settings (up to 1.2 GB is reserved)
    max_total_size = 128_MB;
  else if (mem_scale == 3)  // high settings (when 1.2 - 2.5 GB is reserved in
                            // 64-bit version)
    max_total_size = 256_MB;
  else if (mem_scale == 4)  // for greater concurrency , 2.5-40G all set 512M
    max_total_size = 512_MB;
  else
    max_total_size = 1_GB;

  return max_total_size;
}

int64_t TraceableObject::MaxBufferSize(int coeff)  // how much bytes may be used for typical large buffers
{
  int mem_scale = MemorySettingsScale() + coeff;
  return MemScale2BufSizeSmall(mem_scale);
}

int64_t TraceableObject::MaxBufferSizeForAggr(int64_t size)  // how much bytes may be used for buffers for aggregation
{
  int64_t memsize = 0;
  int mem_scale = MemorySettingsScale();
  if (mem_scale <= 2)
    memsize = MemScale2BufSizeSmall(mem_scale);
  else
    memsize = MemScale2BufSizeLarge(mem_scale);

  memsize = (memsize > size) ? size : memsize;

  return memsize;
}

core::TOCoordinate &TraceableObject::GetCoordinate() { return m_coord; }

}  // namespace mm
}  // namespace Tianmu
