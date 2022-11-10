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

#include "memory_handling_policy.h"

#include <algorithm>

#include "common/assert.h"
#include "common/exception.h"
#include "core/data_cache.h"
#include "core/pack.h"
#include "core/tools.h"
#include "core/transaction.h"
#include "mm/huge_heap_policy.h"

#include "mm/mysql_heap_policy.h"
#include "mm/numa_heap_policy.h"
#include "mm/release_2q.h"
#include "mm/release_all.h"
#include "mm/release_fifo.h"
#include "mm/release_lru.h"
#include "mm/release_null.h"

#include "mm/sys_heap_policy.h"
#include "mm/tcm_heap_policy.h"
#include "mm/traceable_object.h"
#include "system/fet.h"
#include "system/tianmu_system.h"
#include "tcm/page_heap.h"

namespace Tianmu {
namespace mm {

unsigned long long MemoryHandling::getReleaseCount1() { return release_policy_->getCount1(); }

unsigned long long MemoryHandling::getReleaseCount2() { return release_policy_->getCount2(); }

unsigned long long MemoryHandling::getReleaseCount3() { return release_policy_->getCount3(); }

unsigned long long MemoryHandling::getReleaseCount4() { return release_policy_->getCount4(); }

unsigned long long MemoryHandling::getReloaded() { return release_policy_->getReloaded(); }

MemoryHandling::MemoryHandling([[maybe_unused]] size_t comp_heap_size, size_t uncomp_heap_size, std::string hugedir,
                               [[maybe_unused]] core::DataCache *d, size_t hugesize)
    : alloc_blocks_(0),
      alloc_objs_(0),
      alloc_size_(0),
      alloc_pack_(0),
      alloc_temp_(0),
      free_blocks_(0),
      alloc_temp_size_(0),
      alloc_pack_size_(0),
      free_pack_(0),
      free_temp_(0),
      free_pack_size_(0),
      free_temp_size_(0),
      free_size_(0) {
  int64_t adj_mh_size, lt_size;
  std::string conf_error;
  std::string rpolicy = tianmu_sysvar_mm_releasepolicy;
  std::string hpolicy = tianmu_sysvar_mm_policy;
  int hlimit = tianmu_sysvar_mm_hardlimit;
  float ltemp = tianmu_sysvar_mm_largetempratio / 100.0f;

  // rccontrol << lock << "Large Temp Ratio: " << ltemp << unlock;

  // Do we want to enforce a hard limit
  hard_limit_ = (hlimit > 0);

  // calculate adjusted heap sizes if appropriate
  if (ltemp > 0.0) {
    adj_mh_size = (int64_t)(uncomp_heap_size * (1.0 - ltemp));
    lt_size = uncomp_heap_size - adj_mh_size;
  } else {
    adj_mh_size = uncomp_heap_size;
    lt_size = 0;
  }

  TIANMU_LOG(LogCtl_Level::INFO, "Adjusted Main Heap size = %ld", adj_mh_size);
  TIANMU_LOG(LogCtl_Level::INFO, "Adjusted LT Heap size = %ld", lt_size);

  // Which heaps to use
  if (hpolicy == "system") {
    main_heap_ = new SystemHeap(uncomp_heap_size);
    huge_heap_ = new HugeHeap(hugedir, hugesize);
    system_ = new SystemHeap(0);
    large_temp_ = nullptr;
  } else if (hpolicy == "mysql") {
    main_heap_ = new MySQLHeap(uncomp_heap_size);
    huge_heap_ = new HugeHeap(hugedir, hugesize);
    system_ = new SystemHeap(0);
    large_temp_ = nullptr;
  } else {  // default or ""
#ifdef USE_NUMA
    m_main_heap = new NUMAHeap(adj_mh_size);
    if (lt_size != 0)
      m_large_temp = new NUMAHeap(lt_size);
    else
      m_large_temp = nullptr;
#else
    main_heap_ = new TCMHeap(adj_mh_size);
    if (lt_size != 0)
      large_temp_ = new TCMHeap(lt_size);
    else
      large_temp_ = nullptr;
#endif
    huge_heap_ = new HugeHeap(hugedir, hugesize);
    system_ = new SystemHeap(0);
  }

  tcm::_span_allocator.Init();
  main_heap_size_ = int(adj_mh_size >> 20);
  // set this large enough to get a low number of collisions
  // objs.resize(main_heap_MB);
  objs_.clear();

  // rccontrol << lock << "Release Policy: " << rpolicy << unlock;

  if (rpolicy == "fifo")
    release_policy_ = new ReleaseFIFO();
  else if (rpolicy == "all")
    release_policy_ = new ReleaseALL();
  else if (rpolicy == "lru")
    release_policy_ = new ReleaseLRU();
  else if (rpolicy == "2q")
    release_policy_ = new Release2Q(1024, main_heap_size_ * 4, 128);
  else  // default
    release_policy_ = new Release2Q(1024, main_heap_size_ * 4, 128);
  //_releaseStrat = new ReleaseALL( this );
  //_releaseStrat = new ReleaseNULL( this );
}

MemoryHandling::~MemoryHandling() {
  delete main_heap_;
  delete huge_heap_;
  delete system_;
  delete large_temp_;
  delete release_policy_;
}

void MemoryHandling::DumpObjs(std::ostream &out) {
  std::scoped_lock guard(mutex_);
  out << "  MEMORY DUMP: {" << std::endl;
  for (auto &[obj, heap_map] : objs_) {
    out << "    " << obj->GetCoordinate().ToString() << ", locks: " << obj->NumOfLocks() << ", size allocated "
        << obj->SizeAllocated() << std::endl;
    (void)heap_map;
  }
  out << "  }" << std::endl;
}

void *MemoryHandling::alloc(size_t size, BLOCK_TYPE type, TraceableObject *owner, bool nothrow) {
  MEASURE_FET("MemoryHandling::alloc");
  std::scoped_lock guard(mutex_);
  HeapPolicy *heap;

  ASSERT(size < (size_t)(1 << 31), "Oversized alloc request!");
  // choose appropriate heap for this request
  switch (type) {
    case BLOCK_TYPE::BLOCK_TEMPORARY:
      // Disable huge_heap check as
      // 1. it cann't work
      // 2. it is hardcoded off
      // if (m_huge_heap->getHeapStatus() == HEAP_STATUS::HEAP_SUCCESS)
      //    heap = m_huge_heap;
      if ((large_temp_ != nullptr) && (size >= (tianmu_sysvar_mm_large_threshold * 1_MB)))
        switch (owner->TraceableType()) {
          case TO_TYPE::TO_SORTER:
          case TO_TYPE::TO_CACHEDBUFFER:
          case TO_TYPE::TO_INDEXTABLE:
          case TO_TYPE::TO_TEMPORARY:
            heap = large_temp_;
            break;
          default:
            heap = main_heap_;
            break;
        }
      else
        heap = main_heap_;
      break;
    default:
      heap = main_heap_;
  };

  void *res = heap->alloc(size);
  if (res == nullptr) {
    heap = main_heap_;
    ReleaseMemory(size, nullptr);
    res = heap->alloc(size);

    if (res == nullptr) {
      if (hard_limit_) {
        if (nothrow)
          return res;
        throw common::OutOfMemoryException(size);
      }
      res = system_->alloc(size);
      if (res == nullptr) {
        if (nothrow)
          return res;
        rc_control_.lock(current_txn_->GetThreadID())
            << "Failed to alloc block of size " << static_cast<int>(size) << system::unlock;
        throw common::OutOfMemoryException(size);
      } else {
        heap = system_;
      }
    }
  }
  auto it = objs_.find(owner);
  if (it != objs_.end()) {
    it->second->insert(std::make_pair((void *)res, heap));
  } else {
    alloc_objs_++;
    // TBD: pool these objects
    auto tianmu = new PtrHeapMap();
    tianmu->insert(std::make_pair((void *)res, heap));
    objs_.insert(std::make_pair(owner, tianmu));
  }
  size_t bsize = heap->getBlockSize(res);
  alloc_blocks_++;
  alloc_size_ += bsize;
  if (owner != nullptr)
    if (owner->TraceableType() == TO_TYPE::TO_PACK) {
      alloc_pack_size_ += bsize;
      alloc_pack_++;
    } else {
      // I want this for type = BLOCK_TYPE::BLOCK_TEMPORARY but will take this for now
      alloc_temp_size_ += bsize;
      alloc_temp_++;
    }
  else {
    alloc_temp_size_ += bsize;
    alloc_temp_++;
  }
#ifdef MEM_INIT
  std::memset(res, MEM_INIT, bsize);
#endif
  return res;
}

size_t MemoryHandling::rc_msize(void *mh, TraceableObject *owner) {
  MEASURE_FET("MemoryHandling::rc_msize");

  std::scoped_lock guard(mutex_);
  // if( owner == nullptr || mh == 0 )
  if (mh == 0)
    return 0;
  auto it = objs_.find(owner);
  ASSERT(it != objs_.end(), "MSize Owner not found");

  auto h = (it->second)->find(mh);
  ASSERT(h != it->second->end(), "Did not find msize block in map");
  return h->second->getBlockSize(mh);
}

void MemoryHandling::dealloc(void *mh, TraceableObject *owner) {
  MEASURE_FET("MemoryHandling::dealloc");

  std::scoped_lock guard(mutex_);

  if (mh == nullptr)
    return;

  auto it = objs_.find(owner);
  ASSERT(it != objs_.end(), "DeAlloc owner not found");

  auto h = it->second->find(mh);
  ASSERT(h != it->second->end(), "Dealloc heap not found.");
  int bsize = int(h->second->getBlockSize(mh));
#ifdef MEM_CLEAR
  std::memset(mh, MEM_CLEAR, bsize);
#endif
  free_blocks_++;
  free_size_ += bsize;
  if (owner != nullptr)
    if (owner->TraceableType() == TO_TYPE::TO_PACK) {
      free_pack_size_ += bsize;
      free_pack_++;
    } else {
      free_temp_size_ += bsize;
      free_temp_++;
    }
  else {
    free_temp_size_ += bsize;
    free_temp_++;
  }

  h->second->dealloc(mh);
  it->second->erase(h);
  if (it->second->empty()) {
    delete it->second;
    objs_.erase(it);
    alloc_objs_--;
  }
}

bool MemoryHandling::ReleaseMemory(size_t size, [[maybe_unused]] TraceableObject *untouchable) {
  MEASURE_FET("MemoryHandling::ReleaseMemory");

  bool result = false;
  std::scoped_lock guard(release_mutex_);

  // release 10 packs for every MB requested + 20
  unsigned objs = uint(10 * (size >> 20) + 20);

  release_policy_->Release(objs);
  release_count_++;
  release_total_++;
  // some kind of experiment to reduce the cumulative effects of fragmentation
  // - release all packs that have an allocation outside of main heap on
  // non-small size alloc
  // - dont do this everytime because walking through the entire heap is slow
  if (size > static_cast<size_t>(tianmu_sysvar_cachesizethreshold * 1_MB) ||
      release_count_ > tianmu_sysvar_cachereleasethreshold) {
    release_count_ = 0;
    std::vector<TraceableObject *> dps;

    for (auto &it : objs_) {
      if (it.first == nullptr)
        continue;
      if (it.first->IsLocked() || it.first->TraceableType() != TO_TYPE::TO_PACK)
        continue;

      for (auto &mit : *it.second) {
        if (mit.second == system_) {
          dps.push_back(it.first);
          break;
        }
      }
    }

    for (auto &it : dps) {
      //((Pack*)*iter)->GetOwner()->DropObjectByMM(((Pack*)*iter)->GetPackCoordinate());
      it->Release();
    }
    // RELEASE  unlock A1in ,AM queue
    release_policy_->ReleaseFull();
  }

#if 0
// follow the current tradition of throwing away everything that isn't locked
    std::vector<TraceableObject *> dps;
    auto it = m_objs.begin();
    while (it != m_objs.end()) {
        if ( (*it).first != nullptr )
            if ((*it).first->TraceableType() == TO_TYPE::TO_PACK && (*it).first->IsLocked() == false) {
                if ( (*it).first->IsPrefetchUnused() )
                    (*it).first->clearPrefetchUnused();
                else
                    dps.push_back((*it).first);
                result = true;
            }
        it++;
    }

    auto iter = dps.begin();
    for (iter; iter != dps.end(); iter++)
        ((core::Pack *)*iter)->GetOwner()->DropObjectByMM(((core::Pack *)*iter)->GetPackCoordinate());
#endif
  return result;
}

void *MemoryHandling::rc_realloc(void *mh, size_t size, TraceableObject *owner, BLOCK_TYPE type) {
  MEASURE_FET("MemoryHandling::rc_realloc");

  std::scoped_lock guard(mutex_);
  void *res = alloc(size, type, owner);

  if (mh == nullptr)
    return res;

  size_t oldsize = rc_msize(mh, owner);
  std::memcpy(res, mh, std::min(oldsize, size));
  dealloc(mh, owner);
  return res;
}

void MemoryHandling::ReportLeaks() {
  int blocks = 0;
  size_t size = 0;
  for (auto it : objs_) {
    blocks++;
    for (auto it2 : *(it.second)) {
      size += it2.second->getBlockSize(it2.first);
    }
  }
  if (blocks > 0)
    TIANMU_LOG(LogCtl_Level::WARN, "%d memory block(s) leaked total size = %ld", blocks, size);
}

void MemoryHandling::EnsureNoLeakedTraceableObject() {
  bool error_found = false;
  for (auto it : objs_) {
    if (it.first->IsLocked() && (it.first->NumOfLocks() > 1 || it.first->TraceableType() == TO_TYPE::TO_PACK)) {
      error_found = true;
      TIANMU_LOG(LogCtl_Level::ERROR, "Object @[%ld] locked too many times. Object type: %d, no. locks: %d",
                 long(it.first), int(it.first->TraceableType()), int(it.first->NumOfLocks()));
    }
  }
  ASSERT(!error_found, "Objects locked too many times found.");
}

class SimpleHist {
  static const int bins = 10;
  uint64_t total_count, total_sum, bin_count[bins], bin_total[bins], bin_max[bins];
  const char *label;

  inline void update_bin(int bin, uint64_t value) {
    DEBUG_ASSERT(bin < bins);
    bin_count[bin]++;
    bin_total[bin] += value;
  }

 public:
  SimpleHist(const char *l) : total_count(0), total_sum(0), label(l) {
    for (int i = 0; i < bins; i++) bin_count[i] = bin_total[i] = 0;

    bin_max[0] = 32;
    bin_max[1] = 1_KB;
    bin_max[2] = 4_KB;
    bin_max[3] = 16_KB;
    bin_max[4] = 64_KB;
    bin_max[5] = 512_KB;
    bin_max[6] = 4_MB;
    bin_max[7] = 64_MB;
    bin_max[8] = 256_MB;
    bin_max[9] = 0xffffffffffffffffULL;
  }

  void accumulate(uint64_t value) {
    for (int i = 0; i < 10; i++)
      if (value < bin_max[i]) {
        update_bin(i, value);
        break;
      }
    total_count++;
    total_sum += value;
  }

  void print(std::ostream &out) {
    out << "Histogram for " << label << " " << total_sum << " bytes in " << total_count << " blocks " << std::endl;
    out << "Block size [max] = \t\t(block count,sum(block size) over all "
           "blocks in bin)"
        << std::endl;
    for (int i = 0; i < bins; i++) {
      out << "Block size bin [" << bin_max[i] << "] = \t\t(" << bin_count[i] << "," << bin_total[i] << ")" << std::endl;
    }
    out << std::endl;
  }
};

void MemoryHandling::HeapHistogram(std::ostream &out) {
  unsigned packs_locked = 0;

  SimpleHist main_heap("Main Heap Total"), huge_heap("Huge Heap Total"), system_heap("System Heap Total"),
      large_temp("Large Temporary Heap"), packs("TO_TYPE::TO_PACK objects"), sorter("TO_TYPE::TO_SORTER objects"),
      cbit("TO_TYPE::TO_CACHEDBUFFER+TO_TYPE::TO_INDEXTABLE objects"),
      filter("TO_TYPE::TO_FILTER+TO_TYPE::TO_MULTIFILTER2 objects"),
      rsisplice("TO_TYPE::TO_RSINDEX+TO_TYPE::TO_SPLICE objects"), temp("TO_TYPE::TO_TEMPORARY objects"),
      ftree("TO_TYPE::TO_FTREE objects"), other("other objects");

  std::unordered_map<HeapPolicy *, SimpleHist *> used_blocks;

  used_blocks.insert(std::make_pair(main_heap_, &main_heap));
  // used_blocks.insert( std::make_pair(m_huge_heap,&huge_heap) );
  used_blocks.insert(std::make_pair(system_, &system_heap));
  used_blocks.insert(std::make_pair(large_temp_, &large_temp));

  {
    std::scoped_lock guard(mutex_);

    for (auto it : objs_) {
      SimpleHist *block_type;

      if (it.first->TraceableType() == TO_TYPE::TO_PACK && it.first->IsLocked())
        packs_locked++;

      switch (it.first->TraceableType()) {
        case TO_TYPE::TO_PACK:
          block_type = &packs;
          break;
        case TO_TYPE::TO_SORTER:
          block_type = &sorter;
          break;
        case TO_TYPE::TO_CACHEDBUFFER:
        case TO_TYPE::TO_INDEXTABLE:
          block_type = &cbit;
          break;
        case TO_TYPE::TO_FILTER:
        case TO_TYPE::TO_MULTIFILTER2:
          block_type = &filter;
          break;
        case TO_TYPE::TO_RSINDEX:
        case TO_TYPE::TO_SPLICE:
          block_type = &rsisplice;
          break;
        case TO_TYPE::TO_TEMPORARY:
          block_type = &temp;
          break;
        case TO_TYPE::TO_FTREE:
          block_type = &ftree;
          break;
        default:
          block_type = &other;
      }
      for (auto mit : *(it.second)) {
        void *ptr = mit.first;
        HeapPolicy *hp = mit.second;
        auto hist = used_blocks.find(hp);

        if (hist != used_blocks.end()) {
          hist->second->accumulate(hp->getBlockSize(ptr));
          if (block_type != nullptr)
            block_type->accumulate(hp->getBlockSize(ptr));
        }
      }
    }
  }  // close scope for m_mutex guard

  // rccontrol << lock;
  for (auto h : used_blocks) {
    h.second->print(out);
  }
  packs.print(out);
  sorter.print(out);
  cbit.print(out);
  filter.print(out);
  rsisplice.print(out);
  temp.print(out);
  ftree.print(out);
  other.print(out);

  DumpObjs(out);

  out << "Total number of locked packs: " << packs_locked << std::endl;
  // rccontrol << unlock;
}

void MemoryHandling::AssertNoLeak(TraceableObject *o) {
  std::scoped_lock guard(mutex_);
  ASSERT(objs_.find(o) == objs_.end(), "MemoryLeakAssertion");
}

void MemoryHandling::TrackAccess(TraceableObject *o) {
  MEASURE_FET("MemoryHandling::TrackAccess");
  std::scoped_lock guard(release_mutex_);
  release_policy_->Access(o);
}

void MemoryHandling::StopAccessTracking(TraceableObject *o) {
  MEASURE_FET("MemoryHandling::TrackAccess");
  std::scoped_lock guard(release_mutex_);

  if (o->IsTracked())
    release_policy_->Remove(o);
}

}  // namespace mm
}  // namespace Tianmu
