//<!***************************************************************************
/**
 * Copyright (C) 2021-2022 StoneAtom Group Holding Limited
 */
//<!***************************************************************************
#ifndef STONEDB_MM_NUMA_HEAP_POLICY_H_
#define STONEDB_MM_NUMA_HEAP_POLICY_H_
#pragma once

#include <unordered_map>
#include "mm/heap_policy.h"
#include "mm/tcm_heap_policy.h"

namespace stonedb {
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
}  // namespace stonedb

#endif  // STONEDB_MM_NUMA_HEAP_POLICY_H_
