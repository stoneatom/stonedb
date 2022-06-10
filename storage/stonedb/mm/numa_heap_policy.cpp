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

#include "numa_heap_policy.h"

#include "common/assert.h"
#include "system/rc_system.h"

#ifdef USE_NUMA

#define NUMA_VERSION1_COMPATIBILITY
#include <numa.h>

namespace stonedb {
namespace mm {
NUMAHeap::NUMAHeap(size_t size) : HeapPolicy(size) {
  m_avail = (numa_available() != -1);
  rccontrol << lock << "Numa availability: " << m_avail << unlock;

  if (m_avail) {
    int max_nodes = numa_max_node();
    unsigned long node_size;
    if (max_nodes == 0)
      node_size = size;
    else
      node_size = size / (max_nodes + 1);

    rccontrol << lock << "Numa nodes: " << max_nodes << " size (MB): " << (int)(node_size >> 20) << unlock;
    nodemask_t mask = numa_get_run_node_mask();
    nodemask_t tmp = mask;
    for (int i = 0; i < NUMA_NUM_NODES; i++)
      if (nodemask_isset(&mask, i)) {
        nodemask_zero(&tmp);
        nodemask_set(&tmp, i);
        numa_set_membind(&tmp);
        // TBD: handle node allocation failures
        rccontrol << lock << "Allocating size (MB) " << (int)(node_size >> 20) << " on Numa node " << i << unlock;
        m_nodeHeaps.insert(std::make_pair(i, new TCMHeap(node_size)));
      }
    numa_set_membind(&mask);
  } else {
    m_nodeHeaps.insert(std::make_pair(0, new TCMHeap(size)));
  }
}

NUMAHeap::~NUMAHeap() {
  for (auto iter = m_nodeHeaps.begin(); iter != m_nodeHeaps.end();) {
    delete iter->second;
    auto it2 = iter++;
    m_nodeHeaps.erase(it2);
  }
}

void *NUMAHeap::alloc(size_t size) {
  int node;
  void *result;
  if (m_avail)
    node = numa_preferred();
  else
    node = 0;
  auto h = m_nodeHeaps.find(node);
  ASSERT(h != m_nodeHeaps.end());

  result = h->second->alloc(size);
  if (result != NULL) {
    m_blockHeap.insert(std::make_pair(result, h->second));
    return result;
  } else {
    // TBD: allocate on a least distance basis
    h = m_nodeHeaps.begin();
    while (h != m_nodeHeaps.end()) {
      result = h->second->alloc(size);
      if (result != NULL) {
        m_blockHeap.insert(std::make_pair(result, h->second));
        return result;
      }
      h++;
    }
    return NULL;
  }
}

void NUMAHeap::dealloc(void *mh) {
  auto h = m_blockHeap.find(mh);
  ASSERT(h != m_blockHeap.end());

  h->second->dealloc(mh);
  m_blockHeap.erase(h);
}

void *NUMAHeap::rc_realloc(void *mh, size_t size) {
  auto h = m_blockHeap.find(mh);
  ASSERT(h != m_blockHeap.end());

  return h->second->rc_realloc(mh, size);
}

size_t NUMAHeap::getBlockSize(void *mh) {
  auto h = m_blockHeap.find(mh);
  ASSERT(h != m_blockHeap.end());

  return h->second->getBlockSize(mh);
}
}  // namespace mm
}  // namespace stonedb

#endif
