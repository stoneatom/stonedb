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

#include "sys_heap_policy.h"

#include "common/assert.h"
#include "system/rc_system.h"

namespace stonedb {
namespace mm {

SystemHeap::~SystemHeap() {}

void *SystemHeap::alloc(size_t size) {
  if (m_size > 0 && (m_allocsize + size > m_size)) return NULL;

  void *res = malloc(size);
  m_blockSizes.insert(std::make_pair(res, size));
  m_allocsize += size;
  return res;
}

void SystemHeap::dealloc(void *mh) {
  m_allocsize -= getBlockSize(mh);
  m_blockSizes.erase(mh);
  free(mh);
}

void *SystemHeap::rc_realloc(void *mh, size_t size) {
  m_allocsize -= getBlockSize(mh);
  m_blockSizes.erase(mh);
  void *res = realloc(mh, size);
  m_blockSizes.insert(std::make_pair(res, size));
  m_allocsize += size;
  return res;
}

size_t SystemHeap::getBlockSize(void *mh) {
  auto it = m_blockSizes.find(mh);
  ASSERT(it != m_blockSizes.end(), "Invalid block address");
  return it->second;
}

}  // namespace mm
}  // namespace stonedb
