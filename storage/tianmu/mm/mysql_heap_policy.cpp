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

#include "mysql_heap_policy.h"

#include "common/assert.h"
#include "my_sys.h"
#include "system/tianmu_system.h"

namespace Tianmu {
namespace mm {

MySQLHeap::~MySQLHeap() {}

void *MySQLHeap::alloc(size_t size) {
  void *res = my_malloc(PSI_NOT_INSTRUMENTED, size, 0);
  m_blockSizes.insert(std::make_pair(res, size));
  return res;
}

void MySQLHeap::dealloc(void *mh) {
  m_blockSizes.erase(mh);
  my_free(mh);
}

void *MySQLHeap::rc_realloc(void *mh, size_t size) {
  m_blockSizes.erase(mh);
  void *res = my_realloc(PSI_NOT_INSTRUMENTED, mh, size, MY_ALLOW_ZERO_PTR);
  m_blockSizes.insert(std::make_pair(res, size));
  return res;
}

size_t MySQLHeap::getBlockSize(void *mh) {
  auto it = m_blockSizes.find(mh);
  ASSERT(it != m_blockSizes.end(), "Invalid block address");
  return it->second;
}

}  // namespace mm
}  // namespace Tianmu
