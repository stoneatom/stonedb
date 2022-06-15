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

#include "huge_heap_policy.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>

#include "system/rc_system.h"

namespace stonedb {
namespace mm {

HugeHeap::HugeHeap(std::string hugedir, size_t size) : TCMHeap(0) {
  m_heap_frame = NULL;
  // convert size from MB to B and make it a multiple of 2MB
  m_size = 1_MB * (size & ~0x1);
  m_stonedb = HEAP_STATUS::HEAP_ERROR;
  m_fd = -1;

  if (!hugedir.empty() && size > 0) {
    char pidtext[12];
    std::strcpy(m_hugefilename, hugedir.c_str());
    std::sprintf(pidtext, "%d", getpid());
    std::strcat(m_hugefilename, "/sdbhuge.");
    std::strcat(m_hugefilename, pidtext);
    m_fd = open(m_hugefilename, O_CREAT | O_RDWR, 0700);
    if (m_fd < 0) {
      m_stonedb = HEAP_STATUS::HEAP_OUT_OF_MEMORY;
      rccontrol << system::lock << "Memory Manager Error: Unable to create hugepage file: " << m_hugefilename
                << system::unlock;
      return;
    }
    // MAP_SHARED to have mmap fail immediately if not enough pages
    // MAP_PRIVATE does copy on write
    // MAP_POPULATE to create page table entries and avoid future surprises
    m_heap_frame = (char *)mmap(NULL, m_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, m_fd, 0);
    if (m_heap_frame == MAP_FAILED) {
      unlink(m_hugefilename);
      m_stonedb = HEAP_STATUS::HEAP_OUT_OF_MEMORY;
      rccontrol << system::lock << "Memory Manager Error: hugepage file mmap error: " << std::strerror(errno)
                << system::unlock;
      return;
    }

    rccontrol << system::lock << "Huge Heap size (MB) " << (int)(size) << system::unlock;
    // m_size = size;
    // manage the region as a normal 4k pagesize heap
    m_heap.RegisterArea(m_heap_frame, m_size >> kPageShift);
    m_size = size;
    m_stonedb = HEAP_STATUS::HEAP_SUCCESS;
  }
}

HugeHeap::~HugeHeap() {
  if (m_heap_frame != NULL) {
    munmap(m_heap_frame, m_size);
  }
  if (m_fd > 0) {
    close(m_fd);
    unlink(m_hugefilename);
  }
}

}  // namespace mm
}  // namespace stonedb
