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
  heap_frame_ = NULL;
  // convert size from MB to B and make it a multiple of 2MB
  size_ = 1_MB * (size & ~0x1);
  heap_status_ = HEAP_STATUS::HEAP_ERROR;
  fd_ = -1;

  if (!hugedir.empty() && size > 0) {
    char pidtext[12];
    std::strcpy(huge_filename_, hugedir.c_str());
    std::sprintf(pidtext, "%d", getpid());
    std::strcat(huge_filename_, "/sdbhuge.");
    std::strcat(huge_filename_, pidtext);
    fd_ = open(huge_filename_, O_CREAT | O_RDWR, 0700);
    if (fd_ < 0) {
      heap_status_ = HEAP_STATUS::HEAP_OUT_OF_MEMORY;
      rccontrol << system::lock << "Memory Manager Error: Unable to create hugepage file: " << huge_filename_
                << system::unlock;
      return;
    }
    // MAP_SHARED to have mmap fail immediately if not enough pages
    // MAP_PRIVATE does copy on write
    // MAP_POPULATE to create page table entries and avoid future surprises
    heap_frame_ = (char *)mmap(NULL, size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd_, 0);
    if (heap_frame_ == MAP_FAILED) {
      unlink(huge_filename_);
      heap_status_ = HEAP_STATUS::HEAP_OUT_OF_MEMORY;
      rccontrol << system::lock << "Memory Manager Error: hugepage file mmap error: " << std::strerror(errno)
                << system::unlock;
      return;
    }

    rccontrol << system::lock << "Huge Heap size (MB) " << (int)(size) << system::unlock;
    // size_ = size;
    // manage the region as a normal 4k pagesize heap
    m_heap.RegisterArea(heap_frame_, size_ >> kPageShift);
    size_ = size;
    heap_status_ = HEAP_STATUS::HEAP_SUCCESS;
  }
}

HugeHeap::~HugeHeap() {
  if (heap_frame_ != NULL) {
    munmap(heap_frame_, size_);
  }
  if (fd_ > 0) {
    close(fd_);
    unlink(huge_filename_);
  }
}

}  // namespace mm
}  // namespace stonedb
