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
#include "system/tianmu_system.h"
#include "util/log_ctl.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>

namespace Tianmu {
namespace mm {

HugeHeap::HugeHeap(std::string hugedir, size_t size) : TCMHeap(0) {
  {
    char command[2048];
    std::strcpy(command, "rm -rf ");
    std::strcat(command, hugedir.c_str());
    std::strcat(command, "/tianmuhuge.*");
    ::system(command);
  }

  heap_frame_ = nullptr;
  // convert size from MB to B and make it a multiple of 2MB
  size_ = 1_MB * (size & ~0x1);
  heap_status_ = HEAP_STATUS::HEAP_ERROR;
  fd_ = -1;

  if (!hugedir.empty() && size > 0) {
    char pidtext[12];
    std::strcpy(huge_filename_, hugedir.c_str());
    std::sprintf(pidtext, "%d", getpid());
    std::strcat(huge_filename_, "/tianmuhuge.");
    std::strcat(huge_filename_, pidtext);
    fd_ = open(huge_filename_, O_CREAT | O_RDWR, 0666);
    if (fd_ < 0) {
      heap_status_ = HEAP_STATUS::HEAP_OUT_OF_MEMORY;
      tianmu_control_ << system::lock << "Memory Manager Error: Unable to create hugepage file: " << huge_filename_
                      << system::unlock;
      return;
    }

    lseek(fd_, size_ - 1, SEEK_SET);
    write(fd_, " ", 1);
    lseek(fd_, 0, SEEK_SET);

    // MAP_SHARED to have mmap fail immediately if not enough pages
    // MAP_PRIVATE does copy on write
    // MAP_POPULATE to create page table entries and avoid future surprises
    heap_frame_ = (char *)mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd_, 0);
    if (heap_frame_ == MAP_FAILED) {
      unlink(huge_filename_);
      heap_status_ = HEAP_STATUS::HEAP_OUT_OF_MEMORY;
      tianmu_control_ << system::lock << "Memory Manager Error: hugepage file mmap error: " << std::strerror(errno)
                      << system::unlock;
      return;
    }

    tianmu_control_ << system::lock << "Huge Heap size (MB) " << (int)(size) << system::unlock;

    TIANMU_LOG(LogCtl_Level::INFO, "HugeHeap huge_filename_: %s size: %ld size_: %ld kPageShift: %d", huge_filename_,
               size, size_, kPageShift);

    // size_ = size;
    // manage the region as a normal 4k pagesize heap
    m_heap.RegisterArea(heap_frame_, size_ >> kPageShift);
    size_ = size;
    heap_status_ = HEAP_STATUS::HEAP_SUCCESS;
  }
}

HugeHeap::~HugeHeap() {
  if (heap_frame_ != nullptr) {
    munmap(heap_frame_, size_);
  }
  if (fd_ > 0) {
    close(fd_);
    unlink(huge_filename_);
  }
}

}  // namespace mm
}  // namespace Tianmu
