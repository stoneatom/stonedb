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

#include "inc_alloc.h"

namespace Tianmu {
namespace compress {

IncAlloc::IncAlloc(uint fsize) {
  blk_ = 0;
  used_ = 0;
  firstsize_ = fsize;
}

void IncAlloc::freeall() {
  blk_ = 0;
  used_ = 0;
  clearfrag();
}

void IncAlloc::clear() {
  blk_ = 0;
  used_ = 0;
  while (!blocks_.empty()) {
    delete[](char *)(blocks_.back().mem);
    blocks_.pop_back();
  }
  clearfrag();
}

void IncAlloc::clearfrag() {
  for (uint i = 0; i <= MAX_FRAG_SIZE_; i++) frags_[i].clear();
}

void *IncAlloc::_alloc_search(uint size) {
  // find a block with enough space
  while ((blk_ < blocks_.size()) && (blocks_[blk_].size < used_ + size)) {
    blk_++;
    used_ = 0;
  }
  if (blk_ < blocks_.size()) {
    void *mem = (char *)blocks_[blk_].mem + used_;
    used_ += size;
    return mem;
  }

  // allocate a new block
  DEBUG_ASSERT(blk_ == blocks_.size());
  uint bsize = firstsize_;
  if (blk_ > 0) {
    bsize = (uint)(blocks_[blk_ - 1].size * GROW_SIZE_ + ROUNDUP_) / ROUNDUP_ * ROUNDUP_;
    DEBUG_ASSERT(bsize > blocks_[blk_ - 1].size);
    if (bsize < size)
      bsize = size;
  }
  void *mem = new char[bsize];
  if (!mem)
    throw CprsErr::CPRS_ERR_MEM;

  blocks_.push_back(Block(mem, bsize));
  used_ = size;

  return mem;
}

void IncAlloc::GetMemUsg(uint &memblock, uint &memalloc, uint &memused) {
  memblock = 0;
  memalloc = 0;
  uint memfree = 0;
  uint i;
  for (i = 0; i < blocks_.size(); i++) memblock += blocks_[i].size;
  for (i = 0; i < blk_; i++) memalloc += blocks_[i].size;
  memalloc += used_;
  for (i = 1; i <= MAX_FRAG_SIZE_; i++) memfree += uint(i * frags_[i].size());
  memused = memalloc - memfree;
}

void IncAlloc::PrintMemUsg(FILE *f) {
  uint memblock, memalloc, memused;
  GetMemUsg(memblock, memalloc, memused);
  std::fprintf(f, "%u %u %u\n", memblock, memalloc, memused);
}

void IncAlloc::PrintMemUsg(std::ostream &str) {
  uint memblock, memalloc, memused;
  GetMemUsg(memblock, memalloc, memused);
  str << memblock << " " << memalloc << " " << memused << std::endl;
}

}  // namespace compress
}  // namespace Tianmu
