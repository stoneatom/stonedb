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

namespace stonedb {
namespace compress {

IncAlloc::IncAlloc(uint fsize) {
  blk = 0;
  used = 0;
  firstsize = fsize;
}

void IncAlloc::freeall() {
  blk = 0;
  used = 0;
  clearfrag();
}

void IncAlloc::clear() {
  blk = 0;
  used = 0;
  while (!blocks.empty()) {
    delete[] (char *)(blocks.back().mem);
    blocks.pop_back();
  }
  clearfrag();
}

void IncAlloc::clearfrag() {
  for (uint i = 0; i <= MAXFRAGSIZE; i++) frags[i].clear();
}

void *IncAlloc::_alloc_search(uint size) {
  // find a block with enough space
  while ((blk < blocks.size()) && (blocks[blk].size < used + size)) {
    blk++;
    used = 0;
  }
  if (blk < blocks.size()) {
    void *mem = (char *)blocks[blk].mem + used;
    used += size;
    return mem;
  }

  // allocate a new block
  DEBUG_ASSERT(blk == blocks.size());
  uint bsize = firstsize;
  if (blk > 0) {
    bsize = (uint)(blocks[blk - 1].size * GROWSIZE + ROUNDUP) / ROUNDUP * ROUNDUP;
    DEBUG_ASSERT(bsize > blocks[blk - 1].size);
    if (bsize < size) bsize = size;
  }
  void *mem = new char[bsize];
  if (!mem) throw CprsErr::CPRS_ERR_MEM;

  blocks.push_back(Block(mem, bsize));
  used = size;

  return mem;
}

void IncAlloc::GetMemUsg(uint &memblock, uint &memalloc, uint &memused) {
  memblock = 0;
  memalloc = 0;
  uint memfree = 0;
  uint i;
  for (i = 0; i < blocks.size(); i++) memblock += blocks[i].size;
  for (i = 0; i < blk; i++) memalloc += blocks[i].size;
  memalloc += used;
  for (i = 1; i <= MAXFRAGSIZE; i++) memfree += uint(i * frags[i].size());
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
}  // namespace stonedb
