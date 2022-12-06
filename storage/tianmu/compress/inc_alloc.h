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
#ifndef TIANMU_COMPRESS_INC_ALLOC_H_
#define TIANMU_COMPRESS_INC_ALLOC_H_
#pragma once

#include <iostream>
#include <vector>

#include "common/assert.h"
#include "compress/defs.h"

namespace Tianmu {
namespace compress {

// Incremental memory allocator.
// May throw CprsErr::CPRS_ERR_MEM.
class IncAlloc {
  static constexpr int FIRST_SIZE_ = 16384;  // default size of the first block
  static constexpr int ROUNDUP_ = 4096;      // block size will be a multiple of this
  static constexpr double GROW_SIZE_ = 1.2;  // how big is the next block compared to the previous one

  struct Block {
    void *mem;
    uint size;
    Block() : mem(nullptr), size(0) {}
    Block(void *m, uint s) : mem(m), size(s) {}
  };

  std::vector<Block> blocks_;
  uint blk_;   // index of the current block == no. of blocks_ already filled up
  uint used_;  // no. of bytes in block 'blk_' already used_

  uint firstsize_;  // size of the first block to allocate

  // frags_[s] - list of pointers to free'd fragments of size 's';
  // free'd fragments can be reused by alloc()
  static const uint MAX_FRAG_SIZE_ = 6144;
  // static const uint MAXNUMFRAG = 65536 * 2;  // max. no. of fragments of a
  // given size - never used_ as fragments are created on demand and we have no
  // control on their number
  std::vector<void *> frags_[MAX_FRAG_SIZE_ + 1];

  // currently, 'size' must be at most MAX_FRAG_SIZE_
  void *_alloc(uint size) {
    // find a fragment...
    DEBUG_ASSERT(size && (size <= MAX_FRAG_SIZE_));
    auto &frag = frags_[size];
    if (frag.size()) {
      void *mem = frag.back();
      frag.pop_back();
      return mem;
    }

    // ...or take a new one from the current block
    if (blk_ < blocks_.size() && (blocks_[blk_].size >= used_ + size)) {
      void *mem = (char *)blocks_[blk_].mem + used_;
      used_ += size;
      return mem;
    }

    // ...or find/allocate a new block
    return _alloc_search(size);
  }
  void *_alloc_search(uint size);  // more complicated part of _alloc, executed rarely
  // put 'p' into frags_[size]; does NOT check if p points into a block!
  void _free(void *p, uint size) {
    DEBUG_ASSERT(size && (size <= MAX_FRAG_SIZE_));
    frags_[size].push_back(p);
  }

 public:
  template <class T>
  void alloc(T *&p, uint n = 1) {
    p = (T *)_alloc(sizeof(T) * n);
  }
  template <class T>
  void freemem(T *p, uint n = 1) {
    _free(p, sizeof(T) * n);
  }

  void freeall();    // mark all blocks_ as free, without deallocation; clear lists
                     // of fragments
  void clear();      // physically deallocate all blocks_; clear lists of fragments
  void clearfrag();  // clear lists of fragments

  void GetMemUsg(uint &memblock, uint &memalloc, uint &memused);
  void PrintMemUsg(FILE *f);
  void PrintMemUsg(std::ostream &str);

  IncAlloc(uint fsize = FIRST_SIZE_);
  ~IncAlloc() { clear(); }
};

}  // namespace compress
}  // namespace Tianmu

#endif  // TIANMU_COMPRESS_INC_ALLOC_H_
