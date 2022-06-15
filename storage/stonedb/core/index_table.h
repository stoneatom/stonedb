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
#ifndef STONEDB_CORE_INDEX_TABLE_H_
#define STONEDB_CORE_INDEX_TABLE_H_
#pragma once

#include <vector>
#include "mm/traceable_object.h"
#include "system/cacheable_item.h"

namespace stonedb {
namespace core {
class Filter;
class Transaction;

class IndexTable : private system::CacheableItem, public mm::TraceableObject {
 public:
  IndexTable(int64_t _size, int64_t _orig_size, int mem_modifier);
  // size - number of objects, bytes_... = 2, 4 or 8
  // mem_modifier - default 0, may also be +1,2... or -1,2..., when positive =>
  // IndexTable will take more memory for a buffer
  IndexTable(IndexTable &sec);
  ~IndexTable();

  inline void Set64(uint64_t n, uint64_t val) {
    DEBUG_ASSERT(IsLocked());
    DEBUG_ASSERT(n < size);
    int b = int(n >> block_shift);
    if (b != cur_block) LoadBlock(b);
    block_changed = true;
    if (bytes_per_value == 4)
      ((unsigned int *)buf)[n & block_mask] = (unsigned int)val;
    else if (bytes_per_value == 8)
      ((uint64_t *)buf)[n & block_mask] = val;
    else  // bytes_per_value == 2
      ((unsigned short *)buf)[n & block_mask] = (unsigned short)val;
  }
  void SetByFilter(Filter *f,
                   uint32_t power);  // add all positions where filter is one

  inline uint64_t Get64(uint64_t n) {
    DEBUG_ASSERT(IsLocked());
    DEBUG_ASSERT(n < size);
    int b = int(n >> block_shift);
    if (b != cur_block) LoadBlock(b);
    uint64_t ndx = n & block_mask;
    uint64_t res;
    if (bytes_per_value == 4)
      res = ((unsigned int *)buf)[ndx];
    else if (bytes_per_value == 8)
      res = ((uint64_t *)buf)[ndx];
    else
      res = ((unsigned short *)buf)[ndx];
    return res;
  }

  inline uint64_t Get64InsideBlock(uint64_t n) {  // faster version (no block checking)
    DEBUG_ASSERT(int(n >> block_shift) == cur_block);
    uint64_t ndx = n & block_mask;
    uint64_t res;
    if (bytes_per_value == 4)
      res = ((unsigned int *)buf)[ndx];
    else if (bytes_per_value == 8)
      res = ((uint64_t *)buf)[ndx];
    else
      res = ((unsigned short *)buf)[ndx];
    return res;
  }

  inline uint64_t EndOfCurrentBlock(uint64_t n) {  // return the upper bound of this large block (n which will
                                                   // cause reload)
    return ((n >> block_shift) + 1) << block_shift;
  }

  // use only on newly added data, assumption: block_changed == true
  inline void Swap(uint64_t n1, uint64_t n2) {
    DEBUG_ASSERT(IsLocked());
    DEBUG_ASSERT(block_changed);  // use only on newly added data
    uint64_t ndx1 = n1 & block_mask;
    uint64_t ndx2 = n2 & block_mask;
    if (bytes_per_value == 4)
      std::swap(((unsigned int *)buf)[ndx1], ((unsigned int *)buf)[ndx2]);
    else if (bytes_per_value == 8)
      std::swap(((uint64_t *)buf)[ndx1], ((uint64_t *)buf)[ndx2]);
    else
      std::swap(((unsigned short *)buf)[ndx1], ((unsigned short *)buf)[ndx2]);
  }

  int64_t OrigSize() {
    return orig_size;
  }                                         // the size of the original table, or the largest (incl. 0 = NULL) value
                                            // which may occur in table
  uint64_t N() { return size; }             // note: this is the upper size, the table can be used partially!
  int BlockShift() { return block_shift; }  // block = int( tuple >> block_shift )
  void ExpandTo(int64_t new_size);

  // mm::TraceableObject functionality
  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_INDEXTABLE; }

 private:
  void LoadBlock(int b);

  unsigned char *buf = nullptr;  // polymorphic: unsigned short, unsigned int or int64_t

  int max_buffer_size_in_bytes;
  int bytes_per_value;
  int max_block_used;
  size_t buffer_size_in_bytes;
  int block_shift;
  uint64_t block_mask;
  uint64_t size;
  int64_t orig_size;  // the size of the original table, or the largest value
                      // (incl. 0 = NULL) which may occur in the table

  int cur_block;
  bool block_changed;
  Transaction *m_conn;  // external pointer
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_INDEX_TABLE_H_