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

#include "filter.h"
#include "common/assert.h"
#include "core/tools.h"

namespace stonedb {
namespace core {
std::mutex HeapAllocator::mtx;
TheFilterBlockOwner *the_filter_block_owner;

Filter::Filter(int64_t no_obj, uint32_t power, bool all_ones, bool shallow)
    : block_status(0),
      blocks(0),
      block_allocator(0),
      no_of_bits_in_last_block(0),
      shallow(shallow),
      block_last_one(NULL),
      delayed_stats(-1),
      delayed_block(-1),
      delayed_stats_set(-1),
      no_power(power) {
  MEASURE_FET("Filter::Filter(int, bool, bool)");
  DEBUG_ASSERT(no_obj >= 0);
  pack_def = 1 << no_power;
  if (no_obj > common::MAX_ROW_NUMBER) throw common::OutOfMemoryException("Too many tuples.    (48)");

  no_blocks = (no_obj + (pack_def - 1)) >> no_power;
  no_of_bits_in_last_block = (no_obj & (pack_def - 1)) == 0 ? pack_def : int(no_obj & (pack_def - 1));
  if (no_obj == 0) no_of_bits_in_last_block = 0;

  blocks = 0;
  block_changed.resize(no_blocks);
  block_filter = this;
  Construct(all_ones, shallow);
}

Filter::Filter(Filter *f, int64_t no_obj, uint32_t power, bool all_ones, bool shallow)
    : block_status(0),
      blocks(0),
      block_allocator(0),
      no_of_bits_in_last_block(0),
      shallow(shallow),
      block_last_one(NULL),
      delayed_stats(-1),
      delayed_block(-1),
      delayed_stats_set(-1),
      no_power(power) {
  MEASURE_FET("Filter::Filter(int, bool, bool)");
  DEBUG_ASSERT(no_obj >= 0);
  pack_def = 1 << no_power;
  if (no_obj > common::MAX_ROW_NUMBER) throw common::OutOfMemoryException("Too many tuples.    (48)");

  no_blocks = (no_obj + (pack_def - 1)) >> no_power;
  no_of_bits_in_last_block = (no_obj & (pack_def - 1)) == 0 ? pack_def : int(no_obj & (pack_def - 1));
  if (no_obj == 0) no_of_bits_in_last_block = 0;

  blocks = 0;
  block_changed.resize(no_blocks);
  if (shallow) {
    block_filter = f;
  } else {
    block_filter = this;
  }

  Construct(all_ones, shallow);
}
Filter::Filter(const Filter &filter)
    : mm::TraceableObject(filter),
      block_status(0),
      blocks(0),
      no_of_bits_in_last_block(0),
      shallow(false),
      block_last_one(NULL),
      delayed_stats(-1),
      delayed_block(-1),
      delayed_stats_set(-1) {
  MEASURE_FET("Filter::Filter(const Filter&)");
  Filter &tmp_filter = const_cast<Filter &>(filter);
  no_power = filter.no_power;
  pack_def = 1 << no_power;
  no_blocks = filter.NoBlocks();
  no_of_bits_in_last_block = tmp_filter.NoAddBits();
  blocks = new Block *[no_blocks];
  ConstructPool();
  block_filter = this;
  for (size_t i = 0; i < no_blocks; i++) {
    if (tmp_filter.GetBlock(i)) {
      blocks[i] = block_allocator->Alloc(false);
      new (blocks[i]) Block(*(tmp_filter.GetBlock(i)),
                            block_filter);  // block_filter<->this
    } else
      blocks[i] = NULL;
  }
  block_status = new uchar[no_blocks];
  std::memcpy(block_status, filter.block_status, no_blocks);
  block_last_one = new ushort[no_blocks];
  std::memcpy(block_last_one, filter.block_last_one, no_blocks * sizeof(ushort));
  block_changed.resize(no_blocks);
}

void Filter::Construct(bool all_ones, bool shallow) {
  if (!shallow) {
    if (no_blocks > 0) {
      blocks = new Block *[no_blocks];
      block_status = new uchar[no_blocks];
      // set the filter to all empty
      std::memset(block_status, (all_ones ? (int)FB_FULL : (int)FB_EMPTY), no_blocks);
      block_last_one = new ushort[no_blocks];
      for (size_t i = 0; i < no_blocks; i++) {
        block_last_one[i] = pack_def - 1;
        blocks[i] = NULL;
      }
      block_last_one[no_blocks - 1] = no_of_bits_in_last_block - 1;
      // No idea how to create an allocator for the pool below to use SDB heap,
      // due to static methods in allocator
    }
    ConstructPool();
  }
}

void Filter::ConstructPool() {
  bit_block_pool = new boost::pool<HeapAllocator>(bitBlockSize);
  bit_mut = std::make_shared<std::mutex>();
  block_allocator = new BlockAllocator();
}

Filter *Filter::ShallowCopy(Filter &f) {
  Filter *sc = new Filter(&f, f.NoObj(), f.GetPower(), true,
                          true);  // shallow construct - no pool
  // sc->shallow = true;
  sc->block_allocator = f.block_allocator;
  sc->bit_block_pool = f.bit_block_pool;
  sc->bit_mut = f.bit_mut;
  sc->delayed_stats = -1;
  sc->delayed_block = -1;
  sc->delayed_stats_set = -1;
  sc->delayed_block_set = -1;
  sc->blocks = f.blocks;
  sc->block_status = f.block_status;
  sc->block_last_one = f.block_last_one;
  return sc;
}

Filter::~Filter() {
  MEASURE_FET("Filter::~Filter()");
  if (!shallow) {
    if (blocks) {
      delete[] blocks;
    }

    delete[] block_status;
    block_status = NULL;
    delete[] block_last_one;
    block_last_one = NULL;
    delete bit_block_pool;
    bit_block_pool = NULL;
    delete block_allocator;
    block_allocator = NULL;
  }
}

std::unique_ptr<Filter> Filter::Clone() const {
  return std::unique_ptr<Filter>(new Filter(*block_filter));  // block_filter->this
}

void Filter::SetDelayed(size_t b, int pos) {
  DEBUG_ASSERT(b < no_blocks);
  DEBUG_ASSERT(delayed_block == -1);  // no mixing!
  if (block_status[b] == FB_MIXED) {
    Set(b, pos);
    return;
  }
  if (block_status[b] == FB_EMPTY) {
    if (static_cast<size_t>(delayed_block_set) != b) {
      Commit();
      delayed_block_set = b;
      delayed_stats_set = -1;
    }
    if (pos == delayed_stats_set + 1) {
      delayed_stats_set++;
    } else if (pos > delayed_stats_set + 1) {  // then we can't delay
      if (delayed_stats_set >= 0) SetBetween(b, 0, b, delayed_stats_set);
      Set(b, pos);
      delayed_stats_set = -2;  // not to use any longer
    }
    // else do nothing
  }
}

void Filter::Commit() {
  if (delayed_block > -1) {
    if (block_status[delayed_block] == FB_FULL && delayed_stats >= 0)
      ResetBetween(delayed_block, 0, delayed_block, delayed_stats);
    delayed_block = -1;
    delayed_stats = -1;
  }
  if (delayed_block_set > -1) {
    if (block_status[delayed_block_set] == FB_EMPTY && delayed_stats_set >= 0)
      SetBetween(delayed_block_set, 0, delayed_block_set, delayed_stats_set);
    delayed_block_set = -1;
    delayed_stats_set = -1;
  }
}

void Filter::Set() {
  MEASURE_FET("voFilter::Set()");
  if (no_blocks == 0) return;
  std::memset(block_status, FB_FULL, no_blocks);  // set the filter to all full

  for (size_t b = 0; b < no_blocks; b++) {
    block_last_one[b] = pack_def - 1;
    if (blocks[b]) DeleteBlock(b);
  }
  block_last_one[no_blocks - 1] = no_of_bits_in_last_block - 1;
  delayed_stats = -1;
}

void Filter::SetBlock(size_t b) {
  MEASURE_FET("Filter::SetBlock(int)");
  DEBUG_ASSERT(b < no_blocks);
  block_status[b] = FB_FULL;  // set the filter to all full
  block_last_one[b] = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : pack_def - 1);
  if (blocks[b]) DeleteBlock(b);
}

void Filter::Set(size_t b, int n) {
  DEBUG_ASSERT(b < no_blocks);
  bool make_mixed = false;
  block_changed[b] = true;
  if (block_status[b] == FB_FULL) {
    if (n == int(block_last_one[b]) + 1)
      block_last_one[b]++;
    else if (n > int(block_last_one[b]) + 1)  // else do nothing - already set
      make_mixed = true;
  }
  if (make_mixed || block_status[b] == FB_EMPTY) {
    if (n == 0) {
      block_status[b] = FB_FULL;
      block_last_one[b] = 0;
    } else {
      int new_block_size = (b == no_blocks - 1 ? no_of_bits_in_last_block : pack_def);
      block_status[b] = FB_MIXED;
      blocks[b] = block_allocator->Alloc();
      new (blocks[b]) Block(block_filter, new_block_size);  // block_filter<->this
      if (blocks[b] == NULL) throw common::OutOfMemoryException();
    }
    if (make_mixed) {
      blocks[b]->Set(0, block_last_one[b]);  // create a block with a contents before Set
    }
  }
  if (blocks[b]) {
    bool full = blocks[b]->Set(n);
    if (full) SetBlock(b);
  }
}

void Filter::SetBetween(int64_t n1, int64_t n2) {
  if (n1 == n2)
    Set(int(n1 >> no_power), int(n1 & (pack_def - 1)));
  else
    SetBetween(int(n1 >> no_power), int(n1 & (pack_def - 1)), int(n2 >> no_power), int(n2 & (pack_def - 1)));
}

void Filter::SetBetween(size_t b1, int n1, size_t b2, int n2) {
  MEASURE_FET("Filter::SetBetween(...)");
  DEBUG_ASSERT(b2 < no_blocks);
  if (b1 == b2) {
    if (block_status[b1] == FB_FULL && n1 <= int(block_last_one[b1]) + 1) {
      block_last_one[b1] = std::max(block_last_one[b1], ushort(n2));
    } else if (block_status[b1] == FB_EMPTY || block_status[b1] == FB_FULL) {
      // if full block
      if (n1 == 0) {
        block_status[b1] = FB_FULL;
        block_last_one[b1] = n2;
      } else {
        if (b1 == no_blocks - 1) {
          blocks[b1] = block_allocator->Alloc();
          new (blocks[b1]) Block(block_filter, no_of_bits_in_last_block);  // block_filter->this
        } else {
          blocks[b1] = block_allocator->Alloc();
          new (blocks[b1]) Block(block_filter, pack_def);  // block_filter->this
        }
        if (blocks[b1] == NULL) throw common::OutOfMemoryException();
        if (block_status[b1] == FB_FULL)
          blocks[b1]->Set(0,
                          block_last_one[b1]);  // create a block with a contents before Set
        block_status[b1] = FB_MIXED;
      }
    }
    if (blocks[b1]) {
      bool full = blocks[b1]->Set(n1, n2);
      if (full) SetBlock(b1);
    }
  } else {
    if (n1 == 0)
      SetBlock(b1);
    else
      SetBetween(b1, n1, b1,
                 pack_def - 1);  // note that b1 is never the last block
    for (size_t i = b1 + 1; i < b2; i++) SetBlock(i);
    SetBetween(b2, 0, b2, n2);
  }
}

void Filter::Reset() {
  MEASURE_FET("Filter::Reset()");
  std::memset(block_status, FB_EMPTY,
              no_blocks);  // set the filter to all empty
  for (size_t b = 0; b < no_blocks; b++) {
    if (blocks[b]) DeleteBlock(b);
  }
  delayed_stats = -1;
}

void Filter::ResetBlock(size_t b) {
  MEASURE_FET("Filter::ResetBlock(int)");
  DEBUG_ASSERT(b < no_blocks);
  block_status[b] = FB_EMPTY;
  if (blocks[b]) DeleteBlock(b);
}

void Filter::ResetBetween(int64_t n1, int64_t n2) {
  if (n1 == n2)
    Reset(int(n1 >> no_power), int(n1 & (pack_def - 1)));
  else
    ResetBetween(int(n1 >> no_power), int(n1 & (pack_def - 1)), int(n2 >> no_power), int(n2 & (pack_def - 1)));
}

void Filter::ResetBetween(size_t b1, int n1, size_t b2, int n2) {
  DEBUG_ASSERT(b2 <= no_blocks);
  if (b1 == b2) {
    if (block_status[b1] == FB_FULL) {
      // if full block
      if (n1 > 0 && n2 >= block_last_one[b1]) {
        block_last_one[b1] = std::min(ushort(n1 - 1), block_last_one[b1]);
      } else if (n1 == 0 && n2 >= block_last_one[b1]) {
        block_status[b1] = FB_EMPTY;
      } else {
        int new_block_size = (b1 == no_blocks - 1 ? no_of_bits_in_last_block : pack_def);
        blocks[b1] = block_allocator->Alloc();
        block_status[b1] = FB_MIXED;
        if (block_last_one[b1] == new_block_size - 1) {
          new (blocks[b1]) Block(block_filter, new_block_size,
                                 true);  // block_filter->this // set as full,
                                         // then reset a part of it
          if (blocks[b1] == NULL) throw common::OutOfMemoryException();
        } else {
          new (blocks[b1]) Block(block_filter, new_block_size,
                                 false);  // block_filter->this// set as empty,
                                          // then set the beginning
          if (blocks[b1] == NULL) throw common::OutOfMemoryException();
          blocks[b1]->Set(0, block_last_one[b1]);  // create a block with a
                                                   // contents before Reset
        }
      }
    }
    if (blocks[b1]) {
      bool empty = blocks[b1]->Reset(n1, n2);
      if (empty) ResetBlock(b1);
    }
  } else {
    if (n1 == 0)
      ResetBlock(b1);
    else
      ResetBetween(b1, n1, b1,
                   pack_def - 1);  // note that b1 is never the last block
    for (size_t i = b1 + 1; i < b2; i++) ResetBlock(i);
    ResetBetween(b2, 0, b2, n2);
  }
}

void Filter::Reset(Filter &f2) {
  auto mb = std::min(f2.NoBlocks(), NoBlocks());
  for (size_t b = 0; b < mb; b++) {
    if (f2.block_status[b] == FB_FULL)
      ResetBetween(b, 0, b, f2.block_last_one[b]);
    else if (f2.block_status[b] != FB_EMPTY) {  // else no change
      if (block_status[b] == FB_FULL) {
        blocks[b] = block_allocator->Alloc();
        new (blocks[b]) Block(*(f2.GetBlock(b)), block_filter);  // block_filter->this
        blocks[b]->Not();                                        // always nontrivial
        int new_block_size = (b == no_blocks - 1 ? no_of_bits_in_last_block : pack_def);
        if (block_last_one[b] < new_block_size - 1) blocks[b]->Reset(block_last_one[b] + 1, new_block_size - 1);
        block_status[b] = FB_MIXED;
      } else if (blocks[b]) {
        bool empty = blocks[b]->AndNot(*(f2.GetBlock(b)));
        if (empty) ResetBlock(b);
      }
    }
  }
}

bool Filter::Get(size_t b, int n) {
  DEBUG_ASSERT(b < no_blocks);
  if (block_status[b] == FB_EMPTY) return false;
  if (block_status[b] == FB_FULL) return (n <= block_last_one[b]);
  return blocks[b]->Get(n);
}

bool Filter::IsEmpty() {
  for (size_t i = 0; i < no_blocks; i++)
    if (block_status[i] != FB_EMPTY) return false;
  return true;
}

bool Filter::IsEmpty(size_t b) const {
  DEBUG_ASSERT(b < no_blocks);
  return (block_status[b] == FB_EMPTY);
}

bool Filter::IsEmptyBetween(int64_t n1,
                            int64_t n2)  // true if there are only 0 between n1 and n2, inclusively
{
  DEBUG_ASSERT((n1 >= 0) && (n1 <= n2));
  if (n1 == n2) return !Get(n1);
  size_t b1 = int(n1 >> no_power);
  size_t b2 = int(n2 >> no_power);
  int nn1 = int(n1 & (pack_def - 1));
  int nn2 = int(n2 & (pack_def - 1));
  if (b1 == b2) {
    if (block_status[b1] == FB_FULL) return (nn1 > block_last_one[b1]);
    if (block_status[b1] == FB_EMPTY) return true;
    return blocks[b1]->IsEmptyBetween(nn1, nn2);
  } else {
    size_t full_pack_start = (nn1 == 0 ? b1 : b1 + 1);
    size_t full_pack_stop = b2 - 1;
    if ((uint)nn2 == (pack_def - 1) || (b2 == no_blocks - 1 && nn2 == no_of_bits_in_last_block - 1))
      full_pack_stop = b2;
    for (size_t i = full_pack_start; i <= full_pack_stop; i++)
      if (block_status[i] != FB_EMPTY) return false;
    if (b1 != full_pack_start) {
      if (block_status[b1] == FB_FULL) {
        if (nn1 <= block_last_one[b1]) return false;
      } else if (block_status[b1] != FB_EMPTY &&
                 !blocks[b1]->IsEmptyBetween(nn1,
                                             pack_def - 1))  // note that b1 is never the last block
        return false;
    }
    if (b2 != full_pack_stop) {
      if (block_status[b2] == FB_FULL) return false;
      if (block_status[b2] != FB_EMPTY && !blocks[b2]->IsEmptyBetween(0, nn2)) return false;
    }
  }
  return true;
}

bool Filter::IsFullBetween(int64_t n1,
                           int64_t n2)  // true if there are only 1 between n1 and n2, inclusively
{
  DEBUG_ASSERT((n1 >= 0) && (n1 <= n2));
  if (n1 == n2) return Get(n1);
  size_t b1 = int(n1 >> no_power);
  size_t b2 = int(n2 >> no_power);
  int nn1 = int(n1 & (pack_def - 1));
  int nn2 = int(n2 & (pack_def - 1));
  if (b1 == b2) {
    if (block_status[b1] == FB_FULL) return (nn2 <= block_last_one[b1]);
    if (block_status[b1] == FB_EMPTY) return false;
    return blocks[b1]->IsFullBetween(nn1, nn2);
  } else {
    size_t full_pack_start = (nn1 == 0 ? b1 : b1 + 1);
    size_t full_pack_stop = b2 - 1;
    if ((uint)nn2 == (pack_def - 1) || (b2 == no_blocks - 1 && nn2 == no_of_bits_in_last_block - 1))
      full_pack_stop = b2;
    for (size_t i = full_pack_start; i <= full_pack_stop; i++)
      if (!IsFull(i)) return false;
    if (b1 != full_pack_start) {
      if (block_status[b1] == FB_EMPTY) return false;
      if (!IsFull(b1) && !blocks[b1]->IsFullBetween(nn1, pack_def - 1))  // note that b1 is never the last block
        return false;
    }
    if (b2 != full_pack_stop) {
      if (block_status[b2] == FB_EMPTY) return false;
      if (block_status[b2] == FB_FULL) return (nn2 <= block_last_one[b2]);
      if (!blocks[b2]->IsFullBetween(0, nn2)) return false;
    }
  }
  return true;
}

bool Filter::IsFull() const {
  for (size_t b = 0; b < no_blocks; b++)
    if (!IsFull(b)) return false;
  return true;
}

void Filter::CopyBlock(Filter &f, size_t block) {
  DEBUG_ASSERT(block < no_blocks);
  DEBUG_ASSERT(!f.shallow || bit_block_pool == f.bit_block_pool);

  block_status[block] = f.block_status[block];
  block_last_one[block] = f.block_last_one[block];

  if (f.GetBlock(block)) {
    if (bit_block_pool == f.bit_block_pool) {                              // f is a shallow copy of this
      blocks[block] = f.blocks[block]->MoveFromShallowCopy(block_filter);  // block_filter->this
      f.blocks[block] = NULL;
    } else {
      if (blocks[block])
        blocks[block]->CopyFrom(*f.blocks[block],
                                block_filter);  // block_filter->this
      else {
        blocks[block] = block_allocator->Alloc();
        new (blocks[block]) Block(*(f.GetBlock(block)), block_filter);  // block_filter->this
      }
    }
  } else {
    // no block, just status to copy
    if (bit_block_pool == f.bit_block_pool)  // f is a shallow copy of this
      blocks[block] = NULL;
    else if (blocks[block])
      DeleteBlock(block);
  }

  DEBUG_ASSERT(!blocks[block] || blocks[block]->Owner() == block_filter);  // block_filter->this
}

void Filter::DeleteBlock(int pack) {
  blocks[pack]->~Block();
  block_allocator->Dealloc(blocks[pack]);
  blocks[pack] = NULL;
}

bool Filter::IsEqual(Filter &sec) {
  if (no_blocks != sec.no_blocks || no_of_bits_in_last_block != sec.no_of_bits_in_last_block) return false;
  for (size_t b = 0; b < no_blocks; b++) {
    if (block_status[b] != sec.block_status[b]) {
      int64_t bstart = int64_t(b) >> no_power;
      int64_t bstop = bstart + (b < no_blocks - 1 ? (pack_def - 1) : no_of_bits_in_last_block - 1);
      if (block_status[b] == FB_FULL && sec.block_status[b] == FB_MIXED) {  // Note: may still be equal!
        if (!sec.IsFullBetween(bstart, bstart + block_last_one[b])) return false;
        if (bstart + block_last_one[b] < bstop && !sec.IsEmptyBetween(bstart + block_last_one[b] + 1, bstop))
          return false;
        return true;
      }
      if (sec.block_status[b] == FB_FULL && block_status[b] == FB_MIXED) {  // Note: may still be equal!
        if (!IsFullBetween(bstart, bstart + sec.block_last_one[b])) return false;
        if (bstart + sec.block_last_one[b] < bstop && !IsEmptyBetween(bstart + sec.block_last_one[b] + 1, bstop))
          return false;
        return true;
      }
      return false;
    }
    if (block_status[b] == FB_FULL && block_last_one[b] != sec.block_last_one[b]) return false;
    if (blocks[b] && blocks[b]->IsEqual(*sec.GetBlock(b)) == false) return false;
  }
  return true;
}

void Filter::And(Filter &f2) {
  auto mb = std::min(f2.NoBlocks(), NoBlocks());
  for (size_t b = 0; b < mb; b++) {
    if (f2.block_status[b] == FB_EMPTY)
      ResetBlock(b);
    else if (f2.block_status[b] == FB_MIXED) {
      if (block_status[b] == FB_FULL) {
        int old_block_size = block_last_one[b];
        blocks[b] = block_allocator->Alloc();
        new (blocks[b]) Block(*(f2.GetBlock(b)), block_filter);  // block_filter->this
        block_status[b] = FB_MIXED;
        int end_block = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : (pack_def - 1));
        if (old_block_size < end_block) ResetBetween(b, old_block_size + 1, b, end_block);
      } else if (blocks[b]) {
        bool empty = blocks[b]->And(*(f2.GetBlock(b)));
        if (empty) ResetBlock(b);
      }
    } else {  // FB_FULL
      int end_block = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : (pack_def - 1));
      if (f2.block_last_one[b] < end_block) ResetBetween(b, int(f2.block_last_one[b]) + 1, b, end_block);
    }
  }
}

void Filter::Or(Filter &f2, int pack) {
  auto mb = std::min(f2.NoBlocks(), NoBlocks());
  size_t b = (pack == -1 ? 0 : pack);
  for (; b < mb; b++) {
    if (f2.block_status[b] == FB_FULL)
      SetBetween(b, 0, b, f2.block_last_one[b]);
    else if (f2.block_status[b] != FB_EMPTY) {  // else no change
      if (block_status[b] == FB_EMPTY) {
        blocks[b] = block_allocator->Alloc();
        new (blocks[b]) Block(*(f2.GetBlock(b)), block_filter);  // block_filter->this
        block_status[b] = FB_MIXED;
      } else if (blocks[b]) {
        bool full = blocks[b]->Or(*(f2.GetBlock(b)));
        if (full) SetBlock(b);
      } else {  // FB_FULL
        if (block_last_one[b] < (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : (pack_def - 1))) {
          int old_block_size = block_last_one[b];
          blocks[b] = block_allocator->Alloc();
          new (blocks[b]) Block(*(f2.GetBlock(b)), block_filter);  // block_filter->this
          block_status[b] = FB_MIXED;
          SetBetween(b, 0, b, old_block_size);
        }
      }
    }
    if (pack != -1) break;
  }
}

void Filter::Not() {
  for (size_t b = 0; b < no_blocks; b++) {
    if (block_status[b] == FB_FULL) {
      if (block_last_one[b] < (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : (pack_def - 1))) {
        int old_block_size = block_last_one[b];
        block_last_one[b] = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : (pack_def - 1));  // make really full
        ResetBetween(b, 0, b, old_block_size);
      } else
        block_status[b] = FB_EMPTY;
    } else if (block_status[b] == FB_EMPTY) {
      block_status[b] = FB_FULL;
      block_last_one[b] = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : (pack_def - 1));
    } else
      blocks[b]->Not();
  }
}

void Filter::AndNot(Filter &f2)  // reset all positions which are set in f2
{
  auto mb = std::min(f2.NoBlocks(), NoBlocks());
  for (size_t b = 0; b < mb; b++) {
    if (f2.block_status[b] == FB_FULL)
      ResetBetween(b, 0, b, f2.block_last_one[b]);
    else if (f2.block_status[b] != FB_EMPTY && block_status[b] != FB_EMPTY) {  // else no change
      if (block_status[b] == FB_FULL) {
        int old_block_size = block_last_one[b];
        blocks[b] = block_allocator->Alloc();
        new (blocks[b]) Block(*(f2.GetBlock(b)), block_filter);  // block_filter->this
        block_status[b] = FB_MIXED;
        blocks[b]->Not();
        int end_block = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : (pack_def - 1));
        if (old_block_size < end_block) ResetBetween(b, old_block_size + 1, b, end_block);
      } else if (blocks[b]) {
        bool empty = blocks[b]->AndNot(*(f2.GetBlock(b)));
        if (empty) ResetBlock(b);
      }
    }
  }
}

void Filter::SwapPack(Filter &f2, int pack) {
  // WARNING: cannot just swap pointers, as the blocks belong to private pools!
  Block b(block_filter, pack_def);  // block_filter->this

  if (block_status[pack] == FB_MIXED) {
    // save block
    DEBUG_ASSERT(shallow || (blocks[pack] && (blocks[pack]->Owner() == block_filter)));  // block_filter->this//shallow
                                                                                         // copy can have the original
                                                                                         // filter as the block owner

    b.CopyFrom(*(blocks[pack]), block_filter);  // block_filter->this
  }
  if (f2.block_status[pack] == FB_MIXED) {
    DEBUG_ASSERT(f2.blocks[pack] && f2.blocks[pack]->Owner() == &f2);
    if (!blocks[pack]) {
      blocks[pack] = block_allocator->Alloc();
      new (blocks[pack]) Block(*(f2.blocks[pack]), block_filter);  // block_filter->this
    } else
      blocks[pack]->CopyFrom(*(f2.blocks[pack]),
                             block_filter);  // block_filter->this
  } else {
    if (blocks[pack]) DeleteBlock(pack);
  }
  if (block_status[pack] == FB_MIXED) {
    if (!f2.blocks[pack]) {
      f2.blocks[pack] = f2.block_allocator->Alloc();
      new (f2.blocks[pack]) Block(b, &f2);
    } else
      f2.blocks[pack]->CopyFrom(b, &f2);
  } else {
    if (f2.blocks[pack]) f2.DeleteBlock(pack);
  }
  std::swap(block_status[pack], f2.block_status[pack]);
  std::swap(block_last_one[pack], f2.block_last_one[pack]);
  std::swap(block_changed[pack], f2.block_changed[pack]);
  if (block_status[pack] == FB_MIXED) DEBUG_ASSERT(blocks[pack]->Owner() == block_filter);  // block_filter->this
  if (f2.block_status[pack] == FB_MIXED) DEBUG_ASSERT(f2.blocks[pack]->Owner() == &f2);
}

int64_t Filter::NoOnes() const {
  if (no_blocks == 0) return 0;
  int64_t count = 0;
  for (size_t b = 0; b < no_blocks; b++) {
    if (block_status[b] == FB_FULL)
      count += int(block_last_one[b]) + 1;
    else if (blocks[b])
      count += blocks[b]->NoOnes();  // else empty
  }
  return count;
}

uint Filter::NoOnes(int b) {
  if (no_blocks == 0) return 0;
  if (block_status[b] == FB_FULL)
    return uint(block_last_one[b]) + 1;
  else if (blocks[b])
    return blocks[b]->NoOnes();
  return 0;
}

uint Filter::NoOnesUncommited(int b) {
  int uc = 0;
  if (delayed_block == b) {
    if (block_status[b] == FB_FULL && delayed_stats >= 0) uc = -delayed_stats - 1;
  }
  if (delayed_block_set == b) {
    if (block_status[b] == FB_EMPTY && delayed_stats_set >= 0) uc = delayed_stats_set + 1;
  }
  return NoOnes(b) + uc;
}

int64_t Filter::NoOnesBetween(int64_t n1, int64_t n2)  // no of 1 between n1 and n2, inclusively
{
  DEBUG_ASSERT((n1 >= 0) && (n1 <= n2));
  if (n1 == n2) return (Get(n1) ? 1 : 0);
  size_t b1 = int(n1 >> no_power);
  size_t b2 = int(n2 >> no_power);
  int nn1 = int(n1 & (pack_def - 1));
  int nn2 = int(n2 & (pack_def - 1));
  if (b1 == b2) {
    if (block_status[b1] == FB_FULL) {
      nn2 = std::min(nn2, int(block_last_one[b1]));
      if (nn1 > block_last_one[b1]) return 0;
      return nn2 - nn1 + 1;
    }
    if (block_status[b1] == FB_EMPTY) return 0;
    return blocks[b1]->NoOnesBetween(nn1, nn2);
  }
  int64_t counter = 0;
  size_t full_pack_start = (nn1 == 0 ? b1 : b1 + 1);
  size_t full_pack_stop = b2 - 1;
  if ((uint)nn2 == (pack_def - 1) || (b2 == no_blocks - 1 && nn2 == no_of_bits_in_last_block - 1)) full_pack_stop = b2;
  for (size_t i = full_pack_start; i <= full_pack_stop; i++) {
    if (block_status[i] == FB_MIXED)
      counter += blocks[i]->NoOnes();
    else if (block_status[i] == FB_FULL)
      counter += int64_t(block_last_one[i]) + 1;
  }
  if (b1 != full_pack_start) {
    if (block_status[b1] == FB_FULL) {
      if (nn1 <= block_last_one[b1]) counter += block_last_one[b1] - nn1 + 1;
    } else if (block_status[b1] != FB_EMPTY)
      counter += blocks[b1]->NoOnesBetween(nn1, (pack_def - 1));  // note that b1 is never the last block
  }
  if (b2 != full_pack_stop) {
    if (block_status[b2] == FB_FULL)
      counter += std::min(nn2 + 1, int(block_last_one[b2]) + 1);
    else if (block_status[b2] != FB_EMPTY)
      counter += blocks[b2]->NoOnesBetween(0, nn2);
  }
  return counter;
}

int Filter::DensityWeight()  // = 65537 for empty filter or a filter with only
                             // one nonempty block.
{
  // Otherwise it is an average number of ones in nonempty blocks.
  int64_t count = 0;
  int nonempty = 0;
  if (no_blocks > 0) {
    for (size_t b = 0; b < no_blocks; b++) {
      if (block_status[b] == FB_FULL) {
        count += int64_t(block_last_one[b]) + 1;
        nonempty++;
      } else if (blocks[b]) {
        count += blocks[b]->NoOnes();
        nonempty++;
      }
    }
  }
  if (nonempty < 2) return 65537;
  return int(count / double(nonempty));
}

int64_t Filter::NoObj() const {
  int64_t res = int64_t(no_blocks - (no_of_bits_in_last_block ? 1 : 0)) << no_power;
  return res + no_of_bits_in_last_block;
}

int Filter::NoAddBits() const { return no_of_bits_in_last_block; }

void Filter::AddNewBlocks(int new_blocks, bool value, int new_no_bits_last) {
  if (new_blocks > 0) {
    // grow arrays
    Block **tmp_blocks = new Block *[no_blocks + new_blocks];
    uchar *tmp_block_status = new uchar[no_blocks + new_blocks];
    ushort *tmp_block_size = new ushort[no_blocks + new_blocks];
    std::memcpy(tmp_blocks, blocks, sizeof(Block *) * no_blocks);
    std::memcpy(tmp_block_status, block_status, sizeof(uchar) * no_blocks);
    std::memcpy(tmp_block_size, block_last_one, sizeof(ushort) * no_blocks);
    delete[] blocks;
    delete[] block_status;
    delete[] block_last_one;
    blocks = tmp_blocks;
    block_status = tmp_block_status;
    block_last_one = tmp_block_size;

    for (size_t i = no_blocks; i < no_blocks + new_blocks; i++) {
      blocks[i] = NULL;
      if (value) {
        block_status[i] = FB_FULL;
        block_last_one[i] = (i == no_blocks + new_blocks - 1 ? new_no_bits_last - 1 : (pack_def - 1));
      } else
        block_status[i] = FB_EMPTY;
    }
  }
}

char *HeapAllocator::malloc(const size_type bytes) {
  //		std::cerr<< (bytes >> 20) << " for Filter\n";
  // return (char*) Instance()->alloc(bytes, mm::BLOCK_TYPE::BLOCK_TEMPORARY,
  // the_filter_block_owner);
  std::scoped_lock guard(HeapAllocator::mtx);
  void *r;
  try {
    r = the_filter_block_owner->alloc(bytes, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
  } catch (...) {
    return NULL;
  }
  return (char *)r;
}

void HeapAllocator::free(char *const block) {
  // Instance()->dealloc(block, the_filter_block_owner);
  std::scoped_lock guard(HeapAllocator::mtx);
  the_filter_block_owner->dealloc(block);
}

Filter::Block *Filter::BlockAllocator::Alloc(bool sync) {
  if (sync) block_mut.lock();
  if (!free_in_pool) {
    for (int i = 0; i < pool_size; i++) {
      pool[i] = (Filter::Block *)block_object_pool.malloc();
      if (!pool[i]) {
        block_mut.unlock();
        throw common::OutOfMemoryException();
      }
    }
    free_in_pool = pool_size;
    next_ndx = 0;
  }
  free_in_pool--;
  Block *b = pool[next_ndx];
  next_ndx = (next_ndx + pool_stride) % pool_size;
  if (sync) block_mut.unlock();
  return b;
}

void Filter::BlockAllocator::Dealloc(Block *b) {
  std::scoped_lock guard(block_mut);
  block_object_pool.free(b);
}
}  // namespace core
}  // namespace stonedb
