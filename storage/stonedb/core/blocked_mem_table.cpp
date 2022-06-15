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

#include "blocked_mem_table.h"

namespace stonedb {
namespace core {
MemBlockManager::~MemBlockManager() {
  for (auto &it : free_blocks) dealloc(it);

  for (auto &it : used_blocks) dealloc(it);
}

void *MemBlockManager::GetBlock() {
  DEBUG_ASSERT(block_size != -1);

  if (hard_size_limit != -1 && hard_size_limit <= current_size) return NULL;

  {
    std::scoped_lock g(mx);
    void *p;
    if (free_blocks.size() > 0) {
      p = free_blocks[free_blocks.size() - 1];
      free_blocks.pop_back();
    } else {
      p = alloc(block_size, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
      current_size += block_size;
    }
    used_blocks.insert(p);
    return p;
  }
}

int MemBlockManager::MemoryBlocksLeft() {
  if (size_limit == -1) return 9999;  // uninitialized => no limit
  int64_t s;
  {
    std::scoped_lock g(mx);
    s = free_blocks.size();
  }
  int64_t size_in_near_future = current_size + block_size * ((no_threads >> 1) - s);
  if (size_in_near_future > size_limit) return 0;
  return int((size_limit - size_in_near_future) / block_size);
}

void MemBlockManager::FreeBlock(void *b) {
  if (b == NULL) return;
  std::scoped_lock g(mx);
  size_t r = used_blocks.erase(b);
  if (r == 1) {
    if (size_limit == -1) {
      // no limit - keep a few freed blocks
      if (free_blocks.size() > 5) {
        dealloc(b);
        current_size -= block_size;
      } else
        free_blocks.push_back(b);
    } else
      // often freed block are immediately required by other - keep some of them
      // above the limit in the pool
      if (current_size > size_limit + block_size * (no_threads + 1)) {
        dealloc(b);
        current_size -= block_size;
      } else
        free_blocks.push_back(b);
  }  // else not found (already erased)
}

//--------------------------------------------------------------------------

void BlockedRowMemStorage::Init(int rowl, std::shared_ptr<MemBlockManager> mbm, uint64_t initial_size,
                                int min_block_len) {
  DEBUG_ASSERT(no_rows == 0);
  CalculateBlockSize(rowl, min_block_len);
  bman = mbm;
  bman->SetBlockSize(block_size);
  row_len = rowl;
  release = false;
  current = -1;
  if (initial_size > 0)
    for (unsigned int i = 0; i < (initial_size / rows_in_block) + 1; i++) {
      void *b = bman->GetBlock();
      if (!b) throw common::OutOfMemoryException("too large initial BlockedRowMemStorage size");
      blocks.push_back(b);
    }
  no_rows = initial_size;
}

void BlockedRowMemStorage::Clear() {
  for (auto &b : blocks) bman->FreeBlock(b);  // may be NULL already
  blocks.clear();
  no_rows = 0;
  release = false;
  current = -1;
}

BlockedRowMemStorage::BlockedRowMemStorage(const BlockedRowMemStorage &bs)
    : row_len(bs.row_len),
      block_size(bs.block_size),
      npower(bs.npower),
      ndx_mask(bs.ndx_mask),
      rows_in_block(bs.rows_in_block),
      no_rows(bs.no_rows),
      current(bs.current),
      release(bs.release) {
  bman = bs.bman;
  blocks = bs.blocks;  // shallow copy so far
}

//! compute block_sioze so that 2^n rows fit in a block and the chosen block
//! size >= min_block_size
void BlockedRowMemStorage::CalculateBlockSize(int row_len, int min_block_size) {
  npower = 0;
  for (rows_in_block = 1; row_len * rows_in_block < min_block_size; rows_in_block *= 2) {
    ++npower;
  }
  ndx_mask = (1 << npower) - 1;
  block_size = rows_in_block * row_len;
}

int64_t BlockedRowMemStorage::AddRow(const void *r) {
  if ((no_rows & ndx_mask) == 0) {
    void *b = bman->GetBlock();
    blocks.push_back(b);
  }
  std::memcpy(((char *)blocks.back()) + (no_rows & ndx_mask) * row_len, r, row_len);

  return no_rows++;
}

int64_t BlockedRowMemStorage::AddEmptyRow() {
  if ((no_rows & ndx_mask) == 0) {
    void *b = bman->GetBlock();
    blocks.push_back(b);
  }
  std::memset(((char *)blocks.back()) + (no_rows & ndx_mask) * row_len, 0, row_len);

  return no_rows++;
}

void BlockedRowMemStorage::Rewind(bool rel) {
  DEBUG_ASSERT(!release);
  release = rel;
  if (no_rows == 0) {
    current = -1;
  } else {
    current = 0;
  }
}

bool BlockedRowMemStorage::NextRow() {
  if (++current == no_rows) {
    current = -1;
    return false;
  }
  if (release) {
    if ((current & ndx_mask) == 0) {
      bman->FreeBlock(blocks[(current >> npower) - 1]);
      blocks[(current >> npower) - 1] = NULL;
    }
  }

  return true;
}

bool BlockedRowMemStorage::SetCurrentRow(int64_t row) {
  if (row <= no_rows) {
    current = row;
    return true;
  } else {
    return false;
  }
}

bool BlockedRowMemStorage::SetEndRow(int64_t row) {
  if (row <= no_rows) {
    no_rows = row;
    return true;
  } else {
    return false;
  }
}
}  // namespace core
}  // namespace stonedb
