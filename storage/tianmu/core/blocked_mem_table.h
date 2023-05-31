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
#ifndef TIANMU_CORE_BLOCKED_MEM_TABLE_H_
#define TIANMU_CORE_BLOCKED_MEM_TABLE_H_
#pragma once

#include <cstring>
#include <mutex>
#include <set>
#include <vector>

#include "mm/traceable_object.h"

namespace Tianmu {
namespace core {
class MemBlockManager final : public mm::TraceableObject {
 public:
  /*!
   * Create the manager
   * \param mem_limit - the allowed summarized size of blocks in bytes. Used by
   * BlocksAllowed() function \param no_threads - how many parallel threads will
   * be the clients. Influences BlocksAllowed() function \param mem_hard_limit -
   * (in bytes) after summarized size of created blocks exceeds this, further
   * requests for new block will fail. Value -1 means no hard limit \param
   * block_size - size of a block in bytes
   */
  MemBlockManager(int64_t mem_limit, int no_threads, int block_size = 4_MB, int64_t mem_hard_limit = -1)
      : block_size(block_size), size_limit(mem_limit), hard_size_limit(mem_hard_limit), no_threads(no_threads) {}

  MemBlockManager(const MemBlockManager &mbm) = delete;

  /*!
   * Set the block size when it is calculated later, after normal
   * initialization, but before any block has been created.
   */
  void SetBlockSize(int bsize) {
    std::scoped_lock guard(mx);
    DEBUG_ASSERT((current_size == 0 || block_size == bsize));
    block_size = bsize;
  }

  /*!
   * Delete the manager and all the memory blocks it has created, independently
   * if they were freed with FreeBlock() or not
   */
  ~MemBlockManager();

  /*!
   * The memory used by allocated blocks should not exceed a limit given in
   * constructor. Before asking for a block the clients should ask if it is
   * allowed to get a block. \return a number of memory block that can be given
   * to a client
   */
  int MemoryBlocksLeft();

  /*!
   *\return the size of a block in bytes as defined in constructor or Init()
   */
  int BlockSize() { return block_size; }
  /*!
   *  \return the upper limit for the memory summarized across all the potential
   * buffers
   */
  int64_t MaxSize() { return size_limit; }
  /*!
   * A request for a memory block.
   * \return address of a memory block the client can use, or 0 if no blocks can
   * be given (hard limit is set and reached)
   */
  void *GetBlock();

  /*!
   * Return a memory block to the manager.
   * \param b - the address of a block previously obtained through GetBlock()
   */
  void FreeBlock(void *b);

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }

 private:
  std::vector<void *> free_blocks;
  std::set<void *> used_blocks;
  int block_size;          // all sizes in bytes
  int64_t size_limit{-1};  //-1 if not initialized = no limit
  int64_t hard_size_limit{-1};
  int no_threads{1};
  int64_t current_size{0};
  std::mutex mx;
};

class BlockedRowMemStorage {
 public:
  BlockedRowMemStorage() : no_rows(0), current(-1) {}
  //! copy reusing the same external block manager or create a deep copy if
  //! block manager is private
  BlockedRowMemStorage(const BlockedRowMemStorage &bs);

  /*!
   * initialize the storage for rows row_len bytes long, stored in memory blocks
   * provided by mbm.
   */
  void Init(int row_len, std::shared_ptr<MemBlockManager> mbm, uint64_t initial_size, int min_block_len = 4_MB);

  //! memory blocks are release at mbm destruction
  ~BlockedRowMemStorage() = default;

  //! make the storage empty
  void Clear();

  //! provide the pointer to the requested row bytes
  void *GetRow(int64_t r) {
    DEBUG_ASSERT(r < no_rows);
    int64_t b = r >> npower;
    return ((char *)blocks[b]) + row_len * (r & ndx_mask);
  }

  void *operator[](uint64_t idx) { return GetRow(idx); }
  int64_t NoRows() { return no_rows; }
  //! add the row (copy bytes) and return the obtained row number. row_len bytes
  //! will be copied
  int64_t AddRow(const void *r);

  //! add a row and return the obtained row number. row_len bytes will be filled
  //! by 0
  int64_t AddEmptyRow();

  void Set(uint64_t idx, const void *r) {
    DEBUG_ASSERT(idx < static_cast<uint64_t>(no_rows));
    std::memcpy(((char *)blocks[idx >> npower]) + (idx & ndx_mask) * row_len, r, row_len);
  }

  //! rewind the built-in iterator. Must be used before any traversal.
  //! \param release = true: dealloc traversed memory blocks. After traversal
  //! the storage is unusable then
  void Rewind(bool release);

  //! provide the index to the current row, return -1 if iterator is not valid
  int64_t GetCurrent() { return current; }
  bool SetCurrentRow(int64_t row);
  bool SetEndRow(int64_t row);
  //! move the built-in iterator to the next row
  //! \return true : there is the next row, false - all rows traversed and
  //! subsequent GetCurrent() will return 0
  bool NextRow();

 private:
  void CalculateBlockSize(int row_len, int min_block_size);

  std::shared_ptr<MemBlockManager> bman;

  int row_len;         // in bytes;
  int block_size{-1};  // in bytes
  int npower;          // block contains 2^npower rows
  int64_t ndx_mask;    // row_num & ndx_mask == row index in a block
  int rows_in_block;
  int64_t no_rows;  // number of added rows

  std::vector<void *> blocks;

  // iterator related
  int64_t current;
  bool release;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_BLOCKED_MEM_TABLE_H_
