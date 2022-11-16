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
#ifndef TIANMU_CORE_FILTER_H_
#define TIANMU_CORE_FILTER_H_
#pragma once

#include <boost/pool/pool.hpp>
#include <mutex>

#include "common/common_definitions.h"
#include "compress/bit_stream_compressor.h"
#include "mm/traceable_object.h"
#include "system/fet.h"
#include "system/tianmu_system.h"
#include "util/circ_buf.h"

namespace Tianmu {
namespace core {
class HeapAllocator : public mm::TraceableObject {
 public:
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;

  static char *malloc(const size_type bytes);
  static void free(char *const block);

  static std::mutex mtx;
};

class TheFilterBlockOwner : public mm::TraceableObject {
  friend class HeapAllocator;
  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_FILTER; }
};

extern TheFilterBlockOwner *the_filter_block_owner;

class Filter final : public mm::TraceableObject {
 public:
  Filter(int64_t no_obj, uint32_t power, bool all_ones = false, bool shallow = false);
  Filter(Filter *f, int64_t no_obj, uint32_t power, bool all_ones = false, bool shallow = false);
  Filter(const Filter &filter);
  ~Filter();

  // like Filter(const Filter&), but the Block and block bit table pools are
  // shared The Filter::Block::Owner() is wrong in the copy (points to the
  // original), but it ok as the copy uses pools and mutexes from the original
  static Filter *ShallowCopy(Filter &f);

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_FILTER; };
  // Copying operation
  std::unique_ptr<Filter> Clone() const;

  void ResetDelayed(uint64_t n) { ResetDelayed((int)(n >> no_power), (int)(n & (pack_def - 1))); }
  void SetDelayed(uint64_t n) { SetDelayed((int)(n >> no_power), (int)(n & (pack_def - 1))); }
  void Commit();

  // Random and massive access to filter
  void Set();  // all = 1
  // set position n to 1
  void Set(int64_t n) { Set((int)(n >> no_power), (int)(n & (pack_def - 1))); }
  void SetBetween(int64_t n1, int64_t n2);  // set objects n1-n2 to 1
  void Reset();
  void Reset(int64_t n) { Reset((int)(n >> no_power), (int)(n & (pack_def - 1))); }
  void ResetBlock(size_t b);
  void Reset(unsigned int b,
             int n) {  // keep it inline - it's considerably faster
    int optimized_size = -1;
    block_changed[b] = 1;
    if (block_status[b] == FB_FULL) {
      if (n == block_last_one[b]) {
        if (n == 0)
          block_status[b] = FB_EMPTY;
        else
          block_last_one[b]--;
      } else if (n < block_last_one[b]) {  // else no change
        optimized_size = block_last_one[b];
        block_status[b] = FB_MIXED;
        blocks[b] = block_allocator->Alloc();
        if (b == no_blocks - 1)
          new (blocks[b]) Block(block_filter, no_of_bits_in_last_block,
                                true);  // block_filter->this// set as full,
                                        // then reset a part of it
        else
          new (blocks[b]) Block(block_filter, pack_def, true);  // block_filter->this
        if (blocks[b] == nullptr)
          throw common::OutOfMemoryException();
      }
    }
    if (blocks[b]) {
      if (blocks[b]->Reset(n))
        ResetBlock(b);
      if (optimized_size != -1 && (uint)optimized_size != (pack_def - 1) &&
          (b < no_blocks - 1 || optimized_size != no_of_bits_in_last_block - 1)) {
        blocks[b]->Reset(optimized_size + 1, (b < no_blocks - 1 ? (pack_def - 1) : no_of_bits_in_last_block - 1));
      }
    }
  }
  void ResetBetween(int64_t n1, int64_t n2);
  void ResetBetween(size_t b1, int n1, size_t b2, int n2);
  void Reset(Filter &f2);  // reset all positions where f2 is 1
  bool Get(size_t b, int n);
  bool Get(int64_t n) {
    if (no_blocks == 0)
      return false;
    return Get((n >> no_power), (int)(n & (pack_def - 1)));
  }
  bool IsEmpty();
  bool IsEmpty(size_t b) const;  // block b
  bool IsEmptyBetween(int64_t n1,
                      int64_t n2);  // true if there are only 0 between n1 and n2, inclusively
  bool IsFull() const;
  bool IsFull(unsigned int b) const {  // block b
    DEBUG_ASSERT(b < no_blocks);
    return (block_status[b] == FB_FULL &&
            block_last_one[b] == (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : (pack_def - 1)));
  }
  bool IsFullBetween(int64_t n1,
                     int64_t n2);           // true if there are only 1 between n1 and n2, inclusively
  void CopyBlock(Filter &f, size_t block);  // copy block from f to this
  void DeleteBlock(int pack);

  // Logical operations on filter
  bool IsEqual(Filter &sec);
  void And(Filter &f2);
  void Or(Filter &f2,
          int pack = -1);  // if pack is specified, then only one pack is to be ORed
  void Not();
  void AndNot(Filter &f2);  // reset all positions which are set in f2
  void SwapPack(Filter &f2, int pack);

  // Statistics etc.
  int64_t NumOfOnes() const;
  uint NumOfOnes(int b);            // block b
  uint NumOfOnesUncommited(int b);  // with uncommitted Set/Reset
  int64_t NumOfOnesBetween(int64_t n1,
                           int64_t n2);  // no of 1 between n1 and n2, inclusively
  int64_t NumOfObj() const;              // number of objects (table size)
  size_t NumOfBlocks() const { return no_blocks; }
  int NumOfAddBits() const;
  int DensityWeight();  // = 65537 for empty filter or a filter with only one
                        // nonempty block.
  // Otherwise it is an average number of ones in nonempty blocks.

  boost::pool<HeapAllocator> *GetBitBlockPool() { return bit_block_pool; };
  friend class FilterOnesIterator;
  friend class FilterOnesIteratorOrdered;

  class Block;
  class BlockAllocator;
  Block *GetBlock(size_t b) const {
    DEBUG_ASSERT(b < no_blocks);
    return blocks[b];
  };
  uchar GetBlockStatus(size_t i) {
    DEBUG_ASSERT(i < no_blocks);
    return block_status[i];
  };
  uint32_t GetPower() { return no_power; };
  bool GetBlockChangeStatus(uint32_t n) const {
    DEBUG_ASSERT(n < no_blocks);
    return block_changed[n];
  }
  static const uchar FB_FULL = 0;   // block status: full
  static const uchar FB_EMPTY = 1;  // block status: empty
  static const uchar FB_MIXED = 2;  // block status: neither full nor empty

  static const int bitBlockSize = 8192;

 private:
  Filter() = delete;
  void AddNewBlocks(int new_blocks, bool value, int new_no_bits_last);
  void Construct(bool all_ones, bool shallow);
  void ConstructPool();
  void Set(size_t b, int n);                              // set position n in block b to 1
  void SetBlock(size_t b);                                // all in block = 1
  void SetBetween(size_t b1, int n1, size_t b2, int n2);  // set 1 between...
  // Delayed operations on filter
  void ResetDelayed(unsigned int b, int pos) {
    DEBUG_ASSERT(b < no_blocks);
    DEBUG_ASSERT(delayed_block_set == -1);  // no mixing!
    if (block_status[b] == FB_FULL) {
      if (static_cast<size_t>(delayed_block) != b) {
        Commit();
        delayed_block = b;
        delayed_stats = -1;
      }
      if (pos == delayed_stats + 1) {
        delayed_stats++;
      } else if (pos > delayed_stats + 1) {  // then we can't delay
        if (delayed_stats >= 0)
          ResetBetween(b, 0, b, delayed_stats);
        Reset(b, pos);
        delayed_stats = -2;  // not to use any longer
      }
      // else do nothing
    } else if (block_status[b] == FB_MIXED) {
      Reset(b, pos);
      return;
    }
  }
  void SetDelayed(size_t b, int pos);

  size_t no_blocks{0};  // number of blocks
  uchar *block_status;
  std::vector<bool> block_changed;
  Block **blocks;
  BlockAllocator *block_allocator;
  int no_of_bits_in_last_block;  // number of bits in the last block
  bool shallow;
  ushort *block_last_one;  // Used only for FB_FULL: there is '1' on positions 0
                           // - block_last_one[...] and then '0'
  Filter *block_filter;

  int delayed_stats;          // used for delayed Reset: -1 = initial value; -2 =
                              // delayed not possible;
                              // >=0 = actual set position
  int delayed_block;          // block with uncommitted (delayed) changes, or -1
  int delayed_stats_set;      // used for delayed Set: -1 = initial value; -2 =
                              // delayed not possible;
                              // >=0 = actual set position
  int delayed_block_set{-1};  // block with uncommitted (delayed) changes, or -1
  uint32_t no_power;          // 2^no_power per pack max
  uint32_t pack_def;          // 2^no_power

  boost::pool<HeapAllocator> *bit_block_pool = nullptr;  // default allocator uses system heap
  std::shared_ptr<std::mutex> bit_mut;

 public:
  class Block {
    friend class FilterOnesIterator;
    friend class FilterOnesIteratorOrdered;

   public:
    Block(Filter *owner, int _no_obj, bool set_full = false);
    Block(Block const &block, Filter *owner);
    ~Block();

    void CopyFrom(Block const &block, Filter *owner);
    Block *MoveFromShallowCopy(Filter *new_owner);  // just changes owner
    bool Set(int n);                                // the object = 1
    bool Set(int n1, int n2);                       // set 1 between..., true => block is full
    bool Reset(int n) {
      DEBUG_ASSERT(n < no_obj);
      uint mask = lshift1[n & 31];
      uint &cur_bl = block_table[n >> 5];
      if (cur_bl & mask) {  // if this operation change value
        --no_set_bits;
        cur_bl &= ~mask;
      }
      return (no_set_bits == 0);
    }

    bool Reset(int n1, int n2);  // true => block is empty
    void Reset();
    bool Get(int n) { return (block_table[n >> 5] & lshift1[n & 31]); }
    bool IsEqual(Block &b2);
    bool And(Block &b2);  // true => block is empty
    bool Or(Block &b2);   // true => block is full
    void Not();
    bool AndNot(Block &b2);  // true => block is empty
    uint NumOfOnes() { return no_set_bits; }
    bool IsEmptyBetween(int n1, int n2);
    bool IsFullBetween(int n1, int n2);
    int NumOfOnesBetween(int n1, int n2);
    uint NumOfObj() { return no_obj; }

    Filter *Owner() { return owner; }

   protected:
    uint *block_table;  // objects table
    int no_obj;         // all positions, not just '1'-s
    int no_set_bits;    // number of '1'-s
    int block_size;     // size of the block_table table in uint

    Filter *owner;

    static constexpr uint lshift1[] = {
        1,         2,         4,         8,         0x10,       0x20,       0x40,       0x80,
        0x100,     0x200,     0x400,     0x800,     0x1000,     0x2000,     0x4000,     0x8000,
        0x10000,   0x20000,   0x40000,   0x80000,   0x100000,   0x200000,   0x400000,   0x800000,
        0x1000000, 0x2000000, 0x4000000, 0x8000000, 0x10000000, 0x20000000, 0x40000000, 0x80000000,
    };

    static constexpr int posOf1[] = {
        8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2,
        0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0,
        1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1,
        0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0,
        2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3,
        0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0,
        1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    };
    // No idea how to create an allocator for the pool below to use TIANMU heap, due
    // to static methods in allocator
  };
  class BlockAllocator {
   public:
    BlockAllocator() : block_object_pool(sizeof(Filter::Block)) {}
    ~BlockAllocator() = default;
    Block *Alloc(bool sync = true);
    void Dealloc(Block *b);

   private:
    static constexpr int pool_size = 59;
    static const int pool_stride = 7;
    int free_in_pool = 0;
    int next_ndx = 0;
    Block *pool[pool_size];
    std::mutex block_mut;
    boost::pool<HeapAllocator> block_object_pool;
  };
};

class FilterOnesIterator {
 public:
  FilterOnesIterator();
  FilterOnesIterator(Filter *ff, uint32_t power);
  virtual ~FilterOnesIterator() {}
  void Init(Filter *ff, uint32_t power);
  virtual void Rewind();
  void RewindToRow(const int64_t row);
  bool RewindToPack(size_t pack);
  virtual void NextPack();
  bool NextInsidePack();  // like ++, but rewind to pack beginning after its
                          // end. Return false if we just restarted the pack
  bool IsValid() { return valid; }
  int64_t operator*() const {
    DEBUG_ASSERT(valid);
    return cur_position;
  }

  FilterOnesIterator &operator++() {
    DEBUG_ASSERT(valid);  // cannot iterate invalid iterator
    DEBUG_ASSERT(packs_to_go == -1 || packs_to_go > packs_done);

    if (IsEndOfBlock()) {
      if (!IteratorBpp()) {
        valid = false;
        return *this;
      }
    } else
      iterator_n++;  // now we are on the first suspected position - go forward
                     // if it is not 1

    bool found = false;
    while (!found) {
      if (cur_block_full) {
        cur_position = ((int64_t(iterator_b)) << pack_power) + iterator_n;
        return *this;
      } else if (cur_block_empty) {  // Updating iterator can cause this
        if (!IteratorBpp()) {
          valid = false;
          return *this;
        }
      } else {
        found = FindOneInsidePack();
        if (!found) {
          if (!IteratorBpp()) {
            valid = false;
            return *this;
          }
        }
      }
    }
    cur_position = ((int64_t(iterator_b)) << pack_power) + iterator_n;
    return *this;
  }

  bool IsEndOfBlock() {
    return (uint)iterator_n == (pack_def - 1) ||
           (iterator_b == f->no_blocks - 1 && iterator_n == f->no_of_bits_in_last_block - 1) ||
           (cur_block_full && iterator_n >= f->block_last_one[iterator_b]);
  }

  int64_t GetPackSizeLeft();
  int GetPackCount() const { return f->no_blocks; }
  bool InsideOnePack() { return inside_one_pack; }
  int GetCurrPack() {
    DEBUG_ASSERT(valid);
    return iterator_b;
  }

  int GetCurrInPack() {
    DEBUG_ASSERT(valid);
    return iterator_n;
  }

  void ResetDelayed() {  // reset (delayed) the current position of filter
    f->ResetDelayed(iterator_b, iterator_n);
    uchar stat = f->block_status[iterator_b];
    cur_block_full = (stat == f->FB_FULL);  // note: may still be full, because of delaying
    cur_block_empty = (stat == f->FB_EMPTY);
  }

  void ResetCurrentPackrow() {
    f->ResetBlock(iterator_b);
    cur_block_full = false;
    cur_block_empty = true;
  }

  uint32_t GetPackPower() const { return pack_power; }

  //! make copy of the iterator, allowing the copy to traverse at most \e
  //! packs_to_go packs \e packs_to_go
  virtual FilterOnesIterator *Copy(int packs_to_go = -1);

  void SetNoPacksToGo(int n);

  //! get pack number a packs ahead of the current pack
  virtual int Lookahead(int a);

  static const int max_ahead = 30;

 protected:
  bool valid;
  Filter *f;
  int64_t cur_position;
  unsigned int iterator_b;
  int prev_iterator_b;
  int iterator_n;
  bool cur_block_full;
  bool cur_block_empty;
  uint b;
  int bln;
  int lastn;
  int bitsLeft;
  int ones_left_in_block;
  int prev_block;
  bool inside_one_pack;
  void Reset();
  utils::FixedSizeBuffer<int> buffer;
  int packs_to_go;
  int packs_done;
  uint32_t pack_power;
  uint32_t pack_def;  // 2^no_power - 1

  bool FindOneInsidePack();  // refactored from operator++
 private:
  virtual bool IteratorBpp();
};

class PackOrderer;

// iterating through all values 1 in a filter with given order of blocks
// (datapacks)
class FilterOnesIteratorOrdered : public FilterOnesIterator {
 public:
  FilterOnesIteratorOrdered();  // empty initialization: no filter attached
  FilterOnesIteratorOrdered(Filter *ff, PackOrderer *po, uint32_t power);

  void Init(Filter *ff, PackOrderer *po,
            uint32_t power);  // like constructor - reset iterator position
  void Rewind() override;     // iterate from the beginning (the first 1 in Filter)

  //! move to the next 1 position within an associated Filter
  void NextPack() override;  // iterate to a first 1 in the next nonempty pack
  FilterOnesIteratorOrdered *Copy(int packs_to_go = -1) override;
  //! get pack number a packs ahead of the current pack
  int Lookahead(int a) override;
  bool NaturallyOrdered();  // true if the current position and the rest of
                            // packs are in ascending order

 private:
  PackOrderer *po;

 private:
  bool IteratorBpp() override;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_FILTER_H_
