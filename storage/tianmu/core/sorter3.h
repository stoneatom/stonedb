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
#ifndef TIANMU_CORE_SORTER3_H_
#define TIANMU_CORE_SORTER3_H_
#pragma once

#include <queue>

#include "common/common_definitions.h"
#include "mm/traceable_object.h"
#include "system/cacheable_item.h"
#include "system/tianmu_system.h"

namespace Tianmu {
namespace core {
// Base class for various sorting algorithm

class Sorter3 : public mm::TraceableObject {
 public:
  Sorter3(uint _size, uint _key_bytes, uint _total_bytes)
      : conn(current_txn_), key_bytes(_key_bytes), total_bytes(_total_bytes), size(_size){};

  static Sorter3 *CreateSorter(int64_t size, uint key_bytes, uint total_bytes, int64_t limit = -1,
                               int mem_modifier = 0);

  // Sorter functions

  virtual bool PutValue(Sorter3 *s) = 0;
  /*
        Put the data to be sorted.
        It is assumed that total width of the buffer is total_bytes, and that
        key_bytes are used as sorting keys Return false if the initializing may
     be stopped now (i.e. limit is set and all minimal values already found
  */
  virtual bool PutValue(unsigned char *buf) = 0;
  /*
        Return a pointer to a buffer position containing the next sorted row.
        It is assumed that the pointer is valid until next GetNextValue() is
        alled, or the object is deleted. Return nullptr at the end of data.
  */
  virtual unsigned char *GetNextValue() = 0;
  /*
        Rewind is possible only to the very beginning of the sorter. There is no
        need to Rewind() at start.
  */

  virtual void Rewind() = 0;
  /*
        Memory management
        For internal use only.
  */
  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_SORTER; }
  int Compress() {
    return 0;
  }  // the first stage of conserving memory: delete all redundant data
     // (retrievable from the memory)
  virtual int Collapse() {
    return 0;
  }  // the last stage of conserving memory: delete all data, must be reloaded
     // from disk on next use
  virtual const char *Name() const = 0;

  core::Transaction *conn;
  uint GetObjNo() { return no_obj; }

 protected:
  uint key_bytes;
  uint total_bytes;
  uint size;
  uint no_obj;  // a number of values already added
};

// quicksort on one memory buffer

class SorterOnePass : public Sorter3 {
 public:
  SorterOnePass(uint _size, uint _key_bytes, uint _total_bytes);
  ~SorterOnePass();

  bool PutValue(unsigned char *b) override;
  bool PutValue(Sorter3 *s) override;
  unsigned char *GetNextValue() override;
  void Rewind() override { buf_output_pos = buf; }
  const char *Name() const override { return "Quick Sort"; }
  unsigned char *Getbuf() { return buf; }

 protected:
  void QuickSort();
  void BubbleSort(unsigned char *begin, unsigned char *end);
  void Switch(unsigned char *p1, unsigned char *p2) {
    std::memcpy(buf_tmp, p2, total_bytes);
    std::memcpy(p2, p1, total_bytes);
    std::memcpy(p1, buf_tmp, total_bytes);
  }

  unsigned char *buf;             // a buffer for data
  unsigned char *buf_input_pos;   // a position in buf to put the next data portion to
  unsigned char *buf_output_pos;  // a position in buf to read the next data portion from
  unsigned char *buf_end;         // = buf + size * total_bytes
  unsigned char *buf_tmp;         // a buffer for value swapping

  unsigned char **bound_queue;  // the buffer for a (cyclic) queue of boundaries
                                // of quicksorted regions
  int bound_queue_size;

  bool already_sorted;  // false if sorting was not performed yet
};

// multipass quicksort with merging (many buffers needed for merging)

class SorterMultiPass : public SorterOnePass, private system::CacheableItem {
 public:
  SorterMultiPass(uint _size, uint _key_bytes, uint _total_bytes);
  ~SorterMultiPass();

  bool PutValue(unsigned char *buf) override;
  bool PutValue(Sorter3 *s) override;
  unsigned char *GetNextValue() override;
  void Rewind() override;
  const char *Name() const override { return "Merge Sort"; }
  int Collapse() override {
    return 0;
  }  // the last stage of conserving memory: delete all data, must be reloaded
     // from disk on next use

 private:
  int no_blocks;  // current input block number or a number of blocks

  class Keyblock {
   public:
    Keyblock(int _block, unsigned char *_rec, int _key_bytes) : block(_block), rec(_rec), key_bytes(_key_bytes) {}
    bool operator<(const Keyblock &x) const {
      return key_bytes > 0 && std::memcmp(rec, x.rec, key_bytes) > 0;
    }  // ">" not "<" - because of the way 'priority_queue' works
    int block;
    unsigned char *rec;  // pointer to data
    int key_bytes;
  };
  std::priority_queue<Keyblock> heap;  // heap used for merging blocks

  void InitHeap();                               // prepare heap, load beginnings of blocks into memory
  Keyblock GetFromBlock(int b, bool &reloaded);  // get the next value from block, return (-1,
                                                 // nullptr, 0) if the block is empty
  // set reloaded if the data was reloaded (and the last buffer position
  // preserved)

  struct BlockDescription {
    BlockDescription(int b_size)
        : block_size(b_size), file_offset(0), block_start(nullptr), read_offset(-1), buf_size(0) {}
    int block_size;              // total size in bytes
    int file_offset;             // current offset of file read position
    unsigned char *block_start;  // start of buffer prepared for this block
    int read_offset;             // current reading position; special values: -1 - new buf,
                                 // -2 - end of block
    int buf_size;                // size of buffer prepared for this block
  };

  unsigned char *last_row;               // an additional buffer for preserving the last row
                                         // of reloaded contents
  std::vector<BlockDescription> blocks;  // a list of blocks
};

// counting sort for low-cardinality keys (up to 2 bytes); needs two buffers

class SorterCounting : public Sorter3 {
 public:
  SorterCounting(uint _size, uint _key_bytes, uint _total_bytes);
  ~SorterCounting();

  bool PutValue(unsigned char *buf) override;
  bool PutValue(Sorter3 *s) override;
  unsigned char *GetNextValue() override;
  void Rewind() override { buf_output_pos = buf_output; }
  const char *Name() const override { return "Counting Sort"; }
  unsigned char *Getbuf() { return buf; }

 private:
  int Position(unsigned char *b) { return (key_bytes == 1 ? int(*b) : (int(*b) << 8) + int(*(b + 1))); }
  void CountingSort();

  unsigned char *buf;             // a buffer for input data
  unsigned char *buf_output;      // a buffer for sorted data
  unsigned char *buf_input_pos;   // a position in buf to put the next data portion to
  unsigned char *buf_output_pos;  // a position in buf_out to read the next data
                                  // portion from
  unsigned char *buf_end;         // = buf + size * total_bytes
  unsigned char *buf_output_end;  // actual end of output buffer, measured by input data

  int distrib_min,
      distrib_max;  // minimal and maximal histogram position, measured at input

  bool already_sorted;  // false if sorting was not performed yet
};

// sort with a limit matching one memory buffer

class SorterLimit : public SorterOnePass {
 public:
  SorterLimit(uint _size, uint _key_bytes, uint _total_bytes);
  ~SorterLimit();

  bool PutValue(unsigned char *buf) override;
  bool PutValue(Sorter3 *s) override;
  const char *Name() const override { return "Heap Sort"; }

 private:
  // see SoretrOnePass for more fields

  // uint           no_obj;  // a number of values already added
  unsigned char *zero_buf;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_SORTER3_H_
