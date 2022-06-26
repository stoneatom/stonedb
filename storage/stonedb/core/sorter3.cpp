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
#include <iostream>

#include "common/common_definitions.h"
#include "core/bin_tools.h"
#include "core/tools.h"
#include "core/transaction.h"
#include "sorter3.h"
#include "system/fet.h"
#include "system/rc_system.h"

namespace stonedb {
namespace core {
// A static function to create a proper sorter
Sorter3 *Sorter3::CreateSorter(int64_t size, uint key_bytes, uint total_bytes, int64_t limit, int mem_modifier) {
  // Determine the sorting algorithm
  auto max_mem_size = 64_MB;
  if (size_t(size * total_bytes * 2) > 64_MB)
    max_mem_size = mm::TraceableObject::MaxBufferSize(mem_modifier) *
                   2;  // *2, because we allow sorter buffers to be exceptionally big

  int64_t max_no_rows = max_mem_size / total_bytes;
  if (limit != -1 && limit < max_no_rows / 3 && limit < size / 3) {
    DEBUG_ASSERT(limit >= 0);
    return new SorterLimit((uint)limit, key_bytes, total_bytes);
  }
  if (max_no_rows > size) {        // matching the memory
    if (max_no_rows > size * 2 &&  // two buffers needed for counting sort
        ((key_bytes == 1 && size > 1024) || (key_bytes == 2 && size > 256000)))
      return new SorterCounting((uint)size, key_bytes, total_bytes);
    else
      return new SorterOnePass((uint)size, key_bytes, total_bytes);
  }
  return new SorterMultiPass((uint)max_no_rows, key_bytes, total_bytes);
}

SorterOnePass::SorterOnePass(uint _size, uint _key_bytes, uint _total_bytes)
    : Sorter3(_size, _key_bytes, _total_bytes) {
  no_obj = 0;
  bound_queue_size = 2 + size / 10;
  if (bound_queue_size < 10) bound_queue_size = 10;

  buf = NULL;
  bound_queue = NULL;
  buf_tmp = NULL;
  if (size > 0) {
    buf = (unsigned char *)alloc(size * total_bytes, mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);
    if (buf == NULL) throw common::OutOfMemoryException();
    bound_queue =
        (unsigned char **)alloc(bound_queue_size * 2 * sizeof(unsigned char *), mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);
    if (bound_queue == NULL) {
      dealloc(buf);
      throw common::OutOfMemoryException();
    }
    buf_tmp = new unsigned char[total_bytes];
  }
  buf_input_pos = buf;
  buf_output_pos = buf;
  buf_end = buf + size * total_bytes;
  already_sorted = true;  // empty sorter is already sorted
}

SorterOnePass::~SorterOnePass() {
  if (buf) dealloc(buf);
  if (bound_queue) dealloc(bound_queue);
  delete[] buf_tmp;
}

bool SorterOnePass::PutValue(unsigned char *b) {
  DEBUG_ASSERT(buf_input_pos < buf_end);
  already_sorted = false;
  std::memcpy(buf_input_pos, b, total_bytes);
  buf_input_pos += total_bytes;
  if (no_obj < size) no_obj++;
  return true;
}

bool SorterOnePass::PutValue(Sorter3 *s) {
  STONEDB_LOG(LogCtl_Level::WARN, "SorterOnePass PutValue merger, never verified.");
  DEBUG_ASSERT(buf_input_pos < buf_end);
  already_sorted = false;
  SorterOnePass *sp = (SorterOnePass *)s;
  int tmpobj = sp->GetObjNo();
  if (tmpobj) {
    unsigned char *b = sp->Getbuf();  // the beginning of buffer
    std::memcpy(buf_input_pos, b, tmpobj * total_bytes);
    no_obj += tmpobj;
    buf_input_pos += no_obj * total_bytes;
  }
  return true;
}

unsigned char *SorterOnePass::GetNextValue() {
  if (!already_sorted) {
    QuickSort();
    already_sorted = true;
  }
  if (buf_output_pos == buf_input_pos) return NULL;
  unsigned char *res = buf_output_pos;
  buf_output_pos += total_bytes;
  return res;
}

void SorterOnePass::QuickSort() {
#ifdef FUNCTIONS_EXECUTION_TIMES
  FETOperator feto("SorterOnePass::QuickSort(...)");
#endif
  if (key_bytes == 0) return;
  ptrdiff_t bubble_sort_limit = 20 * total_bytes;
  bound_queue[0] = buf;
  bound_queue[1] = buf_input_pos - total_bytes;
  if (buf_input_pos - buf < bubble_sort_limit) {
    BubbleSort(buf, buf_input_pos - total_bytes);
    return;
  }

  unsigned char *mid_val;
  unsigned char *s1;
  unsigned char *s2;  // temporary start and stop
  unsigned char *i;
  unsigned char *j;
  int queue_read = 0, queue_write = 2;  // jump by two
  while (queue_read != queue_write) {   // this loop is instead of Quicksort recurrency
    if (conn->Killed()) throw common::KilledException();
    s1 = bound_queue[queue_read];
    s2 = bound_queue[queue_read + 1];
    queue_read = (queue_read + 2) % bound_queue_size;
    if (s1 == s2) continue;
    // note: quicksorting at least 20 positions
    int r = 2 + rand() % 4;  // random values 2, 3, 4, 5  - for randomized quicksort
    mid_val = s1 + total_bytes * (int64_t((s2 - s1) / total_bytes) / 8) *
                       r;  // middle position, somewhere between 2/8 and 5/8
    i = s1;
    j = s2;
    do {
      while (std::memcmp(i, mid_val, key_bytes) < 0) i += total_bytes;
      while (std::memcmp(j, mid_val, key_bytes) > 0) j -= total_bytes;
      if (i <= j) {
        if (i != j) {
          Switch(i, j);
          if (i == mid_val)
            mid_val = j;  // move the middle point, if needed
          else if (j == mid_val)
            mid_val = i;
        }
        i += total_bytes;
        j -= total_bytes;
      }
    } while (i <= j);
    if (s1 < j) {  // add new intervals to sort queue
      if (j - s1 < bubble_sort_limit)
        BubbleSort(s1, j);
      else {
        bound_queue[queue_write] = s1;
        bound_queue[queue_write + 1] = j;
        queue_write += 2;  // cyclic buffer
        if (queue_write >= bound_queue_size) queue_write -= bound_queue_size;
      }
    }
    if (i < s2) {
      if (s2 - i < bubble_sort_limit)
        BubbleSort(i, s2);
      else {
        bound_queue[queue_write] = i;
        bound_queue[queue_write + 1] = s2;
        queue_write += 2;  // cyclic buffer
        if (queue_write >= bound_queue_size) queue_write -= bound_queue_size;
      }
    }
  }
}

void SorterOnePass::BubbleSort(unsigned char *s1,
                               unsigned char *s2)  // technical function: bubble sort of buf from position s1 to s2
{
#ifdef FUNCTIONS_EXECUTION_TIMES
  FETOperator feto("SorterOnePass::BubbleSort(...)");
#endif

  // NOTE: Why bubble sort?
  // Because it is not so bad for <20 values.
  // Actually, insertion sort with binary search was also considered, but it
  // turned out to be a bit slower.
  unsigned char *j;
  unsigned char *pom;
  j = s2;
  do {
    pom = NULL;
    for (unsigned char *i = s1; i < s2; i += total_bytes) {
      if (std::memcmp(i, i + total_bytes, key_bytes) > 0) {
        Switch(i, i + total_bytes);
        pom = i;
      }
    }
    j = pom;
  } while (j != NULL);
}

SorterMultiPass::SorterMultiPass(uint _size, uint _key_bytes, uint _total_bytes)
    : SorterOnePass(_size, _key_bytes, _total_bytes), system::CacheableItem("JW", "SR3") {
  no_blocks = 0;
  last_row = new unsigned char[total_bytes];
}

SorterMultiPass::~SorterMultiPass() { delete[] last_row; }

bool SorterMultiPass::PutValue(unsigned char *b) {
  if (buf_input_pos == buf_end) {
    QuickSort();
    blocks.push_back(BlockDescription(int(buf_input_pos - buf)));
    CI_Put(no_blocks, buf, int(buf_input_pos - buf));
    no_blocks++;
    buf_input_pos = buf;
  }
  already_sorted = false;
  std::memcpy(buf_input_pos, b, total_bytes);
  buf_input_pos += total_bytes;
  return true;
}

bool SorterMultiPass::PutValue([[maybe_unused]] Sorter3 *s) {
  STONEDB_LOG(LogCtl_Level::WARN, "PutValue Stub, should not see it now");
  return true;
}

unsigned char *SorterMultiPass::GetNextValue() {
  if (!already_sorted) {
    QuickSort();
    blocks.push_back(BlockDescription(int(buf_input_pos - buf)));
    CI_Put(no_blocks, buf, int(buf_input_pos - buf));
    no_blocks++;
    InitHeap();
    already_sorted = true;
  }
  // merge
  if (heap.empty()) return NULL;
  SorterMultiPass::Keyblock cur_val = heap.top();
  unsigned char *res = cur_val.rec;
  heap.pop();
  bool reloaded = false;
  cur_val = GetFromBlock(cur_val.block, reloaded);
  if (cur_val.block != -1)  // not end of block
    heap.push(cur_val);
  if (reloaded) return last_row;
  return res;
}

void SorterMultiPass::Rewind() {
  while (!heap.empty())  // clear the heap
    heap.pop();
  for (int i = 0; i < no_blocks; i++) {
    blocks[i].file_offset = 0;
    blocks[i].read_offset = -1;
  }
  InitHeap();
}

void SorterMultiPass::InitHeap() {
  // block descriptors are initially prepared, but with empty values
  int rows_in_small_buffer = size / no_blocks;
  if (rows_in_small_buffer < 3) {  // extremely rare case: very large data, very small memory
    rows_in_small_buffer = 3;
    dealloc(buf);
    buf = (unsigned char *)alloc(3 * total_bytes * no_blocks, mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);
    if (buf == NULL) throw common::OutOfMemoryException();
  }
  int standard_buf_size = rows_in_small_buffer * total_bytes;
  int last_buf_size =
      (size * total_bytes) -
      standard_buf_size * (no_blocks - 1);  // using the rest of buf (otherwise some bytes would be wasted)
  for (int i = 0; i < no_blocks; i++) {
    blocks[i].block_start = buf + i * standard_buf_size;
    blocks[i].buf_size = (i < no_blocks - 1 ? standard_buf_size : last_buf_size);
    // block defined, now load it
    bool dummy;
    SorterMultiPass::Keyblock kb = GetFromBlock(i, dummy);
    if (kb.block != -1)  // not the end of block
      heap.push(kb);
  }
}

SorterMultiPass::Keyblock SorterMultiPass::GetFromBlock(int b, bool &reloaded) {
  if (blocks[b].read_offset == -2) return SorterMultiPass::Keyblock(-1, NULL, 0);  // end of block
  if (blocks[b].read_offset == -1) {                                               // new buffer, to be read
    if (blocks[b].file_offset > 0) {  // preserve the last row before overwriting the whole buffer
      std::memcpy(last_row, blocks[b].block_start + blocks[b].buf_size - total_bytes, total_bytes);
      reloaded = true;
    }
    int bytes_to_load = std::min(blocks[b].block_size - blocks[b].file_offset, blocks[b].buf_size);
    if (bytes_to_load <= 0) {
      blocks[b].read_offset = -2;
      return SorterMultiPass::Keyblock(-1, NULL, 0);  // end of block
    }
    CI_Get(b, blocks[b].block_start, bytes_to_load, blocks[b].file_offset);
    blocks[b].file_offset += bytes_to_load;
    blocks[b].read_offset = 0;
    blocks[b].buf_size = bytes_to_load;  // may be less than initially given if
                                         // the end of block is near
    if (conn->Killed()) throw common::KilledException();
  }
  Keyblock kb(b, blocks[b].block_start + blocks[b].read_offset, key_bytes);
  blocks[b].read_offset += total_bytes;
  if (blocks[b].read_offset >= blocks[b].buf_size)
    blocks[b].read_offset = -1;  // end of buffer, needs to reload a new portion
                                 // of data in the next GetFromBlock
  return kb;
}

SorterCounting::SorterCounting(uint _size, uint _key_bytes, uint _total_bytes)
    : Sorter3(_size, _key_bytes, _total_bytes) {
  buf = (unsigned char *)alloc(size * total_bytes, mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);
  if (buf == NULL) throw common::OutOfMemoryException();
  buf_output = (unsigned char *)alloc(size * total_bytes, mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);
  if (buf_output == NULL) {
    dealloc(buf);
    throw common::OutOfMemoryException();
  }

  DEBUG_ASSERT(key_bytes <= 2);
  already_sorted = false;
  distrib_min = 65536;
  distrib_max = 0;
  buf_input_pos = buf;
  buf_output_pos = buf_output;
  buf_end = buf + size * total_bytes;
  buf_output_end = NULL;  // to be calculated
}

SorterCounting::~SorterCounting() {
  if (buf) dealloc(buf);
  if (buf_output) dealloc(buf_output);
}

bool SorterCounting::PutValue(unsigned char *b) {
  DEBUG_ASSERT(buf_input_pos < buf_end);
  std::memcpy(buf_input_pos, b, total_bytes);
  int pos = Position(b);
  if (pos > distrib_max) distrib_max = pos;
  if (pos < distrib_min) distrib_min = pos;
  buf_input_pos += total_bytes;
  return true;
}

bool SorterCounting::PutValue([[maybe_unused]] Sorter3 *s) {
  STONEDB_LOG(LogCtl_Level::WARN, "PutValue Stub, should not see it now");
  return true;
}

unsigned char *SorterCounting::GetNextValue() {
  if (!already_sorted) {
    CountingSort();
    already_sorted = true;
  }
  if (buf_output_pos == buf_output_end) return NULL;
  unsigned char *res = buf_output_pos;
  buf_output_pos += total_bytes;
  return res;
}

void SorterCounting::CountingSort() {
  DEBUG_ASSERT(distrib_min <= distrib_max);
  int distrib_size = distrib_max - distrib_min + 1;

  std::vector<int> distrib(distrib_size);
  unsigned char *p = buf;
  while (p < buf_input_pos) {  // first pass: count all cases
    int pos = Position(p) - distrib_min;
    distrib[pos]++;
    p += total_bytes;
  }
  std::vector<unsigned char *> dest;
  dest.reserve(distrib_size);
  int sum = 0;
  for (int i = 0; i < distrib_size; i++) {
    dest.emplace_back(buf_output + total_bytes * sum);
    sum += distrib[i];
  }
  buf_output_end = buf_output + total_bytes * sum;

  p = buf;
  while (p < buf_input_pos) {  // second pass: move to the proper position
    int pos = Position(p) - distrib_min;
    std::memcpy(dest[pos], p, total_bytes);
    dest[pos] += total_bytes;
    p += total_bytes;
  }
}

SorterLimit::SorterLimit(uint _size, uint _key_bytes, uint _total_bytes)
    : SorterOnePass(_size, _key_bytes, _total_bytes) {
  buf_input_pos = buf + size * total_bytes;  // simulate the end of buffer
  zero_buf = NULL;
  if (key_bytes > 0) {
    zero_buf = new unsigned char[key_bytes];
    std::memset(zero_buf, 0, key_bytes);
  }
  no_obj = 0;
}

SorterLimit::~SorterLimit() { delete[] zero_buf; }

bool SorterLimit::PutValue(unsigned char *b) {
  already_sorted = false;
  if (key_bytes == 0) {  // case of constant values: just copy the first rows
    if (no_obj < size) {
      std::memcpy(buf + no_obj * total_bytes, b, total_bytes);
      no_obj++;
    }
  } else if (no_obj < size) {  // the heap is not full
    uint n = ++no_obj;
    std::memcpy(buf + (n - 1) * total_bytes, b, total_bytes);
    uint parent = n >> 1;
    while (parent > 0) {
      unsigned char *p1 = buf + (parent - 1) * total_bytes;
      unsigned char *p2 = buf + (n - 1) * total_bytes;
      if (std::memcmp(p1, p2, key_bytes) >= 0) break;
      Switch(p1, p2);
      n = parent;
      parent = n >> 1;
    }
  } else {
    if (std::memcmp(buf, b, key_bytes) > 0) {  // compare with the first position
      uint n = 1;
      uint child = 2;
      uint child_greater;
      bool cont = (child <= size);
      while (cont) {
        if (child == size || std::memcmp(buf + (child - 1) * total_bytes, buf + child * total_bytes, key_bytes) > 0)
          child_greater = child - 1;
        else
          child_greater = child;
        if (std::memcmp(buf + child_greater * total_bytes, b, key_bytes) > 0) {
          std::memcpy(buf + (n - 1) * total_bytes, buf + child_greater * total_bytes, total_bytes);
          n = child_greater + 1;
          child = n << 1;
          cont = (child <= size);
        } else
          cont = false;
      }
      n--;
      std::memcpy(buf + n * total_bytes, b, total_bytes);
    } else {                                                         // check whether it is not sorted already
      if (std::memcmp(buf, zero_buf, key_bytes) == 0) return false;  // zero => nothing better possible
    }
  }
  return true;
}

// This API only combine the sorter which is already sorted
bool SorterLimit::PutValue(Sorter3 *s) {
  // Just copy the data leave the sort part to QuickSort when GetNextValue is
  // invoked
  already_sorted = false;
  if (std::strcmp(s->Name(), "Heap Sort")) throw common::InternalException("Input sorter does not belong to Heap Sort");

  SorterLimit *sl = (SorterLimit *)s;
  // Note: PutValue(Sorter3 * s) is dependent on PutValue(char *)
  // Assumption/(Fix Me):Following logic has the assumption no_obj is already
  // assigned at PutValue(char*). tmp_noobj would be 0 if PutValue(char *) is
  // not called
  uint tmp_noobj = sl->GetObjNo();
  unsigned char *buf1 = sl->Getbuf();
  STONEDB_LOG(LogCtl_Level::DEBUG, "PutValue: total bytes %d, tmp_noobj %ld ", total_bytes, tmp_noobj);

  if (tmp_noobj) {
    if (no_obj + tmp_noobj <= size) {
      std::memcpy(buf + no_obj * total_bytes, buf1, tmp_noobj * total_bytes);
      no_obj += tmp_noobj;
      STONEDB_LOG(LogCtl_Level::DEBUG, "PutValue: no_obj %ld, tmp_noobj %ld, total_bytes %ld buf %s", no_obj, tmp_noobj,
                  total_bytes, buf);
    } else {
      STONEDB_LOG(LogCtl_Level::ERROR, "error in Putvalue, out of  boundary size %d no_obj+tmp_noobj %d", size,
                  no_obj + tmp_noobj);
      throw common::OutOfMemoryException("Out of boundary after merge sorters together.");
      return false;
    }
  }
  return true;
}
}  // namespace core
}  // namespace stonedb