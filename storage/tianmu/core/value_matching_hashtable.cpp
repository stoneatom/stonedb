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

#include "value_matching_hashtable.h"

#include "core/bin_tools.h"

namespace Tianmu {
namespace core {

ValueMatching_HashTable::ValueMatching_HashTable() {
  max_no_rows = 0;
  one_pass = false;
  ht_mask = 0;
  next_pos_offset = 0;
}

ValueMatching_HashTable::ValueMatching_HashTable(ValueMatching_HashTable &sec)
    : mm::TraceableObject(sec), ValueMatchingTable(sec), t(sec.t) {
  max_no_rows = sec.max_no_rows;
  one_pass = false;
  ht = sec.ht;
  ht_mask = sec.ht_mask;
  next_pos_offset = sec.next_pos_offset;
  bmanager = sec.bmanager;  // use external bmanager
}

void ValueMatching_HashTable::Clear() {
  ValueMatchingTable::Clear();
  t.Clear();
  ht.assign(ht_mask + 1, -1);
}

void ValueMatching_HashTable::Init(int64_t mem_available, int64_t max_no_groups, int _total_width, int _input_buf_width,
                                   int _match_width) {
  total_width = _total_width;
  matching_width = _match_width;
  input_buffer_width = _input_buf_width;
  DEBUG_ASSERT(input_buffer_width > 0);  // otherwise another class should be used
  one_pass = false;

  // add 4 bytes for offset
  total_width = 4 * ((total_width + 3) / 4);  // round up to 4-byte offset
  next_pos_offset = total_width;
  total_width += 4;

  // grouping table sizes
  int64_t desired_mem = max_no_groups * sizeof(int) * 2 + max_no_groups * total_width;  // oversized ht + normal sized t
  if (desired_mem < mem_available) {
    if (max_no_groups > 100000000) {
      // 100 mln is a size which will create too large ht (>1 GB), so it is
      // limited:
      ht_mask = 0x07FFFFFF;  // 134 mln
    } else {
      ht_mask = (unsigned int)(max_no_groups * 1.2);
      ht_mask = (1 << CalculateBinSize(ht_mask)) - 1;  // 001001010  ->  001111111
    }
    one_pass = true;
  } else {  // not enough memory - split between ht and t
    int64_t hts =
        int64_t(mem_available * double(sizeof(int)) / double(total_width + sizeof(int)));  // proportional split
    hts /= sizeof(int);                                                                    // number of positions in ht
    if (hts > 100000000) {
      // 100 mln is a size which will create too large ht (>1 GB), so it is
      // limited:
      ht_mask = 0x07FFFFFF;  // 134 mln
    } else {
      ht_mask = (unsigned int)(hts * 1.2);
      ht_mask = (1 << CalculateBinSize(ht_mask)) - 1;  // 001001010  ->  001111111
    }
  }
  if (ht_mask < 63)
    ht_mask = 63;

  max_no_rows = max_no_groups;
  if (max_no_rows > 2000000000) {
    max_no_rows = 2000000000;  // row number should be 32-bit
    one_pass = false;
  }

  // initialize structures
  DEBUG_ASSERT(mem_available > 0);
  size_t min_block_len = max_no_rows * total_width;
  if (min_block_len > 1_GB && size_t(mem_available) > 256_MB)  // very large space needed (>1 GB) and
                                                               // substantial memory available
    min_block_len = 16_MB;
  else if (min_block_len > 4_MB)
    min_block_len = 4_MB;

  bmanager = std::make_shared<MemBlockManager>(mem_available, 1);
  t.Init(total_width, bmanager, 0, (int)min_block_len);
  Clear();
}

int64_t ValueMatching_HashTable::ByteSize() {
  int64_t res = (ht_mask + 1) * sizeof(int);
  int64_t max_t_size = bmanager->MaxSize();
  if (max_t_size < max_no_rows * total_width)
    return res + max_t_size;
  if (max_no_rows * total_width < bmanager->BlockSize())  // just one block
    return res + bmanager->BlockSize();
  return res + max_no_rows * total_width;
}

bool ValueMatching_HashTable::FindCurrentRow(unsigned char *input_buffer, int64_t &row, bool add_if_new,
                                             int match_width) {
  unsigned int crc_code = HashValue(input_buffer, match_width);
  unsigned int ht_pos = (crc_code & ht_mask);
  unsigned int row_no = ht[ht_pos];
  if (row_no == 0xFFFFFFFF) {  // empty hash position
    if (!add_if_new) {
      row = common::NULL_VALUE_64;
      return false;
    }
    row_no = no_rows;
    ht[ht_pos] = row_no;
  }
  while (row_no < no_rows) {
    unsigned char *cur_row = (unsigned char *)t.GetRow(row_no);
    if (std::memcmp(cur_row, input_buffer, match_width) == 0) {
      row = row_no;
      return true;  // position found
    }

    unsigned int *next_pos = (unsigned int *)(cur_row + next_pos_offset);
    if (*next_pos == 0) {  // not found and no more conflicted values
      if (add_if_new)
        *next_pos = no_rows;
      row_no = no_rows;
    } else {
      DEBUG_ASSERT(row_no < *next_pos);
      row_no = *next_pos;
    }
  }
  // row_no == no_rows in this place
  if (!add_if_new) {
    row = common::NULL_VALUE_64;
    return false;
  }
  row = row_no;
  int64_t new_row = t.AddEmptyRow();  // 0 is set as a "NextPos"
  ASSERT(new_row == row, "wrong row number");
  std::memcpy(t.GetRow(row), input_buffer, match_width);
  no_rows++;
  return false;
}

bool ValueMatching_HashTable::FindCurrentRow(unsigned char *input_buffer, int64_t &row, bool add_if_new) {
  unsigned int crc_code = HashValue(input_buffer, matching_width);
  unsigned int ht_pos = (crc_code & ht_mask);
  unsigned int row_no = ht[ht_pos];
  if (row_no == 0xFFFFFFFF) {  // empty hash position
    if (!add_if_new) {
      row = common::NULL_VALUE_64;
      return false;
    }
    row_no = no_rows;
    ht[ht_pos] = row_no;
  }
  while (row_no < no_rows) {
    unsigned char *cur_row = (unsigned char *)t.GetRow(row_no);
    if (std::memcmp(cur_row, input_buffer, matching_width) == 0) {
      row = row_no;
      return true;  // position found
    }

    unsigned int *next_pos = (unsigned int *)(cur_row + next_pos_offset);
    if (*next_pos == 0) {  // not found and no more conflicted values
      if (add_if_new)
        *next_pos = no_rows;
      row_no = no_rows;
    } else {
      DEBUG_ASSERT(row_no < *next_pos);
      row_no = *next_pos;
    }
  }
  // row_no == no_rows in this place
  if (!add_if_new) {
    row = common::NULL_VALUE_64;
    return false;
  }
  row = row_no;
  int64_t new_row = t.AddEmptyRow();  // 0 is set as a "NextPos"
  ASSERT(new_row == row, "wrong row number");
  std::memcpy(t.GetRow(row), input_buffer, input_buffer_width);
  no_rows++;
  return false;
}

}  // namespace core
}  // namespace Tianmu
