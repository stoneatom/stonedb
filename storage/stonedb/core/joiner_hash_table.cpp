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

#include "joiner_hash_table.h"

#include "core/tools.h"
#include "core/transaction.h"

namespace stonedb {
namespace core {
#define MAX_HASH_CONFLICTS 128

// This is a list of prime steps used to make the probability of persistent
// conflicts reasonably low. The list should not contain 211 (the minimal size
// of the buffer).

const int prime_steps_no = 55;
const int64_t min_prime_not_in_table = 277;
int prime_steps[prime_steps_no] = {7,   11,  13,  17,  19,  23,  29,  31,  37,  41,  43,  47,  53,  59,
                                   61,  67,  71,  73,  79,  83,  89,  97,  101, 103, 107, 109, 113, 127,
                                   131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197,
                                   199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271};

JoinerHashTable::JoinerHashTable() {
  for_count_only = false;
  initialized = false;
  no_of_occupied = 0;
  t = NULL;
  key_buf_width = 0;
  total_width = 0;
  mult_offset = 0;
  no_attr = 0;
  no_key_attr = 0;
  too_many_conflicts = false;
  no_rows = 0;
  rows_limit = 0;
  current_row = 0;
  to_be_returned = 0;
  current_iterate_step = 0;
}

JoinerHashTable::~JoinerHashTable() { dealloc(t); }

bool JoinerHashTable::AddKeyColumn(vcolumn::VirtualColumn *vc, vcolumn::VirtualColumn *vc_matching) {
  size_t cur_col = encoder.size();
  encoder.push_back(ColumnBinEncoder(ColumnBinEncoder::ENCODER_IGNORE_NULLS));  // comparable, non-monotonic,
                                                                                // non-decodable
  bool success = encoder[cur_col].PrepareEncoder(vc, vc_matching);
  size.push_back(encoder[cur_col].GetPrimarySize());
  return success;
}

void JoinerHashTable::AddTupleColumn(int bin_size) { size.push_back(bin_size); }

void JoinerHashTable::Initialize(int64_t max_table_size, bool easy_roughable) {
  // Input buffer format:		<key_1><key_2>...<key_n>
  // Table format:
  // <key_1><key_2>...<key_n><tuple_1>...<tuple_m><multiplier>
  if (max_table_size < 2)  // otherwise strange things may occur
    max_table_size = 2;
  if (initialized) return;
  AddTupleColumn(8);  // add multiplier column
  no_key_attr = int(encoder.size());
  no_attr = int(size.size());
  if (no_key_attr == 0) return;
  if (no_attr - no_key_attr == 1)  // i.e. no tuple columns, just a multiplier
    for_count_only = true;

  column_offset.reset(new int[no_attr]);

  // Prepare buffers and mappings
  key_buf_width = 0;
  for (int i = 0; i < no_key_attr; i++) {
    encoder[i].SetPrimaryOffset(key_buf_width);
    key_buf_width += size[i];
  }
  key_buf_width = 4 * ((key_buf_width + 3) / 4);  // e.g. 1->4, 12->12, 19->20
  total_width = key_buf_width;
  for (int i = no_key_attr; i < no_attr; i++) {
    column_offset[i] = total_width;
    total_width += size[i];
  }
  mult_offset = column_offset[no_attr - 1];

  // create buffers
  if ((max_table_size + 1) * total_width * 1.25 < 64_MB)
    no_rows = int64_t((max_table_size + 1) * 1.25);  // make sure that rows_limit>=max_no_groups
  else {
    // for easy roughable case the buffer may be smaller
    no_rows = mm::TraceableObject::MaxBufferSize(easy_roughable ? -3 : 0) / total_width;  // memory limitation
    if ((max_table_size + 1) * 1.25 < no_rows) no_rows = int64_t((max_table_size + 1) * 1.25);
  }

  // calculate vertical size (not dividable by a set of some prime numbers)
  if (no_rows < min_prime_not_in_table)  // too less rows => high collision probability
    no_rows = min_prime_not_in_table;
  else
    no_rows = (no_rows / min_prime_not_in_table + 1) * min_prime_not_in_table;

  // make sure that the size is not dividable by any of prime_steps
  bool increased = false;
  do {
    increased = false;
    for (int i = 0; i < prime_steps_no; i++)
      if (no_rows % prime_steps[i] == 0) {
        increased = true;
        no_rows += min_prime_not_in_table;
        break;
      }
  } while (increased);
  rows_limit = int64_t(no_rows * 0.9);  // rows_limit is used to determine whether the table is full

  t = (unsigned char *)alloc(((total_width / 4) * no_rows) * 4,
                             mm::BLOCK_TYPE::BLOCK_TEMPORARY);  // no need to cache on disk
  input_buffer.reset(new unsigned char[key_buf_width]);

  // initialize everything
  ClearAll();
  Transaction *m_conn = current_tx;
  rccontrol.lock(m_conn->GetThreadID()) << "Hash join buffer initialized for up to " << no_rows << " rows, "
                                        << key_buf_width << "+" << total_width - key_buf_width << " bytes."
                                        << system::unlock;
  initialized = true;
}

int64_t JoinerHashTable::FindAndAddCurrentRow()  // a position in the current JoinerHashTable,
                                                 // common::NULL_VALUE_64 if no more space
{
  // MEASURE_FET("JoinerHashTable::FindAndAddCurrentRow()");
  if (no_of_occupied >= rows_limit)  // no more space
    return common::NULL_VALUE_64;

  unsigned int crc_code = HashValue(input_buffer.get(), key_buf_width);
  int64_t row = crc_code % no_rows;
  // vertical table size is not dividable by the following step values, so they
  // will eventually iterate through the whole table
  int64_t iterate_step = prime_steps[crc_code % prime_steps_no];
  int64_t startrow = row;
  unsigned char *cur_t;
  do {
    cur_t = t + row * total_width;
    int64_t *multiplier = (int64_t *)(cur_t + mult_offset);
    if (*multiplier != 0) {
      if (std::memcmp(cur_t, input_buffer.get(), key_buf_width) == 0) {
        // i.e. identical row found
        int64_t last_multiplier = *multiplier;
        DEBUG_ASSERT(last_multiplier > 0);
        (*multiplier)++;
        if (for_count_only) return row;
        // iterate several steps forward to find some free location
        row += iterate_step * last_multiplier;
        if (row >= no_rows) row = row % no_rows;

        if (*multiplier > MAX_HASH_CONFLICTS)  // a threshold for switching sides
          too_many_conflicts = true;
      } else {  // some other value found
        // iterate one step forward
        row += iterate_step;
        if (row >= no_rows) row = row % no_rows;
      }
    } else {
      std::memcpy(cur_t, input_buffer.get(), key_buf_width);
      *multiplier = 1;
      no_of_occupied++;
      return row;
    }
  } while (row != startrow);
  return common::NULL_VALUE_64;  // search deadlock (we returned to the same
                                 // position)
}

int64_t JoinerHashTable::InitCurrentRowToGet() {
  // MEASURE_FET("JoinerHashTable::InitCurrentRowToGet()");
  unsigned int crc_code = HashValue(input_buffer.get(), key_buf_width);
  current_row = crc_code % no_rows;
  // vertical table size is not dividable by the following step values, so they
  // will eventually iterate through the whole table
  current_iterate_step = prime_steps[crc_code % prime_steps_no];
  int64_t startrow = current_row;
  to_be_returned = 0;
  unsigned char *cur_t;
  int64_t multiplier;
  do {
    cur_t = t + current_row * total_width;
    multiplier = *(int64_t *)(cur_t + mult_offset);
    if (multiplier != 0) {
      if (std::memcmp(cur_t, input_buffer.get(), key_buf_width) == 0) {
        // i.e. identical row found
        to_be_returned = multiplier;
        return multiplier;
      } else {  // some other value found
        // iterate one step forward
        current_row += current_iterate_step;
        if (current_row >= no_rows) current_row = current_row % no_rows;
      }
    } else {
      current_row = common::NULL_VALUE_64;  // not found
      return 0;
    }
  } while (current_row != startrow);
  current_row = common::NULL_VALUE_64;  // not found at all
  return 0;
}

int64_t JoinerHashTable::GetNextRow() {
  // MEASURE_FET("JoinerHashTable::InitCurrentRowToGet()");
  if (to_be_returned == 0) return common::NULL_VALUE_64;
  int64_t row_to_return = current_row;
  to_be_returned--;
  if (to_be_returned == 0) return row_to_return;
  // now prepare the next row to be returned
  int64_t startrow = current_row;
  current_row += current_iterate_step;
  if (current_row >= no_rows) current_row = current_row % no_rows;
  do {
    DEBUG_ASSERT(*((int *)(t + current_row * total_width + mult_offset)) != 0);
    if (std::memcmp(t + current_row * total_width, input_buffer.get(), key_buf_width) == 0) {
      // i.e. identical row found - keep the current_row
      return row_to_return;
    } else {  // some other value found - iterate one step forward
      current_row += current_iterate_step;
      if (current_row >= no_rows) current_row = current_row % no_rows;
    }
  } while (current_row != startrow);
  return row_to_return;
}

void JoinerHashTable::ClearAll() {
  std::memset(t, 0, total_width * no_rows);
  std::memset(input_buffer.get(), 0, key_buf_width);
  no_of_occupied = 0;
  for (int i = 0; i < no_key_attr; i++) encoder[i].ClearStatistics();
}

bool JoinerHashTable::StringEncoder(int col) { return encoder[col].IsString(); }

bool JoinerHashTable::ImpossibleValues(int col, int64_t local_min, int64_t local_max) {
  return encoder[col].ImpossibleValues(local_min, local_max);
}

bool JoinerHashTable::ImpossibleValues(int col, types::BString &local_min, types::BString &local_max) {
  return encoder[col].ImpossibleValues(local_min, local_max);
}
}  // namespace core
}  // namespace stonedb