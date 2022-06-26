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

#include "group_distinct_table.h"

#include "core/mi_iterator.h"
#include "core/transaction.h"

namespace stonedb {
namespace core {
GroupDistinctTable::GroupDistinctTable(uint32_t power)
    : input_buffer(NULL),
      t(NULL),
      group_bytes(0),
      value_bytes(0),
      total_width(0),
      max_total_size(0),
      no_rows(0),
      rows_limit(0),
      no_of_occupied(0),
      initialized(false),
      use_CRC(false),
      filter_implementation(false),
      f(NULL),
      group_factor(0) {
  max_total_size = 64_MB;
  encoder = NULL;
  input_length = 0;
  pack_power = power;
}

GroupDistinctTable::~GroupDistinctTable() {
  dealloc(t);
  //	delete t;
  delete[] input_buffer;
  delete f;
  delete encoder;
}

void GroupDistinctTable::InitializeVC(int64_t max_no_groups, vcolumn::VirtualColumn *vc, int64_t max_no_rows,
                                      int64_t max_bytes, bool decodable) {
  DEBUG_ASSERT(!initialized);
  if (max_bytes > 0) max_total_size = max_bytes;
  if (max_bytes > 2000000000)  // possible for large aggregation settings, but
                               // not allowed here - limit to 1 GB
    max_total_size = 1_GB;
  if (max_no_rows == common::NULL_VALUE_64) max_no_rows = 0;  // not known
  encoder = new ColumnBinEncoder(
      ColumnBinEncoder::ENCODER_IGNORE_NULLS |
      (decodable ? ColumnBinEncoder::ENCODER_DECODABLE : 0));  // non-monotonic comparable, usually not decodable
  encoder->PrepareEncoder(vc);
  // Filter implementation, if possible:
  if (encoder->MaxCode() != common::NULL_VALUE_64 && encoder->GetPrimarySize() <= 4 && max_no_groups < 1000000000) {
    int64_t filter_positions = max_no_groups * (encoder->MaxCode() + 1);
    if (filter_positions / 8 < max_total_size &&   // general memory limitation
        filter_positions / 8 < 4 * max_no_rows) {  // compare with a good case hash implementation
      filter_implementation = true;
      group_factor = max_no_groups;
      InitializeBuffers(filter_positions);
      initialized = true;
      return;
    }
  }

  // Hash implementation:
  // calculate byte sizes
  group_bytes = 0;
  value_bytes = encoder->GetPrimarySize();
  max_no_groups += 1;  // 0 is reserved for "empty position"
  uint64_t m = max_no_groups;
  while (m > 0) {
    m = m >> 8;
    group_bytes++;
  }
  total_width = value_bytes + group_bytes;

  // create buffers
  InitializeBuffers(max_no_rows);
  initialized = true;
}

void GroupDistinctTable::InitializeB(int max_len, int64_t max_no_rows)  // char* initialization
{
  DEBUG_ASSERT(!initialized);
  input_length = max_len;
  group_bytes = 1;
  if (max_len > HASH_FUNCTION_BYTE_SIZE) {
    value_bytes = HASH_FUNCTION_BYTE_SIZE;
    use_CRC = true;
  } else
    value_bytes = max_len;
  total_width = value_bytes + group_bytes;
  if (max_no_rows * total_width * 2 > max_total_size)
    max_total_size = mm::TraceableObject::MaxBufferSizeForAggr(max_no_rows * total_width *
                                                               2);  // Check memory only for larger tables.

  // create buffers
  InitializeBuffers(max_no_rows);
  initialized = true;
}

void GroupDistinctTable::InitializeBuffers(int64_t max_no_rows)  // max_no_rows = 0   =>  limit not known
{
  Transaction *m_conn = current_tx;
  if (filter_implementation) {  // special case of filter
    f = new Filter(max_no_rows, pack_power);
    f->Reset();
    rows_limit = max_no_rows + 1;  // limits do not apply
    rccontrol.lock(m_conn->GetThreadID())
        << "GroupDistinctTable initialized as Filter for up to " << max_no_rows << " positions." << system::unlock;
    return;
  }
  no_rows = max_total_size / total_width;  // memory limitation
  if (max_no_rows != 0 && (max_no_rows + 1) * 1.3 < no_rows)
    no_rows = int64_t((max_no_rows + 1) * 1.3);  // make sure that rows_limit>=max_no_groups

  // calculate vertical size (not dividable by 17)
  if (no_rows < 67)  // too less groups => high collision probability; 67 is prime.
    no_rows = 67;
  if (no_rows % 17 == 0) no_rows++;
  rows_limit = int64_t(no_rows * 0.9);  // rows_limit is used to determine whether the table is full

  t = (unsigned char *)alloc(total_width * no_rows, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
  //	t = new BlockedRowMemStorage(total_width, &mem_mngr, no_rows);
  input_buffer = (unsigned char *)(new int[total_width / 4 + 1]);  // ensure proper memory alignment
  rccontrol.lock(m_conn->GetThreadID()) << "GroupDistinctTable initialized as Hash(" << no_rows << "), " << group_bytes
                                        << "+" << value_bytes << " bytes." << system::unlock;
  Clear();
}

int64_t GroupDistinctTable::BytesTaken()  // actual size of structures
{
  DEBUG_ASSERT(initialized);
  if (filter_implementation) return f->NoObj() / 8;
  return total_width * no_rows;
}

GDTResult GroupDistinctTable::Find(int64_t group, int64_t val)  // numeric values - check if exists
{
  if (filter_implementation) {
    val = encoder->ValPutValue64(val);
    val = group + group_factor * val;
    if (f->Get(val)) return GDTResult::GDT_EXISTS;
    return GDTResult::GBIMODE_AS_TEXT;  // "Added" means "found" here.
  }
  group += 1;  // offset; 0 means empty position
  std::memmove(input_buffer, (unsigned char *)(&group), group_bytes);
  bool encoded = encoder->PutValue64(input_buffer + group_bytes, val, false);
  ASSERT(encoded, "encode failed!");
  return FindCurrentRow(true);
}

GDTResult GroupDistinctTable::Add(int64_t group, MIIterator &mit) {
  if (filter_implementation) {
    int64_t val = encoder->ValEncode(mit);
    val = group + group_factor * val;
    if (f->Get(val)) return GDTResult::GDT_EXISTS;
    f->Set(val);
    return GDTResult::GBIMODE_AS_TEXT;
  }
  group += 1;  // offset; 0 means empty position
  std::memmove(input_buffer, (unsigned char *)(&group), group_bytes);
  encoder->Encode(input_buffer + group_bytes, mit, NULL, true);
  return FindCurrentRow();
}

GDTResult GroupDistinctTable::Add(int64_t group, int64_t val)  // numeric values
{
  if (filter_implementation) {
    val = encoder->ValPutValue64(val);
    val = group + group_factor * val;
    if (f->Get(val)) return GDTResult::GDT_EXISTS;
    f->Set(val);
    return GDTResult::GBIMODE_AS_TEXT;
  }
  group += 1;  // offset; 0 means empty position
  std::memmove(input_buffer, (unsigned char *)(&group), group_bytes);
  bool encoded = encoder->PutValue64(input_buffer + group_bytes, val, false, true);
  ASSERT(encoded, "encode failed!");
  return FindCurrentRow();
}

GDTResult GroupDistinctTable::Add(unsigned char *v)  // arbitrary vector of bytes
{
  DEBUG_ASSERT(!filter_implementation);
  input_buffer[0] = 1;
  if (use_CRC) {
    std::memset(input_buffer + group_bytes, 0, HASH_FUNCTION_BYTE_SIZE);
    HashMD5(v, input_length, (input_buffer + group_bytes));
  } else
    std::memcpy(input_buffer + group_bytes, v, value_bytes);
  return FindCurrentRow();
}

GDTResult GroupDistinctTable::AddFromCache(unsigned char *input_vector)  // input vector from cache
{
  DEBUG_ASSERT(!filter_implementation);
  std::memcpy(input_buffer, input_vector, total_width);
  return FindCurrentRow();
}

int64_t GroupDistinctTable::GroupNoFromInput()  // decode group number from the
                                                // current input vector
{
  int64_t group = 0;
  std::memcpy((unsigned char *)(&group), input_buffer, group_bytes);
  group -= 1;  // offset; 0 means empty position
  return group;
}

int64_t GroupDistinctTable::ValueFromInput()  // decode original value from the
                                              // current input vector
{
  int64_t val = 0;
  bool is_null = false;
  MIDummyIterator mit(1);
  val = encoder->GetValue64(input_buffer + group_bytes, mit, is_null);
  return val;
}

void GroupDistinctTable::ValueFromInput(types::BString &v)  // decode original value from the current input vector
{
  MIDummyIterator mit(1);
  v = encoder->GetValueT(input_buffer + group_bytes, mit);
}

GDTResult GroupDistinctTable::FindCurrentRow(bool find_only)  // find / insert the current buffer
{
  DEBUG_ASSERT(!filter_implementation);
  unsigned int crc_code = HashValue(input_buffer, total_width);
  int64_t row = crc_code % no_rows;
  int64_t step = 3 + crc_code % 8;
  int64_t local_no_of_checks = 0;
  do {
    unsigned char *p = t + row * total_width;
    //		unsigned char *p = (unsigned char *)(t->GetRow(row));
    if (!RowEmpty(p)) {
      if (std::memcmp(p, input_buffer, total_width) == 0) {
        // i.e. identical row found
        return GDTResult::GDT_EXISTS;
      }
      local_no_of_checks++;
      row += step + local_no_of_checks;
      if (row >= no_rows) row = row % no_rows;
    } else {
      if (!find_only) {
        std::memcpy(p, input_buffer, total_width);
        no_of_occupied++;
      }
      return GDTResult::GBIMODE_AS_TEXT;
    }
  } while (local_no_of_checks < 8);  // search depth
  return GDTResult::GDT_FULL;
}

void GroupDistinctTable::Clear()  // clear the tables
{
  if (filter_implementation) {
    f->Reset();
    return;
  }
  std::memset(t, 0, total_width * no_rows);
  //	for(int64_t row = 0; row < no_rows; row++)
  //		std::memset(t->GetRow(row), 0, total_width);
  std::memset(input_buffer, 0, total_width);
  no_of_occupied = 0;
}
}  // namespace core
}  // namespace stonedb
