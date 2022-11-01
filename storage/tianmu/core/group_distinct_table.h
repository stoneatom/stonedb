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
#ifndef TIANMU_CORE_GROUP_DISTINCT_TABLE_H_
#define TIANMU_CORE_GROUP_DISTINCT_TABLE_H_
#pragma once

#include "core/bin_tools.h"
#include "core/blocked_mem_table.h"
#include "core/column_bin_encoder.h"
#include "core/filter.h"
#include "core/value_matching_table.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
enum class GDTResult {
  GBIMODE_AS_TEXT,  // value successfully added to a table as a new one
  GDT_EXISTS,       // value already in table
  GDT_FULL          // value not found, but cannot add (table full)
};

static const int64_t zero_const = 0;

// Note: this class is less flexible but more packed comparing with GroupTable.
//      Memory usage efficiency and searching speed are key factors.
//
// Functionality: to allow fast adding value vectors (number,number) or
// (number,BString)
//		with checking repetitions. To be used in DISTINCT inside GROUP
// BY. 		Limited memory is assumed (specified in the constructor).
//
class GroupDistinctTable : public mm::TraceableObject {
 public:
  GroupDistinctTable(uint32_t power);  // maximal size of structures; 0 - default
  ~GroupDistinctTable();
  int64_t BytesTaken();  // actual size of structures
  void InitializeB(int max_len,
                   int64_t max_no_rows);  // char* initialization (for arbitrary
                                          // vector of bytes)
  void InitializeVC(int64_t max_no_groups, vcolumn::VirtualColumn *vc, int64_t max_no_rows, int64_t max_bytes,
                    bool decodable);
  void CopyFromValueMatchingTable(ValueMatchingTable *vt) { vm_tab.reset(vt->Clone()); };

  // Assumption: group >= 0
  GDTResult Add(int64_t group,
                MIIterator &mit);                       // a value from the virtual column used to initialize
  GDTResult Add(int64_t group, int64_t val);            // numeric values
  GDTResult Add(unsigned char *v);                      // arbitrary vector of bytes
  GDTResult AddFromCache(unsigned char *input_vector);  // input vector from cache
  GDTResult Find(int64_t group,
                 int64_t val);  // check whether the value is already found
  bool AlreadyFull() { return no_of_occupied >= rows_limit; }
  void Clear();  // clear the table

  int InputBufferSize() { return total_width; }
  unsigned char *InputBuffer() { return input_buffer; }  // for caching purposes
  int64_t GroupNoFromInput();                            // decode group number from the current input vector
  int64_t ValueFromInput();                              // decode original value from the current input vector
  void ValueFromInput(types::BString &);                 // decode original value from
                                                         // the current input vector

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }

 private:
  GDTResult FindCurrentRow(bool find_only = false);  // find / insert the current buffer
  GDTResult FindCurrentRowByVMTable();
  bool RowEmpty(unsigned char *p)  // is this position empty? only if it starts with zeros
  {
    return (std::memcmp(&zero_const, p, group_bytes) == 0);
  }
  void InitializeBuffers(int64_t max_no_rows);
  // create buffers for sizes determined and stored in class fields
  // max_no_rows = 0   =>  limit not known

  // Internal representation: <group><value> (no. of bytes given by variables
  // below) "group" is stored with offset 1, because value 0 means "position
  // empty"
  unsigned char *input_buffer;  // buffer for (encoded) input values
  unsigned char *t;             // buffer for all values
  uint32_t pack_power;
  //	MemBlockManager		mem_mngr;
  //	BlockedRowMemStorage*	t;

  int group_bytes;         // byte width for groups, up to 8
  int value_bytes;         // byte width for values ?? JB: size of elements in hash
                           // table
  int input_length;        // byte width for input values;
  int total_width;         // byte width for all the vector
  int64_t max_total_size;  // max. size of hash table in memory
  int64_t no_rows;         // number of rows in table

  int64_t rows_limit;      // a number of rows which may be safely occupied
  int64_t no_of_occupied;  // a number of rows already occupied in buffer

  bool initialized;
  bool use_CRC;

  // Encoder
  ColumnBinEncoder *encoder;

  // Filter implementation
  bool filter_implementation;
  Filter *f;
  int64_t group_factor;  // (g, v)  ->  f( g + group_factor * v )
  std::unique_ptr<ValueMatchingTable> vm_tab;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_GROUP_DISTINCT_TABLE_H_
