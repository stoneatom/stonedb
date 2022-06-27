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
#ifndef STONEDB_CORE_JOINER_HASH_TABLE_H_
#define STONEDB_CORE_JOINER_HASH_TABLE_H_
#pragma once

#include "core/bin_tools.h"
#include "core/column_bin_encoder.h"
#include "core/filter.h"
#include "core/rc_attr_typeinfo.h"
#include "mm/traceable_object.h"
#include "vc/virtual_column.h"

namespace stonedb {
namespace core {
/*
        JoinerHashTable - a tool for storing values and counters, implementing
   hash table mechanism

        Usage:
*/
class HashJoinNotImplementedHere : public common::Exception {
 public:
  HashJoinNotImplementedHere() : common::Exception(std::string()) {}
};

class JoinerHashTable : public mm::TraceableObject {
 public:
  JoinerHashTable();
  ~JoinerHashTable();

  // Hash table construction

  bool AddKeyColumn(vcolumn::VirtualColumn *vc,
                    vcolumn::VirtualColumn *vc_matching);  // Lookups should not be
                                                           // numerical in case of join,
                                                           // as the code values may be
                                                           // incompatible.
  // Return false if not compatible.
  void AddTupleColumn(int b_size = 8);  // just a tuple number for some
                                        // dimension (interpreted externally)
  // in future: AddOutputColumn(information for materialized data)?

  void Initialize(int64_t max_table_size, bool easy_roughable);
  // initialize buffers basing on the previously defined columns
  // and the best possible upper approximation of number of tuples

  // Hash table usage

  // put values to a temporary buffer (note that it will contain the previous
  // values, which may be reused
  void PutKeyValue(int col, MIIterator &mit) {  // for all values EXCEPT NULLS
    encoder[col].Encode(input_buffer.get(), mit, NULL,
                        true);  // true: update statistics
  }
  void PutMatchedValue(int col, vcolumn::VirtualColumn *vc,
                       MIIterator &mit) {  // for all values EXCEPT NULLS
    encoder[col].Encode(input_buffer.get(), mit, vc);
  }
  void PutMatchedValue(int col, int64_t v) {  // for constants
    bool encoded = encoder[col].PutValue64(input_buffer.get(), v, true,
                                           false);  // true: the second encoded column false: don't update stats
    ASSERT(encoded, "failed to put value");
  }
  int64_t FindAndAddCurrentRow();  // a position in the current JoinerHashTable,
                                   // common::NULL_VALUE_64 if no place left
  bool ImpossibleValues(int col, int64_t local_min, int64_t local_max);
  bool ImpossibleValues(int col, types::BString &local_min, types::BString &local_max);
  // return true only if for sure no value between local_min and local_max was
  // put

  bool StringEncoder(int col);  // return true if the values are encoded as
                                // strings (e.g. lookup is not a string)
  bool IsColumnSizeValid(int col, int size);
  bool TooManyConflicts() { return too_many_conflicts; }
  int64_t InitCurrentRowToGet();  // find the current key buffer in the JoinerHashTable,
                                  // initialize internal iterators
  // return value: a number of rows found (a multiplier)
  int64_t GetNextRow();  // the next row, with the value equal to the buffer,
  // return value == common::NULL_VALUE_64 when no more rows found
  //
  // Usage, to retrieve matching tuples:
  // PutKeyValue(.....)  <- all keys we are searching for
  // InitCurrentRowToGet();
  // do... r = GetNextRow(); ... while( r != common::NULL_VALUE_64 );

  // columns have common numbering, both key and tuple ones
  void PutTupleValue(int col, int64_t row,
                     int64_t v) {  // common::NULL_VALUE_64 stored as 0
    DEBUG_ASSERT(col >= no_key_attr);
    if (size[col] == 4) {
      if (v == common::NULL_VALUE_64)
        *((int *)(t + row * total_width + column_offset[col])) = 0;
      else
        *((int *)(t + row * total_width + column_offset[col])) = int(v + 1);
    } else {
      if (v == common::NULL_VALUE_64)
        *((int64_t *)(t + row * total_width + column_offset[col])) = 0;
      else
        *((int64_t *)(t + row * total_width + column_offset[col])) = v + 1;
    }
  }

  // Hash table output and info

  int NoAttr() { return no_attr; }         // total number of columns
  int NoKeyAttr() { return no_key_attr; }  // number of grouping columns
  int64_t NoRows() { return no_rows; }     // buffer size (max. number of rows)
  int64_t GetTupleValue(int col,
                        int64_t row) {  // columns have common numbering
    DEBUG_ASSERT(col >= no_key_attr);
    if (size[col] == 4) {
      int v = *((int *)(t + row * total_width + column_offset[col]));
      if (v == 0) return common::NULL_VALUE_64;
      return v - 1;
    } else {
      int64_t v = *((int64_t *)(t + row * total_width + column_offset[col]));
      if (v == 0) return common::NULL_VALUE_64;
      return v - 1;
    }
  }

  void ClearAll();  // initialize all

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }

 private:
  int no_of_occupied;

  // content and additional buffers

  // Input buffer format:		<key_1><key_2>...<key_n>
  // Table format:
  // <key_1><key_2>...<key_n><tuple_1>...<tuple_m><multiplier>
  std::unique_ptr<unsigned char[]> input_buffer;

  // a table of offsets of column beginnings wrt. the beginning of a row
  std::unique_ptr<int[]> column_offset;

  unsigned char *t;  // common table for key values and all other

  int mult_offset;          // multiplier offset (one of the above)
  int key_buf_width;        // in bytes
  int total_width;          // in bytes
  bool too_many_conflicts;  // true if there is more conflicts than a threshold
  int64_t no_rows;          // actual buffer size (vertical)
  int64_t rows_limit;       // a capacity of one memory buffer

  std::vector<int> size;  // byte size of the column (used for tuple columns)
  std::vector<ColumnBinEncoder> encoder;

  // column/operation descriptions

  bool initialized;

  int no_attr;
  int no_key_attr;

  bool for_count_only;  // true if there is no other columns than key and
                        // multiplier

  int64_t current_row;     // row value to be returned by the next GetNextRow()
                           // function
  int64_t to_be_returned;  // the number of rows left for GetNextRow()
  int64_t current_iterate_step;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_JOINER_HASH_TABLE_H_