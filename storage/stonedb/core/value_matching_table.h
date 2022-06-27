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
#ifndef STONEDB_CORE_VALUE_MATCHING_TABLE_H_
#define STONEDB_CORE_VALUE_MATCHING_TABLE_H_
#pragma once

#include "core/bin_tools.h"
#include "core/filter.h"
#include "mm/traceable_object.h"

namespace stonedb {
namespace core {
// A structure to store large data rows consisting of <matching_data> and
// <store_data>, divided into "columns" defined as byte offsets. Functionality:
// - checking if an input row containing <matching_data> match any of existing
// rows (return row number),
// - direct access to a row identified by a row number,
// - direct access to a row element identified by a row and column number,
// - adding new <matching_data> element if not found (<store_data> is default in
// this case),
// - iterative access to all filled positions of table.

class ValueMatchingTable {  // abstract class: interface for value matching
                            // (Group By etc.)
 public:
  ValueMatchingTable();
  ValueMatchingTable(ValueMatchingTable &sec);
  virtual ~ValueMatchingTable();
  virtual ValueMatchingTable *Clone() = 0;

  virtual void Clear();

  virtual unsigned char *GetGroupingRow(int64_t row) = 0;
  virtual unsigned char *GetAggregationRow(int64_t row) = 0;

  // Set a position (row number) in the current table,
  // row==common::NULL_VALUE_64 if not found and not added return value: true if
  // already exists, false if put as a new row
  virtual bool FindCurrentRow(unsigned char *input_buffer, int64_t &row, bool add_if_new = true) = 0;

  int64_t NoRows() { return int64_t(no_rows); }  // rows stored so far
  virtual bool IsOnePass() = 0;                  // true if the aggregator is capable of storing all groups
                                                 // up to max_no_groups declared in Init()
  virtual bool NoMoreSpace() = 0;                // hard stop: any new value will be rejected
                                                 // / we already found all groups
  virtual int MemoryBlocksLeft() = 0;            // soft stop: if 0, finish the current pack and start
                                                 // rejecting new values, default: 999
  virtual int64_t ByteSize() = 0;                // total size of the structure (for reporting
                                                 // / parallel threads number evaluation)
  virtual int64_t RowNumberScope() = 0;          // max_nr + 1, where max_nr is the largest row number
                                                 // returned by FindCurrentRow, GetCurrentRow etc.

  // Iterator
  virtual void Rewind(bool release = false) = 0;  // rewind a row iterator, release=true - free
                                                  // memory occupied by traversed rows
  virtual int64_t GetCurrentRow() = 0;            // return a number of current row, or
                                                  // common::NULL_VALUE_64 if there is no more
  virtual void NextRow() = 0;                     // iterate to a next row
  virtual bool RowValid() = 0;                    // false if there is no more rows to iterate
  virtual bool SetCurrentRow([[maybe_unused]] int64_t row) { return true; }
  virtual bool SetEndRow([[maybe_unused]] int64_t row) { return true; }
  /*
          Selection algorithm and initialization of the created object. Input
     values:
          - max. memory to take,
          - upper approx. of a number of groups, maximal group number (code),
          - buffer sizes: total, input (grouping attrs plus grouping UTF),
     matching (grouping attrs only)
  */
  static ValueMatchingTable *CreateNew_ValueMatchingTable(int64_t mem_available, int64_t max_no_groups,
                                                          int64_t max_group_code, int _total_width,
                                                          int _input_buf_width, int _match_width, uint32_t power);

 protected:
  int total_width;         // whole row
  int matching_width;      // a part of one row used to compare matching values
  int input_buffer_width;  // a buffer for matching values plus additional info
                           // (e.g. UTF buffer)

  unsigned int no_rows;  // rows stored so far
};

// Trivial version: just one position

class ValueMatching_OnePosition : public ValueMatchingTable {
 public:
  ValueMatching_OnePosition();
  ValueMatching_OnePosition(ValueMatching_OnePosition &sec);
  virtual ~ValueMatching_OnePosition();

  virtual void Init(int _total_width);  // assumption: match_width =
                                        // input_buf_width = 0, max_group = 1

  ValueMatchingTable *Clone() override { return new ValueMatching_OnePosition(*this); }
  void Clear() override;

  unsigned char *GetGroupingRow([[maybe_unused]] int64_t row) override { return NULL; }
  unsigned char *GetAggregationRow([[maybe_unused]] int64_t row) override { return t_aggr; }
  bool FindCurrentRow(unsigned char *input_buffer, int64_t &row, bool add_if_new = true) override;

  void Rewind([[maybe_unused]] bool release = false) override { iterator_valid = (no_rows > 0); }
  int64_t GetCurrentRow() override { return 0; }
  void NextRow() override { iterator_valid = false; }
  bool RowValid() override { return iterator_valid; }
  bool NoMoreSpace() override { return no_rows > 0; }
  int MemoryBlocksLeft() override { return 999; }  // one-pass only
  bool IsOnePass() override { return true; }
  int64_t ByteSize() override { return total_width; }
  int64_t RowNumberScope() override { return 1; }

 protected:
  unsigned char *t_aggr;
  bool iterator_valid;
};

// Easy case: a group number indicates its position in lookup table

class ValueMatching_LookupTable : public mm::TraceableObject, public ValueMatchingTable {
 public:
  ValueMatching_LookupTable();
  ValueMatching_LookupTable(ValueMatching_LookupTable &sec);
  virtual ~ValueMatching_LookupTable();

  virtual void Init(int64_t max_group_code, int _total_width, int _input_buf_width, int _match_width, uint32_t power);

  ValueMatchingTable *Clone() override { return new ValueMatching_LookupTable(*this); }
  void Clear() override;

  // TO BE OPTIMIZED: actually we don't need any grouping buffer if there's no
  // UTF part
  unsigned char *GetGroupingRow(int64_t row) override {
    DEBUG_ASSERT(row < max_no_rows);
    return t + row * total_width;
  }
  unsigned char *GetAggregationRow(int64_t row) override {
    DEBUG_ASSERT(row < max_no_rows);
    return t_aggr + row * total_width;
  }

  bool FindCurrentRow(unsigned char *input_buffer, int64_t &row, bool add_if_new = true) override;

  void Rewind([[maybe_unused]] bool release = false) override { occupied_iterator = 0; }
  int64_t GetCurrentRow() override { return occupied_table[occupied_iterator]; }
  void NextRow() override { occupied_iterator++; }
  bool RowValid() override { return (occupied_iterator < no_rows); }
  bool SetCurrentRow(int64_t row) override;
  bool SetEndRow(int64_t row) override;
  bool NoMoreSpace() override { return no_rows >= max_no_rows; }
  int MemoryBlocksLeft() override { return 999; }  // one-pass only
  bool IsOnePass() override { return true; }
  int64_t ByteSize() override { return (total_width + sizeof(int)) * max_no_rows; }
  int64_t RowNumberScope() override { return max_no_rows; }
  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }

 protected:
  unsigned char *t;       // common buffer for grouping values and results of aggregations
  unsigned char *t_aggr;  // = t + input_buffer_width, just for speed

  Filter *occupied;
  int *occupied_table;
  int64_t max_no_rows;
  int64_t occupied_iterator;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_VALUE_MATCHING_TABLE_H_
