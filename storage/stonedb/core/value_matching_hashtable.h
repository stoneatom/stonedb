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
#ifndef STONEDB_CORE_VALUE_MATCHING_HASHTABLE_H_
#define STONEDB_CORE_VALUE_MATCHING_HASHTABLE_H_
#pragma once

#include "core/blocked_mem_table.h"
#include "core/value_matching_table.h"

namespace stonedb {
namespace core {

// Expandable hash table

class ValueMatching_HashTable : public mm::TraceableObject, public ValueMatchingTable {
 public:
  ValueMatching_HashTable();
  ValueMatching_HashTable(ValueMatching_HashTable &sec);

  virtual ~ValueMatching_HashTable() = default;

  virtual void Init(int64_t mem_available, int64_t max_no_groups, int _total_width, int _input_buf_width,
                    int _match_width);

  ValueMatchingTable *Clone() override { return new ValueMatching_HashTable(*this); }
  void Clear() override;

  unsigned char *GetGroupingRow(int64_t row) override { return (unsigned char *)t.GetRow(row); }
  unsigned char *GetAggregationRow(int64_t row) override { return (unsigned char *)t.GetRow(row) + input_buffer_width; }

  bool FindCurrentRow(unsigned char *input_buffer, int64_t &row, bool add_if_new = true) override;

  void Rewind(bool release = false) override { t.Rewind(release); }
  int64_t GetCurrentRow() override { return t.GetCurrent(); }
  void NextRow() override { t.NextRow(); }
  bool RowValid() override { return (t.GetCurrent() != -1); }
  bool SetCurrentRow(int64_t row) override { return t.SetCurrentRow(row); }
  bool SetEndRow(int64_t row) override { return t.SetEndRow(row); }
  bool NoMoreSpace() override { return int64_t(no_rows) >= max_no_rows; }
  int MemoryBlocksLeft() override { return (one_pass ? 999 : bmanager->MemoryBlocksLeft()); }
  bool IsOnePass() override { return one_pass; }
  int64_t ByteSize() override;
  int64_t RowNumberScope() override { return max_no_rows; }
  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }

 protected:
  // Definition of the internal structures:
  // ht - constant size hash table of integers: 0xFFFFFFFF is empty position,
  // other - an index of value stored in t t - a container (expandable storage)
  // of numbered rows:
  //     row = <group_val><group_UTF><aggr_buffer><offset_integer>
  //     where <group_val><group_UTF><aggr_buffer> is a buffer to be used by
  //     external classes,
  //           <offset_integer> is a position of the next row with the same hash
  //           value, or 0 if there is no such row

  std::vector<uint32_t> ht;  // table of offsets of hashed values, or 0xFFFFFFFF if position empty
  unsigned int ht_mask;      // (crc_code & ht_mask) is a position of code in ht table;
                             // ht_mask + 1 is the total size (in integers) of ht

  int64_t max_no_rows;
  bool one_pass;        // true if we guarantee one-pass processing
  int next_pos_offset;  // position of the "next position" indicator in stored
                        // row

  BlockedRowMemStorage t;  // row storage itself
  std::shared_ptr<MemBlockManager> bmanager;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_VALUE_MATCHING_HASHTABLE_H_