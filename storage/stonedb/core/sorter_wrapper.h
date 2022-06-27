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
#ifndef STONEDB_CORE_SORTER_WRAPPER_H_
#define STONEDB_CORE_SORTER_WRAPPER_H_
#pragma once

#include "core/column_bin_encoder.h"
#include "core/mi_iterator.h"
#include "core/multi_index.h"
#include "core/sorter3.h"
#include "vc/virtual_column.h"

namespace stonedb {
namespace core {

class SorterWrapper {
 public:
  SorterWrapper(MultiIndex &_mind, int64_t _limit);
  ~SorterWrapper();

  void AddSortedColumn(vcolumn::VirtualColumn *col, int sort_order, bool in_output);

  // MultiIndex is needed to check which dimensions we should virtualize
  void InitSorter(MultiIndex &_mind, bool implicit_logic);

  // return true if the packrow may be skipped because of limit/statistics
  bool InitPackrow(MIIterator &mit);

  bool PutValues(MIIterator &mit);    // set values for all columns based on
                                      // multiindex position
  bool PutValues(SorterWrapper &sw);  // copy values from other tables to input_buf
  // NOTE: data packs must be properly locked by InitPackrow!
  // Return false if no more values are needed (i.e. limit already reached)

  // Must be executed before the first GetValue.
  // Return true if OK, false if there is no more data
  bool FetchNextRow();

  int64_t GetValue64(int col, bool &is_null) { return scol[col].GetValue64(cur_val, cur_mit, is_null); }
  types::BString GetValueT(int col) { return scol[col].GetValueT(cur_val, cur_mit); }
  void SortRoughly(std::vector<PackOrderer> &po);
  Sorter3 *GetSorter() { return s; }

 private:
  int64_t GetEncodedValNum() { return no_values_encoded; }
  Sorter3 *s;
  MultiindexPositionEncoder *mi_encoder;  // if null, then no multiindex position encoding is needed
  int64_t limit;
  int64_t no_of_rows;         // total number of rows to be sorted (upper limit)
  int64_t no_values_encoded;  // values already put to the sorter
  int rough_sort_by;          // the first nontrivial sort column
  int buf_size;               // total number of bytes in sorted rows
  unsigned char *cur_val;     // a pointer to the current (last fetched) output row
  unsigned char *input_buf;   // a buffer of buf_size to prepare rows
  MIDummyIterator cur_mit;    // a position of multiindex for implicit (virtual) columns

  std::vector<ColumnBinEncoder> scol;  // encoders for sorted columns

  // this is a temporary input description of sorting columns
  class SorterColumnDescription {
   public:
    SorterColumnDescription(vcolumn::VirtualColumn *_col, int _sort_order, bool _in_output)
        : col(_col), sort_order(_sort_order), in_output(_in_output) {}
    vcolumn::VirtualColumn *col;
    int sort_order;
    bool in_output;
  };
  void InitOrderByVector(std::vector<int> &order_by);  // a method refactored from InitSorter

  std::vector<SorterColumnDescription> input_cols;  // encoders for sorted columns
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_SORTER_WRAPPER_H_
