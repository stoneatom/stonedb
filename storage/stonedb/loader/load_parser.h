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
#ifndef STONEDB_LOADER_LOAD_PARSER_H_
#define STONEDB_LOADER_LOAD_PARSER_H_
#pragma once

#include <vector>

#include "loader/parsing_strategy.h"
#include "loader/read_buffer.h"
#include "loader/rejecter.h"
#include "loader/value_cache.h"
#include "mm/traceable_object.h"

namespace stonedb {

namespace core {
class RCAttr;
}
namespace index {
class RCTableIndex;
}  // namespace index

namespace system {
class IOParameters;
}  // namespace system

namespace loader {
class LoadParser final {
 public:
  using RCAttrPtrVect_t = std::vector<std::unique_ptr<core::RCAttr>>;

  LoadParser(RCAttrPtrVect_t &attrs, const system::IOParameters &iop, uint packsize,
             std::unique_ptr<system::Stream> &f);
  ~LoadParser() = default;

  uint GetPackrow(uint no_of_rows, std::vector<ValueCache> &vcs);
  int64_t GetNoRejectedRows() const { return rejecter.GetNoRejectedRows(); }
  bool ThresholdExceeded(int64_t no_rows) const { return rejecter.ThresholdExceeded(no_rows); }
  int ProcessInsertIndex(std::shared_ptr<index::RCTableIndex> tab, std::vector<ValueCache> &vcs, uint no_rows);
  int64_t GetNoRow() const { return row_no; }
  int64_t GetDuprow() const { return dup_no; }

 private:
  RCAttrPtrVect_t &attrs;

  std::vector<int64_t> last_pack_size;
  int64_t start_time;

  ReadBuffer read_buffer;

  std::shared_ptr<ParsingStrategy> strategy;
  std::shared_ptr<index::RCTableIndex> tab_index;

  const char *cur_ptr;
  const char *buf_end;
  const system::IOParameters &ioparam;
  uint pack_size;
  Rejecter rejecter;
  uint cur_row = 0;
  int64_t num_of_obj = 0;
  int64_t row_no = 0;
  int64_t dup_no = 0;

  bool MakeRow(std::vector<ValueCache> &value_buffers);
  bool MakeValue(uint col, ValueCache &buffer);
  int binlog_loaded_block(const char *buf_start, const char *buf_end);
};
}  // namespace loader

}  // namespace stonedb

#endif  // STONEDB_LOADER_LOAD_PARSER_H_
