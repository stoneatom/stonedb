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
#ifndef TIANMU_LOADER_LOAD_PARSER_H_
#define TIANMU_LOADER_LOAD_PARSER_H_
#pragma once

#include <vector>

#include "loader/parsing_strategy.h"
#include "loader/read_buffer.h"
#include "loader/rejecter.h"
#include "loader/value_cache.h"
#include "mm/traceable_object.h"

namespace Tianmu {

namespace core {
class TianmuAttr;
}
namespace index {
class TianmuTableIndex;
}  // namespace index

namespace system {
class IOParameters;
}  // namespace system

namespace loader {
class LoadParser final {
 public:
  using TianmuAttrPtrVect_t = std::vector<std::unique_ptr<core::TianmuAttr>>;

  LoadParser(TianmuAttrPtrVect_t &attrs, const system::IOParameters &iop, uint packsize,
             std::unique_ptr<system::Stream> &f);
  ~LoadParser() = default;

  uint GetPackrow(uint no_of_rows, std::vector<ValueCache> &vcs);
  int64_t GetNumOfRejectedRows() const { return rejecter_.GetNumOfRejectedRows(); }
  bool ThresholdExceeded(int64_t no_rows) const { return rejecter_.ThresholdExceeded(no_rows); }
  int ProcessInsertIndex(std::shared_ptr<index::TianmuTableIndex> tab, std::vector<ValueCache> &vcs, uint no_rows);
  int64_t GetNoRow() const { return num_of_row_; }
  int64_t GetDupRow() const { return num_of_dup_; }
  int64_t GetIgnoreRow() const { return num_of_skip_; }

 private:
  TianmuAttrPtrVect_t &attrs_;

  std::vector<int64_t> last_pack_size_;
  int64_t start_time_ = 0;

  ReadBuffer read_buffer_;

  std::shared_ptr<ParsingStrategy> strategy_;
  std::shared_ptr<index::TianmuTableIndex> tab_index_;

  const char *cur_ptr_;
  const char *buf_end_;
  const system::IOParameters &io_param_;
  uint pack_size_ = 0;
  Rejecter rejecter_;
  uint cur_row_ = 0;
  int64_t num_of_obj_ = 0;
  int64_t num_of_row_ = 0;
  int64_t num_of_dup_ = 0;
  int64_t num_of_skip_ = 0;

  bool MakeRow(std::vector<ValueCache> &value_buffers);
  bool MakeValue(uint col, ValueCache &buffer);
  int binlog_loaded_block(const char *buf_start, const char *buf_end);
};
}  // namespace loader

}  // namespace Tianmu

#endif  // TIANMU_LOADER_LOAD_PARSER_H_
