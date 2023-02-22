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
#ifndef TIANMU_LOADER_PARSING_STRATEGY_H_
#define TIANMU_LOADER_PARSING_STRATEGY_H_
#pragma once

#include "common/data_format.h"
#include "core/tianmu_attr_typeinfo.h"
#include "loader/value_cache.h"

namespace Tianmu {
namespace loader {

class ParsingStrategy final {
 public:
  using kmp_next_t = std::vector<int>;
  enum class ParseResult { OK, EOB, ERROR };

 public:
  ParsingStrategy(const system::IOParameters &iop, std::vector<uchar> columns_collations);
  ~ParsingStrategy() {}
  ParseResult GetOneRow(const char *const buf, size_t size, std::vector<ValueCache> &values, uint &rowsize,
                        int &errorinfo, bool eof = false);
  void ReadField(const char *&ptr, const char *&val_beg, Item *&item, uint &index_of_field,
                 std::vector<std::pair<const char *, size_t>> &vec_ptr_field, uint &field_index_in_field_list,
                 const CHARSET_INFO *char_info, bool completed_row = true);
  void SetTHD(THD *thd) { thd_ = thd; }
  THD *GetTHD() const { return thd_; };

 protected:
  core::AttributeTypeInfo &GetATI(ushort col) { return attr_infos_[col]; }

 private:
  std::vector<core::AttributeTypeInfo> attr_infos_;
  THD *thd_{nullptr};
  TABLE *table_{nullptr};
  bool prepared_;

  std::string terminator_;
  std::string delimiter_;  // cloumn separator
  uchar string_qualifier_;
  char escape_char_;  // row separator
  std::string enclose_delimiter_;
  std::string enclose_terminator_;
  std::string tablename_;
  std::string dbname_;

  kmp_next_t kmp_next_delimiter_;
  kmp_next_t kmp_next_enclose_delimiter_;
  kmp_next_t kmp_next_terminator_;
  kmp_next_t kmp_next_enclose_terminator_;

  CHARSET_INFO *charset_info_;
  std::vector<char> temp_buf_;

  bool first_row_prepared_{false};
  std::vector<String *> vec_field_Str_list_;  // table column index<---> String*, string will be delete by THD
  std::vector<uint> vec_field_num_to_index_;  // calculate the order number of the assignment fields and set fields
  std::map<std::string, uint> map_field_name_to_index_;  // field name to the table column index;

  void GuessUnescapedEOL(const char *ptr, const char *buf_end);
  void GuessUnescapedEOLWithEnclose(const char *ptr, const char *const buf_end);

  bool SearchUnescapedPattern(const char *&ptr, const char *const buf_end, const std::string &pattern,
                              const std::vector<int> &kmp_next);
  enum class SearchResult { PATTERN_FOUND, END_OF_BUFFER, END_OF_LINE };
  SearchResult SearchUnescapedPatternNoEOL(const char *&ptr, const char *const buf_end, const std::string &pattern,
                                           const std::string &line_termination, const std::vector<int> &kmp_next);

  void GetEOL(const char *const buf, const char *const buf_end);
  void GetValue(const char *const value_ptr, size_t value_size, ushort col, ValueCache &value);
};

}  // namespace loader
}  // namespace Tianmu

#endif  // TIANMU_LOADER_PARSING_STRATEGY_H_
