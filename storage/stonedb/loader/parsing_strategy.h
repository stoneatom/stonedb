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
#ifndef STONEDB_LOADER_PARSING_STRATEGY_H_
#define STONEDB_LOADER_PARSING_STRATEGY_H_
#pragma once

#include "common/data_format.h"
#include "core/rc_attr_typeinfo.h"
#include "loader/value_cache.h"

namespace stonedb {
namespace loader {

class ParsingStrategy final {
 public:
  using kmp_next_t = std::vector<int>;
  enum class ParseResult { OK, EOB, ERROR };

 public:
  ParsingStrategy(const system::IOParameters &iop, std::vector<uchar> columns_collations);
  ~ParsingStrategy() {}
  ParseResult GetOneRow(const char *const buf, size_t size, std::vector<ValueCache> &values, uint &rowsize,
                        int &errorinfo);

 protected:
  core::AttributeTypeInfo &GetATI(ushort col) { return atis[col]; }

 private:
  std::vector<core::AttributeTypeInfo> atis;
  bool prepared;

  std::string eol;
  std::string delimiter;  // cloumn separator
  uchar string_qualifier;
  char escape_char;  // row separator
  std::string enclose_delimiter;
  std::string enclose_eol;
  std::string tablename;
  std::string dbname;

  kmp_next_t kmp_next_delimiter;
  kmp_next_t kmp_next_enclose_delimiter;
  kmp_next_t kmp_next_eol;
  kmp_next_t kmp_next_enclose_eol;

  CHARSET_INFO *cs_info;
  std::vector<char> temp_buf;

  void GuessUnescapedEOL(const char *ptr, const char *buf_end);
  void GuessUnescapedEOLWithEnclose(const char *ptr, const char *const buf_end);

  bool SearchUnescapedPattern(const char *&ptr, const char *const buf_end, const std::string &pattern,
                              const std::vector<int> &kmp_next);
  enum class SearchResult { PATTERN_FOUND, END_OF_BUFFER, END_OF_LINE };
  SearchResult SearchUnescapedPatternNoEOL(const char *&ptr, const char *const buf_end, const std::string &pattern,
                                           const std::vector<int> &kmp_next);

  void GetEOL(const char *const buf, const char *const buf_end);
  void GetValue(const char *const value_ptr, size_t value_size, ushort col, ValueCache &value);
};

}  // namespace loader
}  // namespace stonedb

#endif  // STONEDB_LOADER_PARSING_STRATEGY_H_
