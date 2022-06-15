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

#include "parsing_strategy.h"

#include <limits>
#include <vector>

#include "core/tools.h"
#include "core/transaction.h"
#include "loader/value_cache.h"
#include "system/io_parameters.h"
#include "system/rc_system.h"
#include "types/value_parser4txt.h"

namespace stonedb {
namespace loader {

static inline void PrepareKMP(ParsingStrategy::kmp_next_t &kmp_next, std::string const &pattern) {
  kmp_next.resize(pattern.length() + 1);
  int b = kmp_next[0] = -1;
  for (int i = 1, lenpat(static_cast<int>(pattern.length())); i <= lenpat; ++i) {
    while ((b > -1) && (pattern[b] != pattern[i - 1])) b = kmp_next[b];
    ++b;
    kmp_next[i] = (pattern[i] == pattern[b]) ? kmp_next[b] : b;
  }
}

ParsingStrategy::ParsingStrategy(const system::IOParameters &iop, std::vector<uchar> columns_collations)
    : atis(iop.ATIs()),
      prepared(false),
      eol(iop.LineTerminator()),
      delimiter(iop.Delimiter()),
      string_qualifier(iop.StringQualifier()),
      escape_char(iop.EscapeCharacter()),
      temp_buf(65536) {
  cs_info = get_charset(iop.CharsetInfoNumber(), 0);
  for (ushort i = 0; i < atis.size(); ++i) {
    if (core::ATI::IsStringType(GetATI(i).Type())) {
      GetATI(i).SetCollation(get_charset(columns_collations[i], 0));
    }
  }

  PrepareKMP(kmp_next_delimiter, delimiter);
  if (string_qualifier) {
    enclose_delimiter = std::string((char *)&string_qualifier, 1) + delimiter;
    PrepareKMP(kmp_next_enclose_delimiter, enclose_delimiter);
  }

  std::string tmpName = iop.GetTableName();
  if (tmpName.find("/") != std::string::npos) {
    tablename = tmpName.substr(tmpName.find("/") + 1);
    dbname = tmpName.substr(0, tmpName.find("/"));
  }
}

inline void ParsingStrategy::GuessUnescapedEOL(const char *ptr, const char *const buf_end) {
  for (; ptr < buf_end; ptr++) {
    if (escape_char && *ptr == escape_char) {
      if (ptr + 2 < buf_end && ptr[1] == '\r' && ptr[2] == '\n')
        ptr += 3;
      else
        ptr += 2;
    } else if (*ptr == '\n') {
      eol = "\n";
      break;
    } else if (*ptr == '\r' && ptr + 1 < buf_end && ptr[1] == '\n') {
      eol = "\r\n";
      break;
    }
  }
}

inline void ParsingStrategy::GuessUnescapedEOLWithEnclose(const char *ptr, const char *const buf_end) {
  for (; ptr < buf_end; ptr++) {
    if (escape_char && *ptr == escape_char)
      ptr += 2;
    else if (string_qualifier && *ptr == string_qualifier) {
      if (ptr + 1 < buf_end && ptr[1] == '\n') {
        eol = "\n";
        break;
      } else if (ptr + 2 < buf_end && ptr[1] == '\r' && ptr[2] == '\n') {
        eol = "\r\n";
        break;
      }
    }
  }
}

inline bool ParsingStrategy::SearchUnescapedPattern(const char *&ptr, const char *const buf_end,
                                                    const std::string &pattern, const std::vector<int> &kmp_next) {
  const char *c_pattern = pattern.c_str();
  size_t size = pattern.size();
  const char *search_end = buf_end;
  if (size == 1) {
    while (ptr < search_end && *ptr != *c_pattern) {
      if (escape_char && *ptr == escape_char)
        ptr += 2;
      else
        ++ptr;
    }
  } else if (size == 2) {
    --search_end;
    while (ptr < search_end && (*ptr != *c_pattern || ptr[1] != c_pattern[1])) {
      if (escape_char && *ptr == escape_char)
        ptr += 2;
      else
        ++ptr;
    }
  } else {
    int b = 0;
    for (; ptr < buf_end; ++ptr) {
      if (escape_char && *ptr == escape_char) {
        b = 0;
        ++ptr;
      } else if (c_pattern[b] != *ptr) {
        if (b != 0) {
          b = kmp_next[b];
          while ((b > -1) && (c_pattern[b] != *ptr)) b = kmp_next[b];
          ++b;
        }
      } else if (size_t(++b) == size) {
        ptr -= size - 1;
        break;
      }
    }
  }
  /*  else if(size == 3) {
          search_end -= 2;
          while(ptr < search_end && (*ptr != c_pattern[0] || *(ptr+1) !=
     c_pattern[1] || *(ptr+2)
     != c_pattern[2])) {
              if(*ptr == escape_char)
                  ptr += 2;
              else
                  ++ptr;
          }
      }
      else
          ASSERT(0, (std::string("Unexpected pattern: '") + delimiter +
     "'").c_str());*/

  return (ptr < search_end);
}

inline bool TailsMatch(const char *s1, const char *s2, const size_t size) {
  uint i = 1;
  while (i < size && s1[i] == s2[i]) i++;
  return (i == size);
}

inline ParsingStrategy::SearchResult ParsingStrategy::SearchUnescapedPatternNoEOL(const char *&ptr,
                                                                                  const char *const buf_end,
                                                                                  const std::string &pattern,
                                                                                  const std::vector<int> &kmp_next) {
  const char *c_pattern = pattern.c_str();
  size_t size = pattern.size();
  const char *c_eol = eol.c_str();
  size_t crlf = eol.size();
  const char *search_end = buf_end;

  if (size == 1) {
    while (ptr < search_end && *ptr != *c_pattern) {
      if (escape_char && *ptr == escape_char)
        ptr += 2;
      else if (*ptr == *c_eol && ptr + crlf <= buf_end && TailsMatch(ptr, c_eol, crlf))
        return SearchResult::END_OF_LINE;
      else
        ++ptr;
    }
  } else if (size == 2) {
    --search_end;
    while (ptr < search_end && (*ptr != *c_pattern || ptr[1] != c_pattern[1])) {
      if (escape_char && *ptr == escape_char)
        ptr += 2;
      else if (*ptr == *c_eol && ptr + crlf <= buf_end && TailsMatch(ptr, c_eol, crlf))
        return SearchResult::END_OF_LINE;
      else
        ++ptr;
    }
  } else {
    int b = 0;
    for (; ptr < buf_end; ++ptr) {
      if (escape_char && *ptr == escape_char) {
        b = 0;
        ++ptr;
      } else if (*ptr == *c_eol && ptr + crlf <= buf_end && TailsMatch(ptr, c_eol, crlf))
        return SearchResult::END_OF_LINE;
      else if (c_pattern[b] != *ptr) {
        if (b != 0) {
          b = kmp_next[b];
          while ((b > -1) && (c_pattern[b] != *ptr)) b = kmp_next[b];
          ++b;
        }
      } else if (size_t(++b) == size) {
        ptr -= size - 1;
        break;
      }
    }
  }
  /*  else if(size == 3) {
          search_end -= 2;
          while(ptr < search_end && (*ptr != c_pattern[0] || *(ptr+1) !=
     c_pattern[1] || *(ptr+2)
     != c_pattern[2])) {
              if(escape_char && *ptr == escape_char)
                  ptr += 2;
              else if (*ptr == eol[0] && (crlf == 1 || *(ptr+1) == eol[1]))
                  return SearchResult::END_OF_LINE;
              else
                  ++ptr;
          }
      }
      else
          ASSERT(0, (std::string("Unexpected pattern: '") + delimiter +
     "'").c_str());*/

  return (ptr < search_end) ? SearchResult::PATTERN_FOUND : SearchResult::END_OF_BUFFER;
}

void ParsingStrategy::GetEOL(const char *const buf, const char *const buf_end) {
  if (eol.size() == 0) {
    const char *ptr = buf;
    for (uint col = 0; col < atis.size() - 1; ++col) {
      if (string_qualifier && *ptr == string_qualifier) {
        if (!SearchUnescapedPattern(++ptr, buf_end, enclose_delimiter, kmp_next_enclose_delimiter))
          throw common::Exception(
              "Unable to detect the line terminating sequence, please specify "
              "it "
              "explicitly.");
        ++ptr;
      } else if (!SearchUnescapedPattern(ptr, buf_end, delimiter, kmp_next_delimiter))
        throw common::Exception(
            "Unable to detect the line terminating sequence, please specify it "
            "explicitly.");
      ptr += delimiter.size();
    }
    if (string_qualifier && *ptr == string_qualifier)
      GuessUnescapedEOLWithEnclose(++ptr, buf_end);
    else
      GuessUnescapedEOL(ptr, buf_end);
  }

  if (eol.size() == 0)
    throw common::Exception(
        "Unable to detect the line terminating sequence, please specify it "
        "explicitly.");
  PrepareKMP(kmp_next_eol, eol);
  if (string_qualifier) {
    enclose_eol = std::string((char *)&string_qualifier, 1) + eol;
    PrepareKMP(kmp_next_enclose_eol, enclose_eol);
  }
}

ParsingStrategy::ParseResult ParsingStrategy::GetOneRow(const char *const buf, size_t size,
                                                        std::vector<ValueCache> &record, uint &rowsize,
                                                        int &errorinfo) {
  const char *buf_end = buf + size;
  if (!prepared) {
    GetEOL(buf, buf_end);
    prepared = true;
  }

  if (buf == buf_end) return ParsingStrategy::ParseResult::EOB;

  const char *ptr = buf;
  bool row_incomplete = false;
  errorinfo = -1;
  for (uint col = 0; col < atis.size() - 1; ++col) {
    const char *val_beg = ptr;
    if (string_qualifier && *ptr == string_qualifier) {
      row_incomplete = !SearchUnescapedPattern(++ptr, buf_end, enclose_delimiter, kmp_next_enclose_delimiter);
      ++ptr;
    } else {
      SearchResult res = SearchUnescapedPatternNoEOL(ptr, buf_end, delimiter, kmp_next_delimiter);
      if (res == SearchResult::END_OF_LINE) {
        GetValue(val_beg, ptr - val_beg, col, record[col]);
        continue;
      }
      row_incomplete = (res == SearchResult::END_OF_BUFFER);
    }

    if (row_incomplete) {
      errorinfo = col;
      break;
    }

    try {
      GetValue(val_beg, ptr - val_beg, col, record[col]);
    } catch (...) {
      if (errorinfo == -1) errorinfo = col;
    }
    ptr += delimiter.size();
  }

  if (!row_incomplete) {
    // the last column
    const char *val_beg = ptr;
    if (string_qualifier && *ptr == string_qualifier) {
      row_incomplete = !SearchUnescapedPattern(++ptr, buf_end, enclose_eol, kmp_next_enclose_eol);
      ++ptr;
    } else
      row_incomplete = !SearchUnescapedPattern(ptr, buf_end, eol, kmp_next_eol);

    if (!row_incomplete) {
      try {
        GetValue(val_beg, ptr - val_beg, atis.size() - 1, record[atis.size() - 1]);
      } catch (...) {
        if (errorinfo == -1) errorinfo = atis.size() - 1;
      }
      ptr += eol.size();
    }
  }

  if (row_incomplete) {
    if (errorinfo == -1) errorinfo = atis.size() - 1;
    return ParsingStrategy::ParseResult::EOB;
  }
  rowsize = uint(ptr - buf);
  return errorinfo == -1 ? ParseResult::OK : ParseResult::ERROR;
}

char TranslateEscapedChar(char c) {
  static char in[] = {'0', 'b', 'n', 'r', 't', char(26)};
  static char out[] = {'\0', '\b', '\n', '\r', '\t', char(26)};
  for (int i = 0; i < 6; i++)
    if (in[i] == c) return out[i];

  return c;
}

void ParsingStrategy::GetValue(const char *value_ptr, size_t value_size, ushort col, ValueCache &buffer) {
  core::AttributeTypeInfo &ati = GetATI(col);

  bool is_enclosed = false;
  if (string_qualifier && *value_ptr == string_qualifier) {
    // trim quotes
    ++value_ptr;
    value_size -= 2;
    is_enclosed = true;
  }

  if (core::ATI::IsCharType(ati.Type())) {
    // trim spaces
    while (value_size > 0 && value_ptr[value_size - 1] == ' ') --value_size;
  }

  // check for null
  bool isnull = false;
  switch (value_size) {
    case 0:
      if (!is_enclosed) isnull = true;
      break;
    case 2:
      if (*value_ptr == '\\' && (value_ptr[1] == 'N' || (!is_enclosed && value_ptr[1] == 'n'))) isnull = true;
      break;
    case 4:
      if (!is_enclosed && strncasecmp(value_ptr, "NULL", 4) == 0) isnull = true;
      break;
    default:
      break;
  }

  if (isnull)
    buffer.ExpectedNull(true);

  else if (core::ATI::IsBinType(ati.Type())) {
    // convert hexadecimal format to binary
    if (value_size % 2) throw common::FormatException(0, 0);
    char *buf = reinterpret_cast<char *>(buffer.Prepare(value_size / 2));
    int p = 0;
    for (uint l = 0; l < value_size; l += 2) {
      int a = 0;
      int b = 0;
      if (isalpha((uchar)value_ptr[l])) {
        char c = tolower(value_ptr[l]);
        if (c < 'a' || c > 'f') throw common::FormatException(0, 0);
        a = c - 'a' + 10;
      } else
        a = value_ptr[l] - '0';
      if (isalpha((uchar)value_ptr[l + 1])) {
        char c = tolower(value_ptr[l + 1]);
        if (c < 'a' || c > 'f') throw common::FormatException(0, 0);
        b = c - 'a' + 10;
      } else
        b = value_ptr[l + 1] - '0';
      buf[p++] = (a << 4) | b;
    }
    buffer.ExpectedSize(int(value_size / 2));

  } else if (core::ATI::IsTxtType(ati.Type())) {
    // process escape characters
    if (ati.CharLen() < (uint)value_size) {
      std::string valueStr(value_ptr, value_size);
      value_size = ati.CharLen();
      STONEDB_LOG(LogCtl_Level::DEBUG, "Data format error. DbName:%s ,TableName:%s ,Col %d, value:%s", dbname.c_str(),
                  tablename.c_str(), col, valueStr.c_str());
      std::stringstream err_msg;
      err_msg << "data truncate,col num" << col << " value:" << valueStr << std::endl;
      common::PushWarning(current_tx->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, err_msg.str().c_str());
    }

    uint reserved = (uint)value_size * ati.CharsetInfo()->mbmaxlen;
    char *buf = reinterpret_cast<char *>(buffer.Prepare(reserved));
    size_t new_size = 0;
    for (size_t j = 0; j < value_size; j++) {
      if (value_ptr[j] == escape_char)
        buf[new_size] = TranslateEscapedChar(value_ptr[++j]);
      else if (value_ptr[j] == *delimiter.c_str())
        break;
      else
        buf[new_size] = value_ptr[j];
      new_size++;
    }

    if (ati.CharsetInfo() != cs_info) {
      // convert between charsets
      uint errors = 0;
      if (ati.CharsetInfo()->mbmaxlen <= cs_info->mbmaxlen)
        new_size = copy_and_convert(buf, reserved, ati.CharsetInfo(), buf, new_size, cs_info, &errors);
      else {
        if (new_size > temp_buf.size()) temp_buf.resize(new_size);
        char *tmpbuf = &temp_buf[0];
        std::memcpy(tmpbuf, buf, new_size);
        new_size = copy_and_convert(buf, reserved, ati.CharsetInfo(), tmpbuf, new_size, cs_info, &errors);
      }
    }

    // check the value length
    size_t char_len = ati.CharsetInfo()->cset->numchars(ati.CharsetInfo(), buf, buf + new_size);
    if (char_len > ati.CharLen()) throw common::FormatException(0, col);

    buffer.ExpectedSize(new_size);

  } else {
    types::BString tmp_string(value_ptr, value_size);
    // reaching here, the parsing function should not be null
    auto function = types::ValueParserForText::GetParsingFuntion(ati);
    if (function(tmp_string, *reinterpret_cast<int64_t *>(buffer.Prepare(sizeof(int64_t)))) ==
        common::ErrorCode::FAILED)
      throw common::FormatException(0,
                                    col);  // TODO: throw appropriate exception
    buffer.ExpectedSize(sizeof(int64_t));
  }
}

}  // namespace loader
}  // namespace stonedb
