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

namespace Tianmu {
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
    : attr_infos_(iop.ATIs()),
      prepared_(false),
      terminator_(iop.LineTerminator()),
      delimiter_(iop.Delimiter()),
      string_qualifier_(iop.StringQualifier()),
      escape_char_(iop.EscapeCharacter()),
      temp_buf_(65536) {
  charset_info_ = get_charset(iop.CharsetInfoNumber(), 0);
  for (ushort i = 0; i < attr_infos_.size(); ++i) {
    if (core::ATI::IsStringType(GetATI(i).Type())) {
      GetATI(i).SetCollation(get_charset(columns_collations[i], 0));
    }
  }

  PrepareKMP(kmp_next_delimiter_, delimiter_);
  if (string_qualifier_) {
    enclose_delimiter_ = std::string((char *)&string_qualifier_, 1) + delimiter_;
    PrepareKMP(kmp_next_enclose_delimiter_, enclose_delimiter_);
  }

  std::string tmpName = iop.GetTableName();
  if (tmpName.find("/") != std::string::npos) {
    tablename_ = tmpName.substr(tmpName.find("/") + 1);
    dbname_ = tmpName.substr(0, tmpName.find("/"));
  }
}

inline void ParsingStrategy::GuessUnescapedEOL(const char *ptr, const char *const buf_end) {
  for (; ptr < buf_end; ptr++) {
    if (escape_char_ && *ptr == escape_char_) {
      if (ptr + 2 < buf_end && ptr[1] == '\r' && ptr[2] == '\n')
        ptr += 3;
      else
        ptr += 2;
    } else if (*ptr == '\n') {
      terminator_ = "\n";
      break;
    } else if (*ptr == '\r' && ptr + 1 < buf_end && ptr[1] == '\n') {
      terminator_ = "\r\n";
      break;
    }
  }
}

inline void ParsingStrategy::GuessUnescapedEOLWithEnclose(const char *ptr, const char *const buf_end) {
  for (; ptr < buf_end; ptr++) {
    if (escape_char_ && *ptr == escape_char_)
      ptr += 2;
    else if (string_qualifier_ && *ptr == string_qualifier_) {
      if (ptr + 1 < buf_end && ptr[1] == '\n') {
        terminator_ = "\n";
        break;
      } else if (ptr + 2 < buf_end && ptr[1] == '\r' && ptr[2] == '\n') {
        terminator_ = "\r\n";
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
      if (escape_char_ && *ptr == escape_char_)
        ptr += 2;
      else
        ++ptr;
    }
  } else if (size == 2) {
    --search_end;
    while (ptr < search_end && (*ptr != *c_pattern || ptr[1] != c_pattern[1])) {
      if (escape_char_ && *ptr == escape_char_)
        ptr += 2;
      else
        ++ptr;
    }
  } else {
    int b = 0;
    for (; ptr < buf_end; ++ptr) {
      if (escape_char_ && *ptr == escape_char_) {
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
              if(*ptr == escape_char_)
                  ptr += 2;
              else
                  ++ptr;
          }
      }
      else
          ASSERT(0, (std::string("Unexpected pattern: '") + delimiter_ +
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
  const char *c_eol = terminator_.c_str();
  size_t crlf = terminator_.size();
  const char *search_end = buf_end;

  if (size == 1) {
    while (ptr < search_end && *ptr != *c_pattern) {
      if (escape_char_ && *ptr == escape_char_)
        ptr += 2;
      else if (*ptr == *c_eol && ptr + crlf <= buf_end && TailsMatch(ptr, c_eol, crlf))
        return SearchResult::END_OF_LINE;
      else
        ++ptr;
    }
  } else if (size == 2) {
    --search_end;
    while (ptr < search_end && (*ptr != *c_pattern || ptr[1] != c_pattern[1])) {
      if (escape_char_ && *ptr == escape_char_)
        ptr += 2;
      else if (*ptr == *c_eol && ptr + crlf <= buf_end && TailsMatch(ptr, c_eol, crlf))
        return SearchResult::END_OF_LINE;
      else
        ++ptr;
    }
  } else {
    int b = 0;
    for (; ptr < buf_end; ++ptr) {
      if (escape_char_ && *ptr == escape_char_) {
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
              if(escape_char_ && *ptr == escape_char_)
                  ptr += 2;
              else if (*ptr == terminator_[0] && (crlf == 1 || *(ptr+1) == terminator_[1]))
                  return SearchResult::END_OF_LINE;
              else
                  ++ptr;
          }
      }
      else
          ASSERT(0, (std::string("Unexpected pattern: '") + delimiter_ +
     "'").c_str());*/

  return (ptr < search_end) ? SearchResult::PATTERN_FOUND : SearchResult::END_OF_BUFFER;
}

void ParsingStrategy::GetEOL(const char *const buf, const char *const buf_end) {
  if (terminator_.size() == 0) {
    const char *ptr = buf;
    for (uint col = 0; col < attr_infos_.size() - 1; ++col) {
      if (string_qualifier_ && *ptr == string_qualifier_) {
        if (!SearchUnescapedPattern(++ptr, buf_end, enclose_delimiter_, kmp_next_enclose_delimiter_))
          throw common::Exception(
              "Unable to detect the line terminating sequence, please specify "
              "it "
              "explicitly.");
        ++ptr;
      } else if (!SearchUnescapedPattern(ptr, buf_end, delimiter_, kmp_next_delimiter_))
        throw common::Exception(
            "Unable to detect the line terminating sequence, please specify it "
            "explicitly.");
      ptr += delimiter_.size();
    }
    if (string_qualifier_ && *ptr == string_qualifier_)
      GuessUnescapedEOLWithEnclose(++ptr, buf_end);
    else
      GuessUnescapedEOL(ptr, buf_end);
  }

  if (terminator_.size() == 0)
    throw common::Exception(
        "Unable to detect the line terminating sequence, please specify it "
        "explicitly.");
  PrepareKMP(kmp_next_terminator_, terminator_);
  if (string_qualifier_) {
    enclose_terminator_ = std::string((char *)&string_qualifier_, 1) + terminator_;
    PrepareKMP(kmp_next_enclose_terminator_, enclose_terminator_);
  }
}

ParsingStrategy::ParseResult ParsingStrategy::GetOneRow(const char *const buf, size_t size,
                                                        std::vector<ValueCache> &record, uint &rowsize,
                                                        int &errorinfo) {
  const char *buf_end = buf + size;
  if (!prepared_) {
    GetEOL(buf, buf_end);
    prepared_ = true;
  }

  if (buf == buf_end)
    return ParsingStrategy::ParseResult::EOB;

  const char *ptr = buf;
  bool row_incomplete = false;
  errorinfo = -1;
  for (uint col = 0; col < attr_infos_.size() - 1; ++col) {
    const char *val_beg = ptr;
    if (string_qualifier_ && *ptr == string_qualifier_) {
      row_incomplete = !SearchUnescapedPattern(++ptr, buf_end, enclose_delimiter_, kmp_next_enclose_delimiter_);
      ++ptr;
    } else {
      SearchResult res = SearchUnescapedPatternNoEOL(ptr, buf_end, delimiter_, kmp_next_delimiter_);
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
      if (errorinfo == -1)
        errorinfo = col;
    }
    ptr += delimiter_.size();
  }

  if (!row_incomplete) {
    // the last column
    const char *val_beg = ptr;
    if (string_qualifier_ && *ptr == string_qualifier_) {
      row_incomplete = !SearchUnescapedPattern(++ptr, buf_end, enclose_terminator_, kmp_next_enclose_terminator_);
      ++ptr;
    } else
      row_incomplete = !SearchUnescapedPattern(ptr, buf_end, terminator_, kmp_next_terminator_);

    if (!row_incomplete) {
      try {
        GetValue(val_beg, ptr - val_beg, attr_infos_.size() - 1, record[attr_infos_.size() - 1]);
      } catch (...) {
        if (errorinfo == -1)
          errorinfo = attr_infos_.size() - 1;
      }
      ptr += terminator_.size();
    }
  }

  if (row_incomplete) {
    if (errorinfo == -1)
      errorinfo = attr_infos_.size() - 1;
    return ParsingStrategy::ParseResult::EOB;
  }
  rowsize = uint(ptr - buf);
  return errorinfo == -1 ? ParseResult::OK : ParseResult::ERROR;
}

char TranslateEscapedChar(char c) {
  static char in[] = {'0', 'b', 'n', 'r', 't', char(26)};
  static char out[] = {'\0', '\b', '\n', '\r', '\t', char(26)};
  for (int i = 0; i < 6; i++)
    if (in[i] == c)
      return out[i];

  return c;
}

void ParsingStrategy::GetValue(const char *value_ptr, size_t value_size, ushort col, ValueCache &buffer) {
  core::AttributeTypeInfo &ati = GetATI(col);

  bool is_enclosed = false;
  if (string_qualifier_ && *value_ptr == string_qualifier_) {
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
      if (!is_enclosed)
        isnull = true;
      break;
    case 2:
      if (*value_ptr == '\\' && (value_ptr[1] == 'N' || (!is_enclosed && value_ptr[1] == 'n')))
        isnull = true;
      break;
    case 4:
      if (!is_enclosed && strncasecmp(value_ptr, "NULL", 4) == 0)
        isnull = true;
      break;
    default:
      break;
  }

  if (isnull)
    buffer.ExpectedNull(true);

  else if (core::ATI::IsBinType(ati.Type())) {
    // convert hexadecimal format to binary
    if (value_size % 2)
      throw common::FormatException(0, 0);
    char *buf = reinterpret_cast<char *>(buffer.Prepare(value_size / 2));
    int p = 0;
    for (uint l = 0; l < value_size; l += 2) {
      int a = 0;
      int b = 0;
      if (isalpha((uchar)value_ptr[l])) {
        char c = tolower(value_ptr[l]);
        if (c < 'a' || c > 'f')
          throw common::FormatException(0, 0);
        a = c - 'a' + 10;
      } else
        a = value_ptr[l] - '0';
      if (isalpha((uchar)value_ptr[l + 1])) {
        char c = tolower(value_ptr[l + 1]);
        if (c < 'a' || c > 'f')
          throw common::FormatException(0, 0);
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
      TIANMU_LOG(LogCtl_Level::DEBUG, "Data format error. DbName:%s ,TableName:%s ,Col %d, value:%s", dbname_.c_str(),
                 tablename_.c_str(), col, valueStr.c_str());
      std::stringstream err_msg;
      err_msg << "data truncate,col num" << col << " value:" << valueStr << std::endl;
      common::PushWarning(current_txn_->Thd(), Sql_condition::SL_WARNING, ER_UNKNOWN_ERROR, err_msg.str().c_str());
    }

    uint reserved = (uint)value_size * ati.CharsetInfo()->mbmaxlen;
    char *buf = reinterpret_cast<char *>(buffer.Prepare(reserved));
    size_t new_size = 0;
    for (size_t j = 0; j < value_size; j++) {
      if (value_ptr[j] == escape_char_)
        buf[new_size] = TranslateEscapedChar(value_ptr[++j]);
      else if (value_ptr[j] == *delimiter_.c_str())
        break;
      else
        buf[new_size] = value_ptr[j];
      new_size++;
    }

    if (ati.CharsetInfo() != charset_info_) {
      // convert between charsets
      uint errors = 0;
      if (ati.CharsetInfo()->mbmaxlen <= charset_info_->mbmaxlen)
        new_size = copy_and_convert(buf, reserved, ati.CharsetInfo(), buf, new_size, charset_info_, &errors);
      else {
        if (new_size > temp_buf_.size())
          temp_buf_.resize(new_size);
        char *tmpbuf = &temp_buf_[0];
        std::memcpy(tmpbuf, buf, new_size);
        new_size = copy_and_convert(buf, reserved, ati.CharsetInfo(), tmpbuf, new_size, charset_info_, &errors);
      }
    }

    // check the value length
    size_t char_len = ati.CharsetInfo()->cset->numchars(ati.CharsetInfo(), buf, buf + new_size);
    if (char_len > ati.CharLen())
      throw common::FormatException(0, col);

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
}  // namespace Tianmu
