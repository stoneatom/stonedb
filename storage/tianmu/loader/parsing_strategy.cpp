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
#include <map>
#include <vector>
#include "core/tools.h"
#include "core/transaction.h"
#include "item_timefunc.h"
#include "loader/value_cache.h"
#include "system/io_parameters.h"
#include "system/tianmu_system.h"
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
      thd_(iop.GetTHD()),
      table_(iop.GetTable()),
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

// continuous double enclosed char is reterpreted as a general char.
// ref:https://dev.mysql.com/doc/refman/5.7/en/load-data.html
inline bool DoubleEnclosedCharMatch(const char enclosed_char, const char *ptr, const char *buf_end) {
  if (enclosed_char && *ptr == enclosed_char && ptr + 1 < buf_end && *(ptr + 1) == enclosed_char) {
    return true;
  }

  return false;
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
      else if (DoubleEnclosedCharMatch(string_qualifier_, ptr, search_end))
        ptr += 2;
      else
        ++ptr;
    }
  } else if (size == 2) {
    --search_end;
    while (ptr < search_end && (*ptr != *c_pattern || ptr[1] != c_pattern[1])) {
      if (escape_char_ && *ptr == escape_char_)
        ptr += 2;
      else if (DoubleEnclosedCharMatch(string_qualifier_, ptr, search_end))
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
      } else if (DoubleEnclosedCharMatch(string_qualifier_, ptr, buf_end)) {
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
                                                                                  const std::string &line_termination,
                                                                                  const std::vector<int> &kmp_next) {
  const char *c_pattern = pattern.c_str();
  size_t size = pattern.size();
  const char *c_eol = line_termination.c_str();
  size_t crlf = line_termination.size();
  const char *search_end = buf_end;

  if (size == 1) {
    while (ptr < search_end && *ptr != *c_pattern) {
      if (escape_char_ && *ptr == escape_char_)
        ptr += 2;
      else if (DoubleEnclosedCharMatch(string_qualifier_, ptr, search_end))
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
      else if (DoubleEnclosedCharMatch(string_qualifier_, ptr, search_end))
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
      } else if (DoubleEnclosedCharMatch(string_qualifier_, ptr, buf_end)) {
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

// copy from sql_load.cc
class Field_tmp_nullability_guard {
 public:
  explicit Field_tmp_nullability_guard(Item *item) : m_field(NULL) {
    if (item->type() == Item::FIELD_ITEM) {
      m_field = ((Item_field *)item)->field;

      m_field->set_tmp_nullable();
    }
  }

  ~Field_tmp_nullability_guard() {
    if (m_field)
      m_field->reset_tmp_nullable();
  }

 private:
  Field *m_field;
};

void ParsingStrategy::ReadField(const char *&ptr, const char *&val_beg, Item *&item, uint &index_of_field,
                                std::vector<std::pair<const char *, size_t>> &vec_ptr_field,
                                uint &field_index_in_field_list, const CHARSET_INFO *char_info, bool completed_row) {
  bool is_enclosed = false;
  char *val_start{nullptr};
  size_t val_len{0};

  if (string_qualifier_ && *val_beg == string_qualifier_ && completed_row) {
    // first char is enclose char, skip it
    val_start = const_cast<char *>(val_beg) + 1;
    // skip the first and the last char which is encolose char
    val_len = ptr - val_beg - 2;
    is_enclosed = true;
  } else {
    val_start = const_cast<char *>(val_beg);
    val_len = ptr - val_beg;
  }

  // check for null
  bool isnull = false;
  switch (val_len) {
    case 0:
      if (!is_enclosed)
        isnull = true;
      break;
    case 2:
      if (*val_start == '\\' && (val_start[1] == 'N' || (!is_enclosed && val_start[1] == 'n')))
        isnull = true;
      break;
    case 4:
      if (string_qualifier_ && !is_enclosed && strncasecmp(val_start, "nullptr", 4) == 0)
        isnull = true;
      break;
    default:
      break;
  }

  Item *real_item = item->real_item();
  Field_tmp_nullability_guard fld_tmp_nullability_guard(real_item);
  if (isnull) {
    if (real_item->type() == Item::FIELD_ITEM) {
      Field *field = ((Item_field *)real_item)->field;
      if (field->reset())  // Set to 0
      {
        my_error(ER_WARN_NULL_TO_NOTNULL, MYF(0), field->field_name, thd_->get_stmt_da()->current_row_for_condition());
        return;
      }
      if (!field->real_maybe_null() && field->type() == FIELD_TYPE_TIMESTAMP) {
        // Specific of TIMESTAMP NOT NULL: set to CURRENT_TIMESTAMP.
        Item_func_now_local::store_in(field);
      } else {
        field->set_null();
      }
      if (!first_row_prepared_) {
        std::string field_name(field->field_name);
        index_of_field = map_field_name_to_index_[field_name];
        vec_field_num_to_index_[field_index_in_field_list] = index_of_field;
      } else {
        index_of_field = vec_field_num_to_index_[field_index_in_field_list];
      }

      vec_ptr_field[index_of_field] = std::make_pair(nullptr, 0);  // nullptr indicates null
      ++field_index_in_field_list;
    } else if (item->type() == Item::STRING_ITEM) {
      auto tmp_item = dynamic_cast<Item_user_var_as_out_param *>(item);
      assert(NULL != tmp_item);
      tmp_item->set_null_value(char_info);
    }
    return;
  } else {
    if (real_item->type() == Item::FIELD_ITEM) {
      Field *field = ((Item_field *)real_item)->field;
      field->set_notnull();
      field->store(val_start, val_len, char_info);
      if (!first_row_prepared_) {
        std::string field_name(field->field_name);
        index_of_field = map_field_name_to_index_[field_name];
        vec_field_num_to_index_[field_index_in_field_list] = index_of_field;
      } else {
        index_of_field = vec_field_num_to_index_[field_index_in_field_list];
      }

      vec_ptr_field[index_of_field] = std::make_pair(val_start, val_len);
      ++field_index_in_field_list;

    } else if (item->type() == Item::STRING_ITEM) {
      ((Item_user_var_as_out_param *)item)->set_value((char *)val_start, val_len, char_info);
    }
  }

  return;
}

ParsingStrategy::ParseResult ParsingStrategy::GetOneRow(const char *const buf, size_t size,
                                                        std::vector<ValueCache> &record, uint &rowsize, int &errorinfo,
                                                        bool eof) {
  const char *buf_end = buf + size;
  if (!prepared_) {
    GetEOL(buf, buf_end);
    prepared_ = true;
  }

  if (buf == buf_end)
    return ParsingStrategy::ParseResult::EOB;

  std::vector<std::pair<const char *, size_t>> vec_ptr_field;
  // default values;
  uint n_fields = table_->s->fields;
  uint i = 0;

  // step1, initial field to defaut value
  restore_record(table_, s->default_values);
  for (i = 0; i < n_fields; i++) {
    Field *field = table_->field[i];

    String *str{nullptr};
    if (!first_row_prepared_) {
      std::string field_name(field->field_name);

      str = new (thd_->mem_root) String(MAX_FIELD_WIDTH);
      String *res = field->val_str(str);
      DEBUG_ASSERT(res);
      vec_field_Str_list_.push_back(str);
      vec_field_num_to_index_.push_back(0);
      map_field_name_to_index_[field_name] = i;
    } else {
      str = vec_field_Str_list_[i];
    }
    vec_ptr_field.emplace_back(str->ptr(), str->length());
  }

  const char *ptr = buf;
  bool row_incomplete = false;
  bool row_data_error = false;
  errorinfo = -1;

  List<Item> &fields_vars = thd_->lex->load_field_list;
  List<Item> &fields = thd_->lex->load_update_list;
  List<Item> &values = thd_->lex->load_value_list;
  Item *fld{nullptr};
  List_iterator_fast<Item> f(fields), v(values);
  sql_exchange *ex = thd_->lex->exchange;
  const CHARSET_INFO *char_info = ex->cs ? ex->cs : thd_->variables.collation_database;

  List_iterator_fast<Item> it(fields_vars);
  Item *item{nullptr};
  uint index{0};
  uint field_index_in_field_list{0};
  uint index_of_field{0};

  // step2, fill the field list with data file content;

  // three condition for matching one field:
  // (1) enclosed-char(optional) + content + enclosed-char(optional) + delimiter-str, return PATTERN_FOUND
  // (2) enclosed-char(optional) + content + enclosed-char(optional) + termination_str, return END_OF_LINE
  // (3) enclosed-char(optional) + content +(without enclosed-char(optional) + delimiter-str/termination_str), return
  // END_OF_BUFFER
  bool enclosed_column = false;

  while ((item = it++)) {
    index++;

    const char *val_beg = ptr;

    enclosed_column = false;
    if (string_qualifier_ && *ptr == string_qualifier_)
      enclosed_column = true;
    const std::string &delimitor = enclosed_column ? enclose_delimiter_ : delimiter_;
    const std::string &line_termination = enclosed_column ? enclose_terminator_ : terminator_;
    const std::vector<int> &kmp_local = enclosed_column ? kmp_next_enclose_delimiter_ : kmp_next_delimiter_;

    if (enclosed_column)
      ++ptr;

    SearchResult res = SearchUnescapedPatternNoEOL(ptr, buf_end, delimitor, line_termination, kmp_local);
    row_incomplete = (res == SearchResult::END_OF_BUFFER);

    if (row_incomplete && eof) {
      // field is incompleted,and reach to the end of buffer, take the incompeted data as the field content;
      ReadField(buf_end, val_beg, item, index_of_field, vec_ptr_field, field_index_in_field_list, char_info, false);
      ptr = buf_end;
      row_incomplete = false;
      ++item;
      break;
    }

    if (row_incomplete) {
      errorinfo = index;
      goto end;
    }

    if (enclosed_column)
      ++ptr;
    ReadField(ptr, val_beg, item, index_of_field, vec_ptr_field, field_index_in_field_list, char_info);

    if (res == SearchResult::PATTERN_FOUND) {
      ptr += delimiter_.size();
    }

    if (res == SearchResult::END_OF_LINE) {
      ++item;
      break;
    }
  }

  if (item) {
    while ((item = it++)) {
      // field is few, warn if occurs;
      ReadField(ptr, ptr, item, index_of_field, vec_ptr_field, field_index_in_field_list, char_info, false);
      push_warning_printf(thd_, Sql_condition::SL_WARNING, ER_WARN_TOO_FEW_RECORDS, ER(ER_WARN_TOO_FEW_RECORDS),
                          thd_->get_stmt_da()->current_row_for_condition());
    }
  }

  if (!row_incomplete && !eof) {
    const char *orig_ptr = ptr;
    SearchResult res = SearchUnescapedPatternNoEOL(ptr, buf_end, terminator_, terminator_, kmp_next_terminator_);
    // check too many records, warn if occurs
    if (res != SearchResult::END_OF_BUFFER) {
      if (orig_ptr != ptr) {
        push_warning_printf(thd_, Sql_condition::SL_WARNING, ER_WARN_TOO_MANY_RECORDS, ER(ER_WARN_TOO_MANY_RECORDS),
                            thd_->get_stmt_da()->current_row_for_condition());
      }
      ptr += terminator_.size();
    }
  }

  if (thd_->killed) {
    row_data_error = true;
    goto end;
  }

  it.rewind();
  while ((item = it++)) {
    Item *real_item = item->real_item();
    if (real_item->type() == Item::FIELD_ITEM)
      ((Item_field *)real_item)->field->check_constraints(ER_WARN_NULL_TO_NOTNULL);
  }
  // step3,field in the set clause
  while ((fld = f++)) {
    Item_field *const field = fld->field_for_view_update();
    DEBUG_ASSERT(field != NULL);
    Field *const rfield = field->field;

    Item *const value = v++;

    if (value->save_in_field(rfield, false) < 0) {
      DEBUG_ASSERT(0);
    }

    rfield->check_constraints(ER_BAD_NULL_ERROR);
    String *str{nullptr};

    if (!first_row_prepared_) {
      std::string field_name(field->field_name);
      str = new (thd_->mem_root) String(MAX_FIELD_WIDTH);
      index_of_field = map_field_name_to_index_[field_name];
      vec_field_num_to_index_[field_index_in_field_list] = index_of_field;
      vec_field_Str_list_[index_of_field] = str;
    } else {
      index_of_field = vec_field_num_to_index_[field_index_in_field_list];
      str = vec_field_Str_list_[index_of_field];
    }

    String *res = field->str_result(str);
    if (!res) {
      vec_ptr_field[index_of_field] = std::make_pair(nullptr, 0);
    } else {
      if (res != str) {
        str->copy(*res);
      }
      vec_ptr_field[index_of_field] = std::make_pair(str->ptr(), str->length());
    }

    ++field_index_in_field_list;
  }

  // step4,row is completed, to make the whole row
  for (uint col = 0; col < attr_infos_.size(); ++col) {
    auto &ptr_field = vec_ptr_field[col];
    GetValue(ptr_field.first, ptr_field.second, col, record[col]);
  }

end:
  if (row_incomplete) {
    if (errorinfo == -1)
      errorinfo = index;
    return ParsingStrategy::ParseResult::EOB;
  }
  first_row_prepared_ = true;
  rowsize = uint(ptr - buf);

  return !row_data_error ? ParseResult::OK : ParseResult::ERROR;
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

  // check for null
  bool isnull = false;
  // null scenario
  if (nullptr == value_ptr) {
    isnull = true;
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
    if (ati.Precision() < static_cast<uint>(value_size)) {
      std::string valueStr(value_ptr, value_size);

      value_size = ati.Precision();
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
      else if (DoubleEnclosedCharMatch(string_qualifier_, value_ptr + j, value_ptr + value_size))
        buf[new_size] = value_ptr[++j];
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
