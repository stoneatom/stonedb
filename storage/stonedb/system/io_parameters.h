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
#ifndef STONEDB_SYSTEM_IO_PARAMETERS_H_
#define STONEDB_SYSTEM_IO_PARAMETERS_H_
#pragma once

#include <cstring>

#include "common/assert.h"
#include "common/common_definitions.h"
#include "common/data_format.h"
#include "core/rc_attr_typeinfo.h"
#include "system/file_system.h"

namespace stonedb {
namespace system {
enum class Parameter {
  PIPEMODE,
  STRING_QUALIFIER,
  CHARSET_INFO_NUMBER,
  CHARSETS_INDEX_FILE,
  LINE_STARTER,
  LINE_TERMINATOR,
  SKIP_LINES,
  LOCAL_LOAD,
  VALUE_LIST_ELEMENTS,
  LOCK_OPTION,
  OPTIONALLY_ENCLOSED
};

class IOParameters {
 public:
  IOParameters() { Init(); }
  IOParameters(std::string base_path, std::string table_name) : base_path(base_path), table_name(table_name) { Init(); }
  IOParameters &operator=(const IOParameters &rhs) = delete;
  IOParameters(const IOParameters &io_params) = delete;

  void SetDelimiter(const std::string &delimiter) { this->delimiter = delimiter; }
  void SetLineTerminator(const std::string &lineTerminator_) { line_terminator = lineTerminator_; }
  void SetParameter(Parameter param, int value) {
    switch (param) {
      case Parameter::STRING_QUALIFIER:
        string_qualifier = (char)value;
        break;
      case Parameter::CHARSET_INFO_NUMBER:
        charset_info_number = value;
        break;
      case Parameter::LOCAL_LOAD:
        local_load = value;
        break;
      case Parameter::OPTIONALLY_ENCLOSED:
        opt_enclosed = value;
        break;
      case Parameter::LOCK_OPTION:
        lock_option = value;
        break;
      default:
        DEBUG_ASSERT(0 && "unexpected value");
        break;
    }
  }

  void SetParameter(Parameter param, const std::string &value) {
    switch (param) {
      case Parameter::LINE_STARTER:
        line_starter = value;
        break;
      case Parameter::LINE_TERMINATOR:
        line_terminator = value;
        break;
      case Parameter::CHARSETS_INDEX_FILE:
        charsets_dir = value;
        break;
      default:
        DEBUG_ASSERT(0 && "unexpected value");
        break;
    }
  }

  void SetParameter(Parameter param, int64_t value) {
    switch (param) {
      case Parameter::SKIP_LINES:
        skip_lines = value;
        break;
      case Parameter::VALUE_LIST_ELEMENTS:
        value_list_elements = value;
        break;
      default:
        DEBUG_ASSERT(0 && "unexpected value");
        break;
    }
  }

  void SetTimeZone(short _sign, short _minute) {
    sign = _sign;
    minute = _minute;
  }
  void GetTimeZone(short &_sign, short &_minute) const {
    _sign = sign;
    _minute = minute;
  }

  void SetEscapeCharacter(char value) { this->escape_character = value; }
  void SetCharsetsDir(const std::string &value) { this->charsets_dir = value; }
  void SetNullsStr(const std::string &null_str) { this->null_str = null_str; }
  void SetOutput(int _mode,
                 const char *fname)  // mode: 0 - standard console-style output with header
  // fname: NULL for console
  // These settings will work until the next change
  {
    curr_output_mode = _mode;
    if (fname)
      std::strcpy(output_path, fname);
    else
      output_path[0] = '\0';
  }

  common::EDF GetEDF() const { return common::DataFormat::GetDataFormat(curr_output_mode)->GetEDF(); }
  const char *Path() const { return output_path; }
  std::string NullsStr() const { return null_str; }
  const std::string &Delimiter() const { return delimiter; }
  const std::string &LineTerminator() const { return line_terminator; }
  char StringQualifier() const { return string_qualifier; }
  char EscapeCharacter() const { return escape_character; }
  std::string TableName() const { return table_name; }
  std::string LineStarter() const { return line_starter; }
  int64_t TableID() const { return skip_lines; }
  int CharsetInfoNumber() const { return charset_info_number; }
  int LocalLoad() const { return local_load; }
  int OptionallyEnclosed() { return opt_enclosed; }
  void SetRejectFile(std::string const &path, int64_t abortOnCount, double abortOnThreshold) {
    reject_file = path;
    abort_on_count = abortOnCount;
    abort_on_threshold = abortOnThreshold;
  }
  std::string GetRejectFile() const { return (reject_file); }
  int64_t GetAbortOnCount() const { return (abort_on_count); }
  double GetAbortOnThreshold() const { return (abort_on_threshold); }
  void SetATIs(const std::vector<core::AttributeTypeInfo> &atis) { this->atis = atis; }
  const std::vector<core::AttributeTypeInfo> &ATIs() const { return atis; }
  bool LoadDelayed() const { return load_delayed_insert; }
  bool load_delayed_insert = false;
  std::string GetTableName() const { return table_name; }
  void SetLogInfo(void *logptr) { loginfo_ptr = logptr; }
  void *GetLogInfo() const { return loginfo_ptr; }

 private:
  std::vector<core::AttributeTypeInfo> atis;
  int curr_output_mode;    // I/O file format - see RCTable::SaveTable parameters
  char output_path[1024];  // input or output file name, set by "interface..."
  std::string delimiter;
  char string_qualifier;
  char escape_character;
  int opt_enclosed;
  std::string line_starter;
  std::string line_terminator;
  std::string charsets_dir;
  int charset_info_number;
  int64_t skip_lines;
  int64_t value_list_elements;
  int local_load;
  int lock_option;

  // TimeZone
  short sign;
  short minute;

  std::string base_path;
  std::string table_name;
  std::string null_str;
  std::string reject_file;
  int64_t abort_on_count;
  double abort_on_threshold;

  std::string install_path;

  void *loginfo_ptr;

 private:
  void Init() {
    output_path[0] = '\0';
    curr_output_mode = 0;
    delimiter = std::string(DEFAULT_DELIMITER);
    delimiter = std::string(";");
    string_qualifier = 0;
    opt_enclosed = 0;
    escape_character = 0;
    sign = 1;
    minute = 0;
    line_starter = std::string("");
    line_terminator = std::string(DEFAULT_LINE_TERMINATOR);
    charset_info_number = 0;
    skip_lines = 0;
    value_list_elements = 0;
    local_load = 0;
    lock_option = -1;
    abort_on_count = 0;
    abort_on_threshold = 0;
    loginfo_ptr = nullptr;
  }
};
}  // namespace system
}  // namespace stonedb

#endif  // STONEDB_SYSTEM_IO_PARAMETERS_H_
