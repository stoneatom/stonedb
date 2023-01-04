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
#ifndef TIANMU_SYSTEM_IO_PARAMETERS_H_
#define TIANMU_SYSTEM_IO_PARAMETERS_H_
#pragma once

#include <cstring>

#include "common/assert.h"
#include "common/common_definitions.h"
#include "common/data_format.h"
#include "core/tianmu_attr_typeinfo.h"
#include "system/file_system.h"

namespace Tianmu {
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
  OPTIONALLY_ENCLOSED,
  TABLE_ID
};

class IOParameters {
 public:
  IOParameters() { Init(); }
  IOParameters(std::string base_path, std::string table_name) : base_path_(base_path), table_name_(table_name) {
    Init();
  }
  IOParameters &operator=(const IOParameters &rhs) = delete;
  IOParameters(const IOParameters &io_params) = delete;

  void SetDelimiter(const std::string &delimiter_) { this->delimiter_ = delimiter_; }
  void SetLineTerminator(const std::string &lineTerminator) { line_terminator_ = lineTerminator; }
  void SetParameter(Parameter param, int value) {
    switch (param) {
      case Parameter::STRING_QUALIFIER:
        string_qualifier_ = (char)value;
        break;
      case Parameter::CHARSET_INFO_NUMBER:
        charset_info_number_ = value;
        break;
      case Parameter::LOCAL_LOAD:
        local_load_ = value;
        break;
      case Parameter::OPTIONALLY_ENCLOSED:
        opt_enclosed_ = value;
        break;
      case Parameter::LOCK_OPTION:
        lock_option_ = value;
        break;
      default:
        DEBUG_ASSERT(0 && "unexpected value");
        break;
    }
  }
  void SetTable(TABLE *table) { table_ = table; };
  TABLE *GetTable() const { return table_; }
  void SetParameter(Parameter param, const std::string &value) {
    switch (param) {
      case Parameter::LINE_STARTER:
        line_starter_ = value;
        break;
      case Parameter::LINE_TERMINATOR:
        line_terminator_ = value;
        break;
      case Parameter::CHARSETS_INDEX_FILE:
        charsets_dir_ = value;
        break;
      default:
        DEBUG_ASSERT(0 && "unexpected value");
        break;
    }
  }

  void SetParameter(Parameter param, int64_t value) {
    switch (param) {
      case Parameter::SKIP_LINES:
        skip_lines_ = value;
        break;
      case Parameter::VALUE_LIST_ELEMENTS:
        value_list_elements_ = value;
        break;
      case Parameter::TABLE_ID:
        table_id_ = value;
        break;
      default:
        DEBUG_ASSERT(0 && "unexpected value");
        break;
    }
  }

  void SetTimeZone(short sign, short minute) {
    sign_ = sign;
    minute_ = minute;
  }

  void GetTimeZone(short &sign, short &minute) const {
    sign = sign_;
    minute = minute_;
  }

  void SetEscapeCharacter(char value) { this->escape_character_ = value; }
  void SetCharsetsDir(const std::string &value) { this->charsets_dir_ = value; }
  void SetNullsStr(const std::string &null_str) { this->null_str_ = null_str; }
  void SetOutput(int mode,
                 const char *fname)  // mode: 0 - standard console-style output with header
  // fname: nullptr for console
  // These settings will work until the next change
  {
    curr_output_mode_ = mode;
    if (fname)
      std::strcpy(output_path_, fname);
    else
      output_path_[0] = '\0';
  }

  common::EDF GetEDF() const { return common::DataFormat::GetDataFormat(curr_output_mode_)->GetEDF(); }
  const char *Path() const { return output_path_; }
  std::string NullsStr() const { return null_str_; }
  const std::string &Delimiter() const { return delimiter_; }
  const std::string &LineTerminator() const { return line_terminator_; }
  char StringQualifier() const { return string_qualifier_; }
  char EscapeCharacter() const { return escape_character_; }
  std::string TableName() const { return table_name_; }
  std::string LineStarter() const { return line_starter_; }
  int64_t TableID() const { return table_id_; }
  int64_t GetSkipLines() const { return skip_lines_; }
  int CharsetInfoNumber() const { return charset_info_number_; }
  int LocalLoad() const { return local_load_; }
  int OptionallyEnclosed() const { return opt_enclosed_; }
  void SetRejectFile(std::string const &path, int64_t abortOnCount, double abortOnThreshold) {
    reject_file_ = path;
    abort_on_count_ = abortOnCount;
    abort_on_threshold_ = abortOnThreshold;
  }
  std::string GetRejectFile() const { return (reject_file_); }
  int64_t GetAbortOnCount() const { return (abort_on_count_); }
  double GetAbortOnThreshold() const { return (abort_on_threshold_); }
  void SetATIs(const std::vector<core::AttributeTypeInfo> &attr_type_info) { this->attr_type_info_ = attr_type_info; }
  const std::vector<core::AttributeTypeInfo> &ATIs() const { return attr_type_info_; }
  bool LoadDelayed() const { return load_delayed_insert_; }
  std::string GetTableName() const { return table_name_; }
  void SetLogInfo(void *logptr) { loginfo_ptr_ = logptr; }
  void *GetLogInfo() const { return loginfo_ptr_; }
  void SetTHD(THD *thd) { thd_ = thd; }
  THD *GetTHD() const { return thd_; }

 public:
  bool load_delayed_insert_ = false;

 private:
  std::vector<core::AttributeTypeInfo> attr_type_info_;
  int curr_output_mode_;    // I/O file format - see TianmuTable::SaveTable parameters
  char output_path_[1024];  // input or output file name, set by "interface..."
  std::string delimiter_;
  char string_qualifier_;
  char escape_character_;
  int opt_enclosed_;

  std::string line_starter_;
  std::string line_terminator_;
  std::string charsets_dir_;
  int charset_info_number_;
  int64_t skip_lines_;
  int64_t table_id_{0};
  int64_t value_list_elements_;
  int local_load_;
  int lock_option_;
  THD *thd_{nullptr};

  // TimeZone
  short sign_;
  short minute_;

  std::string base_path_;
  std::string table_name_;
  std::string null_str_;
  std::string reject_file_;
  int64_t abort_on_count_;
  double abort_on_threshold_;

  std::string install_path_;

  void *loginfo_ptr_;
  TABLE *table_{nullptr};

 private:
  void Init() {
    output_path_[0] = '\0';
    curr_output_mode_ = 0;
    delimiter_ = std::string(DEFAULT_DELIMITER);
    delimiter_ = std::string(";");
    string_qualifier_ = 0;
    opt_enclosed_ = 0;
    escape_character_ = 0;
    sign_ = 1;
    minute_ = 0;
    line_starter_ = std::string("");
    line_terminator_ = std::string(DEFAULT_LINE_TERMINATOR);
    charset_info_number_ = 0;
    skip_lines_ = 0;
    value_list_elements_ = 0;
    local_load_ = 0;
    lock_option_ = -1;
    abort_on_count_ = 0;
    abort_on_threshold_ = 0;
    loginfo_ptr_ = nullptr;
  }
};

}  // namespace system
}  // namespace Tianmu

#endif  // TIANMU_SYSTEM_IO_PARAMETERS_H_
