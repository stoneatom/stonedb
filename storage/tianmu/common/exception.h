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
#ifndef TIANMU_COMMON_EXCEPTION_H_
#define TIANMU_COMMON_EXCEPTION_H_
#pragma once

#include <stdexcept>

#include "common/common_definitions.h"

namespace Tianmu {
namespace common {

enum class ErrorCode {
  SUCCESS = 0,
  FAILED = 1,
  OUT_OF_RANGE = 2,
  VALUE_TRUNCATED = 3,
  CANNOT_OPEN_FILE_OR_PIPE,
  DATA_ERROR,
  UNKNOWN_ERROR,
  WRONG_PARAMETER,
  DATACONVERSION,
  UNSUPPORTED_DATATYPE,
  OUT_OF_MEMORY,
  KILLED,
  DUPP_KEY,
  NOT_FOUND_KEY,
};

static std::string error_messages[] = {
    "Success.",
    "Failed.",
    "Out of range.",
    "Value truncated.",
    "Cannot open file/pipe.",
    "Data error.",
    "Unknown error.",
    "Wrong parameter.",
    "Data conversion error.",
    "Unsupported data type.",
    "Out of memory/disk space.",
    "Killed.",
};

inline bool IsError(ErrorCode tianmu_err_code) { return tianmu_err_code == ErrorCode::FAILED; }
inline bool IsWarning(ErrorCode tianmu_err_code) {
  return tianmu_err_code == ErrorCode::OUT_OF_RANGE || tianmu_err_code == ErrorCode::VALUE_TRUNCATED;
}

class TianmuError {
 public:
  TianmuError(ErrorCode tianmu_error_code = ErrorCode::SUCCESS) : ec_(tianmu_error_code) {}
  TianmuError(ErrorCode tianmu_error_code, std::string message) : ec_(tianmu_error_code), message_(message) {}
  TianmuError(const TianmuError &tianmu_e) : ec_(tianmu_e.ec_), message_(tianmu_e.message_) {}
  TianmuError &operator=(const TianmuError &) = default;
  operator ErrorCode() { return ec_; }
  ErrorCode GetErrorCode() { return ec_; }
  bool operator==(ErrorCode tianmu_ec) { return tianmu_ec == ec_; }
  const std::string &Message() {
    if (!message_.empty())
      return message_;
    return error_messages[static_cast<int>(ec_)];
  }

 private:
  ErrorCode ec_;
  std::string message_;
};

class Exception : public std::runtime_error {
 public:
  Exception(std::string const &msg);
  virtual ~Exception() {}
  virtual const std::string &trace() const { return stack_trace_; }
  virtual const std::string &getExceptionMsg() const { return exception_msg_; }

 protected:
  std::string stack_trace_;
  std::string exception_msg_;
};

// internal error
class InternalException : public Exception {
 public:
  InternalException(std::string const &msg) : Exception(msg) {}
  InternalException(TianmuError tianmu_error) : Exception(tianmu_error.Message()) {}
};

// the system lacks memory or cannot use disk cache
class OutOfMemoryException : public Exception {
 public:
  OutOfMemoryException() : Exception("Insufficient memory space.") {}
  OutOfMemoryException(size_t sz) : Exception("Insufficient memory space. Requested size is " + std::to_string(sz)) {}
  OutOfMemoryException(std::string const &msg) : Exception(msg) {}
};

class KilledException : public Exception {
 public:
  KilledException() : Exception("Process killed") {}
  KilledException(std::string const &msg) : Exception(msg) {}
};

// there are problems with system operations,
// e.g. i/o operations on database files, system functions etc.
class SystemException : public Exception {
 public:
  SystemException(std::string const &msg) : Exception(msg) {}
};

// database is corrupted
class DatabaseException : public Exception {
 public:
  DatabaseException(std::string const &msg) : Exception(msg) {}
};

// database is corrupted
class NoTableFolderException : public DatabaseException {
 public:
  NoTableFolderException(std::string const &dir)
      : DatabaseException(std::string("Table folder ") + dir + " does not exist") {}
};

// user command syntax error
class SyntaxException : public Exception {
 public:
  SyntaxException(std::string const &msg) : Exception(msg) {}
};

// a user command has the inappropriate semantics
// e.g. table or attribute do not exist
class DBObjectException : public Exception {
 public:
  DBObjectException(std::string const &msg) : Exception(msg) {}
};

// user command is too complex (not implemented yet)
class NotImplementedException : public Exception {
 public:
  NotImplementedException(std::string const &msg) : Exception(msg) {}
};

// import file does not exists or can not be open
class FileException : public Exception {
 public:
  FileException(std::string const &msg) : Exception(msg) {}
  FileException(TianmuError tianmu_error) : Exception(tianmu_error.Message()) {}
};

// wrong format of import file
class FormatException : public Exception {
 public:
  unsigned long long m_row_no;
  unsigned int m_field_no;

  FormatException(unsigned long long row_no, unsigned int field_no)
      : Exception("Data format error. Row: " + std::to_string(row_no) + " Column: " + std::to_string(field_no)),
        m_row_no(row_no),
        m_field_no(field_no) {}
  FormatException(std::string const &msg) : Exception(msg), m_row_no(-1), m_field_no(-1) {}
};

class DataTypeConversionException : public Exception {
 public:
  int64_t value;    // converted value
  ColumnType type;  // type to which value is converted
  DataTypeConversionException(std::string const &msg, int64_t val = NULL_VALUE_64, ColumnType t = ColumnType::UNK)
      : Exception(msg), value(val), type(t) {}

  DataTypeConversionException(TianmuError tianmu_error = ErrorCode::DATACONVERSION, int64_t val = NULL_VALUE_64,
                              ColumnType t = ColumnType::UNK)
      : Exception(tianmu_error.Message()), value(val), type(t) {}
};

class UnsupportedDataTypeException : public Exception {
 public:
  UnsupportedDataTypeException(std::string const &msg) : Exception(msg) {}
  UnsupportedDataTypeException(TianmuError tianmu_error = ErrorCode::UNSUPPORTED_DATATYPE)
      : Exception(tianmu_error.Message()) {}
};

class NetStreamException : public Exception {
 public:
  NetStreamException(std::string const &msg) : Exception(msg) {}
  NetStreamException(TianmuError tianmu_error) : Exception(tianmu_error.Message()) {}
};

class AssertException : public Exception {
 public:
  AssertException(const char *cond, const char *file, int line, const std::string &msg);

 private:
};

class DupKeyException : public Exception {
 public:
  DupKeyException(std::string const &msg) : Exception(msg) {}
};

class AutoIncException : public Exception {
 public:
  AutoIncException(std::string const &msg) : Exception(msg) {}
};

}  // namespace common
}  // namespace Tianmu

#endif  // TIANMU_COMMON_EXCEPTION_H_
