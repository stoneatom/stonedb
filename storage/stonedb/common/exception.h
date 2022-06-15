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
#ifndef STONEDB_COMMON_EXCEPTION_H_
#define STONEDB_COMMON_EXCEPTION_H_
#pragma once

#include <stdexcept>

#include "common/common_definitions.h"

namespace stonedb {
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

inline bool IsError(ErrorCode sdbrc) { return sdbrc == ErrorCode::FAILED; }
inline bool IsWarning(ErrorCode sdbrc) {
  return sdbrc == ErrorCode::OUT_OF_RANGE || sdbrc == ErrorCode::VALUE_TRUNCATED;
}

class SDBError {
 public:
  SDBError(ErrorCode sdb_error_code = ErrorCode::SUCCESS) : ec(sdb_error_code) {}
  SDBError(ErrorCode sdb_error_code, std::string message) : ec(sdb_error_code), message(message) {}
  SDBError(const SDBError &sdbe) : ec(sdbe.ec), message(sdbe.message) {}
  operator ErrorCode() { return ec; }
  ErrorCode GetErrorCode() { return ec; }
  bool operator==(ErrorCode sdbec) { return sdbec == ec; }
  const std::string &Message() {
    if (!message.empty()) return message;
    return error_messages[static_cast<int>(ec)];
  }

 private:
  ErrorCode ec;
  std::string message;
};

class Exception : public std::runtime_error {
 public:
  Exception(std::string const &msg);
  virtual ~Exception() {}
  virtual const std::string &trace() const { return stack_trace; }

 protected:
  std::string stack_trace;
};

// internal error
class InternalException : public Exception {
 public:
  InternalException(std::string const &msg) : Exception(msg) {}
  InternalException(SDBError sdberror) : Exception(sdberror.Message()) {}
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
  FileException(SDBError sdberror) : Exception(sdberror.Message()) {}
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
  int64_t value;  // converted value
  CT type;        // type to which value is converted
  DataTypeConversionException(std::string const &msg, int64_t val = NULL_VALUE_64, CT t = CT::UNK)
      : Exception(msg), value(val), type(t) {}

  DataTypeConversionException(SDBError sdberror = ErrorCode::DATACONVERSION, int64_t val = NULL_VALUE_64,
                              CT t = CT::UNK)
      : Exception(sdberror.Message()), value(val), type(t) {}
};

class UnsupportedDataTypeException : public Exception {
 public:
  UnsupportedDataTypeException(std::string const &msg) : Exception(msg) {}
  UnsupportedDataTypeException(SDBError sdberror = ErrorCode::UNSUPPORTED_DATATYPE) : Exception(sdberror.Message()) {}
};

class NetStreamException : public Exception {
 public:
  NetStreamException(std::string const &msg) : Exception(msg) {}
  NetStreamException(SDBError sdberror) : Exception(sdberror.Message()) {}
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
}  // namespace stonedb

#endif  // STONEDB_COMMON_EXCEPTION_H_
