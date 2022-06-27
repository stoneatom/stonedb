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
#ifndef STONEDB_COMMON_COMMON_DEFINITIONS_H_
#define STONEDB_COMMON_COMMON_DEFINITIONS_H_
#pragma once

#include <cfloat>
#include <climits>
#include <memory>
#include <string>

#include "common/mysql_gate.h"

#include "common/defs.h"

namespace stonedb {

constexpr size_t operator""_KB(unsigned long long v) { return 1024u * v; }

constexpr size_t operator""_MB(unsigned long long v) { return 1024u * 1024u * v; }

constexpr size_t operator""_GB(unsigned long long v) { return 1024u * 1024u * 1024u * v; }

namespace common {

void PushWarning(THD *thd, Sql_condition::enum_warning_level level, uint code, const char *msg);

// Column Type
// NOTE: do not change the order of implemented data types! Stored as int(...)
// on disk.
enum class CT : unsigned char {
  STRING,   // string treated either as dictionary value or "free" text
  VARCHAR,  // as above (discerned for compatibility with SQL)
  INT,      // integer 32-bit

  NUM,   // numerical: decimal, up to DEC(18,18)
  DATE,  // numerical (treated as integer in YYYYMMDD format)
  TIME,  // numerical (treated as integer in HHMMSS format)

  BYTEINT,   // integer 8-bit
  SMALLINT,  // integer 16-bit

  BIN,      // free binary (BLOB), no encoding
  BYTE,     // free binary, fixed size, no encoding
  VARBYTE,  // free binary, variable size, no encoding
  REAL,     // double (stored as non-interpreted int64_t, null value is
            // NULL_VALUE_64)
  DATETIME,
  TIMESTAMP,
  DATETIME_N,
  TIMESTAMP_N,
  TIME_N,

  FLOAT,
  YEAR,
  MEDIUMINT,
  BIGINT,
  LONGTEXT,
  UNK = 255
};

enum class PackType { INT, STR };

constexpr double PLUS_INF_DBL = DBL_MAX;
constexpr double MINUS_INF_DBL = DBL_MAX * -1;
constexpr int64_t PLUS_INF_64 = 0x7FFFFFFFFFFFFFFFULL;
constexpr int64_t MINUS_INF_64 = 0x8000000000000000ULL;
constexpr int64_t NULL_VALUE_64 = 0x8000000000000001ULL;
constexpr int32_t NULL_VALUE_32 = 0x80000000;
constexpr short NULL_VALUE_SH = -32768;
constexpr char NULL_VALUE_C = -128;
constexpr uint32_t NULL_VALUE_U = 0xFFFFFFFC;
constexpr int64_t MAX_ROW_NUMBER = 0x00007FFFFFFFFFFFULL;  // 2^47 - 1

constexpr int64_t SDB_BIGINT_MAX = PLUS_INF_64 - 1;
constexpr int64_t SDB_BIGINT_MIN = NULL_VALUE_64;

#define NULL_VALUE_D (*(double *)("\x01\x00\x00\x00\x00\x00\x00\x80"))
#define SDB_INT_MIN (-2147483647)
#define SDB_MEDIUMINT_MAX ((1 << 23) - 1)
#define SDB_MEDIUMINT_MIN (-((1 << 23)))
#define SDB_TINYINT_MAX 127
#define SDB_TINYINT_MIN (-127)
#define SDB_SMALLINT_MAX ((1 << 15) - 1)
#define SDB_SMALLINT_MIN (-((1 << 15) - 1))

#define PACK_INVALID 0
#define FIELD_MAXLENGTH 65535
#define SHORT_MAX 65535
#define DICMAP_MAX 65536

#define ZERO_LENGTH_STRING ""
#define DEFAULT_DELIMITER ";"
#define DEFAULT_LINE_TERMINATOR ""

enum class RSValue : char {
  RS_NONE = 0,    // the pack is empty
  RS_SOME = 1,    // the pack is suspected (but may be empty or full) (i.e.
                  // RSValue::RS_SOME & RSValue::RS_ALL = RSValue::RS_SOME)
  RS_ALL = 2,     // the pack is full
  RS_UNKNOWN = 3  // the pack was not checked yet (i.e. RSValue::RS_UNKNOWN & RSValue::RS_ALL = RSValue::RS_ALL)
};

/**
        The types of support SQL query operators.

        The order of these enumerated values is important and
        relevent to the Descriptor class for the time being.
        Any changes made here must also be reflected in the
        Descriptor class' interim createQueryOperator() member.
 */
enum class Operator {
  O_EQ = 0,
  O_EQ_ALL,
  O_EQ_ANY,
  O_NOT_EQ,
  O_NOT_EQ_ALL,
  O_NOT_EQ_ANY,
  O_LESS,
  O_LESS_ALL,
  O_LESS_ANY,
  O_MORE,
  O_MORE_ALL,
  O_MORE_ANY,
  O_LESS_EQ,
  O_LESS_EQ_ALL,
  O_LESS_EQ_ANY,
  O_MORE_EQ,
  O_MORE_EQ_ALL,
  O_MORE_EQ_ANY,

  O_IS_NULL,
  O_NOT_NULL,
  O_BETWEEN,
  O_NOT_BETWEEN,
  O_LIKE,
  O_NOT_LIKE,
  O_IN,
  O_NOT_IN,
  O_EXISTS,
  O_NOT_EXISTS,

  O_FALSE,
  O_TRUE,     // constants
  O_ESCAPE,   // O_ESCAPE is special terminating value, do not interpret
  O_OR_TREE,  // fake operator indicating complex descriptor

  // below operators correspond to MySQL special operators used in MySQL
  // expression tree
  O_MULT_EQUAL_FUNC,  // a=b=c
  O_NOT_FUNC,         // NOT
  O_NOT_ALL_FUNC,     //
  O_UNKNOWN_FUNC,     //
  O_ERROR,            // undefined operator

  /**
  Enumeration member count.
  This should always be the last member.  It's a count of
  the elements in this enumeration and can be used for both
  compiled-time and run-time bounds checking.
  */
  OPERATOR_ENUM_COUNT
};

enum class LogicalOperator { O_AND, O_OR };

enum class ColOperation {
  DELAYED,
  LISTING,
  COUNT,
  SUM,
  MIN,
  MAX,
  AVG,
  GROUP_BY,
  STD_POP,
  STD_SAMP,
  VAR_POP,
  VAR_SAMP,
  BIT_AND,
  BIT_OR,
  BIT_XOR,
  GROUP_CONCAT
};

// pack data format, stored on disk so only append new ones at the end.
enum class PackFmt : char { DEFAULT, PPM1, PPM2, RANGECODE, LZ4, LOOKUP, NOCOMPRESS, TRIE, ZLIB };

class Tribool {
  // NOTE: in comparisons and assignments use the following three values:
  //
  //  v = true;
  //  v = false;
  //  v = TRIBOOL_UNKNOWN;
  //
  // The last one is an alias (constant) of Tribool() default constructor,
  // which initializes Tribool as the "unknown" value.
  // Do not use the enumerator defined below, it is internal only.

  enum class tribool { TRI_FALSE, TRI_TRUE, TRI_UNKNOWN };

 public:
  Tribool() { v = tribool::TRI_UNKNOWN; }
  Tribool(bool b) { v = (b ? tribool::TRI_TRUE : tribool::TRI_FALSE); }
  bool operator==(Tribool sec) { return v == sec.v; }
  bool operator!=(Tribool sec) { return v != sec.v; }
  const Tribool operator!() {
    return Tribool(v == tribool::TRI_TRUE ? tribool::TRI_FALSE
                                          : (v == tribool::TRI_FALSE ? tribool::TRI_TRUE : tribool::TRI_UNKNOWN));
  }
  static Tribool And(Tribool a, Tribool b) {
    if (a == true && b == true) return true;
    if (a == false || b == false) return false;
    return tribool::TRI_UNKNOWN;
  }
  static Tribool Or(Tribool a, Tribool b) {
    if (a == true || b == true) return true;
    if (a == tribool::TRI_UNKNOWN || b == tribool::TRI_UNKNOWN) return tribool::TRI_UNKNOWN;
    return false;
  }

 private:
  Tribool(tribool b) { v = b; }
  tribool v;
};

const Tribool TRIBOOL_UNKNOWN = Tribool();

union double_int_t {
  double_int_t(const double dd) : d(dd) {}
  double_int_t(const int64_t ii) : i(ii) {}
  double d;
  int64_t i;
};

template <typename base_t, typename T>
std::unique_ptr<base_t> Clone(T const &orig) {
  return std::unique_ptr<base_t>(new T((T &)*orig));
}

inline size_t rows2packs(size_t rows, size_t pss) { return ((rows % (1 << pss)) ? 1 : 0) + (rows / (1 << pss)); }

// Transaction ID is unique for each transaction and grows monotonically.
struct TX_ID final {
  union {
    uint64_t v;
    struct {
      uint32_t v1;
      uint32_t v2;
    };
  };

  TX_ID(uint64_t v = UINT64_MAX) : v(v) {}
  TX_ID(uint32_t v1, uint32_t v2) : v1(v1), v2(v2) {}
  std::string ToString() const;
  bool operator<(const TX_ID &rhs) const { return v < rhs.v; }
};

const TX_ID MAX_XID = std::numeric_limits<uint64_t>::max();

}  // namespace common
}  // namespace stonedb

#endif  // STONEDB_COMMON_COMMON_DEFINITIONS_H_
