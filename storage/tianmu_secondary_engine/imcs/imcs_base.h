/* Copyright (c) 2018, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
 The basic common definitions, which were used by imcs, were defined here.
 Created 2/2/2023 Hom.LEE
*/

#ifndef __IMCS_BASE_H__
#define __IMCS_BASE_H__

#include <cstdint>
#include <limits>

namespace Tianmu {
namespace IMCS {

// the total size of imcs, which is a percentage of total memory.
constexpr unsigned long long IMCS_SIZE_PCT = 0.4;

// this value should not to set to much, the bigger we set, the more memory
// consumed.
constexpr unsigned int MAX_CHUNK_SIZE = 48;

// how many rows in a bucket.
constexpr unsigned int MAX_VECTOR_SIZE = 65535;

// how many rows rows in a tile.
constexpr unsigned int MAX_TILE_SIZE = 1024;

// row max length
constexpr unsigned int MAX_ROW_LENGTH = 65535;

// cell max length
constexpr unsigned int MAX_CELL_LENGTH = 1024;

// how many buckets in a chunk, it's a constant
constexpr unsigned int CHUNK_SIZE = 500;

// The memory size allocated by an IMCU
constexpr unsigned int DEFAULT_IMCU_SIZE = 5 << 20;  // 5MB

// the minimal number of cells in a bucket
constexpr unsigned int MIN_BUCKET_SIZE = 20;

using uint32 = uint32_t;
using int32 = int32_t;
using uint = uint32;

using uint64 = uint64_t;
using int64 = int64_t;

using uint8 = uint8_t;
using int8 = int8_t;

using uchar = unsigned char;

// for database object id.
using TableID = unsigned int;

// Column ID.
using ColumnID = unsigned int;

// Transaction ID. 7 bytes len read from innodb.
using TrxID = unsigned long long int;

// Row ID, 6 bytes len, which is read from innodb.
using RowID = uint32_t;

// Log sequence number.
using LSN = TrxID;

using hash_t = uint64;

enum class STATISTICS_TYPE : uint8 {
  BASE = 0,

  // using histogram algr.
  HIST,
  // CAMP algr for string.
  CAMP,
  END
};

// return code for imcs.
enum class IMCS_STATUS : int8 {
  // done with success.
  SUCCESS = 0,
  // done with failure.
  FAILED
};

// to indicate which stage imcs in.
enum class STAGE : int8 {
  // intial stage. do initiialization in next step
  INITIAL = 0,
  // do iitialization,
  INITIALIZATION,
  // intialization with sucess, ready to go.
  READY,
  // starting to run imcs.
  STARTING,
  // start with success, running.
  RUNNING,
  // shutting down.
  SHUTTING_DOWN,
  // has been halted
  HALTED,
  // in [fast] reconvery stage.
  RECOVERY,
  // fast starting.
  FAST_STARTING
};

// to describe which compression algr imcs uses
enum class COMPRESS_TYPE : int8 { UNKNOWN = 0, ZLIB, ZSTD };

constexpr TrxID TRX_ID_LOW_LIMIT = 0ll;
constexpr TrxID TRX_ID_UP_LIMIT =
    std::numeric_limits<unsigned long long int>::max();

constexpr unsigned long DEFUALT_RAPID_SVR_MEMORY_SIZE = 1 >> 31;
constexpr unsigned long MAX_RAPID_SVR_MEMORY_SIZE = 1 >> 31;

}  // namespace IMCS
}  // namespace Tianmu

#endif  //__IMCS_BASE_H__
