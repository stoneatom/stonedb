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
#ifndef TIANMU_CORE_DPN_H_
#define TIANMU_CORE_DPN_H_
#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>

#include "common/common_definitions.h"

namespace Tianmu {
namespace core {
constexpr uint64_t tag_one = (1L << 48);
constexpr uint64_t tag_mask = 0x0000FFFFFFFFFFFF;
constexpr uint64_t loading_flag = -1;
constexpr uint64_t max_ref_counter = 1UL << 63;

// Data Pack Node. Same layout on disk and in memory
struct DPN final {
 public:
  uint8_t used : 1;    // occupied or not
  uint8_t local : 1;   // owned by a write transaction, thus to-be-commit
  uint8_t synced : 1;  // if the pack data in memory is up to date with the
                       // version on disk
  uint8_t null_compressed : 1;
  uint8_t delete_compressed : 1;
  uint8_t data_compressed : 1;
  uint8_t no_compress : 1;
  uint8_t paddingBit : 1;  // Memory aligned padding has no practical effect
  uint8_t padding[7];      // Memory aligned padding has no practical effect

  uint32_t base;          // index of the DPN from which we copied, used by local pack
  uint32_t numOfRecords;  // number of records
  uint32_t numOfNulls;    // number of nulls
  uint32_t numOfDeleted;  // number of deleted

  uint64_t dataAddress;  // data start address
  uint64_t dataLength;   // data length

  common::TX_ID xmin;  // creation trx id
  common::TX_ID xmax;  // delete trx id
  union {
    int64_t min_i;
    double min_d;
    char min_s[8];
  };
  union {
    int64_t max_i;
    double max_d;
    char max_s[8];
  };
  union {
    int64_t sum_i;
    double sum_d;
    uint64_t maxlen;
  };

 private:
  // a tagged pointer, 16 bits as ref count.
  // Only read-only dpn uses it for ref counting; local dpn is managed only by
  // one write session
  std::atomic_ulong tagged_ptr;

 public:
  bool CAS(uint64_t &expected, uint64_t desired) { return tagged_ptr.compare_exchange_weak(expected, desired); }
  uint64_t GetPackPtr() const { return tagged_ptr.load(); }
  void SetPackPtr(uint64_t v) { tagged_ptr.store(v); }
  /*
    Because the delete bitmap is in the pack,
    when there are deleted records in the pack,
    the package must be stored persistently.
  */
  bool Trivial() const { return (Uniform() || NullOnly()) && numOfDeleted == 0; }
  bool NotTrivial() const { return !Trivial(); }
  bool Uniform() const {
    return numOfNulls == 0 && min_i == max_i;
  }  // for packN, all records are the same and not null
  bool NullOnly() const { return numOfRecords == numOfNulls; }
  bool IsLocal() const { return local == 1; }
  void SetLocal(bool v) { local = v; }

  bool IncRef() {
    auto v = tagged_ptr.load();
    while (v != 0 && v != loading_flag)
      if (tagged_ptr.compare_exchange_weak(v, v + tag_one))
        return true;
    return false;
  }

  // as we are not POD with the atomic long
  DPN() { reset(); }
  DPN(const DPN &dpn) { std::memcpy(this, &dpn, sizeof(DPN)); }
  DPN &operator=(const DPN &dpn) {
    std::memcpy(this, &dpn, sizeof(DPN));
    return *this;
  }
  void reset() { std::memset(this, 0, sizeof(DPN)); }
};

const uint64_t DPN_INVALID_ADDR = -1;
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_DPN_H_
