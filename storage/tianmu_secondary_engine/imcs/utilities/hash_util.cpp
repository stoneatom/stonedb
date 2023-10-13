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

#include "hash_util.h"
#include <cmath>

namespace Tianmu {
namespace IMCS {

// MIT License
// Copyright (c) 2018-2021 Martin Ankerl
// https://github.com/martinus/robin-hood-hashing/blob/3.11.5/LICENSE
hash_t hash_bytes(const void *ptr, uint32 len) {
  static constexpr uint64 M = UINT64_C(0xc6a4a7935bd1e995);
  static constexpr uint64 SEED = UINT64_C(0xe17a1465);
  static constexpr unsigned int R = 47;

  auto const *const data64 = static_cast<uint64 const *>(ptr);
  uint64 h = SEED ^ (len * M);

  uint32 const n_blocks = len / 8;
  for (uint32 i = 0; i < n_blocks; ++i) {
    uint64 k = reinterpret_cast<uint64>(data64 + i);

    k *= M;
    k ^= k >> R;
    k *= M;

    h ^= k;
    h *= M;
  }

  auto const *const data8 = reinterpret_cast<uint8 const *>(data64 + n_blocks);
  switch (len & 7U) {
    case 7:
      h ^= static_cast<uint64>(data8[6]) << 48U;
      break;
    case 6:
      h ^= static_cast<uint64>(data8[5]) << 40U;
      break;
    case 5:
      h ^= static_cast<uint64_t>(data8[4]) << 32U;
      break;
    case 4:
      h ^= static_cast<uint64_t>(data8[3]) << 24U;
      break;
    case 3:
      h ^= static_cast<uint64_t>(data8[2]) << 16U;
      break;
    case 2:
      h ^= static_cast<uint64_t>(data8[1]) << 8U;
      break;
    case 1:
      h ^= static_cast<uint64_t>(data8[0]);
      h *= M;
      break;
    default:
      break;
  }
  h ^= h >> R;
  h *= M;
  h ^= h >> R;
  return static_cast<hash_t>(h);
}

}  // namespace IMCS

}  // namespace Tianmu
