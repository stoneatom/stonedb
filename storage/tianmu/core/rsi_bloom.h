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
#ifndef TIANMU_CORE_RSI_BLOOM_H_
#define TIANMU_CORE_RSI_BLOOM_H_
#pragma once

#include "common/common_definitions.h"
#include "core/bloom_block.h"
#include "core/rsi_index.h"
#include "types/tianmu_data_types.h"

namespace Tianmu {
namespace core {

class PackStr;

class RSIndex_Bloom final : public RSIndex {
 public:
  RSIndex_Bloom(const fs::path &dir, common::TX_ID ver);
  ~RSIndex_Bloom();

  void SaveToFile(common::TX_ID ver) override;
  void Update(common::PACK_INDEX pi, DPN &dpn, const PackStr *pack);
  common::RoughSetValue IsValue(types::BString min_v, types::BString max_v, int pack);

 private:
  static const int FORMAT_VERSION = 2;
  static const int bits_key = 7;

  struct HDR final {
    int32_t ver = FORMAT_VERSION;
    uint32_t no_pack;
  } hdr{};

  // bloom filter
  struct BF final {
    uint32_t len;
    char data[64 * 1024 - sizeof(len)];
  };

  BF *bloom_buffers = nullptr;
  size_t capacity = 0;
  std::unique_ptr<const FilterPolicy> bloom_filter_policy;
};

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_RSI_BLOOM_H_
