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
#ifndef TIANMU_CORE_RSI_CMAP_H_
#define TIANMU_CORE_RSI_CMAP_H_
#pragma once

#include "common/common_definitions.h"
#include "core/rsi_index.h"
#include "types/tianmu_data_types.h"

namespace Tianmu {
namespace core {

class PackStr;

class RSIndex_CMap final : public RSIndex {
 public:
  RSIndex_CMap(const fs::path &dir, common::TX_ID ver);
  ~RSIndex_CMap();
  void SaveToFile(common::TX_ID ver) override;

  uint NumOfPositions() { return hdr.no_positions; }
  // reset the information about the specified pack and prepare it for update
  // (i.e. analyzing again all values)
  void ClearPack(int pack);

  void PutValue(const types::BString &v,
                int pack);  // set information that value v does exist in this pack
  void Create(int64_t _no_obj, int no_pos, int pss);

  common::RoughSetValue IsValue(types::BString min_v, types::BString max_v, int pack);
  // Results:		common::RoughSetValue::RS_NONE - there is no objects having values
  // between min_v and max_v (including)
  //				common::RoughSetValue::RS_SOME - some objects from this pack do
  // have values
  // between min_v and max_v 				common::RoughSetValue::RS_ALL	- all objects
  // from this pack do have values between min_v and max_v
  common::RoughSetValue IsLike(types::BString pattern, int pack, char escape_character);
  int Count(int pack, uint pos);  // a number of ones in the pack on a given
                                  // position 0..no_positions
  bool IsSet(int pack, unsigned char c, uint pos);
  void Update(common::PACK_INDEX pi, DPN &dpn, const PackStr *pack);

 private:
  uint32_t *PackBuf(int64_t i) { return cmap_buffers + (i * hdr.no_positions * CMAP_INTS); }
  // if there is at least one 1 in [first, last]
  bool IsAnySet(int pack, unsigned char first, unsigned char last, uint pos);

  void Set(int pack, unsigned char c, uint pos);
  void UnSet(int pack, unsigned char c, uint pos);

  static uint32_t const CMAP_BITS = 256;
  static uint32_t const CMAP_BYTES = CMAP_BITS / 8;
  static uint32_t const CMAP_INTS = CMAP_BYTES / sizeof(uint32_t);
  static uint32_t const MAX_POS = 64;
  static uint32_t const FORMAT = 2;

 private:
  struct HDR final {
    int32_t ver = FORMAT;
    uint32_t no_pack;
    uint32_t no_positions = MAX_POS;
  } hdr{};

  uint32_t *cmap_buffers = nullptr;
  size_t capacity = 0;
};

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_RSI_CMAP_H_
