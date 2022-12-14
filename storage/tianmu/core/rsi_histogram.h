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
#ifndef TIANMU_CORE_RSI_HISTOGRAM_H_
#define TIANMU_CORE_RSI_HISTOGRAM_H_
#pragma once

#include "common/common_definitions.h"
#include "core/pack_int.h"
#include "core/rsi_index.h"

namespace Tianmu {
namespace core {

class RSIndex_Hist final : public RSIndex {
 public:
  RSIndex_Hist(const fs::path &dir, common::TX_ID ver);
  ~RSIndex_Hist();

  void Init(int64_t _no_obj, bool _fixed, int pss);

  void Update(common::PACK_INDEX pi, DPN &dpn, const PackInt *pack);

  void ClearPack(int pack);  // reset the information about the specified pack

  // reading histogram information
  // Note: this function is thread-safe, i.e. it only reads the data
  common::RoughSetValue IsValue(int64_t min_v, int64_t max_v, int pack, int64_t pack_min, int64_t pack_max);

  //  Return true if there is any common value possible for two packs from two
  //  different columns (histograms).
  // Results:     common::RoughSetValue::RS_NONE - there is no objects having values between
  // min_v and max_v (including)
  //              common::RoughSetValue::RS_SOME - some objects from this pack do have values
  //              between min_v and max_v common::RoughSetValue::RS_ALL  - all objects from
  //              this pack do have values between min_v and max_v
  bool Intersection(int pack, int64_t pack_min, int64_t pack_max, RSIndex_Hist *sec, int pack2, int64_t pack_min2,
                    int64_t pack_max2);

  // number of ones in the pack (up to "width" bits)
  int Count(int pack, int width);

  // if the histogram provides exact information, not just interval-based
  bool ExactMode(int64_t pack_min, int64_t pack_max) { return (uint64_t(pack_max - pack_min) <= (RSI_HIST_BITS - 2)); }

  void SaveToFile(common::TX_ID ver) override;

  bool Fixed() { return hdr.fixed == 1; };

 private:
  void AppendKNs(unsigned int no_kns_to_add);  // append new KNs
  void Alloc(uint64_t no_packs);
  void Deallocate();

  // ignore packs with extreme values common::PLUS_INF_64 - 1; just compatible
  // with old HIST file
  bool IntervalTooLarge(int64_t pack_min, int64_t pack_max) {
    return static_cast<uint64_t>(pack_max - pack_min) > common::PLUS_INF_64 - 1;
  }

 private:
  static const int RSI_HIST_BITS = 1024;
  static const int RSI_HIST_INT_RES = RSI_HIST_BITS / 64;  // use 64-bit int
  static const int FORMAT_VERSION = 2;

  struct BLOCK final {
    int64_t data[RSI_HIST_INT_RES];
  };

  struct HDR final {
    int32_t ver = FORMAT_VERSION;
    int32_t fixed;
    uint64_t no_pack;
  } hdr{};

  BLOCK *hist_buffers = nullptr;
  size_t capacity = 0;
};

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_RSI_HISTOGRAM_H_
