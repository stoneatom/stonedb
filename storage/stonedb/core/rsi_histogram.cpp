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

#include <limits>

#include "core/pack.h"
#include "core/rsi_histogram.h"
#include "system/rc_system.h"
#include "system/stonedb_file.h"

namespace stonedb {
namespace core {
RSIndex_Hist::RSIndex_Hist(const fs::path &dir, common::TX_ID ver) {
  m_path = dir / common::COL_FILTER_HIST_DIR;
  auto fpath = dir / common::COL_FILTER_HIST_DIR / ver.ToString();

  system::StoneDBFile frs_index;

  if (fs::exists(fpath)) {
    frs_index.OpenReadOnly(fpath);
    frs_index.ReadExact(&hdr, sizeof(hdr));
    ASSERT(fs::file_size(fpath) == (sizeof(HDR) + (hdr.no_pack * sizeof(BLOCK))),
           "hist filter corrupted: " + fpath.string());
  }

  ASSERT(hdr.ver == FORMAT_VERSION, "bad hist filter format");

  // allocate more than requested
  capacity = (hdr.no_pack / 1024 + 1) * 1024;
  hist_buffers = static_cast<BLOCK *>(alloc(capacity * sizeof(BLOCK), mm::BLOCK_TYPE::BLOCK_TEMPORARY));
  if (hdr.no_pack > 0) {
    frs_index.ReadExact(hist_buffers, hdr.no_pack * sizeof(BLOCK));
  }
}

RSIndex_Hist::~RSIndex_Hist() {
  dealloc(hist_buffers);
  capacity = 0;
}

void RSIndex_Hist::SaveToFile(common::TX_ID ver) {
  auto fpath = m_path / ver.ToString();
  ASSERT(!fs::exists(fpath), "file already exists: " + fpath.string());

  system::StoneDBFile frs_index;
  frs_index.OpenCreate(fpath);
  frs_index.WriteExact(&hdr, sizeof(hdr));
  frs_index.WriteExact(hist_buffers, hdr.no_pack * sizeof(BLOCK));
  if (stonedb_sysvar_sync_buffers) {
    frs_index.Flush();
  }
}

int RSIndex_Hist::Count(int pack, int width) {
  ASSERT(size_t(pack) < hdr.no_pack);

  int d = 0;

  BLOCK &hist = hist_buffers[pack];

  int stop_int;
  if (width >= RSI_HIST_BITS)
    stop_int = RSI_HIST_INT_RES;  // normal mode
  else
    stop_int = width / 64;  // exact mode

  ASSERT(stop_int <= RSI_HIST_INT_RES);

  for (int i = 0; i < stop_int; i++) d += CalculateBinSum(hist.data[i]);

  int bits_left = width % 64;
  if (stop_int < RSI_HIST_INT_RES && bits_left > 0)
    d += CalculateBinSum(hist.data[stop_int] & ~((uint64_t)-1 << bits_left));
  return d;
}

// Results:		common::RSValue::RS_NONE - there is no objects having values
// between min_v and max_v (including)
//				common::RSValue::RS_SOME - some objects from this pack
// may have
// values between min_v and max_v 				common::RSValue::RS_ALL	-
// all objects from this pack do have values between min_v and max_v
common::RSValue RSIndex_Hist::IsValue(int64_t min_v, int64_t max_v, int pack, int64_t pack_min, int64_t pack_max) {
  ASSERT(size_t(pack) < hdr.no_pack, std::to_string(pack) + " < " + std::to_string(hdr.no_pack));

  if (IntervalTooLarge(pack_min, pack_max) || IntervalTooLarge(min_v, max_v)) return common::RSValue::RS_SOME;
  int min_bit = 0, max_bit = 0;
  if (!Fixed()) {  // floating point
    double dmin_v = *(double *)(&min_v);
    double dmax_v = *(double *)(&max_v);
    double dpack_min = *(double *)(&pack_min);
    double dpack_max = *(double *)(&pack_max);
    DEBUG_ASSERT(dmin_v <= dmax_v);
    if (dmax_v < dpack_min || dmin_v > dpack_max) return common::RSValue::RS_NONE;
    if (dmax_v >= dpack_max && dmin_v <= dpack_min) return common::RSValue::RS_ALL;
    if (dmax_v >= dpack_max || dmin_v <= dpack_min)
      return common::RSValue::RS_SOME;  // pack_min xor pack_max are present
    // now we know that (max_v<pack_max) and (min_v>pack_min) and there is only
    // common::RSValue::RS_SOME or common::RSValue::RS_NONE answer possible
    double interval_len = (dpack_max - dpack_min) / double(RSI_HIST_BITS);
    min_bit = int((dmin_v - dpack_min) / interval_len);
    max_bit = int((dmax_v - dpack_min) / interval_len);
  } else {
    DEBUG_ASSERT(min_v <= max_v);
    if (max_v < pack_min || min_v > pack_max) return common::RSValue::RS_NONE;
    if (max_v >= pack_max && min_v <= pack_min) return common::RSValue::RS_ALL;
    if (max_v >= pack_max || min_v <= pack_min) return common::RSValue::RS_SOME;  // pack_min xor pack_max are present
    // now we know that (max_v<pack_max) and (min_v>pack_min) and there is only
    // common::RSValue::RS_SOME or common::RSValue::RS_NONE answer possible
    if (ExactMode(pack_min, pack_max)) {    // exact mode
      min_bit = int(min_v - pack_min - 1);  // translate into [0,...]
      max_bit = int(max_v - pack_min - 1);
    } else {  // interval mode
      double interval_len = (pack_max - pack_min - 1) / double(RSI_HIST_BITS);
      min_bit = int((min_v - pack_min - 1) / interval_len);
      max_bit = int((max_v - pack_min - 1) / interval_len);
    }
  }
  DEBUG_ASSERT(min_bit >= 0);
  if (max_bit >= RSI_HIST_BITS)
    return common::RSValue::RS_SOME;  // it may happen for extremely large numbers (
                                      // >2^52 )
  for (int i = min_bit; i <= max_bit; i++) {
    if (((*(hist_buffers[pack].data + i / 64) >> (i % 64)) & 0x00000001) != 0) return common::RSValue::RS_SOME;
  }
  return common::RSValue::RS_NONE;
}

bool RSIndex_Hist::Intersection(int pack, int64_t pack_min, int64_t pack_max, RSIndex_Hist *sec, int pack2,
                                int64_t pack_min2, int64_t pack_max2) {
  // we may assume that min-max of packs was already checked
  if (!Fixed() || !sec->Fixed()) return true;  // not implemented - intersection possible
  if (IntervalTooLarge(pack_min, pack_max) || IntervalTooLarge(pack_min2, pack_max2)) return true;

  if (sec->IsValue(pack_min, pack_min, pack2, pack_min2, pack_max2) != common::RSValue::RS_NONE ||
      sec->IsValue(pack_max, pack_max, pack2, pack_min2, pack_max2) != common::RSValue::RS_NONE ||
      IsValue(pack_min2, pack_min2, pack, pack_min, pack_max) != common::RSValue::RS_NONE ||
      IsValue(pack_max2, pack_max2, pack, pack_min, pack_max) != common::RSValue::RS_NONE)
    return true;  // intersection found (extreme values)

  if (ExactMode(pack_min, pack_max) && ExactMode(pack_min2, pack_max2)) {  // exact mode
    int bit1, bit2;
    int64_t min_v = (pack_min < pack_min2 ? pack_min2 + 1 : pack_min + 1);  // these values are possible
    int64_t max_v = (pack_max > pack_max2 ? pack_max2 - 1 : pack_max - 1);
    for (int64_t v = min_v; v <= max_v; v++) {
      bit1 = int(v - pack_min - 1);
      bit2 = int(v - pack_min2 - 1);
      if ((((*(hist_buffers[pack].data + bit1 / 64) >> (bit1 % 64)) & 0x00000001) != 0) &&
          (((*(hist_buffers[pack].data + bit2 / 64) >> (bit2 % 64)) & 0x00000001) != 0))
        return true;  // intersection found
    }
    return false;  // no intersection possible - all values do not math the
                   // second histogram
  }
  // TODO: all other possibilities, not only the exact cases
  return true;  // we cannot foreclose intersecting
}

void RSIndex_Hist::Update(common::PACK_INDEX pi, DPN &dpn, const PackInt *pack) {
  if (pi >= hdr.no_pack) {
    hdr.no_pack = pi + 1;
  }
  if (hdr.no_pack > capacity) {
    capacity += 1024;
    hist_buffers =
        static_cast<BLOCK *>(rc_realloc(hist_buffers, capacity * sizeof(BLOCK), mm::BLOCK_TYPE::BLOCK_TEMPORARY));
    // rclog << lock << "hist filter capacity increased to " << capacity <<
    // system::unlock;
  }

  hist_buffers[pi] = {};  // all zeros

  if (dpn.Trivial())  // For trivial DataPack hist bitmap is reserved as all
                      // zero.
    return;

  hdr.fixed = pack->IsFixed();

  if (IntervalTooLarge(dpn.min_i, dpn.max_i)) return;

  double interval_len;
  if (Fixed()) {
    interval_len = (dpn.max_i - dpn.min_i) / double(RSI_HIST_BITS);
  } else {
    interval_len = (dpn.max_d - dpn.min_d) / double(RSI_HIST_BITS);
  }

  for (size_t i = 0; i < dpn.nr; i++) {
    if (pack->IsNull(i)) continue;

    auto v = pack->GetValInt(i);
    int bit = -1;
    if (!Fixed()) {
      double dv = *(double *)(&v);
      ASSERT(dv >= dpn.min_d && dv <= dpn.max_d);
      if (dv == dpn.min_d || dv == dpn.max_d) continue;
      bit = int((dv - dpn.min_d) / interval_len);
    } else {
      ASSERT(v >= 0 && v + dpn.min_i <= dpn.max_i);
      if (v == 0 || v == dpn.max_i - dpn.min_i) continue;
      if (ExactMode(dpn.min_i, dpn.max_i)) {  // exact mode
        bit = int(v - 1);                     // translate into [0,...]
      } else {                                // interval mode
        bit = int(uint64_t(v - 1) / interval_len);
      }
    }
    ASSERT(bit >= 0, "Invalid bit index: " + std::to_string(bit));
    if (bit >= RSI_HIST_BITS) return;  // it may happen for extremely large numbers ( >2^52 )
    hist_buffers[pi].data[bit / 64] |= (0x00000001ul << (bit % 64));
  }
}
}  // namespace core
}  // namespace stonedb
