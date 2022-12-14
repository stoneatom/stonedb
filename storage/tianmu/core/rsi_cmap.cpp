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

#include "core/rsi_cmap.h"
#include "core/pack_str.h"
#include "system/tianmu_file.h"
#include "system/tianmu_system.h"
#include "util/fs.h"

namespace Tianmu {
namespace core {

RSIndex_CMap::RSIndex_CMap(const fs::path &dir, common::TX_ID ver) {
  m_path = dir / common::COL_FILTER_CMAP_DIR;
  auto fpath = dir / common::COL_FILTER_CMAP_DIR / ver.ToString();

  if (fs::exists(fpath)) {
    system::TianmuFile frs_index;
    frs_index.OpenReadOnly(fpath);
    frs_index.ReadExact(&hdr, sizeof(hdr));

    ASSERT(hdr.no_positions <= MAX_POS);

    capacity = (hdr.no_pack / 1024 + 1) * 1024;
    auto ptr = alloc(capacity * hdr.no_positions * CMAP_BYTES, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
    cmap_buffers = static_cast<uint32_t *>(ptr);
    frs_index.ReadExact(cmap_buffers, hdr.no_pack * hdr.no_positions * CMAP_BYTES);
  } else {
  }
}

RSIndex_CMap::~RSIndex_CMap() { dealloc(cmap_buffers); }

void RSIndex_CMap::Create([[maybe_unused]] int64_t _no_obj, int no_pos, [[maybe_unused]] int pss) {
  hdr.no_positions = no_pos;
}

void RSIndex_CMap::ClearPack(int pack) {
  ASSERT(size_t(pack) < hdr.no_pack);

  if (cmap_buffers != nullptr)
    std::memset(PackBuf(pack), 0, hdr.no_positions * CMAP_BYTES);
}

void RSIndex_CMap::SaveToFile(common::TX_ID ver) {
  auto fpath = m_path / ver.ToString();
  ASSERT(!fs::exists(fpath), "file already exists: " + fpath.string());

  system::TianmuFile frs_index;
  frs_index.OpenCreate(fpath);
  frs_index.WriteExact(&hdr, sizeof(hdr));

  if (cmap_buffers != nullptr) {
    frs_index.WriteExact(cmap_buffers, hdr.no_pack * hdr.no_positions * CMAP_BYTES);
  }

  if (tianmu_sysvar_sync_buffers) {
    frs_index.Flush();
  }
}

int RSIndex_CMap::Count(int pack, uint pos) {
  ASSERT(size_t(pack) < hdr.no_pack);

  int d = 0;
  uint *packBeg(PackBuf(pack) + pos * CMAP_INTS);
  for (uint i = 0; i < CMAP_INTS; i++) {
    d += CalculateBinSum(packBeg[i]);
  }

  return d;
}

// Results:		common::RoughSetValue::RS_NONE - there is no objects having values
// between min_v and max_v (including)
//				common::RoughSetValue::RS_SOME - some objects from this pack
// may have
// values between min_v and max_v 				common::RoughSetValue::RS_ALL	-
// all objects from this pack do have values between min_v and max_v
common::RoughSetValue RSIndex_CMap::IsValue(types::BString min_v, types::BString max_v, int pack) {
  ASSERT(size_t(pack) < hdr.no_pack);

  if (min_v == max_v) {
    auto size = min_v.size() < hdr.no_positions ? min_v.size() : hdr.no_positions;
    for (uint pos = 0; pos < size; pos++) {
      if (!IsSet(pack, (unsigned char)min_v[pos], pos))
        return common::RoughSetValue::RS_NONE;
    }

    return common::RoughSetValue::RS_SOME;
  } else {
    // TODO: may be further optimized
    unsigned char f = 0, l = 255;
    if (min_v.len_ > 0)
      f = (unsigned char)min_v[0];  // min_v.len == 0 usually means -inf

    if (max_v.len_ > 0)
      l = (unsigned char)max_v[0];

    if (f > l || !IsAnySet(pack, f, l, 0))
      return common::RoughSetValue::RS_NONE;

    return common::RoughSetValue::RS_SOME;
  }
}

common::RoughSetValue RSIndex_CMap::IsLike(types::BString pattern, int pack, char escape_character) {
  // we can exclude cases: "ala%..." and "a_l_a%..."
  char *p = pattern.val_;  // just for short...
  uint pos = 0;
  while (pos < pattern.len_ && pos < hdr.no_positions) {
    if (p[pos] == '%' || p[pos] == escape_character)
      break;

    if (p[pos] != '_' && !IsSet(pack, p[pos], pos))
      return common::RoughSetValue::RS_NONE;
    pos++;
  }

  return common::RoughSetValue::RS_SOME;
}

void RSIndex_CMap::PutValue(const types::BString &v, int pack) {
  if (v.IsNullOrEmpty())
    return;

  auto size = v.size() < hdr.no_positions ? v.size() : hdr.no_positions;

  ASSERT(size_t(pack) < hdr.no_pack);
  for (uint i = 0; i < size; i++) {
    Set(pack, (unsigned char)v[i], i);
  }
}

bool RSIndex_CMap::IsSet(int pack, unsigned char c, uint pos) {
  ASSERT(cmap_buffers != nullptr);
  ASSERT(size_t(pack) < hdr.no_pack);
  return ((PackBuf(pack)[pos * CMAP_INTS + c / CMAP_BYTES] >> (c % CMAP_BYTES)) % 2 == 1);
}

// true, if there is at least one 1 in [first, last]
bool RSIndex_CMap::IsAnySet(int pack, unsigned char first, unsigned char last, uint pos) {
  ASSERT(first <= last);
  for (int c = first; c <= last; c++)
    if (IsSet(pack, c, pos))
      return true;

  return false;
}

void RSIndex_CMap::Set(int pack, unsigned char c, uint pos) {
  ASSERT(size_t(pack) < hdr.no_pack);
  PackBuf(pack)[pos * CMAP_INTS + c / CMAP_BYTES] |= (0x00000001u << (c % CMAP_BYTES));
}

void RSIndex_CMap::Update(common::PACK_INDEX pi, DPN &dpn, const PackStr *pack) {
  if (pi >= hdr.no_pack) {
    hdr.no_pack = pi + 1;
  }

  if (hdr.no_pack > capacity) {
    capacity += 1024;
    auto ptr = rc_realloc(cmap_buffers, capacity * hdr.no_positions * CMAP_BYTES, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
    cmap_buffers = static_cast<uint32_t *>(ptr);
    // rclog << lock << "cmap filter capacity increased to " << capacity <<
    // system::unlock;
  }

  ClearPack(pi);
  for (size_t i = 0; i < dpn.numOfRecords; i++)
    if (pack->NotNull(i)) {
      if (dpn.Trivial() || pack->IsNull(i))
        continue;

      types::BString str(pack->GetValueBinary(i));
      if (str.size() > 0)
        PutValue(str, pi);
    }
}

}  // namespace core
}  // namespace Tianmu
