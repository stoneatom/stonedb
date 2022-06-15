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

#include "core/rsi_bloom.h"
#include "core/pack_str.h"
#include "system/rc_system.h"
#include "system/stonedb_file.h"

namespace stonedb {
namespace core {
RSIndex_Bloom::RSIndex_Bloom(const fs::path &dir, common::TX_ID ver) {
  m_path = dir / common::COL_FILTER_BLOOM_DIR;
  auto fpath = dir / common::COL_FILTER_BLOOM_DIR / ver.ToString();

  bloom_filter_policy.reset(NewBloomFilterPolicy(bits_key));

  system::StoneDBFile frs_index;

  if (fs::exists(fpath)) {
    frs_index.OpenReadOnly(fpath);
    frs_index.ReadExact(&hdr, sizeof(hdr));
    ASSERT(hdr.ver == FORMAT_VERSION, "bad bloom filter format");
    ASSERT(fs::file_size(fpath) == (sizeof(HDR) + (hdr.no_pack * sizeof(BF))),
           "bloom filter corrupted: " + fpath.string());
  }

  // allocate more than requested
  capacity = (hdr.no_pack / 1024 + 1) * 1024;
  bloom_buffers = static_cast<BF *>(alloc(capacity * sizeof(BF), mm::BLOCK_TYPE::BLOCK_TEMPORARY));

  if (hdr.no_pack > 0) {
    frs_index.ReadExact(bloom_buffers, hdr.no_pack * sizeof(BF));
  }
}

RSIndex_Bloom::~RSIndex_Bloom() { dealloc(bloom_buffers); }

void RSIndex_Bloom::SaveToFile(common::TX_ID ver) {
  auto fpath = m_path / ver.ToString();
  ASSERT(!fs::exists(fpath), "file already exists: " + fpath.string());
  system::StoneDBFile frs_index;
  frs_index.OpenCreate(fpath);
  frs_index.WriteExact(&hdr, sizeof(hdr));
  frs_index.WriteExact(bloom_buffers, hdr.no_pack * sizeof(BF));
  if (stonedb_sysvar_sync_buffers) {
    frs_index.Flush();
  }
}

common::RSValue RSIndex_Bloom::IsValue(types::BString min_v, types::BString max_v, int pack) {
  if (min_v == max_v) {
    auto &bf = bloom_buffers[pack];
    if (bf.len == 0) {
      // this pack no bloom filter data
      return common::RSValue::RS_SOME;
    }
    Slice key(max_v.val, max_v.size());
    // get filter data
    Slice pack_block(bf.data, bf.len);
    FilterBlockReader reader(bloom_filter_policy.get(), pack_block);
    if (!reader.KeyMayMatch(0, key)) {
      return common::RSValue::RS_NONE;
    }
    return common::RSValue::RS_SOME;
  } else {
    return common::RSValue::RS_SOME;
  }
}

void RSIndex_Bloom::Update(common::PACK_INDEX pi, DPN &dpn, const PackStr *pack) {
  if (pi >= hdr.no_pack) {
    hdr.no_pack = pi + 1;
  }
  if (hdr.no_pack > capacity) {
    capacity += 1024;
    bloom_buffers =
        static_cast<BF *>(rc_realloc(bloom_buffers, capacity * sizeof(BF), mm::BLOCK_TYPE::BLOCK_TEMPORARY));
    //  rclog << lock << "bloom filter capacity increased to " << capacity <<
    //  system::unlock;
  }

  auto bloom_builder = std::make_unique<FilterBlockBuilder>(bloom_filter_policy.get());

  bloom_builder->StartBlock(0);

  for (size_t i = 0; i < dpn.nr; i++)
    if (pack->NotNull(i)) bloom_builder->AddKey(Slice(pack->GetValueBinary(i).ToString()));

  Slice block = bloom_builder->Finish();

  if (block.size() > sizeof(BF) - 4) {
    STONEDB_LOG(LogCtl_Level::WARN, "Bloom len of pack:%d larger than expected", pi);
    bloom_buffers[pi].len = 0;
  } else {
    bloom_buffers[pi].len = block.size();
    std::memcpy(bloom_buffers[pi].data, block.data(), block.size());
  }
}
}  // namespace core
}  // namespace stonedb
