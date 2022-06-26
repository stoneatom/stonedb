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
#include "pack_str.h"

#include <algorithm>

#include "zlib.h"

#include "compress/bit_stream_compressor.h"
#include "compress/lz4.h"
#include "compress/num_compressor.h"
#include "compress/part_dict.h"
#include "compress/text_compressor.h"
#include "core/bin_tools.h"
#include "core/column_share.h"
#include "core/tools.h"
#include "core/value.h"
#include "loader/value_cache.h"
#include "mm/mm_guard.h"
#include "system/stonedb_file.h"
#include "system/stream.h"
#include "system/txt_utils.h"

namespace stonedb {
namespace core {
PackStr::PackStr(DPN *dpn, PackCoordinate pc, ColumnShare *s) : Pack(dpn, pc, s) {
  auto t = s->ColType().GetTypeName();

  if (t == common::CT::BIN || t == common::CT::LONGTEXT)
    data.len_mode = sizeof(uint32_t);
  else
    data.len_mode = sizeof(uint16_t);

  try {
    data.index = (char **)alloc(sizeof(char *) * (1 << s->pss), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    data.lens = alloc((data.len_mode * (1 << s->pss)), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    std::memset(data.lens, 0, data.len_mode * (1 << s->pss));

    if (!dpn->NullOnly()) {
      system::StoneDBFile f;
      f.OpenReadOnly(s->DataFile());
      f.Seek(dpn->addr, SEEK_SET);
      LoadDataFromFile(&f);
    }
  } catch (...) {
    Destroy();
    throw;
  }
}

PackStr::PackStr(const PackStr &aps, const PackCoordinate &pc) : Pack(aps, pc) {
  try {
    data.len_mode = aps.data.len_mode;
    data.lens = alloc((data.len_mode * (1 << s->pss)), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    std::memset(data.lens, 0, data.len_mode * (1 << s->pss));
    data.index = (char **)alloc(sizeof(char *) * (1 << s->pss), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);

    data.sum_len = aps.data.sum_len;
    data.v.push_back({(char *)alloc(data.sum_len + 1, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED), data.sum_len, 0});

    for (uint i = 0; i < aps.dpn->nr; i++) {
      if (aps.IsNull(i)) {
        SetPtr(i, nullptr);
        continue;
      }
      auto value = aps.GetValueBinary(i);
      auto size = value.size();
      if (size != 0) {
        SetPtrSize(i, Put(value.GetDataBytesPointer(), size), size);
      } else {
        SetPtrSize(i, nullptr, 0);
      }
    }
  } catch (...) {
    Destroy();
    throw;
  }
}

std::unique_ptr<Pack> PackStr::Clone(const PackCoordinate &pc) const {
  return std::unique_ptr<PackStr>(new PackStr(*this, pc));
}

void PackStr::LoadDataFromFile(system::Stream *f) {
  FunctionExecutor fe([this]() { Lock(); }, [this]() { Unlock(); });

  if (IsModeNoCompression()) {
    LoadUncompressed(f);
  } else if (s->ColType().GetFmt() == common::PackFmt::TRIE) {
    LoadCompressedTrie(f);
  } else {
    LoadCompressed(f);
  }
}

void PackStr::Destroy() {
  if (state_ == PackStrtate::PACK_ARRAY) {
    for (auto &it : data.v) {
      dealloc(it.ptr);
    }
  } else {
    marisa_trie_.clear();
    compressed_data_.reset(nullptr);
  }
  dealloc(data.index);
  data.index = nullptr;
  dealloc(data.lens);
  data.lens = nullptr;
  Instance()->AssertNoLeak(this);
}

void PackStr::SetMinS(const types::BString &str) {
  if (types::RequiresUTFConversions(s->ColType().GetCollation())) {
    int useful_len = str.RoundUpTo8Bytes(s->ColType().GetCollation());
    str.CopyTo(dpn->min_s, useful_len);
    if (useful_len < 8) dpn->min_s[useful_len] = 0;
  } else
    str.CopyTo(dpn->min_s, sizeof(dpn->min_s));
}

void PackStr::SetMaxS(const types::BString &str) {
  if (types::RequiresUTFConversions(s->ColType().GetCollation())) {
    int useful_len = str.RoundUpTo8Bytes(s->ColType().GetCollation());
    str.CopyTo(dpn->max_s, useful_len);
    if (useful_len < 8) dpn->max_s[useful_len] = 0;
  } else
    str.CopyTo(dpn->max_s, std::min(str.size(), sizeof(dpn->max_s)));
}

size_t PackStr::CalculateMaxLen() const {
  if (data.len_mode == sizeof(ushort))
    return *std::max_element(data.lens16, data.lens16 + dpn->nr);
  else
    return *std::max_element(data.lens32, data.lens32 + dpn->nr);
}

void PackStr::TransformIntoArray() {
  if (state_ == PackStrtate::PACK_ARRAY) return;
  data.lens = alloc((data.len_mode * (1 << s->pss)), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
  data.index = (char **)alloc(sizeof(char *) * (1 << s->pss), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);

  data.v.push_back({(char *)alloc(data.sum_len + 1, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED), data.sum_len, 0});

  for (uint i = 0; i < dpn->nr; i++) {
    if (IsNull(i)) {
      SetPtr(i, nullptr);
      continue;
    }
    auto value = GetValueBinary(i);
    auto size = value.size();
    if (size != 0) {
      SetPtrSize(i, Put(value.GetDataBytesPointer(), size), size);
    } else {
      SetPtrSize(i, nullptr, 0);
    }
  }
  state_ = PackStrtate::PACK_ARRAY;
}

void PackStr::UpdateValue(size_t i, const Value &v) {
  TransformIntoArray();
  dpn->synced = false;

  if (IsNull(i)) {
    // update null to non-null

    // first non-null value?
    if (dpn->NullOnly()) {
      dpn->max_i = -1;
    }

    ASSERT(v.HasValue());
    UnsetNull(i);
    dpn->nn--;
    auto &str = v.GetString();
    if (str.size() == 0) {
      // we don't need to copy any data
      SetPtrSize(i, nullptr, 0);
      return;
    }
    SetPtrSize(i, Put(str.data(), str.size()), str.size());
    data.sum_len += str.size();
  } else {
    // update an original non-null value

    if (!v.HasValue()) {
      // update non-null to null
      data.sum_len -= GetValueBinary(i).size();
      // note that we do not reclaim any space. The buffers will
      // will be compacted when saving to disk
      SetPtrSize(i, nullptr, 0);
      SetNull(i);
      dpn->nn++;
    } else {
      // update non-null to another nonull
      auto vsize = GetValueBinary(i).size();
      ASSERT(data.sum_len >= vsize);
      data.sum_len -= vsize;
      auto &str = v.GetString();
      if (str.size() <= vsize) {
        // rclog << lock << "     JUST overwrite the original data " <<
        // system::unlock;
        std::memcpy(GetPtr(i), str.data(), str.size());
        SetSize(i, str.size());
      } else {
        SetPtrSize(i, Put(str.data(), str.size()), str.size());
      }
      data.sum_len += str.size();
    }
  }

  dpn->maxlen = CalculateMaxLen();
}

void PackStr::LoadValues(const loader::ValueCache *vc) {
  dpn->synced = false;
  auto sz = vc->SumarizedSize();
  data.v.push_back({(char *)alloc(sz, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED), sz, 0});

  auto total = vc->NumOfValues();

  TransformIntoArray();

  for (uint i = 0; i < total; i++) {
    if (vc->IsNull(i) && s->ColType().Nullable()) {
      SetNull(dpn->nr);
      SetPtr(dpn->nr, nullptr);
      dpn->nr++;
      dpn->nn++;
      continue;
    }

    char const *v = 0;
    uint size = 0;
    if (vc->NotNull(i)) {
      v = vc->GetDataBytesPointer(i);
      size = vc->Size(i);
    }
    AppendValue(v, size);
  }

  // update min/max/maxlen in DPN, if there is non-null values loaded
  if (vc->NumOfValues() > vc->NumOfNulls()) {
    types::BString min_s;
    types::BString max_s;
    uint maxlen;
    vc->CalcStrStats(min_s, max_s, maxlen, s->ColType().GetCollation());

    if (!min_s.GreaterEqThanMinUTF(dpn->min_s, s->ColType().GetCollation(), true)) SetMinS(min_s);

    if (!max_s.LessEqThanMaxUTF(dpn->max_s, s->ColType().GetCollation(), true)) SetMaxS(max_s);

    if (dpn->maxlen < maxlen) dpn->maxlen = maxlen;
  }
}

int PackStr::GetCompressBufferSize(size_t size) {
  int compress_len = 0;
  if (s->ColType().GetFmt() == common::PackFmt::LZ4) {
    compress_len = LZ4_COMPRESSBOUND(size);
  } else if (s->ColType().GetFmt() == common::PackFmt::ZLIB) {
    compress_len = compressBound(size);
  } else {
    compress_len = size;
  }
  // 10 - reserve for header
  return compress_len + 10;
}

std::pair<PackStr::UniquePtr, size_t> PackStr::Compress() {
  uint comp_null_buf_size = 0;

  mm::MMGuard<char> comp_null_buf;
  if (dpn->nn > 0) {
    comp_null_buf_size = ((dpn->nr + 7) / 8);
    comp_null_buf = mm::MMGuard<char>((char *)alloc((comp_null_buf_size + 2), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);

    uint cnbl = comp_null_buf_size + 1;
    comp_null_buf[cnbl] = 0xBA;  // just checking - buffer overrun

    compress::BitstreamCompressor bsc;
    CprsErr res = bsc.Compress(comp_null_buf.get(), comp_null_buf_size, (char *)nulls.get(), dpn->nr, dpn->nn);
    if (comp_null_buf[cnbl] != char(0xBA)) {
      STONEDB_ERROR("buffer overrun by BitstreamCompressor!");
    }
    if (res == CprsErr::CPRS_SUCCESS)
      SetModeNullsCompressed();
    else if (res == CprsErr::CPRS_ERR_BUF) {
      comp_null_buf = mm::MMGuard<char>((char *)nulls.get(), *this, false);
      comp_null_buf_size = ((dpn->nr + 7) / 8);
      ResetModeNullsCompressed();
    } else {
      throw common::InternalException("Compression of nulls failed for column " +
                                      std::to_string(pc_column(GetCoordinate().co.pack) + 1) + ", pack " +
                                      std::to_string(pc_dp(GetCoordinate().co.pack) + 1) + " (error " +
                                      std::to_string(static_cast<int>(res)) + ").");
    }
  }

  mm::MMGuard<uint> nc_buffer((uint *)alloc((1 << s->pss) * sizeof(uint32_t), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);

  int onn = 0;
  uint maxv = 0;
  uint cv = 0;
  for (uint o = 0; o < dpn->nr; o++) {
    if (!IsNull(o)) {
      cv = GetSize(o);
      *(nc_buffer.get() + onn++) = cv;
      if (cv > maxv) maxv = cv;
    }
  }

  size_t comp_len_buf_size;
  mm::MMGuard<uint> comp_len_buf;

  if (maxv != 0) {
    comp_len_buf_size = onn * sizeof(uint) + 28;
    comp_len_buf =
        mm::MMGuard<uint>((uint *)alloc(comp_len_buf_size / 4 * sizeof(uint), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);
    uint tmp_comp_len_buf_size = comp_len_buf_size - 8;
    compress::NumCompressor<uint> nc;
    CprsErr res = nc.Compress((char *)(comp_len_buf.get() + 2), tmp_comp_len_buf_size, nc_buffer.get(), onn, maxv);
    if (res != CprsErr::CPRS_SUCCESS) {
      throw common::InternalException("Compression of lengths of values failed for column " +
                                      std::to_string(pc_column(GetCoordinate().co.pack) + 1) + ", pack " +
                                      std::to_string(pc_dp(GetCoordinate().co.pack) + 1) + " error " +
                                      std::to_string(static_cast<int>(res)));
    }
    comp_len_buf_size = tmp_comp_len_buf_size + 8;
  } else {
    comp_len_buf_size = 8;
    comp_len_buf = mm::MMGuard<uint>((uint *)alloc(sizeof(uint) * 2, mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);
  }

  *comp_len_buf.get() = comp_len_buf_size;
  *(comp_len_buf.get() + 1) = maxv;

  compress::TextCompressor tc;
  int zlo = 0;
  for (uint obj = 0; obj < dpn->nr; obj++)
    if (!IsNull(obj) && GetSize(obj) == 0) zlo++;

  auto dlen = GetCompressBufferSize(data.sum_len);

  mm::MMGuard<char> comp_buf((char *)alloc(dlen, mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);

  if (data.sum_len) {
    int objs = (dpn->nr - dpn->nn) - zlo;

    mm::MMGuard<char *> tmp_index((char **)alloc(objs * sizeof(char *), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);
    mm::MMGuard<uint> tmp_len((uint *)alloc(objs * sizeof(uint), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);

    int nid = 0;
    uint packlen = 0;
    for (int id = 0; id < (int)dpn->nr; id++) {
      if (!IsNull(id) && GetSize(id) != 0) {
        tmp_index[nid] = GetPtr(id);
        tmp_len[nid++] = GetSize(id);
        packlen += GetSize(id);
      }
    }

    CprsErr res = tc.Compress(comp_buf.get(), dlen, tmp_index.get(), tmp_len.get(), objs, packlen,
                              static_cast<int>(s->ColType().GetFmt()));
    if (res != CprsErr::CPRS_SUCCESS) {
      std::stringstream msg_buf;
      msg_buf << "Compression of string values failed for column " << (pc_column(GetCoordinate().co.pack) + 1)
              << ", pack " << (pc_dp(GetCoordinate().co.pack) + 1) << " (error " << static_cast<int>(res) << ").";
      throw common::InternalException(msg_buf.str());
    }

  } else {
    dlen = 0;
  }

  size_t comp_buf_size = (comp_null_buf_size > 0 ? 2 + comp_null_buf_size : 0) + comp_len_buf_size + 4 + 4 + dlen;
  UniquePtr compressed_buf = alloc_ptr(comp_buf_size, mm::BLOCK_TYPE::BLOCK_COMPRESSED);
  uchar *p = reinterpret_cast<uchar *>(compressed_buf.get());

  if (dpn->nn > 0) {
    *((ushort *)p) = (ushort)comp_null_buf_size;
    p += 2;
    std::memcpy(p, comp_null_buf.get(), comp_null_buf_size);
    p += comp_null_buf_size;
  }

  if (comp_len_buf_size) std::memcpy(p, comp_len_buf.get(), comp_len_buf_size);

  p += comp_len_buf_size;

  *((uint32_t *)p) = dlen;
  p += sizeof(uint32_t);
  *((uint32_t *)p) = data.sum_len;
  p += sizeof(uint32_t);
  if (dlen) std::memcpy(p, comp_buf.get(), dlen);

  SetModeDataCompressed();

  return std::make_pair(std::move(compressed_buf), comp_buf_size);
}

void PackStr::CompressTrie() {
  DEBUG_ASSERT(state_ == PackStrtate::PACK_ARRAY);
  marisa::Keyset keyset;
  std::size_t sum_len = 0;
  for (uint row = 0; row < dpn->nr; row++) {
    if (!IsNull(row)) {
      keyset.push_back(GetPtr(row), GetSize(row));
      sum_len += GetSize(row);
    }
  }
  marisa_trie_.clear();
  marisa_trie_.build(keyset);
  auto bufsz = marisa_trie_.io_size() + (dpn->nr * sizeof(unsigned short)) + 8;
  dpn->len = bufsz;
  std::ostringstream oss;
  oss << marisa_trie_;
  compressed_data_ = alloc_ptr(bufsz, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
  char *buf_ptr = (char *)compressed_data_.get();
  std::memcpy(buf_ptr, oss.str().data(), oss.str().length());
  buf_ptr += oss.str().length();
  auto sumlenptr = (std::uint64_t *)buf_ptr;
  *sumlenptr = sum_len;
  buf_ptr += 8;
  unsigned short *ids = (unsigned short *)buf_ptr;
  for (uint row = 0, idx = 0; row < dpn->nr; row++) {
    if (IsNull(row)) {
      ids[row] = 0xffff;
    } else {
      auto id = keyset[idx].id();
      DEBUG_ASSERT(id < (dpn->nr - dpn->nn));
      ids[row] = id;
      idx++;
    }
  }
  ids_array_ = ids;

  SetModeDataCompressed();
  for (auto &it : data.v) {
    dealloc(it.ptr);
  }
  data.v.clear();
  state_ = PackStrtate::PACK_TRIE;
}

void PackStr::Save() {
  UniquePtr compressed_buf;
  if (!ShouldNotCompress()) {
    if (data.sum_len > common::MAX_CMPR_SIZE) {
      STONEDB_LOG(LogCtl_Level::WARN,
                  "pack (%d-%d-%d) size %ld exceeds supported compression "
                  "size, will not be compressed!",
                  pc_table(GetCoordinate().co.pack), pc_column(GetCoordinate().co.pack), pc_dp(GetCoordinate().co.pack),
                  data.sum_len);
      SetModeNoCompression();
      dpn->len = NULLS_SIZE + data.sum_len + (data.len_mode * (1 << s->pss));
    } else if (s->ColType().GetFmt() == common::PackFmt::TRIE) {
      CompressTrie();
    } else {
      auto res = Compress();
      dpn->len = res.second;
      compressed_buf = std::move(res.first);
    }
  } else {
    SetModeNoCompression();
    dpn->len = NULLS_SIZE + data.sum_len + (data.len_mode * (1 << s->pss));
  }
  s->alloc_seg(dpn);
  system::StoneDBFile f;
  f.OpenCreate(s->DataFile());
  f.Seek(dpn->addr, SEEK_SET);
  if (IsModeCompressionApplied()) {
    if (state_ == PackStrtate::PACK_TRIE) {
      f.WriteExact(compressed_data_.get(), dpn->len);
    } else {
      f.WriteExact(compressed_buf.get(), dpn->len);
    }
  } else {
    SaveUncompressed(&f);
  }

  ASSERT(f.Tell() == off_t(dpn->addr + dpn->len),
         std::to_string(dpn->addr) + ":" + std::to_string(dpn->len) + "/" + std::to_string(f.Tell()));
  dpn->synced = true;
}

void PackStr::SaveUncompressed(system::Stream *f) {
  f->WriteExact(nulls.get(), NULLS_SIZE);
  f->WriteExact(data.lens, (data.len_mode * (1 << s->pss)));
  if (data.v.empty()) return;

  std::unique_ptr<char[]> buff(new char[data.sum_len]);
  char *ptr = buff.get();
  for (uint i = 0; i < dpn->nr; i++) {
    if (!IsNull(i)) {
      std::memcpy(ptr, data.index[i], GetSize(i));
      ptr += GetSize(i);
    }
  }
  ASSERT(ptr == buff.get() + data.sum_len,
         "lengh sum: " + std::to_string(data.sum_len) + ", copied " + std::to_string(ptr - buff.get()));
  f->WriteExact(buff.get(), data.sum_len);
}

void PackStr::LoadCompressed(system::Stream *f) {
  ASSERT(IsModeCompressionApplied());

  auto compressed_buf = alloc_ptr(dpn->len + 1, mm::BLOCK_TYPE::BLOCK_COMPRESSED);
  f->ReadExact(compressed_buf.get(), dpn->len);

  dpn->synced = true;

  // if (ATI::IsBinType(s->ColType().GetTypeName())) {
  //    throw common::Exception("Compression format no longer supported.");
  //}

  // uncompress the data
  mm::MMGuard<char *> tmp_index((char **)alloc(dpn->nr * sizeof(char *), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);

  char *cur_buf = reinterpret_cast<char *>(compressed_buf.get());

  char (*FREE_PLACE)(reinterpret_cast<char *>(-1));

  uint null_buf_size = 0;
  if (dpn->nn > 0) {
    null_buf_size = (*(ushort *)cur_buf);
    if (!IsModeNullsCompressed())  // flat null encoding
      std::memcpy(nulls.get(), cur_buf + 2, null_buf_size);
    else {
      compress::BitstreamCompressor bsc;
      CprsErr res = bsc.Decompress((char *)nulls.get(), null_buf_size, cur_buf + 2, dpn->nr, dpn->nn);
      if (res != CprsErr::CPRS_SUCCESS) {
        throw common::DatabaseException("Decompression of nulls failed for column " +
                                        std::to_string(pc_column(GetCoordinate().co.pack) + 1) + ", pack " +
                                        std::to_string(pc_dp(GetCoordinate().co.pack) + 1) + " (error " +
                                        std::to_string(static_cast<int>(res)) + ").");
      }
    }
    cur_buf += (null_buf_size + 2);

    for (uint i = 0; i < dpn->nr; i++) {
      if (IsNull(i))
        tmp_index[i] = nullptr;
      else
        tmp_index[i] = FREE_PLACE;  // special value: an object awaiting decoding
    }
  } else
    for (uint i = 0; i < dpn->nr; i++) tmp_index[i] = FREE_PLACE;

  auto comp_len_buf_size = *(uint32_t *)cur_buf;
  auto maxv = *(uint32_t *)(cur_buf + 4);

  if (maxv != 0) {
    compress::NumCompressor<uint> nc;
    mm::MMGuard<uint> cn_ptr((uint *)alloc((1 << s->pss) * sizeof(uint), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);
    CprsErr res = nc.Decompress(cn_ptr.get(), (char *)(cur_buf + 8), comp_len_buf_size - 8, dpn->nr - dpn->nn, maxv);
    if (res != CprsErr::CPRS_SUCCESS) {
      std::stringstream msg_buf;
      msg_buf << "Decompression of lengths of std::string values failed for column "
              << (pc_column(GetCoordinate().co.pack) + 1) << ", pack " << (pc_dp(GetCoordinate().co.pack) + 1)
              << " (error " << static_cast<int>(res) << ").";
      throw common::DatabaseException(msg_buf.str());
    }

    int oid = 0;
    for (uint o = 0; o < dpn->nr; o++)
      if (!IsNull(int(o))) SetSize(o, (uint)cn_ptr[oid++]);
  } else {
    for (uint o = 0; o < dpn->nr; o++)
      if (!IsNull(int(o))) SetSize(o, 0);
  }
  cur_buf += comp_len_buf_size;

  auto dlen = *(uint32_t *)cur_buf;
  cur_buf += sizeof(dlen);
  data.sum_len = *(uint32_t *)cur_buf;
  cur_buf += sizeof(uint32_t);

  ASSERT(cur_buf + dlen == dpn->len + reinterpret_cast<char *>(compressed_buf.get()),
         std::to_string(data.sum_len) + "/" + std::to_string(dpn->len) + "/" + std::to_string(dlen));

  int zlo = 0;
  for (uint obj = 0; obj < dpn->nr; obj++)
    if (!IsNull(obj) && GetSize(obj) == 0) zlo++;
  int objs = dpn->nr - dpn->nn - zlo;

  if (objs) {
    mm::MMGuard<uint> tmp_len((uint *)alloc(objs * sizeof(uint), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);
    for (uint tmp_id = 0, id = 0; id < dpn->nr; id++)
      if (!IsNull(id) && GetSize(id) != 0) tmp_len[tmp_id++] = GetSize(id);

    if (dlen) {
      data.v.push_back({(char *)alloc(data.sum_len, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED), data.sum_len, 0});
      compress::TextCompressor tc;
      CprsErr res =
          tc.Decompress(data.v.front().ptr, data.sum_len, cur_buf, dlen, tmp_index.get(), tmp_len.get(), objs);
      if (res != CprsErr::CPRS_SUCCESS) {
        std::stringstream msg_buf;
        msg_buf << "Decompression of std::string values failed for column " << (pc_column(GetCoordinate().co.pack) + 1)
                << ", pack " << (pc_dp(GetCoordinate().co.pack) + 1) << " (error " << static_cast<int>(res) << ").";
        throw common::DatabaseException(msg_buf.str());
      }
    }
  }

  for (uint tmp_id = 0, id = 0; id < dpn->nr; id++) {
    if (!IsNull(id) && GetSize(id) != 0)
      SetPtr(id, (char *)tmp_index[tmp_id++]);
    else {
      SetSize(id, 0);
      SetPtr(id, 0);
    }
  }
}

void PackStr::LoadCompressedTrie(system::Stream *f) {
  ASSERT(IsModeCompressionApplied());

  compressed_data_ = alloc_ptr(dpn->len + 1, mm::BLOCK_TYPE::BLOCK_COMPRESSED);
  f->ReadExact(compressed_data_.get(), dpn->len);
  auto trie_length = dpn->len - (dpn->nr * sizeof(unsigned short)) - 8;
  marisa_trie_.map(compressed_data_.get(), trie_length);
  dpn->synced = true;
  char *buf_ptr = (char *)compressed_data_.get();
  data.sum_len = *(std::uint64_t *)(buf_ptr + trie_length);
  ids_array_ = (unsigned short *)(buf_ptr + trie_length + 8);
  if (dpn->nn > 0) {
    for (uint row = 0; row < dpn->nr; row++) {
      if (ids_array_[row] == 0xffff) SetNull(row);
    }
  }
  state_ = PackStrtate::PACK_TRIE;
}

types::BString PackStr::GetStringValueTrie(int ono) const {
  marisa::Agent agent;
  std::size_t keyid = ids_array_[ono];
  agent.set_query(keyid);
  marisa_trie_.reverse_lookup(agent);
  return types::BString(agent.key().ptr(), agent.key().length(), 1);
}

types::BString PackStr::GetValueBinary(int ono) const {
  if (IsNull(ono)) return types::BString();
  DEBUG_ASSERT(ono < (int)dpn->nr);
  if (state_ == PackStrtate::PACK_TRIE) return GetStringValueTrie(ono);
  size_t str_size;
  if (data.len_mode == sizeof(ushort))
    str_size = data.lens16[ono];
  else
    str_size = data.lens32[ono];
  if (str_size == 0) return ZERO_LENGTH_STRING;
  return types::BString(data.index[ono], str_size);
}

void PackStr::LoadUncompressed(system::Stream *f) {
  auto sz = dpn->len;
  f->ReadExact(nulls.get(), NULLS_SIZE);
  sz -= NULLS_SIZE;
  f->ReadExact(data.lens, (data.len_mode * (1 << s->pss)));
  sz -= (data.len_mode * (1 << s->pss));

  data.v.push_back({(char *)alloc(sz + 1, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED), sz, 0});
  f->ReadExact(data.v.back().ptr, sz);
  data.v.back().pos = sz;
  data.sum_len = 0;
  for (uint i = 0; i < dpn->nr; i++) {
    if (!IsNull(i) && GetSize(i) != 0) {
      SetPtr(i, data.v.front().ptr + data.sum_len);
      data.sum_len += GetSize(i);
    } else {
      SetPtrSize(i, nullptr, 0);
    }
  }
  ASSERT(data.sum_len == sz, "bad pack! " + std::to_string(data.sum_len) + "/" + std::to_string(sz));
}

bool PackStr::Lookup(const types::BString &pattern, uint16_t &id) {
  marisa::Agent agent;
  agent.set_query(pattern.GetDataBytesPointer(), pattern.size());
  if (!marisa_trie_.lookup(agent)) {
    return false;
  }
  id = agent.key().id();
  return true;
}

bool PackStr::IsNotMatched(int row, uint16_t &id) { return ids_array_[row] != id; }

bool PackStr::LikePrefix(const types::BString &pattern, std::size_t prefixlen, std::unordered_set<uint16_t> &ids) {
  marisa::Agent agent;
  agent.set_query(pattern.begin(), prefixlen);
  while (marisa_trie_.predictive_search(agent)) {
    ids.insert((uint16_t)agent.key().id());
  }
  return true;
}

bool PackStr::IsNotMatched(int row, const std::unordered_set<uint16_t> &ids) {
  return ids.find(ids_array_[row]) == ids.end();
}
}  // namespace core
}  // namespace stonedb
