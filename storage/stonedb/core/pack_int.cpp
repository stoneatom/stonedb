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

#include <algorithm>
#include <unordered_map>

#include "compress/bit_stream_compressor.h"
#include "compress/num_compressor.h"
#include "core/bin_tools.h"
#include "core/column_share.h"
#include "core/value.h"
#include "loader/value_cache.h"
#include "mm/mm_guard.h"
#include "system/stonedb_file.h"

#include "core/pack_int.h"

namespace stonedb {
namespace core {
PackInt::PackInt(DPN *dpn, PackCoordinate pc, ColumnShare *s) : Pack(dpn, pc, s) {
  is_real = ATI::IsRealType(s->ColType().GetTypeName());

  if (dpn->NotTrivial()) {
    system::StoneDBFile f;
    f.OpenReadOnly(s->DataFile());
    f.Seek(dpn->addr, SEEK_SET);
    LoadDataFromFile(&f);
  }
}

PackInt::PackInt(const PackInt &apn, const PackCoordinate &pc) : Pack(apn, pc), is_real(apn.is_real) {
  data.vt = apn.data.vt;
  if (apn.data.vt > 0) {
    ASSERT(apn.data.ptr != nullptr);
    data.ptr = alloc(data.vt * apn.dpn->nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    std::memcpy(data.ptr, apn.data.ptr, data.vt * apn.dpn->nr);
  }
}

std::unique_ptr<Pack> PackInt::Clone(const PackCoordinate &pc) const {
  return std::unique_ptr<Pack>(new PackInt(*this, pc));
}

inline uint8_t PackInt::GetValueSize(uint64_t v) const {
  if (is_real) return 8;

  auto bit_rate = GetBitLen(v);
  if (bit_rate <= 8)
    return 1;
  else if (bit_rate <= 16)
    return 2;
  else if (bit_rate <= 32)
    return 4;
  else
    return 8;
}

namespace {
using Key = std::pair<uint8_t, uint8_t>;
using Func = std::function<void(void *, void *, void *, int64_t)>;
struct Hash {
  std::size_t operator()(const Key &k) const { return std::hash<uint16_t>{}(k.first + (k.second << 8)); }
};

std::unordered_map<Key, Func, Hash> xf{
    {{1, 1},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint8_t *)src, (uint8_t *)end, (uint8_t *)dst,
                      [delta](uint8_t i) -> uint8_t { return i + delta; });
     }},
    {{1, 2},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint8_t *)src, (uint8_t *)end, (uint16_t *)dst,
                      [delta](uint8_t i) -> uint16_t { return i + delta; });
     }},
    {{1, 4},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint8_t *)src, (uint8_t *)end, (uint32_t *)dst,
                      [delta](uint8_t i) -> uint32_t { return i + delta; });
     }},
    {{1, 8},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint8_t *)src, (uint8_t *)end, (uint64_t *)dst,
                      [delta](uint8_t i) -> uint64_t { return i + delta; });
     }},
    {{2, 1},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint16_t *)src, (uint16_t *)end, (uint8_t *)dst,
                      [delta](uint16_t i) -> uint8_t { return i + delta; });
     }},
    {{2, 2},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint16_t *)src, (uint16_t *)end, (uint16_t *)dst,
                      [delta](uint16_t i) -> uint16_t { return i + delta; });
     }},
    {{2, 4},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint16_t *)src, (uint16_t *)end, (uint32_t *)dst,
                      [delta](uint16_t i) -> uint32_t { return i + delta; });
     }},
    {{2, 8},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint16_t *)src, (uint16_t *)end, (uint64_t *)dst,
                      [delta](uint16_t i) -> uint64_t { return i + delta; });
     }},

    {{4, 1},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint32_t *)src, (uint32_t *)end, (uint8_t *)dst,
                      [delta](uint32_t i) -> uint8_t { return i + delta; });
     }},

    {{4, 2},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint32_t *)src, (uint32_t *)end, (uint16_t *)dst,
                      [delta](uint32_t i) -> uint16_t { return i + delta; });
     }},

    {{4, 4},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint32_t *)src, (uint32_t *)end, (uint32_t *)dst,
                      [delta](uint32_t i) -> uint32_t { return i + delta; });
     }},
    {{4, 8},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint32_t *)src, (uint32_t *)end, (uint64_t *)dst,
                      [delta](uint32_t i) -> uint64_t { return i + delta; });
     }},
    {{8, 1},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint64_t *)src, (uint64_t *)end, (uint8_t *)dst,
                      [delta](uint64_t i) -> uint8_t { return i + delta; });
     }},
    {{8, 2},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint64_t *)src, (uint64_t *)end, (uint16_t *)dst,
                      [delta](uint64_t i) -> uint16_t { return i + delta; });
     }},
    {{8, 4},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint64_t *)src, (uint64_t *)end, (uint32_t *)dst,
                      [delta](uint64_t i) -> uint32_t { return i + delta; });
     }},
    {{8, 8},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform((uint64_t *)src, (uint64_t *)end, (uint64_t *)dst,
                      [delta](uint64_t i) -> uint64_t { return i + delta; });
     }},
};
};  // namespace

// expand or shrink the buffer if type size changes
void PackInt::ExpandOrShrink(uint64_t maxv, int64_t delta) {
  auto new_vt = GetValueSize(maxv);
  if (new_vt != data.vt || delta != 0) {
    auto tmp = alloc(new_vt * dpn->nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    xf[{data.vt, new_vt}](data.ptr, data.pint8 + (data.vt * dpn->nr), tmp, delta);
    dealloc(data.ptr);
    data.ptr = tmp;
    data.vt = new_vt;
  }
}

void PackInt::UpdateValueFloat(size_t i, const Value &v) {
  if (IsNull(i)) {
    ASSERT(v.HasValue());

    // update null to non-null
    dpn->synced = false;
    UnsetNull(i);

    auto d = v.GetDouble();
    dpn->sum_d += d;

    if (dpn->NullOnly()) {
      dpn->nn--;
      // so this is the first non-null element
      dpn->min_d = d;
      dpn->max_d = d;
      data.vt = 8;
      data.ptr = alloc(data.vt * dpn->nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
      std::memset(data.ptr, 0, data.vt * dpn->nr);  // ?????
      SetValD(i, d);
    } else {
      dpn->nn--;
      ASSERT(data.ptr != nullptr);

      SetValD(i, d);

      if (d < dpn->min_d) dpn->min_d = d;

      if (d > dpn->max_d) dpn->max_d = d;
    }
  } else {
    // update an original non-null value

    if (data.empty()) {
      ASSERT(dpn->Uniform());
      data.vt = 8;
      data.ptr = alloc(data.vt * dpn->nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
      for (uint i = 0; i < dpn->nr; i++) SetValD(i, dpn->min_d);
    }
    auto oldv = GetValDouble(i);

    ASSERT(oldv <= dpn->max_d);

    // get max/min **without** the value to update
    auto newmax = std::numeric_limits<double>::min();
    auto newmin = std::numeric_limits<double>::max();
    for (uint j = 0; j < dpn->nr; j++) {
      if (i != j && NotNull(j)) {
        auto val = GetValDouble(j);
        if (val > newmax) newmax = val;
        if (val < newmin) newmin = val;
      }
    }
    if (!v.HasValue()) {
      // update non-null to null
      SetNull(i);
      dpn->nn++;
      // easy mode
      dpn->sum_d -= oldv;
      return;
    }

    // update non-null to another nonull
    auto newv = v.GetDouble();
    ASSERT(oldv != newv);

    newmin = std::min(newmin, newv);
    newmax = std::max(newmax, newv);
    SetValD(i, newv);
    dpn->min_d = newmin;
    dpn->max_d = newmax;
    dpn->sum_d -= oldv;
    dpn->sum_d += newv;
  }
}

void PackInt::UpdateValue(size_t i, const Value &v) {
  if (is_real)
    UpdateValueFloat(i, v);
  else
    UpdateValueFixed(i, v);

  // release buffer if the pack becomes trivial
  if (dpn->Trivial()) {
    dealloc(data.ptr);
    data.ptr = nullptr;
    data.vt = 0;
    dpn->addr = DPN_INVALID_ADDR;
  }
}

void PackInt::UpdateValueFixed(size_t i, const Value &v) {
  if (IsNull(i)) {
    ASSERT(v.HasValue());

    // update null to non-null
    dpn->synced = false;
    UnsetNull(i);

    auto l = v.GetInt();
    dpn->sum_i += l;

    if (dpn->NullOnly()) {
      // so this is the first non-null element
      dpn->min_i = l;
      dpn->max_i = l;

      data.vt = 1;
      data.ptr = alloc(data.vt * dpn->nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
      // for fixed number, it is sufficient to simply zero the data
      std::memset(data.ptr, 0, data.vt * dpn->nr);
      dpn->nn--;
    } else {
      ASSERT(data.ptr != nullptr);
      dpn->nn--;

      if (l >= dpn->min_i && l <= dpn->max_i) {
        // this simple mode...
        SetVal64(i, l - dpn->min_i);

        // we are done
        return;
      }

      decltype(data.vt) new_vt;
      int64_t delta = 0;
      if (l < dpn->min_i) {
        delta = dpn->min_i - l;
        dpn->min_i = l;
        new_vt = GetValueSize(dpn->max_i - dpn->min_i);
      } else {  // so l > dpn->max
        dpn->max_i = l;
        new_vt = GetValueSize(dpn->max_i - dpn->min_i);
      }
      if (new_vt > data.vt) {
        auto tmp = alloc(new_vt * dpn->nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
        xf[{data.vt, new_vt}](data.ptr, data.pint8 + (data.vt * dpn->nr), tmp, delta);
        dealloc(data.ptr);
        data.ptr = tmp;
        data.vt = new_vt;
      }
      SetVal64(i, l - dpn->min_i);
    }
  } else {
    // update an original non-null value

    if (data.empty()) {
      ASSERT(dpn->Uniform());
      data.vt = GetValueSize(dpn->max_i - dpn->min_i);
      data.ptr = alloc(data.vt * dpn->nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
      std::memset(data.ptr, 0, data.vt * dpn->nr);
    }
    auto oldv = GetValInt(i);
    oldv += dpn->min_i;

    ASSERT(oldv <= dpn->max_i);

    // get max/min **without** the value to update
    int64_t newmax = std::numeric_limits<int64_t>::min();
    int64_t newmin = std::numeric_limits<int64_t>::max();

    for (uint j = 0; j < dpn->nr; j++) {
      if (i != j && NotNull(j)) {
        auto val = GetValInt(j) + dpn->min_i;
        if (val > newmax) newmax = val;
        if (val < newmin) newmin = val;
      }
    }

    if (!v.HasValue()) {
      // update non-null to null
      SetNull(i);
      dpn->nn++;
      // easy mode
      dpn->sum_i -= oldv;

      if (!dpn->NullOnly()) ExpandOrShrink(newmax - newmin, 0);

      return;
    }

    // update non-null to another nonull
    auto newv = v.GetInt();
    ASSERT(oldv != newv);

    newmin = std::min(newmin, newv);
    newmax = std::max(newmax, newv);

    ExpandOrShrink(newmax - newmin, dpn->min_i - newmin);
    SetVal64(i, newv - newmin);

    dpn->min_i = newmin;
    dpn->max_i = newmax;
    dpn->sum_i -= oldv;
    dpn->sum_i += newv;
  }
}

void PackInt::LoadValuesDouble(const loader::ValueCache *vc, const std::optional<common::double_int_t> &nv) {
  // if this is the first non-null value, set up min/max
  if (dpn->NullOnly()) {
    dpn->min_d = std::numeric_limits<double>::max();
    dpn->max_d = std::numeric_limits<double>::min();
  }
  auto new_min = std::min(vc->MinDouble(), dpn->min_d);
  auto new_max = std::max(vc->MaxDouble(), dpn->max_d);
  auto new_nr = dpn->nr + vc->NumOfValues();
  if (dpn->Trivial()) {
    ASSERT(data.ptr == nullptr);
    data.vt = sizeof(double);
    data.ptr = alloc(data.vt * new_nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    if (dpn->Uniform()) {
      std::fill_n((double *)data.ptr, new_nr, dpn->min_d);
    }
  } else {
    // expanding existing pack data
    ASSERT(data.ptr != nullptr);
    data.ptr = rc_realloc(data.ptr, data.vt * new_nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
  }

  dpn->synced = false;

  for (size_t i = 0; i < vc->NumOfValues(); i++) {
    if (!vc->IsNull(i)) {
      AppendValue(*(uint64_t *)vc->GetDataBytesPointer(i));
    } else {
      if (nv.has_value())
        AppendValue(nv->i);
      else
        AppendNull();
    }
  }
  // sum has already been updated outside
  dpn->min_d = new_min;
  dpn->max_d = new_max;
}

void PackInt::LoadValues(const loader::ValueCache *vc, const std::optional<common::double_int_t> &nv) {
  if (is_real)
    LoadValuesDouble(vc, nv);
  else
    LoadValuesFixed(vc, nv);
}

void PackInt::LoadValuesFixed(const loader::ValueCache *vc, const std::optional<common::double_int_t> &nv) {
  // if this is the first non-null value, set up min/max
  if (dpn->NullOnly()) {
    dpn->min_i = std::numeric_limits<int64_t>::max();
    dpn->max_i = std::numeric_limits<int64_t>::min();
  }

  auto new_min = std::min(vc->MinInt(), dpn->min_i);
  auto new_max = std::max(vc->MaxInt(), dpn->max_i);
  auto new_vt = GetValueSize(new_max - new_min);
  auto new_nr = dpn->nr + vc->NumOfValues();

  ASSERT(new_vt >= data.vt);

  auto delta = dpn->min_i - new_min;

  if (dpn->Trivial()) {
    ASSERT(data.ptr == nullptr);
    data.ptr = alloc(new_vt * new_nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    if (dpn->Uniform()) {
      switch (new_vt) {
        case 8:
          std::fill_n((uint64_t *)data.ptr, new_nr, delta);
          break;
        case 4:
          std::fill_n((uint32_t *)data.ptr, new_nr, delta);
          break;
        case 2:
          std::fill_n((uint16_t *)data.ptr, new_nr, delta);
          break;
        case 1:
          std::fill_n((uint8_t *)data.ptr, new_nr, delta);
          break;
        default:
          STONEDB_ERROR("bad value type in pakcN");
          break;
      }
    }
  } else {
    // expanding existing pack data
    ASSERT(data.ptr != nullptr);
    auto tmp_ptr = alloc(new_vt * new_nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    xf[{data.vt, new_vt}](data.ptr, data.pint8 + (data.vt * dpn->nr), tmp_ptr, delta);
    dealloc(data.ptr);
    data.ptr = tmp_ptr;
  }

  data.vt = new_vt;

  dpn->synced = false;

  for (size_t i = 0; i < vc->NumOfValues(); i++) {
    if (vc->NotNull(i)) {
      AppendValue(*(uint64_t *)vc->GetDataBytesPointer(i) - new_min);
    } else {
      if (nv.has_value())
        AppendValue(nv->i - new_min);
      else
        AppendNull();
    }
  }
  // sum has already been updated outside
  dpn->min_i = new_min;
  dpn->max_i = new_max;
}

void PackInt::Destroy() {
  dealloc(data.ptr);
  data.ptr = 0;
}

PackInt::~PackInt() {
  DestructionLock();
  Destroy();
}

void PackInt::LoadDataFromFile(system::Stream *fcurfile) {
  FunctionExecutor fe([this]() { Lock(); }, [this]() { Unlock(); });

  data.vt = GetValueSize(dpn->max_i - dpn->min_i);
  if (IsModeNoCompression()) {
    if (dpn->nn) {
      fcurfile->ReadExact(nulls.get(), NULLS_SIZE);
    }
    ASSERT(data.vt * dpn->nr != 0);
    data.ptr = alloc(data.vt * dpn->nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    fcurfile->ReadExact(data.ptr, data.vt * dpn->nr);
    dpn->synced = false;
  } else {
    UniquePtr uptr = alloc_ptr(dpn->len + 1, mm::BLOCK_TYPE::BLOCK_COMPRESSED);
    fcurfile->ReadExact(uptr.get(), dpn->len);
    dpn->synced = true;
    {
      ASSERT(!IsModeNoCompression());

      uint *cur_buf = (uint *)uptr.get();
      if (data.ptr == nullptr && data.vt > 0) data.ptr = alloc(data.vt * dpn->nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);

      // decompress nulls

      if (dpn->nn > 0) {
        uint null_buf_size = 0;
        null_buf_size = (*(ushort *)cur_buf);
        if (null_buf_size > NULLS_SIZE) throw common::DatabaseException("Unexpected bytes found in data pack.");
        if (!IsModeNullsCompressed())  // no nulls compression
          std::memcpy(nulls.get(), (char *)cur_buf + 2, null_buf_size);
        else {
          compress::BitstreamCompressor bsc;
          CprsErr res = bsc.Decompress((char *)nulls.get(), null_buf_size, (char *)cur_buf + 2, dpn->nr, dpn->nn);
          if (res != CprsErr::CPRS_SUCCESS) {
            std::stringstream msg_buf;
            msg_buf << "Decompression of nulls failed for column " << (pc_column(GetCoordinate().co.pack) + 1)
                    << ", pack " << (pc_dp(GetCoordinate().co.pack) + 1) << " (error " << static_cast<int>(res) << ").";
            throw common::DatabaseException(msg_buf.str());
          }
        }
        cur_buf = (uint *)((char *)cur_buf + null_buf_size + 2);
      }

      if (IsModeDataCompressed() && data.vt > 0 && *(uint64_t *)(cur_buf + 1) != (uint64_t)0) {
        if (data.vt == 1) {
          compress::NumCompressor<uchar> nc;
          DecompressAndInsertNulls(nc, cur_buf);
        } else if (data.vt == 2) {
          compress::NumCompressor<ushort> nc;
          DecompressAndInsertNulls(nc, cur_buf);
        } else if (data.vt == 4) {
          compress::NumCompressor<uint> nc;
          DecompressAndInsertNulls(nc, cur_buf);
        } else {
          compress::NumCompressor<uint64_t> nc;
          DecompressAndInsertNulls(nc, cur_buf);
        }
      } else if (data.vt > 0) {
        for (uint o = 0; o < dpn->nr; o++)
          if (!IsNull(int(o))) SetVal64(o, 0);
      }
    }
  }
  dpn->synced = true;
}

void PackInt::Save() {
  UniquePtr uptr;
  if (ShouldNotCompress()) {
    SetModeNoCompression();
    dpn->len = 0;
    if (dpn->nn) dpn->len += NULLS_SIZE;
    dpn->len += data.vt * dpn->nr;
  } else {
    auto res = Compress();
    dpn->len = res.second;
    uptr = std::move(res.first);
  }

  s->alloc_seg(dpn);
  system::StoneDBFile f;
  f.OpenCreate(s->DataFile());
  f.Seek(dpn->addr, SEEK_SET);
  if (ShouldNotCompress())
    SaveUncompressed(&f);
  else
    f.WriteExact(uptr.get(), dpn->len);

  ASSERT(f.Tell() == off_t(dpn->addr + dpn->len));

  f.Close();
  dpn->synced = true;
}

void PackInt::SaveUncompressed(system::Stream *f) {
  if (dpn->nn) f->WriteExact(nulls.get(), NULLS_SIZE);
  if (data.ptr) f->WriteExact(data.ptr, data.vt * dpn->nr);
}

template <typename etype>
void PackInt::RemoveNullsAndCompress(compress::NumCompressor<etype> &nc, char *tmp_comp_buffer, uint &tmp_cb_len,
                                     uint64_t &maxv) {
  mm::MMGuard<etype> tmp_data;
  if (dpn->nn > 0) {
    tmp_data = mm::MMGuard<etype>(
        (etype *)(alloc((dpn->nr - dpn->nn) * sizeof(etype), mm::BLOCK_TYPE::BLOCK_TEMPORARY)), *this);
    for (uint i = 0, d = 0; i < dpn->nr; i++) {
      if (!IsNull(i)) tmp_data[d++] = ((etype *)(data.ptr))[i];
    }
  } else
    tmp_data = mm::MMGuard<etype>((etype *)data.ptr, *this, false);

  CprsErr res = nc.Compress(tmp_comp_buffer, tmp_cb_len, tmp_data.get(), dpn->nr - dpn->nn, (etype)(maxv));
  if (res != CprsErr::CPRS_SUCCESS) {
    std::stringstream msg_buf;
    msg_buf << "Compression of numerical values failed for column " << (pc_column(GetCoordinate().co.pack) + 1)
            << ", pack " << (pc_dp(GetCoordinate().co.pack) + 1) << " (error " << static_cast<int>(res) << ").";
    throw common::InternalException(msg_buf.str());
  }
}

template <typename etype>
void PackInt::DecompressAndInsertNulls(compress::NumCompressor<etype> &nc, uint *&cur_buf) {
  CprsErr res = nc.Decompress(data.ptr, (char *)((cur_buf + 3)), *cur_buf, dpn->nr - dpn->nn,
                              (etype) * (uint64_t *)((cur_buf + 1)));
  if (res != CprsErr::CPRS_SUCCESS) {
    std::stringstream msg_buf;
    msg_buf << "Decompression of numerical values failed for column " << (pc_column(GetCoordinate().co.pack) + 1)
            << ", pack " << (pc_dp(GetCoordinate().co.pack) + 1) << " (error " << static_cast<int>(res) << ").";
    throw common::DatabaseException(msg_buf.str());
  }
  etype *d = ((etype *)(data.ptr)) + dpn->nr - 1;
  etype *s = ((etype *)(data.ptr)) + dpn->nr - dpn->nn - 1;
  for (int i = dpn->nr - 1; d > s; i--) {
    if (IsNull(i))
      --d;
    else
      *(d--) = *(s--);
  }
}

std::pair<PackInt::UniquePtr, size_t> PackInt::Compress() {
  uint *cur_buf = NULL;
  size_t buffer_size = 0;
  mm::MMGuard<char> tmp_comp_buffer;

  uint tmp_cb_len = 0;
  SetModeDataCompressed();

  uint64_t maxv = 0;
  if (data.ptr) {  // else maxv remains 0
    uint64_t cv = 0;
    for (uint o = 0; o < dpn->nr; o++) {
      if (!IsNull(o)) {
        cv = (uint64_t)GetValInt(o);
        if (cv > maxv) maxv = cv;
      }
    }
  }

  if (maxv != 0) {
    // ASSERT(last_set + 1 == dpn->nr - dpn->nn, "Expression evaluation
    // failed!");
    if (data.vt == 1) {
      compress::NumCompressor<uchar> nc;
      tmp_cb_len = (dpn->nr - dpn->nn) * sizeof(uchar) + 20;
      if (tmp_cb_len)
        tmp_comp_buffer =
            mm::MMGuard<char>((char *)alloc(tmp_cb_len * sizeof(char), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);
      RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
    } else if (data.vt == 2) {
      compress::NumCompressor<ushort> nc;
      tmp_cb_len = (dpn->nr - dpn->nn) * sizeof(ushort) + 20;
      if (tmp_cb_len)
        tmp_comp_buffer =
            mm::MMGuard<char>((char *)alloc(tmp_cb_len * sizeof(char), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);
      RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
    } else if (data.vt == 4) {
      compress::NumCompressor<uint> nc;
      tmp_cb_len = (dpn->nr - dpn->nn) * sizeof(uint) + 20;
      if (tmp_cb_len)
        tmp_comp_buffer =
            mm::MMGuard<char>((char *)alloc(tmp_cb_len * sizeof(char), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);
      RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
    } else {
      compress::NumCompressor<uint64_t> nc;
      tmp_cb_len = (dpn->nr - dpn->nn) * sizeof(uint64_t) + 20;
      if (tmp_cb_len)
        tmp_comp_buffer =
            mm::MMGuard<char>((char *)alloc(tmp_cb_len * sizeof(char), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);
      RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
    }
    buffer_size += tmp_cb_len;
  }
  buffer_size += 12;
  // compress nulls
  uint null_buf_size = ((dpn->nr + 7) / 8);
  mm::MMGuard<uchar> comp_null_buf;

  if (dpn->nn > 0) {
    comp_null_buf =
        mm::MMGuard<uchar>((uchar *)alloc((null_buf_size + 2) * sizeof(char), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);
    uint cnbl = null_buf_size + 1;
    comp_null_buf[cnbl] = 0xBA;  // just checking - buffer overrun
    compress::BitstreamCompressor bsc;
    CprsErr res = bsc.Compress((char *)comp_null_buf.get(), null_buf_size, (char *)nulls.get(), dpn->nr, dpn->nn);
    if (comp_null_buf[cnbl] != 0xBA) {
      STONEDB_LOG(LogCtl_Level::ERROR, "buffer overrun by BitstreamCompressor (N f).");
      ASSERT(0, "ERROR: buffer overrun by BitstreamCompressor (N f).");
    }
    if (res == CprsErr::CPRS_SUCCESS)
      SetModeNullsCompressed();
    else if (res == CprsErr::CPRS_ERR_BUF) {
      comp_null_buf = mm::MMGuard<uchar>((uchar *)nulls.get(), *this, false);
      null_buf_size = ((dpn->nr + 7) / 8);
      ResetModeNullsCompressed();
    } else {
      std::stringstream msg_buf;
      msg_buf << "Compression of nulls failed for column " << (pc_column(GetCoordinate().co.pack) + 1) << ", pack "
              << (pc_dp(GetCoordinate().co.pack) + 1) << " (error " << static_cast<int>(res) << ").";
      throw common::InternalException(msg_buf.str());
    }
    buffer_size += null_buf_size + 2;
  }
  UniquePtr ptr = alloc_ptr(buffer_size, mm::BLOCK_TYPE::BLOCK_COMPRESSED);
  uchar *compressed_buf = reinterpret_cast<uchar *>(ptr.get());
  std::memset(compressed_buf, 0, buffer_size);
  cur_buf = (uint *)compressed_buf;
  if (dpn->nn > 0) {
    if (null_buf_size > NULLS_SIZE) throw common::DatabaseException("Unexpected bytes found (PackInt::Compress).");
    *(ushort *)compressed_buf = (ushort)null_buf_size;
    std::memcpy(compressed_buf + 2, comp_null_buf.get(), null_buf_size);
    cur_buf = (uint *)(compressed_buf + null_buf_size + 2);
  }

  *cur_buf = tmp_cb_len;
  *(uint64_t *)(cur_buf + 1) = maxv;
  std::memcpy(cur_buf + 3, tmp_comp_buffer.get(), tmp_cb_len);
  return std::make_pair(std::move(ptr), buffer_size);
}
}  // namespace core
}  // namespace stonedb
