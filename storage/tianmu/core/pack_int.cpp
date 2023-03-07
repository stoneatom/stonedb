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

#include "core/pack_int.h"

#include <algorithm>
#include <unordered_map>

#include "compress/bit_stream_compressor.h"
#include "compress/num_compressor.h"
#include "core/bin_tools.h"
#include "core/column_share.h"
#include "core/value.h"
#include "loader/value_cache.h"
#include "system/tianmu_file.h"

namespace Tianmu {
namespace core {
PackInt::PackInt(DPN *dpn, PackCoordinate pc, ColumnShare *s) : Pack(dpn, pc, s) {
  is_real_ = ATI::IsRealType(s->ColType().GetTypeName());

  if (dpn_->NotTrivial()) {
    system::TianmuFile f;
    f.OpenReadOnly(s->DataFile());
    f.Seek(dpn_->dataAddress, SEEK_SET);
    LoadDataFromFile(&f);
  }
}

PackInt::PackInt(const PackInt &apn, const PackCoordinate &pc) : Pack(apn, pc), is_real_(apn.is_real_) {
  data_.value_type_ = apn.data_.value_type_;
  if (apn.data_.value_type_ > 0) {
    ASSERT(apn.data_.ptr_ != nullptr);
    data_.ptr_ = alloc(data_.value_type_ * apn.dpn_->numOfRecords, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    std::memcpy(data_.ptr_, apn.data_.ptr_, data_.value_type_ * apn.dpn_->numOfRecords);
  }
}

std::unique_ptr<Pack> PackInt::Clone(const PackCoordinate &pc) const {
  return std::unique_ptr<Pack>(new PackInt(*this, pc));
}

inline uint8_t PackInt::GetValueSize(uint64_t v) const {
  if (is_real_)
    return 8;

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
       std::transform(static_cast<uint8_t *>(src), static_cast<uint8_t *>(end), static_cast<uint8_t *>(dst),
                      [delta](uint8_t i) -> uint8_t { return i + delta; });
     }},
    {{1, 2},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint8_t *>(src), static_cast<uint8_t *>(end), static_cast<uint16_t *>(dst),
                      [delta](uint8_t i) -> uint16_t { return i + delta; });
     }},
    {{1, 4},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint8_t *>(src), static_cast<uint8_t *>(end), static_cast<uint32_t *>(dst),
                      [delta](uint8_t i) -> uint32_t { return i + delta; });
     }},
    {{1, 8},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint8_t *>(src), static_cast<uint8_t *>(end), static_cast<uint64_t *>(dst),
                      [delta](uint8_t i) -> uint64_t { return i + delta; });
     }},
    {{2, 1},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint16_t *>(src), static_cast<uint16_t *>(end), static_cast<uint8_t *>(dst),
                      [delta](uint16_t i) -> uint8_t { return i + delta; });
     }},
    {{2, 2},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint16_t *>(src), static_cast<uint16_t *>(end), static_cast<uint16_t *>(dst),
                      [delta](uint16_t i) -> uint16_t { return i + delta; });
     }},
    {{2, 4},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint16_t *>(src), static_cast<uint16_t *>(end), static_cast<uint32_t *>(dst),
                      [delta](uint16_t i) -> uint32_t { return i + delta; });
     }},
    {{2, 8},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint16_t *>(src), static_cast<uint16_t *>(end), static_cast<uint64_t *>(dst),
                      [delta](uint16_t i) -> uint64_t { return i + delta; });
     }},

    {{4, 1},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint32_t *>(src), static_cast<uint32_t *>(end), static_cast<uint8_t *>(dst),
                      [delta](uint32_t i) -> uint8_t { return i + delta; });
     }},

    {{4, 2},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint32_t *>(src), static_cast<uint32_t *>(end), static_cast<uint16_t *>(dst),
                      [delta](uint32_t i) -> uint16_t { return i + delta; });
     }},

    {{4, 4},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint32_t *>(src), static_cast<uint32_t *>(end), static_cast<uint32_t *>(dst),
                      [delta](uint32_t i) -> uint32_t { return i + delta; });
     }},
    {{4, 8},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint32_t *>(src), static_cast<uint32_t *>(end), static_cast<uint64_t *>(dst),
                      [delta](uint32_t i) -> uint64_t { return i + delta; });
     }},
    {{8, 1},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint64_t *>(src), static_cast<uint64_t *>(end), static_cast<uint8_t *>(dst),
                      [delta](uint64_t i) -> uint8_t { return i + delta; });
     }},
    {{8, 2},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint64_t *>(src), static_cast<uint64_t *>(end), static_cast<uint16_t *>(dst),
                      [delta](uint64_t i) -> uint16_t { return i + delta; });
     }},
    {{8, 4},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint64_t *>(src), static_cast<uint64_t *>(end), static_cast<uint32_t *>(dst),
                      [delta](uint64_t i) -> uint32_t { return i + delta; });
     }},
    {{8, 8},
     [](void *src, void *end, void *dst, int64_t delta) {
       std::transform(static_cast<uint64_t *>(src), static_cast<uint64_t *>(end), static_cast<uint64_t *>(dst),
                      [delta](uint64_t i) -> uint64_t { return i + delta; });
     }},
};
};  // namespace

// expand or shrink the buffer if type size changes
void PackInt::ExpandOrShrink(uint64_t maxv, int64_t delta) {
  auto new_vt = GetValueSize(maxv);
  if (new_vt != data_.value_type_ || delta != 0) {
    auto tmp = alloc(new_vt * dpn_->numOfRecords, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    xf[{data_.value_type_, new_vt}](data_.ptr_, data_.ptr_int8_ + (data_.value_type_ * dpn_->numOfRecords), tmp, delta);
    dealloc(data_.ptr_);
    data_.ptr_ = tmp;
    data_.value_type_ = new_vt;
  }
}

void PackInt::UpdateValueFloat(size_t locationInPack, const Value &v) {
  if (IsNull(locationInPack)) {
    ASSERT(v.HasValue());

    // update null to non-null
    dpn_->synced = false;
    UnsetNull(locationInPack);

    auto d = v.GetDouble();
    dpn_->sum_d += d;

    if (dpn_->NullOnly()) {
      dpn_->numOfNulls--;
      // so this is the first non-null element
      dpn_->min_d = d;
      dpn_->max_d = d;
      data_.value_type_ = 8;
      data_.ptr_ = alloc(data_.value_type_ * dpn_->numOfRecords, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
      std::memset(data_.ptr_, 0, data_.value_type_ * dpn_->numOfRecords);
      SetValD(locationInPack, d);
    } else {
      dpn_->numOfNulls--;
      ASSERT(data_.ptr_ != nullptr);

      SetValD(locationInPack, d);

      if (d < dpn_->min_d)
        dpn_->min_d = d;

      if (d > dpn_->max_d)
        dpn_->max_d = d;
    }
  } else {
    // update an original non-null value

    if (data_.empty()) {
      ASSERT(dpn_->Uniform());
      data_.value_type_ = 8;
      data_.ptr_ = alloc(data_.value_type_ * dpn_->numOfRecords, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
      for (uint i = 0; i < dpn_->numOfRecords; i++) SetValD(i, dpn_->min_d);
    }
    auto oldv = GetValDouble(locationInPack);

    ASSERT(oldv <= dpn_->max_d);

    // get max/min **without** the value to update
    auto newmax = std::numeric_limits<double>::min();
    auto newmin = std::numeric_limits<double>::max();
    for (uint j = 0; j < dpn_->numOfRecords; j++) {
      if (locationInPack != j && NotNull(j)) {
        auto val = GetValDouble(j);
        if (val > newmax)
          newmax = val;
        if (val < newmin)
          newmin = val;
      }
    }
    if (!v.HasValue()) {
      // update non-null to null
      SetNull(locationInPack);
      dpn_->numOfNulls++;
      // easy mode
      dpn_->sum_d -= oldv;
      return;
    }

    // update non-null to another nonull
    auto newv = v.GetDouble();
    ASSERT(oldv != newv);

    newmin = std::min(newmin, newv);
    newmax = std::max(newmax, newv);
    SetValD(locationInPack, newv);
    dpn_->min_d = newmin;
    dpn_->max_d = newmax;
    dpn_->sum_d -= oldv;
    dpn_->sum_d += newv;
  }
}

void PackInt::UpdateValue(size_t locationInPack, const Value &v) {
  if (IsDeleted(locationInPack))
    return;
  if (is_real_)
    UpdateValueFloat(locationInPack, v);
  else
    UpdateValueFixed(locationInPack, v);

  // release buffer if the pack becomes trivial
  if (dpn_->Trivial()) {
    dealloc(data_.ptr_);
    data_.ptr_ = nullptr;
    data_.value_type_ = 0;
    dpn_->dataAddress = DPN_INVALID_ADDR;
  }
}

void PackInt::DeleteByRow(size_t locationInPack) {
  if (IsDeleted(locationInPack))
    return;
  dpn_->synced = false;

  if (!IsNull(locationInPack)) {
    if (is_real_) {
      if (data_.empty()) {
        ASSERT(dpn_->Uniform());
        data_.value_type_ = 8;
        data_.ptr_ = alloc(data_.value_type_ * dpn_->numOfRecords, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
        for (uint i = 0; i < dpn_->numOfRecords; i++) SetValD(i, dpn_->min_d);
      }
      auto oldv = GetValDouble(locationInPack);

      ASSERT(oldv <= dpn_->max_d);
      dpn_->sum_d -= oldv;

      if (oldv == dpn_->min_d || oldv == dpn_->max_d) {
        // get max/min **without** the value to update
        auto newmax = std::numeric_limits<double>::min();
        auto newmin = std::numeric_limits<double>::max();
        for (uint j = 0; j < dpn_->numOfRecords; j++) {
          if (locationInPack != j && NotNull(j)) {
            auto val = GetValDouble(j);
            if (val > newmax)
              newmax = val;
            if (val < newmin)
              newmin = val;
          }
        }
        dpn_->min_d = newmin;
        dpn_->max_d = newmax;
      }
    } else {
      if (data_.empty()) {
        ASSERT(dpn_->Uniform());
        data_.value_type_ = GetValueSize(dpn_->max_i - dpn_->min_i);
        data_.ptr_ = alloc(data_.value_type_ * dpn_->numOfRecords, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
        std::memset(data_.ptr_, 0, data_.value_type_ * dpn_->numOfRecords);
      }
      auto oldv = GetValInt(locationInPack);
      oldv += dpn_->min_i;
      ASSERT(oldv <= dpn_->max_i);
      dpn_->sum_i -= oldv;
      if (oldv == dpn_->min_i || oldv == dpn_->max_i) {
        // get max/min **without** the value to update
        auto newmax = std::numeric_limits<int64_t>::min();
        auto newmin = std::numeric_limits<int64_t>::max();
        for (uint j = 0; j < dpn_->numOfRecords; j++) {
          if (locationInPack != j && NotNull(j)) {
            auto val = GetValInt(j) + dpn_->min_i;
            if (val > newmax)
              newmax = val;
            if (val < newmin)
              newmin = val;
          }
        }
        ExpandOrShrink(newmax - newmin, dpn_->min_i - newmin);
        dpn_->min_i = newmin;
        dpn_->max_i = newmax;
      }
    }
    SetNull(locationInPack);
    dpn_->numOfNulls++;
  }
  SetDeleted(locationInPack);
  dpn_->numOfDeleted++;
}

void PackInt::UpdateValueFixed(size_t locationInPack, const Value &v) {
  if (IsNull(locationInPack)) {
    ASSERT(v.HasValue());

    // update null to non-null
    dpn_->synced = false;
    UnsetNull(locationInPack);

    auto l = v.GetInt();
    dpn_->sum_i += l;

    if (dpn_->NullOnly()) {
      // so this is the first non-null element
      dpn_->min_i = l;
      dpn_->max_i = l;

      data_.value_type_ = 1;
      data_.ptr_ = alloc(data_.value_type_ * dpn_->numOfRecords, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
      // for fixed number, it is sufficient to simply zero the data_
      std::memset(data_.ptr_, 0, data_.value_type_ * dpn_->numOfRecords);
      dpn_->numOfNulls--;
      SetVal64(locationInPack, l - dpn_->min_i);
    } else {
      ASSERT(data_.ptr_ != nullptr);
      dpn_->numOfNulls--;

      if (l >= dpn_->min_i && l <= dpn_->max_i) {
        // this simple mode...
        SetVal64(locationInPack, l - dpn_->min_i);

        // we are done
        return;
      }

      decltype(data_.value_type_) new_vt;
      int64_t delta = 0;
      if (l < dpn_->min_i) {
        delta = dpn_->min_i - l;
        dpn_->min_i = l;
        new_vt = GetValueSize(dpn_->max_i - dpn_->min_i);
      } else {  // so l > dpn_->max
        dpn_->max_i = l;
        new_vt = GetValueSize(dpn_->max_i - dpn_->min_i);
      }
      if (new_vt != data_.value_type_ || delta != 0) {
        auto tmp = alloc(new_vt * dpn_->numOfRecords, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
        xf[{data_.value_type_, new_vt}](data_.ptr_, data_.ptr_int8_ + (data_.value_type_ * dpn_->numOfRecords), tmp,
                                        delta);
        dealloc(data_.ptr_);
        data_.ptr_ = tmp;
        data_.value_type_ = new_vt;
      }
      SetVal64(locationInPack, l - dpn_->min_i);
    }
  } else {
    // update an original non-null value

    if (data_.empty()) {
      ASSERT(dpn_->Uniform());
      data_.value_type_ = GetValueSize(dpn_->max_i - dpn_->min_i);
      data_.ptr_ = alloc(data_.value_type_ * dpn_->numOfRecords, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
      std::memset(data_.ptr_, 0, data_.value_type_ * dpn_->numOfRecords);
    }
    auto oldv = GetValInt(locationInPack);
    oldv += dpn_->min_i;

    ASSERT(oldv <= dpn_->max_i);

    // get max/min **without** the value to update
    int64_t newmax = std::numeric_limits<int64_t>::min();
    int64_t newmin = std::numeric_limits<int64_t>::max();

    for (uint j = 0; j < dpn_->numOfRecords; j++) {
      if (locationInPack != j && NotNull(j)) {
        auto val = GetValInt(j) + dpn_->min_i;
        if (val > newmax)
          newmax = val;
        if (val < newmin)
          newmin = val;
      }
    }

    if (!v.HasValue()) {
      // update non-null to null
      SetNull(locationInPack);
      dpn_->numOfNulls++;
      // easy mode
      dpn_->sum_i -= oldv;

      if (!dpn_->NullOnly())
        ExpandOrShrink(newmax - newmin, 0);

      return;
    }

    // update non-null to another nonull
    auto newv = v.GetInt();
    //    ASSERT(oldv != newv);

    newmin = std::min(newmin, newv);
    newmax = std::max(newmax, newv);

    ExpandOrShrink(newmax - newmin, dpn_->min_i - newmin);
    SetVal64(locationInPack, newv - newmin);

    dpn_->min_i = newmin;
    dpn_->max_i = newmax;
    dpn_->sum_i -= oldv;
    dpn_->sum_i += newv;
  }
}

void PackInt::LoadValuesDouble(const loader::ValueCache *vc, const std::optional<common::double_int_t> &nv) {
  // if this is the first non-null value, set up min/max
  if (dpn_->NullOnly()) {
    dpn_->min_d = std::numeric_limits<double>::max();
    dpn_->max_d = std::numeric_limits<double>::min();
  }
  auto new_min = std::min(vc->MinDouble(), dpn_->min_d);
  auto new_max = std::max(vc->MaxDouble(), dpn_->max_d);
  auto new_nr = dpn_->numOfRecords + vc->NumOfValues();
  if (dpn_->Trivial()) {
    ASSERT(data_.ptr_ == nullptr);
    data_.value_type_ = sizeof(double);
    data_.ptr_ = alloc(data_.value_type_ * new_nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    if (dpn_->Uniform()) {
      std::fill_n(reinterpret_cast<double *>(data_.ptr_), new_nr, dpn_->min_d);
    }
  } else {
    // expanding existing pack data_
    ASSERT(data_.ptr_ != nullptr);
    data_.ptr_ = rc_realloc(data_.ptr_, data_.value_type_ * new_nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
  }

  dpn_->synced = false;

  for (size_t i = 0; i < vc->NumOfValues(); i++) {
    if (vc->NotNull(i)) {
      AppendValue(*reinterpret_cast<uint64_t *>(const_cast<char *>(vc->GetDataBytesPointer(i))));
    } else {
      if (nv.has_value())
        AppendValue(nv->i);
      else
        AppendNull();
      if (vc->IsDelete(i)) {
        if (!IsNull(dpn_->numOfRecords - 1)) {
          SetNull(dpn_->numOfRecords - 1);
          dpn_->numOfNulls++;
        }
        SetDeleted(dpn_->numOfRecords - 1);
        dpn_->numOfDeleted++;
      }
    }
  }
  // sum has already been updated outside
  dpn_->min_d = new_min;
  dpn_->max_d = new_max;
}

void PackInt::LoadValues(const loader::ValueCache *vc, const std::optional<common::double_int_t> &nv) {
  if (is_real_)
    LoadValuesDouble(vc, nv);
  else
    LoadValuesFixed(vc, nv);
}

void PackInt::LoadValuesFixed(const loader::ValueCache *vc, const std::optional<common::double_int_t> &nv) {
  // if this is the first non-null value, set up min/max
  if (dpn_->NullOnly()) {
    dpn_->min_i = std::numeric_limits<int64_t>::max();
    dpn_->max_i = std::numeric_limits<int64_t>::min();
  }

  auto new_min = std::min(vc->MinInt(), dpn_->min_i);
  auto new_max = std::max(vc->MaxInt(), dpn_->max_i);
  auto new_vt = GetValueSize(new_max - new_min);
  auto new_nr = dpn_->numOfRecords + vc->NumOfValues();

  ASSERT(new_vt >= data_.value_type_);

  auto delta = dpn_->min_i - new_min;

  if (dpn_->Trivial()) {
    ASSERT(data_.ptr_ == nullptr);
    data_.ptr_ = alloc(new_vt * new_nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    if (dpn_->Uniform()) {
      switch (new_vt) {
        case 8:
          std::fill_n(static_cast<uint64_t *>(data_.ptr_), new_nr, delta);
          break;
        case 4:
          std::fill_n(static_cast<uint32_t *>(data_.ptr_), new_nr, delta);
          break;
        case 2:
          std::fill_n(static_cast<uint16_t *>(data_.ptr_), new_nr, delta);
          break;
        case 1:
          std::fill_n(static_cast<uint8_t *>(data_.ptr_), new_nr, delta);
          break;
        default:
          TIANMU_ERROR("bad value type in pakcN");
          break;
      }
    }
  } else {
    // expanding existing pack data_
    ASSERT(data_.ptr_ != nullptr);
    auto tmp_ptr = alloc(new_vt * new_nr, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    xf[{data_.value_type_, new_vt}](data_.ptr_, data_.ptr_int8_ + (data_.value_type_ * dpn_->numOfRecords), tmp_ptr,
                                    delta);
    dealloc(data_.ptr_);
    data_.ptr_ = tmp_ptr;
  }

  data_.value_type_ = new_vt;

  dpn_->synced = false;

  for (size_t i = 0; i < vc->NumOfValues(); i++) {
    if (vc->NotNull(i)) {
      AppendValue(*(reinterpret_cast<uint64_t *>(const_cast<char *>(vc->GetDataBytesPointer(i)))) - new_min);
    } else {
      if (nv.has_value())
        AppendValue(nv->i - new_min);
      else
        AppendNull();
      if (vc->IsDelete(i)) {
        if (!IsNull(dpn_->numOfRecords - 1)) {
          SetNull(dpn_->numOfRecords - 1);
          dpn_->numOfNulls++;
        }
        SetDeleted(dpn_->numOfRecords - 1);
        dpn_->numOfDeleted++;
      }
    }
  }
  // sum has already been updated outside
  dpn_->min_i = new_min;
  dpn_->max_i = new_max;
}

void PackInt::Destroy() {
  dealloc(data_.ptr_);
  data_.ptr_ = 0;
}

PackInt::~PackInt() {
  DestructionLock();
  Destroy();
}

void PackInt::LoadDataFromFile(system::Stream *fcurfile) {
  FunctionExecutor fe([this]() { Lock(); }, [this]() { Unlock(); });

  data_.value_type_ = GetValueSize(dpn_->max_i - dpn_->min_i);
  if (IsModeNoCompression()) {
    if (dpn_->numOfNulls) {
      fcurfile->ReadExact(nulls_ptr_.get(), bitmap_size_);
    }
    if (dpn_->numOfDeleted) {
      fcurfile->ReadExact(deletes_ptr_.get(), bitmap_size_);
    }
    ASSERT(data_.value_type_ * dpn_->numOfRecords != 0);
    data_.ptr_ = alloc(data_.value_type_ * dpn_->numOfRecords, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    fcurfile->ReadExact(data_.ptr_, data_.value_type_ * dpn_->numOfRecords);
    dpn_->synced = false;
  } else {
    UniquePtr uptr = alloc_ptr(dpn_->dataLength + 1, mm::BLOCK_TYPE::BLOCK_COMPRESSED);
    fcurfile->ReadExact(uptr.get(), dpn_->dataLength);
    dpn_->synced = true;
    {
      ASSERT(!IsModeNoCompression());

      uint *cur_buf = static_cast<uint *>(uptr.get());
      if (data_.ptr_ == nullptr && data_.value_type_ > 0)
        data_.ptr_ = alloc(data_.value_type_ * dpn_->numOfRecords, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);

      // decompress nulls

      if (dpn_->numOfNulls > 0) {
        uint null_buf_size = 0;
        null_buf_size = (*reinterpret_cast<ushort *>(cur_buf));
        if (null_buf_size > bitmap_size_)
          throw common::DatabaseException("Unexpected bytes found in data pack.");
        if (!IsModeNullsCompressed())  // no nulls compression
          std::memcpy(nulls_ptr_.get(), reinterpret_cast<char *>(cur_buf) + sizeof(ushort), null_buf_size);
        else {
          compress::BitstreamCompressor bsc;
          CprsErr res =
              bsc.Decompress(reinterpret_cast<char *>(nulls_ptr_.get()), null_buf_size,
                             reinterpret_cast<char *>(cur_buf) + sizeof(ushort), dpn_->numOfRecords, dpn_->numOfNulls);
          if (res != CprsErr::CPRS_SUCCESS) {
            throw common::DatabaseException("Decompression of nulls failed for column " +
                                            std::to_string(pc_column(GetCoordinate().co.pack) + 1) + ", pack " +
                                            std::to_string(pc_dp(GetCoordinate().co.pack) + 1) + " (error " +
                                            std::to_string(static_cast<int>(res)) + ").");
          }
        }
        cur_buf = reinterpret_cast<uint *>(reinterpret_cast<char *>(cur_buf) + null_buf_size + sizeof(ushort));
      }

      if (dpn_->numOfDeleted > 0) {
        uint delete_buf_size = 0;
        delete_buf_size = *(reinterpret_cast<ushort *>(cur_buf));
        if (delete_buf_size > bitmap_size_)
          throw common::DatabaseException("Unexpected bytes found in data pack.");
        if (!IsModeDeletesCompressed())  // no deletes compression
          std::memcpy(deletes_ptr_.get(), reinterpret_cast<char *>(cur_buf) + sizeof(ushort), delete_buf_size);
        else {
          compress::BitstreamCompressor bsc;
          CprsErr res = bsc.Decompress(reinterpret_cast<char *>(deletes_ptr_.get()), delete_buf_size,
                                       reinterpret_cast<char *>(cur_buf) + sizeof(ushort), dpn_->numOfRecords,
                                       dpn_->numOfDeleted);
          if (res != CprsErr::CPRS_SUCCESS) {
            throw common::DatabaseException("Decompression of deletes failed for column " +
                                            std::to_string(pc_column(GetCoordinate().co.pack) + 1) + ", pack " +
                                            std::to_string(pc_dp(GetCoordinate().co.pack) + 1) + " (error " +
                                            std::to_string(static_cast<int>(res)) + ").");
          }
        }
        cur_buf = reinterpret_cast<uint *>(reinterpret_cast<char *>(cur_buf) + delete_buf_size + sizeof(ushort));
      }

      if (IsModeDataCompressed() && data_.value_type_ > 0 &&
          *reinterpret_cast<uint64_t *>(cur_buf + 1) != (uint64_t)0) {
        if (data_.value_type_ == 1) {
          compress::NumCompressor<uchar> nc;
          DecompressAndInsertNulls(nc, cur_buf);
        } else if (data_.value_type_ == 2) {
          compress::NumCompressor<ushort> nc;
          DecompressAndInsertNulls(nc, cur_buf);
        } else if (data_.value_type_ == 4) {
          compress::NumCompressor<uint> nc;
          DecompressAndInsertNulls(nc, cur_buf);
        } else {
          compress::NumCompressor<uint64_t> nc;
          DecompressAndInsertNulls(nc, cur_buf);
        }
      } else if (data_.value_type_ > 0) {
        for (uint o = 0; o < dpn_->numOfRecords; o++)
          if (!IsNull(int(o)))
            SetVal64(o, 0);
      }
    }
  }
  dpn_->synced = true;
}

void PackInt::Save() {
  UniquePtr uptr;
  if (ShouldNotCompress()) {
    SetModeNoCompression();
    dpn_->dataLength = 0;
    if (dpn_->numOfNulls)
      dpn_->dataLength += bitmap_size_;
    if (dpn_->numOfDeleted)
      dpn_->dataLength += bitmap_size_;
    dpn_->dataLength += data_.value_type_ * dpn_->numOfRecords;
  } else {
    auto res = Compress();
    dpn_->dataLength = res.second;
    uptr = std::move(res.first);
  }

  col_share_->alloc_seg(dpn_);
  system::TianmuFile f;
  f.OpenCreate(col_share_->DataFile());
  f.Seek(dpn_->dataAddress, SEEK_SET);
  if (ShouldNotCompress())
    SaveUncompressed(&f);
  else
    f.WriteExact(uptr.get(), dpn_->dataLength);

  ASSERT(f.Tell() == off_t(dpn_->dataAddress + dpn_->dataLength));

  f.Close();
  dpn_->synced = true;
}

void PackInt::SaveUncompressed(system::Stream *f) {
  if (dpn_->numOfNulls)
    f->WriteExact(nulls_ptr_.get(), bitmap_size_);
  if (dpn_->numOfDeleted)
    f->WriteExact(deletes_ptr_.get(), bitmap_size_);
  if (data_.ptr_)
    f->WriteExact(data_.ptr_, data_.value_type_ * dpn_->numOfRecords);
}

template <typename etype>
void PackInt::RemoveNullsAndCompress(compress::NumCompressor<etype> &nc, char *tmp_comp_buffer, uint &tmp_cb_len,
                                     uint64_t &maxv) {
  mm::MMGuard<etype> tmp_data;
  if (dpn_->numOfNulls > 0) {
    tmp_data = mm::MMGuard<etype>(static_cast<etype *>(alloc((dpn_->numOfRecords - dpn_->numOfNulls) * sizeof(etype),
                                                             mm::BLOCK_TYPE::BLOCK_TEMPORARY)),
                                  *this);
    for (uint i = 0, d = 0; i < dpn_->numOfRecords; i++) {
      if (!IsNull(i))
        tmp_data[d++] = (static_cast<etype *>(data_.ptr_))[i];
    }
  } else
    tmp_data = mm::MMGuard<etype>(static_cast<etype *>(data_.ptr_), *this, false);

  CprsErr res =
      nc.Compress(tmp_comp_buffer, tmp_cb_len, tmp_data.get(), dpn_->numOfRecords - dpn_->numOfNulls, (etype)(maxv));
  if (res != CprsErr::CPRS_SUCCESS) {
    std::stringstream msg_buf;
    msg_buf << "Compression of numerical values failed for column " << (pc_column(GetCoordinate().co.pack) + 1)
            << ", pack " << (pc_dp(GetCoordinate().co.pack) + 1) << " (error " << static_cast<int>(res) << ").";
    throw common::InternalException(msg_buf.str());
  }
}

template <typename etype>
void PackInt::DecompressAndInsertNulls(compress::NumCompressor<etype> &nc, uint *&cur_buf) {
  CprsErr res =
      nc.Decompress(data_.ptr_, reinterpret_cast<char *>((cur_buf + 3)), *cur_buf,
                    dpn_->numOfRecords - dpn_->numOfNulls, (etype) * (reinterpret_cast<uint64_t *>(cur_buf + 1)));
  if (res != CprsErr::CPRS_SUCCESS) {
    std::stringstream msg_buf;
    msg_buf << "Decompression of numerical values failed for column " << (pc_column(GetCoordinate().co.pack) + 1)
            << ", pack " << (pc_dp(GetCoordinate().co.pack) + 1) << " (error " << static_cast<int>(res) << ").";
    throw common::DatabaseException(msg_buf.str());
  }
  etype *d = (static_cast<etype *>(data_.ptr_)) + dpn_->numOfRecords - 1;
  etype *s = (static_cast<etype *>(data_.ptr_)) + dpn_->numOfRecords - dpn_->numOfNulls - 1;
  for (int i = dpn_->numOfRecords - 1; d > s; i--) {
    if (IsNull(i))
      --d;
    else
      *(d--) = *(s--);
  }
}

std::pair<PackInt::UniquePtr, size_t> PackInt::Compress() {
  uint *cur_buf = nullptr;
  size_t buffer_size = 0;
  mm::MMGuard<char> tmp_comp_buffer;

  uint tmp_cb_len = 0;
  SetModeDataCompressed();

  uint64_t maxv = 0;
  if (data_.ptr_) {  // else maxv remains 0
    uint64_t cv = 0;
    for (uint o = 0; o < dpn_->numOfRecords; o++) {
      if (!IsNull(o)) {
        cv = (uint64_t)GetValInt(o);
        if (cv > maxv)
          maxv = cv;
      }
    }
  }

  if (maxv != 0) {
    // ASSERT(last_set + 1 == dpn_->numOfRecords - dpn_->numOfNulls, "Expression evaluation
    // failed!");
    if (data_.value_type_ == 1) {
      compress::NumCompressor<uchar> nc;
      tmp_cb_len = (dpn_->numOfRecords - dpn_->numOfNulls) * sizeof(uchar) + 20;
      if (tmp_cb_len)
        tmp_comp_buffer = mm::MMGuard<char>(
            reinterpret_cast<char *>(alloc(tmp_cb_len * sizeof(char), mm::BLOCK_TYPE::BLOCK_TEMPORARY)), *this);
      RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
    } else if (data_.value_type_ == 2) {
      compress::NumCompressor<ushort> nc;
      tmp_cb_len = (dpn_->numOfRecords - dpn_->numOfNulls) * sizeof(ushort) + 20;
      if (tmp_cb_len)
        tmp_comp_buffer = mm::MMGuard<char>(
            reinterpret_cast<char *>(alloc(tmp_cb_len * sizeof(char), mm::BLOCK_TYPE::BLOCK_TEMPORARY)), *this);
      RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
    } else if (data_.value_type_ == 4) {
      compress::NumCompressor<uint> nc;
      tmp_cb_len = (dpn_->numOfRecords - dpn_->numOfNulls) * sizeof(uint) + 20;
      if (tmp_cb_len)
        tmp_comp_buffer = mm::MMGuard<char>(
            reinterpret_cast<char *>(alloc(tmp_cb_len * sizeof(char), mm::BLOCK_TYPE::BLOCK_TEMPORARY)), *this);
      RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
    } else {
      compress::NumCompressor<uint64_t> nc;
      tmp_cb_len = (dpn_->numOfRecords - dpn_->numOfNulls) * sizeof(uint64_t) + 20;
      if (tmp_cb_len)
        tmp_comp_buffer = mm::MMGuard<char>(
            reinterpret_cast<char *>(alloc(tmp_cb_len * sizeof(char), mm::BLOCK_TYPE::BLOCK_TEMPORARY)), *this);
      RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
    }
    buffer_size += tmp_cb_len;
  }
  buffer_size += 12;
  // compress nulls
  uint comp_null_buf_size = 0;
  mm::MMGuard<uchar> comp_null_buf;
  if (dpn_->numOfNulls > 0) {
    if (CompressedBitMap(comp_null_buf, comp_null_buf_size, nulls_ptr_, dpn_->numOfNulls)) {
      SetModeNullsCompressed();
    } else {
      ResetModeNullsCompressed();
    }
  }
  uint comp_delete_buf_size = 0;
  mm::MMGuard<uchar> comp_delete_buf;
  if (dpn_->numOfDeleted > 0) {
    if (CompressedBitMap(comp_delete_buf, comp_delete_buf_size, deletes_ptr_, dpn_->numOfDeleted)) {
      SetModeDeletesCompressed();
    } else {
      ResetModeDeletesCompressed();
    }
  }

  buffer_size += (comp_null_buf_size > 0 ? sizeof(ushort) + comp_null_buf_size : 0);
  buffer_size += (comp_delete_buf_size > 0 ? sizeof(ushort) + comp_delete_buf_size : 0);

  UniquePtr ptr = alloc_ptr(buffer_size, mm::BLOCK_TYPE::BLOCK_COMPRESSED);
  uchar *compressed_buf = reinterpret_cast<uchar *>(ptr.get());
  std::memset(compressed_buf, 0, buffer_size);
  cur_buf = reinterpret_cast<uint *>(compressed_buf);

  if (dpn_->numOfNulls > 0) {
    if (comp_null_buf_size > bitmap_size_)
      throw common::DatabaseException("Unexpected bytes found (PackInt::Compress).");

    *reinterpret_cast<ushort *>(compressed_buf) = (ushort)(comp_null_buf_size);
    std::memcpy(compressed_buf + sizeof(ushort), comp_null_buf.get(), comp_null_buf_size);
    cur_buf = reinterpret_cast<uint *>(compressed_buf + comp_null_buf_size + sizeof(ushort));
    compressed_buf = reinterpret_cast<uchar *>(cur_buf);
  }

  if (dpn_->numOfDeleted > 0) {
    if (comp_delete_buf_size > bitmap_size_)
      throw common::DatabaseException("Unexpected bytes found (PackInt::Compress).");

    *reinterpret_cast<ushort *>(compressed_buf) = (ushort)(comp_delete_buf_size);
    std::memcpy(compressed_buf + sizeof(ushort), comp_delete_buf.get(), comp_delete_buf_size);
    cur_buf = reinterpret_cast<uint *>(compressed_buf + comp_delete_buf_size + sizeof(ushort));
  }

  *cur_buf = tmp_cb_len;
  *reinterpret_cast<uint64_t *>(cur_buf + 1) = maxv;
  std::memcpy(cur_buf + 3, tmp_comp_buffer.get(), tmp_cb_len);
  return std::make_pair(std::move(ptr), buffer_size);
}
}  // namespace core
}  // namespace Tianmu
