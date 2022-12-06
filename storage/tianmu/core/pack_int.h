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
#ifndef TIANMU_CORE_PACK_INT_H_
#define TIANMU_CORE_PACK_INT_H_
#pragma once

#include "core/pack.h"
#include "core/value.h"

namespace Tianmu {

namespace system {
class Stream;
}  // namespace system

namespace loader {
class ValueCache;
}  // namespace loader

namespace compress {
template <class T>
class NumCompressor;
}  // namespace compress

namespace core {
class ColumnShare;

class PackInt final : public Pack {
 public:
  PackInt(DPN *dpn, PackCoordinate pc, ColumnShare *s);
  ~PackInt();

  // overrides
  std::unique_ptr<Pack> Clone(const PackCoordinate &pc) const override;
  void LoadDataFromFile(system::Stream *fcurfile) override;
  void Save() override;
  void UpdateValue(size_t locationInPack, const Value &v) override;
  void DeleteByRow(size_t locationInPack) override;

  void LoadValues(const loader::ValueCache *vc, const std::optional<common::double_int_t> &null_value);
  int64_t GetValInt(int locationInPack) const override { return data_[locationInPack]; }
  double GetValDouble(int locationInPack) const override {
    ASSERT(is_real_);
    return data_.ptr_double_[locationInPack];
  }
  bool IsFixed() const { return !is_real_; }

 protected:
  std::pair<UniquePtr, size_t> Compress() override;
  void Destroy() override;

 private:
  PackInt(const PackInt &apn, const PackCoordinate &pc);

  void AppendValue(uint64_t v) {
    dpn_->numOfRecords++;
    SetVal64(dpn_->numOfRecords - 1, v);
  }

  void AppendNull() {
    SetNull(dpn_->numOfRecords);
    dpn_->numOfNulls++;
    dpn_->numOfRecords++;
  }
  void SetValD(uint n, double v) {
    dpn_->synced = false;
    ASSERT(n < dpn_->numOfRecords);
    ASSERT(is_real_);
    data_.ptr_double_[n] = v;
  }
  void SetVal64(uint n, uint64_t v) {
    dpn_->synced = false;
    ASSERT(n < dpn_->numOfRecords);
    switch (data_.value_type_) {
      case 8:
        data_.ptr_int64_[n] = v;
        return;
      case 4:
        data_.ptr_int32_[n] = v;
        return;
      case 2:
        data_.ptr_int16_[n] = v;
        return;
      case 1:
        data_.ptr_int8_[n] = v;
        return;
      default:
        TIANMU_ERROR("bad value type in pakcN");
    }
  }
  void SetVal64(uint n, const Value &v) {
    if (v.HasValue())
      SetVal64(n, v.GetInt());
    else
      SetNull(n);
  }
  void UpdateValueFloat(size_t locationInPack, const Value &v);
  void UpdateValueFixed(size_t locationInPack, const Value &v);
  void ExpandOrShrink(uint64_t maxv, int64_t delta);
  void SaveCompressed(system::Stream *fcurfile);
  void SaveUncompressed(system::Stream *fcurfile);
  void LoadValuesDouble(const loader::ValueCache *vc, const std::optional<common::double_int_t> &nv);
  void LoadValuesFixed(const loader::ValueCache *vc, const std::optional<common::double_int_t> &nv);

  uint8_t GetValueSize(uint64_t v) const;

  template <typename etype>
  void DecompressAndInsertNulls(compress::NumCompressor<etype> &nc, uint *&cur_buf);
  template <typename etype>
  void RemoveNullsAndCompress(compress::NumCompressor<etype> &nc, char *tmp_comp_buffer, uint &tmp_cb_len,
                              uint64_t &maxv);

  bool is_real_ = false;
  struct {
    int64_t operator[](size_t n) const {
      switch (value_type_) {
        case 8:
          return ptr_int64_[n];
        case 4:
          return ptr_int32_[n];
        case 2:
          return ptr_int16_[n];
        case 1:
          return ptr_int8_[n];
        default:
          TIANMU_ERROR("bad value type in pakcN");
      }
    }
    bool empty() const { return ptr_ == nullptr; }

   public:
    unsigned char value_type_;
    union {
      uint8_t *ptr_int8_;
      uint16_t *ptr_int16_;
      uint32_t *ptr_int32_;
      uint64_t *ptr_int64_;
      double *ptr_double_;
      void *ptr_;
    };
  } data_ = {};
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_PACK_INT_H_
