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
#ifndef STONEDB_CORE_PACK_INT_H_
#define STONEDB_CORE_PACK_INT_H_
#pragma once

#include "core/pack.h"
#include "core/value.h"

namespace stonedb {

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
  void UpdateValue(size_t i, const Value &v) override;

  void LoadValues(const loader::ValueCache *vc, const std::optional<common::double_int_t> &null_value);
  int64_t GetValInt(int n) const override { return data[n]; }
  double GetValDouble(int n) const override {
    ASSERT(is_real);
    return data.pdouble[n];
  }
  bool IsFixed() const { return !is_real; }

 protected:
  std::pair<UniquePtr, size_t> Compress() override;
  void Destroy() override;

 private:
  PackInt(const PackInt &apn, const PackCoordinate &pc);

  void AppendValue(uint64_t v) {
    dpn->nr++;
    SetVal64(dpn->nr - 1, v);
  }

  void AppendNull() {
    SetNull(dpn->nr);
    dpn->nn++;
    dpn->nr++;
  }
  void SetValD(uint n, double v) {
    dpn->synced = false;
    ASSERT(n < dpn->nr);
    ASSERT(is_real);
    data.pdouble[n] = v;
  }
  void SetVal64(uint n, uint64_t v) {
    dpn->synced = false;
    ASSERT(n < dpn->nr);
    switch (data.vt) {
      case 8:
        data.pint64[n] = v;
        return;
      case 4:
        data.pint32[n] = v;
        return;
      case 2:
        data.pint16[n] = v;
        return;
      case 1:
        data.pint8[n] = v;
        return;
      default:
        STONEDB_ERROR("bad value type in pakcN");
    }
  }
  void SetVal64(uint n, const Value &v) {
    if (v.HasValue())
      SetVal64(n, v.GetInt());
    else
      SetNull(n);
  }
  void UpdateValueFloat(size_t i, const Value &v);
  void UpdateValueFixed(size_t i, const Value &v);
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

  bool is_real = false;
  struct {
    int64_t operator[](size_t n) const {
      switch (vt) {
        case 8:
          return pint64[n];
        case 4:
          return pint32[n];
        case 2:
          return pint16[n];
        case 1:
          return pint8[n];
        default:
          STONEDB_ERROR("bad value type in pakcN");
      }
    }
    bool empty() const { return ptr == nullptr; }

   public:
    unsigned char vt;
    union {
      uint8_t *pint8;
      uint16_t *pint16;
      uint32_t *pint32;
      uint64_t *pint64;
      double *pdouble;
      void *ptr;
    };
  } data = {};
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_PACK_INT_H_
