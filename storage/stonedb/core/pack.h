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
#ifndef STONEDB_CORE_PACK_H_
#define STONEDB_CORE_PACK_H_
#pragma once

#include "core/dpn.h"
#include "core/tools.h"
#include "mm/traceable_object.h"
#include "types/rc_data_types.h"

namespace stonedb {
namespace system {
class Stream;
}  // namespace system
namespace core {
class ColumnShare;
class Value;

// table of modes:
//  0 - trivial data: all values are derivable from the statistics, or nulls
//  only,
//      the pack physically doesn't exist, only statistics
//  1 - unloaded (on disk)
//  2 - loaded to memory
//  3 - no data yet, empty pack

class Pack : public mm::TraceableObject {
 public:
  virtual ~Pack() = default;

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_PACK; }
  void Release() override;

  virtual std::unique_ptr<Pack> Clone(const PackCoordinate &pc) const = 0;
  virtual void LoadDataFromFile(system::Stream *fcurfile) = 0;
  virtual void Save() = 0;
  virtual void UpdateValue(size_t i, const Value &v) = 0;

  virtual int64_t GetValInt(int n) const;
  virtual double GetValDouble(int n) const;
  virtual types::BString GetValueBinary(int i) const;

  void SetNull(int i) {
    int mask = 1 << (i % 32);
    ASSERT((nulls[i >> 5] & mask) == 0);
    nulls[i >> 5] |= mask;
  }

  void UnsetNull(int i) {
    int mask = ~(1 << (i % 32));
    ASSERT(IsNull(i), "already null!");
    nulls[i >> 5] &= mask;
  }

  bool IsNull(int i) const {
    if (dpn->nn == dpn->nr) return true;
    return ((nulls[i >> 5] & ((uint32_t)(1) << (i % 32))) != 0);
  }
  bool NotNull(int i) const { return !IsNull(i); }
  void InitNull() {
    if (dpn->NullOnly()) {
      for (uint i = 0; i < dpn->nn; i++) SetNull(i);
    }
  }
  PackCoordinate GetPackCoordinate() const { return m_coord.co.pack; }
  void SetDPN(DPN *new_dpn) { dpn = new_dpn; }

 protected:
  Pack(DPN *dpn, PackCoordinate pc, ColumnShare *s);
  Pack(const Pack &ap, const PackCoordinate &pc);
  virtual std::pair<UniquePtr, size_t> Compress() = 0;
  virtual void Destroy() = 0;

  bool ShouldNotCompress() const;
  bool IsModeNullsCompressed() const { return dpn->null_compressed; }
  bool IsModeDataCompressed() const { return dpn->data_compressed; }
  bool IsModeCompressionApplied() const { return IsModeDataCompressed() || IsModeNullsCompressed(); }
  bool IsModeNoCompression() const { return dpn->no_compress; }
  void ResetModeNoCompression() { dpn->no_compress = 0; }
  void ResetModeCompressed() {
    dpn->null_compressed = 0;
    dpn->data_compressed = 0;
  }
  void SetModeNoCompression() {
    ResetModeCompressed();
    dpn->no_compress = 1;
  }
  void SetModeDataCompressed() {
    ResetModeNoCompression();
    dpn->data_compressed = 1;
  }
  void SetModeNullsCompressed() {
    ResetModeNoCompression();
    dpn->null_compressed = 1;
  }
  void ResetModeNullsCompressed() { dpn->null_compressed = 0; }

 protected:
  ColumnShare *s = nullptr;
  size_t NULLS_SIZE;
  DPN *dpn = nullptr;

  std::unique_ptr<uint32_t[]> nulls;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_PACK_H_
