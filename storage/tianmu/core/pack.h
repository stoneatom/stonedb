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
#ifndef TIANMU_CORE_PACK_H_
#define TIANMU_CORE_PACK_H_
#pragma once

#include "core/dpn.h"
#include "core/tools.h"
#include "mm/traceable_object.h"
#include "types/rc_data_types.h"

namespace Tianmu {
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
//  1 - unloaded (on disc)
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
    ASSERT((nulls_ptr_[i >> 5] & mask) == 0);
    nulls_ptr_[i >> 5] |= mask;
  }

  void UnsetNull(int i) {
    int mask = ~(1 << (i % 32));
    ASSERT(IsNull(i), "already null!");
    nulls_ptr_[i >> 5] &= mask;
  }

  bool IsNull(int i) const {
    if (dpn_->nn == dpn_->nr)
      return true;
    return ((nulls_ptr_[i >> 5] & ((uint32_t)(1) << (i % 32))) != 0);
  }
  bool NotNull(int i) const { return !IsNull(i); }
  void InitNull() {
    if (dpn_->NullOnly()) {
      for (uint i = 0; i < dpn_->nn; i++) SetNull(i);
    }
  }
  PackCoordinate GetPackCoordinate() const { return m_coord.co.pack; }
  void SetDPN(DPN *new_dpn_) { dpn_ = new_dpn_; }

 protected:
  Pack(DPN *dpn_, PackCoordinate pc, ColumnShare *col_share);
  Pack(const Pack &ap, const PackCoordinate &pc);
  virtual std::pair<UniquePtr, size_t> Compress() = 0;
  virtual void Destroy() = 0;

  bool ShouldNotCompress() const;
  bool IsModeNullsCompressed() const { return dpn_->null_compressed; }
  bool IsModeDataCompressed() const { return dpn_->data_compressed; }
  bool IsModeCompressionApplied() const { return IsModeDataCompressed() || IsModeNullsCompressed(); }
  bool IsModeNoCompression() const { return dpn_->no_compress; }
  void ResetModeNoCompression() { dpn_->no_compress = 0; }
  void ResetModeCompressed() {
    dpn_->null_compressed = 0;
    dpn_->data_compressed = 0;
  }
  void SetModeNoCompression() {
    ResetModeCompressed();
    dpn_->no_compress = 1;
  }
  void SetModeDataCompressed() {
    ResetModeNoCompression();
    dpn_->data_compressed = 1;
  }
  void SetModeNullsCompressed() {
    ResetModeNoCompression();
    dpn_->null_compressed = 1;
  }
  void ResetModeNullsCompressed() { dpn_->null_compressed = 0; }

 protected:
  ColumnShare *col_share_ = nullptr;
  size_t kNullSize_;
  DPN *dpn_ = nullptr;

  std::unique_ptr<uint32_t[]> nulls_ptr_;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_PACK_H_
