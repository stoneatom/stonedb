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
#include "mm/mm_guard.h"
#include "mm/traceable_object.h"
#include "types/tianmu_data_types.h"

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
  virtual void DeleteByRow(size_t i) = 0;

  virtual int64_t GetValInt(int n) const;
  virtual double GetValDouble(int n) const;
  virtual types::BString GetValueBinary(int i) const;

  void SetNull(int locationInPack) {
    int mask = 1 << (locationInPack % 32);
    ASSERT((nulls_ptr_[locationInPack >> 5] & mask) == 0);
    nulls_ptr_[locationInPack >> 5] |= mask;
  }

  void UnsetNull(int locationInPack) {
    int mask = ~(1 << (locationInPack % 32));
    ASSERT(IsNull(locationInPack), "already null!");
    nulls_ptr_[locationInPack >> 5] &= mask;
  }

  bool IsNull(int locationInPack) const {
    if (dpn_->numOfNulls == dpn_->numOfRecords)
      return true;
    return ((nulls_ptr_[locationInPack >> 5] & ((uint32_t)(1) << (locationInPack % 32))) != 0);
  }

  void SetDeleted(int locationInPack) {
    int mask = 1 << (locationInPack % 32);
    ASSERT((deletes_ptr_[locationInPack >> 5] & mask) == 0);
    deletes_ptr_[locationInPack >> 5] |= mask;
  }

  void UnsetDeleted(int locationInPack) {
    int mask = ~(1 << (locationInPack % 32));
    ASSERT(IsDeleted(locationInPack), "already deleted!");
    deletes_ptr_[locationInPack >> 5] &= mask;
  }

  // If the line in the package has been deleted, return true; otherwise, return false
  bool IsDeleted(int locationInPack) const {
    if (dpn_->numOfDeleted == dpn_->numOfRecords)
      return true;
    return ((deletes_ptr_[locationInPack >> 5] & ((uint32_t)(1) << (locationInPack % 32))) != 0);
  }

  bool NotNull(int locationInPack) const { return !IsNull(locationInPack); }
  void InitNull() {
    if (dpn_->NullOnly()) {
      for (uint i = 0; i < dpn_->numOfNulls; i++) SetNull(i);
    }
  }
  PackCoordinate GetPackCoordinate() const { return m_coord.co.pack; }
  void SetDPN(DPN *new_dpn) { dpn_ = new_dpn; }

  // Compress bitmap
  bool CompressedBitMap(mm::MMGuard<uchar> &comp_buf, uint &comp_buf_size, std::unique_ptr<uint32_t[]> &ptr_buf,
                        uint32_t &dpn_num1);

 protected:
  Pack(DPN *dpn, PackCoordinate pc, ColumnShare *col_share);
  Pack(const Pack &ap, const PackCoordinate &pc);
  virtual std::pair<UniquePtr, size_t> Compress() = 0;
  virtual void Destroy() = 0;

  bool ShouldNotCompress() const;
  bool IsModeNullsCompressed() const { return dpn_->null_compressed; }
  bool IsModeDeletesCompressed() const { return dpn_->delete_compressed; }
  bool IsModeDataCompressed() const { return dpn_->data_compressed; }
  bool IsModeCompressionApplied() const { return IsModeDataCompressed() || IsModeNullsCompressed(); }
  bool IsModeNoCompression() const { return dpn_->no_compress; }
  void ResetModeNoCompression() { dpn_->no_compress = 0; }
  void ResetModeCompressed() {
    dpn_->null_compressed = 0;
    dpn_->data_compressed = 0;
    dpn_->delete_compressed = 0;
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

  void SetModeDeletesCompressed() {
    ResetModeNoCompression();
    dpn_->delete_compressed = 1;
  }
  void ResetModeDeletesCompressed() { dpn_->delete_compressed = 0; }

 protected:
  ColumnShare *col_share_ = nullptr;
  size_t bitmap_size_;
  DPN *dpn_ = nullptr;

  /*
  The actual storage form of a bitmap is an array of type int32.
  The principle is to use the 32-bit space occupied by a value of type int32 to
  store and record the states of these 32 values using 0 or 1.
  The total number of bits in the bitmap is equal to the total number of rows in the pack,
  and the position of the data in the pack and the position in the bitmap are also one-to-one correspondence
  This can effectively save space.
  */
  std::unique_ptr<uint32_t[]> nulls_ptr_;    // Null bitmap
  std::unique_ptr<uint32_t[]> deletes_ptr_;  // deleted bitmap
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_PACK_H_
