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

#include "core/pack.h"
#include "compress/bit_stream_compressor.h"
#include "compress/num_compressor.h"
#include "core/bin_tools.h"
#include "core/column_share.h"
#include "core/data_cache.h"
#include "core/tools.h"
#include "core/value.h"
#include "loader/value_cache.h"
#include "system/tianmu_file.h"

namespace Tianmu {
namespace core {
Pack::Pack(DPN *dpn, PackCoordinate pc, ColumnShare *col_share) : col_share_(col_share), dpn_(dpn) {
  bitmap_size_ = (1 << col_share->pss) / 8;
  nulls_ptr_ = std::make_unique<uint32_t[]>(bitmap_size_ / sizeof(uint32_t));
  deletes_ptr_ = std::make_unique<uint32_t[]>(bitmap_size_ / sizeof(uint32_t));
  // nulls MUST be initialized in the constructor, there are 3 cases in total:
  //   1. All values are nullptr. It is initialized here by InitNull();
  //   2. All values are uniform. Then it would be all zeros already.
  //   3. Otherwise. It would be loaded from disk by PackInt() or PackStr().
  InitNull();
  m_coord.ID = COORD_TYPE::PACK;
  m_coord.co.pack = pc;
}

Pack::Pack(const Pack &ap, const PackCoordinate &pc)
    : mm::TraceableObject(ap), col_share_(ap.col_share_), dpn_(ap.dpn_) {
  m_coord.ID = COORD_TYPE::PACK;
  m_coord.co.pack = pc;
  bitmap_size_ = ap.bitmap_size_;
  nulls_ptr_ = std::make_unique<uint32_t[]>(bitmap_size_ / sizeof(uint32_t));
  deletes_ptr_ = std::make_unique<uint32_t[]>(bitmap_size_ / sizeof(uint32_t));
  std::memcpy(nulls_ptr_.get(), ap.nulls_ptr_.get(), bitmap_size_);
  std::memcpy(deletes_ptr_.get(), ap.deletes_ptr_.get(), bitmap_size_);
}

int64_t Pack::GetValInt([[maybe_unused]] int n) const {
  TIANMU_ERROR("Not implemented");
  return 0;
}

double Pack::GetValDouble([[maybe_unused]] int n) const {
  TIANMU_ERROR("Not implemented");
  return 0;
}

types::BString Pack::GetValueBinary([[maybe_unused]] int n) const {
  TIANMU_ERROR("Not implemented");
  return 0;
}

void Pack::Release() {
  if (owner)
    owner->DropObjectByMM(GetPackCoordinate());
}

bool Pack::ShouldNotCompress() const {
  return (dpn_->numOfRecords < (1U << col_share_->pss)) ||
         (col_share_->ColType().GetFmt() == common::PackFmt::NOCOMPRESS);
}

bool Pack::CompressedBitMap(mm::MMGuard<uchar> &comp_buf, uint &comp_buf_size, std::unique_ptr<uint32_t[]> &ptr_buf,
                            uint32_t &dpn_num1) {
  // Number of bits in bytes
  int bitsInBytes = 8;
  // Fill in values to prevent boundary errors
  int padding = bitsInBytes - 1;
  // Because the maximum size of dpn_->numofrecords is 65536, the buffer used by bitmaps is also limited
  comp_buf_size = ((dpn_->numOfRecords + padding) / bitsInBytes);

  comp_buf = mm::MMGuard<uchar>(
      static_cast<uchar *>(alloc((comp_buf_size + sizeof(ushort)) * sizeof(char), mm::BLOCK_TYPE::BLOCK_TEMPORARY)),
      *this);
  uint cnbl = comp_buf_size + 1;
  comp_buf[cnbl] = 0xBA;  // just checking - buffer overrun
  compress::BitstreamCompressor bsc;
  CprsErr res = bsc.Compress(reinterpret_cast<char *>(comp_buf.get()), comp_buf_size,
                             reinterpret_cast<char *>(ptr_buf.get()), dpn_->numOfRecords, dpn_num1);
  if (comp_buf[cnbl] != 0xBA) {
    TIANMU_LOG(LogCtl_Level::ERROR, "buffer overrun by BitstreamCompressor (N f).");
    ASSERT(0, "ERROR: buffer overrun by BitstreamCompressor (N f).");
  }
  if (res == CprsErr::CPRS_SUCCESS)
    return true;
  else if (res == CprsErr::CPRS_ERR_BUF) {
    comp_buf = mm::MMGuard<uchar>(reinterpret_cast<uchar *>(ptr_buf.get()), *this, false);
    comp_buf_size = ((dpn_->numOfRecords + padding) / bitsInBytes);
    return false;
  } else {
    throw common::DatabaseException("Compression of nulls or deletes failed for column " +
                                    std::to_string(pc_column(GetCoordinate().co.pack) + 1) + ", pack " +
                                    std::to_string(pc_dp(GetCoordinate().co.pack) + 1) + " (error " +
                                    std::to_string(static_cast<int>(res)) + ").");
  }
}

}  // namespace core
}  // namespace Tianmu
