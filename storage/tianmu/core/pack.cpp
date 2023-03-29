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

#include "core/column_share.h"
#include "core/data_cache.h"
#include "core/tools.h"

namespace Tianmu {
namespace core {
Pack::Pack(DPN *dpn, PackCoordinate pc, ColumnShare *col_share) : col_share_(col_share), dpn_(dpn) {
  bitmapSize_ = (1 << col_share_->pss) / 8;
  nulls_ptr_ = std::make_unique<uint32_t[]>(bitmapSize_ / sizeof(uint32_t));
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
  bitmapSize_ = ap.bitmapSize_;
  nulls_ptr_ = std::make_unique<uint32_t[]>(bitmapSize_ / sizeof(uint32_t));
  std::memcpy(nulls_ptr_.get(), ap.nulls_ptr_.get(), bitmapSize_);
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
}  // namespace core
}  // namespace Tianmu
