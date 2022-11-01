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
#ifndef TIANMU_VC_VIRTUAL_COLUMN_H_
#define TIANMU_VC_VIRTUAL_COLUMN_H_
#pragma once

#include "core/pack_guardian.h"
#include "vc/virtual_column_base.h"

namespace Tianmu {
namespace core {
class MIIterator;
}  // namespace core

namespace vcolumn {
/*! \brief A column defined by an expression (including a subquery) or
 * encapsulating a PhysicalColumn VirtualColumn is associated with an
 * core::MultiIndex object and cannot exist without it. Values contained in
 * VirtualColumn object may not exist physically, they can be computed on
 * demand.
 *
 */

class VirtualColumn : public VirtualColumnBase {
 public:
  VirtualColumn(core::ColumnType const &col_type, core::MultiIndex *multi_index)
      : VirtualColumnBase(col_type, multi_index), vc_pack_guard_(this) {}
  VirtualColumn(VirtualColumn const &vc) : VirtualColumnBase(vc), vc_pack_guard_(this) {}
  virtual ~VirtualColumn() { vc_pack_guard_.UnlockAll(); }

  void LockSourcePacks(const core::MIIterator &mit) override { vc_pack_guard_.LockPackrow(mit); }
  void UnlockSourcePacks() override { vc_pack_guard_.UnlockAll(); }

 private:
  core::VCPackGuardian vc_pack_guard_;
};
}  // namespace vcolumn

// TODO()
extern vcolumn::VirtualColumn *CreateVCCopy(vcolumn::VirtualColumn *vc);

}  // namespace Tianmu

#endif  // TIANMU_VC_VIRTUAL_COLUMN_H_
