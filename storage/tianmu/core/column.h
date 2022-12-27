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
#ifndef TIANMU_CORE_COLUMN_H_
#define TIANMU_CORE_COLUMN_H_
#pragma once

#include "common/common_definitions.h"
#include "core/column_type.h"
#include "types/tianmu_data_types.h"

namespace Tianmu {
namespace core {
enum class PackOntologicalStatus { NULLS_ONLY = 0, UNIFORM, UNIFORM_AND_NULLS, SEQUENTIAL, NORMAL };

/*! \brief Base class for columns.
 *
 * Defines the common interface for TianmuAttr, Attr and vcolumn::VirtualColumn
 */
class Column {
 public:
  Column(ColumnType ct = ColumnType()) : ct(ct) {}
  Column(const Column &c) {
    if (this != &c)
      *this = c;
  }
  constexpr Column &operator=(const Column &) = default;
  inline const ColumnType &Type() const { return ct; }
  inline common::ColumnType TypeName() const { return ct.GetTypeName(); }
  inline void SetTypeName(common::ColumnType type) { ct.SetTypeName(type); }
  void SetCollation(DTCollation collation) { ct.SetCollation(collation); }
  DTCollation GetCollation() { return ct.GetCollation(); }

 protected:
  ColumnType ct;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_COLUMN_H_
