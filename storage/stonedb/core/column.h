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
#ifndef STONEDB_CORE_COLUMN_H_
#define STONEDB_CORE_COLUMN_H_
#pragma once

#include "common/common_definitions.h"
#include "core/column_type.h"
#include "types/rc_data_types.h"

namespace stonedb {
namespace core {
enum class PackOntologicalStatus { NULLS_ONLY, UNIFORM, UNIFORM_AND_NULLS, SEQUENTIAL, NORMAL };

/*! \brief Base class for columns.
 *
 * Defines the common interface for RCAttr, Attr and vcolumn::VirtualColumn
 */
class Column {
 public:
  Column(ColumnType ct = ColumnType()) : ct(ct) {}
  Column(const Column &c) {
    if (this != &c) *this = c;
  }

  inline const ColumnType &Type() const { return ct; }
  inline common::CT TypeName() const { return ct.GetTypeName(); }
  inline void SetTypeName(common::CT type) { ct.SetTypeName(type); }
  void SetCollation(DTCollation collation) { ct.SetCollation(collation); }
  DTCollation GetCollation() { return ct.GetCollation(); }

 protected:
  ColumnType ct;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_COLUMN_H_
