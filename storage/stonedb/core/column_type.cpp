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

#include <cstring>

#include "core/column_type.h"
#include "core/data_type.h"
#include "core/quick_math.h"

namespace stonedb {
namespace core {
ColumnType::ColumnType(const DataType &dt) {
  if (dt.IsFixed()) {
    ColumnType ct(dt.attrtype, false, common::PackFmt::DEFAULT, QuickMath::precision10(dt.fixmax), dt.fixscale);
    std::swap(ct, *this);
  } else if (dt.IsFloat()) {
    ColumnType ct(common::CT::REAL, false, common::PackFmt::DEFAULT, QuickMath::precision10(dt.fixmax), -1);
    std::swap(ct, *this);
  } else if (dt.IsString()) {
    ColumnType ct(dt.attrtype, false, common::PackFmt::DEFAULT, dt.precision, 0, dt.collation);
    std::swap(ct, *this);
  } else if (dt.IsDateTime()) {
    ColumnType ct(dt.attrtype, false, common::PackFmt::DEFAULT, dt.precision, 0, dt.collation);
    std::swap(ct, *this);
  } else
    STONEDB_ERROR("invalid conversion DataType -> ColumnType");
}

bool ColumnType::operator==(const ColumnType &ct2) const {
  if (type == ct2.type && IsLookup() == ct2.IsLookup() &&
      std::strcmp(collation.collation->csname, ct2.collation.collation->csname) == 0 &&
      (type != common::CT::NUM || (type == common::CT::NUM && precision == ct2.precision && scale == ct2.scale)))
    return true;
  else
    return false;
}

uint ColumnType::InternalSize() {
  if (IsLookup())
    return 4;
  else if (ATI::IsStringType(type))
    return precision;
  else if (type == common::CT::INT || type == common::CT::MEDIUMINT)
    return 4;
  else if (type == common::CT::SMALLINT)
    return 2;
  else if (type == common::CT::BYTEINT || type == common::CT::YEAR)
    return 1;
  return 8;
}

ColumnType ColumnType::RemovedLookup() const {
  ColumnType noLookup(*this);
  noLookup.fmt = common::PackFmt::DEFAULT;
  return noLookup;
}
}  // namespace core
}  // namespace stonedb