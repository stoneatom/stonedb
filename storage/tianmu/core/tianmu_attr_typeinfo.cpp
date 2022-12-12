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

#include "tianmu_attr_typeinfo.h"
#include "common/data_format.h"
#include "common/txt_data_format.h"
#include "core/tianmu_attr.h"
#include "types/tianmu_data_types.h"
#include "types/tianmu_num.h"

namespace Tianmu {
namespace core {
int ATI::TextSize(common::ColumnType attrt, uint precision, int scale, DTCollation collation) {
  return common::TxtDataFormat::StaticExtrnalSize(attrt, precision, scale, &collation);
}

const types::TianmuDataType &AttributeTypeInfo::ValuePrototype() const {
  if (Lookup() || ATI::IsNumericType(attrt_))
    return types::TianmuNum::NullValue();
  if (ATI::IsStringType(attrt_))
    return types::BString::NullValue();
  DEBUG_ASSERT(ATI::IsDateTimeType(attrt_));
  return types::TianmuDateTime::NullValue();
}
}  // namespace core
}  // namespace Tianmu
