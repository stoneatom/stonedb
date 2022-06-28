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

#include "rc_attr_typeinfo.h"
#include "common/data_format.h"
#include "common/txt_data_format.h"
#include "core/rc_attr.h"
#include "types/rc_data_types.h"
#include "types/rc_num.h"

namespace stonedb {
namespace core {
int ATI::TextSize(common::CT attrt, uint precision, int scale, DTCollation collation) {
  return common::TxtDataFormat::StaticExtrnalSize(attrt, precision, scale, &collation);
}

const types::RCDataType &AttributeTypeInfo::ValuePrototype() const {
  if (Lookup() || ATI::IsNumericType(attrt)) return types::RCNum::NullValue();
  if (ATI::IsStringType(attrt)) return types::BString::NullValue();
  DEBUG_ASSERT(ATI::IsDateTimeType(attrt));
  return types::RCDateTime::NullValue();
}
}  // namespace core
}  // namespace stonedb
