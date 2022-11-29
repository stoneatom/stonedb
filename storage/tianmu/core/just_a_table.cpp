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

#include "just_a_table.h"

#include "common/assert.h"
#include "core/cq_term.h"
#include "core/filter.h"
#include "core/temp_table.h"

namespace Tianmu {
namespace core {
ValueOrNull JustATable::GetComplexValue(const int64_t obj, const int attr) {
  if (obj == common::NULL_VALUE_64 || IsNull(obj, attr))
    return ValueOrNull();

  ColumnType ct = GetColumnType(attr);
  if (ct.GetTypeName() == common::ColumnType::TIMESTAMP) {
    // needs to convert UTC/GMT time stored on server to time zone of client
    types::BString s;
    GetTable_S(s, obj, attr);
    MYSQL_TIME myt;
    MYSQL_TIME_STATUS not_used;
    // convert UTC timestamp given in string into TIME structure
    str_to_datetime(s.GetDataBytesPointer(), s.len_, &myt, TIME_DATETIME_ONLY, &not_used);
    return ValueOrNull(types::TianmuDateTime(myt, common::ColumnType::TIMESTAMP).GetInt64());
  }
  if (ct.IsFixed() || ct.IsFloat() || ct.IsDateTime())
    return ValueOrNull(GetTable64(obj, attr));
  if (ct.IsString()) {
    ValueOrNull val;
    types::BString s;
    GetTable_S(s, obj, attr);
    val.SetBString(s);
    return val;
  }
  throw common::Exception("Unrecognized data type in JustATable::GetComplexValue");
}
}  // namespace core
}  // namespace Tianmu
