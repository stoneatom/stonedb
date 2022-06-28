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

#include "types/rc_num.h"

namespace stonedb {
namespace types {
RCValueObject::RCValueObject() {}

RCValueObject::RCValueObject(const RCValueObject &rcvo) {
  if (rcvo.value.get()) construct(*rcvo.value);
}

RCValueObject::RCValueObject(const RCDataType &rcdt) { construct(rcdt); }

RCValueObject::~RCValueObject() {}

RCValueObject &RCValueObject::operator=(const RCValueObject &rcvo) {
  if (rcvo.value.get())
    construct(*rcvo.value);
  else
    value.reset();
  return *this;
}

inline void RCValueObject::construct(const RCDataType &rcdt) { value = rcdt.Clone(); }

bool RCValueObject::compare(const RCValueObject &rcvo1, const RCValueObject &rcvo2, common::Operator op,
                            char like_esc) {
  if (rcvo1.IsNull() || rcvo2.IsNull())
    return false;
  else
    return RCDataType::compare(*rcvo1.value, *rcvo2.value, op, like_esc);
}

bool RCValueObject::compare(const RCValueObject &rcvo, common::Operator op, char like_esc) const {
  return compare(*this, rcvo, op, like_esc);
}

bool RCValueObject::operator==(const RCValueObject &rcvo) const {
  if (IsNull() || rcvo.IsNull()) return false;
  return *value == *rcvo.value;
}

bool RCValueObject::operator<(const RCValueObject &rcvo) const {
  if (IsNull() || rcvo.IsNull()) return false;
  return *value < *rcvo.value;
}

bool RCValueObject::operator>(const RCValueObject &rcvo) const {
  if (IsNull() || rcvo.IsNull()) return false;
  return *value > *rcvo.value;
}

bool RCValueObject::operator>=(const RCValueObject &rcvo) const {
  if (IsNull() || rcvo.IsNull()) return false;
  return *value >= *rcvo.value;
}

bool RCValueObject::operator<=(const RCValueObject &rcvo) const {
  if (IsNull() || rcvo.IsNull()) return false;
  return *value <= *rcvo.value;
}

bool RCValueObject::operator!=(const RCValueObject &rcvo) const {
  if (IsNull() || rcvo.IsNull()) return false;
  return *value != *rcvo.value;
}

bool RCValueObject::operator==(const RCDataType &rcn) const {
  if (IsNull() || rcn.IsNull()) return false;
  return *value == rcn;
}

bool RCValueObject::operator<(const RCDataType &rcn) const {
  if (IsNull() || rcn.IsNull()) return false;
  return *value < rcn;
}

bool RCValueObject::operator>(const RCDataType &rcn) const {
  if (IsNull() || rcn.IsNull()) return false;
  return *value > rcn;
}

bool RCValueObject::operator>=(const RCDataType &rcn) const {
  if (IsNull() || rcn.IsNull()) return false;
  return *value >= rcn;
}

bool RCValueObject::operator<=(const RCDataType &rcdt) const {
  if (IsNull() || rcdt.IsNull()) return false;
  return *value <= rcdt;
}

bool RCValueObject::operator!=(const RCDataType &rcn) const {
  if (IsNull() || rcn.IsNull()) return false;
  return *value != rcn;
}

bool RCValueObject::IsNull() const { return value.get() ? value->IsNull() : true; }

RCDataType &RCValueObject::operator*() const { return value.get() ? *value.get() : RCNum::NullValue(); }

RCValueObject::operator RCNum &() const {
  if (IsNull()) return RCNum::NullValue();
  if (GetValueType() == ValueTypeEnum::NUMERIC_TYPE || GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return static_cast<RCNum &>(*value);
  STONEDB_ERROR("Bad cast in RCValueObject::RCNum&()");
  return static_cast<RCNum &>(*value);
}

RCValueObject::operator RCDateTime &() const {
  if (IsNull()) return RCDateTime::NullValue();
  if (GetValueType() == ValueTypeEnum::DATE_TIME_TYPE) return static_cast<RCDateTime &>(*value);
  STONEDB_ERROR("Bad cast in RCValueObject::RCDateTime&()");
  return static_cast<RCDateTime &>(*value);
}

BString RCValueObject::ToBString() const {
  if (IsNull()) return BString();
  return value->ToBString();
}

uint RCValueObject::GetHashCode() const {
  if (IsNull()) return 0;
  return value->GetHashCode();
}
}  // namespace types
}  // namespace stonedb
