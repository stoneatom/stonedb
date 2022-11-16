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

#include "types/tianmu_num.h"

namespace Tianmu {
namespace types {

TianmuValueObject::TianmuValueObject() {}

TianmuValueObject::TianmuValueObject(const TianmuValueObject &tianmu_value_obj) {
  if (tianmu_value_obj.value_.get())
    construct(*tianmu_value_obj.value_);
}

TianmuValueObject::TianmuValueObject(const TianmuDataType &tianmu_dt) { construct(tianmu_dt); }

TianmuValueObject::~TianmuValueObject() {}

TianmuValueObject &TianmuValueObject::operator=(const TianmuValueObject &tianmu_value_obj) {
  if (tianmu_value_obj.value_.get())
    construct(*tianmu_value_obj.value_);
  else
    value_.reset();
  return *this;
}

inline void TianmuValueObject::construct(const TianmuDataType &tianmu_dt) { value_ = tianmu_dt.Clone(); }

bool TianmuValueObject::compare(const TianmuValueObject &tianmu_value_obj1, const TianmuValueObject &tianmu_value_obj2,
                                common::Operator op, char like_esc) {
  if (tianmu_value_obj1.IsNull() || tianmu_value_obj2.IsNull())
    return false;
  else
    return TianmuDataType::compare(*tianmu_value_obj1.value_, *tianmu_value_obj2.value_, op, like_esc);
}

bool TianmuValueObject::compare(const TianmuValueObject &tianmu_value_obj, common::Operator op, char like_esc) const {
  return compare(*this, tianmu_value_obj, op, like_esc);
}

bool TianmuValueObject::operator==(const TianmuValueObject &tianmu_value_obj) const {
  if (IsNull() || tianmu_value_obj.IsNull())
    return false;
  return *value_ == *tianmu_value_obj.value_;
}

bool TianmuValueObject::operator<(const TianmuValueObject &tianmu_value_obj) const {
  if (IsNull() || tianmu_value_obj.IsNull())
    return false;
  return *value_ < *tianmu_value_obj.value_;
}

bool TianmuValueObject::operator>(const TianmuValueObject &tianmu_value_obj) const {
  if (IsNull() || tianmu_value_obj.IsNull())
    return false;
  return *value_ > *tianmu_value_obj.value_;
}

bool TianmuValueObject::operator>=(const TianmuValueObject &tianmu_value_obj) const {
  if (IsNull() || tianmu_value_obj.IsNull())
    return false;
  return *value_ >= *tianmu_value_obj.value_;
}

bool TianmuValueObject::operator<=(const TianmuValueObject &tianmu_value_obj) const {
  if (IsNull() || tianmu_value_obj.IsNull())
    return false;
  return *value_ <= *tianmu_value_obj.value_;
}

bool TianmuValueObject::operator!=(const TianmuValueObject &tianmu_value_obj) const {
  if (IsNull() || tianmu_value_obj.IsNull())
    return false;
  return *value_ != *tianmu_value_obj.value_;
}

bool TianmuValueObject::operator==(const TianmuDataType &tianmu_n) const {
  if (IsNull() || tianmu_n.IsNull())
    return false;
  return *value_ == tianmu_n;
}

bool TianmuValueObject::operator<(const TianmuDataType &tianmu_n) const {
  if (IsNull() || tianmu_n.IsNull())
    return false;
  return *value_ < tianmu_n;
}

bool TianmuValueObject::operator>(const TianmuDataType &tianmu_n) const {
  if (IsNull() || tianmu_n.IsNull())
    return false;
  return *value_ > tianmu_n;
}

bool TianmuValueObject::operator>=(const TianmuDataType &tianmu_n) const {
  if (IsNull() || tianmu_n.IsNull())
    return false;
  return *value_ >= tianmu_n;
}

bool TianmuValueObject::operator<=(const TianmuDataType &tianmu_dt) const {
  if (IsNull() || tianmu_dt.IsNull())
    return false;
  return *value_ <= tianmu_dt;
}

bool TianmuValueObject::operator!=(const TianmuDataType &tianmu_n) const {
  if (IsNull() || tianmu_n.IsNull())
    return false;
  return *value_ != tianmu_n;
}

bool TianmuValueObject::IsNull() const { return value_.get() ? value_->IsNull() : true; }

TianmuDataType &TianmuValueObject::operator*() const { return value_.get() ? *value_.get() : TianmuNum::NullValue(); }

TianmuValueObject::operator TianmuNum &() const {
  if (IsNull())
    return TianmuNum::NullValue();
  if (GetValueType() == ValueTypeEnum::NUMERIC_TYPE || GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return static_cast<TianmuNum &>(*value_);

  TIANMU_ERROR("Bad cast in TianmuValueObject::TianmuNum&()");
  return static_cast<TianmuNum &>(*value_);
}

TianmuValueObject::operator TianmuDateTime &() const {
  if (IsNull())
    return TianmuDateTime::NullValue();
  if (GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return static_cast<TianmuDateTime &>(*value_);

  TIANMU_ERROR("Bad cast in TianmuValueObject::TianmuDateTime&()");
  return static_cast<TianmuDateTime &>(*value_);
}

BString TianmuValueObject::ToBString() const {
  if (IsNull())
    return BString();
  return value_->ToBString();
}

uint TianmuValueObject::GetHashCode() const {
  if (IsNull())
    return 0;
  return value_->GetHashCode();
}

}  // namespace types
}  // namespace Tianmu
