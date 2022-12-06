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

#include "tianmu_data_types.h"

#include "core/tianmu_attr.h"
#include "core/tianmu_attr_typeinfo.h"

namespace Tianmu {
namespace types {

TianmuDataType::~TianmuDataType() {}

bool TianmuDataType::AreComperable(const TianmuDataType &tianmu_dt) const {
  return TianmuDataType::AreComperable(*this, tianmu_dt);
}

bool TianmuDataType::AreComperable(const TianmuDataType &tianmu_dt1, const TianmuDataType &tianmu_dt2) {
  common::ColumnType att1 = tianmu_dt1.Type();
  common::ColumnType att2 = tianmu_dt2.Type();
  return AreComparable(att1, att2);
}

bool TianmuDataType::compare(const TianmuDataType &tianmu_dt1, const TianmuDataType &tianmu_dt2, common::Operator op,
                             char like_esc) {
  // DEBUG_ASSERT(TianmuDataType::AreComperable(tianmu_dt1, tianmu_dt2));
  if (op == common::Operator::O_LIKE || op == common::Operator::O_NOT_LIKE) {
    if (tianmu_dt1.IsNull() || tianmu_dt2.IsNull())
      return false;
    BString x, y;
    BString *tianmu_s1 = dynamic_cast<BString *>(const_cast<TianmuDataType *>(&tianmu_dt1));
    if (!tianmu_s1) {
      x = tianmu_dt1.ToBString();
      tianmu_s1 = &x;
    }
    BString *tianmu_s2 = dynamic_cast<BString *>(const_cast<TianmuDataType *>(&tianmu_dt2));
    if (!tianmu_s2) {
      y = tianmu_dt2.ToBString();
      tianmu_s2 = &y;
    }
    bool res = tianmu_s1->Like(*tianmu_s2, like_esc);
    if (op == common::Operator::O_LIKE)
      return res;
    else
      return !res;
  } else if (!tianmu_dt1.IsNull() && !tianmu_dt2.IsNull() &&
             (((op == common::Operator::O_EQ) && tianmu_dt1 == tianmu_dt2) ||
              (op == common::Operator::O_NOT_EQ && tianmu_dt1 != tianmu_dt2) ||
              (op == common::Operator::O_LESS && tianmu_dt1 < tianmu_dt2) ||
              (op == common::Operator::O_LESS_EQ && (tianmu_dt1 < tianmu_dt2 || tianmu_dt1 == tianmu_dt2)) ||
              (op == common::Operator::O_MORE && (!(tianmu_dt1 < tianmu_dt2) && tianmu_dt1 != tianmu_dt2)) ||
              (op == common::Operator::O_MORE_EQ && (!(tianmu_dt1 < tianmu_dt2) || tianmu_dt1 == tianmu_dt2))))
    return true;
  return false;
}

bool TianmuDataType::compare(const TianmuDataType &tianmu_dt, common::Operator op, char like_esc) const {
  return TianmuDataType::compare(*this, tianmu_dt, op, like_esc);
}

bool AreComparable(common::ColumnType attr1_t, common::ColumnType attr2_t) {
  if (attr1_t == attr2_t)
    return true;
  if ((core::ATI::IsDateTimeType(attr1_t)) && (core::ATI::IsDateTimeType(attr2_t)))
    return true;
  if ((core::ATI::IsTxtType(attr2_t) && attr1_t == common::ColumnType::VARBYTE) ||
      (core::ATI::IsTxtType(attr1_t) && attr2_t == common::ColumnType::VARBYTE))
    return true;
  if ((((attr1_t == common::ColumnType::TIME) || (attr1_t == common::ColumnType::DATE)) &&
       attr2_t != common::ColumnType::DATETIME) ||
      (((attr2_t == common::ColumnType::TIME) || (attr2_t == common::ColumnType::DATE)) &&
       attr1_t != common::ColumnType::DATETIME) ||
      (core::ATI::IsBinType(attr1_t) && !core::ATI::IsBinType(attr2_t)) ||
      (core::ATI::IsBinType(attr2_t) && !core::ATI::IsBinType(attr1_t)) ||
      (core::ATI::IsTxtType(attr1_t) && !core::ATI::IsTxtType(attr2_t)) ||
      (core::ATI::IsTxtType(attr2_t) && !core::ATI::IsTxtType(attr1_t)))
    return false;
  return true;
}

bool TianmuDataType::ToDecimal(const TianmuDataType &in, int scale, TianmuNum &out) {
  if (TianmuNum *tianmu_n = dynamic_cast<TianmuNum *>(const_cast<TianmuDataType *>(&in))) {
    if (tianmu_n->IsDecimal(scale)) {
      out = tianmu_n->ToDecimal(scale);
      return true;
    }
  } else if (BString *tianmu_s = dynamic_cast<BString *>(const_cast<TianmuDataType *>(&in))) {
    if (TianmuNum::Parse(*tianmu_s, out) == common::ErrorCode::SUCCESS)
      return true;
  }
  return false;
}

bool TianmuDataType::ToInt(const TianmuDataType &in, TianmuNum &out) {
  if (TianmuNum *tianmu_n = dynamic_cast<TianmuNum *>(const_cast<TianmuDataType *>(&in))) {
    if (tianmu_n->IsInt()) {
      out = tianmu_n->ToInt();
      return true;
    }
  } else if (BString *tianmu_s = dynamic_cast<BString *>(const_cast<TianmuDataType *>(&in))) {
    if (TianmuNum::Parse(*tianmu_s, out) == common::ErrorCode::SUCCESS)
      return true;
  }
  return false;
}

bool TianmuDataType::ToReal(const TianmuDataType &in, TianmuNum &out) {
  if (TianmuNum *tianmu_n = dynamic_cast<TianmuNum *>(const_cast<TianmuDataType *>(&in))) {
    if (tianmu_n->IsReal()) {
      out = tianmu_n->ToReal();
      return true;
    }
  } else if (BString *tianmu_s = dynamic_cast<BString *>(const_cast<TianmuDataType *>(&in))) {
    if (TianmuNum::ParseReal(*tianmu_s, out, common::ColumnType::UNK) == common::ErrorCode::SUCCESS)
      return true;
  }
  return false;
}

ValueTypeEnum TianmuDataType::GetValueType(common::ColumnType attr_type) {
  if (core::ATI::IsNumericType(attr_type))
    return ValueTypeEnum::NUMERIC_TYPE;
  else if (core::ATI::IsDateTimeType(attr_type))
    return ValueTypeEnum::DATE_TIME_TYPE;
  else if (core::ATI::IsStringType(attr_type))
    return ValueTypeEnum::STRING_TYPE;
  return ValueTypeEnum::NULL_TYPE;
}

}  // namespace types
}  // namespace Tianmu
