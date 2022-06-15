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

#include "rc_data_types.h"

#include "core/rc_attr.h"
#include "core/rc_attr_typeinfo.h"

namespace stonedb {
namespace types {
RCDataType::~RCDataType() {}

bool RCDataType::AreComperable(const RCDataType &rcdt) const { return RCDataType::AreComperable(*this, rcdt); }

bool RCDataType::AreComperable(const RCDataType &rcdt1, const RCDataType &rcdt2) {
  common::CT att1 = rcdt1.Type();
  common::CT att2 = rcdt2.Type();
  return AreComparable(att1, att2);
}

bool RCDataType::compare(const RCDataType &rcdt1, const RCDataType &rcdt2, common::Operator op, char like_esc) {
  // DEBUG_ASSERT(RCDataType::AreComperable(rcdt1, rcdt2));
  if (op == common::Operator::O_LIKE || op == common::Operator::O_NOT_LIKE) {
    if (rcdt1.IsNull() || rcdt2.IsNull()) return false;
    BString x, y;
    BString *rcbs1 = dynamic_cast<BString *>(const_cast<RCDataType *>(&rcdt1));
    if (!rcbs1) {
      x = rcdt1.ToBString();
      rcbs1 = &x;
    }
    BString *rcbs2 = dynamic_cast<BString *>(const_cast<RCDataType *>(&rcdt2));
    if (!rcbs2) {
      y = rcdt2.ToBString();
      rcbs2 = &y;
    }
    bool res = rcbs1->Like(*rcbs2, like_esc);
    if (op == common::Operator::O_LIKE)
      return res;
    else
      return !res;
  } else if (!rcdt1.IsNull() && !rcdt2.IsNull() &&
             (((op == common::Operator::O_EQ) && rcdt1 == rcdt2) ||
              (op == common::Operator::O_NOT_EQ && rcdt1 != rcdt2) ||
              (op == common::Operator::O_LESS && rcdt1 < rcdt2) ||
              (op == common::Operator::O_LESS_EQ && (rcdt1 < rcdt2 || rcdt1 == rcdt2)) ||
              (op == common::Operator::O_MORE && (!(rcdt1 < rcdt2) && rcdt1 != rcdt2)) ||
              (op == common::Operator::O_MORE_EQ && (!(rcdt1 < rcdt2) || rcdt1 == rcdt2))))
    return true;
  return false;
}

bool RCDataType::compare(const RCDataType &rcdt, common::Operator op, char like_esc) const {
  return RCDataType::compare(*this, rcdt, op, like_esc);
}

bool AreComparable(common::CT attr1_t, common::CT attr2_t) {
  if (attr1_t == attr2_t) return true;
  if ((core::ATI::IsDateTimeType(attr1_t)) && (core::ATI::IsDateTimeType(attr2_t))) return true;
  if ((core::ATI::IsTxtType(attr2_t) && attr1_t == common::CT::VARBYTE) ||
      (core::ATI::IsTxtType(attr1_t) && attr2_t == common::CT::VARBYTE))
    return true;
  if ((((attr1_t == common::CT::TIME) || (attr1_t == common::CT::DATE)) && attr2_t != common::CT::DATETIME) ||
      (((attr2_t == common::CT::TIME) || (attr2_t == common::CT::DATE)) && attr1_t != common::CT::DATETIME) ||
      (core::ATI::IsBinType(attr1_t) && !core::ATI::IsBinType(attr2_t)) ||
      (core::ATI::IsBinType(attr2_t) && !core::ATI::IsBinType(attr1_t)) ||
      (core::ATI::IsTxtType(attr1_t) && !core::ATI::IsTxtType(attr2_t)) ||
      (core::ATI::IsTxtType(attr2_t) && !core::ATI::IsTxtType(attr1_t)))
    return false;
  return true;
}

bool RCDataType::ToDecimal(const RCDataType &in, int scale, RCNum &out) {
  if (RCNum *rcn = dynamic_cast<RCNum *>(const_cast<RCDataType *>(&in))) {
    if (rcn->IsDecimal(scale)) {
      out = rcn->ToDecimal(scale);
      return true;
    }
  } else if (BString *rcs = dynamic_cast<BString *>(const_cast<RCDataType *>(&in))) {
    if (RCNum::Parse(*rcs, out) == common::ErrorCode::SUCCESS) return true;
  }
  return false;
}

bool RCDataType::ToInt(const RCDataType &in, RCNum &out) {
  if (RCNum *rcn = dynamic_cast<RCNum *>(const_cast<RCDataType *>(&in))) {
    if (rcn->IsInt()) {
      out = rcn->ToInt();
      return true;
    }
  } else if (BString *rcs = dynamic_cast<BString *>(const_cast<RCDataType *>(&in))) {
    if (RCNum::Parse(*rcs, out) == common::ErrorCode::SUCCESS) return true;
  }
  return false;
}

bool RCDataType::ToReal(const RCDataType &in, RCNum &out) {
  if (RCNum *rcn = dynamic_cast<RCNum *>(const_cast<RCDataType *>(&in))) {
    if (rcn->IsReal()) {
      out = rcn->ToReal();
      return true;
    }
  } else if (BString *rcs = dynamic_cast<BString *>(const_cast<RCDataType *>(&in))) {
    if (RCNum::ParseReal(*rcs, out, common::CT::UNK) == common::ErrorCode::SUCCESS) return true;
  }
  return false;
}

ValueTypeEnum RCDataType::GetValueType(common::CT attr_type) {
  if (core::ATI::IsNumericType(attr_type))
    return ValueTypeEnum::NUMERIC_TYPE;
  else if (core::ATI::IsDateTimeType(attr_type))
    return ValueTypeEnum::DATE_TIME_TYPE;
  else if (core::ATI::IsStringType(attr_type))
    return ValueTypeEnum::STRING_TYPE;
  return ValueTypeEnum::NULL_TYPE;
}
}  // namespace types
}  // namespace stonedb
