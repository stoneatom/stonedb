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

#include "tianmu_item_types.h"

#include "common/common_definitions.h"
#include "item_sum.h"

namespace Tianmu {
namespace types {

ItemSumInTianmuBase::ItemSumInTianmuBase() : Item_sum_num() {}
ItemSumInTianmuBase::~ItemSumInTianmuBase() {}

longlong ItemSumInTianmuBase::val_int() {
  DEBUG_ASSERT(fixed == 1);
  return (longlong)count_;
}

String *ItemSumInTianmuBase::val_str([[maybe_unused]] String *str) { return nullptr; }
my_decimal *ItemSumInTianmuBase::val_decimal(my_decimal *) { return nullptr; }

void ItemSumInTianmuBase::int64_value(int64_t &value) {
  fixed = 1;
  count_ = value;
}

void ItemSumInTianmuBase::clear() {}
bool ItemSumInTianmuBase::add() { return 0; }
void ItemSumInTianmuBase::update_field() {}

ItemSumSumTianmuBase::ItemSumSumTianmuBase() : Item_sum_num() { sum_ = 0; }
ItemSumSumTianmuBase::~ItemSumSumTianmuBase() {}

double ItemSumSumTianmuBase::val_real() {
  DEBUG_ASSERT(fixed == 1);
  if (hybrid_type_ == DECIMAL_RESULT)
    my_decimal2double(E_DEC_FATAL_ERROR, decimal_buffs_, &sum_);
  return sum_;
}

my_decimal *ItemSumSumTianmuBase::val_decimal(my_decimal *val) {
  if (hybrid_type_ == DECIMAL_RESULT)
    return decimal_buffs_;
  return val_decimal_from_real(val);
}

String *ItemSumSumTianmuBase::val_str(String *str) {
  if (hybrid_type_ == DECIMAL_RESULT)
    return val_string_from_decimal(str);
  return val_string_from_real(str);
}

longlong ItemSumSumTianmuBase::val_int() {
  DEBUG_ASSERT(fixed == 1);
  if (hybrid_type_ == DECIMAL_RESULT) {
    longlong result;
    my_decimal2int(E_DEC_FATAL_ERROR, decimal_buffs_, unsigned_flag, &result);
    return result;
  }
  return (longlong)rint(val_real());
}

my_decimal *ItemSumSumTianmuBase::dec_value() {
  fixed = 1;
  return decimal_buffs_;
}

double &ItemSumSumTianmuBase::real_value() {
  fixed = 1;
  return sum_;
}

void ItemSumSumTianmuBase::real_value(double &val) {
  fixed = 1;
  sum_ = val;
}

void ItemSumSumTianmuBase::clear() {}
bool ItemSumSumTianmuBase::add() { return false; }
void ItemSumSumTianmuBase::update_field() {}

ItemSumHybridTianmuBase::ItemSumHybridTianmuBase() : Item_sum() {
  is_values_ = true;
  sum_longint_ = 0;
  my_decimal_set_zero(&sum_decimal_);
  sum_ = 0.0;
  value_.length(0);
  null_value = 1;
}

ItemSumHybridTianmuBase::~ItemSumHybridTianmuBase() {}

void ItemSumHybridTianmuBase::clear() {
  switch (hybrid_type_) {
    case INT_RESULT:
      sum_longint_ = 0;
      break;
    case DECIMAL_RESULT:
      my_decimal_set_zero(&sum_decimal_);
      break;
    case REAL_RESULT:
      sum_ = 0.0;
      break;
    default:
      value_.length(0);
  }
  null_value = 1;
}

double ItemSumHybridTianmuBase::val_real() {
  DEBUG_ASSERT(fixed == 1);
  if (null_value)
    return 0.0;
  switch (hybrid_type_) {
    case STRING_RESULT: {
      char *end_not_used;
      int err_not_used;
      String *res;
      res = val_str(&str_value);
      return (res ? my_strntod(res->charset(), (char *)res->ptr(), res->length(), &end_not_used, &err_not_used) : 0.0);
    }
    case INT_RESULT:
      if (unsigned_flag)
        return ulonglong2double(sum_longint_);
      return (double)sum_longint_;
    case DECIMAL_RESULT:
      my_decimal2double(E_DEC_FATAL_ERROR, &sum_decimal_, &sum_);
      return sum_;
    case REAL_RESULT:
      return sum_;
    case ROW_RESULT:
    default:
      // This case should never be chosen
      TIANMU_ERROR("type not supported");
      return 0;
  }
}

longlong ItemSumHybridTianmuBase::val_int() {
  DEBUG_ASSERT(fixed == 1);
  if (null_value)
    return 0;
  switch (hybrid_type_) {
    case INT_RESULT:
      return sum_longint_;
    case DECIMAL_RESULT: {
      longlong result;
      my_decimal2int(E_DEC_FATAL_ERROR, &sum_decimal_, unsigned_flag, &result);
      return result;
    }
    default:
      return (longlong)rint(ItemSumHybridTianmuBase::val_real());
  }
}

my_decimal *ItemSumHybridTianmuBase::val_decimal(my_decimal *val) {
  DEBUG_ASSERT(fixed == 1);
  if (null_value)
    return 0;
  switch (hybrid_type_) {
    case STRING_RESULT:
      string2my_decimal(E_DEC_FATAL_ERROR, &value_, val);
      break;
    case REAL_RESULT:
      double2my_decimal(E_DEC_FATAL_ERROR, sum_, val);
      break;
    case DECIMAL_RESULT:
      val = &sum_decimal_;
      break;
    case INT_RESULT:
      int2my_decimal(E_DEC_FATAL_ERROR, sum_longint_, unsigned_flag, val);
      break;
    case ROW_RESULT:
    default:
      // This case should never be chosen
      TIANMU_ERROR("type not supported");
      break;
  }
  return val;  // Keep compiler happy
}

String *ItemSumHybridTianmuBase::val_str(String *str) {
  DEBUG_ASSERT(fixed == 1);
  if (null_value)
    return (String *)0;
  switch (hybrid_type_) {
    case STRING_RESULT:
      return &value_;
    case REAL_RESULT:
      str->set_real(sum_, decimals, &my_charset_bin);
      break;
    case DECIMAL_RESULT:
      my_decimal2string(E_DEC_FATAL_ERROR, &sum_decimal_, 0, 0, 0, str);
      return str;
    case INT_RESULT:
      if (unsigned_flag)
        str->set((ulonglong)sum_longint_, &my_charset_bin);
      else
        str->set((longlong)sum_longint_, &my_charset_bin);
      break;
    case ROW_RESULT:
    default:
      // This case should never be chosen
      TIANMU_ERROR("type not supported");
      break;
  }
  return str;  // Keep compiler happy
}

my_decimal *ItemSumHybridTianmuBase::dec_value() {
  fixed = 1;
  return &sum_decimal_;
}
double &ItemSumHybridTianmuBase::real_value() {
  fixed = 1;
  return sum_;
}

int64_t &ItemSumHybridTianmuBase::int64_value() {
  fixed = 1;
  return (int64_t &)sum_longint_;
}
String *ItemSumHybridTianmuBase::string_value() {
  fixed = 1;
  return &value_;
}

bool ItemSumHybridTianmuBase::add() { return false; }
void ItemSumHybridTianmuBase::update_field() {}

}  // namespace types
}  // namespace Tianmu
