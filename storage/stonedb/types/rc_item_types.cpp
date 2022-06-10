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

#include "rc_item_types.h"

#include "common/common_definitions.h"

namespace stonedb {
namespace types {
/************************************* Item_sum_int_rcbase
 * ************************************/

Item_sum_int_rcbase::Item_sum_int_rcbase() : Item_sum_num() {}
Item_sum_int_rcbase::~Item_sum_int_rcbase() {}
longlong Item_sum_int_rcbase::val_int() {
  DEBUG_ASSERT(fixed == 1);
  return (longlong)count;
}
String *Item_sum_int_rcbase::val_str([[maybe_unused]] String *str) { return NULL; }
my_decimal *Item_sum_int_rcbase::val_decimal(my_decimal *) { return NULL; }

// TODO: Test this part
// void Item_sum_int_rcbase::int64_value(int64_t &value)
void Item_sum_int_rcbase::int64_value(int64_t &value) {
  fixed = 1;
  count = value;
}
// Not used
void Item_sum_int_rcbase::clear() {}
bool Item_sum_int_rcbase::add() { return 0; }
void Item_sum_int_rcbase::update_field() {}

/************************************* Item_sum_sum_rcbase
 * ************************************/

Item_sum_sum_rcbase::Item_sum_sum_rcbase() : Item_sum_num() { sum = 0; }
Item_sum_sum_rcbase::~Item_sum_sum_rcbase() {}

double Item_sum_sum_rcbase::val_real() {
  DEBUG_ASSERT(fixed == 1);
  if (hybrid_type == DECIMAL_RESULT) my_decimal2double(E_DEC_FATAL_ERROR, dec_buffs, &sum);
  return sum;
}
my_decimal *Item_sum_sum_rcbase::val_decimal(my_decimal *val) {
  if (hybrid_type == DECIMAL_RESULT) return dec_buffs;
  return val_decimal_from_real(val);
}
String *Item_sum_sum_rcbase::val_str(String *str) {
  if (hybrid_type == DECIMAL_RESULT) return val_string_from_decimal(str);
  return val_string_from_real(str);
}
longlong Item_sum_sum_rcbase::val_int() {
  DEBUG_ASSERT(fixed == 1);
  if (hybrid_type == DECIMAL_RESULT) {
    longlong result;
    my_decimal2int(E_DEC_FATAL_ERROR, dec_buffs, unsigned_flag, &result);
    return result;
  }
  return (longlong)rint(val_real());
}

my_decimal *Item_sum_sum_rcbase::dec_value() {
  fixed = 1;
  return dec_buffs;
}
double &Item_sum_sum_rcbase::real_value() {
  fixed = 1;
  return sum;
}
void Item_sum_sum_rcbase::real_value(double &val) {
  fixed = 1;
  sum = val;
}

// Not used

void Item_sum_sum_rcbase::clear() {}
bool Item_sum_sum_rcbase::add() { return false; }
void Item_sum_sum_rcbase::update_field() {}

/************************************* Item_sum_distinct_rcbase
 * ************************************/

// Item_sum_distinct_rcbase::Item_sum_distinct_rcbase()
//	:Item_sum_num()
//{
//}
// Item_sum_distinct_rcbase::~Item_sum_distinct_rcbase()
//{
//}
// double Item_sum_distinct_rcbase::val_real()
//{
//	return val.traits->val_real(&val);
//}
// my_decimal *Item_sum_distinct_rcbase::val_decimal(my_decimal *to)
//{
//	if (null_value)
//		return 0;
//	return val.traits->val_decimal(&val, to);
//}
// longlong Item_sum_distinct_rcbase::val_int()
//{
//	return val.traits->val_int(&val, unsigned_flag);
//}
// String *Item_sum_distinct_rcbase::val_str(String *str)
//{
//	if (null_value)
//		return 0;
//	return val.traits->val_str(&val, str, decimals);
//}
// my_decimal* Item_sum_distinct_rcbase::dec_value()
//{
//	val.traits = Hybrid_type_traits_decimal::instance();
//	val.used_dec_buf_no = 0;
//	decimals = 4;
//	return &val.dec_buf[val.used_dec_buf_no];
//}
// void Item_sum_distinct_rcbase::real_value(double &value)
//{
//	val.traits = Hybrid_type_traits::instance();
//	val.real = value;
//}
//
////Not used
// void Item_sum_distinct_rcbase::clear(){}
// bool Item_sum_distinct_rcbase::add(){ return 0; }
// void Item_sum_distinct_rcbase::update_field(){}
// const char* Item_sum_distinct_rcbase::func_name() const
//{
//	return NULL;
//}

/************************************* Item_sum_hybrid_rcbase
 * ************************************/

Item_sum_hybrid_rcbase::Item_sum_hybrid_rcbase() : Item_sum() {
  was_values = true;
  sum_int = 0;
  my_decimal_set_zero(&sum_dec);
  sum = 0.0;
  value.length(0);
  null_value = 1;
}
Item_sum_hybrid_rcbase::~Item_sum_hybrid_rcbase() {}

void Item_sum_hybrid_rcbase::clear() {
  switch (hybrid_type) {
    case INT_RESULT:
      sum_int = 0;
      break;
    case DECIMAL_RESULT:
      my_decimal_set_zero(&sum_dec);
      break;
    case REAL_RESULT:
      sum = 0.0;
      break;
    default:
      value.length(0);
  }
  null_value = 1;
}

double Item_sum_hybrid_rcbase::val_real() {
  DEBUG_ASSERT(fixed == 1);
  if (null_value) return 0.0;
  switch (hybrid_type) {
    case STRING_RESULT: {
      char *end_not_used;
      int err_not_used;
      String *res;
      res = val_str(&str_value);
      return (res ? my_strntod(res->charset(), (char *)res->ptr(), res->length(), &end_not_used, &err_not_used) : 0.0);
    }
    case INT_RESULT:
      if (unsigned_flag) return ulonglong2double(sum_int);
      return (double)sum_int;
    case DECIMAL_RESULT:
      my_decimal2double(E_DEC_FATAL_ERROR, &sum_dec, &sum);
      return sum;
    case REAL_RESULT:
      return sum;
    case ROW_RESULT:
    default:
      // This case should never be chosen
      STONEDB_ERROR("type not supported");
      return 0;
  }
}

longlong Item_sum_hybrid_rcbase::val_int() {
  DEBUG_ASSERT(fixed == 1);
  if (null_value) return 0;
  switch (hybrid_type) {
    case INT_RESULT:
      return sum_int;
    case DECIMAL_RESULT: {
      longlong result;
      my_decimal2int(E_DEC_FATAL_ERROR, &sum_dec, unsigned_flag, &result);
      return result;
    }
    default:
      return (longlong)rint(Item_sum_hybrid_rcbase::val_real());
  }
}

my_decimal *Item_sum_hybrid_rcbase::val_decimal(my_decimal *val) {
  DEBUG_ASSERT(fixed == 1);
  if (null_value) return 0;
  switch (hybrid_type) {
    case STRING_RESULT:
      string2my_decimal(E_DEC_FATAL_ERROR, &value, val);
      break;
    case REAL_RESULT:
      double2my_decimal(E_DEC_FATAL_ERROR, sum, val);
      break;
    case DECIMAL_RESULT:
      val = &sum_dec;
      break;
    case INT_RESULT:
      int2my_decimal(E_DEC_FATAL_ERROR, sum_int, unsigned_flag, val);
      break;
    case ROW_RESULT:
    default:
      // This case should never be chosen
      STONEDB_ERROR("type not supported");
      break;
  }
  return val;  // Keep compiler happy
}

String *Item_sum_hybrid_rcbase::val_str(String *str) {
  DEBUG_ASSERT(fixed == 1);
  if (null_value) return (String *)0;
  switch (hybrid_type) {
    case STRING_RESULT:
      return &value;
    case REAL_RESULT:
      str->set_real(sum, decimals, &my_charset_bin);
      break;
    case DECIMAL_RESULT:
      my_decimal2string(E_DEC_FATAL_ERROR, &sum_dec, 0, 0, 0, str);
      return str;
    case INT_RESULT:
      if (unsigned_flag)
        str->set((ulonglong)sum_int, &my_charset_bin);
      else
        str->set((longlong)sum_int, &my_charset_bin);
      break;
    case ROW_RESULT:
    default:
      // This case should never be chosen
      STONEDB_ERROR("type not supported");
      break;
  }
  return str;  // Keep compiler happy
}

my_decimal *Item_sum_hybrid_rcbase::dec_value() {
  fixed = 1;
  return &sum_dec;
}
double &Item_sum_hybrid_rcbase::real_value() {
  fixed = 1;
  return sum;
}
// TODO: test this part
// int64_t& Item_sum_hybrid_rcbase::int64_value()
int64_t &Item_sum_hybrid_rcbase::int64_value() {
  fixed = 1;
  return (int64_t &)sum_int;
}
String *Item_sum_hybrid_rcbase::string_value() {
  fixed = 1;
  return &value;
}

// Not used

bool Item_sum_hybrid_rcbase::add() { return false; }
void Item_sum_hybrid_rcbase::update_field() {}
}  // namespace types
}  // namespace stonedb
