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
#ifndef STONEDB_TYPES_RC_ITEM_TYPES_H_
#define STONEDB_TYPES_RC_ITEM_TYPES_H_
#pragma once

#include "common/assert.h"
#include "common/common_definitions.h"

namespace stonedb {
namespace types {
class Item_sum_int_rcbase : public Item_sum_num {
 public:
  int64_t count;

  Item_sum_int_rcbase();
  ~Item_sum_int_rcbase();

  longlong val_int() override;

  double val_real() override {
    DEBUG_ASSERT(fixed == 1);
    return (double)val_int();
  }
  String *val_str(String *str) override;
  my_decimal *val_decimal(my_decimal *) override;

  enum Sumfunctype sum_func() const override { return COUNT_FUNC; }
  enum Item_result result_type() const override { return INT_RESULT; }
  void fix_length_and_dec() override {
    decimals = 0;
    max_length = 21;
    maybe_null = null_value = 0;
  }

  // void int64_value(int64_t &value);
  void int64_value(int64_t &value);

  void clear() override;
  bool add() override;
  void update_field() override;
  const char *func_name() const override { return "count("; };
};

class Item_sum_sum_rcbase : public Item_sum_num {
 public:
  Item_result hybrid_type;
  double sum;
  my_decimal dec_buffs[1];
  // my_decimal dec_buffs[2];
  // uint curr_dec_buff;
  // void fix_length_and_dec();

  Item_sum_sum_rcbase();
  ~Item_sum_sum_rcbase();

  enum Sumfunctype sum_func() const override { return SUM_FUNC; }
  enum Item_result result_type() const override { return hybrid_type; }
  double val_real() override;
  my_decimal *val_decimal(my_decimal *) override;
  String *val_str(String *) override;
  longlong val_int() override;

  my_decimal *dec_value();
  double &real_value();
  void real_value(double &);

  void clear() override;
  bool add() override;
  void update_field() override;
  void reset_field() override {}
  const char *func_name() const override { return "sum("; }
};
// class Item_sum_distinct_rcbase :public Item_sum_num
//{
// public:
//  /* storage for the summation result */
//  ulonglong count;
//  Hybrid_type val;
//
//  enum enum_field_types table_field_type;
//
//  Item_sum_distinct_rcbase();
//  ~Item_sum_distinct_rcbase();
//
//
//  double val_real();
//  my_decimal* val_decimal(my_decimal *);
//  longlong val_int();
//  String* val_str(String *str);
//
//  enum Sumfunctype sum_func () const { return SUM_DISTINCT_FUNC; }
//  enum Item_result result_type () const { return val.traits->type(); }
//
//  my_decimal* dec_value();
//  void real_value(double &value);
//
//  void clear();
//  bool add();
//  void update_field();
//  const char *func_name() const;
//};

class Item_sum_hybrid_rcbase : public Item_sum {
 protected:
  String tmp_value;
  double sum;
  longlong sum_int;
  my_decimal sum_dec;

  //  int cmp_sign;
  //  table_map used_table_cache;
  bool was_values;  // Set if we have found at least one row (for max/min only)

 public:
  String value;
  enum_field_types hybrid_field_type;
  Item_result hybrid_type;

  Item_sum_hybrid_rcbase();
  ~Item_sum_hybrid_rcbase();

  void clear() override;
  double val_real() override;
  longlong val_int() override;
  my_decimal *val_decimal(my_decimal *) override;
  String *val_str(String *) override;

  enum Sumfunctype sum_func() const override { return MIN_FUNC; }
  enum Item_result result_type() const override { return hybrid_type; }
  enum enum_field_types field_type() const override { return hybrid_field_type; }
  //  void update_field();
  //  void min_max_update_str_field();
  //  void min_max_update_real_field();
  //  void min_max_update_int_field();
  //  void min_max_update_decimal_field();
  //  void cleanup();
  bool any_value() { return was_values; }
  my_decimal *dec_value();
  double &real_value();
  int64_t &int64_value();
  String *string_value();

  bool add() override;
  void update_field() override;
  void reset_field() override {}
  const char *func_name() const override { return "min("; }
  bool get_date(MYSQL_TIME *ltime, uint fuzzydate) override { return get_date_from_string(ltime, fuzzydate); }
  bool get_time(MYSQL_TIME *ltime) override { return get_time_from_string(ltime); }
};
}  // namespace types
}  // namespace stonedb

#endif  // STONEDB_TYPES_RC_ITEM_TYPES_H_
