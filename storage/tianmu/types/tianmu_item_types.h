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
#ifndef TIANMU_TYPES_TIANMU_ITEM_TYPES_H_
#define TIANMU_TYPES_TIANMU_ITEM_TYPES_H_
#pragma once

#include "common/assert.h"
#include "common/common_definitions.h"

namespace Tianmu {
namespace types {

class ItemSumInTianmuBase : public Item_sum_num {
 public:
  ItemSumInTianmuBase();
  ~ItemSumInTianmuBase();

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

  void int64_value(int64_t &value);

  void clear() override;
  bool add() override;
  void update_field() override;
  const char *func_name() const override { return "count("; };

 public:
  int64_t count_;
};

class ItemSumSumTianmuBase : public Item_sum_num {
 public:
  ItemSumSumTianmuBase();
  ~ItemSumSumTianmuBase();

  enum Sumfunctype sum_func() const override { return SUM_FUNC; }
  enum Item_result result_type() const override { return hybrid_type_; }
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

 public:
  double sum_;
  Item_result hybrid_type_;
  my_decimal decimal_buffs_[1];
};

class ItemSumHybridTianmuBase : public Item_sum {
 public:
  ItemSumHybridTianmuBase();
  ~ItemSumHybridTianmuBase();

  void clear() override;
  double val_real() override;
  longlong val_int() override;
  my_decimal *val_decimal(my_decimal *) override;
  String *val_str(String *) override;

  enum Sumfunctype sum_func() const override { return MIN_FUNC; }
  enum Item_result result_type() const override { return hybrid_type_; }
  enum enum_field_types field_type() const override { return hybrid_field_type_; }

  bool any_value() { return is_values_; }
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

 public:
  String value_;
  enum_field_types hybrid_field_type_;
  Item_result hybrid_type_;

 protected:
  String tmp_value_;
  double sum_;
  longlong sum_longint_;
  my_decimal sum_decimal_;
  bool is_values_;  // Set if we have found at least one row (for max/min only)
};

}  // namespace types
}  // namespace Tianmu

#endif  // TIANMU_TYPES_TIANMU_ITEM_TYPES_H_
