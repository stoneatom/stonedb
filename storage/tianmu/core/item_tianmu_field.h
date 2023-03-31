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
#ifndef TIANMU_CORE_ITEM_TIANMUFIELD_H_
#define TIANMU_CORE_ITEM_TIANMUFIELD_H_
#pragma once

#include <map>
#include <mutex>
#include <set>

#include "common/common_definitions.h"
#include "core/data_type.h"
#include "core/value_or_null.h"
#include "core/var_id.h"

namespace Tianmu {
namespace core {
// damn it... should be C linkage.
// this is not exported yet in msyql 5.6
extern "C" int decimal_shift(decimal_t *dec, int shift);

static inline int my_decimal_shift(uint mask, my_decimal *res, int shift) {
  return check_result_and_overflow(mask, decimal_shift((decimal_t *)res, shift), res);
}

class Item_tianmufield : public Item_field {
 public:
  std::vector<VarID> varID;  // TIANMU identifiers of the variable/field/column
                             // represented by 'ifield' for each tianmufield usage
  Item_tianmufield(Item_field *ifield, VarID varID);
  virtual ~Item_tianmufield();

  Item_field *OriginalItem() { return ifield; }
  void SetBuf(ValueOrNull *&b);
  void ClearBuf();
  void SetType(DataType t);

  // Implementation of MySQL Item interface

  static_assert(sizeof(Type) >= 2, "invalid type size");
  enum class enumTIANMUFiledItem {
    TIANMUFIELD_ITEM = 12345
  };  // WARNING: it is risky. We are assuming sizeof(enum) >= 2 in a superclass
      // (Item)
  static Type get_tianmuitem_type() { return (Type)enumTIANMUFiledItem::TIANMUFIELD_ITEM; }
  Type type() const override { return (Type)enumTIANMUFiledItem::TIANMUFIELD_ITEM; }
  bool is_null() override { return buf->IsNull(); }
  bool is_null_result() override { return buf->IsNull(); }
  double val_real() override;
  double val_result() override { return val_real(); }
  longlong val_int() override;
  longlong val_time_temporal() override {
    MYSQL_TIME ltime;
    if (get_time(&ltime) == 1)
      return 0;
    return TIME_to_longlong_time_packed(&ltime);
  }
  longlong val_date_temporal() override {
    MYSQL_TIME ltime;
    get_date(&ltime, 0);
    return TIME_to_longlong_datetime_packed(&ltime);
  }
  longlong val_int_result() override { return val_int(); }
  bool val_bool_result() override { return (val_int() ? true : false); }
  String *val_str(String *s) override;
  String *str_result(String *s) override { return val_str(s); }
  my_decimal *val_decimal(my_decimal *d) override;
  my_decimal *val_decimal_result(my_decimal *d) override { return val_decimal(d); }
  bool get_date(MYSQL_TIME *ltime, uint fuzzydate) override;
  bool get_date_result(MYSQL_TIME *ltime, uint fuzzydate) override { return get_date(ltime, fuzzydate); }
  bool get_time(MYSQL_TIME *ltime) override;
  bool get_timeval(struct timeval *tm, int *warnings) override;
  table_map used_tables() const override { return ifield->used_tables(); }
  enum Item_result result_type() const override {
    return was_aggregation ? aggregation_result : Item_field::result_type();
  }
  enum_field_types field_type() const override { return ifield->field_type(); }
  bool const_item() const override { return false; }
  const char *full_name() const override { return fullname; }
  bool operator==(Item_tianmufield const &) const;
  bool IsAggregation() { return was_aggregation; }
  const ValueOrNull GetCurrentValue();
  type_conversion_status save_in_field_inner(Field *field, bool no_conversions) override;

  // possibly more functions of Item_field should be redifined to redirect them
  // to ifield bool result_as_longlong()
  //{
  //	return ifield->result_as_longlong();
  // }

 private:
  // Translate TIANMU value stored in 'buf' into MySQL value stored in 'ivalue'
  void FeedValue();

  Item_field *ifield;  // for recovery of original item tree structure

  // Type of values coming in from TIANMU
  DataType tianmu_type;

  // TIANMU buffer where current field value will be searched for
  ValueOrNull *buf;

  //! Indicates whether 'buf' field is owned (was allocated) by this object
  bool isBufOwner;

  // MySQL buffer (constant value) keeping the current value of this field.
  // Actual Item subclass of this object depends on what type comes in from TIANMU
  Item *ivalue;
  bool was_aggregation;  // this tianmufield replaces an aggregation
  Item_result aggregation_result;
  char fullname[32];
  Item_tianmufield(Item_tianmufield const &);
  Item_tianmufield &operator=(Item_tianmufield const &);
};

// This subclass is created only to get access to protected field
// 'decimal_value' of Item_decimal.
class Item_tianmudecimal : public Item_decimal {
 public:
  Item_tianmudecimal(DataType t);

  void Set(int64_t val);
  String *val_str(String *) override;

 private:
  int scale;
  int64_t scaleCoef;
  std::mutex mtx;
};

//! Base class for TIANMU's Item classes to store date/time values of columns
//! occuring in a complex expression
class Item_tianmudatetime_base : public Item {
 protected:
  types::DT dt;
  common::ColumnType at;

 public:
  void Set(int64_t x, common::ColumnType at) {
    dt.val = x;
    this->at = at;
  }
  Type type() const override { return (Type)-1; }
  double val_real() override { return (double)val_int(); }
  my_decimal *val_decimal(my_decimal *d) override;
  bool get_time(MYSQL_TIME *ltime) override;
};

class Item_tianmudatetime : public Item_tianmudatetime_base {
  longlong val_int() override;
  String *val_str(String *s) override;
  bool get_date(MYSQL_TIME *ltime, uint fuzzydate) override;
  bool get_time(MYSQL_TIME *ltime) override;
};

class Item_tianmutimestamp : public Item_tianmudatetime {};

class Item_tianmudate : public Item_tianmudatetime_base {
  longlong val_int() override;
  String *val_str(String *s) override;
  bool get_date(MYSQL_TIME *ltime, uint fuzzydate) override;
  bool get_time(MYSQL_TIME *ltime) override;
};

class Item_tianmutime : public Item_tianmudatetime_base {
  longlong val_int() override;
  String *val_str(String *s) override;
  bool get_date(MYSQL_TIME *ltime, uint fuzzydate) override;
};

class Item_tianmuyear : public Item_tianmudatetime_base {
  //! Is it 4- or 2-digit year (2009 or 09).
  //! This info is taken from MySQL.
  int length;

 public:
  Item_tianmuyear(int len) : length(len) { DEBUG_ASSERT(length == 2 || length == 4); }
  longlong val_int() override;
  String *val_str(String *s) override;
  bool get_date(MYSQL_TIME *ltime, uint fuzzydate) override;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_ITEM_TIANMUFIELD_H_
