/* Copyright (c) 2018, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef TIANMU_IMCS_STATISTICS_NUMERIC_STATISTIC_VALUE_H
#define TIANMU_IMCS_STATISTICS_NUMERIC_STATISTIC_VALUE_H

#include "../imcs_base.h"
#include "my_decimal.h"

namespace Tianmu {
namespace IMCS {
struct Numeric_value {
  union Numeric_value_union {
    Numeric_value_union() { decimal.init(); }
    ~Numeric_value_union() {}
    int8_t tinyint;
    int16_t smallint;
    int32_t integer{0};
    int64_t bigint;
    float float_;
    double double_;
    my_decimal decimal;
  } value;

  template <typename T>
  T &get_val_ref();

  template <typename T>
  void set_val(T &val);
};

template <>
inline void Numeric_value::set_val(int8_t &val) {
  value.tinyint = val;
}

template <>
inline void Numeric_value::set_val(int16_t &val) {
  value.smallint = val;
}

template <>
inline void Numeric_value::set_val(int32_t &val) {
  value.integer = val;
}

template <>
inline void Numeric_value::set_val(int64_t &val) {
  value.integer = val;
}

template <>
inline void Numeric_value::set_val(float &val) {
  value.float_ = val;
}

template <>
inline void Numeric_value::set_val(double &val) {
  value.double_ = val;
}

template <>
inline void Numeric_value::set_val(my_decimal &val) {
  value.decimal = val;
}

template <>
inline int8_t &Numeric_value::get_val_ref() {
  return value.tinyint;
}

template <>
inline int16_t &Numeric_value::get_val_ref() {
  return value.smallint;
}

template <>
inline int32_t &Numeric_value::get_val_ref() {
  return value.integer;
}

template <>
inline int64_t &Numeric_value::get_val_ref() {
  return value.bigint;
}

template <>
inline float &Numeric_value::get_val_ref() {
  return value.float_;
}

template <>
inline double &Numeric_value::get_val_ref() {
  return value.double_;
}

template <>
inline my_decimal &Numeric_value::get_val_ref() {
  return value.decimal;
}

}  // namespace IMCS

}  // namespace Tianmu

#endif