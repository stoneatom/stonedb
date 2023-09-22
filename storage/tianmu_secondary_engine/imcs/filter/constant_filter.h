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

#ifndef TIANMU_IMCS_FILTER_CONSTANT_FILTER_H
#define TIANMU_IMCS_FILTER_CONSTANT_FILTER_H

#include "../imcs_base.h"
#include "field.h"
#include "table_filter.h"

namespace Tianmu {
namespace IMCS {

enum class Comparison_type : uint8_t {
  EQUAL,
  NOT_EQUAL,
  LESS_THAN,
  GREATER_THAN,
  LESS_THAN_OR_EQUAL_TO,
  GREATER_THAN_OR_EQUAL_TO
};

class Constant_filter : public Table_filter {
 private:
  static constexpr Table_filter_type TYPE =
      Table_filter_type::CONSTANT_COMPARISON;

 public:
  Constant_filter(Comparison_type comparison_type, Item_num *value,
                  enum_field_types field_type)
      : Table_filter(TYPE),
        comparison_type_(comparison_type),
        value_(value),
        field_type_(field_type) {}
  virtual ~Constant_filter() = default;

  Filter_eval_result check_zone_map(Zone_map &zone_map) override;

 private:
  template <typename T>
  Filter_eval_result check_min_max(T min, T max, T constant);

 private:
  enum_field_types field_type_;
  Comparison_type comparison_type_;
  Item_num *value_;
};

}  // namespace IMCS
}  // namespace Tianmu

#endif