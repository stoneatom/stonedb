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

#include "constant_filter.h"

#include "../common/comparison_operator.h"

namespace Tianmu {
namespace IMCS {

Filter_eval_result Constant_filter::check_zone_map(Zone_map &zone_map) {
  int64 max_val_int, min_val_int;
  double max_val_double, min_val_double;
  my_decimal max_val_decimal, min_val_decimal;
  switch (field_type_) {
    case MYSQL_TYPE_TINY:
      max_val_int = zone_map.max.get_val_ref<int8>();
      min_val_int = zone_map.min.get_val_ref<int8>();
    case MYSQL_TYPE_SHORT:
      max_val_int = zone_map.max.get_val_ref<int16>();
      min_val_int = zone_map.min.get_val_ref<int16>();
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
      max_val_int = zone_map.max.get_val_ref<int32>();
      min_val_int = zone_map.min.get_val_ref<int32>();
    case MYSQL_TYPE_LONGLONG: {
      max_val_int = zone_map.max.get_val_ref<int64>();
      min_val_int = zone_map.min.get_val_ref<int64>();
      switch (value_->type()) {
        case Item::INT_ITEM: {
          int64 val = value_->val_int();
          return check_min_max(min_val_int, max_val_int, val);
        }
        case Item::REAL_ITEM: {
          double val = value_->val_real();
          return check_min_max((double)min_val_int, (double)max_val_int, val);
        }
        case Item::DECIMAL_ITEM: {
          my_decimal *val = value_->val_decimal(nullptr);
          int2my_decimal(E_DEC_FATAL_ERROR, max_val_int, false,
                         &max_val_decimal);
          int2my_decimal(E_DEC_FATAL_ERROR, min_val_int, false,
                         &min_val_decimal);
          return check_min_max(min_val_decimal, max_val_decimal, *val);
        }
        case Item::STRING_ITEM:
          // TODO: handle string item
        case Item::NULL_ITEM:
          // TODO: handle null item
        default:
          return Filter_eval_result::NO_PRUNING_POSSIBLE;
      }
    }
    case MYSQL_TYPE_FLOAT:
      max_val_double = zone_map.max.get_val_ref<float>();
      min_val_double = zone_map.min.get_val_ref<float>();
    case MYSQL_TYPE_DOUBLE: {
      max_val_double = zone_map.max.get_val_ref<double>();
      min_val_double = zone_map.min.get_val_ref<double>();
      switch (value_->type()) {
        case Item::INT_ITEM: {
          int64 val = value_->val_int();
          return check_min_max(min_val_double, max_val_double, (double)val);
        }
        case Item::REAL_ITEM: {
          double val = value_->val_real();
          return check_min_max((double)min_val_int, (double)max_val_int, val);
        }
        case Item::DECIMAL_ITEM: {
          my_decimal *val = value_->val_decimal(nullptr);
          double2my_decimal(E_DEC_FATAL_ERROR, max_val_double,
                            &max_val_decimal);
          double2my_decimal(E_DEC_FATAL_ERROR, min_val_double,
                            &min_val_decimal);
          return check_min_max(min_val_decimal, max_val_decimal, *val);
        }
        case Item::STRING_ITEM:
          // TODO: handle string item
        case Item::NULL_ITEM:
          // TODO: handle null item
        default:
          return Filter_eval_result::NO_PRUNING_POSSIBLE;
      }
    }
    case MYSQL_TYPE_DECIMAL: {
      max_val_decimal = zone_map.max.get_val_ref<my_decimal>();
      min_val_decimal = zone_map.min.get_val_ref<my_decimal>();
      switch (value_->type()) {
        case Item::INT_ITEM: {
          int64 val = value_->val_int();
          my_decimal val_decimal;
          int2my_decimal(E_DEC_FATAL_ERROR, val, false, &val_decimal);
          return check_min_max(min_val_decimal, max_val_decimal, val_decimal);
        }
        case Item::REAL_ITEM: {
          double val = value_->val_real();
          my_decimal val_decimal;
          double2my_decimal(E_DEC_FATAL_ERROR, val, &val_decimal);
          return check_min_max(min_val_decimal, max_val_decimal, val_decimal);
        }
        case Item::DECIMAL_ITEM: {
          my_decimal *val = value_->val_decimal(nullptr);
          return check_min_max(min_val_decimal, max_val_decimal, *val);
        }
        case Item::STRING_ITEM:
          // TODO: handle string item
        case Item::NULL_ITEM:
          // TODO: handle null item
        default:
          return Filter_eval_result::NO_PRUNING_POSSIBLE;
      }
    }
    default:
      return Filter_eval_result::NO_PRUNING_POSSIBLE;
  }
}

template <typename T>
bool constant_exact_range(T min, T max, T constant) {
  return equal(min, constant) && equal(max, constant);
}

template <typename T>
bool constant_in_range(T min, T max, T constant) {
  return !less_than(constant, min) && !greater_than(constant, max);
}

template <typename T>
Filter_eval_result Constant_filter::check_min_max(T min, T max, T constant) {
  switch (comparison_type_) {
    case Comparison_type::EQUAL:
      if (constant_exact_range(min, max, constant)) {
        return Filter_eval_result::FILTER_ALWAYS_TRUE;
      }
      if (constant_in_range(min, max, constant)) {
        return Filter_eval_result::NO_PRUNING_POSSIBLE;
      }
      return Filter_eval_result::FILTER_ALWAYS_FALSE;
    case Comparison_type::NOT_EQUAL:
      if (!constant_in_range(min, max, constant)) {
        return Filter_eval_result::FILTER_ALWAYS_TRUE;
      }
      if (constant_exact_range(min, max, constant)) {
        return Filter_eval_result::FILTER_ALWAYS_FALSE;
      }
      return Filter_eval_result::NO_PRUNING_POSSIBLE;
    case Comparison_type::LESS_THAN:
      if (less_than(max, constant)) {
        return Filter_eval_result::FILTER_ALWAYS_TRUE;
      }
      if (less_than(constant, min) || equal(constant, min)) {
        return Filter_eval_result::FILTER_ALWAYS_FALSE;
      }
      return Filter_eval_result::NO_PRUNING_POSSIBLE;
    case Comparison_type::GREATER_THAN:
      if (less_than(constant, min)) {
        return Filter_eval_result::FILTER_ALWAYS_TRUE;
      }
      if (less_than(max, constant) || equal(max, constant)) {
        return Filter_eval_result::FILTER_ALWAYS_FALSE;
      }
      return Filter_eval_result::NO_PRUNING_POSSIBLE;
    case Comparison_type::LESS_THAN_OR_EQUAL_TO:
      // max <= constant => !(max > constant)
      if (!greater_than(max, constant)) {
        return Filter_eval_result::FILTER_ALWAYS_TRUE;
      }
      // min > constant
      if (greater_than(min, constant)) {
        return Filter_eval_result::FILTER_ALWAYS_FALSE;
      }
      return Filter_eval_result::NO_PRUNING_POSSIBLE;
    case Comparison_type::GREATER_THAN_OR_EQUAL_TO:
      // min >= constant => !(min < constant)
      if (!less_than(min, constant)) {
        return Filter_eval_result::FILTER_ALWAYS_TRUE;
      }
      // max < constant
      if (less_than(max, constant)) {
        return Filter_eval_result::FILTER_ALWAYS_FALSE;
      }
      return Filter_eval_result::NO_PRUNING_POSSIBLE;
    default:
      return Filter_eval_result::NO_PRUNING_POSSIBLE;
  }
}

}  // namespace IMCS

}  // namespace Tianmu