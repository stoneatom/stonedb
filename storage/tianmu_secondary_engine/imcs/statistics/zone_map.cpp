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

#include "zone_map.h"

#include "../common/comparison_operator.h"
#include "my_decimal.h"
#include "read0read.h"

namespace Tianmu {
namespace IMCS {

template <typename T>
void Zone_map::update_template(T val, uint64 trx_id) {
  if (!initial) {
    initial = true;
    min.set_val(val);
    max.set_val(val);
  } else {
    if (less_than(val, min.get_val_ref<T>())) {
      min.set_val(val);
    }
    if (greater_than(val, max.get_val_ref<T>())) {
      max.set_val(val);
    }
  }
  update_trx_id(trx_id);
  count++;
}

template <typename T>
bool Zone_map::agg_need_reorganize_template(T val) {
  assert(initial);
  if (equal(val, min.get_val_ref<T>()) || equal(val, max.get_val_ref<T>())) {
    return true;
  }
  return false;
}

void Tianmu::IMCS::Zone_map::update_with_not_null(TianmuSecondaryShare *share,
                                                  enum_field_types type,
                                                  Cell *cell, uint64 trx_id) {
  switch (type) {
    case MYSQL_TYPE_TINY:
      update_template(cell->get_val<int8>(), trx_id);
      break;
    case MYSQL_TYPE_SHORT:
      update_template(cell->get_val<int16>(), trx_id);
      break;
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
      update_template(cell->get_val<int32>(), trx_id);
      break;
    case MYSQL_TYPE_LONGLONG:
      update_template(cell->get_val<int64>(), trx_id);
      break;
    case MYSQL_TYPE_FLOAT:
      update_template(cell->get_val<float>(), trx_id);
      break;
    case MYSQL_TYPE_DOUBLE:
      update_template(cell->get_val<double>(), trx_id);
      break;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      update_template(cell->get_val<my_decimal>(), trx_id);
      break;
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR:
      // TODO: handle string
    default:
      break;
  }
}

void Zone_map::update_with_null(uint64 trx_id) {
  count++;
  null_count++;
  update_trx_id(trx_id);
}

template <typename T>
void Zone_map::update_with_zone_map_template(Zone_map &zone_map) {
  count += zone_map.count;
  null_count += zone_map.null_count;
  min_trx_id_ = std::min(min_trx_id_, zone_map.min_trx_id_);
  max_trx_id_ = std::max(max_trx_id_, zone_map.max_trx_id_);
  if (!initial) {
    min.set_val(zone_map.min.get_val_ref<T>());
    max.set_val(zone_map.max.get_val_ref<T>());
  } else {
    if (less_than(zone_map.min.get_val_ref<T>(), min.get_val_ref<T>())) {
      min.set_val(zone_map.min.get_val_ref<T>());
    }
    if (greater_than(zone_map.max.get_val_ref<T>(), max.get_val_ref<T>())) {
      max.set_val(zone_map.max.get_val_ref<T>());
    }
  }
}

void Zone_map::update_with_zone_map(Zone_map &zone_map, enum_field_types type) {
  bool flag = false;
  switch (type) {
    case MYSQL_TYPE_TINY:
      update_with_zone_map_template<int8>(zone_map);
      break;
    case MYSQL_TYPE_SHORT:
      update_with_zone_map_template<int16>(zone_map);
      break;
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
      update_with_zone_map_template<int32>(zone_map);
      break;
    case MYSQL_TYPE_LONGLONG:
      update_with_zone_map_template<int64>(zone_map);
      break;
    case MYSQL_TYPE_FLOAT:
      update_with_zone_map_template<float>(zone_map);
      break;
    case MYSQL_TYPE_DOUBLE:
      update_with_zone_map_template<double>(zone_map);
      break;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      update_with_zone_map_template<my_decimal>(zone_map);
      break;
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR:
      // TODO: handle string
      assert(0);
    default:
      break;
  }
}

int Zone_map::update_delete_with_not_null(TianmuSecondaryShare *share,
                                          enum_field_types type, Cell *cell,
                                          uint64 trx_id) {
  count--;
  update_trx_id(trx_id);
  int ret = RET_SUCCESS;
  bool flag = false;
  switch (type) {
    case MYSQL_TYPE_TINY:
      flag = agg_need_reorganize_template(cell->get_val<int8>());
      break;
    case MYSQL_TYPE_SHORT:
      flag = agg_need_reorganize_template(cell->get_val<int16>());
      break;
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
      flag = agg_need_reorganize_template(cell->get_val<int32>());
      break;
    case MYSQL_TYPE_LONGLONG:
      flag = agg_need_reorganize_template(cell->get_val<int64>());
      break;
    case MYSQL_TYPE_FLOAT:
      flag = agg_need_reorganize_template(cell->get_val<float>());
      break;
    case MYSQL_TYPE_DOUBLE:
      flag = agg_need_reorganize_template(cell->get_val<double>());
      break;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      flag = agg_need_reorganize_template(cell->get_val<my_decimal>());
      break;
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR:
      // TODO: handle string
      assert(0);
    default:
      break;
  }
  if (flag) {
    ret = RET_NEED_REORGANIZE_ZONE_MAP;
  }
  return ret;
}

int Zone_map::update_delete_with_null(uint64 trx_id) {
  count--;
  null_count--;
  update_trx_id(trx_id);
  return RET_SUCCESS;
}

bool Zone_map::can_use(ReadView *read_view) {
  return read_view->sees(max_trx_id_);
}

bool Zone_map::can_skip(ReadView *read_view) {
  return read_view->low_limit_id() <= min_trx_id_;
}

}  // namespace IMCS

}  // namespace Tianmu