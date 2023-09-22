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

#ifndef TIANMU_IMCS_STATISTICS_ZONE_MAP_VALUE_H
#define TIANMU_IMCS_STATISTICS_ZONE_MAP_VALUE_H

#include "../common/common_def.h"
#include "../data/cell.h"
#include "../imcs_base.h"
#include "numeric_stat_value.h"

class ReadView;

namespace Tianmu {
namespace IMCS {

struct Zone_map {
  Numeric_value max;

  Numeric_value min;

  uint64 count;

  uint64 null_count;

  uint64 min_trx_id_, max_trx_id_;

  // min/max initial state
  bool initial;

  Zone_map()
      : initial(false),
        count(0),
        null_count(0),
        min_trx_id_(TRX_ID_UP_LIMIT),
        max_trx_id_(TRX_ID_LOW_LIMIT) {}

  void clear() {
    initial = false;
    count = 0;
    null_count = 0;
    min_trx_id_ = TRX_ID_UP_LIMIT;
    max_trx_id_ = TRX_ID_LOW_LIMIT;
  }

  bool has_null() { return null_count > 0; }

  bool has_not_null() { return null_count != count; }

  void update_with_not_null(TianmuSecondaryShare *share, enum_field_types type,
                            Cell *cell, uint64 trx_id);

  void update_with_null(uint64 trx_id);

  template <typename T>
  void update_with_zone_map_template(Zone_map &zone_map);

  void update_with_zone_map(Zone_map &zone_map, enum_field_types type);

  int update_delete_with_not_null(TianmuSecondaryShare *share,
                                  enum_field_types type, Cell *cell,
                                  uint64 trx_id);

  int update_delete_with_null(uint64 trx_id);

  bool can_use(ReadView *read_view);

  bool can_skip(ReadView *read_view);

 private:
  template <typename T>
  void update_template(T val, uint64 trx_id);

  template <typename T>
  bool agg_need_reorganize_template(T val);

  void update_trx_id(uint64 trx_id) {
    min_trx_id_ = std::min(min_trx_id_, trx_id);
    max_trx_id_ = std::max(max_trx_id_, trx_id);
  }
};

}  // namespace IMCS

}  // namespace Tianmu

#endif