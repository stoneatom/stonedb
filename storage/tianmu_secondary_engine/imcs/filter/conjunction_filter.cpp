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

#include "conjunction_filter.h"

namespace Tianmu {
namespace IMCS {
Filter_eval_result Conjunction_or_filter::check_zone_map(Zone_map &zone_map) {
  Filter_eval_result result = Filter_eval_result::FILTER_ALWAYS_FALSE;
  for (auto &filter : child_filters_) {
    Filter_eval_result res = filter->check_zone_map(zone_map);
    if (res == Filter_eval_result::FILTER_ALWAYS_TRUE) {
      return Filter_eval_result::FILTER_ALWAYS_TRUE;
    } else if (res == Filter_eval_result::NO_PRUNING_POSSIBLE) {
      result = Filter_eval_result::NO_PRUNING_POSSIBLE;
    }
  }
  return result;
}
Filter_eval_result Conjunction_and_filter::check_zone_map(Zone_map &zone_map) {
  Filter_eval_result result = Filter_eval_result::FILTER_ALWAYS_TRUE;
  for (auto &filter : child_filters_) {
    Filter_eval_result res = filter->check_zone_map(zone_map);
    if (res == Filter_eval_result::FILTER_ALWAYS_FALSE) {
      return Filter_eval_result::FILTER_ALWAYS_FALSE;
    } else if (res == Filter_eval_result::NO_PRUNING_POSSIBLE) {
      result = Filter_eval_result::NO_PRUNING_POSSIBLE;
    }
  }
  return result;
}
}  // namespace IMCS

}  // namespace Tianmu