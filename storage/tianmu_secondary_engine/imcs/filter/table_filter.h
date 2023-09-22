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

#ifndef TIANMU_IMCS_FILTER_TABLE_FILTER_H
#define TIANMU_IMCS_FILTER_TABLE_FILTER_H

#include <memory>
#include <unordered_map>

#include "../common/common_def.h"
#include "../imcs_base.h"
#include "../statistics/zone_map.h"
#include "item.h"

namespace Tianmu {
namespace IMCS {

enum class Filter_eval_result : uint8_t {
  NO_PRUNING_POSSIBLE,
  FILTER_ALWAYS_TRUE,
  FILTER_ALWAYS_FALSE
};

enum class Table_filter_type : uint8_t {
  CONSTANT_COMPARISON,  // constant comparison (e.g. =C, >C, >=C, <C, <=C)
  IS_NULL,
  IS_NOT_NULL,
  CONJUNCTION_OR,
  CONJUNCTION_AND
};

class Table_filter {
 public:
  Table_filter(Table_filter_type filter_type) : filter_type_(filter_type) {}
  virtual ~Table_filter() = default;

  Table_filter_type filter_type() { return filter_type_; }
  virtual Filter_eval_result check_zone_map(Zone_map &zone_map) = 0;

 protected:
  Table_filter_type filter_type_;
};

class Table_filter_set {
 public:
  Table_filter_set() {}
  virtual ~Table_filter_set() { filters.clear(); }

  std::unordered_map<uint32, std::unique_ptr<Table_filter>> filters;
};

std::unique_ptr<Table_filter_set> create_filter_set(TianmuSecondaryShare *share,
                                                    Item *item);

}  // namespace IMCS
}  // namespace Tianmu

#endif