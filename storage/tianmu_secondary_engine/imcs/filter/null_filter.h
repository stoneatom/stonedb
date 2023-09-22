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

#ifndef TIANMU_IMCS_FILTER_NULL_FILTER_H
#define TIANMU_IMCS_FILTER_NULL_FILTER_H

#include "table_filter.h"

namespace Tianmu {
namespace IMCS {

class Null_filter : public Table_filter {
 private:
  static constexpr Table_filter_type TYPE = Table_filter_type::IS_NULL;

 public:
  Null_filter() : Table_filter(TYPE) {}
  virtual ~Null_filter() = default;

  Filter_eval_result check_zone_map(Zone_map &zone_map) override;
};

class Not_null_filter : public Table_filter {
 private:
  static constexpr Table_filter_type TYPE = Table_filter_type::IS_NOT_NULL;

 public:
  Not_null_filter() : Table_filter(TYPE) {}
  virtual ~Not_null_filter() = default;

  Filter_eval_result check_zone_map(Zone_map &zone_map) override;
};

}  // namespace IMCS

}  // namespace Tianmu

#endif