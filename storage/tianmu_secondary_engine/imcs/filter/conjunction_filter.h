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

#ifndef TIANMU_IMCS_FILTER_CONJUNCTION_FILTER_H
#define TIANMU_IMCS_FILTER_CONJUNCTION_FILTER_H

#include <memory>
#include <vector>

#include "table_filter.h"

namespace Tianmu {
namespace IMCS {

class Conjunction_filter : public Table_filter {
 public:
  Conjunction_filter(Table_filter_type type) : Table_filter(type) {}
  virtual ~Conjunction_filter() = default;

  void add_child(std::unique_ptr<Table_filter> &filter) {
    child_filters_.push_back(std::move(filter));
  }

 protected:
  std::vector<std::unique_ptr<Table_filter>> child_filters_;
};

class Conjunction_or_filter : public Conjunction_filter {
 private:
  static constexpr Table_filter_type TYPE = Table_filter_type::CONJUNCTION_OR;

 public:
  Conjunction_or_filter() : Conjunction_filter(TYPE) {}
  virtual ~Conjunction_or_filter() = default;

  Filter_eval_result check_zone_map(Zone_map &zone_map) override;
};

class Conjunction_and_filter : public Conjunction_filter {
 private:
  static constexpr Table_filter_type TYPE = Table_filter_type::CONJUNCTION_AND;

 public:
  Conjunction_and_filter() : Conjunction_filter(TYPE) {}
  virtual ~Conjunction_and_filter() = default;

  Filter_eval_result check_zone_map(Zone_map &zone_map) override;
};

}  // namespace IMCS

}  // namespace Tianmu

#endif