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

#ifndef TIANMU_IMCS_ROW_MANAGER_H
#define TIANMU_IMCS_ROW_MANAGER_H

#include <mutex>
#include <string>
#include <unordered_map>
#include "data/pk.h"
#include "imcs_base.h"
#include "sql/field.h"
#include "sql_string.h"

namespace Tianmu {
namespace IMCS {

class Row_manager {
 public:
  virtual int get_row_id(PK &pk, uint64 *row_id) = 0;
  virtual int gen_row_id(PK &pk, uint64 *row_id) = 0;
};

class Mem_row_manager : public Row_manager {
 public:
  Mem_row_manager() : count_(0ll) {}
  virtual ~Mem_row_manager() { mapper_.clear(); }

  virtual int get_row_id(PK &pk, uint64 *row_id) override;

  virtual int gen_row_id(PK &pk, uint64 *row_id) override;

 private:
  std::unordered_map<PK, uint64, hash_PK_t> mapper_;

  uint64 count_;

  std::mutex mutex_;
};

// TODO: extendible hash table
class Disk_row_manager : public Row_manager {
 public:
  Disk_row_manager() {}
  virtual ~Disk_row_manager() {}

  virtual int get_row_id(PK &pk, uint64 *row_id) override;

  virtual int gen_row_id(PK &pk, uint64 *row_id) override;
};

}  // namespace IMCS
}  // namespace Tianmu

#endif