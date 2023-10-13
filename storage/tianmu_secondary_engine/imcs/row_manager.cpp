#include "row_manager.h"
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

#include "common/error.h"
#include "row_manager.h"

namespace Tianmu {
namespace IMCS {
int Mem_row_manager::get_row_id(PK &pk, uint64 *row_id) {
  std::lock_guard<std::mutex> lock_(mutex_);
  auto it = mapper_.find(pk);
  if (it == mapper_.end()) {
    return RET_PK_ROW_ID_NOT_EXIST;
  }
  *row_id = it->second;
  return RET_SUCCESS;
}

int Mem_row_manager::gen_row_id(PK &pk, uint64 *row_id) {
  std::lock_guard<std::mutex> lock_(mutex_);
  int ret = RET_SUCCESS;
  auto it = mapper_.find(pk);
  if (it != mapper_.end()) {
    return RET_PK_ROW_ID_ALREADY_EXIST;
  }
  *row_id = count_++;
  // auto pr = std::pair<PK, uint64>(pk, *row_id);
  auto res = mapper_.insert({pk, *row_id});
  size_t sz = mapper_.size();
  int count = mapper_.count(pk);
  return RET_SUCCESS;
}

int Disk_row_manager::get_row_id(PK &pk, uint64 *row_id) { return 0; }

int Disk_row_manager::gen_row_id(PK &pk, uint64 *row_id) { return 0; }

}  // namespace IMCS
}  // namespace Tianmu
