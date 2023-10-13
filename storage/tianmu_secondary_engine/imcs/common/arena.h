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

#ifndef TIANMU_IMCS_COMMON_ARENA_H
#define TIANMU_IMCS_COMMON_ARENA_H

#include <atomic>
#include "../imcs_base.h"
#include "stdlib.h"

namespace Tianmu {
namespace IMCS {
class Arena {
 public:
  Arena() = default;

  Arena(uint32 size) : size_(size) {
    // TODO: update to ut::malloc
    alloc_ptr_ = begin_ = (uchar *)malloc(size);
  }
  ~Arena() {
    free(begin_);
    begin_ = nullptr;
    alloc_ptr_ = nullptr;
  }

  // return nullptr to indicate allocating failed
  uchar *allocate(uint32 bytes);

 private:
  // point to the first byte that has not assgined
  std::atomic<uchar *> alloc_ptr_;

  // point to the first byte that the arena has
  uchar *begin_;

  // memory size that the arena has
  uint32 size_;
};

}  // namespace IMCS
}  // namespace Tianmu

#endif