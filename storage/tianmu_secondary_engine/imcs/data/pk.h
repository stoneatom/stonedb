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

#ifndef TIANMU_IMCS_DATA_PK_H
#define TIANMU_IMCS_DATA_PK_H

#include <stdlib.h>
#include <memory>
#include "../imcs_base.h"
#include "../utilities/hash_util.h"
#include "sql/field.h"

namespace Tianmu {
namespace IMCS {

// Only the case of a single primary key is currently considered.
class PK {
 public:
  PK(Field *field) {
    field_length_ = field->pack_length();
    field_ptr_.reset(
        static_cast<uchar *>(malloc(field_length_ * sizeof(uchar))));
    memcpy(field_ptr_.get(), field->field_ptr(), field_length_);
    hashcode_ = hash_bytes(static_cast<const void *>(field_ptr_.get()),
                           static_cast<uint32>(field_length_));
  }
  PK(const PK &pk) {
    field_length_ = pk.field_length_;
    field_ptr_ = pk.field_ptr_;
    hashcode_ = pk.hashcode_;
  }
  PK() {}
  ~PK() {}

  uchar *ptr() const { return field_ptr_.get(); }

  uint64_t hash() const { return hashcode_; }

  uint32 length() const { return field_length_; }

  bool operator==(const PK &rhs) const {
    if (hash() != rhs.hash()) return false;
    uint32 l_len = length();
    uint32 r_len = rhs.length();
    if (l_len != r_len) return false;
    uint32 len = l_len;
    return (len == 0) ? 0 : memcmp(ptr(), rhs.ptr(), len) == 0;
  }

  void operator=(const PK &pk) {
    field_length_ = pk.field_length_;
    field_ptr_ = pk.field_ptr_;
    hashcode_ = pk.hashcode_;
  }

 private:
  std::shared_ptr<uchar> field_ptr_;

  uint32 field_length_;

  uint64_t hashcode_;
};

struct hash_PK_t {
  size_t operator()(const PK &pk) const {
    return reinterpret_cast<size_t>(pk.hash());
  }
};

}  // namespace IMCS
}  // namespace Tianmu

#endif