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

#include "dictionary_encoder.h"
#include "mysqld_error.h"

namespace Tianmu {
namespace IMCS {

uint32 DictionaryEncoder::encode(Field *field) {
  // TODO: find a rwlock that has upgrade opration
  rwlock_scoped_lock wrlock(&rwlock_, true, __FILE__, __LINE__);
  assert(field->type() == MYSQL_TYPE_VAR_STRING ||
         field->type() == MYSQL_TYPE_STRING ||
         field->type() == MYSQL_TYPE_VARCHAR);
  uchar *buf = arena_.allocate(field->pack_length());
  String str(reinterpret_cast<char *>(buf), sizeof(buf), &my_charset_bin);
  field->val_str(&str);
  auto it = dict_.find(str);
  if (it == dict_.end()) {
    count_++;
    auto res = dict_.insert({str, count_});
    it = res.first;
    pool_.push_back(str);
  }
  return it->second;
}

String *DictionaryEncoder::decode(uint32 code) {
  rwlock_scoped_lock rdlock(&rwlock_, false, __FILE__, __LINE__);
  if (code < (uint32)pool_.size()) {
    return &pool_[code];
  }
  return nullptr;
}

}  // namespace IMCS
}  // namespace Tianmu