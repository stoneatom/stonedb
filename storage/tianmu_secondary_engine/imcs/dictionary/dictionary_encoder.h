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

#include <mutex>
#include <unordered_map>
#include "../common/arena.h"
#include "../imcs_base.h"
#include "../utilities/hash_util.h"
#include "field.h"
#include "mysql/psi/mysql_rwlock.h"
#include "rwlock_scoped_lock.h"
#include "sql_string.h"

#ifndef TIANMU_IMCS_DIRECTORY_DICTIONARY_ENCODER_H
#define TIANMU_IMCS_DIRECTORY_DICTIONARY_ENCODER_H

namespace Tianmu {
namespace IMCS {

struct String_equal {
  bool operator()(const String &lhs, const String &rhs) const {
    return memcmp((void *)(lhs.ptr()), (void *)(rhs.ptr()), lhs.length()) == 0;
  }
};

struct String_hash {
  size_t operator()(const String &str) const {
    hash_t hash = hash_bytes(static_cast<const void *>(str.ptr()),
                             static_cast<uint32>(str.length()));
    return static_cast<size_t>(hash);
  }
};

class DictionaryEncoder {
 public:
  DictionaryEncoder() : count_(0), arena_() {
    mysql_rwlock_init((PSI_rwlock_key)0, &rwlock_);
  }
  ~DictionaryEncoder() {
    dict_.clear();
    pool_.clear();
    count_ = 0;
    mysql_rwlock_destroy(&rwlock_);
  }

  uint32 encode(Field *field);

  String *decode(uint32 code);

 private:
  // TODO: is pthread_rwlock_t can guarantee visibility
  uint32 count_;

  std::unordered_map<String, uint32, String_hash, String_equal> dict_;

  std::vector<String> pool_;

  mysql_rwlock_t rwlock_;

  Arena arena_;
};

}  // namespace IMCS
}  // namespace Tianmu

#endif
