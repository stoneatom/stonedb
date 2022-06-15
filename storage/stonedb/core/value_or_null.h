/* Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
   Use is subject to license terms

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335 USA
*/
#ifndef STONEDB_CORE_VALUE_OR_NULL_H_
#define STONEDB_CORE_VALUE_OR_NULL_H_
#pragma once

#include <optional>

#include "common/common_definitions.h"
#include "types/rc_data_types.h"

namespace stonedb {
namespace core {
class ValueOrNull final {
  friend class Item_sdbfield;

 public:
  ValueOrNull() : x(common::NULL_VALUE_64), null(true) {}
  explicit ValueOrNull(int64_t x) : x(x), null(false) { DEBUG_ASSERT(x != common::NULL_VALUE_64); }
  ValueOrNull(types::RCNum const &rcn);
  ValueOrNull(types::RCDateTime const &rcdt);
  ValueOrNull(types::BString const &rcs);
  ValueOrNull(ValueOrNull const &von);

  ~ValueOrNull() { Clear(); }
  ValueOrNull &operator=(ValueOrNull const &von);
  bool operator!=(ValueOrNull const &v) const {
    if (null && v.null) return false;

    if ((null && (!v.null)) || (!null && v.null)) return true;

    bool diff = false;
    if (sp) {
      diff = (v.sp ? ((len != v.len) || (std::memcmp(sp, v.sp, len) != 0)) : true);
    } else if (v.sp || (x != v.x))
      diff = true;
    return diff;
  }

  bool IsNull() const { return null; }
  bool IsString() const { return sp != nullptr; }
  bool NotNull() const { return !null; }
  size_t StrLen() const { return len; }
  int64_t Get64() const { return x; }

  void SetFixed(int64_t v) {
    Clear();
    x = v;
    null = false;
  }
  void SetDouble(double v) {
    Clear();
    x = *(int64_t *)&v;
    null = false;
  }
  double GetDouble() const { return *(double *)&x; }
  //! assign an externally allocated string value
  void SetString(char *s, uint n) {
    Clear();
    sp = s;
    len = n;
    null = false;
  }

  //! assign a string from BString
  //! \param rcs may be null, if is persistent, then create a string copy and
  //! become string owner
  void SetBString(const types::BString &rcs);

  //! create a local copy of the string pointed by sp
  void MakeStringOwner();

  /*! Get a string in the form of RSBString
   * \param rcs is given the string value from this
   * Warning 1: the string pointed to by 'rcs' should be copied as soon as
   * possible and not used later on (it may point to an internal buffer of MySQL
   * String object ). Warning 2: 'rcs' must NOT be persistent, otherwise memory
   * leaks may occur
   */
  void GetBString(types::BString &rcs) const;
  std::optional<std::string> ToString() const;

 private:
  int64_t x;                  // 8-byte value of an expression; interpreted as int64_t or double
  char *sp = nullptr;         // != 0 if string value assigned
  uint len = 0;               // string length; used only for ValueType::VT_STRING
  bool string_owner = false;  // if true, destructor must deallocate sp
  bool null;

  void Swap(ValueOrNull &von);
  void Clear() {
    if (string_owner) delete[] sp;
    sp = nullptr;
    string_owner = false;
    null = true;
    x = common::NULL_VALUE_64;
    len = 0;
  }
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_VALUE_OR_NULL_H_