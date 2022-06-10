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

#include "core/value_or_null.h"

#include "common/assert.h"
#include "types/rc_num.h"

namespace stonedb {
namespace core {
void ValueOrNull::SetBString(const types::BString &rcs) {
  Clear();
  if (!rcs.IsNull()) {
    null = false;
    if (rcs.IsPersistent()) {
      string_owner = true;
      sp = new char[rcs.len + 1];
      std::memcpy(sp, rcs.val, rcs.len);
      sp[rcs.len] = 0;
    } else {
      sp = rcs.val;
      string_owner = false;
    }
    len = rcs.len;
  }
}

void ValueOrNull::MakeStringOwner() {
  if (!sp || string_owner) return;
  char *tmp = new char[len + 1];
  std::memcpy(tmp, sp, len);
  tmp[len] = 0;
  sp = tmp;
  string_owner = true;
}

std::optional<std::string> ValueOrNull::ToString() const {
  if (null) return {};

  return std::string(sp, len);
}

void ValueOrNull::GetBString(types::BString &rcs) const {
  if (null) {
    types::BString rcs_null;
    rcs = rcs_null;
  } else {
    // copy either from sp or x
    if (sp)
      rcs = types::BString(sp, len, true);
    else
      rcs = types::RCNum(x).ToBString();
    rcs.MakePersistent();
  }
}

ValueOrNull::ValueOrNull(ValueOrNull const &von)
    : x(von.x),
      sp(von.string_owner ? new char[von.len + 1] : von.sp),
      len(von.len),
      string_owner(von.string_owner),
      null(von.null) {
  if (string_owner) {
    std::memcpy(sp, von.sp, len);
    sp[len] = 0;
  }
}

ValueOrNull &ValueOrNull::operator=(ValueOrNull const &von) {
  if (&von != this) {
    ValueOrNull tmp(von);
    Swap(tmp);
  }
  return (*this);
}

ValueOrNull::ValueOrNull(types::RCNum const &rcn) : x(rcn.GetValueInt64()), null(rcn.IsNull()) {}

ValueOrNull::ValueOrNull(types::RCDateTime const &rcdt) : x(rcdt.GetInt64()), null(rcdt.IsNull()) {}

ValueOrNull::ValueOrNull(types::BString const &rcs)
    : x(common::NULL_VALUE_64), sp(new char[rcs.len + 1]), len(rcs.len), string_owner(true), null(rcs.IsNull()) {
  std::memcpy(sp, rcs.val, len);
  sp[len] = 0;
}

void ValueOrNull::Swap(ValueOrNull &von) {
  if (&von != this) {
    std::swap(null, von.null);
    std::swap(x, von.x);
    std::swap(sp, von.sp);
    std::swap(len, von.len);
    std::swap(string_owner, von.string_owner);
  }
}
}  // namespace core
}  // namespace stonedb
