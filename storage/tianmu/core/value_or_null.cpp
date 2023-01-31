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
#include "types/tianmu_num.h"
#include "util/log_ctl.h"

namespace Tianmu {
namespace core {
void ValueOrNull::SetBString(const types::BString &tianmu_s) {
  Clear();
  if (!tianmu_s.IsNull()) {
    null = false;
    if (tianmu_s.IsPersistent()) {
      string_owner = true;
      sp = new char[tianmu_s.len_ + 1];
      std::memcpy(sp, tianmu_s.val_, tianmu_s.len_);
      sp[tianmu_s.len_] = 0;
    } else {
      sp = tianmu_s.val_;
      string_owner = false;
    }
    len = tianmu_s.len_;
  }
}

void ValueOrNull::MakeStringOwner() {
  if (!sp || string_owner)
    return;
  char *tmp = new char[len + 1];
  std::memcpy(tmp, sp, len);
  tmp[len] = 0;
  sp = tmp;
  string_owner = true;
}

std::optional<std::string> ValueOrNull::ToString() const {
  if (null)
    return {};

  return std::string(sp, len);
}

void ValueOrNull::GetBString(types::BString &tianmu_s) const {
  if (null) {
    types::BString rcs_null;
    tianmu_s = rcs_null;
  } else {
    // copy either from sp or x
    if (sp)
      tianmu_s = types::BString(sp, len, true);
    else
      tianmu_s = types::TianmuNum(x).ToBString();
    tianmu_s.MakePersistent();
  }
}

ValueOrNull::ValueOrNull(ValueOrNull const &von)
    : x(von.x), len(von.len), string_owner(von.string_owner), null(von.null) {
  if (string_owner) {
    sp = new char[len + 1];
    std::memcpy(sp, von.sp, len);
    sp[len] = 0;
  } else {
    sp = von.sp;
  }
}

ValueOrNull &ValueOrNull::operator=(ValueOrNull const &von) {
  if (&von != this) {
    ValueOrNull tmp(von);
    Swap(tmp);
  }
  return (*this);
}

ValueOrNull::ValueOrNull(types::TianmuNum const &tianmu_n) : x(tianmu_n.GetValueInt64()), null(tianmu_n.IsNull()) {}

ValueOrNull::ValueOrNull(types::TianmuDateTime const &tianmu_dt) : x(tianmu_dt.GetInt64()), null(tianmu_dt.IsNull()) {}

ValueOrNull::ValueOrNull(types::BString const &tianmu_s)
    : x(common::NULL_VALUE_64),
      sp(new char[tianmu_s.len_ + 1]),
      len(tianmu_s.len_),
      string_owner(true),
      null(tianmu_s.IsNull()) {
  std::memcpy(sp, tianmu_s.val_, len);
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
}  // namespace Tianmu
