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

#ifndef TIANM_IMCS_UTILITIES_PTR_UTIL_H
#define TIANM_IMCS_UTILITIES_PTR_UTIL_H

#include <cstring>

#include "../imcs_base.h"

namespace Tianmu {
namespace IMCS {

inline uchar *set_ptr(uchar *ptr, int32 val) {
  ptr[0] = static_cast<uchar>(val);
  ptr[1] = static_cast<uchar>(val >> 8);
  ptr[2] = static_cast<uchar>(val >> 16);
  ptr[3] = static_cast<uchar>(val >> 24);
  return ptr + 4;
}

inline void val_ptr(const uchar *ptr, int8_t *val) {
  *val = static_cast<int8_t>(ptr[0]);
}

inline void val_ptr(const uchar *ptr, int16_t *val) {
  *val = static_cast<int16_t>(ptr[0]) | (static_cast<int16_t>(ptr[1]) << 8);
}

inline void val_ptr(const uchar *ptr, int32_t *val) {
  *val = static_cast<int32_t>(ptr[0]) | (static_cast<int32_t>(ptr[1]) << 8) |
         (static_cast<int32_t>(ptr[2]) << 16) |
         (static_cast<int32_t>(ptr[2]) << 24);
}

inline void val_ptr(const uchar *ptr, uint32_t *val) {
  *val = static_cast<uint32>(ptr[0]) | (static_cast<uint32>(ptr[1]) << 8) |
         (static_cast<uint32>(ptr[2]) << 16) |
         (static_cast<uint32>(ptr[3]) << 24);
}

inline void val_ptr(const uchar *ptr, int64_t *val) {
  *val =
      static_cast<int64>(ptr[0]) | (static_cast<int64>(ptr[1]) << 8) |
      (static_cast<int64>(ptr[2]) << 16) | (static_cast<int64>(ptr[3]) << 24) |
      (static_cast<int64>(ptr[3]) << 32) | (static_cast<int64>(ptr[3]) << 40) |
      (static_cast<int64>(ptr[3]) << 48) | (static_cast<int64>(ptr[3]) << 56);
}

inline void val_ptr(const uchar *ptr, float *val) {
  memcpy(val, static_cast<const void *>(ptr), sizeof(float));
}

inline void val_ptr(const uchar *ptr, double *val) {
  memcpy(&val, static_cast<const void *>(ptr), sizeof(double));
}

}  // namespace IMCS
}  // namespace Tianmu

#endif
