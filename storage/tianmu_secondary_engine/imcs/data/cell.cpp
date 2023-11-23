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

#include "cell.h"

#include <memory>

#include "../common/assert.h"
#include "../common/common_def.h"
#include "../utilities/ptr_util.h"
// #include "data0type.h"

namespace Tianmu {
namespace IMCS {

template <>
int8_t Cell::get_val() {
  assert(size() >= sizeof(int8_t));
  int8_t val;
  val_ptr(data_, &val);
  return val;
}

template <>
int16_t Cell::get_val() {
  assert(size() >= sizeof(int16_t));
  int16_t val;
  val_ptr(data_, &val);
  return val;
}

template <>
int32_t Cell::get_val() {
  assert(size() >= 3);  // MYSQL_TYPE_INT24 or MYSQL_TYPE_LONG
  int32 res = static_cast<int32_t>(data_[0]) |
              (static_cast<int32_t>(data_[1]) << 8) |
              (static_cast<int32_t>(data_[2]) << 16);
  if (size() == 4) {
    res |= (static_cast<int32_t>(data_[3]) << 24);
  }
  return res;
}

template <>
uint32 Cell::get_val() {
  assert(size() >= sizeof(uint32));
  uint32 val;
  val_ptr(data_, &val);
  return val;
}

template <>
int64_t Cell::get_val() {
  assert(size() >= sizeof(int64_t));
  int64_t val;
  val_ptr(data_, &val);
  return val;
}

template <>
float Cell::get_val() {
  assert(size() >= sizeof(float));
  float val;
  val_ptr(data_, &val);
}

template <>
double Cell::get_val() {
  assert(size() >= sizeof(double));
  double val;
  val_ptr(data_, &val);
  return val;
}

template <>
my_decimal Cell::get_val() {
  if (size_ & NEWDECIMAL_FLAG) {
    // copy from field_newdecimal
    uchar *ptr = data_;
    uint32 precision;
    val_ptr(ptr, &precision);
    ptr += 4;
    uint32 dec;
    val_ptr(ptr, &dec);
    ptr += 4;
    my_decimal val;
    // TODO: uncheck m_keep_precision, force to false now
    binary2my_decimal(E_DEC_FATAL_ERROR, ptr, &val, precision, dec, false);
    // TODO: uncheck operator=
    return val;
  } else {
    // copy from field_decimal
    int not_used;
    const char *end_not_used;
    double dval = my_strntod(&my_charset_bin, pointer_cast<const char *>(data_),
                             size_, &end_not_used, &not_used);
    my_decimal val;
    double2my_decimal(E_DEC_FATAL_ERROR, dval, &val);
    // TODO: uncheck operator=
    return val;
  }
}

int Cell::get_cell_len(Field *field, uint32 *len) {
  switch (field->type()) {
    // case MYSQL_TYPE_TINY:
    //   *len = sizeof(int8);
    //   return RET_SUCCESS;
    // case MYSQL_TYPE_SHORT:
    //   *len = sizeof(int16);
    //   return RET_SUCCESS;
    // case MYSQL_TYPE_INT24:
    //   *len = 3;
    //   return RET_SUCCESS;
    // case MYSQL_TYPE_LONG:
    //   *len = sizeof(int32);
    //   return RET_SUCCESS;
    // case MYSQL_TYPE_LONGLONG:
    //   *len = sizeof(long long);
    //   return RET_SUCCESS;
    // case MYSQL_TYPE_FLOAT:
    //   *len = sizeof(float);
    //   return RET_SUCCESS;
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_DECIMAL:
      // TODO: uncheck
      *len = field->pack_length();
      return RET_SUCCESS;
    case MYSQL_TYPE_NEWDECIMAL:
      // TODO: uncheck
      *len = NEWDECIMAL_PREFIX + field->pack_length();
      return RET_SUCCESS;
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR:
      *len = sizeof(uint32);
      return RET_SUCCESS;
    default:
      return RET_MYSQL_TYPE_NOT_SUPPORTED;
  }
}

int Cell::set_val(TianmuSecondaryShare *share, Field *field) {
  int ret = RET_SUCCESS;
  switch (field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_DECIMAL:
      memcpy(data_, field->field_ptr(), size_);
      return RET_SUCCESS;
    case MYSQL_TYPE_NEWDECIMAL: {
      Field_new_decimal *field_new_decimal =
          dynamic_cast<Field_new_decimal *>(field);
      uint32 precision = field_new_decimal->precision;
      uint32 dec = field_new_decimal->dec;
      uchar *ptr = data_;
      ptr = set_ptr(ptr, precision);
      ptr = set_ptr(ptr, dec);
      memcpy(ptr, field->field_ptr(), size() - NEWDECIMAL_PREFIX);
      size_ |= NEWDECIMAL_FLAG;
      return RET_SUCCESS;
    }
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR: {
      uint32 code = share->encoder_.encode(field);
      set_ptr(data_, code);
      return RET_SUCCESS;
    }
    default:
      return RET_MYSQL_TYPE_NOT_SUPPORTED;
  }
}

void str_fill_rec(String *str, uchar *rec) {
  uint32 lenlen;
  uint32 len = str->length();
  if (len & (1 << 12)) {
    rec[0] = static_cast<uchar>(len & 0xfful);
    rec[1] = static_cast<uchar>((len >> 8) & 0xfful);
    lenlen = 2;
  } else {
    rec[0] = static_cast<uchar>(len & 0xfful);
    lenlen = 1;
  }
  memcpy(rec + lenlen, str->ptr(), len);
}

int Cell::fill_rec(TianmuSecondaryShare *share, enum_field_types type,
                   uchar *rec) {
  switch (type) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_DECIMAL:
      memcpy(rec, data_, size());
      return RET_SUCCESS;
    case MYSQL_TYPE_NEWDECIMAL:
      memcpy(rec, data_ + NEWDECIMAL_PREFIX, size() - NEWDECIMAL_PREFIX);
      return RET_SUCCESS;
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR: {
      uint32 code = get_val<uint32>();
      String *str = share->encoder_.decode(code);
      str_fill_rec(str, rec);
      return RET_SUCCESS;
    }
    default:
      return RET_MYSQL_TYPE_NOT_SUPPORTED;
  }
}
}  // namespace IMCS

}  // namespace Tianmu