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

/**
  A cell maps into a row, a column. In IMCS, a row will be splited into a lot of
  independent columns. A coloumn data we call it, a cell. For example,

  +----------------------------------------------------------------------+
  |______col1_____|_________col2_____|___....___________|___colN_________+
  |      val1              val2          ....               val3         +
  +----------------------------------------------------------------------+

  this row will splite into [val1], [val2], [...], [valN]. Here, [val1] calls
  a cell, [val2] also calls a cell.

  Created 7/2/2023 Hom.LEE
 */

#ifndef __CELL_H__
#define __CELL_H__

#include <string>

#include "../common/common_def.h"
#include "../common/error.h"
#include "../imcs_base.h"
#include "field.h"

namespace Tianmu {
namespace IMCS {

class Cell {
 private:
  static constexpr uint32 NEWDECIMAL_FLAG = (1u << 31);
  static constexpr uint8 NEWDECIMAL_PREFIX = 4 + 4;

 public:
  Cell(uint32 size) : size_(size), data_(nullptr) {}
  virtual ~Cell() = default;

  static int get_cell_len(Field *field, uint32 *len);

  int set_val(TianmuSecondaryShare *share, Field *field);

  void set_size(uint32 size) { size_ = size; }

  void set_data(uchar *data) { data_ = data; }

  uint32 size() const { return size_ & (NEWDECIMAL_FLAG - 1); }

  const uchar *data() const { return data_; }

  int fill_rec(TianmuSecondaryShare *share, enum_field_types type, uchar *rec);

  template <typename T>
  T get_val();

 private:
#ifdef DEBUG
  // row id to indicate which rows this cell belongs to.
  RowID row_id_;
#endif

  // size of this cell.
  uint32 size_;

  // data buffer, use the dynamic memory to store data.
  uchar *data_;
};

}  // namespace IMCS
}  // namespace Tianmu

#endif  //__CELL_H__
