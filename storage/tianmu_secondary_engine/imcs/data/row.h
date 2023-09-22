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
  the rows format defintition. a innodb rows will be splited into columns. and
  one column is in one chunk. An innodb row has too many info. there are some
  important info should be kept, such as: taxnid, rowid, length, data, pageid,
  spaceid, null, etc.

  innodb row format.
  ------------------------------------------------------------------------------
   var length list | null list | header info | col1 ....   colN
  ------------------------------------------------------------------------------

  Created 3/2/2023 Hom.LEE
 */

#ifndef TIANMU_IMCU_DATA_ROW_H
#define TIANMU_IMCU_DATA_ROW_H

#include "../imcs_base.h"
#include "pk.h"

class ReadView;

namespace Tianmu {
namespace IMCS {

class Row {
 public:
  Row()
      : insert_id_(TRX_ID_UP_LIMIT),
        delete_id_(TRX_ID_LOW_LIMIT),
        is_deleted_(false) {}
  virtual ~Row() {}

  void set_insert_id(uint64 trx_id) { insert_id_ = trx_id; }

  uint64 insert_id() { return insert_id_; }

  void set_delete_id(uint64 trx_id) { delete_id_ = trx_id; }

  uint64 delete_id() { return delete_id_; }

  bool deleted() { return delete_id_ != TRX_ID_LOW_LIMIT; }

  bool visible(ReadView *read_view);

  void set_delete(uint64 trx_id) {
    set_delete_id(trx_id);
    is_deleted_ = true;
  }

 private:
  // Trx ID of this row.
  uint64 insert_id_;
  uint64 delete_id_;

  bool is_deleted_;
};

}  // namespace IMCS
}  // namespace Tianmu

#endif