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

#include "row.h"

#include "read0read.h"

namespace Tianmu {
namespace IMCS {

bool trx_visible(ReadView *read_view, uint64 trx_id) {
  table_name_t table_name;  // just for apdator
  return read_view->changes_visible(trx_id, table_name);
}

bool Row::visible(ReadView *read_view) {
  if (deleted() && trx_visible(read_view, delete_id_)) {
    return false;
  }
  if (trx_visible(read_view, insert_id_)) {
    return true;
  }
  return false;
}

}  // namespace IMCS
}  // namespace Tianmu
