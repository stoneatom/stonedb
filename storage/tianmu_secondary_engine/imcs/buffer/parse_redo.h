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

#ifndef __PARSE_REDO_H__
#define __PARSE_REDO_H__

#include "log.h"  // adapt for log0sys.h
#include "log0sys.h"
#include "mtr0types.h"
#include "fil0fil.h"
#include "mtr0log.h"

namespace Tianmu {
namespace IMCS {

byte *parse_log_body(mlog_id_t type, byte *ptr, byte *end_ptr,
                     space_id_t space_id, page_no_t page_no, ulint parsed_bytes,
                     lsn_t start_lsn, bool apply_to_rapid);

#endif
}
}