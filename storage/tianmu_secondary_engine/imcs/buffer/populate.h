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
  This buffer to store the changes which made in inndb by DML on primary table,
  such as insert a row into the primary tble or update or delete. If an
  insertion statement, the data inserted, will be added into IMCS directly,
  because the data has not been changed, and if a deletion statement, the data
  you deleted, will remove from IMCS according to their keys directly. But, the
  update operation is another topic. If the data we want to update is in
  secondary engine, we should do updating operation to update IMCS's data
  correspondingly. And, if the data we update is not in secondary engine, we
  will do nonthing. Two kind of sync machnsims are introuduce, sharp and lazy.
  Now, we only support lazy synch. This buffer stores the latest version.
  Therefore, if a column in a query is loaded into secondary engine. the buffer
  should be checked when it returns the result to user. the changes in this
  buffer should merge to some tiles.

  When read view is considered. insertion operations become complicated. if trx1
  insert into t1.col1 and t1.col1 is in secondary engine. There are two things
  will happen. if trx1 is not commmitted, then the other trans will not see
  these changes until trx1 is committed. Hence, version link should be availble
  to support multi-version concurrent view. The deletion and update operations
  meet the same problem.

  how to sync the buffer to IMCS. there are three ways. 1): time runs out. 2:)
  buffer is full. 3:) when the related data is using by a query.

  Created 2/2/2023 Hom.LEE
 */

#ifndef __POPULATE_H__
#define __POPULATE_H__

#include "../imcs_base.h"
#include "log.h"  // adapt for log0types.h
#include "log0types.h"

struct log_t;

namespace Tianmu {
namespace IMCS {

constexpr ulong RAPID_REPOPULATE_EVERY = 500;  // 500ms

void start_backgroud_threads(log_t &log);

void repopulate(log_t *log_ptr);

size_t compute_rapid_event_slot(lsn_t lsn);

}  // namespace IMCS
}  // namespace Tianmu

#endif  //__POPULATE_H__
