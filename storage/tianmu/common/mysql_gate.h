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
#ifndef TIANMU_COMMON_MYSQL_GATE_H_
#define TIANMU_COMMON_MYSQL_GATE_H_
#pragma once

#include <string>
#include <vector>

#define MYSQL_SERVER 1

#include "item_strfunc.h"
#include "item_sum.h"
#include "key.h"
#include "lock.h"
#include "mysqld_suffix.h"
#include "rpl_slave.h"
#include "sp_rcontext.h"
#include "sql_array.h"
#include "sql_base.h"
#include "sql_class.h"
#include "sql_load.h"
#include "sql_optimizer.h"
#include "sql_parse.h"
#include "sql_plugin.h"
#include "sql_select.h"
#include "sql_show.h"
#include "sql_time.h"
#include "sql_tmp_table.h"
#include "transaction.h"
#include "tztime.h"

#include "m_ctype.h"
#include "my_bit.h"
#include "my_thread_local.h"
#include "mysql_com.h"
#include "probes_mysql.h"

/* Putting macros named like `max', `min' or `test'
 * into a header is a terrible idea. */

#undef max
#undef min
#undef bool
#undef test
#undef sleep

#undef pthread_mutex_init
#undef pthread_mutex_lock
#undef pthread_mutex_unlock
#undef pthread_mutex_destroy
#undef pthread_mutex_wait
#undef pthread_mutex_timedwait
#undef pthread_mutex_trylock
#undef pthread_mutex_t
#undef pthread_cond_init
#undef pthread_cond_wait
#undef pthread_cond_timedwait
#undef pthread_cond_t
#undef pthread_attr_getstacksize

// TODO()
using fields_t = std::vector<enum_field_types>;

my_time_t tianmu_sec_since_epoch(int year, int mon, int mday, int hour, int min, int sec);

namespace Tianmu {
namespace common {
bool IsTimeStampZero(MYSQL_TIME &t);
void GMTSec2GMTTime(MYSQL_TIME *tmp, my_time_t t);

int wildcmp(const DTCollation &collation, const char *str, const char *str_end, const char *wildstr,
            const char *wildend, int escape, int w_one, int w_many);
size_t strnxfrm(const DTCollation &collation, uchar *src, size_t src_len, const uchar *dest, size_t dest_len);

void SetMySQLTHD(THD *thd);
}  // namespace common
}  // namespace Tianmu

#endif  // TIANMU_COMMON_MYSQL_GATE_H_
