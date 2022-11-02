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

#include "common/mysql_gate.h"

#include <iostream>

namespace Tianmu {
namespace common {

bool IsTimeStampZero(MYSQL_TIME &t) {
  return t.day == 0 && t.hour == 0 && t.minute == 0 && t.month == 0 && t.second == 0 && t.second_part == 0 &&
         t.year == 0;
}

void GMTSec2GMTTime(MYSQL_TIME *tmp, my_time_t t) {
  time_t tmp_t = (time_t)t;
  struct tm tmp_tm;
  gmtime_r(&tmp_t, &tmp_tm);
  localtime_to_TIME(tmp, &tmp_tm);
  tmp->time_type = MYSQL_TIMESTAMP_DATETIME;
}

int wildcmp(const DTCollation &collation, const char *str, const char *str_end, const char *wildstr,
            const char *wildend, int escape, int w_one, int w_many) {
  return collation.collation->coll->wildcmp(collation.collation, str, str_end, wildstr, wildend, escape, w_one, w_many);
}

size_t strnxfrm(const DTCollation &collation, uchar *src, size_t src_len, const uchar *dest, size_t dest_len) {
  return collation.collation->coll->strnxfrm(collation.collation, src, src_len, src_len, dest, dest_len,
                                             MY_STRXFRM_PAD_WITH_SPACE);
}

void SetMySQLTHD(THD *thd) {
  my_thread_set_THR_THD(thd);
  my_thread_set_THR_MALLOC(&thd->mem_root);
}

}  // namespace common
}  // namespace Tianmu
