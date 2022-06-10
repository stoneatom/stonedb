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

#include "export2file.h"

#include "common/assert.h"
#include "core/engine.h"

namespace stonedb {
namespace exporter {

select_sdb_export::select_sdb_export(select_export *se)
    : select_export(se->get_sql_exchange()), se(se), prepared(false) {}

int select_sdb_export::prepare(List<Item> &list, SELECT_LEX_UNIT *u) {
  bool blob_flag = 0;
  unit = u;
  {
    List_iterator_fast<Item> li(list);
    Item *item;
    while ((item = li++)) {
      if (item->max_length >= MAX_BLOB_WIDTH) {
        blob_flag = 1;
        break;
      }
    }
  }
  field_term_length = exchange->field_term->length();
  if (!exchange->line_term->length()) exchange->line_term = exchange->field_term;  // Use this if it exists
  field_sep_char = (exchange->enclosed->length() ? (*exchange->enclosed)[0]
                    : field_term_length          ? (*exchange->field_term)[0]
                                                 : INT_MAX);
  escape_char = (exchange->escaped->length() ? (*exchange->escaped)[0] : -1);
  line_sep_char = (exchange->line_term->length() ? (*exchange->line_term)[0] : INT_MAX);
  if (!field_term_length) exchange->opt_enclosed = 0;
  if (!exchange->enclosed->length()) exchange->opt_enclosed = 1;  // A little quicker loop
  fixed_row_size = (!field_term_length && !exchange->enclosed->length() && !blob_flag);

  prepared = true;
  return 0;
}

void select_sdb_export::SetRowCount(ha_rows x) { row_count = x; }

void select_sdb_export::SendOk(THD *thd) { ::my_ok(thd, row_count); }

sql_exchange *select_sdb_export::SqlExchange() { return exchange; }

bool select_sdb_export::send_data(List<Item> &items) { return se->send_data(items); }

}  // namespace exporter
}  // namespace stonedb
