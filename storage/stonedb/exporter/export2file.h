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
#ifndef STONEDB_EXPORTER_EXPORT2FILE_H_
#define STONEDB_EXPORTER_EXPORT2FILE_H_
#pragma once

#include "core/temp_table.h"
#include "system/io_parameters.h"

namespace stonedb {
namespace exporter {

class select_sdb_export : public select_export {
 public:
  // select_sdb_export(sql_exchange *ex);
  select_sdb_export(select_export *se);
  ~select_sdb_export(){};
  int prepare(List<Item> &list, SELECT_LEX_UNIT *u) override;
  void SetRowCount(ha_rows x);
  void SendOk(THD *thd);
  sql_exchange *SqlExchange();
  bool IsPrepared() const { return prepared; };
  bool send_data(List<Item> &items) override;

 private:
  select_export *se;
  bool prepared;
};

}  // namespace exporter
}  // namespace stonedb

#endif  // STONEDB_EXPORTER_EXPORT2FILE_H_
