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
#ifndef TIANMU_EXPORTER_EXPORT2FILE_H_
#define TIANMU_EXPORTER_EXPORT2FILE_H_
#pragma once

#include "core/temp_table.h"
#include "system/io_parameters.h"

namespace Tianmu {
namespace exporter {

class select_tianmu_export : public Query_result_export {
 public:
  // select_tianmu_export(sql_exchange *ex);
  select_tianmu_export(Query_result_export *se);
  ~select_tianmu_export(){};
  int prepare(List<Item> &list, SELECT_LEX_UNIT *u) override;
  void SetRowCount(ha_rows x);
  void SendOk(THD *thd);
  sql_exchange *SqlExchange();
  bool IsPrepared() const { return prepared_; };
  bool send_data(List<Item> &items) override;

 private:
  Query_result_export *select_export_;
  bool prepared_;
};

}  // namespace exporter
}  // namespace Tianmu

#endif  // TIANMU_EXPORTER_EXPORT2FILE_H_
