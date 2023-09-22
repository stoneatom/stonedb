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

#ifndef PLUGIN_SECONDARY_ENGINE_TIANMU_HA_SECONDARY_H_
#define PLUGIN_SECONDARY_ENGINE_TIANMU_HA_SECONDARY_H_

#include <memory>
#include <map>

#include "../imcs/filter/table_filter.h"
#include "../imcs/imcs_base.h"
#include "my_base.h"
#include "sql/handler.h"
#include "../imcs/common/common_def.h"

class THD;
struct TABLE;
struct TABLE_SHARE;
class ReadView;
struct THR_LOCK_DATA;

namespace dd {
class Table;
}

namespace Tianmu_secondary_engine {

using Tianmu::IMCS::STAGE;
/**
 * The tianmu storage engine is used for MySQL server functionality
 * related to secondary storage engines.
 *
 * There are currently no secondary storage engines mature enough to be merged
 * into mysql-trunk. Therefore, this bare-minimum storage engine, with no
 * actual functionality and implementing only the absolutely necessary handler
 * interfaces to allow setting it as a secondary engine of a table, was created
 * to facilitate pushing MySQL server code changes to mysql-trunk with test
 * coverage without depending on ongoing work of other storage engines.
 *
 */
/**
 * TODO: Here, we should add another type of TABLE and TABLE_SHARE. Because we
 * want the data are written into in-memory table of Tianmu in column format.
 *
 */
enum class ENGINE_TYPE : int { ET_IN_MEMORY = 0, ET_ON_DISK, ET_BOTH };

static const char *innobase_hton_name = "Innodb";

class ha_tianmu_secondary : public handler {
 public:
  ha_tianmu_secondary(handlerton *hton, TABLE_SHARE *table_share);
  virtual ~ha_tianmu_secondary() = default;

 private:
  int create(const char *, TABLE *, HA_CREATE_INFO *, dd::Table *) override;

  int open(const char *name, int mode, unsigned int test_if_locked,
           const dd::Table *table_def) override;

  int close() override;

  int rnd_init(bool) override;

  int rnd_next(uchar *) override;

  int rnd_end() override;

  int rnd_pos(unsigned char *, unsigned char *) override;

  const Item *cond_push(const Item *cond) override;

  int info(unsigned int) override;

  ha_rows records_in_range(unsigned int index, key_range *min_key,
                           key_range *max_key) override;

  void position(const unsigned char *) override {}

  unsigned long index_flags(unsigned int, unsigned int, bool) const override;

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             thr_lock_type lock_type) override;

  Table_flags table_flags() const override;

  const char *table_type() const override { return "TIANMU_RAPID"; }

  int load_table(THD *thd, const TABLE &table) override;

  int unload_table(THD *thd, const char *db_name, const char *table_name,
                   bool error_if_not_loaded) override;

  THR_LOCK_DATA m_lock;

 private:
  bool primary_inited_;
  ENGINE_TYPE ent_type_;

  std::shared_ptr<Tianmu::IMCS::Table_filter_set> filter_set_;

  // the row id of the next scan need start
  uint64 cur_row_id_;

  Item *cond_;

  ReadView *read_view_;
};

}  // namespace Tianmu_secondary_engine

#endif  // PLUGIN_SECONDARY_ENGINE_TIANMU_HA_SECONDARY_H_
