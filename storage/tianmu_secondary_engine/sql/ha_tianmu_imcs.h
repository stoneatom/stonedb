/* Copyright (c) 2000, 2022, Oracle and/or its affiliates.

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
   @file tianmu_secondary_engine/ha_tianmu_icms.cc

   this used for tianmu in-memory column store. to hold the data which load from
   innodb via secondary engine load/unload command.

   Created 2/2/2023 Hom.LEE
*/

#ifndef __HA_TIANMU_IMCS_H__
#define __HA_TIANMU_IMCS_H__

#include "../imcs/imcs_base.h"
#include "../handler/ha_tianmu_secondary_engine.h"

#include "my_base.h"
#include "sql/handler.h"

namespace Tianmu {
namespace IMCS {
/**
  imcs(in-memory column store, aka IMCS), which is used for accelerating the
  analytical processing for mysql. IMCS as the secondary engine of mysql, which
  acts as a secondary engine. when a alter table with secondary_load statment
  was executed, mysql will read the data from primary table (aka, innodb table)
  and loads into secondary engine in column format.
*/

/*
class IMCS_TABLE;
class IMCS_TABLE_SHARE;
*/
using namespace Tianmu_secondary_engine;

class ha_tianmu_imcs : public ha_tianmu_secondary {
 public:
  ha_tianmu_imcs(handlerton *hton, TABLE_SHARE *table_share);

 private:
  int create(const char *, TABLE *, HA_CREATE_INFO *, dd::Table *) override;
  int open(const char *name, int mode, unsigned int test_if_locked,
           const dd::Table *table_def) override;
  int close() override { return 0; }

  int rnd_init(bool) override { return 0; }

  int rnd_next(unsigned char *) override { return HA_ERR_END_OF_FILE; }

  int rnd_pos(unsigned char *, unsigned char *) override {
    return HA_ERR_WRONG_COMMAND;
  }

  int info(unsigned int) override;

  ha_rows records_in_range(unsigned int index, key_range *min_key,
                           key_range *max_key) override;

  void position(const unsigned char *) override {}

  unsigned long index_flags(unsigned int, unsigned int, bool) const override;

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             thr_lock_type lock_type) override;

  Table_flags table_flags() const override;

  const char *table_type() const override { return "TIANMU_RAPID_IN_MEMORY"; }

  int load_table(THD *thd, const TABLE &table) override;

  int unload_table(THD *thd, const char *db_name, const char *table_name,
                   bool error_if_not_loaded) override;

  THR_LOCK_DATA m_lock;

 private:
  // FOR a in-memory column store table.
  // IMCS_TABLE* imcs_table_;
  // IMCS_TABLE_SHARE* imcs_table_share_;
};

}  // namespace IMCS
}  // namespace Tianmu

#endif  //__HA_TIANMU_IMCS_H__
