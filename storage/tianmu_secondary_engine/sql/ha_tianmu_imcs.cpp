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
   The handler of imcs for tianmu.
   Created 2/2/2023 Hom.LEE
*/
#include "ha_tianmu_imcs.h"

namespace {
struct IMCSShare {
  THR_LOCK lock;
  IMCSShare() { thr_lock_init(&lock); }
  ~IMCSShare() { thr_lock_delete(&lock); }

  // Not copyable. The THR_LOCK object must stay where it is in memory
  // after it has been initialized.
  IMCSShare(const IMCSShare &) = delete;
  IMCSShare &operator=(const IMCSShare &) = delete;
};

}  // namespace

namespace Tianmu {
namespace IMCS {

// ctor
ha_tianmu_imcs::ha_tianmu_imcs(handlerton *hton, TABLE_SHARE *table_share)
    : ha_tianmu_secondary(hton, table_share) {}

int ha_tianmu_imcs::create(const char *, TABLE *, HA_CREATE_INFO *,
                           dd::Table *) {
  return HA_ERR_WRONG_COMMAND;
}

int ha_tianmu_imcs::open(const char *, int, unsigned int, const dd::Table *) {
#if 0
  //Here we try to use ImcsShare to 
  TianmuSecondaryShare *share =
      loaded_tables->get(table_share->db.str, table_share->table_name.str);
  if (share == nullptr) {
    // The table has not been loaded into the secondary storage engine yet.
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "Table has not been loaded");
    return HA_ERR_GENERIC;
  }
  thr_lock_data_init(&share->lock, &m_lock, nullptr);
#endif
  return 0;
}

int ha_tianmu_imcs::info(unsigned int flags) {
  // Get the cardinality statistics from the primary storage engine.
  handler *primary = ha_get_primary_handler();
  int ret = primary->info(flags);
  if (ret == 0) {
    stats.records = primary->stats.records;
  }
  return ret;
}

ha_rows ha_tianmu_imcs::records_in_range(unsigned int index, key_range *min_key,
                                         key_range *max_key) {
  // Get the number of records in the range from the primary storage engine.
  return ha_get_primary_handler()->records_in_range(index, min_key, max_key);
}

unsigned long ha_tianmu_imcs::index_flags(unsigned int idx, unsigned int part,
                                          bool all_parts) const {
  const handler *primary = ha_get_primary_handler();
  const unsigned long primary_flags =
      primary == nullptr ? 0 : primary->index_flags(idx, part, all_parts);

  // Inherit the following index flags from the primary handler, if they are
  // set:
  //
  // HA_READ_RANGE - to signal that ranges can be read from the index, so that
  // the optimizer can use the index to estimate the number of rows in a range.
  //
  // HA_KEY_SCAN_NOT_ROR - to signal if the index returns records in rowid
  // order. Used to disable use of the index in the range optimizer if it is not
  // in rowid order.
  return ((HA_READ_RANGE | HA_KEY_SCAN_NOT_ROR) & primary_flags);
}

THR_LOCK_DATA **ha_tianmu_imcs::store_lock(THD *, THR_LOCK_DATA **to,
                                           thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE && m_lock.type == TL_UNLOCK)
    m_lock.type = lock_type;
  *to++ = &m_lock;
  return to;
}

handler::Table_flags ha_tianmu_imcs::table_flags() const {
  // Secondary engines do not support index access. Indexes are only used for
  // cost estimates.
  return HA_NO_INDEX_ACCESS;
}

// load innodb table data into imcs table.
int ha_tianmu_imcs::load_table(THD *thd, const TABLE &table) { return 0; }

// unload the imcs data.
int ha_tianmu_imcs::unload_table(THD *thd, const char *db_name,
                                 const char *table_name,
                                 bool error_if_not_loaded) {
  return 0;
}
}  // namespace IMCS
}  // namespace Tianmu
