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

/****************************************************************************
 * To define the common things, such as data type.
 ****************************************************************************/

#ifndef TIANMU_IMCS_COMMON_COMMON_DEF_H
#define TIANMU_IMCS_COMMON_COMMON_DEF_H

#include <cstring>
#include <map>
#include <unordered_map>
#include <vector>

#include "../dictionary/dictionary_encoder.h"
#include "../imcs_base.h"
#include "../row_manager.h"
#include "../statistics/numeric_stat_value.h"
#include "error.h"
#include "item.h"
#include "table.h"
#include "thr_lock.h"

namespace Tianmu {
namespace IMCS {
struct TianmuSecondaryShare;
}

}  // namespace Tianmu

// Map from (db_name, table_name) to the TianmuSecondaryShare with table state.
class LoadedTables {
  std::map<std::pair<std::string, std::string>,
           Tianmu::IMCS::TianmuSecondaryShare *>
      m_tables;
  std::mutex m_mutex;

 public:
  void add(const std::string &db, const std::string &table) {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_tables.emplace(std::piecewise_construct, std::make_tuple(db, table),
                     std::make_tuple());
  }

  void add(const std::string &db, const std::string &table,
           Tianmu::IMCS::TianmuSecondaryShare *share) {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_tables.insert({std::make_pair(db, table), share});
  }

  Tianmu::IMCS::TianmuSecondaryShare *get(const std::string &db,
                                          const std::string &table) {
    std::lock_guard<std::mutex> guard(m_mutex);
    auto it = m_tables.find(std::make_pair(db, table));
    return it == m_tables.end() ? nullptr : it->second;
  }

  void erase(const std::string &db, const std::string &table) {
    std::lock_guard<std::mutex> guard(m_mutex);
    auto it = m_tables.find(std::make_pair(db, table));
    if (it != m_tables.end()) {
      m_tables.erase(it);
    }
  }
};

namespace Tianmu {
namespace IMCS {

struct Field_pruned {
  enum_field_types type;

  std::string field_name;

  // equals to field->pack_length()
  uint32 mysq_col_len;

  // the field length stored in rapid
  uint32 rpd_length;
};

// singleton instance for a table
struct TianmuSecondaryShare {
  THR_LOCK lock;

  // TABLE *table_;

  // imcu index list of the corresponding table, the real imcu is in the imcs
  // singleton instance
  std::vector<uint32> imcu_idx_list_;

  // map fron field to column index
  // TODO: unordered_map => map?
  std::map<std::string, uint32> field_to_col_idx_;

  std::vector<Field_pruned> fields_;

  // the number of rows in a imcu
  uint32 imcu_rows_;

  // the number of bucket in a chunk
  uint32 n_buckets_;

  // the number of rows in a bucket
  // equal to the number of cells in a bucket
  uint32 n_cells_;

  // the size of a imcu in byte
  uint32 imcu_size_;

  // directory of the table
  IMCS::DictionaryEncoder encoder_;

  // map from pk to row id
  Row_manager *row_manager_;

  TABLE table_;

  uint32 primary_key_idx_;

  std::string primary_key_name_;

  TianmuSecondaryShare(const TABLE &table);
  TianmuSecondaryShare() {
    thr_lock_init(&lock);
    row_manager_ = new Mem_row_manager();
  }
  ~TianmuSecondaryShare() { thr_lock_delete(&lock); }

  int get_n_rows_in_imcu(uint32 imcu_size, uint32 *n_buckets, uint32 *n_cells);

  uint32 position_imcu(uint64 idx) { return idx / imcu_rows_; }

  uint32 offset_imcu(uint64 idx) { return idx % imcu_rows_; }

  uint32 position_bucket(uint32 idx) { return idx / n_cells_; }

  uint32 offset_bucket(uint32 idx) { return idx % n_cells_; }

  // Not copyable. The THR_LOCK object must stay where it is in memory
  // after it has been initialized.
  TianmuSecondaryShare(const TianmuSecondaryShare &) = delete;
  TianmuSecondaryShare &operator=(const TianmuSecondaryShare &) = delete;
};

const char *get_ret_message(int code);

}  // namespace IMCS
}  // namespace Tianmu

#endif