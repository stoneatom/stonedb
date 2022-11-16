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
#ifndef TIANMU_CORE_TABLE_SHARE_H_
#define TIANMU_CORE_TABLE_SHARE_H_
#pragma once

#include "common/common_definitions.h"
#include "common/defs.h"
#include "core/column_share.h"

#include <list>
#include <mutex>
#include <string>
#include <vector>

namespace Tianmu {
namespace core {
class TianmuTable;

struct TABLE_META {
  uint64_t magic = common::FILE_MAGIC;
  uint32_t ver = common::TABLE_DATA_VERSION;
  uint32_t id;
  uint32_t pss = 16;
};

class TableShare final {
 public:
  TableShare(const fs::path &table_path, const TABLE_SHARE *table_share);
  TableShare() = delete;
  TableShare(TableShare const &) = delete;
  ~TableShare();
  void operator=(TableShare const &x) = delete;

  std::string Path() const {
    auto p = table_path;
    return p.replace_extension().string();
  }

  size_t PackSize() const { return 1 << meta.pss; }
  uint8_t PackSizeShift() const { return meta.pss; }
  uint32_t TabID() const { return meta.id; }
  size_t NumOfCols() const { return no_cols; }
  std::shared_ptr<TianmuTable> GetSnapshot();
  std::shared_ptr<TianmuTable> GetTableForWrite();

  unsigned long GetCreateTime();
  unsigned long GetUpdateTime();

  ColumnShare *GetColumnShare(size_t i) { return m_columns[i].get(); }
  void CommitWrite(TianmuTable *t);
  void Reset();
  // MySQL lock
  THR_LOCK thr_lock;

 private:
  TABLE_META meta;
  size_t no_cols;
  fs::path table_path;

  std::vector<std::unique_ptr<ColumnShare>> m_columns;

  std::shared_ptr<TianmuTable> current;
  std::mutex current_mtx;

  std::list<std::weak_ptr<TianmuTable>> versions;

  std::weak_ptr<TianmuTable> write_table;
  std::mutex write_table_mtx;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_TABLE_SHARE_H_
