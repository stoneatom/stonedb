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

#include <algorithm>
#include <fstream>
#include <mutex>

#include "core/rc_table.h"
#include "core/table_share.h"

namespace stonedb {
namespace core {
TableShare::TableShare(const fs::path &table_path, const TABLE_SHARE *table_share)
    : no_cols(table_share->fields), table_path(table_path) {
  system::StoneDBFile ftbl;
  ftbl.OpenReadOnly(table_path / common::TABLE_DESC_FILE);
  ftbl.ReadExact(&meta, sizeof(meta));

  if (meta.magic != common::FILE_MAGIC)
    throw common::DatabaseException("Bad format of table definition in " + table_path.string() + ": bad signature!");

  if (meta.ver != common::TABLE_DATA_VERSION)
    throw common::DatabaseException("Bad format of table definition in " + table_path.string() + ": invalid version!");

  if (meta.pss > common::MAX_PSS)
    throw common::DatabaseException("Bad format of table definition in " + table_path.string() +
                                    ": invalid pack size shift " + std::to_string(meta.pss));

  m_columns.reserve(no_cols);

  system::StoneDBFile fv;
  fv.OpenReadOnly(table_path / common::TABLE_VERSION_FILE);
  for (uint i = 0; i < no_cols; i++) {
    common::TX_ID xid;
    fv.ReadExact(&xid, sizeof(xid));
    m_columns.emplace_back(std::make_unique<ColumnShare>(
        this, xid, i, table_path / common::COLUMN_DIR / std::to_string(i), table_share->field[i]));
  }

  thr_lock_init(&thr_lock);
}

TableShare::~TableShare() {
  thr_lock_delete(&thr_lock);

  if (current.use_count() > 1)
    STONEDB_LOG(LogCtl_Level::FATAL, "TableShare still has ref outside by current %ld", current.use_count());

  if (!write_table.expired()) STONEDB_LOG(LogCtl_Level::FATAL, "TableShare still has ref outside by write table");

  for (auto &t : versions)
    if (!t.expired()) STONEDB_LOG(LogCtl_Level::FATAL, "TableShare still has ref outside by old versions");
}

std::shared_ptr<RCTable> TableShare::GetSnapshot() {
  std::scoped_lock guard(current_mtx);
  if (!current) current = std::make_shared<RCTable>(table_path, this);

  return current;
}

std::shared_ptr<RCTable> TableShare::GetTableForWrite() {
  std::unique_lock<std::mutex> lk(write_table_mtx);
  ASSERT(write_table.expired(), "Table write cannot be concurrent!");
  auto ptr = std::make_shared<RCTable>(table_path, this, current_tx);
  ptr->write_lock = std::move(lk);
  write_table = ptr;

  return ptr;
}

void TableShare::CommitWrite(RCTable *t) {
  auto sp = write_table.lock();

  ASSERT(sp, "No open tables to commit!");
  ASSERT(sp.get() == t, "Wrong table version to commit!");

  {
    std::scoped_lock guard(current_mtx);

    // The lock should be released even there is exception!
    std::shared_ptr<void> defer(nullptr, [t](...) { t->write_lock.unlock(); });

    std::weak_ptr<RCTable> save = current;
    current = sp;
    write_table.reset();
    t->PostCommit();
    if (!save.expired()) versions.push_back(save);
  }
}

unsigned long TableShare::GetCreateTime() {
  return system::GetFileCreateTime((table_path / common::TABLE_DESC_FILE).string());
}

unsigned long TableShare::GetUpdateTime() {
  return system::GetFileTime((table_path / common::TABLE_VERSION_FILE).string());
}
}  // namespace core
}  // namespace stonedb