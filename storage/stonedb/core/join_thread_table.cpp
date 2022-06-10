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

#include "core/join_thread_table.h"

#include "base/util/spinlock.h"
#include "common/assert.h"

namespace stonedb {
namespace core {
//------------------------JoinThreadTable::Finder------------------------------------
JoinThreadTable::Finder::Finder(JoinThreadTable *thread_table, std::string *key_buffer)
    : thread_table_(thread_table), hash_table_finder_(thread_table_->GetHashTable(), key_buffer) {}

int64_t JoinThreadTable::Finder::GetNextRow(bool auto_reset) {
  int64_t row = hash_table_finder_.GetNextRow();
  if (row != common::NULL_VALUE_64) {
    if (auto_reset) thread_table_->ResetTraversed(row);
  }
  return row;
}

//  JoinThreadTable: join thread base class.
JoinThreadTable::JoinThreadTable(std::vector<ColumnBinEncoder> &&column_encoder, bool ignore_conflicts)
    : column_encoder_(std::move(column_encoder)), ignore_conflicts_(ignore_conflicts) {}

void JoinThreadTable::GetColumnEncoder(std::vector<ColumnBinEncoder> *column_encoder) const {
  column_encoder->assign(column_encoder_.begin(), column_encoder_.end());
}

int64_t JoinThreadTable::AddKeyValue(const std::string &key_buffer, bool *too_many_conflicts) {
  int64_t hash_row = GetHashTable()->AddKeyValue(key_buffer, too_many_conflicts);
  if (hash_row != common::NULL_VALUE_64) {
    *too_many_conflicts = *too_many_conflicts && !ignore_conflicts_;
    if (!(*too_many_conflicts)) SetTraversed(hash_row);
  }
  return hash_row;
}

// JoinThreadTableImpl: owned hash table independent.
class JoinThreadTableImpl : public JoinThreadTable {
 public:
  JoinThreadTableImpl(uint32_t pack_power, const HashTable::CreateParams &create_params,
                      std::vector<ColumnBinEncoder> &&column_encoder, bool watch_traversed = false,
                      bool ignore_conflicts = false)
      : JoinThreadTable(std::move(column_encoder), ignore_conflicts) {
    hash_table_ = std::make_unique<HashTable>(create_params);
    if (watch_traversed) outer_filter_ = std::make_unique<Filter>(hash_table_->GetCount(), pack_power, false);
  }

  JoinThreadTableImpl(JoinThreadTableImpl &&table) = default;
  ~JoinThreadTableImpl() override {}

  // Overridden from JoinThreadTable:
  int64_t GetOuterTraversedCount() const override { return outer_filter_->NoOnes(); }
  bool IsOuterTraversed(int64_t hash_row) override { return outer_filter_->Get(hash_row); }
  void SetTraversed(int64_t hash_row) override {
    if (outer_filter_) outer_filter_->Set(hash_row);
  }
  void ResetTraversed(int64_t hash_row) override {
    if (outer_filter_) outer_filter_->Reset(hash_row);
  }
  HashTable *GetHashTable() override { return hash_table_.get(); }
  const HashTable *GetHashTable() const override { return hash_table_.get(); }

 protected:
  std::unique_ptr<HashTable> hash_table_;
  std::unique_ptr<Filter> outer_filter_;
};

using HighSyncFilter = SyncFilter<base::util::spinlock>;

// JoinThreadTableShared: share hash table between multi threads.
class JoinThreadTableShared : public JoinThreadTable {
 public:
  JoinThreadTableShared(std::shared_ptr<HashTable> hash_table, std::shared_ptr<HighSyncFilter> outer_filter,
                        std::vector<ColumnBinEncoder> &&column_encoder, bool ignore_conflicts = false)
      : JoinThreadTable(std::move(column_encoder), ignore_conflicts),
        hash_table_(hash_table),
        outer_filter_(outer_filter) {}

  JoinThreadTableShared(JoinThreadTableShared &&table) = default;
  ~JoinThreadTableShared() override {}

  // Overridden from JoinThreadTable:
  int64_t GetOuterTraversedCount() const override { return outer_filter_->GetOnesCount(); }
  bool IsOuterTraversed(int64_t hash_row) override { return outer_filter_->Get(hash_row); }
  void SetTraversed(int64_t hash_row) override {
    if (outer_filter_) outer_filter_->Set(hash_row, true);
  }
  void ResetTraversed(int64_t hash_row) override {
    if (outer_filter_) outer_filter_->Reset(hash_row, true);
  }
  HashTable *GetHashTable() override { return hash_table_.get(); }
  const HashTable *GetHashTable() const override { return hash_table_.get(); }

 protected:
  std::shared_ptr<HashTable> hash_table_;
  std::shared_ptr<HighSyncFilter> outer_filter_;
};

// JoinThreadTableManagerImpl
class JoinThreadTableManagerImpl : public JoinThreadTableManager {
 public:
  JoinThreadTableManagerImpl(uint32_t pack_power, size_t table_count, const HashTable::CreateParams &create_params,
                             std::vector<ColumnBinEncoder> &&column_encoder, bool watch_traversed = false,
                             bool ignore_conflicts = false) {
    DEBUG_ASSERT(table_count >= 1u);
    tables_.reserve(table_count);
    HashTable::CreateParams slice_create_params(create_params);
    slice_create_params.max_table_size /= table_count;
    for (size_t index = 0; index < table_count; ++index) {
      std::vector<ColumnBinEncoder> slice_column_encoder(column_encoder.begin(), column_encoder.end());
      tables_.emplace_back(pack_power, slice_create_params, std::move(slice_column_encoder), watch_traversed,
                           ignore_conflicts);
    }
  }

  ~JoinThreadTableManagerImpl() override {}

  // Overriden from JoinThreadTableManager:
  bool IsShared() const override { return false; }
  size_t GetTableCount() const override { return tables_.size(); }
  JoinThreadTable *GetTable(size_t index) override { return &tables_[index]; }

 private:
  std::vector<JoinThreadTableImpl> tables_;
};

class JoinThreadTableManagerShared : public JoinThreadTableManager {
 public:
  JoinThreadTableManagerShared(uint32_t pack_power, size_t table_count, const HashTable::CreateParams &create_params,
                               std::vector<ColumnBinEncoder> &&column_encoder, bool watch_traversed = false,
                               bool ignore_conflicts = false) {
    DEBUG_ASSERT(table_count >= 1u);

    hash_table_ = std::make_shared<HashTable>(create_params);
    if (watch_traversed) outer_filter_ = std::make_shared<HighSyncFilter>(hash_table_->GetCount(), pack_power, false);

    tables_.reserve(table_count);
    for (size_t index = 0; index < table_count; ++index) {
      std::vector<ColumnBinEncoder> slice_column_encoder(column_encoder.begin(), column_encoder.end());
      tables_.emplace_back(hash_table_, outer_filter_, std::move(slice_column_encoder), ignore_conflicts);
    }
  }

  ~JoinThreadTableManagerShared() override {}

  // Overriden from JoinThreadTableManager:
  bool IsShared() const override { return true; }
  size_t GetTableCount() const override { return tables_.size(); }
  JoinThreadTable *GetTable(size_t index) override { return &tables_[index]; }

 private:
  std::shared_ptr<HashTable> hash_table_;
  std::shared_ptr<HighSyncFilter> outer_filter_;
  std::vector<JoinThreadTableShared> tables_;
};

// static
JoinThreadTableManager *JoinThreadTableManager::CreateTableManager(uint32_t pack_power, size_t table_count,
                                                                   const HashTable::CreateParams &create_params,
                                                                   std::vector<ColumnBinEncoder> &&column_encoder,
                                                                   bool watch_traversed, bool ignore_conflicts) {
  return new JoinThreadTableManagerImpl(pack_power, table_count, create_params, std::move(column_encoder),
                                        watch_traversed, ignore_conflicts);
}

// static
JoinThreadTableManager *JoinThreadTableManager::CreateSharedTableManager(uint32_t pack_power, size_t table_count,
                                                                         const HashTable::CreateParams &create_params,
                                                                         std::vector<ColumnBinEncoder> &&column_encoder,
                                                                         bool watch_traversed, bool ignore_conflicts) {
  return new JoinThreadTableManagerShared(pack_power, table_count, create_params, std::move(column_encoder),
                                          watch_traversed, ignore_conflicts);
}

}  // namespace core
}  // namespace stonedb
