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
#ifndef TIANMU_CORE_JOIN_THREAD_TABLE_H_
#define TIANMU_CORE_JOIN_THREAD_TABLE_H_
#pragma once

#include <memory>
#include <vector>

#include "common/assert.h"
#include "core/column_bin_encoder.h"
#include "core/filter.h"
#include "core/hash_table.h"
#include "core/temp_table.h"

namespace Tianmu {
namespace core {
// To avoid loading TempTable's data from multithreads, we should prelock packs
// of TempTable before invoke LockPackForUse.
class TempTablePackLocker {
 public:
  TempTablePackLocker(std::vector<vcolumn::VirtualColumn *> &vc, int cond_hashed, int packs)
      : vc_(vc), cond_hashed_(cond_hashed), packs_(packs) {
    for (int index = 0; index < cond_hashed_; ++index) {
      for (auto &var_map : vc_[index]->GetVarMap()) {
        std::shared_ptr<JustATable> tab = var_map.GetTabPtr();
        if (tab->TableType() == TType::TEMP_TABLE) {
          dynamic_cast<TempTable *>(tab.get())->ForceFullMaterialize();
          tab->LockPackForUse(var_map.col_ndx, packs_);
        }
      }
    }
  }

  ~TempTablePackLocker() {
    for (int index = 0; index < cond_hashed_; ++index) {
      for (auto &var_map : vc_[index]->GetVarMap()) {
        std::shared_ptr<JustATable> tab = var_map.GetTabPtr();
        if (tab->TableType() == TType::TEMP_TABLE) {
          tab->UnlockPackFromUse(var_map.col_ndx, packs_);
        }
      }
    }
  }

 private:
  std::vector<vcolumn::VirtualColumn *> &vc_;
  int cond_hashed_;
  int packs_;
};

template <typename TMutex>
class SyncFilter {
 public:
  SyncFilter(int64_t count, uint32_t power, bool all_ones = false) : filter_(new Filter(count, power, all_ones)) {}
  ~SyncFilter() = default;

  int64_t GetOnesCount() const { return filter_->NumOfOnes(); }
  bool IsEmpty() { return filter_->IsEmpty(); }
  bool Get(int64_t position) { return filter_->Get(position); }

  void Set(int64_t position, bool should_sync = false) {
    if (should_sync) {
      std::scoped_lock lk(sync_lock_);
      filter_->Set(position);
    } else {
      filter_->Set(position);
    }
  }

  void Reset(int64_t position, bool should_sync = false) {
    if (should_sync) {
      std::scoped_lock lk(sync_lock_);
      filter_->Reset(position);
    } else {
      filter_->Reset(position);
    }
  }

  void ResetDelayed(uint64_t position, bool should_sync = false) {
    if (should_sync) {
      std::scoped_lock lk(sync_lock_);
      filter_->ResetDelayed(position);
    } else {
      filter_->ResetDelayed(position);
    }
  }

  void Commit(bool should_sync = false) {
    if (should_sync) {
      std::scoped_lock lk(sync_lock_);
      filter_->Commit();
    } else {
      filter_->Commit();
    }
  }

 private:
  std::unique_ptr<Filter> filter_;
  TMutex sync_lock_;
};

class JoinThreadTable {
 public:
  class Finder {
   public:
    Finder(JoinThreadTable *thread_table, std::string *key_buffer);
    ~Finder() = default;

    int64_t GetMatchedRows() const { return hash_table_finder_.GetMatchedRows(); }
    int64_t GetNextRow(bool auto_reset = true);

   private:
    JoinThreadTable *thread_table_;
    HashTable::Finder hash_table_finder_;
  };

  JoinThreadTable(std::vector<ColumnBinEncoder> &&column_encoder, bool ignore_conflicts);
  virtual ~JoinThreadTable() {}

  void GetColumnEncoder(std::vector<ColumnBinEncoder> *column_encoder) const;
  ColumnBinEncoder *GetColumnEncoder(size_t cond_index) { return &column_encoder_[cond_index]; }

  virtual int64_t GetOuterTraversedCount() const = 0;
  virtual bool IsOuterTraversed(int64_t hash_row) = 0;
  virtual void SetTraversed(int64_t hash_row) = 0;
  virtual void ResetTraversed(int64_t hash_row) = 0;
  virtual HashTable *GetHashTable() = 0;
  virtual const HashTable *GetHashTable() const = 0;

  int64_t GetHashCount() const { return GetHashTable()->GetCount(); }
  int GetKeyBufferWidth() const { return GetHashTable()->GetKeyBufferWidth(); }
  int64_t AddKeyValue(const std::string &key_buffer, bool *too_many_conflicts = nullptr);
  int64_t GetTupleValue(int col, int64_t row) { return GetHashTable()->GetTupleValue(col, row); }
  void SetTupleValue(int col, int64_t row, int64_t value) { GetHashTable()->SetTupleValue(col, row, value); }

 protected:
  std::vector<ColumnBinEncoder> column_encoder_;
  bool ignore_conflicts_ = false;
};

class JoinThreadTableManager {
 public:
  virtual ~JoinThreadTableManager() {}

  static JoinThreadTableManager *CreateTableManager(uint32_t pack_power, size_t table_count,
                                                    const HashTable::CreateParams &create_params,
                                                    std::vector<ColumnBinEncoder> &&column_encoder,
                                                    bool watch_traversed = false, bool ignore_conflicts = false);
  static JoinThreadTableManager *CreateSharedTableManager(uint32_t pack_power, size_t table_count,
                                                          const HashTable::CreateParams &create_params,
                                                          std::vector<ColumnBinEncoder> &&column_encoder,
                                                          bool watch_traversed = false, bool ignore_conflicts = false);

  virtual bool IsShared() const = 0;
  virtual size_t GetTableCount() const = 0;
  virtual JoinThreadTable *GetTable(size_t index) = 0;
};

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_JOIN_THREAD_TABLE_H_
