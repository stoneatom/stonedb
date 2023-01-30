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
#ifndef TIANMU_CORE_TIANMU_TABLE_H_
#define TIANMU_CORE_TIANMU_TABLE_H_
#pragma once

#include <string>

#include "common/common_definitions.h"
#include "core/just_a_table.h"
#include "core/table_share.h"
#include "core/tianmu_attr.h"
#include "core/tianmu_mem_table.h"
#include "util/fs.h"

namespace Tianmu {

namespace system {
class IOParameters;
}  // namespace system

namespace core {
struct AttrInfo {
  common::ColumnType type;
  int size;
  int precision;
  bool no_nulls;
  bool actually_unique;
  int64_t uncomp_size;
  int64_t comp_size;
};

struct TableOption {
  std::vector<AttributeTypeInfo> atis;
  fs::path path;
  std::string name;
  int id;
  uint8_t pss;
};

class DataPackLock : public FunctionExecutor {
 public:
  DataPackLock(TianmuAttr *attr, const int &id)
      : FunctionExecutor(std::bind(&TianmuAttr::LockPackForUse, attr, id),
                         std::bind(&TianmuAttr::UnlockPackFromUse, attr, id)) {}
};

class TableShare;

class TianmuTable final : public JustATable {
 public:
  TianmuTable() = delete;
  TianmuTable(const TianmuTable &) = delete;
  TianmuTable &operator=(const TianmuTable &) = delete;
  TianmuTable(std::string const &path, TableShare *share, Transaction *tx = nullptr);
  ~TianmuTable() = default;

  static void CreateNew(const std::shared_ptr<TableOption> &opt);
  static uint32_t GetTableId(const fs::path &dir);
  static void Alter(const std::string &path, std::vector<Field *> &new_cols, std::vector<Field *> &old_cols,
                    size_t no_objs);
  void Truncate();
  void UpdateItem(uint64_t row, uint64_t col, Value v, core::Transaction *current_transaction);
  void DeleteItem(uint64_t row, uint64_t col, core::Transaction *current_transaction);

  void LockPackInfoForUse();     // lock attribute data against memory manager
  void UnlockPackInfoFromUse();  // return attribute data to memory manager
  void LockPackForUse(unsigned attr, unsigned pack_no) override;
  void UnlockPackFromUse(unsigned attr, unsigned pack_no) override;

  int GetID() const;
  TType TableType() const override { return TType::TABLE; }
  uint NumOfAttrs() const override { return m_attrs.size(); }
  uint NumOfDisplaybleAttrs() const override { return NumOfAttrs(); }
  std::vector<AttrInfo> GetAttributesInfo();  // attributes info
  std::vector<AttributeTypeInfo> GetATIs(bool orig = false) override;
  const ColumnType &GetColumnType(int col) override;
  PhysicalColumn *GetColumn(int col_no) override { return m_attrs[col_no].get(); }
  const char *GetTableName() override {
    DEBUG_ASSERT(share != nullptr);
    return const_cast<const TableShare *>(share)->Path().c_str();
  }
  const char *GetFieldName(int attr) override;
  TianmuAttr *GetAttr(int n_a);
  const TableShare *GetTableShare() { return share; };

  // Transaction management
  bool Verify();
  void CommitVersion();
  void Rollback(common::TX_ID xid, bool = false);
  void PostCommit();

  // Data access & information
  int64_t NumOfObj() override;
  int64_t NumOfDeleted();
  int64_t NumOfValues() { return NumOfObj(); }

  void GetTable_S(types::BString &s, int64_t obj, int attr) override;
  int64_t GetTable64(int64_t obj, int attr) override;  // value from table in 1-level numerical form
  bool IsNull(int64_t obj,
              int attr) override;  // return true if the value of attr. is null
  types::TianmuValueObject GetValue(int64_t obj, int attr, Transaction *conn = nullptr);
  const fs::path &Path() { return m_path; }

  // Query execution

  // for numerical: best rough approximation of min/max for a given filter (or
  // global min if filter is nullptr)
  int64_t RoughMin(int n_a, Filter *f = nullptr);
  int64_t RoughMax(int n_a, Filter *f = nullptr);

  uint MaxStringSize(int n_a, Filter *f = nullptr) override;
  void FillRowByRowid(TABLE *table, int64_t obj);
  void DisplayRSI();
  uint32_t Getpackpower() const override;
  int64_t NoRecordsLoaded() { return no_loaded_rows; }
  int64_t NoRecordsDuped() { return no_dup_rows; }
  int Insert(TABLE *table);
  void LoadDataInfile(system::IOParameters &iop);
  void InsertMemRow(std::unique_ptr<char[]> buf, uint32_t size);
  int MergeMemTable(system::IOParameters &iop);

  std::unique_lock<std::mutex> write_lock;

 private:
  uint64_t ProceedNormal(system::IOParameters &iop);
  uint64_t ProcessDelayed(system::IOParameters &iop);
  void Field2VC(Field *f, loader::ValueCache &vc, size_t col);
  int binlog_load_query_log_event(system::IOParameters &iop);
  int binlog_insert2load_log_event(system::IOParameters &iop);
  int binlog_insert2load_block(std::vector<loader::ValueCache> &vcs, uint load_obj, system::IOParameters &iop);
  size_t max_row_length(std::vector<loader::ValueCache> &vcs, uint row, uint delimiter);

 private:
  TableShare *share;
  std::string db_name;

  Transaction *m_tx = nullptr;

  std::vector<std::unique_ptr<TianmuAttr>> m_attrs;
  std::shared_ptr<TianmuMemTable> m_mem_table;

  std::vector<common::TX_ID> m_versions;

  fs::path m_path;

  size_t no_rejected_rows = 0;
  uint64_t no_loaded_rows = 0;
  uint64_t no_dup_rows = 0;

 public:
  class Iterator final {
    friend class TianmuTable;

   public:
    Iterator() = default;

   private:
    Iterator(TianmuTable &table, std::shared_ptr<Filter> filter);
    void Initialize(const std::vector<bool> &attrs);

   public:
    bool operator==(const Iterator &iter);
    bool operator!=(const Iterator &iter) { return !(*this == iter); }
    void operator++(int);

    std::shared_ptr<types::TianmuDataType> &GetData(int col) {
      FetchValues();
      return record[col];
    }

    void MoveToRow(int64_t row_id);
    int64_t GetCurrentRowId() const { return position; }
    bool Inited() const { return table != nullptr; }

   private:
    void FetchValues();
    void UnlockPacks(int64_t new_row_id);
    void LockPacks();

   private:
    TianmuTable *table = nullptr;
    int64_t position = -1;
    Transaction *conn = nullptr;
    bool current_record_fetched = false;
    std::shared_ptr<Filter> filter;
    FilterOnesIterator it;

    std::vector<std::shared_ptr<types::TianmuDataType>> record;
    std::vector<std::function<void(size_t)>> values_fetchers;
    std::vector<std::unique_ptr<DataPackLock>> dp_locks;
    std::vector<TianmuAttr *> attrs;

   private:
    static Iterator CreateBegin(TianmuTable &table, std::shared_ptr<Filter> filter, const std::vector<bool> &attrs);
    static Iterator CreateEnd();
  };

  Iterator Begin(const std::vector<bool> &attrs, Filter &filter) {
    return Iterator::CreateBegin(*this, std::shared_ptr<Filter>(new Filter(filter)), attrs);
  }
  Iterator Begin(const std::vector<bool> &attrs) {
    return Iterator::CreateBegin(*this, std::shared_ptr<Filter>(new Filter(NumOfObj(), Getpackpower(), true)), attrs);
  }
  Iterator End() { return Iterator::CreateEnd(); }
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_TIANMU_TABLE_H_
