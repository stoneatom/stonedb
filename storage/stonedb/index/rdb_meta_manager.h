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
#ifndef STONEDB_INDEX_RDB_META_MANAGER_H_
#define STONEDB_INDEX_RDB_META_MANAGER_H_
#pragma once

#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "index/rdb_utils.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/utilities/transaction_db.h"
#include "system/rc_system.h"

namespace stonedb {
namespace core {
class RCMemTable;
}
namespace index {
class DICTManager;
class RdbKey;
class CFManager;
class DDLManager;
class RdbTable;

const std::string DEFAULT_CF_NAME("default");
const std::string DEFAULT_ROWSTORE_NAME("__rowstore__.default");
const std::string DEFAULT_ROWSTORE_PREFIX("__rowstore__.");
const std::string DEFAULT_SYSTEM_CF_NAME("__system__");
const char QUALIFIER_VALUE_SEP = '=';
const char *const CF_NAME_QUALIFIER = "cfname";
const char QUALIFIER_SEP = ';';
const std::string SPACE(CHUNKSIZE, 0x20);
constexpr int REVERSE_CF_FLAG = 0x1;
constexpr int INDEX_NUMBER_SIZE = 0x4;
constexpr int VERSION_SIZE = 0x2;

enum class MetaType {
  DDL_INDEX = 1,
  INDEX_INFO,
  CF_INFO,
  MAX_INDEX_ID,
  DDL_DROP_INDEX_ONGOING,
  DDL_MEMTABLE,
  END_DICT_INDEX_ID = 255
};

enum class enumVersion {
  DDL_VERSION = 1,
  CF_VERSION = 1,
  MAX_INDEX_ID_VERSION = 1,
  DROP_INDEX_ONGOING_VERSION = 1,
  CREATE_INDEX_ONGOING_VERSION = 1,
};

enum class enumIndexInfo {
  INDEX_INFO_VERSION_INITIAL = 1,
  INDEX_INFO_VERSION_COLS = 2,
};

enum class enumIndexType { INDEX_TYPE_PRIMARY = 1, INDEX_TYPE_SECONDARY, INDEX_TYPE_HIDDEN_PRIMARY };

class RdbKey {
 public:
  RdbKey(const RdbKey &k);
  RdbKey(uint indexnr, uint keyno, rocksdb::ColumnFamilyHandle *cf_handle, uint16_t index_ver, uchar index_type,
         bool is_reverse_cf, const char *name, std::vector<ColAttr> &cols);

  RdbKey &operator=(const RdbKey &) = delete;

  // Convert a key from KeyTupleFormat to mem-comparable form
  void pack_key(StringWriter &key, std::vector<std::string_view> &fields, StringWriter &info);
  common::ErrorCode unpack_key(StringReader &key, StringReader &value, std::vector<std::string> &fields);
  void pack_field_number(StringWriter &key, std::string_view &field, uchar flag);
  int unpack_field_number(StringReader &key, std::string &field, uchar flag);
  void pack_field_string(StringWriter &info, StringWriter &key, std::string_view &field);
  int unpack_field_string(StringReader &key, StringReader &value, std::string &field);
  void get_key_cols(std::vector<uint> &cols);
  int cmp_full_keys(const rocksdb::Slice &a, const rocksdb::Slice &b) const {
    ASSERT(covers_key(a), "covers_key fail!");
    ASSERT(covers_key(b), "covers_key fail!");
    return memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
  }
  // Check if given mem-comparable key belongs to this index
  bool covers_key(const rocksdb::Slice &slice) const {
    if (slice.size() < INDEX_NUMBER_SIZE) return false;
    if (memcmp(slice.data(), m_index_nr_be, INDEX_NUMBER_SIZE)) return false;

    return true;
  }
  bool value_matches_prefix(const rocksdb::Slice &value, const rocksdb::Slice &prefix) const {
    return covers_key(value) && !cmp_full_keys(value, prefix);
  }
  GlobalId get_gl_index_id() const {
    const GlobalId gl_index_id = {m_cf_handle->GetID(), m_indexnr};
    return gl_index_id;
  }
  rocksdb::ColumnFamilyHandle *get_cf() const { return m_cf_handle; }
  std::string get_boundary_key(bool lower) {
    uchar index[INDEX_NUMBER_SIZE];
    lower ? be_store_index(index, m_indexnr) : be_store_index(index, m_indexnr + 1);
    return std::string(reinterpret_cast<char *>(index), INDEX_NUMBER_SIZE);
  }
  static const std::vector<std::string> parse_into_tokens(const std::string &s, const char delim);
  static const std::string parse_comment(const std::string &comment);

 private:
  friend class RdbTable;
  const uint32_t m_indexnr;
  uchar m_index_nr_be[INDEX_NUMBER_SIZE];
  rocksdb::ColumnFamilyHandle *m_cf_handle;

 public:
  uint16_t m_index_ver;
  uchar m_index_type;
  bool m_is_reverse;
  std::string m_name;

 private:
  uint m_keyno;
  std::vector<ColAttr> m_cols;
};

class RdbTable {
 public:
  RdbTable(const RdbTable &) = delete;
  RdbTable &operator=(const RdbTable &) = delete;
  explicit RdbTable(const std::string &name);
  explicit RdbTable(const rocksdb::Slice &slice, const size_t &pos = 0);
  ~RdbTable();
  bool if_exist_cf(DICTManager *dict);
  void put_dict(DICTManager *dict, rocksdb::WriteBatch *const batch, uchar *const key, size_t keylen);

  const std::string &fullname() const { return m_fullname; }
  const std::string &dbname() const { return m_dbname; }
  const std::string &tablename() const { return m_tablename; }
  std::vector<std::shared_ptr<RdbKey>> m_rdbkeys;

 private:
  void set_name(const std::string &name);

 private:
  std::string m_fullname;
  std::string m_dbname;
  std::string m_tablename;
};

class SeqGenerator {
 public:
  SeqGenerator(const SeqGenerator &) = delete;
  SeqGenerator &operator=(const SeqGenerator &) = delete;
  SeqGenerator() = default;
  void init(const uint &initial_number) { m_next_number = initial_number; }
  uint get_and_update_next_number(DICTManager *const dict);

 private:
  uint m_next_number = 0;
  std::recursive_mutex m_mutex;
};

class DDLManager {
 public:
  DDLManager(const DDLManager &) = delete;
  DDLManager &operator=(const DDLManager &) = delete;
  DDLManager() = default;

  bool init(DICTManager *const dict, CFManager *const cf_manager);
  void cleanup();
  std::shared_ptr<RdbTable> find(const std::string &table_name);
  void put_and_write(std::shared_ptr<RdbTable> key_descr, rocksdb::WriteBatch *const batch);
  void remove(std::shared_ptr<RdbTable> tbl, rocksdb::WriteBatch *const batch);
  bool rename(const std::string &from, const std::string &to, rocksdb::WriteBatch *const batch);
  std::shared_ptr<core::RCMemTable> find_mem(const std::string &table_name);
  void put_mem(std::shared_ptr<core::RCMemTable> tb_mem, rocksdb::WriteBatch *const batch);
  void remove_mem(std::shared_ptr<core::RCMemTable> tb_mem, rocksdb::WriteBatch *const batch);
  bool rename_mem(std::string &from, std::string &to, rocksdb::WriteBatch *const batch);

  uint get_and_update_next_number(DICTManager *const dict) { return m_sequence.get_and_update_next_number(dict); }

 private:
  // Put the data into in-memory table (only)
  void put(std::shared_ptr<RdbTable> tbl);

 private:
  DICTManager *m_dict = nullptr;
  CFManager *m_cf = nullptr;
  // Contains RdbTable elements
  std::unordered_map<std::string, std::shared_ptr<RdbTable>> m_ddl_hash;
  std::unordered_map<std::string, std::shared_ptr<core::RCMemTable>> m_mem_hash;
  std::recursive_mutex m_lock;
  std::recursive_mutex m_mem_lock;
  SeqGenerator m_sequence;
};

/*
  Meta description
  --------------------------------------------------------------
  Type:  MetaType::DDL_INDEX
  key:    0x01(4B)+dbname.tablename
  value : version, {cf_id, index_id}*n_indexes_of_the_table
  --------------------------------------------------------------
  Type: MetaType::INDEX_INFO
  key: 0x2(4B) + cf_id(4B)+ index_id(4B)
  value: version(2B), index_type(1B), coltype(1B), unsignedflag(1B)
  --------------------------------------------------------------
  Type:  MetaType::MAX_INDEX_ID
  key:    0x7(4B)
  value : index_id(4B)
 ---------------------------------------------------------------
  Type:  MetaType::CF_INFO
  key:    0x3(4B)+ cf_id
  value : version, {is_reverse_cf, is_auto_cf (deprecated), is_per_partition_cf}
  ---------------------------------------------------------------
  version(2B)   {cf_id, index_id}(4B)  {is_reverse_cf, is_auto_cf (deprecated),
 is_per_partition_cf} (4B)
*/
class DICTManager {
 public:
  DICTManager(const DICTManager &) = delete;
  DICTManager &operator=(const DICTManager &) = delete;
  DICTManager() = default;

  bool init(rocksdb::DB *const rdb_dict, CFManager *const cf_manager);
  inline void lock() { m_mutex.lock(); }
  inline void unlock() { m_mutex.unlock(); }
  void cleanup(){};
  inline rocksdb::ColumnFamilyHandle *get_system_cf() const { return m_system_cfh; }
  std::unique_ptr<rocksdb::WriteBatch> begin() const;
  bool commit(rocksdb::WriteBatch *const batch, const bool &sync = true) const;
  rocksdb::Status get_value(const rocksdb::Slice &key, std::string *const value) const;
  void put_key(rocksdb::WriteBatchBase *const batch, const rocksdb::Slice &key, const rocksdb::Slice &value) const;
  void delete_key(rocksdb::WriteBatchBase *batch, const rocksdb::Slice &key) const;
  std::shared_ptr<rocksdb::Iterator> new_iterator() const;
  void save_index_info(rocksdb::WriteBatch *batch, uint16_t index_ver, uchar index_type, uint index_id, uint cf_id,
                       std::vector<ColAttr> &cols) const;
  void delete_index_info(rocksdb::WriteBatch *batch, const GlobalId &index_id) const;
  bool get_index_info(const GlobalId &gl_index_id, uint16_t &index_ver, uchar &index_type,
                      std::vector<ColAttr> &cols) const;
  void add_cf_flags(rocksdb::WriteBatch *const batch, const uint &cf_id, const uint &cf_flags) const;
  bool get_cf_flags(const uint &cf_id, uint32_t &cf_flags) const;

  void add_drop_table(std::vector<std::shared_ptr<RdbKey>> &keys, rocksdb::WriteBatch *const batch) const;
  void add_drop_index(const std::vector<GlobalId> &gl_index_ids, rocksdb::WriteBatch *const batch) const;

  bool get_max_index_id(uint32_t *const index_id) const;
  bool update_max_index_id(rocksdb::WriteBatch *const batch, const uint32_t &index_id) const;
  void get_ongoing_index(std::vector<GlobalId> &ids, MetaType dd_type) const;
  void start_ongoing_index(rocksdb::WriteBatch *const batch, const GlobalId &index_id, MetaType dd_type) const;
  void end_ongoing_index(rocksdb::WriteBatch *const batch, const GlobalId &id, MetaType dd_type) const;
  void finish_indexes(const std::vector<GlobalId> &ids, MetaType dd_type) const;
  bool is_drop_index_ongoing(const GlobalId &gl_index_id, MetaType dd_type) const;

 private:
  void dump_index_id(StringWriter &id, MetaType dict_type, const GlobalId &gl_index_id) const;
  void delete_with_prefix(rocksdb::WriteBatch *const batch, MetaType dict_type, const GlobalId &gl_index_id) const;

 private:
  std::mutex m_mutex;
  rocksdb::DB *m_db = nullptr;
  rocksdb::ColumnFamilyHandle *m_system_cfh = nullptr;
  uchar m_max_index[INDEX_NUMBER_SIZE] = {0};
};

class CFManager {
 public:
  CFManager(const CFManager &) = delete;
  CFManager &operator=(const CFManager &) = delete;
  CFManager() = default;

  void init(std::vector<rocksdb::ColumnFamilyHandle *> &handles);
  void cleanup();
  rocksdb::ColumnFamilyHandle *get_or_create_cf(rocksdb::DB *const rdb, const std::string &cf_name);
  rocksdb::ColumnFamilyHandle *get_cf_by_id(const uint32_t &id);
  std::vector<rocksdb::ColumnFamilyHandle *> get_all_cf();

 private:
  std::map<std::string, rocksdb::ColumnFamilyHandle *> m_cf_name_map;
  std::map<uint32_t, rocksdb::ColumnFamilyHandle *> m_cf_id_map;
  std::recursive_mutex m_mutex;
};

inline bool IsRowStoreCF(std::string cf_name) {
  if (cf_name.size() < DEFAULT_ROWSTORE_PREFIX.size()) return false;

  return (strncmp(cf_name.data(), DEFAULT_ROWSTORE_PREFIX.data(), DEFAULT_ROWSTORE_PREFIX.size()) == 0);
};

}  // namespace index
}  // namespace stonedb

#endif  // STONEDB_INDEX_RDB_META_MANAGER_H_
