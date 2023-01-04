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
#ifndef TIANMU_INDEX_RDB_META_MANAGER_H_
#define TIANMU_INDEX_RDB_META_MANAGER_H_
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
#include "system/tianmu_system.h"

namespace Tianmu {
namespace core {
class DeltaTable;
}
namespace index {
class DICTManager;
class RdbKey;
class CFManager;
class DDLManager;
class RdbTable;

const std::string DEFAULT_CF_NAME("default");
const std::string DEFAULT_ROWSTORE_NAME("__rowstore__.default");
const std::string DEFAULT_DELTA_STORE_PREFIX("__rowstore__.");
const std::string DEFAULT_SYSTEM_CF_NAME("__system__");

const char QUALIFIER_VALUE_SEP = '=';
const char *const CF_NAME_QUALIFIER = "cfname";
const char QUALIFIER_SEP = ';';
// mem for pack string.
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

enum class VersionType {
  DDL_VERSION = 1,
  CF_VERSION = 1,
  MAX_INDEX_ID_VERSION = 1,
  DROP_INDEX_ONGOING_VERSION = 1,
  CREATE_INDEX_ONGOING_VERSION = 1,
};

enum class IndexInfoType {
  INDEX_INFO_VERSION_INITIAL = 1,
  INDEX_INFO_VERSION_COLS = 2,
};

enum class IndexType { INDEX_TYPE_PRIMARY = 1, INDEX_TYPE_SECONDARY, INDEX_TYPE_HIDDEN_PRIMARY };

// index of rocksdb table for tianmu.
class RdbKey {
 public:
  RdbKey(const RdbKey &k);
  RdbKey(uint index_pos, uint keyno, rocksdb::ColumnFamilyHandle *cf_handle, uint16_t index_ver, uchar index_type,
         bool is_reverse_cf, const char *name, std::vector<ColAttr> &cols);

  RdbKey &operator=(const RdbKey &) = delete;

  // Convert a key from KeyTupleFormat to mem-comparable form
  void pack_key(StringWriter &key, std::vector<std::string> &fields, StringWriter &info);
  common::ErrorCode unpack_key(StringReader &key, StringReader &value, std::vector<std::string> &fields);

  // pack and unpack field num.
  void pack_field_number(StringWriter &key, std::string &field, uchar flag);
  common::ErrorCode unpack_field_number(StringReader &key, std::string &field, uchar flag);

  // pack and unpack field value.
  void pack_field_string(StringWriter &info, StringWriter &key, std::string &field);
  common::ErrorCode unpack_field_string(StringReader &key, StringReader &value, std::string &field);

  void get_key_cols(std::vector<uint> &cols);

  int cmp_full_keys(const rocksdb::Slice &a, const rocksdb::Slice &b) const {
    ASSERT(covers_key(a), "covers_key fail!");
    ASSERT(covers_key(b), "covers_key fail!");
    return memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
  }

  // Check if given mem-comparable key belongs to this index
  bool covers_key(const rocksdb::Slice &slice) const {
    if (slice.size() < INDEX_NUMBER_SIZE)
      return false;
    if (memcmp(slice.data(), index_pos_be_, INDEX_NUMBER_SIZE))
      return false;

    return true;
  }

  // Check if prefix is matched.
  bool value_matches_prefix(const rocksdb::Slice &value, const rocksdb::Slice &prefix) const {
    return covers_key(value) && !cmp_full_keys(value, prefix);
  }

  GlobalId get_gl_index_id() const {
    const GlobalId gl_index_id = {cf_handle_->GetID(), index_pos_};
    return gl_index_id;
  }

  rocksdb::ColumnFamilyHandle *get_cf() const { return cf_handle_; }

  std::string get_boundary_key(bool lower) {
    uchar index[INDEX_NUMBER_SIZE];
    lower ? be_store_index(index, index_pos_) : be_store_index(index, index_pos_ + 1);
    return std::string(reinterpret_cast<char *>(index), INDEX_NUMBER_SIZE);
  }

  static const std::vector<std::string> parse_into_tokens(const std::string &s, const char delim);
  static const std::string parse_comment(const std::string &comment);

  // gets index pos
  uint32_t GetIndexPos() const { return index_pos_; }
  // gets index version.
  uint16_t GetIndexVersion() const { return index_ver_; }
  // gets index type.
  uchar GetIndexType() const { return index_type_; }
  // key is in reverse order.
  bool IsInReverseOrder() const { return is_reverse_order_; }
  // index key name.
  std::string GetIndexName() { return key_name_; }
  // gets Attrs of Cols.
  std::vector<ColAttr> GetColAttrs() const { return cols_; }

 private:
  friend class RdbTable;
  // ith pos in TABLE_SHARE::key_info[].
  const uint32_t index_pos_;
  // pos of index in big endian order.
  uchar index_pos_be_[INDEX_NUMBER_SIZE];
  // handler of column family.
  rocksdb::ColumnFamilyHandle *cf_handle_;
  // index version.
  uint16_t index_ver_;
  // index type.
  uchar index_type_;
  // is in reverse order.
  bool is_reverse_order_;
  // index key name.
  std::string key_name_;

  // uint m_keyno;
  // atributes of key.
  std::vector<ColAttr> cols_;
};

// rocksdb table for tianmu.
class RdbTable {
 public:
  RdbTable(const RdbTable &) = delete;
  RdbTable &operator=(const RdbTable &) = delete;

  explicit RdbTable(const std::string &name);
  explicit RdbTable(const rocksdb::Slice &slice, const size_t &pos = 0);
  virtual ~RdbTable();

  // does the cf exists in dicts.
  bool if_exist_cf(DICTManager *dict);
  // stores the dicts.
  void put_dict(DICTManager *dict, rocksdb::WriteBatch *const batch, uchar *const key, size_t keylen);

  // gets the mem vars.
  const std::string &fullname() const { return full_name_; }
  const std::string &dbname() const { return db_name_; }
  const std::string &tablename() const { return table_name_; }

  std::vector<std::shared_ptr<RdbKey>> &GetRdbTableKeys() { return rdb_keys_; }

 private:
  void set_name(const std::string &name);
  // keys of rdb table.
  std::vector<std::shared_ptr<RdbKey>> rdb_keys_;
  // full table name.
  std::string full_name_;
  // db name.
  std::string db_name_;
  // table name.
  std::string table_name_;
};

// Seq Gen. the step length use default value: 1.
class SeqGenerator {
 public:
  SeqGenerator(const SeqGenerator &) = delete;
  SeqGenerator &operator=(const SeqGenerator &) = delete;
  SeqGenerator() = default;

  // do initialization.
  void init(const uint &initial_number) { next_number_ = initial_number; }
  // get the next seq id.
  uint get_and_update_next_number(DICTManager *const dict);

 private:
  // the next seq num.
  std::atomic<uint> next_number_{0};
  // the mutex of seq, make sure it's mult-thread safe. not necessary.
  // std::recursive_mutex seq_mutex_;
};

// DDL manager.
class DDLManager {
 public:
  DDLManager(const DDLManager &) = delete;
  DDLManager &operator=(const DDLManager &) = delete;
  DDLManager() = default;

  // do initialization.
  bool init(DICTManager *const dict, CFManager *const cf_manager_);
  // do cleanup.
  void cleanup();

  // find the handler by table name.
  std::shared_ptr<RdbTable> find(const std::string &table_name);
  // find the mem handler by table name.
  std::shared_ptr<core::DeltaTable> find_delta(const std::string &table_name);
  // store a rc mem table into DDL mananger.
  void put_delta(std::shared_ptr<core::DeltaTable> delta, rocksdb::WriteBatch *const batch);

  // write dictionary into tbl.
  void put_and_write(std::shared_ptr<RdbTable> tbl, rocksdb::WriteBatch *const batch);
  // rename fromm `from` to `to`.
  bool rename(const std::string &from, const std::string &to, rocksdb::WriteBatch *const batch);

  // remove the tbl from dictionary.
  void remove(std::shared_ptr<RdbTable> tbl, rocksdb::WriteBatch *const batch);
  // remove tbl from mem hash table.
  void remove_delta(std::shared_ptr<core::DeltaTable> tb_mem, rocksdb::WriteBatch *const batch);
  bool rename_delta(std::string &from, std::string &to, rocksdb::WriteBatch *const batch);
  // get the next seq num.
  uint get_and_update_next_number(DICTManager *const dict) { return seq_gen_.get_and_update_next_number(dict); }

 private:
  // Put the data into in-memory table (only)
  void put(std::shared_ptr<RdbTable> tbl);
  // dict mananger handler.
  DICTManager *dict_ = nullptr;
  // column family manager handler.
  CFManager *cf_ = nullptr;

  // Contains RdbTable elements
  std::unordered_map<std::string, std::shared_ptr<RdbTable>> ddl_hash_;
  std::unordered_map<std::string, std::shared_ptr<core::DeltaTable>> delta_hash_;
  std::recursive_mutex lock_;
  std::recursive_mutex mem_lock_;
  SeqGenerator seq_gen_;
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

  // do initalization, gets or create a column family.
  bool init(rocksdb::DB *const rdb_dict, CFManager *const cf_manager_);
  // do cleanup.
  void cleanup(){};

  // locks operations.
  inline void lock() { dict_mutex_.lock(); }
  inline void unlock() { dict_mutex_.unlock(); }

  inline rocksdb::ColumnFamilyHandle *get_system_cf() const { return system_cf_; }

  std::unique_ptr<rocksdb::WriteBatch> begin() const;
  bool commit(rocksdb::WriteBatch *const batch, const bool &sync = true) const;

  rocksdb::Status get_value(const rocksdb::Slice &key, std::string *const value) const;

  // key operations, such as put and delete key.
  void put_key(rocksdb::WriteBatchBase *const batch, const rocksdb::Slice &key, const rocksdb::Slice &value) const;
  void delete_key(rocksdb::WriteBatchBase *batch, const rocksdb::Slice &key) const;

  // get a new interator.
  std::shared_ptr<rocksdb::Iterator> new_iterator() const;

  // index info operations.
  void save_index_info(rocksdb::WriteBatch *batch, uint16_t index_ver, uchar index_type, uint index_id, uint cf_id,
                       std::vector<ColAttr> &cols) const;
  void delete_index_info(rocksdb::WriteBatch *batch, const GlobalId &index_id) const;
  bool get_index_info(const GlobalId &gl_index_id, uint16_t &index_ver, uchar &index_type,
                      std::vector<ColAttr> &cols) const;

  // column family flags operations.
  void add_cf_flags(rocksdb::WriteBatch *const batch, const uint &cf_id, const uint &cf_flags) const;
  bool get_cf_flags(const uint &cf_id, uint32_t &cf_flags) const;

  // table operations.
  void add_drop_table(std::vector<std::shared_ptr<RdbKey>> &keys, rocksdb::WriteBatch *const batch) const;
  void add_drop_index(const std::vector<GlobalId> &gl_index_ids, rocksdb::WriteBatch *const batch) const;

  // index id operations.
  bool get_max_index_id(uint32_t *const index_id) const;
  bool update_max_index_id(rocksdb::WriteBatch *const batch, const uint32_t &index_id) const;

  // ongoing_index operations.
  void get_ongoing_index(std::vector<GlobalId> &ids, MetaType dd_type) const;
  void start_ongoing_index(rocksdb::WriteBatch *const batch, const GlobalId &index_id, MetaType dd_type) const;
  void end_ongoing_index(rocksdb::WriteBatch *const batch, const GlobalId &id, MetaType dd_type) const;
  bool is_drop_index_ongoing(const GlobalId &gl_index_id, MetaType dd_type) const;

  void finish_indexes(const std::vector<GlobalId> &ids, MetaType dd_type) const;

 private:
  void dump_index_id(StringWriter &id, MetaType dict_type, const GlobalId &gl_index_id) const;
  void delete_with_prefix(rocksdb::WriteBatch *const batch, MetaType dict_type, const GlobalId &gl_index_id) const;

  // mutex for dict.
  std::mutex dict_mutex_;
  // handler of db;
  rocksdb::DB *db_ = nullptr;
  // system column family.
  rocksdb::ColumnFamilyHandle *system_cf_ = nullptr;
  // max index of ervery INDEX_NUMBER_SIZE.
  uchar max_index_[INDEX_NUMBER_SIZE] = {0};
};

// Column Family Mananger.
class CFManager {
 public:
  CFManager(const CFManager &) = delete;
  CFManager &operator=(const CFManager &) = delete;
  CFManager() = default;

  void init(std::vector<rocksdb::ColumnFamilyHandle *> &handles);
  void cleanup();

  // Gets the column family if exists, otherwise, create a new one.
  rocksdb::ColumnFamilyHandle *get_or_create_cf(rocksdb::DB *const rdb_, const std::string &cf_name);
  // Gets the column family by cf id.
  rocksdb::ColumnFamilyHandle *get_cf_by_id(const uint32_t &id);
  // Gets all of the column family in this db.
  std::vector<rocksdb::ColumnFamilyHandle *> get_all_cf();

 private:
  // name to column family map.
  std::map<std::string, rocksdb::ColumnFamilyHandle *> cf_name_map_;
  // id to column family map.
  std::map<uint32_t, rocksdb::ColumnFamilyHandle *> cf_id_map_;
  // mutex for the maps.
  std::recursive_mutex cf_mutex_;
};

inline bool IsDeltaStoreCF(std::string cf_name) {
  if (cf_name.size() < DEFAULT_DELTA_STORE_PREFIX.size())
    return false;

  return (strncmp(cf_name.data(), DEFAULT_DELTA_STORE_PREFIX.data(), DEFAULT_DELTA_STORE_PREFIX.size()) == 0);
};

}  // namespace index
}  // namespace Tianmu

#endif  // TIANMU_INDEX_RDB_META_MANAGER_H_
