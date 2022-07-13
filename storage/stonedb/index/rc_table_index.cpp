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

#include "rc_table_index.h"

#include "rocksdb/status.h"

#include "common/common_definitions.h"
#include "core/transaction.h"
#include "handler/stonedb_handler.h"
#include "index/rdb_meta_manager.h"

namespace stonedb {
namespace index {

// using ROCKSDB_NAMESPACE::Status;

const std::string generate_cf_name(uint index, TABLE *table) {
  char *comment = table->key_info[index].comment.str;
  std::string key_comment = comment ? comment : "";
  std::string cf_name = RdbKey::parse_comment(key_comment);
  if (cf_name.empty() && !key_comment.empty()) return key_comment;

  return cf_name;
}

void create_rdbkey(TABLE *table, uint i, std::shared_ptr<RdbKey> &new_key_def, rocksdb::ColumnFamilyHandle *cf_handle) {
  uint index_id = kvstore->GetNextIndexId();
  std::vector<ColAttr> vcols;
  KEY *key_info = &table->key_info[i];
  bool unsigned_flag;
  for (uint n = 0; n < key_info->actual_key_parts; n++) {
    Field *f = key_info->key_part[n].field;
    switch (f->type()) {
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_TINY: {
        unsigned_flag = ((Field_num *)f)->unsigned_flag;

      } break;
      default:
        unsigned_flag = false;
        break;
    }
    vcols.emplace_back(ColAttr{key_info->key_part[n].field->field_index, f->type(), unsigned_flag});
  }

  const char *const key_name = table->key_info[i].name;
  uchar index_type = (i == table->s->primary_key) ? static_cast<uchar>(enumIndexType::INDEX_TYPE_PRIMARY)
                                                  : static_cast<uchar>(enumIndexType::INDEX_TYPE_SECONDARY);
  uint16_t index_ver = (key_info->actual_key_parts > 1)
                           ? static_cast<uint16_t>(enumIndexInfo::INDEX_INFO_VERSION_COLS)
                           : static_cast<uint16_t>(enumIndexInfo::INDEX_INFO_VERSION_INITIAL);
  new_key_def = std::make_shared<RdbKey>(index_id, i, cf_handle, index_ver, index_type, false, key_name, vcols);
}

// Create structures needed for storing data in rocksdb. This is called when the
// table is created.
common::ErrorCode create_keys_and_cf(TABLE *table, std::shared_ptr<RdbTable> rdb_tbl) {
  for (uint i = 0; i < rdb_tbl->m_rdbkeys.size(); i++) {
    std::string cf_name = generate_cf_name(i, table);
    if (cf_name == DEFAULT_SYSTEM_CF_NAME)
      throw common::Exception("column family not valid for storing index data. cf: " + DEFAULT_SYSTEM_CF_NAME);

    rocksdb::ColumnFamilyHandle *cf_handle = kvstore->GetCfHandle(cf_name);

    if (!cf_handle) {
      return common::ErrorCode::FAILED;
    }
    create_rdbkey(table, i, rdb_tbl->m_rdbkeys[i], cf_handle);
  }

  return common::ErrorCode::SUCCESS;
}

/* Returns index of primary key */
uint pk_index(const TABLE *const table, std::shared_ptr<RdbTable> tbl_def) {
  return table->s->primary_key == MAX_INDEXES ? tbl_def->m_rdbkeys.size() - 1 : table->s->primary_key;
}

RCTableIndex::RCTableIndex(const std::string &name, TABLE *table) {
  std::string fullname;

  if (!NormalizeName(name, fullname)) {
    STONEDB_LOG(LogCtl_Level::WARN, "normalize tablename %s fail!", name.c_str());
    return;
  }
  m_tbl = kvstore->FindTable(fullname);
  if (m_tbl == nullptr) {
    STONEDB_LOG(LogCtl_Level::WARN, "find table %s fail!", fullname.c_str());
    return;
  }
  m_keyid = table->s->primary_key;
  m_rdbkey = m_tbl->m_rdbkeys[pk_index(table, m_tbl)];
  // compatible version that primary key make up of one part
  if (table->key_info[m_keyid].actual_key_parts == 1)
    m_cols.push_back(table->key_info[m_keyid].key_part[0].field->field_index);
  else
    m_rdbkey->get_key_cols(m_cols);
  m_enable = true;
}

bool RCTableIndex::FindIndexTable(const std::string &name) {
  std::string str;
  if (!NormalizeName(name, str)) {
    throw common::Exception("Normalization wrong of table  " + name);
  }
  if (kvstore->FindTable(str)) {
    return true;
  }

  return false;
}

common::ErrorCode RCTableIndex::CreateIndexTable(const std::string &name, TABLE *table) {
  std::string str;
  if (!NormalizeName(name, str)) {
    throw common::Exception("Normalization wrong of table  " + name);
  }

  if (table->s->keys > 1) {
    STONEDB_LOG(LogCtl_Level::WARN, "Table :%s have other keys except primary key, only use primary key!", name.data());
  }

  //  Create table/key descriptions and put them into the data dictionary
  std::shared_ptr<RdbTable> tbl = std::make_shared<RdbTable>(str);
  tbl->m_rdbkeys.resize(table->s->keys);

  if (create_keys_and_cf(table, tbl) != common::ErrorCode::SUCCESS) {
    return common::ErrorCode::FAILED;
  }

  return kvstore->KVWriteTableMeta(tbl);
}

common::ErrorCode RCTableIndex::DropIndexTable(const std::string &name) {
  std::string str;
  if (!NormalizeName(name, str)) {
    throw common::Exception("Exception: table name  " + name);
  }

  //  Find the table in the hash
  return kvstore->KVDelTableMeta(str);
}

common::ErrorCode RCTableIndex::RefreshIndexTable(const std::string &name) {
  std::string fullname;

  if (!NormalizeName(name, fullname)) {
    return common::ErrorCode::FAILED;
  }
  m_tbl = kvstore->FindTable(fullname);
  if (m_tbl == nullptr) {
    STONEDB_LOG(LogCtl_Level::WARN, "table %s init ddl error", fullname.c_str());
    return common::ErrorCode::FAILED;
  }
  m_rdbkey = m_tbl->m_rdbkeys[m_keyid];
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode RCTableIndex::RenameIndexTable(const std::string &from, const std::string &to) {
  std::string sname, dname;

  if (!NormalizeName(from, sname)) {
    return common::ErrorCode::FAILED;
  }
  if (!NormalizeName(to, dname)) {
    return common::ErrorCode::FAILED;
  }

  return kvstore->KVRenameTableMeta(sname, dname);
}

void RCTableIndex::TruncateIndexTable() {
  rocksdb::WriteOptions wopts;
  rocksdb::ReadOptions ropts;
  ropts.total_order_seek = true;
  uchar key_buf[INDEX_NUMBER_SIZE];
  for (auto &rdbkey : m_tbl->m_rdbkeys) {
    be_store_index(key_buf, rdbkey->get_gl_index_id().index_id);
    auto cf = rdbkey->get_cf();
    std::unique_ptr<rocksdb::Iterator> it(kvstore->GetScanIter(ropts, cf));
    it->Seek({(const char *)key_buf, INDEX_NUMBER_SIZE});

    while (it->Valid()) {
      auto key = it->key();
      if (!rdbkey->covers_key(key)) {
        break;
      }
      if (!kvstore->KVDeleteKey(wopts, cf, key)) {
        throw common::Exception("Rdb delete key fail!");
      }
      it->Next();
    }
  }
}

common::ErrorCode RCTableIndex::CheckUniqueness(core::Transaction *tx, const rocksdb::Slice &pk_slice) {
  std::string retrieved_value;

  rocksdb::Status s = tx->KVTrans().Get(m_rdbkey->get_cf(), pk_slice, &retrieved_value);

  if (s.IsBusy() && !s.IsDeadlock()) {
    tx->KVTrans().Releasesnapshot();
    tx->KVTrans().Acquiresnapshot();
    s = tx->KVTrans().Get(m_rdbkey->get_cf(), pk_slice, &retrieved_value);
  }

  if (!s.ok() && !s.IsNotFound()) {
    // STONEDB_LOG(LogCtl_Level::ERROR, "RockDb read fail:%s", s.ToString().c_str());
    STONEDB_LOG(LogCtl_Level::ERROR, "RockDb read fail:%s", s.getState());
    return common::ErrorCode::FAILED;
  }

  if (!s.IsNotFound()) {
    return common::ErrorCode::DUPP_KEY;
  }

  return common::ErrorCode::SUCCESS;
}

common::ErrorCode RCTableIndex::InsertIndex(core::Transaction *tx, std::vector<std::string_view> &fields,
                                            uint64_t row) {
  StringWriter value, key;

  m_rdbkey->pack_key(key, fields, value);

  common::ErrorCode rc = CheckUniqueness(tx, {(const char *)key.ptr(), key.length()});
  if (rc != common::ErrorCode::SUCCESS) return rc;

  value.write_uint64(row);
  const auto cf = m_rdbkey->get_cf();
  const auto s =
      tx->KVTrans().Put(cf, {(const char *)key.ptr(), key.length()}, {(const char *)value.ptr(), value.length()});
  if (!s.ok()) {
    STONEDB_LOG(LogCtl_Level::ERROR, "RockDb: insert key fail!");
    rc = common::ErrorCode::FAILED;
  }
  return rc;
}

common::ErrorCode RCTableIndex::UpdateIndex(core::Transaction *tx, std::string_view &nkey, std::string_view &okey,
                                            uint64_t row) {
  StringWriter value, packkey;
  std::vector<std::string_view> ofields, nfields;

  ofields.emplace_back(okey);
  nfields.emplace_back(nkey);

  m_rdbkey->pack_key(packkey, ofields, value);
  common::ErrorCode rc = CheckUniqueness(tx, {(const char *)packkey.ptr(), packkey.length()});
  if (rc == common::ErrorCode::DUPP_KEY) {
    const auto cf = m_rdbkey->get_cf();
    tx->KVTrans().Delete(cf, {(const char *)packkey.ptr(), packkey.length()});
  } else {
    STONEDB_LOG(LogCtl_Level::WARN, "RockDb: don't have the key for update!");
  }
  rc = InsertIndex(tx, nfields, row);
  return rc;
}

common::ErrorCode RCTableIndex::GetRowByKey(core::Transaction *tx, std::vector<std::string_view> &fields,
                                            uint64_t &row) {
  std::string value;
  StringWriter packkey, info;
  m_rdbkey->pack_key(packkey, fields, info);
  rocksdb::Status s = tx->KVTrans().Get(m_rdbkey->get_cf(), {(const char *)packkey.ptr(), packkey.length()}, &value);

  if (!s.IsNotFound() && !s.ok()) {
    return common::ErrorCode::FAILED;
  }

  if (s.IsNotFound()) return common::ErrorCode::NOT_FOUND_KEY;

  StringReader reader({value.data(), value.length()});
  // ver compatible
  if (m_rdbkey->m_index_ver > static_cast<uint16_t>(enumIndexInfo::INDEX_INFO_VERSION_INITIAL)) {
    uint16_t packlen;
    reader.read_uint16(&packlen);
    reader.read(packlen);
  }
  reader.read_uint64(&row);
  return common::ErrorCode::SUCCESS;
}

void KeyIterator::ScanToKey(std::shared_ptr<RCTableIndex> tab, std::vector<std::string_view> &fields,
                            common::Operator op) {
  if (!tab || !trans) {
    return;
  }
  StringWriter packkey, info;
  valid = true;
  rdbkey = tab->m_rdbkey;
  rdbkey->pack_key(packkey, fields, info);
  rocksdb::Slice key_slice((const char *)packkey.ptr(), packkey.length());

  iter = std::shared_ptr<rocksdb::Iterator>(trans->GetIterator(rdbkey->get_cf(), true));
  switch (op) {
    case common::Operator::O_EQ:  //==
      iter->Seek(key_slice);
      if (!iter->Valid() || !rdbkey->value_matches_prefix(iter->key(), key_slice)) valid = false;
      break;
    case common::Operator::O_MORE_EQ:  //'>='
      iter->Seek(key_slice);
      if (!iter->Valid() || !rdbkey->covers_key(iter->key())) valid = false;
      break;
    case common::Operator::O_MORE:  //'>'
      iter->Seek(key_slice);
      if (!iter->Valid() || rdbkey->value_matches_prefix(iter->key(), key_slice)) {
        if (rdbkey->m_is_reverse) {
          iter->Prev();
        } else {
          iter->Next();
        }
      }
      if (!iter->Valid() || !rdbkey->covers_key(iter->key())) valid = false;
      break;
    default:
      STONEDB_LOG(LogCtl_Level::ERROR, "key not support this op:%d", op);
      valid = false;
      break;
  }
}

void KeyIterator::ScanToEdge(std::shared_ptr<RCTableIndex> tab, bool forward) {
  if (!tab || !trans) {
    return;
  }
  valid = true;
  rdbkey = tab->m_rdbkey;
  iter = std::shared_ptr<rocksdb::Iterator>(trans->GetIterator(rdbkey->get_cf(), true));
  std::string key = rdbkey->get_boundary_key(forward);

  rocksdb::Slice key_slice((const char *)key.data(), key.length());

  iter->Seek(key_slice);
  if (forward) {
    if (!iter->Valid() || !rdbkey->value_matches_prefix(iter->key(), key_slice)) valid = false;
  } else {
    if (!iter)
      valid = false;
    else {
      iter->Prev();
      if (!iter->Valid() || !rdbkey->covers_key(iter->key())) valid = false;
    }
  }
}

KeyIterator &KeyIterator::operator++() {
  if (!iter || !iter->Valid() || !rdbkey) {
    valid = false;
    return *this;
  }

  iter->Next();
  if (!iter->Valid() || !rdbkey->covers_key(iter->key())) {
    valid = false;
    return *this;
  }

  return *this;
}

KeyIterator &KeyIterator::operator--() {
  if (!iter || !iter->Valid() || !rdbkey) {
    valid = false;
    return *this;
  }

  iter->Prev();
  if (!iter->Valid() || !rdbkey->covers_key(iter->key())) {
    valid = false;
    return *this;
  }

  return *this;
}

common::ErrorCode KeyIterator::GetCurKV(std::vector<std::string> &keys, uint64_t &row) {
  StringReader key({iter->key().data(), iter->key().size()});
  StringReader value({iter->value().data(), iter->value().size()});

  common::ErrorCode ret = rdbkey->unpack_key(key, value, keys);
  value.read_uint64(&row);
  return ret;
}

}  // namespace index
}  // namespace stonedb
