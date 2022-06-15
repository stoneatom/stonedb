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

#include "rdb_meta_manager.h"

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <set>
#include <utility>
#include <vector>

#include "key.h"
#include "m_ctype.h"
#include "my_bit.h"

#include "core/engine.h"
#include "core/rc_mem_table.h"
#include "index/kv_store.h"
#include "index/rdb_utils.h"

namespace stonedb {
namespace index {

RdbKey::RdbKey(uint indexnr, uint keyno, rocksdb::ColumnFamilyHandle *cf_handle, uint16_t index_ver, uchar index_type,
               bool is_reverse_cf, const char *_name, std::vector<ColAttr> &cols)
    : m_indexnr(indexnr),
      m_cf_handle(cf_handle),
      m_index_ver(index_ver),
      m_index_type(index_type),
      m_is_reverse(is_reverse_cf),
      m_name(_name),
      m_keyno(keyno),
      m_cols(cols) {
  be_store_index(m_index_nr_be, m_indexnr);

  ASSERT(m_cf_handle != nullptr, "m_cf_handle is NULL");
}

RdbKey::RdbKey(const RdbKey &k)
    : m_indexnr(k.m_indexnr),
      m_cf_handle(k.m_cf_handle),
      m_index_ver(k.m_index_ver),
      m_is_reverse(k.m_is_reverse),
      m_name(k.m_name),
      m_keyno(k.m_keyno),
      m_cols(k.m_cols) {
  be_store_index(m_index_nr_be, m_indexnr);
}

const std::vector<std::string> RdbKey::parse_into_tokens(const std::string &s, const char delim) {
  std::vector<std::string> tokens;
  std::string t;
  std::stringstream ss(s);

  while (getline(ss, t, delim)) {
    tokens.push_back(t);
  }
  return tokens;
}

const std::string RdbKey::parse_comment(const std::string &comment) {
  std::string empty_result;
  if (comment.empty()) {
    return empty_result;
  }

  std::vector<std::string> v = parse_into_tokens(comment, QUALIFIER_SEP);
  std::string search = CF_NAME_QUALIFIER;
  for (const auto &it : v) {
    if (it.substr(0, search.length()) == search) {
      std::vector<std::string> tokens = parse_into_tokens(it, QUALIFIER_VALUE_SEP);
      if (tokens.size() == 2) {
        return tokens[1];
      } else {
        return empty_result;
      }
    }
  }

  return empty_result;
}
void RdbKey::get_key_cols(std::vector<uint> &cols) {
  for (auto &col : m_cols) {
    cols.push_back(col.col_no);
  }
}

void RdbKey::pack_field_number(StringWriter &key, std::string_view &field, uchar flag) {
  uchar tuple[INTSIZE] = {0};
  copy_integer<false>(tuple, INTSIZE, (const uchar *)field.data(), INTSIZE, flag);
  key.write(tuple, sizeof(tuple));
}

int RdbKey::unpack_field_number(StringReader &key, std::string &field, uchar flag) {
  const uchar *from;
  if (!(from = (const uchar *)key.read(INTSIZE))) return -1;

  int64_t value = 0;
  char *buf = reinterpret_cast<char *>(&value);
  char sign_byte = from[0];
  if (flag)
    buf[INTSIZE - 1] = sign_byte;
  else
    buf[INTSIZE - 1] = sign_byte ^ 128;  // Reverse the sign bit.
  for (uint i = 0, j = INTSIZE - 1; i < INTSIZE - 1; ++i, --j) buf[i] = from[j];

  field.append(reinterpret_cast<char *>(&value), INTSIZE);
  return 0;
}

void RdbKey::pack_field_string(StringWriter &info, StringWriter &key, std::string_view &field) {
  // version compatible
  if (m_index_ver == static_cast<uint16_t>(enumIndexInfo::INDEX_INFO_VERSION_INITIAL)) {
    key.write((const uchar *)field.data(), field.length());
    return;
  }
  StringReader data(field);
  size_t pad_bytes = 0;
  while (true) {
    size_t copy_len = std::min<size_t>(CHUNKSIZE - 1, data.remain_len());
    pad_bytes = CHUNKSIZE - 1 - copy_len;
    key.write((const uchar *)data.read(copy_len), copy_len);
    Separator separator;
    if (pad_bytes) {
      key.write((const uchar *)SPACE.data(), pad_bytes);
      separator = Separator::EQ_SPACES;
    } else {
      int cmp = 0;
      size_t bytes = std::min(CHUNKSIZE - 1, data.remain_len());
      if (bytes > 0) cmp = memcmp(data.current_ptr(), SPACE.data(), bytes);
      if (cmp < 0) {
        separator = Separator::LE_SPACES;
      } else if (cmp > 0) {
        separator = Separator::GE_SPACES;
      } else {
        // It turns out all the rest are spaces.
        separator = Separator::EQ_SPACES;
      }
    }
    key.write_uint8(static_cast<uint>(separator));  // last segment

    if (separator == Separator::EQ_SPACES) break;
  }
  // pack info only save pad bytes len of last chunk
  info.write_uint16(pad_bytes);
}

int RdbKey::unpack_field_string(StringReader &key, StringReader &info, std::string &field) {
  bool finished = false;
  const char *ptr;
  uint16_t pad_bytes = 0;
  // version compatible
  if (m_index_ver == static_cast<uint16_t>(enumIndexInfo::INDEX_INFO_VERSION_INITIAL)) {
    field.append(key.current_ptr(), key.remain_len());
    return 0;
  }
  // Decode the length of padding bytes of field for value
  info.read_uint16(&pad_bytes);
  // Decode the key
  while ((ptr = (const char *)key.read(CHUNKSIZE))) {
    size_t used_bytes;
    const char last_byte = ptr[CHUNKSIZE - 1];

    if (last_byte == static_cast<char>(Separator::EQ_SPACES)) {
      // this is the last segment
      if (pad_bytes > (CHUNKSIZE - 1)) return -1;
      used_bytes = (CHUNKSIZE - 1) - pad_bytes;
      finished = true;
    } else {
      if (last_byte != static_cast<char>(Separator::LE_SPACES) && last_byte != static_cast<char>(Separator::GE_SPACES))
        return -1;
      used_bytes = CHUNKSIZE - 1;
    }

    // Now, need to decode used_bytes of data and append them to the value.

    field.append(ptr, used_bytes);
    if (finished) {
      break;
    }
  }

  return 0;
}

// cmp packed
void RdbKey::pack_key(StringWriter &key, std::vector<std::string_view> &fields, StringWriter &info) {
  ASSERT(m_cols.size() >= fields.size(), "fields size larger than keyparts size");
  key.clear();
  info.clear();
  key.write_uint32(m_indexnr);
  // version compatible
  if (m_index_ver > static_cast<uint16_t>(enumIndexInfo::INDEX_INFO_VERSION_INITIAL)) info.write_uint16(0);
  size_t pos = info.length();

  for (uint i = 0; i < fields.size(); i++) {
    switch (m_cols[i].col_type) {
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_TINY:

      case MYSQL_TYPE_DOUBLE:
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_NEWDECIMAL:
      case MYSQL_TYPE_TIMESTAMP:
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_NEWDATE:
      case MYSQL_TYPE_DATETIME2:
      case MYSQL_TYPE_TIMESTAMP2:
      case MYSQL_TYPE_TIME2:
      case MYSQL_TYPE_YEAR: {
        pack_field_number(key, fields[i], m_cols[i].col_flag);
        break;
      }
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_STRING: {
        pack_field_string(info, key, fields[i]);
        break;
      }
      default:
        break;
    }
  }

  // version compatible
  if (m_index_ver > static_cast<uint16_t>(enumIndexInfo::INDEX_INFO_VERSION_INITIAL)) {
    // process packinfo len
    size_t len = info.length() - pos;
    info.write_uint16_at(0, len);
  }
}

common::ErrorCode RdbKey::unpack_key(StringReader &key, StringReader &value, std::vector<std::string> &fields) {
  uint32_t index_number = 0;
  uint16_t info_len = 0;

  key.read_uint32(&index_number);
  // version compatible
  if (m_index_ver > static_cast<uint16_t>(enumIndexInfo::INDEX_INFO_VERSION_INITIAL)) value.read_uint16(&info_len);

  for (auto &col : m_cols) {
    std::string field;
    switch (col.col_type) {
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_TIMESTAMP:
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_NEWDATE:
      case MYSQL_TYPE_DATETIME2:
      case MYSQL_TYPE_TIMESTAMP2:
      case MYSQL_TYPE_TIME2:
      case MYSQL_TYPE_YEAR:

      case MYSQL_TYPE_DOUBLE:
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_NEWDECIMAL: {
        if (unpack_field_number(key, field, col.col_flag) < 0) {
          STONEDB_LOG(LogCtl_Level::ERROR, "unpack numeric field failed!");
          return common::ErrorCode::FAILED;
        }
      } break;

      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_STRING: {
        // case sensitive for character
        if (unpack_field_string(key, value, field) < 0) {
          STONEDB_LOG(LogCtl_Level::ERROR, "unpack string field failed!");
          return common::ErrorCode::FAILED;
        }
      } break;
      default:
        break;
    }
    fields.emplace_back(field);
  }
  return common::ErrorCode::SUCCESS;
}

RdbTable::RdbTable(const std::string &name) { set_name(name); }

RdbTable::RdbTable(const rocksdb::Slice &slice, const size_t &pos) {
  set_name(std::string(slice.data() + pos, slice.size() - pos));
}

RdbTable::~RdbTable() { m_rdbkeys.clear(); }

// Put table definition DDL entry. Actual write is done at DICTManager::commit
void RdbTable::put_dict(DICTManager *dict, rocksdb::WriteBatch *const batch, uchar *const key, size_t keylen) {
  StringWriter value;
  value.write_uint16(static_cast<uint>(enumVersion::DDL_VERSION));

  for (auto &kd : m_rdbkeys) {
    uchar flags = (kd->m_is_reverse ? REVERSE_CF_FLAG : 0);
    const uint cf_id = kd->get_cf()->GetID();
    if (!if_exist_cf(dict)) {
      dict->add_cf_flags(batch, cf_id, flags);
    }
    value.write_uint32(cf_id);
    value.write_uint32(kd->m_indexnr);
    dict->save_index_info(batch, kd->m_index_ver, kd->m_index_type, kd->m_indexnr, cf_id, kd->m_cols);
  }
  dict->put_key(batch, {(char *)key, keylen}, {(char *)value.ptr(), value.length()});
}

bool RdbTable::if_exist_cf(DICTManager *dict) {
  for (auto &kd : m_rdbkeys) {
    uint32_t flags;
    const uint cf_id = kd->get_cf()->GetID();
    if (dict->get_cf_flags(cf_id, flags)) {
      return true;
    }
  }
  return false;
}

void RdbTable::set_name(const std::string &name) {
  // Normalize  dbname.tablename.
  m_fullname = name;
  size_t dotpos = name.find('.');
  if (dotpos == std::string::npos) {
    STONEDB_LOG(LogCtl_Level::ERROR, "table name :%s format wrong", name.data());
    return;
  }
  m_dbname = name.substr(0, dotpos);
  m_tablename = name.substr(++dotpos);
}

bool DDLManager::init(DICTManager *const dict, CFManager *const cf_manager) {
  m_dict = dict;
  m_cf = cf_manager;
  uchar ddl_entry[INDEX_NUMBER_SIZE];
  be_store_index(ddl_entry, static_cast<uint32_t>(MetaType::DDL_INDEX));

  std::shared_ptr<rocksdb::Iterator> it = m_dict->new_iterator();
  uint max_index_id = 0;
  m_dict->get_max_index_id(&max_index_id);

  for (it->Seek({(char *)ddl_entry, INDEX_NUMBER_SIZE}); it->Valid(); it->Next()) {
    const uchar *ptr, *ptr_end;
    const rocksdb::Slice key = it->key();
    const rocksdb::Slice val = it->value();

    if (key.size() >= INDEX_NUMBER_SIZE && memcmp(key.data(), ddl_entry, INDEX_NUMBER_SIZE)) break;

    if (key.size() <= INDEX_NUMBER_SIZE) {
      STONEDB_LOG(LogCtl_Level::ERROR, "RocksDB: Table_store: key has length %d (corruption)", (int)key.size());
      return false;
    }
    std::shared_ptr<RdbTable> tdef = std::make_shared<RdbTable>(key, INDEX_NUMBER_SIZE);

    // read the DDLs.
    const int real_val_size = val.size() - VERSION_SIZE;
    if (real_val_size % sizeof(GlobalId) > 0) {
      STONEDB_LOG(LogCtl_Level::ERROR, "RocksDB: Table_store: invalid keylist for table %s", tdef->fullname().c_str());
      return false;
    }

    tdef->m_rdbkeys.resize(real_val_size / sizeof(GlobalId));

    ptr = reinterpret_cast<const uchar *>(val.data());
    const int version = be_read_uint16(&ptr);
    if (version != static_cast<uint>(enumVersion::DDL_VERSION)) {
      STONEDB_LOG(LogCtl_Level::ERROR,
                  "RocksDB: DDL ENTRY Version was not expected.Expected: %d, "
                  "Actual: %d",
                  static_cast<uint>(enumVersion::DDL_VERSION), version);
      return false;
    }
    ptr_end = ptr + real_val_size;
    for (uint keyno = 0; ptr < ptr_end; keyno++) {
      GlobalId gl_index_id;
      be_read_gl_index(&ptr, &gl_index_id);
      uint16_t index_ver = 0;
      uint32_t flags = 0;
      uchar index_type = 0;
      std::vector<ColAttr> vcols;
      if (!m_dict->get_index_info(gl_index_id, index_ver, index_type, vcols)) {
        STONEDB_LOG(LogCtl_Level::ERROR,
                    "RocksDB: Could not get INDEXINFO for Index Number "
                    "(%u,%u), table %s",
                    gl_index_id.cf_id, gl_index_id.index_id, tdef->fullname().c_str());
        return false;
      }
      if (max_index_id < gl_index_id.index_id) {
        STONEDB_LOG(LogCtl_Level::ERROR,
                    "RocksDB: Found MetaType::MAX_INDEX_ID %u, but also found larger "
                    "index %u from INDEXINFO.",
                    max_index_id, gl_index_id.index_id);
        return false;
      }
      // In some abnormal condition, gl_index_id.cf_id(CF Number) is greater
      // than 0 like 3 or 4 just log it and set the CF to 0.
      if (gl_index_id.cf_id != 0) {
        STONEDB_LOG(LogCtl_Level::ERROR,
                    "StoneDB-RocksDB: Could not get Column Family Flags for CF "
                    "Number %d, table %s",
                    gl_index_id.cf_id, tdef->fullname().c_str());
        gl_index_id.cf_id = 0;
      }
      if (!m_dict->get_cf_flags(gl_index_id.cf_id, flags)) {
        STONEDB_LOG(LogCtl_Level::ERROR,
                    "RocksDB: Could not get Column Family Flags for CF Number "
                    "%d, table %s",
                    gl_index_id.cf_id, tdef->fullname().c_str());
        return false;
      }
      rocksdb::ColumnFamilyHandle *const cfh = cf_manager->get_cf_by_id(gl_index_id.cf_id);

      tdef->m_rdbkeys[keyno] = std::make_shared<RdbKey>(gl_index_id.index_id, keyno, cfh, index_ver, index_type,
                                                        flags & REVERSE_CF_FLAG, "", vcols);
    }
    put(tdef);
  }

  // create memory table
  std::scoped_lock guard(m_mem_lock);
  uchar mem_table_entry[INDEX_NUMBER_SIZE];
  be_store_index(mem_table_entry, static_cast<uint32_t>(MetaType::DDL_MEMTABLE));
  const rocksdb::Slice mem_table_entry_slice((char *)mem_table_entry, INDEX_NUMBER_SIZE);
  for (it->Seek(mem_table_entry_slice); it->Valid(); it->Next()) {
    const rocksdb::Slice key = it->key();
    const rocksdb::Slice val = it->value();

    if (key.size() >= INDEX_NUMBER_SIZE && !key.starts_with(mem_table_entry_slice)) break;

    if (key.size() < INDEX_NUMBER_SIZE) {
      break;
    }

    const uchar *ptr = reinterpret_cast<const uchar *>(val.data());
    const int version = be_read_uint16(&ptr);
    if (version != static_cast<uint>(enumVersion::DDL_VERSION)) {
      STONEDB_LOG(LogCtl_Level::ERROR,
                  "RocksDB: DDL MEMTABLE ENTRY Version was not expected or "
                  "currupt.Expected: %d, Actual: %d",
                  static_cast<uint>(enumVersion::DDL_VERSION), version);
      return false;
    }

    const uint32_t cf_id = be_read_uint32(&ptr);
    const uint32_t memtable_id = be_read_uint32(&ptr);
    std::string table_name = std::string(key.data() + INDEX_NUMBER_SIZE, key.size() - INDEX_NUMBER_SIZE);
    if (max_index_id < memtable_id) {
      STONEDB_LOG(LogCtl_Level::ERROR, "RocksDB: Found MAX_MEM_ID %u, but also found larger memtable id %u.",
                  max_index_id, memtable_id);
      return false;
    }
    std::shared_ptr<core::RCMemTable> tb_mem = std::make_shared<core::RCMemTable>(table_name, memtable_id, cf_id);
    m_mem_hash[table_name] = tb_mem;
  }

  if (max_index_id < static_cast<uint32_t>(MetaType::END_DICT_INDEX_ID)) {
    max_index_id = static_cast<uint32_t>(MetaType::END_DICT_INDEX_ID);
  }
  m_sequence.init(max_index_id + 1);

  return true;
}

std::shared_ptr<RdbTable> DDLManager::find(const std::string &table_name) {
  std::shared_ptr<RdbTable> rec;
  std::scoped_lock guard(m_lock);

  auto iter = m_ddl_hash.find(table_name);
  if (iter != m_ddl_hash.end()) rec = iter->second;

  return rec;
}

// Put table definition of `tbl` into the mapping, and also write it to the
// on-disk data dictionary.
void DDLManager::put_and_write(std::shared_ptr<RdbTable> tbl, rocksdb::WriteBatch *const batch) {
  StringWriter key;
  key.write_uint32(static_cast<uint32_t>(MetaType::DDL_INDEX));

  const std::string &dbname_tablename = tbl->fullname();
  key.write((uchar *)dbname_tablename.data(), dbname_tablename.length());

  tbl->put_dict(m_dict, batch, key.ptr(), key.length());
  put(tbl);
}

void DDLManager::put(std::shared_ptr<RdbTable> tbl) {
  std::scoped_lock guard(m_lock);

  const std::string &dbname_tablename = tbl->fullname();
  m_ddl_hash[dbname_tablename] = tbl;
}

void DDLManager::remove(std::shared_ptr<RdbTable> tbl, rocksdb::WriteBatch *const batch) {
  std::scoped_lock guard(m_lock);

  uchar buf[FN_LEN * 2 + INDEX_NUMBER_SIZE];
  uint pos = 0;

  be_store_index(buf, static_cast<uint32_t>(MetaType::DDL_INDEX));
  pos += INDEX_NUMBER_SIZE;

  const std::string &dbname_tablename = tbl->fullname();
  memcpy(buf + pos, dbname_tablename.c_str(), dbname_tablename.size());
  pos += dbname_tablename.size();

  m_dict->delete_key(batch, {(const char *)buf, pos});
  auto iter = m_ddl_hash.find(dbname_tablename);
  if (iter != m_ddl_hash.end()) {
    m_ddl_hash.erase(iter);
  }
}

bool DDLManager::rename(const std::string &from, const std::string &to, rocksdb::WriteBatch *const batch) {
  std::scoped_lock guard(m_lock);
  std::shared_ptr<RdbTable> rec = find(from);
  if (!rec) {
    return false;
  }

  std::shared_ptr<RdbTable> new_rec = std::make_shared<RdbTable>(to);
  new_rec->m_rdbkeys = rec->m_rdbkeys;

  // Create a new key
  StringWriter key;
  key.write_uint32(static_cast<uint32_t>(MetaType::DDL_INDEX));

  const std::string &dbname_tablename = new_rec->fullname();
  key.write((uchar *)dbname_tablename.data(), dbname_tablename.length());
  if (rec->if_exist_cf(m_dict)) {
    remove(rec, batch);
    new_rec->put_dict(m_dict, batch, key.ptr(), key.length());
    put(new_rec);
  } else {
    STONEDB_LOG(LogCtl_Level::WARN, "Rename table:%s have no cf_definition", from.data());
  }
  return true;
}

void DDLManager::cleanup() {
  {
    std::scoped_lock rdb_guard(m_lock);
    m_ddl_hash.clear();
  }
  {
    std::scoped_lock mem_guard(m_mem_lock);
    m_mem_hash.clear();
  }
}

std::shared_ptr<core::RCMemTable> DDLManager::find_mem(const std::string &table_name) {
  std::scoped_lock guard(m_mem_lock);

  auto iter = m_mem_hash.find(table_name);
  if (iter != m_mem_hash.end()) return iter->second;

  return nullptr;
}

void DDLManager::put_mem(std::shared_ptr<core::RCMemTable> tb_mem, rocksdb::WriteBatch *const batch) {
  std::scoped_lock guard(m_mem_lock);

  StringWriter key;
  std::string table_name = tb_mem->FullName();
  key.write_uint32(static_cast<uint32_t>(MetaType::DDL_MEMTABLE));
  key.write((const uchar *)table_name.c_str(), table_name.size());

  StringWriter value;
  value.write_uint16(static_cast<uint>(enumVersion::DDL_VERSION));
  value.write_uint32(tb_mem->GetCFHandle()->GetID());
  value.write_uint32(tb_mem->GetMemID());

  m_dict->put_key(batch, {(char *)key.ptr(), key.length()}, {(char *)value.ptr(), value.length()});
  m_mem_hash[table_name] = tb_mem;
}

void DDLManager::remove_mem(std::shared_ptr<core::RCMemTable> tb_mem, rocksdb::WriteBatch *const batch) {
  std::scoped_lock guard(m_mem_lock);

  StringWriter key;
  const std::string &table_name = tb_mem->FullName();
  key.write_uint32(static_cast<uint32_t>(MetaType::DDL_MEMTABLE));
  key.write((const uchar *)table_name.c_str(), table_name.size());
  m_dict->delete_key(batch, {(const char *)key.ptr(), key.length()});

  auto iter = m_mem_hash.find(table_name);
  if (iter != m_mem_hash.end()) {
    m_mem_hash.erase(iter);
  }
}

bool DDLManager::rename_mem(std::string &from, std::string &to, rocksdb::WriteBatch *const batch) {
  std::scoped_lock guard(m_mem_lock);

  StringWriter skey;
  skey.write_uint32(static_cast<uint32_t>(MetaType::DDL_MEMTABLE));
  skey.write((const uchar *)from.c_str(), from.size());

  std::string origin_value;
  m_dict->get_value({(const char *)skey.ptr(), skey.length()}, &origin_value);
  m_dict->delete_key(batch, {(const char *)skey.ptr(), skey.length()});

  StringWriter dkey;
  dkey.write_uint32(static_cast<uint32_t>(MetaType::DDL_MEMTABLE));
  dkey.write((const uchar *)to.c_str(), to.size());
  m_dict->put_key(batch, {(const char *)dkey.ptr(), dkey.length()}, origin_value);

  auto iter = m_mem_hash.find(from);
  if (iter == m_mem_hash.end()) return false;

  auto tb_mem = iter->second;
  m_mem_hash.erase(iter);
  m_mem_hash[to] = tb_mem;
  return true;
}

bool DICTManager::init(rocksdb::DB *const rdb_dict, CFManager *const cf_manager) {
  m_db = rdb_dict;
  m_system_cfh = cf_manager->get_or_create_cf(m_db, DEFAULT_SYSTEM_CF_NAME);

  if (m_system_cfh == nullptr) return false;

  be_store_index(m_max_index, static_cast<uint32_t>(MetaType::MAX_INDEX_ID));

  return true;
}

std::unique_ptr<rocksdb::WriteBatch> DICTManager::begin() const {
  return std::unique_ptr<rocksdb::WriteBatch>(new rocksdb::WriteBatch);
}

void DICTManager::put_key(rocksdb::WriteBatchBase *const batch, const rocksdb::Slice &key,
                          const rocksdb::Slice &value) const {
  batch->Put(m_system_cfh, key, value);
}

rocksdb::Status DICTManager::get_value(const rocksdb::Slice &key, std::string *const value) const {
  rocksdb::ReadOptions options;
  options.total_order_seek = true;
  return m_db->Get(options, m_system_cfh, key, value);
}

void DICTManager::delete_key(rocksdb::WriteBatchBase *batch, const rocksdb::Slice &key) const {
  batch->Delete(m_system_cfh, key);
}

std::shared_ptr<rocksdb::Iterator> DICTManager::new_iterator() const {
  rocksdb::ReadOptions read_options;
  read_options.total_order_seek = true;
  return std::shared_ptr<rocksdb::Iterator>(m_db->NewIterator(read_options, m_system_cfh));
}

bool DICTManager::commit(rocksdb::WriteBatch *const batch, const bool &sync) const {
  if (!batch) return false;

  rocksdb::WriteOptions options;
  options.sync = sync;
  rocksdb::Status s = m_db->Write(options, batch);
  bool res = s.ok();
  if (!res) {
    STONEDB_LOG(LogCtl_Level::ERROR, "DICTManager::commit error");
  }
  batch->Clear();
  return res;
}

void DICTManager::dump_index_id(StringWriter &id, MetaType dict_type, const GlobalId &gl_index_id) const {
  id.write_uint32(static_cast<uint32_t>(dict_type));
  id.write_uint32(gl_index_id.cf_id);
  id.write_uint32(gl_index_id.index_id);
}

void DICTManager::delete_with_prefix(rocksdb::WriteBatch *const batch, MetaType dict_type,
                                     const GlobalId &gl_index_id) const {
  StringWriter dumpid;
  dump_index_id(dumpid, dict_type, gl_index_id);

  delete_key(batch, {(char *)dumpid.ptr(), dumpid.length()});
}

void DICTManager::save_index_info(rocksdb::WriteBatch *batch, uint16_t index_ver, uchar index_type, uint32_t index_id,
                                  uint32_t cf_id, std::vector<ColAttr> &cols) const {
  StringWriter dumpid;
  GlobalId gl_index_id = {cf_id, index_id};
  dump_index_id(dumpid, MetaType::INDEX_INFO, gl_index_id);

  StringWriter value;
  value.write_uint16(index_ver);
  value.write_uint8(index_type);

  if (index_ver > static_cast<uint16_t>(enumIndexInfo::INDEX_INFO_VERSION_INITIAL)) {
    value.write_uint32(cols.size());
    for (auto &col : cols) {
      value.write_uint16(col.col_no);
      value.write_uint8(col.col_type);
      value.write_uint8(col.col_flag);
    }
  } else {
    value.write_uint8(cols[0].col_type);
    value.write_uint8(cols[0].col_flag);
  }

  batch->Put(m_system_cfh, {(char *)dumpid.ptr(), dumpid.length()}, {(char *)value.ptr(), value.length()});
}

void DICTManager::add_cf_flags(rocksdb::WriteBatch *const batch, const uint32_t &cf_id,
                               const uint32_t &cf_flags) const {
  ASSERT(batch != nullptr, "batch is NULL");
  StringWriter key;
  key.write_uint32(static_cast<uint32_t>(MetaType::CF_INFO));
  key.write_uint32(cf_id);

  StringWriter value;
  value.write_uint16(static_cast<uint>(enumVersion::CF_VERSION));
  value.write_uint32(cf_flags);

  batch->Put(m_system_cfh, {(char *)key.ptr(), key.length()}, {(char *)value.ptr(), value.length()});
}

void DICTManager::delete_index_info(rocksdb::WriteBatch *batch, const GlobalId &gl_index_id) const {
  delete_with_prefix(batch, MetaType::INDEX_INFO, gl_index_id);
}

// Get MetaType::INDEX_INFO from cf __system__
bool DICTManager::get_index_info(const GlobalId &gl_index_id, uint16_t &index_ver, uchar &index_type,
                                 std::vector<ColAttr> &cols) const {
  bool found = false;
  bool error = false;
  std::string value;
  StringWriter dumpid;
  dump_index_id(dumpid, MetaType::INDEX_INFO, gl_index_id);

  const rocksdb::Status &status = get_value({(char *)dumpid.ptr(), dumpid.length()}, &value);
  if (status.ok()) {
    StringReader reader(value);
    reader.read_uint16(&index_ver);

    switch (static_cast<enumIndexInfo>(index_ver)) {
      case enumIndexInfo::INDEX_INFO_VERSION_INITIAL: {
        uchar type;
        uchar flag;
        reader.read_uint8(&index_type);
        reader.read_uint8(&type);
        reader.read_uint8(&flag);
        cols.emplace_back(ColAttr{0, type, flag});

        found = true;
        break;
      }
      case enumIndexInfo::INDEX_INFO_VERSION_COLS: {
        uint32_t cols_sz = 0;
        reader.read_uint8(&index_type);
        reader.read_uint32(&cols_sz);
        cols.resize(cols_sz);
        for (uint32_t i = 0; i < cols_sz; i++) {
          reader.read_uint16(&cols[i].col_no);
          reader.read_uint8(&cols[i].col_type);
          reader.read_uint8(&cols[i].col_flag);
        }
        found = true;
        break;
      }
      default:
        error = true;
        break;
    }
  }
  if (error) {
    STONEDB_LOG(LogCtl_Level::ERROR, "RocksDB: Found invalid key version number (%u, %u) ", index_ver, index_type);
  }
  return found;
}
bool DICTManager::get_cf_flags(const uint32_t &cf_id, uint32_t &cf_flags) const {
  bool found = false;
  std::string value;
  StringWriter key;
  key.write_uint32(static_cast<uint32_t>(MetaType::CF_INFO));
  key.write_uint32(cf_id);

  const rocksdb::Status status = get_value({reinterpret_cast<char *>(key.ptr()), key.length()}, &value);

  if (status.ok()) {
    uint16_t version = 0;
    StringReader reader(value);
    reader.read_uint16(&version);
    if (version == static_cast<uint>(enumVersion::CF_VERSION)) {
      reader.read_uint32(&cf_flags);
      found = true;
    }
  }

  return found;
}

// This function is supposed to be called by DROP TABLE
void DICTManager::add_drop_table(std::vector<std::shared_ptr<RdbKey>> &keys, rocksdb::WriteBatch *const batch) const {
  std::vector<GlobalId> dropped_index_ids;
  for (auto &rdbkey : keys) {
    dropped_index_ids.push_back(rdbkey->get_gl_index_id());
  }

  add_drop_index(dropped_index_ids, batch);
}

void DICTManager::add_drop_index(const std::vector<GlobalId> &gl_index_ids, rocksdb::WriteBatch *const batch) const {
  for (const auto &gl_index_id : gl_index_ids) {
    delete_index_info(batch, gl_index_id);
    start_ongoing_index(batch, gl_index_id, MetaType::DDL_DROP_INDEX_ONGOING);
  }
}

bool DICTManager::get_max_index_id(uint32_t *const index_id) const {
  bool found = false;
  std::string value;

  const rocksdb::Status status = get_value({(char *)m_max_index, INDEX_NUMBER_SIZE}, &value);
  if (status.ok()) {
    uint16_t version = 0;
    StringReader reader({value.data(), value.length()});
    reader.read_uint16(&version);
    if (version == static_cast<uint>(enumVersion::MAX_INDEX_ID_VERSION)) {
      reader.read_uint32(index_id);
      found = true;
    }
  }
  return found;
}

bool DICTManager::update_max_index_id(rocksdb::WriteBatch *const batch, const uint32_t &index_id) const {
  uint32_t old_index_id = -1;
  if (get_max_index_id(&old_index_id)) {
    if (old_index_id > index_id) {
      STONEDB_LOG(LogCtl_Level::ERROR, "RocksDB: Found max index id %u but trying to update to %u.", old_index_id,
                  index_id);
      return true;
    }
  }
  StringWriter value;
  value.write_uint16(static_cast<uint>(enumVersion::MAX_INDEX_ID_VERSION));
  value.write_uint32(index_id);
  batch->Put(m_system_cfh, {(char *)m_max_index, INDEX_NUMBER_SIZE}, {(char *)value.ptr(), value.length()});

  return false;
}

void DICTManager::get_ongoing_index(std::vector<GlobalId> &ids, MetaType dd_type) const {
  uchar index_buf[INDEX_NUMBER_SIZE];
  be_store_uint32(index_buf, static_cast<uint32_t>(dd_type));
  uint32_t upper_type = static_cast<uint32_t>(dd_type) + 1;
  uchar upper_buf[INDEX_NUMBER_SIZE];
  be_store_uint32(upper_buf, upper_type);
  rocksdb::Slice upper_slice((char *)upper_buf, INDEX_NUMBER_SIZE);

  rocksdb::ReadOptions read_options;
  read_options.iterate_upper_bound = &upper_slice;
  auto it = std::shared_ptr<rocksdb::Iterator>(m_db->NewIterator(read_options, m_system_cfh));
  for (it->Seek({reinterpret_cast<const char *>(index_buf), INDEX_NUMBER_SIZE}); it->Valid(); it->Next()) {
    rocksdb::Slice key = it->key();
    const uchar *const ptr = (const uchar *)key.data();

    if (key.size() != INDEX_NUMBER_SIZE * 3 || be_to_uint32(ptr) != static_cast<uint32_t>(dd_type)) {
      break;
    }
    GlobalId index_id;
    index_id.cf_id = be_to_uint32(ptr + INDEX_NUMBER_SIZE);
    index_id.index_id = be_to_uint32(ptr + 2 * INDEX_NUMBER_SIZE);
    ids.push_back(index_id);
  }
}

void DICTManager::start_ongoing_index(rocksdb::WriteBatch *const batch, const GlobalId &id, MetaType dd_type) const {
  StringWriter dumpid;
  dump_index_id(dumpid, dd_type, id);
  StringWriter value;
  // version as needed
  if (dd_type == MetaType::DDL_DROP_INDEX_ONGOING) {
    value.write_uint16(static_cast<uint>(enumVersion::DROP_INDEX_ONGOING_VERSION));
  } else {
    value.write_uint16(static_cast<uint>(enumVersion::CREATE_INDEX_ONGOING_VERSION));
  }

  batch->Put(m_system_cfh, {(char *)dumpid.ptr(), dumpid.length()}, {(char *)value.ptr(), value.length()});
}

void DICTManager::end_ongoing_index(rocksdb::WriteBatch *const batch, const GlobalId &id, MetaType dd_type) const {
  delete_with_prefix(batch, dd_type, id);
}

void DICTManager::finish_indexes(const std::vector<GlobalId> &ids, MetaType dd_type) const {
  const std::unique_ptr<rocksdb::WriteBatch> wb = begin();
  rocksdb::WriteBatch *const batch = wb.get();
  for (const auto &index_id : ids) {
    end_ongoing_index(batch, index_id, dd_type);
  }
  commit(batch);
}

bool DICTManager::is_drop_index_ongoing(const GlobalId &gl_index_id, MetaType dd_type) const {
  std::string value;
  StringWriter dumpid;
  dump_index_id(dumpid, dd_type, gl_index_id);

  return get_value({(char *)dumpid.ptr(), dumpid.length()}, &value).ok();
}

uint SeqGenerator::get_and_update_next_number(DICTManager *const dict) {
  uint res;
  std::scoped_lock guard(m_mutex);

  res = m_next_number++;

  const std::unique_ptr<rocksdb::WriteBatch> wb = dict->begin();
  rocksdb::WriteBatch *const batch = wb.get();
  ASSERT(batch != nullptr, "batch is NULL");
  dict->update_max_index_id(batch, res);
  dict->commit(batch);
  return res;
}

void CFManager::init(std::vector<rocksdb::ColumnFamilyHandle *> &handles) {
  for (auto cfh : handles) {
    m_cf_name_map[cfh->GetName()] = cfh;
    m_cf_id_map[cfh->GetID()] = cfh;
  }
}

void CFManager::cleanup() {
  for (auto it : m_cf_name_map) {
    delete it.second;
  }
}

// Find column family by name. If it doesn't exist, create it
rocksdb::ColumnFamilyHandle *CFManager::get_or_create_cf(rocksdb::DB *const rdb, const std::string &cf_name_arg) {
  rocksdb::ColumnFamilyHandle *cf_handle = nullptr;

  const std::string &cf_name = cf_name_arg.empty() ? DEFAULT_CF_NAME : cf_name_arg;
  std::scoped_lock guard(m_mutex);

  auto it = m_cf_name_map.find(cf_name);
  if (it != m_cf_name_map.end()) {
    cf_handle = it->second;
  } else {
    rocksdb::ColumnFamilyOptions opts;
    if (!IsRowStoreCF(cf_name)) opts.compaction_filter_factory.reset(new index::IndexCompactFilterFactory);
    const rocksdb::Status s = rdb->CreateColumnFamily(opts, cf_name, &cf_handle);
    if (s.ok()) {
      m_cf_name_map[cf_handle->GetName()] = cf_handle;
      m_cf_id_map[cf_handle->GetID()] = cf_handle;
    } else {
      cf_handle = nullptr;
    }
  }

  return cf_handle;
}

rocksdb::ColumnFamilyHandle *CFManager::get_cf_by_id(const uint32_t &id) {
  rocksdb::ColumnFamilyHandle *cf_handle = nullptr;
  std::scoped_lock guard(m_mutex);

  const auto it = m_cf_id_map.find(id);
  if (it != m_cf_id_map.end()) cf_handle = it->second;

  return cf_handle;
}

std::vector<rocksdb::ColumnFamilyHandle *> CFManager::get_all_cf(void) {
  std::vector<rocksdb::ColumnFamilyHandle *> list;
  std::scoped_lock guard(m_mutex);

  for (auto it : m_cf_id_map) {
    ASSERT(it.second != nullptr, "it.second is NULL");
    list.push_back(it.second);
  }
  return list;
}

}  // namespace index
}  // namespace stonedb
