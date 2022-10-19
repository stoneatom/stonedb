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

#include <arpa/inet.h>
#include <netdb.h>  // stonedb8 for gethostbyname()
#include <iostream>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "mysql/plugin.h"
#include "sql/binlog.h"
#include "sql/sql_plugin_var.h"  // stonedb8 due to conflicting macro definitions SYS_VAR

#include "common/assert.h"
#include "common/exception.h"
#include "core/compilation_tools.h"
#include "core/compiled_query.h"
#include "core/temp_table.h"
#include "core/tools.h"
#include "core/transaction.h"
#include "core/value.h"
#include "handler/ha_tianmu.h"
#include "mm/initializer.h"
#include "system/configuration.h"
#include "system/file_out.h"
#include "util/fs.h"

#define MYSQL_SERVER 1

namespace Tianmu {
namespace DBHandler {

char *strmov_str(char *dst, const char *src) {
  while ((*dst++ = *src++))
    ;
  return dst - 1;
}

const Alter_inplace_info::HA_ALTER_FLAGS TianmuHandler::TIANMU_SUPPORTED_ALTER_ADD_DROP_ORDER =
    Alter_inplace_info::ADD_COLUMN | Alter_inplace_info::DROP_COLUMN | Alter_inplace_info::ALTER_STORED_COLUMN_ORDER;
const Alter_inplace_info::HA_ALTER_FLAGS TianmuHandler::TIANMU_SUPPORTED_ALTER_COLUMN_NAME =
    Alter_inplace_info::ALTER_COLUMN_DEFAULT | Alter_inplace_info::ALTER_COLUMN_NAME;
/////////////////////////////////////////////////////////////////////
//
// NOTICE: ALL EXCEPTIONS SHOULD BE CAUGHT in the handler API!!!
//         MySQL doesn't use exception.
//
/////////////////////////////////////////////////////////////////////

/*
 Example of simple lock controls. The "share" it creates is structure we will
 pass to each rcbase handler. Do you have to have one of these? Well, you have
 pieces that are used for locking, and they are needed to function.
 */

bool rcbase_query_caching_of_table_permitted(THD *thd, [[maybe_unused]] char *full_name,
                                             [[maybe_unused]] uint full_name_len, [[maybe_unused]] ulonglong *unused) {
  if (!thd_test_options(thd, (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) return ((bool)true);
  return ((bool)false);
}

static core::Value GetValueFromField(Field *f) {
  core::Value v;

  if (f->is_null()) return v;

  switch (f->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONGLONG:
      v.SetInt(f->val_int());
      break;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
      v.SetDouble(f->val_real());
      break;
    case MYSQL_TYPE_NEWDECIMAL: {
      auto dec_f = dynamic_cast<Field_new_decimal *>(f);
      v.SetInt(std::lround(dec_f->val_real() * types::PowOfTen(dec_f->dec)));
      break;
    }
    case MYSQL_TYPE_TIMESTAMP: {
      MYSQL_TIME my_time;
      std::memset(&my_time, 0, sizeof(my_time));
      f->get_time(&my_time);
      // convert to UTC
      if (!common::IsTimeStampZero(my_time)) {
        bool myb;
        my_time_t secs_utc = current_txn_->Thd()->variables.time_zone->TIME_to_gmt_sec(&my_time, &myb);
        common::GMTSec2GMTTime(&my_time, secs_utc);
      }
      types::DT dt = {};
      dt.year = my_time.year;
      dt.month = my_time.month;
      dt.day = my_time.day;
      dt.hour = my_time.hour;
      dt.minute = my_time.minute;
      dt.second = my_time.second;
      v.SetInt(dt.val);
      break;
    }
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_DATETIME2: {
      MYSQL_TIME my_time;
      std::memset(&my_time, 0, sizeof(my_time));
      f->get_time(&my_time);
      types::DT dt = {};
      dt.year = my_time.year;
      dt.month = my_time.month;
      dt.day = my_time.day;
      dt.hour = my_time.hour;
      dt.minute = my_time.minute;
      dt.second = my_time.second;
      v.SetInt(dt.val);
      break;
    }
    case MYSQL_TYPE_YEAR:  // what the hell?
    {
      types::DT dt = {};
      dt.year = f->val_int();
      v.SetInt(dt.val);
      break;
    }
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING: {
      String str;
      f->val_str(&str);
      v.SetString(const_cast<char *>(str.ptr()), str.length());
      break;
    }
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_NULL:
    case MYSQL_TYPE_BIT:
    default:
      throw common::Exception("unsupported mysql type " + std::to_string(f->type()));
      break;
  }
  return v;
}

TianmuHandler::TianmuHandler(handlerton *hton, TABLE_SHARE *table_arg, bool partitioned)
    : handler(hton, table_arg), m_partitioned(partitioned) {
  ref_length = sizeof(uint64_t);
}

namespace {
std::vector<bool> GetAttrsUseIndicator(TABLE *table) {
  int col_id = 0;
  std::vector<bool> attr_uses;
  for (Field **field = table->field; *field; ++field, ++col_id) {
    if (bitmap_is_set(table->read_set, col_id) || bitmap_is_set(table->write_set, col_id))
      attr_uses.push_back(true);
    else
      attr_uses.push_back(false);
  }
  return attr_uses;
}
}  // namespace

static bool is_delay_insert(THD *thd) {
  return tianmu_sysvar_insert_delayed &&
         (thd_sql_command(thd) == SQLCOM_INSERT || thd_sql_command(thd) == SQLCOM_INSERT_SELECT) &&
         thd->lex->duplicates != DUP_UPDATE;
}

/*
 The idea with handler::store_lock() is the following:

 The statement decided which locks we should need for the table
 for updates/deletes/inserts we get WRITE locks, for SELECT... we get
 read locks.

 Before adding the lock into the table lock handler (see thr_lock.c)
 mysqld calls store lock with the requested locks.  Store lock can now
 modify a write lock to a read lock (or some other lock), ignore the
 lock (if we don't want to use MySQL table locks at all) or add locks
 for many tables (like we do when we are using a MERGE handler).

 Berkeley DB for example  changes all WRITE locks to TL_WRITE_ALLOW_WRITE
 (which signals that we are doing WRITES, but we are still allowing other
 reader's and writer's.

 When releasing locks, store_lock() are also called. In this case one
 usually doesn't have to do anything.

 In some exceptional cases MySQL may send a request for a TL_IGNORE;
 This means that we are requesting the same lock as last time and this
 should also be ignored. (This may happen when someone does a flush
 table when we have opened a part of the tables, in which case mysqld
 closes and reopens the tables and tries to get the same locks at last
 time).  In the future we will probably try to remove this.

 Called from lock.cc by get_lock_data().
 */
THR_LOCK_DATA **TianmuHandler::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
  if (lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE) {
    if (is_delay_insert(thd))
      lock_type = TL_READ;
    else
      lock_type = TL_WRITE_CONCURRENT_INSERT;
  }

  if (lock_type != TL_IGNORE && m_lock.type == TL_UNLOCK) m_lock.type = lock_type;
  *to++ = &m_lock;
  return to;
}

/*
 First you should go read the section "locking functions for mysql" in
 lock.cc to understand this.
 This create a lock on the table. If you are implementing a storage engine
 that can handle transacations look at ha_berkely.cc to see how you will
 want to goo about doing this. Otherwise you should consider calling flock()
 here.

 Called from lock.cc by lock_external() and unlock_external(). Also called
 from sql_table.cc by copy_data_between_tables().
 */
int TianmuHandler::external_lock(THD *thd, int lock_type) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = 1;

  // static const char *ss[] = { "READ LOCK", "WRITE LOCK", "UNLOCK", };
  // rclog << lock << "external lock table " << m_table_name << " type: " <<
  // ss[lock_type] << " command: " << thd->lex->sql_command << unlock;

  if (thd->lex->sql_command == SQLCOM_LOCK_TABLES) DBUG_RETURN(HA_ERR_WRONG_COMMAND);

  if (is_delay_insert(thd) && lock_type == F_WRLCK) DBUG_RETURN(0);

  try {
    if (lock_type == F_UNLCK) {
      if (thd->lex->sql_command == SQLCOM_UNLOCK_TABLES) current_txn_->ExplicitUnlockTables();

      if (thd->killed) ha_rcengine_->Rollback(thd, true);
      if (current_txn_) {
        current_txn_->RemoveTable(share);
        if (current_txn_->Empty()) {
          ha_rcengine_->ClearTx(thd);
        }
      }
    } else {
      auto tx = ha_rcengine_->GetTx(thd);
      if (thd->lex->sql_command == SQLCOM_LOCK_TABLES) tx->ExplicitLockTables();

      if (lock_type == F_RDLCK) {
        tx->AddTableRD(share);
      } else {
        tx->AddTableWR(share);
        trans_register_ha(thd, false, tianmu_hton, NULL);
        if (thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
          trans_register_ha(thd, true, tianmu_hton, NULL);
      }
    }
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "Tianmu internal error", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  // destroy the tx on failure
  if (ret != 0) ha_rcengine_->ClearTx(thd);

  DBUG_RETURN(ret);
}

namespace {
inline bool has_dup_key(std::shared_ptr<index::RCTableIndex> &indextab, TABLE *table, size_t &row) {
  common::ErrorCode ret;
  std::vector<std::string_view> records;
  KEY *key = table->key_info + table->s->primary_key;

  for (uint i = 0; i < key->actual_key_parts; i++) {
    uint col = key->key_part[i].field->field_index();
    Field *f = table->field[col];
    switch (f->type()) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONGLONG: {
        int64_t v = f->val_int();
        records.emplace_back((const char *)&v, sizeof(int64_t));
        break;
      }
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE: {
        double v = f->val_real();
        records.emplace_back((const char *)&v, sizeof(double));
        break;
      }
      case MYSQL_TYPE_NEWDECIMAL: {
        auto dec_f = dynamic_cast<Field_new_decimal *>(f);
        double v = std::lround(dec_f->val_real() * types::PowOfTen(dec_f->dec));
        records.emplace_back((const char *)&v, sizeof(double));
        break;
      }
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_TIME2:
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_NEWDATE:
      case MYSQL_TYPE_TIMESTAMP2:
      case MYSQL_TYPE_DATETIME2: {
        MYSQL_TIME my_time;
        std::memset(&my_time, 0, sizeof(my_time));
        f->get_time(&my_time);
        types::DT dt(my_time);
        records.emplace_back((const char *)&dt.val, sizeof(int64_t));
        break;
      }

      case MYSQL_TYPE_TIMESTAMP: {
        MYSQL_TIME my_time;
        std::memset(&my_time, 0, sizeof(my_time));
        f->get_time(&my_time);
        auto saved = my_time.second_part;
        // convert to UTC
        if (!common::IsTimeStampZero(my_time)) {
          bool myb;
          my_time_t secs_utc = current_thd->variables.time_zone->TIME_to_gmt_sec(&my_time, &myb);
          common::GMTSec2GMTTime(&my_time, secs_utc);
        }
        my_time.second_part = saved;
        types::DT dt(my_time);
        records.emplace_back((const char *)&dt.val, sizeof(int64_t));
        break;
      }
      case MYSQL_TYPE_YEAR:  // what the hell?
      {
        types::DT dt = {};
        dt.year = f->val_int();
        records.emplace_back((const char *)&dt.val, sizeof(int64_t));
        break;
      }
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_STRING: {
        String str;
        f->val_str(&str);
        records.emplace_back(str.ptr(), str.length());
        break;
      }
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_GEOMETRY:
      case MYSQL_TYPE_NULL:
      case MYSQL_TYPE_BIT:
      default:
        throw common::Exception("unsupported mysql type " + std::to_string(f->type()));
        break;
    }
  }

  ret = indextab->GetRowByKey(current_txn_, records, row);
  return (ret == common::ErrorCode::SUCCESS);
}
}  // namespace

/*
 write_row() inserts a row. No extra() hint is given currently if a bulk load
 is happeneding. buf() is a byte array of data. You can use the field
 information to extract the data from the native byte array type.
 Example of this would be:
 for (Field **field=table->field ; *field ; field++)
 {
 ...
 }

 See ha_tina.cc for an example of extracting all of the data as strings.
 ha_berekly.cc has an example of how to store it intact by "packing" it
 for ha_berkeley's own native storage type.

 See the note for update_row() on auto_increments and timestamps. This
 case also applied to write_row().

 Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
 sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.
 */
int TianmuHandler::write_row([[maybe_unused]] uchar *buf) {
  int ret = 1;
  DBUG_ENTER(__PRETTY_FUNCTION__);
  try {
    if (ha_thd()->lex->duplicates == DUP_UPDATE) {
      if (auto indextab = ha_rcengine_->GetTableIndex(m_table_name)) {
        if (size_t row; has_dup_key(indextab, table, row)) {
          m_dupkey_pos = row;
          DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
        }
      }
    }
    ret = ha_rcengine_->InsertRow(m_table_name, current_txn_, table, share);
  } catch (common::OutOfMemoryException &e) {
    DBUG_RETURN(ER_LOCK_WAIT_TIMEOUT);
  } catch (common::DatabaseException &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught in Engine::InsertRow: %s.", e.what());
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
  } catch (common::FormatException &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught in Engine::InsertRow: %s Row: %llu, field %u.", e.what(),
               e.m_row_no, e.m_field_no);
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
  } catch (common::FileException &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught in Engine::InsertRow: %s.", e.what());
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
  } catch (common::Exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught in Engine::InsertRow: %s.", e.what());
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught in Engine::InsertRow: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
  DBUG_RETURN(ret);
}

/*
 Yes, update_row() does what you expect, it updates a row. old_data will have
 the previous row record in it, while new_data will have the newest data in
 it.
 Keep in mind that the server can do updates based on ordering if an ORDER BY
 clause was used. Consecutive ordering is not guarenteed.
 Currently new_data will not have an updated auto_increament record, or
 and updated timestamp field. You can do these for example by doing these:
 if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_UPDATE)
 table->timestamp_field->set_time();
 if (table->next_number_field && record == table->record[0])
 update_auto_increment();

 Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.
 */
int TianmuHandler::update_row(const uchar *old_data, uchar *new_data) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int ret = HA_ERR_INTERNAL_ERROR;
  auto org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  std::shared_ptr<void> defer(nullptr,
                              [org_bitmap, this](...) { dbug_tmp_restore_column_map(table->write_set, org_bitmap); });

  try {
    auto tab = current_txn_->GetTableByPath(m_table_name);

    for (uint i = 0; i < table->s->fields; i++) {
      if (!bitmap_is_set(table->write_set, i)) {
        continue;
      }
      auto field = table->field[i];
      if (field->is_nullable()) {
        if (field->is_null_in_record(old_data) && field->is_null_in_record(new_data)) {
          continue;
        }

        if (field->is_null_in_record(new_data)) {
          core::Value null;
          tab->UpdateItem(current_position, i, null);
          continue;
        }
      }
      auto o_ptr = field->field_ptr() - table->record[0] + old_data;
      auto n_ptr = field->field_ptr() - table->record[0] + new_data;
      if (field->is_null_in_record(old_data) || std::memcmp(o_ptr, n_ptr, field->pack_length()) != 0) {
        my_bitmap_map *org_bitmap2 = dbug_tmp_use_all_columns(table, table->read_set);
        std::shared_ptr<void> defer(
            nullptr, [org_bitmap2, this](...) { dbug_tmp_restore_column_map(table->read_set, org_bitmap2); });
        core::Value v = GetValueFromField(field);
        tab->UpdateItem(current_position, i, v);
      }
    }
    ha_rcengine_->IncTianmuStatUpdate();
    DBUG_RETURN(0);
  } catch (common::DatabaseException &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Update exception: %s.", e.what());
  } catch (common::FileException &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Update exception: %s.", e.what());
  } catch (common::DupKeyException &e) {
    ret = HA_ERR_FOUND_DUPP_KEY;
    TIANMU_LOG(LogCtl_Level::ERROR, "Update exception: %s.", e.what());
  } catch (common::Exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Update exception: %s.", e.what());
  } catch (std::exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Update exception: %s.", e.what());
  } catch (...) {
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
  DBUG_RETURN(ret);
}

/*
 This will delete a row. buf will contain a copy of the row to be deleted.
 The server will call this right after the current row has been called (from
 either a previous rnd_nexT() or index call).
 If you keep a pointer to the last row or can access a primary key it will
 make doing the deletion quite a bit easier.
 Keep in mind that the server does no guarentee consecutive deletions. ORDER BY
 clauses can be used.

 Called in sql_acl.cc and sql_udf.cc to manage internal table information.
 Called in sql_delete.cc, sql_insert.cc, and sql_select.cc. In sql_select it is
 used for removing duplicates while in insert it is used for REPLACE calls.
 */
int TianmuHandler::delete_row([[maybe_unused]] const uchar *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/*
 Used to delete all rows in a table. Both for cases of truncate and
 for cases where the optimizer realizes that all rows will be
 removed as a result of a SQL statement.

 Called from item_sum.cc by Item_func_group_concat::clear(),
 Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
 Called from sql_delete.cc by mysql_delete().
 Called from sql_select.cc by JOIN::rein*it.
 Called from sql_union.cc by st_select_lex_unit::exec().
 */
int TianmuHandler::delete_all_rows() {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int TianmuHandler::rename_table(const char *from, const char *to, const dd::Table *from_table_def,
                                dd::Table *to_table_def) {  // stonedb8 TODO
  try {
    ha_rcengine_->RenameTable(current_txn_, from, to, from_table_def, to_table_def, ha_thd());
    return 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
  return 1;
}

void TianmuHandler::update_create_info([[maybe_unused]] HA_CREATE_INFO *create_info) {}

/*
 ::info() is used to return information to the optimizer.
 see my_base.h for the complete description

 Currently this table handler doesn't implement most of the fields
 really needed. SHOW also makes use of this data
 Another note, you will probably want to have the following in your
 code:
 if (records < 2)
 records = 2;
 The reason is that the server will optimize for cases of only a single
 record. If in a table scan you don't know the number of records
 it will probably be better to set records to two so you can return
 as many records as you need.
 Along with records a few more variables you may wish to set are:
 records
 deleted
 data_file_length
 index_file_length
 delete_length
 check_time
 Take a look at the public variables in handler.h for more information.

 Called in:
 filesort.cc
 ha_heap.cc
 item_sum.cc
 opt_sum.cc
 sql_delete.cc
 sql_delete.cc
 sql_derived.cc
 sql_select.cc
 sql_select.cc
 sql_select.cc
 sql_select.cc
 sql_select.cc
 sql_show.cc
 sql_show.cc
 sql_show.cc
 sql_show.cc
 sql_table.cc
 sql_union.cc
 sql_update.cc

 */
int TianmuHandler::info(uint flag) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int ret = 1;
  try {
    std::scoped_lock guard(global_mutex_);
    if (flag & HA_STATUS_VARIABLE) {
      std::shared_ptr<core::RCTable> tab;
      if (current_txn_ != nullptr) {
        tab = current_txn_->GetTableByPath(m_table_name);
      } else
        tab = ha_rcengine_->GetTableRD(m_table_name);
      stats.records = (ha_rows)tab->NumOfValues();
      stats.data_file_length = 0;
      stats.mean_rec_length = 0;
      if (stats.records > 0) {
        std::vector<core::AttrInfo> attr_info(tab->GetAttributesInfo());
        uint no_attrs = tab->NumOfAttrs();
        for (uint j = 0; j < no_attrs; j++) stats.data_file_length += attr_info[j].comp_size;  // compressed size
        stats.mean_rec_length = ulong(stats.data_file_length / stats.records);
      }
    }

    if (flag & HA_STATUS_CONST) stats.create_time = share->GetCreateTime();

    if (flag & HA_STATUS_TIME) stats.update_time = share->GetUpdateTime();

    if (flag & HA_STATUS_ERRKEY) {
      errkey = 0;  // TODO: for now only support one pk index
      my_store_ptr(dup_ref, ref_length, m_dupkey_pos);
    }

    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

/* this should return 0 for concurrent insert to work */
bool tianmu_check_status([[maybe_unused]] void *param) { return 0; }

/*
 Used for opening tables. The name will be the name of the file.
 A table is opened when it needs to be opened. For instance
 when a request comes in for a select on the table (tables are not
 open and closed for each request, they are cached).

 Called from handler.cc by handler::ha_open(). The server opens all tables by
 calling ha_open() which then calls the handler specific open().
 */
int TianmuHandler::open(const char *name, [[maybe_unused]] int mode, [[maybe_unused]] uint test_if_locked,
                        [[maybe_unused]] const dd::Table *table_def) {  // stonedb8 TODO
  DBUG_ENTER(__PRETTY_FUNCTION__);

  m_table_name = name;
  int ret = 1;

  try {
    // TODO:
    // Probably we don't need to search from the map each time.
    // Keeping the share together with mysql handler cache makes
    // more sense that would mean once a table is opened the TableShare
    // would be kept.
    if (!(share = ha_rcengine_->GetTableShare(table_share))) DBUG_RETURN(ret);

    thr_lock_data_init(&share->thr_lock, &m_lock, NULL);
    share->thr_lock.check_status = tianmu_check_status;
    // have primary key, use table index
    if (table->s->primary_key != MAX_INDEXES) ha_rcengine_->AddTableIndex(name, table, ha_thd());
    ha_rcengine_->AddMemTable(table, share);
    ret = 0;
  } catch (common::Exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "Error from Tianmu engine", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "A tianmu exception is caught: %s", e.what());
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  info(HA_STATUS_CONST);

  DBUG_RETURN(ret);
}

int TianmuHandler::free_share() {
  share.reset();
  return 0;
}

/*
 Closes a table. We call the free_share() function to free any resources
 that we have allocated in the "shared" structure.

 Called from sql_base.cc, sql_select.cc, and table.cc.
 In sql_select.cc it is only used to close up temporary tables or during
 the process where a temporary table is converted over to being a
 myisam table.
 For sql_base.cc look at close_data_tables().
 */
int TianmuHandler::close() {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  DBUG_RETURN(free_share());
}

int TianmuHandler::fill_row_by_id([[maybe_unused]] uchar *buf, uint64_t rowid) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_KEY_NOT_FOUND;
  try {
    auto tab = current_txn_->GetTableByPath(m_table_name);
    if (tab) {
      my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);
      std::shared_ptr<void> defer(
          nullptr, [org_bitmap, this](...) { dbug_tmp_restore_column_map(table->write_set, org_bitmap); });

      tab->FillRowByRowid(table, rowid);
      current_position = rowid;
      rc = 0;
    }
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  }
  DBUG_RETURN(rc);
}

int TianmuHandler::index_init(uint index, [[maybe_unused]] bool sorted) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  active_index = index;
  DBUG_RETURN(0);
}

int TianmuHandler::index_end() {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  active_index = MAX_KEY;
  DBUG_RETURN(0);
}

/*
 Positions an index cursor to the index specified in the handle. Fetches the
 row if available. If the key value is null, begin at the first key of the
 index.
 */
int TianmuHandler::index_read([[maybe_unused]] uchar *buf, [[maybe_unused]] const uchar *key,
                              [[maybe_unused]] uint key_len __attribute__((unused)),
                              enum ha_rkey_function find_flag __attribute__((unused))) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_KEY_NOT_FOUND;
  try {
    auto index = ha_rcengine_->GetTableIndex(m_table_name);
    if (index && (active_index == table_share->primary_key)) {
      std::vector<std::string_view> keys;
      key_convert(key, key_len, index->KeyCols(), keys);
      // support equality fullkey lookup over primary key, using full tuple
      if (find_flag == HA_READ_KEY_EXACT) {
        uint64_t rowid;
        if (index->GetRowByKey(current_txn_, keys, rowid) == common::ErrorCode::SUCCESS) {
          rc = fill_row_by_id(buf, rowid);
        }
      } else if (find_flag == HA_READ_AFTER_KEY || find_flag == HA_READ_KEY_OR_NEXT) {
        auto iter = current_txn_->KVTrans().KeyIter();
        common::Operator op = (find_flag == HA_READ_AFTER_KEY) ? common::Operator::O_MORE : common::Operator::O_MORE_EQ;
        iter->ScanToKey(index, keys, op);
        uint64_t rowid;
        iter->GetRowid(rowid);
        rc = fill_row_by_id(buf, rowid);

      } else {
        // not support HA_READ_PREFIX_LAST_OR_PREV HA_READ_PREFIX_LAST
        rc = HA_ERR_WRONG_COMMAND;
        TIANMU_LOG(LogCtl_Level::ERROR, "Error: index_read not support prefix search");
      }

    } else {
      // other index not support
      rc = HA_ERR_WRONG_INDEX;
      TIANMU_LOG(LogCtl_Level::ERROR, "Error: index_read only support primary key");
    }

  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  }

  DBUG_RETURN(rc);
}

/*
 Used to read forward through the index.
 */
int TianmuHandler::index_next([[maybe_unused]] uchar *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_END_OF_FILE;
  try {
    auto iter = current_txn_->KVTrans().KeyIter();
    ++(*iter);
    if (iter->IsValid()) {
      uint64_t rowid;
      iter->GetRowid(rowid);
      rc = fill_row_by_id(buf, rowid);
    } else {
      rc = HA_ERR_KEY_NOT_FOUND;
    }
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  }
  DBUG_RETURN(rc);
}

/*
 Used to read backwards through the index.
 */
int TianmuHandler::index_prev([[maybe_unused]] uchar *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_END_OF_FILE;
  try {
    auto iter = current_txn_->KVTrans().KeyIter();
    --(*iter);
    if (iter->IsValid()) {
      uint64_t rowid;
      iter->GetRowid(rowid);
      rc = fill_row_by_id(buf, rowid);
    } else {
      rc = HA_ERR_KEY_NOT_FOUND;
    }
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  }
  DBUG_RETURN(rc);
}

/*
 index_first() asks for the first key in the index.

 Called from opt_range.cc, opt_sum.cc, sql_handler.cc,
 and sql_select.cc.
 */
int TianmuHandler::index_first([[maybe_unused]] uchar *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_END_OF_FILE;
  try {
    auto index = ha_rcengine_->GetTableIndex(m_table_name);
    if (index && current_txn_) {
      uint64_t rowid;
      auto iter = current_txn_->KVTrans().KeyIter();
      iter->ScanToEdge(index, true);
      if (iter->IsValid()) {
        iter->GetRowid(rowid);
        rc = fill_row_by_id(buf, rowid);
      } else {
        rc = HA_ERR_KEY_NOT_FOUND;
      }
    }
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  }
  DBUG_RETURN(rc);
}

/*
 index_last() asks for the last key in the index.

 Called from opt_range.cc, opt_sum.cc, sql_handler.cc,
 and sql_select.cc.
 */
int TianmuHandler::index_last([[maybe_unused]] uchar *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_END_OF_FILE;
  try {
    auto index = ha_rcengine_->GetTableIndex(m_table_name);
    if (index && current_txn_) {
      uint64_t rowid;
      auto iter = current_txn_->KVTrans().KeyIter();
      iter->ScanToEdge(index, false);
      if (iter->IsValid()) {
        iter->GetRowid(rowid);
        rc = fill_row_by_id(buf, rowid);
      } else {
        rc = HA_ERR_KEY_NOT_FOUND;
      }
    }
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  }
  DBUG_RETURN(rc);
}

/*
 rnd_init() is called when the system wants the storage engine to do a table
 scan.
 See the example in the introduction at the top of this file to see when
 rnd_init() is called.

 Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
 sql_table.cc, and sql_update.cc.
 */
int TianmuHandler::rnd_init(bool scan) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = 1;
  try {
    if (m_query && !m_result && table_ptr->NumOfObj() != 0) {
      m_cq->Result(m_tmp_table);  // it is ALWAYS -2 though....
      m_result = true;

      try {
        core::FunctionExecutor fe(std::bind(&core::Query::LockPackInfoForUse, std::ref(m_query)),
                                  std::bind(&core::Query::UnlockPackInfoFromUse, std::ref(m_query)));

        core::TempTable *push_down_result = m_query->Preexecute(*m_cq, NULL, false);
        if (!push_down_result || push_down_result->NumOfTables() != 1)
          throw common::InternalException("core::Query execution returned no result object");

        core::Filter *filter(push_down_result->GetMultiIndexP()->GetFilter(0));
        if (!filter)
          filter_ptr.reset(
              new core::Filter(push_down_result->GetMultiIndexP()->OrigSize(0), table_ptr->Getpackpower()));
        else
          filter_ptr.reset(new core::Filter(*filter));

        table_ptr = push_down_result->GetTableP(0);
        table_new_iter = ((core::RCTable *)table_ptr)->Begin(GetAttrsUseIndicator(table), *filter);
        table_new_iter_end = ((core::RCTable *)table_ptr)->End();
      } catch (common::Exception const &e) {
        rc_control_ << system::lock << "Error in push-down execution, push-down execution aborted: " << e.what()
                    << system::unlock;
        TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught in push-down execution: %s", e.what());
      }
      m_query.reset();
      m_cq.reset();
    } else {
      if (scan && filter_ptr.get()) {
        table_new_iter = ((core::RCTable *)table_ptr)->Begin(GetAttrsUseIndicator(table), *filter_ptr);
        table_new_iter_end = ((core::RCTable *)table_ptr)->End();
      } else {
        std::shared_ptr<core::RCTable> rctp;
        ha_rcengine_->GetTableIterator(m_table_name, table_new_iter, table_new_iter_end, rctp,
                                       GetAttrsUseIndicator(table), table->in_use);
        table_ptr = rctp.get();
        filter_ptr.reset();
      }
    }
    ret = 0;
    blob_buffers.resize(0);
    if (table_ptr != NULL) blob_buffers.resize(table_ptr->NumOfDisplaybleAttrs());
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

int TianmuHandler::rnd_end() {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  reset();
  DBUG_RETURN(0);
}

/*
 This is called for each row of the table scan. When you run out of records
 you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
 The Field structure for the table is the key to getting data into buf
 in a manner that will allow the server to understand it.

 Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
 sql_table.cc, and sql_update.cc.
 */
int TianmuHandler::rnd_next(uchar *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = HA_ERR_END_OF_FILE;
  try {
    table->set_found_row();  // stonedb8
    if (fill_row(buf) == HA_ERR_END_OF_FILE) {
      table->set_no_row();  // stonedb8
      DBUG_RETURN(ret);
    }
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

/*
 position() is called after each call to rnd_next() if the data needs
 to be ordered. You can do something like the following to store
 the position:
 my_store_ptr(ref, ref_length, current_position);

 The server uses ref to store data. ref_length in the above case is
 the size needed to store current_position. ref is just a byte array
 that the server will maintain. If you are using offsets to mark rows, then
 current_position should be the offset. If it is a primary key like in
 BDB, then it needs to be a primary key.

 Called from filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc.
 */
void TianmuHandler::position([[maybe_unused]] const uchar *record) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  my_store_ptr(ref, ref_length, current_position);

  DBUG_VOID_RETURN;
}

/*
 This is like rnd_next, but you are given a position to use
 to determine the row. The position will be of the type that you stored in
 ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
 or position you saved when position() was called.
 Called from filesort.cc records.cc sql_insert.cc sql_select.cc sql_update.cc.
 */
int TianmuHandler::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = HA_ERR_END_OF_FILE;
  try {
    uint64_t position = my_get_ptr(pos, ref_length);

    filter_ptr = std::make_unique<core::Filter>(position + 1, share->PackSizeShift());
    filter_ptr->Reset();
    filter_ptr->Set(position);

    auto tab_ptr = ha_rcengine_->GetTx(table->in_use)->GetTableByPath(m_table_name);
    table_new_iter = tab_ptr->Begin(GetAttrsUseIndicator(table), *filter_ptr);
    table_new_iter_end = tab_ptr->End();
    table_ptr = tab_ptr.get();

    table_new_iter.MoveToRow(position);
    table->set_found_row();  // stonedb8
    blob_buffers.resize(table->s->fields);
    if (fill_row(buf) == HA_ERR_END_OF_FILE) {
      table->set_no_row();  // stonedb8
      DBUG_RETURN(ret);
    }
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

/*
 extra() is called whenever the server wishes to send a hint to
 the storage engine. The myisam engine implements the most hints.
 ha_innodb.cc has the most exhaustive list of these hints.
 */
int TianmuHandler::extra([[maybe_unused]] enum ha_extra_function operation) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  /* This preemptive delete might cause problems here.
   * Other place where it can be put is TianmuHandler::external_lock().
   */
  // stonedb8 TODO: HA_EXTRA_NO_CACHE is deleted
  /*
  if (operation == HA_EXTRA_NO_CACHE) {
    m_cq.reset();
    m_query.reset();
  }
  */
  DBUG_RETURN(0);
}

int TianmuHandler::start_stmt(THD *thd, thr_lock_type lock_type) {
  try {
    if (lock_type == TL_WRITE_CONCURRENT_INSERT || lock_type == TL_WRITE_DEFAULT || lock_type == TL_WRITE) {
      trans_register_ha(thd, false, tianmu_hton, NULL);
      if (thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
        trans_register_ha(thd, true, tianmu_hton, NULL);
      }
      current_txn_ = ha_rcengine_->GetTx(thd);
      current_txn_->AddTableWRIfNeeded(share);
    }
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
  return 0;
}

/*
 Ask rcbase handler about permission to cache table during query registration.
 If current thread is in non-autocommit, we don't permit any mysql query
 caching.
 */
// stonedb8 TODO: register_query_cache_table is deleted
/*
bool TianmuHandler::register_query_cache_table(THD *thd, char *table_key, size_t key_length,
                                                   qc_engine_callback *call_back,
                                                   [[maybe_unused]] ulonglong *engine_data) {
  *call_back = rcbase_query_caching_of_table_permitted;
  return rcbase_query_caching_of_table_permitted(thd, table_key, key_length, 0);
}
*/

/*
 Used to delete a table. By the time delete_table() has been called all
 opened references to this table will have been closed (and your globally
 shared references released. The variable name will just be the name of
 the table. You will need to remove any files you have created at this point.

 If you do not implement this, the default delete_table() is called from
 handler.cc and it will delete all files with the file extentions returned
 by bas_ext(). // stonedb8 TODO bas_ext() has been deleted in mysql8.0

 Called from handler.cc by delete_table and  ha_create_table(). Only used
 during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
 the storage engine.
 */
int TianmuHandler::delete_table(const char *name, const dd::Table *table_def) {  // stonedb8 TODO
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int ret = 1;
  try {
    ha_rcengine_->DeleteTable(name, table_def, ha_thd());
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

/*
 Given a starting key, and an ending key estimate the number of rows that
 will exist between the two. end_key may be empty which in case determine
 if start_key matches any rows.

 Called from opt_range.cc by check_quick_keys().
 */
ha_rows TianmuHandler::records_in_range([[maybe_unused]] uint inx, [[maybe_unused]] key_range *min_key,
                                        [[maybe_unused]] key_range *max_key) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  DBUG_RETURN(10);  // low number to force index usage
}

/*
 create() is called to create a database. The variable name will have the name
 of the table. When create() is called you do not need to worry about opening
 the table. Also, the FRM file will have already been created so adjusting
 create_info will not do you any good. You can overwrite the frm file at this
 point if you wish to change the table definition, but there are no methods
 currently provided for doing that.

 Called from handle.cc by ha_create_table().
 */
int TianmuHandler::create(const char *name, TABLE *table_arg, [[maybe_unused]] HA_CREATE_INFO *info,
                          dd::Table *table_def) {  // stonedb8 TODO
  DBUG_ENTER(__PRETTY_FUNCTION__);
  try {
    ha_rcengine_->CreateTable(name, table_arg, table_def);
    DBUG_RETURN(0);
  } catch (common::AutoIncException &e) {
    my_message(ER_WRONG_AUTO_KEY, e.what(), MYF(0));
  } catch (common::UnsupportedDataTypeException &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
  } catch (fs::filesystem_error &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "filesystem_error on table creation '%s'", e.what());
    fs::remove_all(std::string(name) + common::TIANMU_EXT);
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(1);
}

int TianmuHandler::truncate(dd::Table *table_def) {  // stonedb8 TODO
  int ret = 0;
  try {
    ha_rcengine_->TruncateTable(m_table_name, table_def, ha_thd());
  } catch (std::exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
    ret = 1;
  } catch (...) {
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
    ret = 1;
  }

  return ret;
}

int TianmuHandler::fill_row(uchar *buf) {
  if (table_new_iter == table_new_iter_end) return HA_ERR_END_OF_FILE;

  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  std::shared_ptr<char[]> buffer;

  // we should pack the row into `buf` but seems it just use record[0] blindly.
  // So this is a workaround to handle the case that `buf` is not record[0].
  if (buf != table->record[0]) {
    buffer.reset(new char[table->s->reclength]);
    std::memcpy(buffer.get(), table->record[0], table->s->reclength);
  }

  for (uint col_id = 0; col_id < table->s->fields; col_id++)
    core::Engine::ConvertToField(table->field[col_id], *(table_new_iter.GetData(col_id)), &blob_buffers[col_id]);

  if (buf != table->record[0]) {
    std::memcpy(buf, table->record[0], table->s->reclength);
    std::memcpy(table->record[0], buffer.get(), table->s->reclength);
  }

  current_position = table_new_iter.GetCurrentRowId();
  table_new_iter++;

  dbug_tmp_restore_column_map(table->write_set, org_bitmap);

  return 0;
}

char *TianmuHandler::update_table_comment(const char *comment) {
  char *ret = const_cast<char *>(comment);
  try {
    uint length = (uint)std::strlen(comment);
    char *str = NULL;
    uint extra_len = 0;

    if (length > 64000 - 3) {
      return (const_cast<char *>(comment));  // string too long
    }

    //  get size & ratio
    int64_t sum_c = 0, sum_u = 0;
    std::vector<core::AttrInfo> attr_info = ha_rcengine_->GetTableAttributesInfo(m_table_name, table_share);
    for (uint j = 0; j < attr_info.size(); j++) {
      sum_c += attr_info[j].comp_size;
      sum_u += attr_info[j].uncomp_size;
    }
    char buf[256];
    double ratio = (sum_c > 0 ? double(sum_u) / double(sum_c) : 0);
    int count = std::sprintf(buf, "Overall compression ratio: %.3f, Raw size=%ld MB", ratio, sum_u >> 20);
    extra_len += count;

    str = (char *)my_malloc(PSI_NOT_INSTRUMENTED, length + extra_len + 3, MYF(0));
    if (str) {
      char *pos = str + length;
      if (length) {
        std::memcpy(str, comment, length);
        *pos++ = ';';
        *pos++ = ' ';
      }

      std::memcpy(pos, buf, extra_len);
      pos[extra_len] = 0;
    }
    ret = str;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  return ret;
}

bool TianmuHandler::explain_message(const Item *a_cond, String *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  if (current_txn_->Explain()) {
    cond_push(a_cond);
    std::string str = current_txn_->GetExplainMsg();
    buf->append(str.c_str(), str.length());
  }
  DBUG_RETURN(true);
}

int TianmuHandler::set_cond_iter() {
  int ret = 1;
  if (m_query && !m_result && table_ptr->NumOfObj() != 0) {
    m_cq->Result(m_tmp_table);  // it is ALWAYS -2 though....
    m_result = true;

    try {
      core::FunctionExecutor fe(std::bind(&core::Query::LockPackInfoForUse, std::ref(m_query)),
                                std::bind(&core::Query::UnlockPackInfoFromUse, std::ref(m_query)));

      core::TempTable *push_down_result = m_query->Preexecute(*m_cq, NULL, false);
      if (!push_down_result || push_down_result->NumOfTables() != 1)
        throw common::InternalException("core::Query execution returned no result object");

      core::Filter *filter(push_down_result->GetMultiIndexP()->GetFilter(0));
      if (!filter)
        filter_ptr.reset(new core::Filter(push_down_result->GetMultiIndexP()->OrigSize(0), table_ptr->Getpackpower()));
      else
        filter_ptr.reset(new core::Filter(*filter));

      table_ptr = push_down_result->GetTableP(0);
      table_new_iter = ((core::RCTable *)table_ptr)->Begin(GetAttrsUseIndicator(table), *filter);
      table_new_iter_end = ((core::RCTable *)table_ptr)->End();
      ret = 0;
    } catch (common::Exception const &e) {
      rc_control_ << system::lock << "Error in push-down execution, push-down execution aborted: " << e.what()
                  << system::unlock;
      TIANMU_LOG(LogCtl_Level::ERROR, "Error in push-down execution, push-down execution aborted: %s", e.what());
    }
    m_query.reset();
    m_cq.reset();
  }
  return ret;
}

const Item *TianmuHandler::cond_push(const Item *a_cond) {
  Item const *ret = a_cond;
  Item *cond = const_cast<Item *>(a_cond);

  try {
    if (!m_query) {
      std::shared_ptr<core::RCTable> rctp;
      ha_rcengine_->GetTableIterator(m_table_name, table_new_iter, table_new_iter_end, rctp,
                                     GetAttrsUseIndicator(table), table->in_use);
      table_ptr = rctp.get();
      m_query.reset(new core::Query(current_txn_));
      m_cq.reset(new core::CompiledQuery);
      m_result = false;

      m_query->AddTable(rctp);
      core::TabID t_out;
      m_cq->TableAlias(t_out,
                       core::TabID(0));  // we apply it to the only table in this query
      m_cq->TmpTable(m_tmp_table, t_out);

      std::string ext_alias;
      if (table->pos_in_table_list->referencing_view)
        ext_alias = std::string(table->pos_in_table_list->referencing_view->table_name);
      else
        ext_alias = std::string(table->s->table_name.str);
      ext_alias += std::string(":") + std::string(table->alias);
      m_query->table_alias2index_ptr.insert(std::make_pair(ext_alias, std::make_pair(t_out.n, table)));

      int col_no = 0;
      core::AttrID col, vc;
      core::TabID tab(m_tmp_table);

      my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
      for (Field **field = table->field; *field; field++) {
        core::AttrID at;
        if (bitmap_is_set(table->read_set, col_no)) {
          col.n = col_no++;
          m_cq->CreateVirtualColumn(vc.n, m_tmp_table, t_out, col);
          m_cq->AddColumn(at, m_tmp_table, core::CQTerm(vc.n), common::ColOperation::LISTING, (*field)->field_name,
                          false);
        }
      }
      dbug_tmp_restore_column_map(table->read_set, org_bitmap);
    }

    if (m_result)
      return a_cond;  // if m_result there is already a result command in
                      // compilation

    std::unique_ptr<core::CompiledQuery> tmp_cq(new core::CompiledQuery(*m_cq));
    core::CondID cond_id;
    if (m_query->BuildConditions(cond, cond_id, tmp_cq.get(), m_tmp_table, core::CondType::WHERE_COND, false) ==
        Query_route_to::TO_MYSQL) {
      m_query.reset();
      return a_cond;
    }

    tmp_cq->AddConds(m_tmp_table, cond_id, core::CondType::WHERE_COND);
    tmp_cq->ApplyConds(m_tmp_table);
    m_cq.reset(tmp_cq.release());
    // reset  table_new_iter with push condition
    set_cond_iter();
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
  return ret;
}

int TianmuHandler::reset() {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = 1;
  try {
    table_new_iter = core::RCTable::Iterator();
    table_new_iter_end = core::RCTable::Iterator();
    table_ptr = NULL;
    filter_ptr.reset();
    m_query.reset();
    m_cq.reset();
    m_result = false;
    blob_buffers.resize(0);
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

enum_alter_inplace_result TianmuHandler::check_if_supported_inplace_alter([[maybe_unused]] TABLE *altered_table,
                                                                          Alter_inplace_info *ha_alter_info) {
  if ((ha_alter_info->handler_flags & ~TIANMU_SUPPORTED_ALTER_ADD_DROP_ORDER) &&
      (ha_alter_info->handler_flags != TIANMU_SUPPORTED_ALTER_COLUMN_NAME)) {
    return HA_ALTER_ERROR;
  }
  return HA_ALTER_INPLACE_EXCLUSIVE_LOCK;
}

bool TianmuHandler::inplace_alter_table(TABLE *altered_table [[maybe_unused]],
                                        Alter_inplace_info *ha_alter_info [[maybe_unused]],
                                        const dd::Table *old_table_def [[maybe_unused]],
                                        dd::Table *new_table_def [[maybe_unused]]) {  // stonedb8 TODO
  try {
    if (!(ha_alter_info->handler_flags & ~TIANMU_SUPPORTED_ALTER_ADD_DROP_ORDER)) {
      std::vector<Field *> v_old(table_share->field, table_share->field + table_share->fields);
      std::vector<Field *> v_new(altered_table->s->field, altered_table->s->field + altered_table->s->fields);
      ha_rcengine_->PrepareAlterTable(m_table_name, v_new, v_old, ha_thd());
      return false;
    } else if (ha_alter_info->handler_flags == TIANMU_SUPPORTED_ALTER_COLUMN_NAME) {
      return false;
    }
  } catch (std::exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "Unable to inplace alter table", MYF(0));

  return true;
}

bool TianmuHandler::commit_inplace_alter_table(TABLE *altered_table [[maybe_unused]],
                                               Alter_inplace_info *ha_alter_info [[maybe_unused]],
                                               bool commit [[maybe_unused]],
                                               const dd::Table *old_table_def [[maybe_unused]],
                                               dd::Table *new_table_def [[maybe_unused]]) {  // stonedb8 TODO
  if (!commit) {
    TIANMU_LOG(LogCtl_Level::INFO, "Alter table failed : %s%s", m_table_name.c_str(), " rollback");
    return true;
  }
  if (ha_alter_info->handler_flags == TIANMU_SUPPORTED_ALTER_COLUMN_NAME) {
    return false;
  }
  if ((ha_alter_info->handler_flags & ~TIANMU_SUPPORTED_ALTER_ADD_DROP_ORDER)) {
    TIANMU_LOG(LogCtl_Level::INFO, "Altered table not support type %llu", ha_alter_info->handler_flags);
    return true;
  }
  fs::path tmp_dir(m_table_name + ".tmp");
  fs::path tab_dir(m_table_name + common::TIANMU_EXT);
  fs::path bak_dir(m_table_name + ".backup");

  try {
    fs::rename(tab_dir, bak_dir);
    fs::rename(tmp_dir, tab_dir);

    // now we are safe to clean up
    std::unordered_set<std::string> s;
    for (auto &it : fs::directory_iterator(tab_dir / common::COLUMN_DIR)) {
      auto target = fs::read_symlink(it.path()).string();
      if (!target.empty()) s.insert(target);
    }

    for (auto &it : fs::directory_iterator(bak_dir / common::COLUMN_DIR)) {
      auto target = fs::read_symlink(it.path()).string();
      if (target.empty()) continue;
      auto search = s.find(target);
      if (search == s.end()) {
        fs::remove_all(target);
        TIANMU_LOG(LogCtl_Level::INFO, "removing %s", target.c_str());
      }
    }
    fs::remove_all(bak_dir);
  } catch (fs::filesystem_error &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "file system error: %s %s|%s", e.what(), e.path1().string().c_str(),
               e.path2().string().c_str());
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "Failed to commit alter table", MYF(0));
    return true;
  }
  return false;
}
/*
 key: mysql format, may be union key, need changed to kvstore key format

 */
void TianmuHandler::key_convert(const uchar *key, uint key_len, std::vector<uint> cols,
                                std::vector<std::string_view> &keys) {
  key_restore(table->record[0], const_cast<uchar *>(key), &table->key_info[active_index], key_len);

  Field **field = table->field;
  std::vector<std::string> records;
  for (auto &i : cols) {
    Field *f = field[i];
    // stonedb8 init for union primary key
    bitmap_set_bit(table->read_set, f->field_index());
    size_t length;
    if (f->is_null()) {
      throw common::Exception("Priamry key part can not be NULL");
    }
    if (f->is_flag_set(BLOB_FLAG))  // stonedb8
      length = dynamic_cast<Field_blob *>(f)->get_length();
    else
      length = f->row_pack_length();

    std::unique_ptr<char[]> buf(new char[length]);
    char *ptr = buf.get();

    switch (f->type()) {
      case MYSQL_TYPE_TINY: {
        int64_t v = f->val_int();
        if (v > TIANMU_TINYINT_MAX)
          v = TIANMU_TINYINT_MAX;
        else if (v < TIANMU_TINYINT_MIN)
          v = TIANMU_TINYINT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_SHORT: {
        int64_t v = f->val_int();
        if (v > TIANMU_SMALLINT_MAX)
          v = TIANMU_SMALLINT_MAX;
        else if (v < TIANMU_SMALLINT_MIN)
          v = TIANMU_SMALLINT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_LONG: {
        int64_t v = f->val_int();
        if (v > std::numeric_limits<int>::max())
          v = std::numeric_limits<int>::max();
        else if (v < TIANMU_INT_MIN)
          v = TIANMU_INT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_INT24: {
        int64_t v = f->val_int();
        if (v > TIANMU_MEDIUMINT_MAX)
          v = TIANMU_MEDIUMINT_MAX;
        else if (v < TIANMU_MEDIUMINT_MIN)
          v = TIANMU_MEDIUMINT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_LONGLONG: {
        int64_t v = f->val_int();
        if (v > common::TIANMU_BIGINT_MAX)
          v = common::TIANMU_BIGINT_MAX;
        else if (v < common::TIANMU_BIGINT_MIN)
          v = common::TIANMU_BIGINT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE: {
        double v = f->val_real();
        *(int64_t *)ptr = *reinterpret_cast<int64_t *>(&v);
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_NEWDECIMAL: {
        auto dec_f = dynamic_cast<Field_new_decimal *>(f);
        *(int64_t *)ptr = std::lround(dec_f->val_real() * types::PowOfTen(dec_f->dec));
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_TIME2:
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_NEWDATE:
      case MYSQL_TYPE_TIMESTAMP2:
      case MYSQL_TYPE_DATETIME2: {
        MYSQL_TIME my_time;
        std::memset(&my_time, 0, sizeof(my_time));
        f->get_time(&my_time);
        types::DT dt(my_time);
        *(int64_t *)ptr = dt.val;
        ptr += sizeof(int64_t);
      } break;

      case MYSQL_TYPE_TIMESTAMP: {
        MYSQL_TIME my_time;
        std::memset(&my_time, 0, sizeof(my_time));
        f->get_time(&my_time);
        auto saved = my_time.second_part;
        // convert to UTC
        if (!common::IsTimeStampZero(my_time)) {
          bool myb;
          my_time_t secs_utc = current_thd->variables.time_zone->TIME_to_gmt_sec(&my_time, &myb);
          common::GMTSec2GMTTime(&my_time, secs_utc);
        }
        my_time.second_part = saved;
        types::DT dt(my_time);
        *(int64_t *)ptr = dt.val;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_YEAR:  // what the hell?
      {
        types::DT dt = {};
        dt.year = f->val_int();
        *(int64_t *)ptr = dt.val;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_STRING: {
        String str;
        f->val_str(&str);
        *(uint32_t *)ptr = str.length();
        ptr += sizeof(uint32_t);
        std::memcpy(ptr, str.ptr(), str.length());
        ptr += str.length();
      } break;
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_GEOMETRY:
      case MYSQL_TYPE_NULL:
      case MYSQL_TYPE_BIT:
      default:
        throw common::Exception("unsupported mysql type " + std::to_string(f->type()));
        break;
    }
    records.emplace_back((const char *)buf.get(), ptr - buf.get());
  }

  for (auto &elem : records) {
    keys.emplace_back(elem.data(), elem.size());
  }
}

static int rcbase_done_func([[maybe_unused]] void *p) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  if (ha_rcengine_) {
    delete ha_rcengine_;
    ha_rcengine_ = nullptr;
  }
  if (ha_kvstore_) {
    delete ha_kvstore_;
    ha_kvstore_ = nullptr;
  }

  DBUG_RETURN(0);
}

handler *rcbase_create_handler(handlerton *hton, TABLE_SHARE *table, bool partitioned, MEM_ROOT *mem_root) {
  return new (mem_root) TianmuHandler(hton, table, partitioned);
}

int rcbase_panic_func([[maybe_unused]] handlerton *hton, enum ha_panic_function flag) {
  if (flag == HA_PANIC_CLOSE) {
    delete ha_rcengine_;
    ha_rcengine_ = nullptr;
    delete ha_kvstore_;
    ha_kvstore_ = nullptr;
  }
  return 0;
}

int rcbase_rollback([[maybe_unused]] handlerton *hton, THD *thd, bool all) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = 1;
  try {
    ha_rcengine_->Rollback(thd, all);
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception error is caught: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

int rcbase_close_connection(handlerton *hton, THD *thd) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = 1;
  try {
    rcbase_rollback(hton, thd, true);
    ha_rcengine_->ClearTx(thd);
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception error is caught: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

int rcbase_commit([[maybe_unused]] handlerton *hton, THD *thd, bool all) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = 1;
  std::string error_message;

  if (!(thd->is_error() || thd->killed || thd->transaction_rollback_request)) {
    try {
      ha_rcengine_->CommitTx(thd, all);
      ret = 0;
    } catch (std::exception &e) {
      error_message = std::string("Error: ") + e.what();
    } catch (...) {
      error_message = std::string(__func__) + " An unknown system exception error caught.";
    }
  }

  if (ret) {
    try {
      ha_rcengine_->Rollback(thd, all, true);
      if (!error_message.empty()) {
        TIANMU_LOG(LogCtl_Level::ERROR, "%s", error_message.c_str());
        my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), error_message.c_str(), MYF(0));
      }
    } catch (std::exception &e) {
      if (!error_message.empty()) {
        TIANMU_LOG(LogCtl_Level::ERROR, "%s", error_message.c_str());
        my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), error_message.c_str(), MYF(0));
      }
      my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR),
                 (std::string("Failed to rollback transaction. Error: ") + e.what()).c_str(), MYF(0));
      TIANMU_LOG(LogCtl_Level::ERROR, "Failed to rollback transaction. Error: %s.", e.what());
    } catch (...) {
      if (!error_message.empty()) {
        TIANMU_LOG(LogCtl_Level::ERROR, "%s", error_message.c_str());
        my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), error_message.c_str(), MYF(0));
      }
      my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "Failed to rollback transaction. Unknown error.",
                 MYF(0));
      TIANMU_LOG(LogCtl_Level::ERROR, "Failed to rollback transaction. Unknown error.");
    }
  }

  DBUG_RETURN(ret);
}

bool rcbase_show_status([[maybe_unused]] handlerton *hton, THD *thd, stat_print_fn *pprint, enum ha_stat_type stat) {
  const static char *hton_name = "TIANMU";

  std::ostringstream buf(std::ostringstream::out);

  buf << std::endl;

  if (stat == HA_ENGINE_STATUS) {
    mm::TraceableObject::Instance()->HeapHistogram(buf);
    return pprint(thd, hton_name, uint(std::strlen(hton_name)), "Heap Histograms", uint(std::strlen("Heap Histograms")),
                  buf.str().c_str(), uint(buf.str().length()));
  }
  return false;
}

static int init_variables() {
  opt_binlog_order_commits = false;
  return 0;
}

// Gives the file extension of an Tianmu single-table tablespace.
static const char *ha_tianmu_exts[] = {common::TIANMU_EXT, NullS};

int rcbase_init_func(void *p) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  // move hton here, just like other storate like innodb, myisam, csv... does.
  // handlerton is a singleton structure - one instance per storage engine.
  // defined in sql/handler.h
  handlerton *tianmu_hton;
  tianmu_hton = (handlerton *)p;

  if (init_variables()) {
    DBUG_RETURN(1);
  }

  tianmu_hton->state = SHOW_OPTION_YES;
  tianmu_hton->db_type = DB_TYPE_TIANMU;
  tianmu_hton->create = rcbase_create_handler;
  tianmu_hton->flags = HTON_NO_FLAGS;
  tianmu_hton->panic = rcbase_panic_func;
  tianmu_hton->close_connection = rcbase_close_connection;
  tianmu_hton->commit = rcbase_commit;
  tianmu_hton->rollback = rcbase_rollback;
  tianmu_hton->show_status = rcbase_show_status;
  tianmu_hton->file_extensions = ha_tianmu_exts;

  int ret = 1;
  ha_rcengine_ = nullptr;

  try {
    std::string log_file = mysql_home_ptr;
    log_setup(log_file + "/log/tianmu.log");
    rc_control_.addOutput(new system::FileOut(log_file + "/log/trace.log"));
    rc_querylog_.addOutput(new system::FileOut(log_file + "/log/query.log"));
    struct hostent *hent = nullptr;
    hent = gethostbyname(glob_hostname);
    if (hent) strmov_str(global_hostIP_, inet_ntoa(*(struct in_addr *)(hent->h_addr_list[0])));
    snprintf(global_serverinfo_, sizeof(global_serverinfo_), "\tServerIp:%s\tServerHostName:%s\tServerPort:%d",
             global_hostIP_, glob_hostname, mysqld_port);
    ha_kvstore_ = new index::KVStore();
    ha_kvstore_->Init();
    ha_rcengine_ = new core::Engine();
    ret = ha_rcengine_->Init(tianmu_hton->slot);
    {
      TIANMU_LOG(LogCtl_Level::INFO,
                 "\n"
                 "------------------------------------------------------------"
                 "----------------------------------"
                 "-------------\n"
                 "    ######  ########  #######  ##     ## ######## ######   "
                 "######   \n"
                 "   ##    ##    ##    ##     ## ####   ## ##       ##    ## "
                 "##    ## \n"
                 "   ##          ##    ##     ## ## ##  ## ##       ##    ## "
                 "##    ## \n"
                 "    ######     ##    ##     ## ##  ## ## ######   ##    ## "
                 "######## \n"
                 "         ##    ##    ##     ## ##   #### ##       ##    ## "
                 "##    ## \n"
                 "   ##    ##    ##    ##     ## ##    ### ##       ##    ## "
                 "##    ## \n"
                 "    ######     ##     #######  ##     ## ######## ######   "
                 "######   \n"
                 "------------------------------------------------------------"
                 "----------------------------------"
                 "-------------\n");
    }

  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception error is caught: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

struct st_mysql_storage_engine tianmu_storage_engine = {MYSQL_HANDLERTON_INTERFACE_VERSION};

int get_DelayedBufferUsage_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_CHAR;
  var->value = buff;
  std::string str = ha_rcengine_->DelayedBufferStat();
  std::memcpy(buff, str.c_str(), str.length() + 1);
  return 0;
}

int get_RowStoreUsage_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_CHAR;
  var->value = buff;
  std::string str = ha_rcengine_->RowStoreStat();
  std::memcpy(buff, str.c_str(), str.length() + 1);
  return 0;
}

int get_InsertPerMinute_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = ha_rcengine_->GetIPM();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_QueryPerMinute_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = ha_rcengine_->GetQPM();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_LoadPerMinute_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = ha_rcengine_->GetLPM();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_Freeable_StatusVar([[maybe_unused]] MYSQL_THD thd, struct SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = mm::TraceableObject::GetFreeableSize();
  outvar->value = tmp;
  outvar->type = SHOW_LONGLONG;
  return 0;
}

int get_UnFreeable_StatusVar([[maybe_unused]] MYSQL_THD thd, struct SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = mm::TraceableObject::GetUnFreeableSize();
  outvar->value = tmp;
  outvar->type = SHOW_LONGLONG;
  return 0;
}

int get_MemoryScale_StatusVar([[maybe_unused]] MYSQL_THD thd, struct SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = mm::TraceableObject::MemorySettingsScale();
  outvar->value = tmp;
  outvar->type = SHOW_LONGLONG;
  return 0;
}

int get_InsertTotal_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = ha_rcengine_->GetIT();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_QueryTotal_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = ha_rcengine_->GetQT();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_LoadTotal_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = ha_rcengine_->GetLT();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_LoadDupTotal_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = ha_rcengine_->GetLDT();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_LoadDupPerMinute_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = ha_rcengine_->GetLDPM();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_UpdateTotal_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = ha_rcengine_->GetUT();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_UpdatePerMinute_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = ha_rcengine_->GetUPM();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

char masteslave_info[8192];

SHOW_VAR tianmu_masterslave_dump[] = {{"info", masteslave_info, SHOW_CHAR, SHOW_SCOPE_UNDEF},
                                      {NullS, NullS, SHOW_LONG, SHOW_SCOPE_UNDEF}};

//  showtype
//  =====================
//  SHOW_UNDEF, SHOW_BOOL, SHOW_INT, SHOW_LONG,
//  SHOW_LONGLONG, SHOW_CHAR, SHOW_CHAR_PTR,
//  SHOW_ARRAY, SHOW_FUNC, SHOW_DOUBLE

int tianmu_throw_error_func([[maybe_unused]] MYSQL_THD thd, [[maybe_unused]] struct SYS_VAR *var,
                            [[maybe_unused]] void *save, struct st_mysql_value *value) {
  int buffer_length = 512;
  char buff[512] = {0};

  DEBUG_ASSERT(value->value_type(value) == MYSQL_VALUE_TYPE_STRING);

  my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), value->val_str(value, buff, &buffer_length), MYF(0));
  return -1;
}

static void update_func_str([[maybe_unused]] THD *thd, struct SYS_VAR *var, void *tgt, const void *save) {
  char *old = *(char **)tgt;
  *(char **)tgt = *reinterpret_cast<char **>(const_cast<void *>(save));
  if (var->flags & PLUGIN_VAR_MEMALLOC) {
    *(char **)tgt = my_strdup(PSI_NOT_INSTRUMENTED, *reinterpret_cast<char **>(const_cast<void *>(save)), MYF(0));
    my_free(old);
  }
}

void refresh_sys_table_func([[maybe_unused]] MYSQL_THD thd, [[maybe_unused]] struct SYS_VAR *var, void *tgt,
                            const void *save) {
  *(bool *)tgt = *reinterpret_cast<bool *>(const_cast<void *>(save)) ? true : false;
}

// Tianmu engine used only
void debug_update(MYSQL_THD thd, struct SYS_VAR *var, void *var_ptr, const void *save);
void trace_update(MYSQL_THD thd, struct SYS_VAR *var, void *var_ptr, const void *save);
void controlquerylog_update(MYSQL_THD thd, struct SYS_VAR *var, void *var_ptr, const void *save);
void start_async_update(MYSQL_THD thd, struct SYS_VAR *var, void *var_ptr, const void *save);
extern void async_join_update(MYSQL_THD thd, struct SYS_VAR *var, void *var_ptr, const void *save);
// Tianmu engine used only end

#define STATUS_FUNCTION(name, showtype, member)                                                    \
  int get_##name##_StatusVar([[maybe_unused]] MYSQL_THD thd, struct SHOW_VAR *outvar, char *tmp) { \
    *((int64_t *)tmp) = ha_rcengine_->cache.member();                                              \
    outvar->value = tmp;                                                                           \
    outvar->type = showtype;                                                                       \
    return 0;                                                                                      \
  }

#define MM_STATUS_FUNCTION(name, showtype, member)                                                 \
  int get_##name##_StatusVar([[maybe_unused]] MYSQL_THD thd, struct SHOW_VAR *outvar, char *tmp) { \
    *((int64_t *)tmp) = mm::TraceableObject::Instance()->member();                                 \
    outvar->value = tmp;                                                                           \
    outvar->type = showtype;                                                                       \
    return 0;                                                                                      \
  }

#define STATUS_MEMBER(name, label) \
  { "Tianmu_" #label, (char *)get_##name##_StatusVar, SHOW_FUNC, SHOW_SCOPE_GLOBAL }

STATUS_FUNCTION(gdchits, SHOW_LONGLONG, getCacheHits)
STATUS_FUNCTION(gdcmisses, SHOW_LONGLONG, getCacheMisses)
STATUS_FUNCTION(gdcreleased, SHOW_LONGLONG, getReleased)
STATUS_FUNCTION(gdcreadwait, SHOW_LONGLONG, getReadWait)
STATUS_FUNCTION(gdcfalsewakeup, SHOW_LONGLONG, getFalseWakeup)
STATUS_FUNCTION(gdcreadwaitinprogress, SHOW_LONGLONG, getReadWaitInProgress)
STATUS_FUNCTION(gdcpackloads, SHOW_LONGLONG, getPackLoads)
STATUS_FUNCTION(gdcloaderrors, SHOW_LONGLONG, getLoadErrors)
STATUS_FUNCTION(gdcredecompress, SHOW_LONGLONG, getReDecompress)

MM_STATUS_FUNCTION(mmallocblocks, SHOW_LONGLONG, getAllocBlocks)
MM_STATUS_FUNCTION(mmallocobjs, SHOW_LONGLONG, getAllocObjs)
MM_STATUS_FUNCTION(mmallocsize, SHOW_LONGLONG, getAllocSize)
MM_STATUS_FUNCTION(mmallocpack, SHOW_LONGLONG, getAllocPack)
MM_STATUS_FUNCTION(mmalloctemp, SHOW_LONGLONG, getAllocTemp)
MM_STATUS_FUNCTION(mmalloctempsize, SHOW_LONGLONG, getAllocTempSize)
MM_STATUS_FUNCTION(mmallocpacksize, SHOW_LONGLONG, getAllocPackSize)
MM_STATUS_FUNCTION(mmfreeblocks, SHOW_LONGLONG, getFreeBlocks)
MM_STATUS_FUNCTION(mmfreepacks, SHOW_LONGLONG, getFreePacks)
MM_STATUS_FUNCTION(mmfreetemp, SHOW_LONGLONG, getFreeTemp)
MM_STATUS_FUNCTION(mmfreepacksize, SHOW_LONGLONG, getFreePackSize)
MM_STATUS_FUNCTION(mmfreetempsize, SHOW_LONGLONG, getFreeTempSize)
MM_STATUS_FUNCTION(mmfreesize, SHOW_LONGLONG, getFreeSize)
MM_STATUS_FUNCTION(mmrelease1, SHOW_LONGLONG, getReleaseCount1)
MM_STATUS_FUNCTION(mmrelease2, SHOW_LONGLONG, getReleaseCount2)
MM_STATUS_FUNCTION(mmrelease3, SHOW_LONGLONG, getReleaseCount3)
MM_STATUS_FUNCTION(mmrelease4, SHOW_LONGLONG, getReleaseCount4)
MM_STATUS_FUNCTION(mmreloaded, SHOW_LONGLONG, getReloaded)
MM_STATUS_FUNCTION(mmreleasecount, SHOW_LONGLONG, getReleaseCount)
MM_STATUS_FUNCTION(mmreleasetotal, SHOW_LONGLONG, getReleaseTotal)

static struct SHOW_VAR statusvars[] = {
    STATUS_MEMBER(gdchits, gdc_hits),
    STATUS_MEMBER(gdcmisses, gdc_misses),
    STATUS_MEMBER(gdcreleased, gdc_released),
    STATUS_MEMBER(gdcreadwait, gdc_readwait),
    STATUS_MEMBER(gdcfalsewakeup, gdc_false_wakeup),
    STATUS_MEMBER(gdcreadwaitinprogress, gdc_read_wait_in_progress),
    STATUS_MEMBER(gdcpackloads, gdc_pack_loads),
    STATUS_MEMBER(gdcloaderrors, gdc_load_errors),
    STATUS_MEMBER(gdcredecompress, gdc_redecompress),
    STATUS_MEMBER(mmrelease1, mm_release1),
    STATUS_MEMBER(mmrelease2, mm_release2),
    STATUS_MEMBER(mmrelease3, mm_release3),
    STATUS_MEMBER(mmrelease4, mm_release4),
    STATUS_MEMBER(mmallocblocks, mm_alloc_blocs),
    STATUS_MEMBER(mmallocobjs, mm_alloc_objs),
    STATUS_MEMBER(mmallocsize, mm_alloc_size),
    STATUS_MEMBER(mmallocpack, mm_alloc_packs),
    STATUS_MEMBER(mmalloctemp, mm_alloc_temp),
    STATUS_MEMBER(mmalloctempsize, mm_alloc_temp_size),
    STATUS_MEMBER(mmallocpacksize, mm_alloc_pack_size),
    STATUS_MEMBER(mmfreeblocks, mm_free_blocks),
    STATUS_MEMBER(mmfreepacks, mm_free_packs),
    STATUS_MEMBER(mmfreetemp, mm_free_temp),
    STATUS_MEMBER(mmfreepacksize, mm_free_pack_size),
    STATUS_MEMBER(mmfreetempsize, mm_free_temp_size),
    STATUS_MEMBER(mmfreesize, mm_free_size),
    STATUS_MEMBER(mmreloaded, mm_reloaded),
    STATUS_MEMBER(mmreleasecount, mm_release_count),
    STATUS_MEMBER(mmreleasetotal, mm_release_total),
    STATUS_MEMBER(DelayedBufferUsage, delay_buffer_usage),
    STATUS_MEMBER(RowStoreUsage, row_store_usage),
    STATUS_MEMBER(Freeable, mm_freeable),
    STATUS_MEMBER(InsertPerMinute, insert_per_minute),
    STATUS_MEMBER(LoadPerMinute, load_per_minute),
    STATUS_MEMBER(QueryPerMinute, query_per_minute),
    STATUS_MEMBER(MemoryScale, mm_scale),
    STATUS_MEMBER(UnFreeable, mm_unfreeable),
    STATUS_MEMBER(InsertTotal, insert_total),
    STATUS_MEMBER(LoadTotal, load_total),
    STATUS_MEMBER(QueryTotal, query_total),
    STATUS_MEMBER(LoadDupPerMinute, load_dup_per_minute),
    STATUS_MEMBER(LoadDupTotal, load_dup_total),
    STATUS_MEMBER(UpdatePerMinute, update_per_minute),
    STATUS_MEMBER(UpdateTotal, update_total),
    {0, 0, SHOW_UNDEF, SHOW_SCOPE_UNDEF},
};

static MYSQL_SYSVAR_BOOL(refresh_sys_tianmu, tianmu_sysvar_refresh_sys_table, PLUGIN_VAR_BOOL, "-", nullptr,
                         refresh_sys_table_func, true);
static MYSQL_THDVAR_STR(trigger_error, PLUGIN_VAR_STR | PLUGIN_VAR_THDLOCAL, "-", tianmu_throw_error_func,
                        update_func_str, nullptr);

static MYSQL_SYSVAR_INT(ini_allowmysqlquerypath, tianmu_sysvar_allowmysqlquerypath, PLUGIN_VAR_READONLY, "-", nullptr,
                        nullptr, 0, 0, 1, 0);
static MYSQL_SYSVAR_STR(ini_cachefolder, tianmu_sysvar_cachefolder, PLUGIN_VAR_READONLY, "-", nullptr, nullptr,
                        "cache");
static MYSQL_SYSVAR_INT(ini_knlevel, tianmu_sysvar_knlevel, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, 99, 0, 99, 0);
static MYSQL_SYSVAR_BOOL(ini_pushdown, tianmu_sysvar_pushdown, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, true);
static MYSQL_SYSVAR_INT(ini_servermainheapsize, tianmu_sysvar_servermainheapsize, PLUGIN_VAR_READONLY, "-", nullptr,
                        nullptr, 0, 0, 1000000, 0);
static MYSQL_SYSVAR_BOOL(ini_usemysqlimportexportdefaults, tianmu_sysvar_usemysqlimportexportdefaults,
                         PLUGIN_VAR_READONLY, "-", nullptr, nullptr, false);
static MYSQL_SYSVAR_INT(ini_threadpoolsize, tianmu_sysvar_threadpoolsize, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, 1,
                        0, 1000000, 0);
static MYSQL_SYSVAR_INT(ini_cachesizethreshold, tianmu_sysvar_cachesizethreshold, PLUGIN_VAR_INT, "-", nullptr, nullptr,
                        4, 0, 1024, 0);
static MYSQL_SYSVAR_INT(ini_cachereleasethreshold, tianmu_sysvar_cachereleasethreshold, PLUGIN_VAR_INT, "-", nullptr,
                        nullptr, 100, 0, 100000, 0);
static MYSQL_SYSVAR_BOOL(insert_delayed, tianmu_sysvar_insert_delayed, PLUGIN_VAR_READONLY, "-", nullptr, nullptr,
                         true);
static MYSQL_SYSVAR_INT(insert_cntthreshold, tianmu_sysvar_insert_cntthreshold, PLUGIN_VAR_READONLY, "-", nullptr,
                        nullptr, 2, 0, 1000, 0);
static MYSQL_SYSVAR_INT(insert_numthreshold, tianmu_sysvar_insert_numthreshold, PLUGIN_VAR_READONLY, "-", nullptr,
                        nullptr, 10000, 0, 100000, 0);
static MYSQL_SYSVAR_INT(insert_wait_ms, tianmu_sysvar_insert_wait_ms, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, 100,
                        10, 10000, 0);
static MYSQL_SYSVAR_INT(insert_wait_time, tianmu_sysvar_insert_wait_time, PLUGIN_VAR_INT, "-", nullptr, nullptr, 1000,
                        0, 600000, 0);
static MYSQL_SYSVAR_INT(insert_max_buffered, tianmu_sysvar_insert_max_buffered, PLUGIN_VAR_READONLY, "-", nullptr,
                        nullptr, 65536, 0, 10000000, 0);
static MYSQL_SYSVAR_BOOL(compensation_start, tianmu_sysvar_compensation_start, PLUGIN_VAR_BOOL, "-", nullptr, nullptr,
                         false);
static MYSQL_SYSVAR_STR(hugefiledir, tianmu_sysvar_hugefiledir, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, "");
static MYSQL_SYSVAR_INT(cachinglevel, tianmu_sysvar_cachinglevel, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, 1, 0, 512,
                        0);
static MYSQL_SYSVAR_STR(mm_policy, tianmu_sysvar_mm_policy, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, "");
static MYSQL_SYSVAR_INT(mm_hardlimit, tianmu_sysvar_mm_hardlimit, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, 0, 0, 1,
                        0);
static MYSQL_SYSVAR_STR(mm_releasepolicy, tianmu_sysvar_mm_releasepolicy, PLUGIN_VAR_READONLY, "-", nullptr, nullptr,
                        "2q");
static MYSQL_SYSVAR_INT(mm_largetempratio, tianmu_sysvar_mm_largetempratio, PLUGIN_VAR_READONLY, "-", nullptr, nullptr,
                        0, 0, 99, 0);
static MYSQL_SYSVAR_INT(mm_largetemppool_threshold, tianmu_sysvar_mm_large_threshold, PLUGIN_VAR_INT,
                        "size threshold in MB for using large temp thread pool", nullptr, nullptr, 16, 0, 10240, 0);
static MYSQL_SYSVAR_INT(sync_buffers, tianmu_sysvar_sync_buffers, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, 0, 0, 1,
                        0);

static MYSQL_SYSVAR_INT(query_threads, tianmu_sysvar_query_threads, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, 0, 0,
                        100, 0);
static MYSQL_SYSVAR_INT(load_threads, tianmu_sysvar_load_threads, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, 0, 0, 100,
                        0);
static MYSQL_SYSVAR_INT(bg_load_threads, tianmu_sysvar_bg_load_threads, PLUGIN_VAR_READONLY, "-", nullptr, nullptr, 0,
                        0, 100, 0);
static MYSQL_SYSVAR_INT(insert_buffer_size, tianmu_sysvar_insert_buffer_size, PLUGIN_VAR_READONLY, "-", nullptr,
                        nullptr, 512, 512, 10000, 0);

static MYSQL_THDVAR_INT(session_debug_level, PLUGIN_VAR_INT, "session debug level", nullptr, debug_update, 3, 0, 5, 0);
static MYSQL_THDVAR_INT(control_trace, PLUGIN_VAR_OPCMDARG, "ini controltrace", nullptr, trace_update, 0, 0, 100, 0);
static MYSQL_SYSVAR_INT(global_debug_level, tianmu_sysvar_global_debug_level, PLUGIN_VAR_INT, "global debug level",
                        nullptr, nullptr, 4, 0, 5, 0);

static MYSQL_SYSVAR_INT(distinct_cache_size, tianmu_sysvar_distcache_size, PLUGIN_VAR_INT,
                        "Upper byte limit for GroupDistinctCache buffer", nullptr, nullptr, 64, 64, 256, 0);
static MYSQL_SYSVAR_BOOL(filterevaluation_speedup, tianmu_sysvar_filterevaluation_speedup, PLUGIN_VAR_BOOL, "-",
                         nullptr, nullptr, true);
static MYSQL_SYSVAR_BOOL(groupby_speedup, tianmu_sysvar_groupby_speedup, PLUGIN_VAR_BOOL, "-", nullptr, nullptr, true);
static MYSQL_SYSVAR_BOOL(orderby_speedup, tianmu_sysvar_orderby_speedup, PLUGIN_VAR_BOOL, "-", nullptr, nullptr, false);
static MYSQL_SYSVAR_INT(join_parallel, tianmu_sysvar_join_parallel, PLUGIN_VAR_INT,
                        "join matching parallel: 0-Disabled, 1-Auto, N-specify count", nullptr, nullptr, 1, 0, 1000, 0);
static MYSQL_SYSVAR_INT(join_splitrows, tianmu_sysvar_join_splitrows, PLUGIN_VAR_INT,
                        "join split rows:0-Disabled, 1-Auto, N-specify count", nullptr, nullptr, 0, 0, 1000, 0);
static MYSQL_SYSVAR_BOOL(minmax_speedup, tianmu_sysvar_minmax_speedup, PLUGIN_VAR_BOOL, "-", nullptr, nullptr, true);
static MYSQL_SYSVAR_UINT(index_cache_size, tianmu_sysvar_index_cache_size, PLUGIN_VAR_READONLY,
                         "Index cache size in MB", nullptr, nullptr, 0, 0, 65536, 0);
static MYSQL_SYSVAR_BOOL(index_search, tianmu_sysvar_index_search, PLUGIN_VAR_BOOL, "-", nullptr, nullptr, true);
static MYSQL_SYSVAR_BOOL(enable_rowstore, tianmu_sysvar_enable_rowstore, PLUGIN_VAR_BOOL, "-", nullptr, nullptr, true);
static MYSQL_SYSVAR_BOOL(parallel_filloutput, tianmu_sysvar_parallel_filloutput, PLUGIN_VAR_BOOL, "-", nullptr, nullptr,
                         true);
static MYSQL_SYSVAR_BOOL(parallel_mapjoin, tianmu_sysvar_parallel_mapjoin, PLUGIN_VAR_BOOL, "-", nullptr, nullptr,
                         false);

static MYSQL_SYSVAR_INT(max_execution_time, tianmu_sysvar_max_execution_time, PLUGIN_VAR_INT,
                        "max query execution time in seconds", nullptr, nullptr, 0, 0, 10000, 0);
static MYSQL_SYSVAR_INT(ini_controlquerylog, tianmu_sysvar_controlquerylog, PLUGIN_VAR_INT, "global controlquerylog",
                        nullptr, controlquerylog_update, 1, 0, 100, 0);

static const char *dist_policy_names[] = {"round-robin", "random", "space", 0};
static TYPELIB policy_typelib_t = {array_elements(dist_policy_names) - 1, "dist_policy_names", dist_policy_names, 0};
static MYSQL_SYSVAR_ENUM(data_distribution_policy, tianmu_sysvar_dist_policy, PLUGIN_VAR_RQCMDARG,
                         "Specifies the policy to distribute column data among multiple data "
                         "directories."
                         "Possible values are round-robin(default), random, and space",
                         nullptr, nullptr, 2, &policy_typelib_t);

static MYSQL_SYSVAR_INT(disk_usage_threshold, tianmu_sysvar_disk_usage_threshold, PLUGIN_VAR_INT,
                        "Specifies the disk usage threshold for data diretories.", nullptr, nullptr, 85, 10, 99, 0);

static MYSQL_SYSVAR_UINT(lookup_max_size, tianmu_sysvar_lookup_max_size, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Lookup dictionary max size", nullptr, nullptr, 100000, 1000, 1000000, 0);

static MYSQL_SYSVAR_BOOL(qps_log, tianmu_sysvar_qps_log, PLUGIN_VAR_BOOL, "-", nullptr, nullptr, true);

static MYSQL_SYSVAR_BOOL(force_hashjoin, tianmu_sysvar_force_hashjoin, PLUGIN_VAR_BOOL, "-", nullptr, nullptr, false);
static MYSQL_SYSVAR_INT(start_async, tianmu_sysvar_start_async, PLUGIN_VAR_INT,
                        "Enable async, specifies async threads x/100 * cpus", nullptr, start_async_update, 0, 0, 100,
                        0);
static MYSQL_SYSVAR_STR(async_join, tianmu_sysvar_async_join, PLUGIN_VAR_STR,
                        "Set async join params: packStep;traverseCount;matchCount", nullptr, async_join_update,
                        "1;0;0;0");
static MYSQL_SYSVAR_BOOL(join_disable_switch_side, tianmu_sysvar_join_disable_switch_side, PLUGIN_VAR_BOOL, "-",
                         nullptr, nullptr, false);
static MYSQL_SYSVAR_BOOL(enable_histogram_cmap_bloom, tianmu_sysvar_enable_histogram_cmap_bloom, PLUGIN_VAR_BOOL, "-",
                         nullptr, nullptr, false);

static MYSQL_SYSVAR_UINT(result_sender_rows, tianmu_sysvar_result_sender_rows, PLUGIN_VAR_UNSIGNED,
                         "The number of rows to load at a time when processing "
                         "queries like select xxx from yyya",
                         nullptr, nullptr, 65536, 1024, 131072, 0);

void debug_update(MYSQL_THD thd, [[maybe_unused]] struct SYS_VAR *var, void *var_ptr, const void *save) {
  if (ha_rcengine_) {
    auto cur_conn = ha_rcengine_->GetTx(thd);
    // set debug_level for connection level
    cur_conn->SetDebugLevel(*(reinterpret_cast<int *>(const_cast<void *>(save))));
  }
  *((int *)var_ptr) = *(reinterpret_cast<int *>(const_cast<void *>(save)));
}

void trace_update(MYSQL_THD thd, [[maybe_unused]] struct SYS_VAR *var, void *var_ptr, const void *save) {
  *((int *)var_ptr) = *(reinterpret_cast<int *>(const_cast<void *>(save)));
  // get global mysql_sysvar_control_trace
  tianmu_sysvar_controltrace = THDVAR(nullptr, control_trace);
  if (ha_rcengine_) {
    core::Transaction *cur_conn = ha_rcengine_->GetTx(thd);
    cur_conn->SetSessionTrace(*(reinterpret_cast<int *>(const_cast<void *>(save))));
    ConfigureRCControl();
  }
}

void controlquerylog_update([[maybe_unused]] MYSQL_THD thd, [[maybe_unused]] struct SYS_VAR *var, void *var_ptr,
                            const void *save) {
  *((int *)var_ptr) = *(reinterpret_cast<int *>(const_cast<void *>(save)));
  int control = *((int *)var_ptr);
  if (ha_rcengine_) {
    control ? rc_querylog_.setOn() : rc_querylog_.setOff();
  }
}

void start_async_update([[maybe_unused]] MYSQL_THD thd, [[maybe_unused]] struct SYS_VAR *var, void *var_ptr,
                        const void *save) {
  int percent = *(reinterpret_cast<int *>(const_cast<void *>(save)));
  *((int *)var_ptr) = percent;
  if (ha_rcengine_) {
    ha_rcengine_->ResetTaskExecutor(percent);
  }
}

void resolve_async_join_settings(const std::string &settings) {
  std::vector<std::string> splits_vec;
  boost::split(splits_vec, settings, boost::is_any_of(";"));
  if (splits_vec.size() >= 4) {
    try {
      tianmu_sysvar_async_join_setting.pack_per_step = boost::lexical_cast<int>(splits_vec[0]);
      tianmu_sysvar_async_join_setting.rows_per_step = boost::lexical_cast<int>(splits_vec[1]);
      tianmu_sysvar_async_join_setting.traverse_slices = boost::lexical_cast<int>(splits_vec[2]);
      tianmu_sysvar_async_join_setting.match_slices = boost::lexical_cast<int>(splits_vec[3]);
    } catch (...) {
      TIANMU_LOG(LogCtl_Level::ERROR, "Failed resolve async join settings");
    }
  }
}

void async_join_update([[maybe_unused]] MYSQL_THD thd, [[maybe_unused]] struct SYS_VAR *var,
                       [[maybe_unused]] void *var_ptr, const void *save) {
  const char *str = *static_cast<const char *const *>(save);
  std::string settings(str);
  resolve_async_join_settings(settings);
}

// stonedb8
static struct SYS_VAR *tianmu_system_variables[] = {MYSQL_SYSVAR(bg_load_threads),
                                                    MYSQL_SYSVAR(cachinglevel),
                                                    MYSQL_SYSVAR(compensation_start),
                                                    MYSQL_SYSVAR(control_trace),
                                                    MYSQL_SYSVAR(data_distribution_policy),
                                                    MYSQL_SYSVAR(disk_usage_threshold),
                                                    MYSQL_SYSVAR(distinct_cache_size),
                                                    MYSQL_SYSVAR(filterevaluation_speedup),
                                                    MYSQL_SYSVAR(global_debug_level),
                                                    MYSQL_SYSVAR(groupby_speedup),
                                                    MYSQL_SYSVAR(hugefiledir),
                                                    MYSQL_SYSVAR(index_cache_size),
                                                    MYSQL_SYSVAR(index_search),
                                                    MYSQL_SYSVAR(enable_rowstore),
                                                    MYSQL_SYSVAR(ini_allowmysqlquerypath),
                                                    MYSQL_SYSVAR(ini_cachefolder),
                                                    MYSQL_SYSVAR(ini_cachereleasethreshold),
                                                    MYSQL_SYSVAR(ini_cachesizethreshold),
                                                    MYSQL_SYSVAR(ini_controlquerylog),
                                                    MYSQL_SYSVAR(ini_knlevel),
                                                    MYSQL_SYSVAR(ini_pushdown),
                                                    MYSQL_SYSVAR(ini_servermainheapsize),
                                                    MYSQL_SYSVAR(ini_threadpoolsize),
                                                    MYSQL_SYSVAR(ini_usemysqlimportexportdefaults),
                                                    MYSQL_SYSVAR(insert_buffer_size),
                                                    MYSQL_SYSVAR(insert_cntthreshold),
                                                    MYSQL_SYSVAR(insert_delayed),
                                                    MYSQL_SYSVAR(insert_max_buffered),
                                                    MYSQL_SYSVAR(insert_numthreshold),
                                                    MYSQL_SYSVAR(insert_wait_ms),
                                                    MYSQL_SYSVAR(insert_wait_time),
                                                    MYSQL_SYSVAR(join_disable_switch_side),
                                                    MYSQL_SYSVAR(enable_histogram_cmap_bloom),
                                                    MYSQL_SYSVAR(join_parallel),
                                                    MYSQL_SYSVAR(join_splitrows),
                                                    MYSQL_SYSVAR(load_threads),
                                                    MYSQL_SYSVAR(lookup_max_size),
                                                    MYSQL_SYSVAR(max_execution_time),
                                                    MYSQL_SYSVAR(minmax_speedup),
                                                    MYSQL_SYSVAR(mm_hardlimit),
                                                    MYSQL_SYSVAR(mm_largetempratio),
                                                    MYSQL_SYSVAR(mm_largetemppool_threshold),
                                                    MYSQL_SYSVAR(mm_policy),
                                                    MYSQL_SYSVAR(mm_releasepolicy),
                                                    MYSQL_SYSVAR(orderby_speedup),
                                                    MYSQL_SYSVAR(parallel_filloutput),
                                                    MYSQL_SYSVAR(parallel_mapjoin),
                                                    MYSQL_SYSVAR(qps_log),
                                                    MYSQL_SYSVAR(query_threads),
                                                    MYSQL_SYSVAR(refresh_sys_tianmu),
                                                    MYSQL_SYSVAR(session_debug_level),
                                                    MYSQL_SYSVAR(sync_buffers),
                                                    MYSQL_SYSVAR(trigger_error),
                                                    MYSQL_SYSVAR(async_join),
                                                    MYSQL_SYSVAR(force_hashjoin),
                                                    MYSQL_SYSVAR(start_async),
                                                    MYSQL_SYSVAR(result_sender_rows),
                                                    nullptr};

}  // namespace DBHandler
}  // namespace Tianmu

mysql_declare_plugin(tianmu){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &Tianmu::DBHandler::tianmu_storage_engine,
    "TIANMU",
    "StoneAtom Group Holding Limited",
    "Tianmu storage engine",
    PLUGIN_LICENSE_GPL,
    Tianmu::DBHandler::rcbase_init_func, /* Plugin Init */
    nullptr,                             /* Plugin Check uninstall */
    Tianmu::DBHandler::rcbase_done_func, /* Plugin Deinit */
    0x0001 /* tianmu version 0.1 */,
    Tianmu::DBHandler::statusvars,              /* status variables  */
    Tianmu::DBHandler::tianmu_system_variables, /* system variables  */
    nullptr,                                    /* config options    */
    0                                           /* flags for plugin */
} mysql_declare_plugin_end;
