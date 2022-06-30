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

#include "stonedb_handler.h"

#include <iostream>

#include "mysql/plugin.h"

#include "common/assert.h"
#include "common/exception.h"
#include "core/compilation_tools.h"
#include "core/compiled_query.h"
#include "core/temp_table.h"
#include "core/tools.h"
#include "core/transaction.h"
#include "core/value.h"
#include "system/configuration.h"
#include "util/fs.h"

#define MYSQL_SERVER 1

namespace stonedb {
namespace dbhandler {

const Alter_inplace_info::HA_ALTER_FLAGS StonedbHandler::STONEDB_SUPPORTED_ALTER_ADD_DROP_ORDER =
    Alter_inplace_info::ADD_COLUMN | Alter_inplace_info::DROP_COLUMN | Alter_inplace_info::ALTER_COLUMN_ORDER;
const Alter_inplace_info::HA_ALTER_FLAGS StonedbHandler::STONEDB_SUPPORTED_ALTER_COLUMN_NAME =
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

my_bool rcbase_query_caching_of_table_permitted(THD *thd, [[maybe_unused]] char *full_name,
                                                [[maybe_unused]] uint full_name_len,
                                                [[maybe_unused]] ulonglong *unused) {
  if (!thd_test_options(thd, (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) return ((my_bool)TRUE);
  return ((my_bool)FALSE);
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
        my_bool myb;
        my_time_t secs_utc = current_tx->Thd()->variables.time_zone->TIME_to_gmt_sec(&my_time, &myb);
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

StonedbHandler::StonedbHandler(handlerton *hton, TABLE_SHARE *table_arg) : handler(hton, table_arg) {
  ref_length = sizeof(uint64_t);
}

const char **StonedbHandler::bas_ext() const {
  static const char *ha_rcbase_exts[] = {common::STONEDB_EXT, 0};
  return ha_rcbase_exts;
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
  return stonedb_sysvar_insert_delayed &&
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
THR_LOCK_DATA **StonedbHandler::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
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
int StonedbHandler::external_lock(THD *thd, int lock_type) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = 1;

  // static const char *ss[] = { "READ LOCK", "WRITE LOCK", "UNLOCK", };
  // rclog << lock << "external lock table " << m_table_name << " type: " <<
  // ss[lock_type] << " command: " << thd->lex->sql_command << unlock;

  if (thd->lex->sql_command == SQLCOM_LOCK_TABLES) DBUG_RETURN(HA_ERR_WRONG_COMMAND);

  if (is_delay_insert(thd) && lock_type == F_WRLCK) DBUG_RETURN(0);

  try {
    if (lock_type == F_UNLCK) {
      if (thd->lex->sql_command == SQLCOM_UNLOCK_TABLES) current_tx->ExplicitUnlockTables();

      if (thd->killed) rceng->Rollback(thd, true);
      if (current_tx) {
        current_tx->RemoveTable(share);
        if (current_tx->Empty()) {
          rceng->ClearTx(thd);
        }
      }
    } else {
      auto tx = rceng->GetTx(thd);
      if (thd->lex->sql_command == SQLCOM_LOCK_TABLES) tx->ExplicitLockTables();

      if (lock_type == F_RDLCK) {
        tx->AddTableRD(share);
      } else {
        tx->AddTableWR(share);
        trans_register_ha(thd, false, rcbase_hton);
        if (thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) trans_register_ha(thd, true, rcbase_hton);
      }
    }
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "StoneDB internal error", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  // destroy the tx on failure
  if (ret != 0) rceng->ClearTx(thd);

  DBUG_RETURN(ret);
}

namespace {
inline bool has_dup_key(std::shared_ptr<index::RCTableIndex> &indextab, TABLE *table, size_t &row) {
  common::ErrorCode ret;
  std::vector<std::string_view> records;
  KEY *key = table->key_info + table->s->primary_key;

  for (uint i = 0; i < key->actual_key_parts; i++) {
    uint col = key->key_part[i].field->field_index;
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
          my_bool myb;
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

  ret = indextab->GetRowByKey(current_tx, records, row);
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
 sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.
 */
int StonedbHandler::write_row([[maybe_unused]] uchar *buf) {
  int ret = 1;
  DBUG_ENTER(__PRETTY_FUNCTION__);
  try {
    if (ha_thd()->lex->duplicates == DUP_UPDATE) {
      if (auto indextab = rceng->GetTableIndex(m_table_name)) {
        if (size_t row; has_dup_key(indextab, table, row)) {
          m_dupkey_pos = row;
          DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
        }
      }
    }
    ret = rceng->InsertRow(m_table_name, current_tx, table, share);
  } catch (common::OutOfMemoryException &e) {
    DBUG_RETURN(ER_LOCK_WAIT_TIMEOUT);
  } catch (common::DatabaseException &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught in Engine::InsertRow: %s.", e.what());
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
  } catch (common::FormatException &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught in Engine::InsertRow: %s Row: %ld, field %u.", e.what(),
                e.m_row_no, e.m_field_no);
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
  } catch (common::FileException &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught in Engine::InsertRow: %s.", e.what());
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
  } catch (common::Exception &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught in Engine::InsertRow: %s.", e.what());
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught in Engine::InsertRow: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
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
int StonedbHandler::update_row(const uchar *old_data, uchar *new_data) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int ret = HA_ERR_INTERNAL_ERROR;
  auto org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  std::shared_ptr<void> defer(nullptr,
                              [org_bitmap, this](...) { dbug_tmp_restore_column_map(table->write_set, org_bitmap); });

  try {
    auto tab = current_tx->GetTableByPath(m_table_name);

    for (uint i = 0; i < table->s->fields; i++) {
      if (!bitmap_is_set(table->write_set, i)) {
        continue;
      }
      auto field = table->field[i];
      if (field->real_maybe_null()) {
        if (field->is_null_in_record(old_data) && field->is_null_in_record(new_data)) {
          continue;
        }

        if (field->is_null_in_record(new_data)) {
          core::Value null;
          tab->UpdateItem(current_position, i, null);
          continue;
        }
      }
      auto o_ptr = field->ptr - table->record[0] + old_data;
      auto n_ptr = field->ptr - table->record[0] + new_data;
      if (field->is_null_in_record(old_data) || std::memcmp(o_ptr, n_ptr, field->pack_length()) != 0) {
        my_bitmap_map *org_bitmap2 = dbug_tmp_use_all_columns(table, table->read_set);
        std::shared_ptr<void> defer(
            nullptr, [org_bitmap2, this](...) { dbug_tmp_restore_column_map(table->read_set, org_bitmap2); });
        core::Value v = GetValueFromField(field);
        tab->UpdateItem(current_position, i, v);
      }
    }
    rceng->IncStonedbStatUpdate();
    DBUG_RETURN(0);
  } catch (common::DatabaseException &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Update exception: %s.", e.what());
  } catch (common::FileException &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Update exception: %s.", e.what());
  } catch (common::DupKeyException &e) {
    ret = HA_ERR_FOUND_DUPP_KEY;
    STONEDB_LOG(LogCtl_Level::ERROR, "Update exception: %s.", e.what());
  } catch (common::Exception &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Update exception: %s.", e.what());
  } catch (std::exception &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Update exception: %s.", e.what());
  } catch (...) {
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
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
int StonedbHandler::delete_row([[maybe_unused]] const uchar *buf) {
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
int StonedbHandler::delete_all_rows() {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int StonedbHandler::rename_table(const char *from, const char *to) {
  try {
    rceng->RenameTable(current_tx, from, to, ha_thd());
    return 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
  return 1;
}

void StonedbHandler::update_create_info([[maybe_unused]] HA_CREATE_INFO *create_info) {}

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
int StonedbHandler::info(uint flag) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int ret = 1;
  try {
    std::scoped_lock guard(global_mutex);
    if (flag & HA_STATUS_VARIABLE) {
      std::shared_ptr<core::RCTable> tab;
      if (current_tx != nullptr) {
        tab = current_tx->GetTableByPath(m_table_name);
      } else
        tab = rceng->GetTableRD(m_table_name);
      stats.records = (ha_rows)tab->NumOfValues();
      stats.data_file_length = 0;
      stats.mean_rec_length = 0;
      if (stats.records > 0) {
        std::vector<core::AttrInfo> attr_info(tab->GetAttributesInfo());
        uint num_of_attrs = tab->NumOfAttrs();
        for (uint j = 0; j < num_of_attrs; j++) stats.data_file_length += attr_info[j].comp_size;  // compressed size
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
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

/* this should return 0 for concurrent insert to work */
my_bool stonedb_check_status([[maybe_unused]] void *param) { return 0; }

/*
 Used for opening tables. The name will be the name of the file.
 A table is opened when it needs to be opened. For instance
 when a request comes in for a select on the table (tables are not
 open and closed for each request, they are cached).

 Called from handler.cc by handler::ha_open(). The server opens all tables by
 calling ha_open() which then calls the handler specific open().
 */
int StonedbHandler::open(const char *name, [[maybe_unused]] int mode, [[maybe_unused]] uint test_if_locked) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  m_table_name = name;
  int ret = 1;

  try {
    // TODO:
    // Probably we don't need to search from the map each time.
    // Keeping the share together with mysql handler cache makes
    // more sense that would mean once a table is opened the TableShare
    // would be kept.
    if (!(share = rceng->GetTableShare(table_share))) DBUG_RETURN(ret);

    thr_lock_data_init(&share->thr_lock, &m_lock, NULL);
    share->thr_lock.check_status = stonedb_check_status;
    // have primary key, use table index
    if (table->s->primary_key != MAX_INDEXES) rceng->AddTableIndex(name, table, ha_thd());
    rceng->AddMemTable(table, share);
    ret = 0;
  } catch (common::Exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "Error from StoneDB engine", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "A stonedb exception is caught: %s", e.what());
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  info(HA_STATUS_CONST);

  DBUG_RETURN(ret);
}

int StonedbHandler::free_share() {
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
int StonedbHandler::close() {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  DBUG_RETURN(free_share());
}

int StonedbHandler::fill_row_by_id([[maybe_unused]] uchar *buf, uint64_t rowid) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_KEY_NOT_FOUND;
  try {
    auto tab = current_tx->GetTableByPath(m_table_name);
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
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  }
  DBUG_RETURN(rc);
}

int StonedbHandler::index_init(uint index, [[maybe_unused]] bool sorted) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  active_index = index;
  DBUG_RETURN(0);
}

int StonedbHandler::index_end() {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  active_index = MAX_KEY;
  DBUG_RETURN(0);
}

/*
 Positions an index cursor to the index specified in the handle. Fetches the
 row if available. If the key value is null, begin at the first key of the
 index.
 */
int StonedbHandler::index_read([[maybe_unused]] uchar *buf, [[maybe_unused]] const uchar *key,
                               [[maybe_unused]] uint key_len __attribute__((unused)),
                               enum ha_rkey_function find_flag __attribute__((unused))) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_KEY_NOT_FOUND;
  try {
    auto index = rceng->GetTableIndex(m_table_name);
    if (index && (active_index == table_share->primary_key)) {
      std::vector<std::string_view> keys;
      key_convert(key, key_len, index->KeyCols(), keys);
      // support equality fullkey lookup over primary key, using full tuple
      if (find_flag == HA_READ_KEY_EXACT) {
        uint64_t rowid;
        if (index->GetRowByKey(current_tx, keys, rowid) == common::ErrorCode::SUCCESS) {
          rc = fill_row_by_id(buf, rowid);
        }
      } else if (find_flag == HA_READ_AFTER_KEY || find_flag == HA_READ_KEY_OR_NEXT) {
        auto iter = current_tx->KVTrans().KeyIter();
        common::Operator op = (find_flag == HA_READ_AFTER_KEY) ? common::Operator::O_MORE : common::Operator::O_MORE_EQ;
        iter->ScanToKey(index, keys, op);
        uint64_t rowid;
        iter->GetRowid(rowid);
        rc = fill_row_by_id(buf, rowid);

      } else {
        // not support HA_READ_PREFIX_LAST_OR_PREV HA_READ_PREFIX_LAST
        rc = HA_ERR_WRONG_COMMAND;
        STONEDB_LOG(LogCtl_Level::ERROR, "Error: index_read not support prefix search");
      }

    } else {
      // other index not support
      rc = HA_ERR_WRONG_INDEX;
      STONEDB_LOG(LogCtl_Level::ERROR, "Error: index_read only support primary key");
    }

  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  }

  DBUG_RETURN(rc);
}

/*
 Used to read forward through the index.
 */
int StonedbHandler::index_next([[maybe_unused]] uchar *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_END_OF_FILE;
  try {
    auto iter = current_tx->KVTrans().KeyIter();
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
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  }
  DBUG_RETURN(rc);
}

/*
 Used to read backwards through the index.
 */
int StonedbHandler::index_prev([[maybe_unused]] uchar *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_END_OF_FILE;
  try {
    auto iter = current_tx->KVTrans().KeyIter();
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
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  }
  DBUG_RETURN(rc);
}

/*
 index_first() asks for the first key in the index.

 Called from opt_range.cc, opt_sum.cc, sql_handler.cc,
 and sql_select.cc.
 */
int StonedbHandler::index_first([[maybe_unused]] uchar *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_END_OF_FILE;
  try {
    auto index = rceng->GetTableIndex(m_table_name);
    if (index && current_tx) {
      uint64_t rowid;
      auto iter = current_tx->KVTrans().KeyIter();
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
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  }
  DBUG_RETURN(rc);
}

/*
 index_last() asks for the last key in the index.

 Called from opt_range.cc, opt_sum.cc, sql_handler.cc,
 and sql_select.cc.
 */
int StonedbHandler::index_last([[maybe_unused]] uchar *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int rc = HA_ERR_END_OF_FILE;
  try {
    auto index = rceng->GetTableIndex(m_table_name);
    if (index && current_tx) {
      uint64_t rowid;
      auto iter = current_tx->KVTrans().KeyIter();
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
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
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
int StonedbHandler::rnd_init(bool scan) {
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
        if (!push_down_result || push_down_result->NoTables() != 1)
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
        rccontrol << system::lock << "Error in push-down execution, push-down execution aborted: " << e.what()
                  << system::unlock;
        STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught in push-down execution: %s", e.what());
      }
      m_query.reset();
      m_cq.reset();
    } else {
      if (scan && filter_ptr.get()) {
        table_new_iter = ((core::RCTable *)table_ptr)->Begin(GetAttrsUseIndicator(table), *filter_ptr);
        table_new_iter_end = ((core::RCTable *)table_ptr)->End();
      } else {
        std::shared_ptr<core::RCTable> rctp;
        rceng->GetTableIterator(m_table_name, table_new_iter, table_new_iter_end, rctp, GetAttrsUseIndicator(table),
                                table->in_use);
        table_ptr = rctp.get();
        filter_ptr.reset();
      }
    }
    ret = 0;
    blob_buffers.resize(0);
    if (table_ptr != NULL) blob_buffers.resize(table_ptr->NumOfDisplaybleAttrs());
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

int StonedbHandler::rnd_end() {
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
int StonedbHandler::rnd_next(uchar *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = HA_ERR_END_OF_FILE;
  try {
    table->status = 0;
    if (fill_row(buf) == HA_ERR_END_OF_FILE) {
      table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(ret);
    }
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
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
void StonedbHandler::position([[maybe_unused]] const uchar *record) {
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
int StonedbHandler::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = HA_ERR_END_OF_FILE;
  try {
    uint64_t position = my_get_ptr(pos, ref_length);

    filter_ptr = std::make_unique<core::Filter>(position + 1, share->PackSizeShift());
    filter_ptr->Reset();
    filter_ptr->Set(position);

    auto tab_ptr = rceng->GetTx(table->in_use)->GetTableByPath(m_table_name);
    table_new_iter = tab_ptr->Begin(GetAttrsUseIndicator(table), *filter_ptr);
    table_new_iter_end = tab_ptr->End();
    table_ptr = tab_ptr.get();

    table_new_iter.MoveToRow(position);
    table->status = 0;
    blob_buffers.resize(table->s->fields);
    if (fill_row(buf) == HA_ERR_END_OF_FILE) {
      table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(ret);
    }
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

/*
 extra() is called whenever the server wishes to send a hint to
 the storage engine. The myisam engine implements the most hints.
 ha_innodb.cc has the most exhaustive list of these hints.
 */
int StonedbHandler::extra(enum ha_extra_function operation) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  /* This preemptive delete might cause problems here.
   * Other place where it can be put is StonedbHandler::external_lock().
   */
  if (operation == HA_EXTRA_NO_CACHE) {
    m_cq.reset();
    m_query.reset();
  }
  DBUG_RETURN(0);
}

int StonedbHandler::start_stmt(THD *thd, thr_lock_type lock_type) {
  try {
    if (lock_type == TL_WRITE_CONCURRENT_INSERT || lock_type == TL_WRITE_DEFAULT || lock_type == TL_WRITE) {
      trans_register_ha(thd, true, rcbase_hton);
      trans_register_ha(thd, false, rcbase_hton);
      current_tx->AddTableWRIfNeeded(share);
    }
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
  return 0;
}

/*
 Ask rcbase handler about permission to cache table during query registration.
 If current thread is in non-autocommit, we don't permit any mysql query
 caching.
 */
my_bool StonedbHandler::register_query_cache_table(THD *thd, char *table_key, uint key_length,
                                                   qc_engine_callback *call_back,
                                                   [[maybe_unused]] ulonglong *engine_data) {
  *call_back = rcbase_query_caching_of_table_permitted;
  return rcbase_query_caching_of_table_permitted(thd, table_key, key_length, 0);
}

/*
 Used to delete a table. By the time delete_table() has been called all
 opened references to this table will have been closed (and your globally
 shared references released. The variable name will just be the name of
 the table. You will need to remove any files you have created at this point.

 If you do not implement this, the default delete_table() is called from
 handler.cc and it will delete all files with the file extentions returned
 by bas_ext().

 Called from handler.cc by delete_table and  ha_create_table(). Only used
 during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
 the storage engine.
 */
int StonedbHandler::delete_table(const char *name) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  int ret = 1;
  try {
    rceng->DeleteTable(name, ha_thd());
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

/*
 Given a starting key, and an ending key estimate the number of rows that
 will exist between the two. end_key may be empty which in case determine
 if start_key matches any rows.

 Called from opt_range.cc by check_quick_keys().
 */
ha_rows StonedbHandler::records_in_range([[maybe_unused]] uint inx, [[maybe_unused]] key_range *min_key,
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
int StonedbHandler::create(const char *name, TABLE *table_arg, [[maybe_unused]] HA_CREATE_INFO *create_info) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  try {
    rceng->CreateTable(name, table_arg);
    DBUG_RETURN(0);
  } catch (common::AutoIncException &e) {
    my_message(ER_WRONG_AUTO_KEY, e.what(), MYF(0));
  } catch (common::UnsupportedDataTypeException &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
  } catch (fs::filesystem_error &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "filesystem_error on table creation '%s'", e.what());
    fs::remove_all(std::string(name) + common::STONEDB_EXT);
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(1);
}

int StonedbHandler::truncate() {
  int ret = 0;
  try {
    rceng->TruncateTable(m_table_name, ha_thd());
  } catch (std::exception &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
    ret = 1;
  } catch (...) {
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
    ret = 1;
  }

  return ret;
}

int StonedbHandler::fill_row(uchar *buf) {
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

char *StonedbHandler::update_table_comment(const char *comment) {
  char *ret = const_cast<char *>(comment);
  try {
    uint length = (uint)std::strlen(comment);
    char *str = NULL;
    uint extra_len = 0;

    if (length > 64000 - 3) {
      return ((char *)comment);  // string too long
    }

    //  get size & ratio
    int64_t sum_c = 0, sum_u = 0;
    std::vector<core::AttrInfo> attr_info = rceng->GetTableAttributesInfo(m_table_name, table_share);
    for (uint j = 0; j < attr_info.size(); j++) {
      sum_c += attr_info[j].comp_size;
      sum_u += attr_info[j].uncomp_size;
    }
    char buf[256];
    double ratio = (sum_c > 0 ? double(sum_u) / double(sum_c) : 0);
    int count = std::sprintf(buf, "Overall compression ratio: %.3f, Raw size=%ld MB", ratio, sum_u >> 20);
    extra_len += count;

    str = (char *)my_malloc(length + extra_len + 3, MYF(0));
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
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  return ret;
}

bool StonedbHandler::explain_message(const Item *a_cond, String *buf) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  if (current_tx->Explain()) {
    cond_push(a_cond);
    std::string str = current_tx->GetExplainMsg();
    buf->append(str.c_str(), str.length());
  }
  DBUG_RETURN(TRUE);
}

int StonedbHandler::set_cond_iter() {
  int ret = 1;
  if (m_query && !m_result && table_ptr->NumOfObj() != 0) {
    m_cq->Result(m_tmp_table);  // it is ALWAYS -2 though....
    m_result = true;

    try {
      core::FunctionExecutor fe(std::bind(&core::Query::LockPackInfoForUse, std::ref(m_query)),
                                std::bind(&core::Query::UnlockPackInfoFromUse, std::ref(m_query)));

      core::TempTable *push_down_result = m_query->Preexecute(*m_cq, NULL, false);
      if (!push_down_result || push_down_result->NoTables() != 1)
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
      rccontrol << system::lock << "Error in push-down execution, push-down execution aborted: " << e.what()
                << system::unlock;
      STONEDB_LOG(LogCtl_Level::ERROR, "Error in push-down execution, push-down execution aborted: %s", e.what());
    }
    m_query.reset();
    m_cq.reset();
  }
  return ret;
}

const Item *StonedbHandler::cond_push(const Item *a_cond) {
  Item const *ret = a_cond;
  Item *cond = const_cast<Item *>(a_cond);

  try {
    if (!m_query) {
      std::shared_ptr<core::RCTable> rctp;
      rceng->GetTableIterator(m_table_name, table_new_iter, table_new_iter_end, rctp, GetAttrsUseIndicator(table),
                              table->in_use);
      table_ptr = rctp.get();
      m_query.reset(new core::Query(current_tx));
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
    if (!m_query->BuildConditions(cond, cond_id, tmp_cq.get(), m_tmp_table, core::CondType::WHERE_COND, false)) {
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
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }
  return ret;
}

int StonedbHandler::reset() {
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
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

enum_alter_inplace_result StonedbHandler::check_if_supported_inplace_alter([[maybe_unused]] TABLE *altered_table,
                                                                           Alter_inplace_info *ha_alter_info) {
  if ((ha_alter_info->handler_flags & ~STONEDB_SUPPORTED_ALTER_ADD_DROP_ORDER) &&
      (ha_alter_info->handler_flags != STONEDB_SUPPORTED_ALTER_COLUMN_NAME)) {
    return HA_ALTER_ERROR;
  }
  return HA_ALTER_INPLACE_EXCLUSIVE_LOCK;
}

bool StonedbHandler::inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info) {
  try {
    if (!(ha_alter_info->handler_flags & ~STONEDB_SUPPORTED_ALTER_ADD_DROP_ORDER)) {
      std::vector<Field *> v_old(table_share->field, table_share->field + table_share->fields);
      std::vector<Field *> v_new(altered_table->s->field, altered_table->s->field + altered_table->s->fields);
      rceng->PrepareAlterTable(m_table_name, v_new, v_old, ha_thd());
      return false;
    } else if (ha_alter_info->handler_flags == STONEDB_SUPPORTED_ALTER_COLUMN_NAME) {
      return false;
    }
  } catch (std::exception &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
  } catch (...) {
    STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "Unable to inplace alter table", MYF(0));

  return true;
}

bool StonedbHandler::commit_inplace_alter_table([[maybe_unused]] TABLE *altered_table,
                                                Alter_inplace_info *ha_alter_info, bool commit) {
  if (!commit) {
    STONEDB_LOG(LogCtl_Level::INFO, "Alter table failed : %s%s", m_table_name.c_str(), " rollback");
    return true;
  }
  if (ha_alter_info->handler_flags == STONEDB_SUPPORTED_ALTER_COLUMN_NAME) {
    return false;
  }
  if ((ha_alter_info->handler_flags & ~STONEDB_SUPPORTED_ALTER_ADD_DROP_ORDER)) {
    STONEDB_LOG(LogCtl_Level::INFO, "Altered table not support type %lu", ha_alter_info->handler_flags);
    return true;
  }
  fs::path tmp_dir(m_table_name + ".tmp");
  fs::path tab_dir(m_table_name + common::STONEDB_EXT);
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
        STONEDB_LOG(LogCtl_Level::INFO, "removing %s", target.c_str());
      }
    }
    fs::remove_all(bak_dir);
  } catch (fs::filesystem_error &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "file system error: %s %s|%s", e.what(), e.path1().string().c_str(),
                e.path2().string().c_str());
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "Failed to commit alter table", MYF(0));
    return true;
  }
  return false;
}
/*
 key: mysql format, may be union key, need changed to kvstore key format

 */
void StonedbHandler::key_convert(const uchar *key, uint key_len, std::vector<uint> cols,
                                 std::vector<std::string_view> &keys) {
  key_restore(table->record[0], (uchar *)key, &table->key_info[active_index], key_len);

  Field **field = table->field;
  std::vector<std::string> records;
  for (auto &i : cols) {
    Field *f = field[i];
    size_t length;
    if (f->is_null()) {
      throw common::Exception("Priamry key part can not be NULL");
    }
    if (f->flags & BLOB_FLAG)
      length = dynamic_cast<Field_blob *>(f)->get_length();
    else
      length = f->row_pack_length();

    std::unique_ptr<char[]> buf(new char[length]);
    char *ptr = buf.get();

    switch (f->type()) {
      case MYSQL_TYPE_TINY: {
        int64_t v = f->val_int();
        if (v > SDB_TINYINT_MAX)
          v = SDB_TINYINT_MAX;
        else if (v < SDB_TINYINT_MIN)
          v = SDB_TINYINT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_SHORT: {
        int64_t v = f->val_int();
        if (v > SDB_SMALLINT_MAX)
          v = SDB_SMALLINT_MAX;
        else if (v < SDB_SMALLINT_MIN)
          v = SDB_SMALLINT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_LONG: {
        int64_t v = f->val_int();
        if (v > std::numeric_limits<int>::max())
          v = std::numeric_limits<int>::max();
        else if (v < SDB_INT_MIN)
          v = SDB_INT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_INT24: {
        int64_t v = f->val_int();
        if (v > SDB_MEDIUMINT_MAX)
          v = SDB_MEDIUMINT_MAX;
        else if (v < SDB_MEDIUMINT_MIN)
          v = SDB_MEDIUMINT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_LONGLONG: {
        int64_t v = f->val_int();
        if (v > common::SDB_BIGINT_MAX)
          v = common::SDB_BIGINT_MAX;
        else if (v < common::SDB_BIGINT_MIN)
          v = common::SDB_BIGINT_MIN;
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
          my_bool myb;
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

}  // namespace dbhandler
}  // namespace stonedb
