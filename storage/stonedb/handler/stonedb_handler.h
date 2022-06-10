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
#ifndef STONEDB_HANDLER_STONEDB_HANDLER_H_
#define STONEDB_HANDLER_STONEDB_HANDLER_H_
#pragma once

#include "common/common_definitions.h"
#include "core/engine.h"

namespace stonedb {
namespace dbhandler {

// Class definition for the storage engine
class StonedbHandler final : public handler {
 public:
  StonedbHandler(handlerton *hton, TABLE_SHARE *table_arg);
  virtual ~StonedbHandler() = default;
  /* The name that will be used for display purposes */
  const char *table_type() const override { return "STONEDB"; }
  /*
   Get the row type from the storage engine.  If this method returns
   ROW_TYPE_NOT_USED, the information in HA_CREATE_INFO should be used.
   */
  enum row_type get_row_type() const override { return ROW_TYPE_COMPRESSED; }
  /*
   The name of the index type that will be used for display
   don't implement this method unless you really have indexes
   */
  const char *index_type([[maybe_unused]] uint inx) override { return "LSMTREE"; }
  const char **bas_ext() const override;
  /*
   This is a list of flags that says what the storage engine
   implements. The current table flags are documented in
   handler.h
   */
  ulonglong table_flags() const override {
    return HA_NON_KEY_AUTO_INC | HA_REC_NOT_IN_SEQ | HA_PARTIAL_COLUMN_READ | HA_BINLOG_STMT_CAPABLE |
           HA_BLOCK_CONST_TABLE | HA_PRIMARY_KEY_REQUIRED_FOR_POSITION | HA_NULL_IN_KEY | HA_DUPLICATE_POS |
           HA_PRIMARY_KEY_IN_READ_INDEX;
  }
  /*
   This is a bitmap of flags that says how the storage engine
   implements indexes. The current index flags are documented in
   handler.h. If you do not implement indexes, just return zero
   here.

   part is the key part to check. First key part is 0
   If all_parts it's set, MySQL want to know the flags for the combined
   index up to and including 'part'.
   */
  ulong index_flags([[maybe_unused]] uint inx, [[maybe_unused]] uint part,
                    [[maybe_unused]] bool all_parts) const override {
    return 0;
  }
  /*
   unireg.cc will call the following to make sure that the storage engine can
   handle the data it is about to send.

   Return *real* limits of your storage engine here. MySQL will do
   min(your_limits, MySQL_limits) automatically

   There is no need to implement ..._key_... methods if you don't suport
   indexes.
   */
  /*
  support primary key
  */
  uint max_supported_record_length() const override { return HA_MAX_REC_LENGTH; }
  uint max_supported_keys() const override { return MAX_INDEXES; }
  uint max_supported_key_parts() const override { return MAX_REF_PARTS; }
  uint max_supported_key_length() const override { return 1024; }
  /*
   Called in test_quick_select to determine if indexes should be used.
   */
  double scan_time() override { return (double)(stats.records + stats.deleted) / 20.0 + 10; }
  // The next method will never be called if you do not implement indexes.
  double read_time([[maybe_unused]] uint index, [[maybe_unused]] uint ranges, ha_rows rows) override {
    return (double)rows / 20.0 + 1;
  }

  int open(const char *name, int mode,
           uint test_if_locked) override;  // required
  int close() override;                    // required

  int write_row(uchar *buf __attribute__((unused))) override;
  int update_row(const uchar *old_data, uchar *new_data) override;
  int delete_row(const uchar *buf) override;
  int index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag) override;
  int index_next(uchar *buf) override;
  int index_prev(uchar *buf) override;
  int index_first(uchar *buf) override;
  int index_last(uchar *buf) override;
  int index_init(uint index, bool sorted) override;
  int index_end() override;
  /*
   unlike index_init(), rnd_init() can be called two times
   without rnd_end() in between (it only makes sense if scan=1).
   then the second call should prepare for the new table scan
   (e.g if rnd_init allocates the cursor, second call should
   position it to the start of the table, no need to deallocate
   and allocate it again
   */
  int rnd_init(bool scan) override;  // required

  int rnd_end() override;
  int rnd_next(uchar *buf) override;             // required
  int rnd_pos(uchar *buf, uchar *pos) override;  // required
  void position(const uchar *record) override;   // required
  int info(uint) override;                       // required

  int extra(enum ha_extra_function operation) override;
  int external_lock(THD *thd, int lock_type) override;  // required
  int start_stmt(THD *thd, thr_lock_type lock_type) override;
  int delete_all_rows() override;
  ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key) override;
  int delete_table(const char *from) override;
  int rename_table(const char *from, const char *to) override;
  int create(const char *name, TABLE *form,
             HA_CREATE_INFO *create_info) override;  // required
  int truncate() override;

  enum_alter_inplace_result check_if_supported_inplace_alter(TABLE *altered_table,
                                                             Alter_inplace_info *ha_alter_info) override;
  bool inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info) override;
  bool commit_inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info, bool commit) override;

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type) override;  // required

  char *update_table_comment(const char *comment) override;
  bool explain_message(const Item *a_cond, String *buf) override;
  /* Condition push-down operation */
  const Item *cond_push(const Item *cond) override;
  void cond_pop() override {}
  int reset() override;

  my_bool register_query_cache_table(THD *thd, char *table_key, uint key_length, qc_engine_callback *call_back,
                                     ulonglong *engine_data) override;
  void update_create_info(HA_CREATE_INFO *create_info) override;
  int fill_row_by_id(uchar *buf, uint64_t rowid);
  void key_convert(const uchar *key, uint key_len, std::vector<uint> cols, std::vector<std::string_view> &keys);

 public:
  static const Alter_inplace_info::HA_ALTER_FLAGS STONEDB_SUPPORTED_ALTER_ADD_DROP_ORDER;
  static const Alter_inplace_info::HA_ALTER_FLAGS STONEDB_SUPPORTED_ALTER_COLUMN_NAME;

 protected:
  int set_cond_iter();
  int fill_row(uchar *buf);
  int free_share();

  std::shared_ptr<core::TableShare> share;

  THR_LOCK_DATA m_lock; /* MySQL lock */
  std::string m_table_name;
  uint m_dupkey_pos = -1;

  core::JustATable *table_ptr = nullptr;
  std::unique_ptr<core::Filter> filter_ptr;
  uint64_t current_position;

  core::RCTable::Iterator table_new_iter;
  core::RCTable::Iterator table_new_iter_end;

  std::unique_ptr<core::Query> m_query;
  core::TabID m_tmp_table;
  std::unique_ptr<core::CompiledQuery> m_cq;
  bool m_result = false;
  std::vector<std::vector<uchar>> blob_buffers;
};

}  // namespace dbhandler
}  // namespace stonedb

#endif  // STONEDB_HANDLER_STONEDB_HANDLER_H_
