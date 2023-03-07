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
#ifndef TIANMU_CORE_TRANSACTION_H_
#define TIANMU_CORE_TRANSACTION_H_
#pragma once

#include <unordered_map>

#include "common/sequence_generator.h"
#include "core/engine.h"
#include "index/kv_store.h"
#include "index/kv_transaction.h"
namespace Tianmu {
namespace core {

class Transaction final {
  static common::SequenceGenerator seq_generator_;

 private:
  common::TX_ID txn_id_;

  std::unordered_map<std::string, std::shared_ptr<TianmuTable>> modified_tables_;
  std::unordered_map<std::string, std::shared_ptr<TianmuTable>> readonly_tables_;

  std::atomic_int display_lock_{0};  // if >0 disable messages e.g. in subqueries

  bool display_attr_stats_ = false;  // if set, statistics on attributes should
                                     // be displayed at the end of query
  THD *thd;
  int session_trace_ = 0;
  int debug_level_ = 0;
  std::string explain_msg_;
  index::KVTransaction kv_trans_;
  common::LoadSource load_source_;

  uint32_t insert_row_num_ = 0;

 public:
  ulong GetThreadID() const;
  THD *Thd() const { return thd; }
  bool Killed() const { return (thd ? thd->killed != 0 : false); }
  void SetDisplayAttrStats(bool v = true) { display_attr_stats_ = v; }
  bool DisplayAttrStats() { return display_attr_stats_; }
  void SuspendDisplay();
  void ResumeDisplay();
  void ResetDisplay();
  void SetDebugLevel(int dbg_lev) { debug_level_ = dbg_lev; }
  int DebugLevel() { return debug_level_; }
  void SetSessionTrace(int trace) { session_trace_ = trace; }
  bool Explain() { return (thd ? thd->lex->describe : false); }
  std::string GetExplainMsg() { return explain_msg_; }
  void SetExplainMsg(const std::string &msg) { explain_msg_ = msg; }
  bool explicit_lock_tables_ = false;

  Transaction(THD *thd) : txn_id_(seq_generator_.NextID()), thd(thd) {}
  Transaction() = delete;
  ~Transaction() = default;

  common::TX_ID GetID() const { return txn_id_; }
  common::LoadSource LoadSource() const { return load_source_; }
  std::shared_ptr<TianmuTable> GetTableByPathIfExists(const std::string &table_path);
  std::shared_ptr<TianmuTable> GetTableByPath(const std::string &table_path);

  void SetLoadSource(common::LoadSource ls) { load_source_ = ls; }
  void ExplicitLockTables() { explicit_lock_tables_ = true; }
  void ExplicitUnlockTables() { explicit_lock_tables_ = false; }
  void AddTableRD(std::shared_ptr<TableShare> &share);
  void AddTableWR(std::shared_ptr<TableShare> &share);
  void AddTableWRIfNeeded(std::shared_ptr<TableShare> &share);
  void RemoveTable(std::shared_ptr<TableShare> &share);
  bool Empty() const { return modified_tables_.empty() && readonly_tables_.empty(); }
  void Commit(THD *thd);
  void Rollback(THD *thd, bool force_error_message);
  index::KVTransaction &KVTrans() { return kv_trans_; }

  uint32_t &GetInsertRowNum() { return insert_row_num_; }
  void AddInsertRowNum(uint32_t row_num = 1) { insert_row_num_ += row_num; }
  void ResetInsertRowNum() { insert_row_num_ = 0; }
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_TRANSACTION_H_
