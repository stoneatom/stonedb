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
#ifndef STONEDB_CORE_TRANSACTION_H_
#define STONEDB_CORE_TRANSACTION_H_
#pragma once

#include <unordered_map>

#include "common/sequence_generator.h"
#include "core/engine.h"
#include "index/kv_store.h"
#include "index/kv_transaction.h"
namespace stonedb {
namespace core {
class Transaction final {
  static common::SequenceGenerator sg;

 private:
  common::TX_ID tid;

  std::unordered_map<std::string, std::shared_ptr<RCTable>> m_modified_tables;
  std::unordered_map<std::string, std::shared_ptr<RCTable>> m_readonly_tables;

  std::atomic_int display_lock{0};  // if >0 disable messages e.g. in subqueries

  bool display_attr_stats = false;  // if set, statistics on attributes should
                                    // be displayed at the end of query
  THD *thd;
  int session_trace = 0;
  int debug_level = 0;
  std::string explain_msg;
  index::KVTransaction kv_trans;

 public:
  ulong GetThreadID() const;
  THD *Thd() const { return thd; }
  bool Killed() const { return (thd ? thd->killed != 0 : false); }
  void SetDisplayAttrStats(bool v = true) { display_attr_stats = v; }
  bool DisplayAttrStats() { return display_attr_stats; }
  void SuspendDisplay();
  void ResumeDisplay();
  void ResetDisplay();
  void SetDebugLevel(int dbg_lev) { debug_level = dbg_lev; }
  int DebugLevel() { return debug_level; }
  void SetSessionTrace(int trace) { session_trace = trace; }
  bool Explain() { return (thd ? thd->lex->describe : false); }
  std::string GetExplainMsg() { return explain_msg; }
  void SetExplainMsg(const std::string &msg) { explain_msg = msg; }
  bool m_explicit_lock_tables = false;

  Transaction(THD *thd) : tid(sg.NextID()), thd(thd) {}
  Transaction() = delete;
  ~Transaction() = default;

  common::TX_ID GetID() const { return tid; }
  std::shared_ptr<RCTable> GetTableByPathIfExists(const std::string &table_path);
  std::shared_ptr<RCTable> GetTableByPath(const std::string &table_path);

  void ExplicitLockTables() { m_explicit_lock_tables = true; };
  void ExplicitUnlockTables() { m_explicit_lock_tables = false; };
  void AddTableRD(std::shared_ptr<TableShare> &share);
  void AddTableWR(std::shared_ptr<TableShare> &share);
  void AddTableWRIfNeeded(std::shared_ptr<TableShare> &share);
  void RemoveTable(std::shared_ptr<TableShare> &share);
  bool Empty() const { return m_modified_tables.empty() && m_readonly_tables.empty(); }
  void Commit(THD *thd);
  void Rollback(THD *thd, bool force_error_message);
  index::KVTransaction &KVTrans() { return kv_trans; }
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_TRANSACTION_H_
