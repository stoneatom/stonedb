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

#include "joiner.h"

#include "core/joiner_hash.h"
#include "core/joiner_mapped.h"
#include "core/joiner_sort.h"
#include "core/parallel_hash_join.h"
#include "core/query.h"
#include "vc/const_column.h"
#include "vc/virtual_column.h"

namespace stonedb {
namespace core {
TwoDimensionalJoiner::TwoDimensionalJoiner(MultiIndex *_mind,  // multi-index to be updated
                                           TempTable *_table, JoinTips &_tips)
    : tips(_tips), m_conn(current_tx) {
  mind = _mind;
  table = _table;
  why_failed = JoinFailure::NOT_FAILED;
}

TwoDimensionalJoiner::~TwoDimensionalJoiner() {
  // Note that mind and rmind are external pointers
}

JoinAlgType TwoDimensionalJoiner::ChooseJoinAlgorithm([[maybe_unused]] MultiIndex &mind, Condition &cond) {
  JoinAlgType join_alg = JoinAlgType::JTYPE_GENERAL;

  if (cond[0].IsType_JoinSimple() && cond[0].op == common::Operator::O_EQ) {
    if ((cond.Size() == 1) && !stonedb_sysvar_force_hashjoin)
      join_alg = JoinAlgType::JTYPE_MAP;  // available types checked inside
    else
      join_alg = JoinAlgType::JTYPE_HASH;
  } else {
    if (cond[0].IsType_JoinSimple() &&
        (cond[0].op == common::Operator::O_MORE_EQ || cond[0].op == common::Operator::O_MORE ||
         cond[0].op == common::Operator::O_LESS_EQ || cond[0].op == common::Operator::O_LESS))
      join_alg = JoinAlgType::JTYPE_SORT;
  }
  return join_alg;
}

JoinAlgType TwoDimensionalJoiner::ChooseJoinAlgorithm(JoinFailure join_result, JoinAlgType prev_type,
                                                      [[maybe_unused]] size_t desc_size) {
  if (join_result == JoinFailure::FAIL_1N_TOO_HARD) return JoinAlgType::JTYPE_HASH;
  if (join_result == JoinFailure::FAIL_WRONG_SIDES) return prev_type;
  // the easiest strategy: in case of any problems, use general joiner
  return JoinAlgType::JTYPE_GENERAL;
}

std::unique_ptr<TwoDimensionalJoiner> TwoDimensionalJoiner::CreateJoiner(JoinAlgType join_alg_type, MultiIndex &mind,
                                                                         JoinTips &tips, TempTable *table) {
  switch (join_alg_type) {
    case JoinAlgType::JTYPE_HASH:
      return CreateHashJoiner(&mind, table, tips);
    case JoinAlgType::JTYPE_SORT:
      return std::unique_ptr<TwoDimensionalJoiner>(new JoinerSort(&mind, table, tips));
    case JoinAlgType::JTYPE_MAP:
      return std::unique_ptr<TwoDimensionalJoiner>(stonedb_sysvar_parallel_mapjoin
                                                       ? (new JoinerParallelMapped(&mind, table, tips))
                                                       : (new JoinerMapped(&mind, table, tips)));
    case JoinAlgType::JTYPE_GENERAL:
      return std::unique_ptr<TwoDimensionalJoiner>(new JoinerGeneral(&mind, table, tips));
    default:
      STONEDB_ERROR("Join algorithm not implemented");
  }
  return std::unique_ptr<TwoDimensionalJoiner>();
}

JoinTips::JoinTips(MultiIndex &mind) {
  limit = -1;
  count_only = false;
  for (int i = 0; i < mind.NoDimensions(); i++) {
    forget_now.push_back(mind.IsForgotten(i));
    distinct_only.push_back(false);
    null_only.push_back(false);
  }
}

JoinTips::JoinTips(const JoinTips &sec) {
  limit = sec.limit;
  count_only = sec.count_only;
  forget_now = sec.forget_now;
  distinct_only = sec.distinct_only;
  null_only = sec.null_only;
}
}  // namespace core
}  // namespace stonedb
