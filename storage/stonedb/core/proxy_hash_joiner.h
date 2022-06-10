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
#ifndef STONEDB_CORE_PROXY_HASH_JOINER_H_
#define STONEDB_CORE_PROXY_HASH_JOINER_H_
#pragma once

#include <memory>

#include "core/joiner.h"

namespace stonedb {
namespace core {
class ProxyHashJoiner : public TwoDimensionalJoiner {
 public:
  ProxyHashJoiner(MultiIndex *mind, TempTable *table, JoinTips &tips);
  ~ProxyHashJoiner();

  // Overridden from TwoDimensionalJoiner:
  void ExecuteJoinConditions(Condition &cond) override;
  void ForceSwitchingSides() override { force_switching_sides_ = true; }

 private:
  class Action;
  std::unique_ptr<Action> action_;
  bool force_switching_sides_ = false;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_PROXY_HASH_JOINER_H_