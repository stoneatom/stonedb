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
#ifndef TIANMU_SYSTEM_TIANMU_SYSTEM_H_
#define TIANMU_SYSTEM_TIANMU_SYSTEM_H_
#pragma once

#include <shared_mutex>

#include "system/channel.h"
#include "system/configuration.h"
#include "util/log_ctl.h"

namespace Tianmu {

namespace core {
class Engine;
class Transaction;
}  // namespace core

namespace index {
class KVStore;
}  // namespace index

// Channel for debugging information, not
// displayed in the standard running mode.
extern system::Channel tianmu_control_;
// the channel for query log.
extern system::Channel tianmu_querylog_;
// host ip addr.
extern char global_hostIP_[FN_REFLEN];
// host server info string.
extern char global_serverinfo_[FN_REFLEN];

// row-column engine handler.
extern core::Engine *ha_tianmu_engine_;
// key-value store handler.
extern index::KVStore *ha_kvstore_;
// global mutex.
extern std::mutex global_mutex_;
// drop or rename mutex.
extern std::shared_mutex drop_rename_mutex_;
// current transaction handler.
extern thread_local core::Transaction *current_txn_;

}  // namespace Tianmu

#endif  // TIANMU_SYSTEM_TIANMU_SYSTEM_H_
