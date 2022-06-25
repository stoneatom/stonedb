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
#ifndef STONEDB_SYSTEM_RC_SYSTEM_H_
#define STONEDB_SYSTEM_RC_SYSTEM_H_
#pragma once

#include <shared_mutex>

#include "system/channel.h"
#include "system/configuration.h"
#include "util/log_ctl.h"

namespace stonedb {
namespace core {
class Engine;
class Transaction;
}  // namespace core
namespace index {

class KVStore;

}

extern system::Channel rccontrol;  // Channel for debugging information, not
                                   // displayed in the standard running mode.
extern system::Channel rcquerylog;
extern char glob_hostip[FN_REFLEN];
extern char glob_serverInfo[FN_REFLEN];

extern core::Engine *rceng;
extern index::KVStore *kvstore;
extern std::mutex global_mutex;
extern std::shared_mutex drop_rename_mutex;
extern thread_local core::Transaction *current_tx;
}  // namespace stonedb

#endif  // STONEDB_SYSTEM_RC_SYSTEM_H_
