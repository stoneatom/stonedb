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

#include "rc_system.h"

#include <sys/stat.h>

#include "core/tools.h"
#include "system/fet.h"

namespace stonedb {
system::Channel rccontrol(true);
system::Channel rcquerylog(true);
char glob_hostip[FN_REFLEN];
char glob_serverInfo[FN_REFLEN];
core::Engine *rceng = 0;
index::KVStore *kvstore = 0;

std::mutex global_mutex;
std::shared_mutex drop_rename_mutex;

#ifdef FUNCTIONS_EXECUTION_TIMES
LoadedDataPackCounter count_distinct_dp_loads;
LoadedDataPackCounter count_distinct_dp_decompressions;
FunctionsExecutionTimes *fet = NULL;
uint64_t NoBytesReadByDPs = 0;
uint64_t SizeOfUncompressedDP = 0;
#endif

}  // namespace stonedb
