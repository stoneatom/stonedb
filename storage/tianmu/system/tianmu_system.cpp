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

#include "tianmu_system.h"

#include <sys/stat.h>

#include "core/tools.h"
#include "system/fet.h"

namespace Tianmu {

// row-column channel control.
system::Channel tianmu_control_(true);
// query log channel flag.
system::Channel tianmu_querylog_(true);
// global host ip addr.
char global_hostIP_[FN_REFLEN];
// global server info.
char global_serverinfo_[FN_REFLEN];
// key-value engine handler.
core::Engine *ha_tianmu_engine_ = nullptr;
// key-value store handler.
index::KVStore *ha_kvstore_ = nullptr;
// global mutex
std::mutex global_mutex_;
// mutex of drop or rename
std::shared_mutex drop_rename_mutex_;

#ifdef FUNCTIONS_EXECUTION_TIMES
// num of distinct DP loaded.
LoadedDataPackCounter count_distinct_dp_loads;
// num of distinct decompressed DP.
LoadedDataPackCounter count_distinct_dp_decompressions;
// exe time of func.
FunctionsExecutionTimes *fet = nullptr;
// num of bytes read by DP.
uint64_t NumOfBytesReadByDPs = 0;
// size of un-compressed DP.
uint64_t SizeOfUncompressedDP = 0;
#endif

}  // namespace Tianmu
