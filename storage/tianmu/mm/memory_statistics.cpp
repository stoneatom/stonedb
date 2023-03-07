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

#include "mm/memory_statistics.h"

namespace Tianmu {

MemoryStatisticsOS *MemoryStatisticsOS::instance = nullptr;

void memory_statistics_record(const char *model, const char *deal) {
  const auto mem_info = MemoryStatisticsOS::Instance()->GetMemInfo();
  const auto mem_self = MemoryStatisticsOS::Instance()->GetSelfStatm();
  TIANMU_LOG(LogCtl_Level::INFO,
             "[ %s ] [ %s ] { mem_total: %lu mem_free: %lu mem_available: %lu buffers: %lu cached: %lu swap_cached: %lu"
             " swap_total: %lu swap_free: %lu swap_used: %lu free_total: %lu } { virt: "
             "%lu resident: %lu shared: %lu data_and_stack: %lu }",
             model, deal, mem_info.mem_total, mem_info.mem_free, mem_info.mem_available, mem_info.buffers,
             mem_info.cached, mem_info.swap_cached, mem_info.swap_total, mem_info.swap_free, mem_info.swap_used,
             mem_info.free_total, mem_self.virt, mem_self.resident, mem_self.shared, mem_self.data_and_stack);
}

}  // namespace Tianmu
