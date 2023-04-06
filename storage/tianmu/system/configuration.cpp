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

#include "system/tianmu_system.h"

// Variables to show config through mysql 'show variables' command
char tianmu_sysvar_pushdown;
char tianmu_sysvar_refresh_sys_table;
char tianmu_sysvar_usemysqlimportexportdefaults;
char *tianmu_sysvar_cachefolder;
char *tianmu_sysvar_hugefiledir;
char *tianmu_sysvar_mm_policy;
char *tianmu_sysvar_mm_releasepolicy;
unsigned int tianmu_os_least_mem;
unsigned int tianmu_sysvar_hugefilesize;
unsigned int tianmu_sysvar_allowmysqlquerypath;
unsigned int tianmu_sysvar_bg_load_threads;
unsigned int tianmu_sysvar_cachereleasethreshold;
unsigned int tianmu_sysvar_cachesizethreshold;
unsigned int tianmu_sysvar_cachinglevel;
unsigned int tianmu_sysvar_controlquerylog;
unsigned int tianmu_sysvar_controltrace;
unsigned int tianmu_sysvar_disk_usage_threshold;
unsigned int tianmu_sysvar_distcache_size;
unsigned int tianmu_sysvar_global_debug_level;
unsigned int tianmu_sysvar_insert_buffer_size;
unsigned int tianmu_sysvar_insert_cntthreshold;
unsigned int tianmu_sysvar_insert_max_buffered;
unsigned int tianmu_sysvar_insert_numthreshold;
unsigned int tianmu_sysvar_insert_wait_ms;
unsigned int tianmu_sysvar_insert_wait_time;
unsigned int tianmu_sysvar_knlevel;
unsigned int tianmu_sysvar_load_threads;
unsigned int tianmu_sysvar_max_execution_time;
unsigned int tianmu_sysvar_mm_hardlimit;
unsigned int tianmu_sysvar_mm_large_threshold;
unsigned int tianmu_sysvar_mm_largetempratio;
unsigned int tianmu_sysvar_query_threads;
unsigned int tianmu_sysvar_servermainheapsize;
unsigned int tianmu_sysvar_sync_buffers;
unsigned int tianmu_sysvar_threadpoolsize;
unsigned int tianmu_sysvar_join_parallel;
unsigned int tianmu_sysvar_join_splitrows;
unsigned int tianmu_sysvar_delete_or_update_threads;
unsigned int tianmu_sysvar_merge_rocks_expected_count;
unsigned int tianmu_sysvar_insert_write_batch_size;
unsigned int tianmu_sysvar_log_loop_interval;
my_bool tianmu_sysvar_compensation_start;
my_bool tianmu_sysvar_filterevaluation_speedup;
my_bool tianmu_sysvar_groupby_speedup;
unsigned int tianmu_sysvar_groupby_parallel_degree;
unsigned long long tianmu_sysvar_groupby_parallel_rows_minimum;
unsigned int tianmu_sysvar_slow_query_record_interval;
unsigned int tianmu_sysvar_index_cache_size;
my_bool tianmu_sysvar_index_search;
my_bool tianmu_sysvar_enable_rowstore;
my_bool tianmu_sysvar_insert_delayed;
my_bool tianmu_sysvar_minmax_speedup;
my_bool tianmu_sysvar_orderby_speedup;
my_bool tianmu_sysvar_parallel_filloutput;
my_bool tianmu_sysvar_parallel_mapjoin;
my_bool tianmu_sysvar_qps_log;
my_bool tianmu_sysvar_large_prefix;
unsigned int tianmu_sysvar_lookup_max_size;
unsigned long tianmu_sysvar_dist_policy;
char tianmu_sysvar_force_hashjoin;
unsigned int tianmu_sysvar_start_async;
char *tianmu_sysvar_async_join;
char tianmu_sysvar_join_disable_switch_side;
char tianmu_sysvar_enable_histogram_cmap_bloom;
unsigned int tianmu_sysvar_result_sender_rows;

async_join_setting tianmu_sysvar_async_join_setting;

void ConfigureRCControl() {
  using namespace Tianmu;
  int control_level = tianmu_sysvar_controltrace;
  (control_level > 0) ? tianmu_control_.setOn() : tianmu_control_.setOff();
}
