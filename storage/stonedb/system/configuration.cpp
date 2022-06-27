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

#include "system/rc_system.h"

// Variables to show config through mysql 'show variables' command
char stonedb_sysvar_pushdown;
char stonedb_sysvar_refresh_sys_table;
char stonedb_sysvar_usemysqlimportexportdefaults;
char *stonedb_sysvar_cachefolder;
char *stonedb_sysvar_hugefiledir;
char *stonedb_sysvar_mm_policy;
char *stonedb_sysvar_mm_releasepolicy;
int stonedb_sysvar_allowmysqlquerypath;
int stonedb_sysvar_bg_load_threads;
int stonedb_sysvar_cachereleasethreshold;
int stonedb_sysvar_cachesizethreshold;
int stonedb_sysvar_cachinglevel;
int stonedb_sysvar_controlquerylog;
int stonedb_sysvar_controltrace;
int stonedb_sysvar_disk_usage_threshold;
int stonedb_sysvar_distcache_size;
int stonedb_sysvar_global_debug_level;
int stonedb_sysvar_insert_buffer_size;
int stonedb_sysvar_insert_cntthreshold;
int stonedb_sysvar_insert_max_buffered;
int stonedb_sysvar_insert_numthreshold;
int stonedb_sysvar_insert_wait_ms;
int stonedb_sysvar_insert_wait_time;
int stonedb_sysvar_knlevel;
int stonedb_sysvar_load_threads;
int stonedb_sysvar_max_execution_time;
int stonedb_sysvar_mm_hardlimit;
int stonedb_sysvar_mm_large_threshold;
int stonedb_sysvar_mm_largetempratio;
int stonedb_sysvar_query_threads;
int stonedb_sysvar_servermainheapsize;
int stonedb_sysvar_sync_buffers;
int stonedb_sysvar_threadpoolsize;
int stonedb_sysvar_join_parallel;
int stonedb_sysvar_join_splitrows;
my_bool stonedb_sysvar_compensation_start;
my_bool stonedb_sysvar_filterevaluation_speedup;
my_bool stonedb_sysvar_groupby_speedup;
unsigned int stonedb_sysvar_index_cache_size;
my_bool stonedb_sysvar_index_search;
my_bool stonedb_sysvar_enable_rowstore;
my_bool stonedb_sysvar_insert_delayed;
my_bool stonedb_sysvar_minmax_speedup;
my_bool stonedb_sysvar_orderby_speedup;
my_bool stonedb_sysvar_parallel_filloutput;
my_bool stonedb_sysvar_parallel_mapjoin;
my_bool stonedb_sysvar_qps_log;
unsigned int stonedb_sysvar_lookup_max_size;
unsigned long stonedb_sysvar_dist_policy;
char stonedb_sysvar_force_hashjoin;
int stonedb_sysvar_start_async;
char *stonedb_sysvar_async_join;
char stonedb_sysvar_join_disable_switch_side;
char stonedb_sysvar_enable_histogram_cmap_bloom;
unsigned int stonedb_sysvar_result_sender_rows;

async_join_setting stonedb_sysvar_async_join_setting;

void ConfigureRCControl() {
  using namespace stonedb;
  int control_level = stonedb_sysvar_controltrace;
  (control_level > 0) ? rccontrol.setOn() : rccontrol.setOff();
}
