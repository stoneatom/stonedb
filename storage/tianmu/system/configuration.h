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
#ifndef TIANMU_SYSTEM_CONFIGURATION_H_
#define TIANMU_SYSTEM_CONFIGURATION_H_
#pragma once

extern char tianmu_sysvar_compensation_start;
extern char tianmu_sysvar_filterevaluation_speedup;
extern char tianmu_sysvar_groupby_speedup;
extern unsigned int tianmu_sysvar_index_cache_size;
extern char tianmu_sysvar_index_search;
extern char tianmu_sysvar_enable_rowstore;
extern char tianmu_sysvar_insert_delayed;
extern char tianmu_sysvar_minmax_speedup;
extern char tianmu_sysvar_orderby_speedup;
extern char tianmu_sysvar_parallel_filloutput;
extern char tianmu_sysvar_parallel_mapjoin;
extern char tianmu_sysvar_pushdown;
extern char tianmu_sysvar_qps_log;
extern char tianmu_sysvar_refresh_sys_table;
extern char tianmu_sysvar_large_prefix;
extern char tianmu_sysvar_usemysqlimportexportdefaults;
extern char *tianmu_sysvar_cachefolder;
extern char *tianmu_sysvar_hugefiledir;
extern char *tianmu_sysvar_mm_policy;
extern char *tianmu_sysvar_mm_releasepolicy;
extern int tianmu_sysvar_allowmysqlquerypath;
extern int tianmu_sysvar_bg_load_threads;
extern int tianmu_sysvar_cachereleasethreshold;
extern int tianmu_sysvar_cachesizethreshold;
extern int tianmu_sysvar_cachinglevel;
extern int tianmu_sysvar_controlquerylog;
extern int tianmu_sysvar_controltrace;
extern int tianmu_sysvar_disk_usage_threshold;
extern int tianmu_sysvar_distcache_size;
extern int tianmu_sysvar_global_debug_level;
extern int tianmu_sysvar_insert_buffer_size;
extern int tianmu_sysvar_insert_cntthreshold;
extern int tianmu_sysvar_insert_max_buffered;
extern int tianmu_sysvar_insert_numthreshold;
extern int tianmu_sysvar_insert_wait_ms;
extern int tianmu_sysvar_insert_wait_time;
extern int tianmu_sysvar_join_parallel;
extern int tianmu_sysvar_join_splitrows;
extern int tianmu_sysvar_knlevel;
extern int tianmu_sysvar_load_threads;
extern unsigned int tianmu_sysvar_lookup_max_size;
extern int tianmu_sysvar_max_execution_time;
extern int tianmu_sysvar_mm_hardlimit;
extern int tianmu_sysvar_mm_large_threshold;
extern int tianmu_sysvar_mm_largetempratio;
extern int tianmu_sysvar_query_threads;
extern int tianmu_sysvar_servermainheapsize;
extern int tianmu_sysvar_sync_buffers;
extern int tianmu_sysvar_threadpoolsize;
extern unsigned long tianmu_sysvar_dist_policy;
extern char tianmu_sysvar_force_hashjoin;
extern int tianmu_sysvar_start_async;
// Format: a;b;c;d
// a : iterator pack count one step,default 1, 0 is disable
// b : iterator rows count one step,default 0, 0 is disable,if a and b all is
// setted, use a priority c : traverse slices count, default 0 is the async
// thread count d : match slices count, default 0 is the async thread count
extern char *tianmu_sysvar_async_join;
// If there is too much conflict in the hash join building hash table, program
// will automatically switching another table to make a hash table,
// tianmu_sysvar_join_disable_switch_side is the option to avoid this switching
// process.
extern char tianmu_sysvar_join_disable_switch_side;
// enable histogram/cmap/bloom filtering
extern char tianmu_sysvar_enable_histogram_cmap_bloom;
// The number of rows to load at a time when processing queries like select xxx
// from yyy
extern unsigned int tianmu_sysvar_result_sender_rows;

void ConfigureRCControl();

struct async_join_setting {
  int pack_per_step = 1;
  int rows_per_step = 0;
  int traverse_slices = 0;
  int match_slices = 0;

  bool is_enabled() const { return (pack_per_step > 0) || (rows_per_step > 0); }
};

extern async_join_setting tianmu_sysvar_async_join_setting;

#endif  // TIANMU_SYSTEM_CONFIGURATION_H_
