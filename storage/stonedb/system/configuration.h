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
#ifndef STONEDB_SYSTEM_CONFIGURATION_H_
#define STONEDB_SYSTEM_CONFIGURATION_H_
#pragma once

extern char stonedb_sysvar_compensation_start;
extern char stonedb_sysvar_filterevaluation_speedup;
extern char stonedb_sysvar_groupby_speedup;
extern unsigned int stonedb_sysvar_index_cache_size;
extern char stonedb_sysvar_index_search;
extern char stonedb_sysvar_enable_rowstore;
extern char stonedb_sysvar_insert_delayed;
extern char stonedb_sysvar_minmax_speedup;
extern char stonedb_sysvar_orderby_speedup;
extern char stonedb_sysvar_parallel_filloutput;
extern char stonedb_sysvar_parallel_mapjoin;
extern char stonedb_sysvar_pushdown;
extern char stonedb_sysvar_qps_log;
extern char stonedb_sysvar_refresh_sys_table;
extern char stonedb_sysvar_usemysqlimportexportdefaults;
extern char *stonedb_sysvar_cachefolder;
extern char *stonedb_sysvar_hugefiledir;
extern char *stonedb_sysvar_mm_policy;
extern char *stonedb_sysvar_mm_releasepolicy;
extern int stonedb_sysvar_allowmysqlquerypath;
extern int stonedb_sysvar_bg_load_threads;
extern int stonedb_sysvar_cachereleasethreshold;
extern int stonedb_sysvar_cachesizethreshold;
extern int stonedb_sysvar_cachinglevel;
extern int stonedb_sysvar_controlquerylog;
extern int stonedb_sysvar_controltrace;
extern int stonedb_sysvar_disk_usage_threshold;
extern int stonedb_sysvar_distcache_size;
extern int stonedb_sysvar_global_debug_level;
extern int stonedb_sysvar_insert_buffer_size;
extern int stonedb_sysvar_insert_cntthreshold;
extern int stonedb_sysvar_insert_max_buffered;
extern int stonedb_sysvar_insert_numthreshold;
extern int stonedb_sysvar_insert_wait_ms;
extern int stonedb_sysvar_insert_wait_time;
extern int stonedb_sysvar_join_parallel;
extern int stonedb_sysvar_join_splitrows;
extern int stonedb_sysvar_knlevel;
extern int stonedb_sysvar_load_threads;
extern unsigned int stonedb_sysvar_lookup_max_size;
extern int stonedb_sysvar_max_execution_time;
extern int stonedb_sysvar_mm_hardlimit;
extern int stonedb_sysvar_mm_large_threshold;
extern int stonedb_sysvar_mm_largetempratio;
extern int stonedb_sysvar_query_threads;
extern int stonedb_sysvar_servermainheapsize;
extern int stonedb_sysvar_sync_buffers;
extern int stonedb_sysvar_threadpoolsize;
extern unsigned long stonedb_sysvar_dist_policy;
extern char stonedb_sysvar_force_hashjoin;
extern int stonedb_sysvar_start_async;
// Format: a;b;c;d
// a : iterator pack count one step,default 1, 0 is disable
// b : iterator rows count one step,default 0, 0 is disable,if a and b all is
// setted, use a priority c : traverse slices count, default 0 is the async
// thread count d : match slices count, default 0 is the async thread count
extern char *stonedb_sysvar_async_join;
// If there is too much conflict in the hash join building hash table, program
// will automatically switching another table to make a hash table,
// stonedb_sysvar_join_disable_switch_side is the option to avoid this switching
// process.
extern char stonedb_sysvar_join_disable_switch_side;
// enable histogram/cmap/bloom filtering
extern char stonedb_sysvar_enable_histogram_cmap_bloom;
// The number of rows to load at a time when processing queries like select xxx
// from yyy
extern unsigned int stonedb_sysvar_result_sender_rows;

void ConfigureRCControl();

struct async_join_setting {
  int pack_per_step = 1;
  int rows_per_step = 0;
  int traverse_slices = 0;
  int match_slices = 0;

  bool is_enabled() const { return (pack_per_step > 0) || (rows_per_step > 0); }
};
extern async_join_setting stonedb_sysvar_async_join_setting;

#endif  // STONEDB_SYSTEM_CONFIGURATION_H_
