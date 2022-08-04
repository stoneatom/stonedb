#   Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
#   Use is subject to license terms
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; version 2 of the License.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335  USA
#
# This is the CMakeLists for StoneDB

INCLUDE(CheckFunctionExists)
INCLUDE(CheckCSourceCompiles)
INCLUDE(CheckCSourceRuns)

# OS tests
IF(UNIX)
  IF(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    CHECK_INCLUDE_FILES (libaio.h HAVE_LIBAIO_H)
    CHECK_LIBRARY_EXISTS(aio io_queue_init "" HAVE_LIBAIO)
    ADD_DEFINITIONS("-DUNIV_LINUX -D_GNU_SOURCE=1")
    IF(HAVE_LIBAIO_H AND HAVE_LIBAIO)
      ADD_DEFINITIONS(-DLINUX_NATIVE_AIO=1)
      LINK_LIBRARIES(aio)
    ENDIF()
  ELSEIF(CMAKE_SYSTEM_NAME MATCHES "HP*")
    ADD_DEFINITIONS("-DUNIV_HPUX")
  ELSEIF(CMAKE_SYSTEM_NAME STREQUAL "AIX")
    ADD_DEFINITIONS("-DUNIV_AIX")
  ELSEIF(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    ADD_DEFINITIONS("-DUNIV_SOLARIS")
  ENDIF()
ENDIF()

IF(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
# After: WL#5825 Using C++ Standard Library with MySQL code
#       we no longer use -fno-exceptions
#	SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")
ENDIF()

# Enable InnoDB's UNIV_DEBUG and UNIV_SYNC_DEBUG in debug builds
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DUNIV_DEBUG -DUNIV_SYNC_DEBUG")

# Add -Wconversion if compiling with GCC
## As of Mar 15 2011 this flag causes 3573+ warnings. If you are reading this
## please fix them and enable the following code:
#IF(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wconversion")
#ENDIF()

CHECK_FUNCTION_EXISTS(sched_getcpu  HAVE_SCHED_GETCPU)

IF(NOT MSVC)
# either define HAVE_IB_GCC_ATOMIC_BUILTINS or not
IF(NOT CMAKE_CROSSCOMPILING)
  CHECK_C_SOURCE_RUNS(
  "
  int main()
  {
    long	x;
    long	y;
    long	res;

    x = 10;
    y = 123;
    res = __sync_bool_compare_and_swap(&x, x, y);
    if (!res || x != y) {
      return(1);
    }

    x = 10;
    y = 123;
    res = __sync_bool_compare_and_swap(&x, x + 1, y);
    if (res || x != 10) {
      return(1);
    }
    x = 10;
    y = 123;
    res = __sync_add_and_fetch(&x, y);
    if (res != 123 + 10 || x != 123 + 10) {
      return(1);
    }
    return(0);
  }"
  HAVE_IB_GCC_ATOMIC_BUILTINS
  )
  CHECK_C_SOURCE_RUNS(
  "
  int main()
  {
    long	res;
    char	c;

    c = 10;
    res = __sync_lock_test_and_set(&c, 123);
    if (res != 10 || c != 123) {
      return(1);
    }
    return(0);
  }"
  HAVE_IB_GCC_ATOMIC_BUILTINS_BYTE
  )
  CHECK_C_SOURCE_RUNS(
  "#include<stdint.h>
  int main()
  {
    int64_t	x,y,res;

    x = 10;
    y = 123;
    res = __sync_sub_and_fetch(&y, x);
    if (res != y || y != 113) {
      return(1);
    }
    res = __sync_add_and_fetch(&y, x);
    if (res != y || y != 123) {
      return(1);
    }
    return(0);
  }"
  HAVE_IB_GCC_ATOMIC_BUILTINS_64
  )
  CHECK_C_SOURCE_RUNS(
  "#include<stdint.h>
  int main()
  {
    __sync_synchronize();
    return(0);
  }"
  HAVE_IB_GCC_SYNC_SYNCHRONISE
  )
  CHECK_C_SOURCE_RUNS(
  "#include<stdint.h>
  int main()
  {
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    __atomic_thread_fence(__ATOMIC_RELEASE);
    return(0);
  }"
  HAVE_IB_GCC_ATOMIC_THREAD_FENCE
  )
ENDIF()

IF(HAVE_IB_GCC_ATOMIC_BUILTINS)
 ADD_DEFINITIONS(-DHAVE_IB_GCC_ATOMIC_BUILTINS=1)
ENDIF()

IF(HAVE_IB_GCC_ATOMIC_BUILTINS_BYTE)
 ADD_DEFINITIONS(-DHAVE_IB_GCC_ATOMIC_BUILTINS_BYTE=1)
ENDIF()

IF(HAVE_IB_GCC_ATOMIC_BUILTINS_64)
 ADD_DEFINITIONS(-DHAVE_IB_GCC_ATOMIC_BUILTINS_64=1)
ENDIF()

IF(HAVE_IB_GCC_SYNC_SYNCHRONISE)
 ADD_DEFINITIONS(-DHAVE_IB_GCC_SYNC_SYNCHRONISE=1)
ENDIF()

IF(HAVE_IB_GCC_ATOMIC_THREAD_FENCE)
 ADD_DEFINITIONS(-DHAVE_IB_GCC_ATOMIC_THREAD_FENCE=1)
ENDIF()

 # either define HAVE_IB_ATOMIC_PTHREAD_T_GCC or not
IF(NOT CMAKE_CROSSCOMPILING)
  CHECK_C_SOURCE_RUNS(
  "
  #include <pthread.h>
  #include <string.h>

  int main() {
    pthread_t       x1;
    pthread_t       x2;
    pthread_t       x3;

    memset(&x1, 0x0, sizeof(x1));
    memset(&x2, 0x0, sizeof(x2));
    memset(&x3, 0x0, sizeof(x3));

    __sync_bool_compare_and_swap(&x1, x2, x3);

    return(0);
  }"
  HAVE_IB_ATOMIC_PTHREAD_T_GCC)
ENDIF()
IF(HAVE_IB_ATOMIC_PTHREAD_T_GCC)
  ADD_DEFINITIONS(-DHAVE_IB_ATOMIC_PTHREAD_T_GCC=1)
ENDIF()

ENDIF(NOT MSVC)

CHECK_FUNCTION_EXISTS(asprintf  HAVE_ASPRINTF)
CHECK_FUNCTION_EXISTS(vasprintf  HAVE_VASPRINTF)

# Solaris atomics
IF(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
  CHECK_FUNCTION_EXISTS(atomic_cas_ulong  HAVE_ATOMIC_CAS_ULONG)
  CHECK_FUNCTION_EXISTS(atomic_cas_32 HAVE_ATOMIC_CAS_32)
  CHECK_FUNCTION_EXISTS(atomic_cas_64 HAVE_ATOMIC_CAS_64)
  CHECK_FUNCTION_EXISTS(atomic_add_long_nv HAVE_ATOMIC_ADD_LONG_NV)
  CHECK_FUNCTION_EXISTS(atomic_swap_uchar HAVE_ATOMIC_SWAP_UCHAR)
  IF(HAVE_ATOMIC_CAS_ULONG AND
     HAVE_ATOMIC_CAS_32 AND
     HAVE_ATOMIC_CAS_64 AND
     HAVE_ATOMIC_ADD_LONG_NV AND
     HAVE_ATOMIC_SWAP_UCHAR)
    SET(HAVE_IB_SOLARIS_ATOMICS 1)
  ENDIF()

  IF(HAVE_IB_SOLARIS_ATOMICS)
    ADD_DEFINITIONS(-DHAVE_IB_SOLARIS_ATOMICS=1)
  ENDIF()

  IF(NOT CMAKE_CROSSCOMPILING)
  # either define HAVE_IB_ATOMIC_PTHREAD_T_SOLARIS or not
  CHECK_C_SOURCE_COMPILES(
  "   #include <pthread.h>
      #include <string.h>

      int main(int argc, char** argv) {
        pthread_t       x1;
        pthread_t       x2;
        pthread_t       x3;

        memset(&x1, 0x0, sizeof(x1));
        memset(&x2, 0x0, sizeof(x2));
        memset(&x3, 0x0, sizeof(x3));

        if (sizeof(pthread_t) == 4) {

          atomic_cas_32(&x1, x2, x3);

        } else if (sizeof(pthread_t) == 8) {

          atomic_cas_64(&x1, x2, x3);

        } else {

          return(1);
        }

      return(0);
    }
  " HAVE_IB_ATOMIC_PTHREAD_T_SOLARIS)
  CHECK_C_SOURCE_COMPILES(
  "#include <mbarrier.h>
  int main() {
    __machine_r_barrier();
    __machine_w_barrier();
    return(0);
  }"
  HAVE_IB_MACHINE_BARRIER_SOLARIS)
  ENDIF()
  IF(HAVE_IB_ATOMIC_PTHREAD_T_SOLARIS)
    ADD_DEFINITIONS(-DHAVE_IB_ATOMIC_PTHREAD_T_SOLARIS=1)
  ENDIF()
  IF(HAVE_IB_MACHINE_BARRIER_SOLARIS)
    ADD_DEFINITIONS(-DHAVE_IB_MACHINE_BARRIER_SOLARIS=1)
  ENDIF()
ENDIF()


IF(UNIX)
# this is needed to know which one of atomic_cas_32() or atomic_cas_64()
# to use in the source
SET(CMAKE_EXTRA_INCLUDE_FILES pthread.h)
CHECK_TYPE_SIZE(pthread_t SIZEOF_PTHREAD_T)
SET(CMAKE_EXTRA_INCLUDE_FILES)
ENDIF()

IF(SIZEOF_PTHREAD_T)
  ADD_DEFINITIONS(-DSIZEOF_PTHREAD_T=${SIZEOF_PTHREAD_T})
ENDIF()

IF(MSVC)
  ADD_DEFINITIONS(-DHAVE_WINDOWS_ATOMICS)
  ADD_DEFINITIONS(-DHAVE_WINDOWS_MM_FENCE)
ENDIF()

# Include directories under stonedb
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/storage/stonedb
                   )

# Sun Studio bug with -xO2
IF(CMAKE_CXX_COMPILER_ID MATCHES "SunPro"
	AND CMAKE_CXX_FLAGS_RELEASE MATCHES "O2"
	AND NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
	# Sun Studio 12 crashes with -xO2 flag, but not with higher optimization
	# -xO3
	SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_SOURCE_DIR}/rem/rem0rec.cc
    PROPERTIES COMPILE_FLAGS -xO3)
ENDIF()

# Removing compiler optimizations for innodb/mem/* files on 64-bit Windows
# due to 64-bit compiler error, See MySQL Bug #19424, #36366, #34297
IF (MSVC AND CMAKE_SIZEOF_VOID_P EQUAL 8)
	SET_SOURCE_FILES_PROPERTIES(mem/mem0mem.cc mem/mem0pool.cc
				    PROPERTIES COMPILE_FLAGS -Od)
ENDIF()

SET(STONEDB_STATIC_LIBS ${STONEDB_STATIC_LIBS}
                    "-lboost_system"
                    "-lboost_thread"
                    "-lstdc++"
                    "-lstdc++fs"
                    "-lrt"
                    "-lmarisa"
                    #"-lzstd"
                    "-lrocksdb"
                    )

SET(STONEDB_SOURCES
  base/core/app_template.cpp
  base/core/exception_hacks.cpp
  base/core/fsqual.cpp
  base/core/fstream.cpp
  base/core/metrics.cpp
  base/core/posix.cpp
  base/core/reactor.cpp
  base/core/resource.cpp
  base/core/scollectd.cpp
  base/core/systemwide_memory_barrier.cpp
  base/core/thread.cpp
  base/fmt/format.cpp
  base/fmt/ostream.cpp
  base/fmt/posix.cpp
  base/fmt/printf.cpp
  base/net/posix_stack.cpp
  base/net/stack.cpp
  base/util/conversions.cpp
  base/util/log.cpp
  base/util/program_options.cpp
  common/common_definitions.cpp
  common/data_format.cpp
  common/exception.cpp
  common/mysql_gate.cpp
  common/txt_data_format.cpp
  compress/arith_coder.cpp
  compress/basic_data_filt.cpp
  compress/bit_stream_compressor.cpp
  compress/data_stream.cpp
  compress/dictionary.cpp
  compress/inc_alloc.cpp
  compress/inc_wgraph.cpp
  compress/lz4.cpp
  compress/part_dict.cpp
  compress/ppm.cpp
  compress/suffix_tree.cpp
  compress/text_compressor.cpp
  compress/top_bit_dict.cpp
  compress/word_graph.cpp
  core/aggregation_algorithm.cpp
  core/aggregator_advanced.cpp
  core/aggregator_basic.cpp
  core/bin_tools.cpp
  core/blocked_mem_table.cpp
  core/bloom_block.cpp
  core/bloom_coding.cpp
  core/cached_buffer.cpp
  core/column_bin_encoder.cpp
  core/column_share.cpp
  core/column_type.cpp
  core/compilation_tools.cpp
  core/compiled_query.cpp
  core/condition.cpp
  core/condition_encoder.cpp
  core/cq_term.cpp
  core/data_cache.cpp
  core/data_type.cpp
  core/descriptor.cpp
  core/dimension_group.cpp
  core/dimension_group_multiple.cpp
  core/dimension_group_virtual.cpp
  core/engine_convert.cpp
  core/engine.cpp
  core/engine_execute.cpp
  core/engine_results.cpp
  core/filter_block.cpp
  core/filter.cpp
  core/filter_iterators.cpp
  core/ftree.cpp
  core/groupby_wrapper.cpp
  core/group_distinct_cache.cpp
  core/group_distinct_table.cpp
  core/group_table.cpp
  core/hash_table.cpp
  core/index_table.cpp
  core/item_sdbfield.cpp
  core/joiner.cpp
  core/joiner_general.cpp
  core/joiner_hash.cpp
  core/joiner_hash_table.cpp
  core/joiner_mapped.cpp
  core/joiner_sort.cpp
  core/join_thread_table.cpp
  core/just_a_table.cpp
  core/mi_iterator.cpp
  core/mi_new_contents.cpp
  core/mi_rough_sorter.cpp
  core/mi_step_iterator.cpp
  core/mi_updating_iterator.cpp
  core/multi_index_builder.cpp
  core/multi_index.cpp
  core/mysql_expression.cpp
  core/pack.cpp
  core/pack_guardian.cpp
  core/pack_int.cpp
  core/pack_orderer.cpp
  core/pack_str.cpp
  core/parallel_hash_join.cpp
  core/parameterized_filter.cpp
  core/proxy_hash_joiner.cpp
  core/query_compile.cpp
  core/query.cpp
  core/quick_math.cpp
  core/rc_attr.cpp
  core/rcattr_exeq_rs.cpp
  core/rcattr_exqp.cpp
  core/rc_attr_typeinfo.cpp
  core/rc_mem_table.cpp
  core/rc_table.cpp
  core/rough_multi_index.cpp
  core/rough_value.cpp
  core/rsi_bloom.cpp
  core/rsi_cmap.cpp
  core/rsi_histogram.cpp
  core/sorter3.cpp
  core/sorter_wrapper.cpp
  core/table_share.cpp
  core/task_executor.cpp
  core/temp_table_com.cpp
  core/temp_table.cpp
  core/temp_table_low.cpp
  core/temp_table_roughquery.cpp
  core/transaction.cpp
  core/value_matching_hashtable.cpp
  core/value_matching_table.cpp
  core/value_or_null.cpp
  core/value_set.cpp
  exporter/data_exporter.cpp
  exporter/data_exporter_txt.cpp
  exporter/export2file.cpp
  handler/ha_rcengine.cpp
  handler/stonedb_handler_com.cpp
  handler/stonedb_handler.cpp
  index/kv_store.cpp
  index/kv_transaction.cpp
  index/rc_table_index.cpp
  index/rdb_meta_manager.cpp
  loader/load_parser.cpp
  loader/parsing_strategy.cpp
  loader/read_buffer.cpp
  loader/rejecter.cpp
  loader/value_cache.cpp
  mm/huge_heap_policy.cpp
  mm/initializer.cpp
  mm/memory_handling_policy.cpp
  mm/mysql_heap_policy.cpp
  mm/numa_heap_policy.cpp
  mm/release2q.cpp
  mm/release_all.cpp
  mm/release_fifo.cpp
  mm/release_lru.cpp
  mm/release_tracker.cpp
  mm/sys_heap_policy.cpp
  mm/tcm/page_heap.cpp
  mm/tcm/span.cpp
  mm/tcm/tccommon.cpp
  mm/tcm_heap_policy.cpp
  mm/traceable_object.cpp
  system/cacheable_item.cpp
  system/channel.cpp
  system/configuration.cpp
  system/fet.cpp
  system/file.cpp
  system/file_system.cpp
  system/large_buffer.cpp
  system/net_stream.cpp
  system/rc_system.cpp
  system/res_manager_base.cpp
  system/res_manager.cpp
  system/stream.cpp
  system/txt_utils.cpp
  types/bstring.cpp
  types/rc_data_types.cpp
  types/rc_datetime.cpp
  types/rc_item_types.cpp
  types/rc_num.cpp
  types/rc_value_object.cpp
  types/text_stat.cpp
  types/value_parser4txt.cpp
  util/log_ctl.cpp
  util/stack_trace.cpp
  util/thread_pool.cpp
  util/timer.cpp
  vc/const_column.cpp
  vc/const_expr_column.cpp
  vc/expr_column.cpp
  vc/in_set_column.cpp
  vc/single_column.cpp
  vc/subselect_column.cpp
  vc/type_cast_column.cpp
  vc/virtual_column_base.cpp
	)

IF(WITH_STONEDB)
  # Legacy option
  SET(WITH_STONEDB_ENGINE TRUE)
ENDIF()

add_definitions(-D_GLIBCXX_USE_CXX11_ABI=1)

#MYSQL_ADD_PLUGIN(stonedb ${STONEDB_SOURCES} STORAGE_ENGINE
#  MANDATORY
#  MODULE_OUTPUT_NAME ha_stonedb
#  LINK_LIBRARIES ${STONEDB_STATIC_LIBS})
MYSQL_ADD_PLUGIN(stonedb ${STONEDB_SOURCES} STORAGE_ENGINE
			MANDATORY
			STATIC_ONLY
			LINK_LIBRARIES ${STONEDB_STATIC_LIBS})

add_subdirectory(async_tests)
