# Copyright (c) 2018, 2022, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

DISABLE_MISSING_PROFILE_WARNING()
ADD_DEFINITIONS(-DMYSQL_SERVER)
ADD_DEFINITIONS(-DMUTEX_EVENT)

SET(TIANMU_SECONDARY_ENGINE_SOURCES
  handler/ha_tianmu_secondary_engine.cpp
  imcs/imcs.cpp
  imcs/row_manager.cpp
  imcs/data/imcu.cpp
  imcs/data/chunk.cpp
  imcs/data/bucket.cpp
  imcs/data/row.cpp
  imcs/data/cell.cpp
  imcs/filter/null_filter.cpp
  imcs/filter/table_filter.cpp
  imcs/filter/constant_filter.cpp
  imcs/filter/conjunction_filter.cpp
  imcs/utilities/hash_util.cpp
  imcs/utilities/ptr_util.cpp
  imcs/statistics/zone_map.cpp

  #common
  imcs/common/arena.cc
  imcs/common/common_def.cpp
  imcs/common/comparison_operator.cpp

  #buffer 
  imcs/buffer/populate.cpp
  imcs/buffer/parse_redo.cpp
  imcs/buffer/apply_redo.cpp

  #compress
  imcs/compress/compress.cpp
  imcs/compress/arith_coder.cpp
  imcs/compress/data_stream.cpp

  #dictionary
  imcs/dictionary/dictionary_encoder.cpp

  #mm
  imcs/mm/

  #statistics
  imcs/statistics/histogram.cpp
  imcs/statistics/camp.cpp

  #sql
  sql/ha_tianmu_imcs.cpp
)

include_directories(${CMAKE_SOURCE_DIR}/sql)
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/storage/innobase/include)
include_directories(${CMAKE_SOURCE_DIR}/storage/innobase/)


IF(WITH_TIANMU_SECONDARY_STORAGE_ENGINE AND NOT WITHOUT_TIANMU_SECONDARY_STORAGE_ENGINE)
  MYSQL_ADD_PLUGIN(tianmu_rapid
                   ${TIANMU_SECONDARY_ENGINE_SOURCES} 
                   STORAGE_ENGINE DEFAULT)
ELSEIF(NOT WITHOUT_TIANMU_SECONDARY_STORAGE_ENGINE)
  MYSQL_ADD_PLUGIN(tianmu_secondary_engine 
                   ${TIANMU_SECONDARY_ENGINE_SOURCES} 
                   STORAGE_ENGINE MODULE_ONLY)
ENDIF()
