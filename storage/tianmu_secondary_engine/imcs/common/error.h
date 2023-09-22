/* Copyright (c) 2018, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef TIANMU_IMCS_COMMON_ERROR_H
#define TIANMU_IMCS_COMMON_ERROR_H

// return value in imcs
#define RET_FAIL(code) unlikely((ret = (code)) != RET_SUCCESS)
#define RET_NULL(ret) unlikely((ret) == nullptr)

constexpr int RET_SUCCESS = 0;
constexpr int RET_INSERT_FAILED_NO_EXTRA_MEM = 1;
constexpr int RET_CELL_INDEX_NOT_MATCH = 2;
constexpr int RET_CELL_LEN_NOT_MATCH = 3;
constexpr int RET_IMCU_SIZE_TOO_SMALL = 4;
constexpr int RET_MYSQL_TYPE_NOT_SUPPORTED = 5;
constexpr int RET_PK_ROW_ID_NOT_EXIST = 6;
constexpr int RET_PK_ROW_ID_ALREADY_EXIST = 7;
constexpr int RET_IMCS_INNER_ERROR = 8;
constexpr int RET_INDEX_OUT_OF_BOUND = 9;
constexpr int RET_FIELD_NOT_MATCH = 10;
constexpr int RET_CREATE_IMCU_ERR = 11;
constexpr int RET_NOT_FOUND = 12;
constexpr int RET_NOT_FOUND_COMPLETE_MTR = 13;
constexpr int RET_NEED_REORGANIZE_ZONE_MAP = 14;

#define RET_INSERT_FAILED_NO_EXTRA_MEM_MSG "No exrea memory to insert."
#define RET_CELL_INDEX_NOT_MATCH_MSG "Cell index doesn't match."
#define RET_CELL_LEN_NOT_MATCH_MSG "Cell's length doesn't match."
#define RET_IMCU_SIZE_TOO_SMALL_MSG "Imcu size is too small."
#define RET_MYSQL_TYPE_NOT_SUPPORTED_MSG "The MySQL Type is not supported."
#define RET_PK_ROW_ID_NOT_EXIST_MSG "map from pk to row id doesn't exist."
#define RET_PK_ROW_ID_ALREADY_EXIST_MSG "map from pk to row id already exist."
#define RET_IMCS_INNER_ERROR_MSG "Imcs has inner error."
#define RET_INDEX_OUT_OF_BOUND_MSG "Index is out of bound."
#define RET_FIELD_NOT_MATCH_MSG "Field doesn't match."
#define RET_CREATE_IMCU_ERR_MSG "Create Imcu has failed."

#endif