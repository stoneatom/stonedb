# Copyright (c) 2014, 2021, Oracle and/or its affiliates.
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
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

# We want rocksdb 6.12.6 in order to build our stonedb code.
# The rocksdb tarball is fairly big, and takes several minutes
# to download. So we recommend downloading/unpacking it
# only once, in a place visible from any bzr sandbox.
# We use only header files, so there should be no binary dependencies.

# Invoke with -DWITH_ROCKSDB=<directory> or set WITH_ROCKSDB in environment.
# If WITH_ROCKSDB is *not* set, or is set to the special value "system",
# we assume that the correct version (see below)
# is installed on the compile host in the standard location.

SET(ROCKSDB_PACKAGE_NAME "vrocksdb-6.12.6")
SET(ROCKSDB_TARBALL "${ROCKSDB_PACKAGE_NAME}.tar.gz")
SET(ROCKSDB_DOWNLOAD_URL
  "https://github.com/facebook/rocksdb/archive/refs/tags/${ROCKSDB_TARBALL}"
  )

SET(OLD_PACKAGE_NAMES "vrocksdb-6.10.6")

MACRO(RESET_ROCKSDB_VARIABLES)
  UNSET(ROCKSDB_INCLUDE_DIR)
  UNSET(ROCKSDB_INCLUDE_DIR CACHE)
  UNSET(LOCAL_ROCKSDB_DIR)
  UNSET(LOCAL_ROCKSDB_DIR CACHE)
  UNSET(LOCAL_ROCKSDB_ZIP)
  UNSET(LOCAL_ROCKSDB_ZIP CACHE)
ENDMACRO()

MACRO(ECHO_ROCKSDB_VARIABLES)
  MESSAGE(STATUS "ROCKSDB_INCLUDE_DIR ${ROCKSDB_INCLUDE_DIR}")
  MESSAGE(STATUS "LOCAL_ROCKSDB_DIR ${LOCAL_ROCKSDB_DIR}")
  MESSAGE(STATUS "LOCAL_ROCKSDB_ZIP ${LOCAL_ROCKSDB_ZIP}")
ENDMACRO()

MACRO(LOOKUP_OLD_PACKAGE_NAMES)
  FOREACH(pkg ${OLD_PACKAGE_NAMES})
    FIND_FILE(OLD_ROCKSDB_DIR
              NAMES "${pkg}"
              PATHS ${WITH_ROCKSDB}
              NO_DEFAULT_PATH
             )
    IF(OLD_ROCKSDB_DIR)
      MESSAGE(STATUS "")
      MESSAGE(STATUS "Found old rocksdb installation at ${OLD_ROCKSDB_DIR}")
      MESSAGE(STATUS "You must upgrade to ${ROCKSDB_PACKAGE_NAME}")
      MESSAGE(STATUS "")
    ENDIF()
    UNSET(OLD_ROCKSDB_DIR)
    UNSET(OLD_ROCKSDB_DIR CACHE)
  ENDFOREACH()
ENDMACRO()

MACRO(COULD_NOT_FIND_ROCKSDB)
  LOOKUP_OLD_PACKAGE_NAMES()
  ECHO_ROCKSDB_VARIABLES()
  RESET_ROCKSDB_VARIABLES()
  MESSAGE(STATUS "Could not find (the correct version of) rocksdb.")
  MESSAGE(STATUS "MySQL currently requires ${ROCKSDB_PACKAGE_NAME}\n")
  MESSAGE(FATAL_ERROR
    "You can download it with -DDOWNLOAD_ROCKSDB=1 -DWITH_ROCKSDB=<directory>\n"
    "This CMake script will look for rocksdb in <directory>. "
    "If it is not there, it will download and unpack it "
    "(in that directory) for you.\n"
    "If you are inside a firewall, you may need to use an http proxy:\n"
    "export http_proxy=http://example.com:80\n"
    )
ENDMACRO()

# Pick value from environment if not set on command line.
IF(DEFINED ENV{WITH_ROCKSDB} AND NOT DEFINED WITH_ROCKSDB)
  SET(WITH_ROCKSDB "$ENV{WITH_ROCKSDB}")
ENDIF()

# Pick value from environment if not set on command line.
IF(DEFINED ENV{ROCKSDB_ROOT} AND NOT DEFINED WITH_ROCKSDB)
  SET(WITH_ROCKSDB "$ENV{ROCKSDB_ROOT}")
ENDIF()

IF(WITH_ROCKSDB AND WITH_ROCKSDB STREQUAL "system")
  UNSET(WITH_ROCKSDB)
  UNSET(WITH_ROCKSDB CACHE)
ENDIF()

# Update the cache, to make it visible in cmake-gui.
SET(WITH_ROCKSDB ${WITH_ROCKSDB} CACHE PATH
  "Path to rocksdb sources: a directory, or a tarball to be unzipped.")

# If the value of WITH_ROCKSDB changes, we must unset all dependent variables:
IF(OLD_WITH_ROCKSDB)
  IF(NOT "${OLD_WITH_ROCKSDB}" STREQUAL "${WITH_ROCKSDB}")
    RESET_ROCKSDB_VARIABLES()
  ENDIF()
ENDIF()

SET(OLD_WITH_ROCKSDB ${WITH_ROCKSDB} CACHE INTERNAL
  "Previous version of WITH_ROCKSDB" FORCE)

IF (WITH_ROCKSDB)
  ## Did we get a full path name, including file name?
  IF (${WITH_ROCKSDB} MATCHES ".*\\.tar.gz" OR ${WITH_ROCKSDB} MATCHES ".*\\.zip")
    GET_FILENAME_COMPONENT(ROCKSDB_DIR ${WITH_ROCKSDB} PATH)
    GET_FILENAME_COMPONENT(ROCKSDB_ZIP ${WITH_ROCKSDB} NAME)
    FIND_FILE(LOCAL_ROCKSDB_ZIP
              NAMES ${ROCKSDB_ZIP}
              PATHS ${ROCKSDB_DIR}
              NO_DEFAULT_PATH
             )
  ENDIF()
  ## Did we get a path name to the directory of the .tar.gz or .zip file?
  FIND_FILE(LOCAL_ROCKSDB_ZIP
            NAMES "${ROCKSDB_PACKAGE_NAME}.tar.gz" "${ROCKSDB_PACKAGE_NAME}.zip"
            PATHS ${WITH_ROCKSDB}
            NO_DEFAULT_PATH
           )
  ## Did we get a path name to the directory of an unzipped version?
  FIND_FILE(LOCAL_ROCKSDB_DIR
            NAMES "${ROCKSDB_PACKAGE_NAME}"
            PATHS ${WITH_ROCKSDB}
            NO_DEFAULT_PATH
           )
  ## Did we get a path name to an unzippped version?
  FIND_PATH(LOCAL_ROCKSDB_DIR
            NAMES "include/rocksdb/version.h"
            PATHS ${WITH_ROCKSDB}
            NO_DEFAULT_PATH
           )
  IF(LOCAL_ROCKSDB_DIR)
    MESSAGE(STATUS "Local rocksdb dir ${LOCAL_ROCKSDB_DIR}")
  ENDIF()
  IF(LOCAL_ROCKSDB_ZIP)
    MESSAGE(STATUS "Local rocksdb zip ${LOCAL_ROCKSDB_ZIP}")
  ENDIF()
ENDIF()

# There is a similar option in unittest/gunit.
# But the rocksdb tarball is much bigger, so we have a separate option.
OPTION(DOWNLOAD_ROCKSDB "Download rocksdb from sourceforge." OFF)
SET(DOWNLOAD_ROCKSDB_TIMEOUT 6000 CACHE STRING
  "Timeout in seconds when downloading rocksdb.")

# If we could not find it, then maybe download it.
IF(WITH_ROCKSDB AND NOT LOCAL_ROCKSDB_ZIP AND NOT LOCAL_ROCKSDB_DIR)
  IF(NOT DOWNLOAD_ROCKSDB)
    MESSAGE(STATUS "WITH_ROCKSDB=${WITH_ROCKSDB}")
    COULD_NOT_FIND_ROCKSDB()
  ENDIF()
  # Download the tarball
  MESSAGE(STATUS "Downloading ${ROCKSDB_TARBALL} to ${WITH_ROCKSDB}")
  FILE(DOWNLOAD ${ROCKSDB_DOWNLOAD_URL}
    ${WITH_ROCKSDB}/${ROCKSDB_TARBALL}
    TIMEOUT ${DOWNLOAD_ROCKSDB_TIMEOUT}
    STATUS ERR
    SHOW_PROGRESS
  )
  IF(ERR EQUAL 0)
    SET(LOCAL_ROCKSDB_ZIP "${WITH_ROCKSDB}/${ROCKSDB_TARBALL}")
    SET(LOCAL_ROCKSDB_ZIP "${WITH_ROCKSDB}/${ROCKSDB_TARBALL}" CACHE INTERNAL "")
  ELSE()
    MESSAGE(STATUS "Download failed, error: ${ERR}")
    # A failed DOWNLOAD leaves an empty file, remove it
    FILE(REMOVE ${WITH_ROCKSDB}/${ROCKSDB_TARBALL})
    # STATUS returns a list of length 2
    LIST(GET ERR 0 NUMERIC_RETURN)
    IF(NUMERIC_RETURN EQUAL 28)
      MESSAGE(FATAL_ERROR
        "You can try downloading ${ROCKSDB_DOWNLOAD_URL} manually"
        " using curl/wget or a similar tool,"
        " or increase the value of DOWNLOAD_ROCKSDB_TIMEOUT"
        " (which is now ${DOWNLOAD_ROCKSDB_TIMEOUT} seconds)"
      )
    ENDIF()
    MESSAGE(FATAL_ERROR
      "You can try downloading ${ROCKSDB_DOWNLOAD_URL} manually"
      " using curl/wget or a similar tool"
      )
  ENDIF()
ENDIF()

IF(LOCAL_ROCKSDB_ZIP AND NOT LOCAL_ROCKSDB_DIR)
  GET_FILENAME_COMPONENT(LOCAL_ROCKSDB_DIR ${LOCAL_ROCKSDB_ZIP} PATH)
  IF(NOT EXISTS "${LOCAL_ROCKSDB_DIR}/${ROCKSDB_PACKAGE_NAME}")
    MESSAGE(STATUS "cd ${LOCAL_ROCKSDB_DIR}; tar xfz ${LOCAL_ROCKSDB_ZIP}")
    EXECUTE_PROCESS(
      COMMAND ${CMAKE_COMMAND} -E tar xfz "${LOCAL_ROCKSDB_ZIP}"
      WORKING_DIRECTORY "${LOCAL_ROCKSDB_DIR}"
      RESULT_VARIABLE tar_result
      )
    IF (tar_result MATCHES 0)
      SET(ROCKSDB_FOUND 1 CACHE INTERNAL "")
    ELSE()
      MESSAGE(STATUS "WITH_ROCKSDB ${WITH_ROCKSDB}.")
      MESSAGE(STATUS "Failed to extract files.\n"
        "   Please try downloading and extracting yourself.\n"
        "   The url is: ${ROCKSDB_DOWNLOAD_URL}")
      MESSAGE(FATAL_ERROR "Giving up.")
    ENDIF()
  ENDIF()
ENDIF()

# Search for the version file, first in LOCAL_ROCKSDB_DIR or WITH_ROCKSDB
FIND_PATH(ROCKSDB_INCLUDE_DIR
  NAMES "include/rocksdb/version.h"
  NO_DEFAULT_PATH
  PATHS ${LOCAL_ROCKSDB_DIR}
        ${LOCAL_ROCKSDB_DIR}/${ROCKSDB_PACKAGE_NAME}
        ${WITH_ROCKSDB}
)
MESSAGE("found ROCKSDB_INCLUDE_DIR:${ROCKSDB_INCLUDE_DIR}")
# Then search in standard places (if not found above).
FIND_PATH(ROCKSDB_INCLUDE_DIR
  NAMES include/rocksdb/version.h
)

IF(NOT ROCKSDB_INCLUDE_DIR)
  MESSAGE(STATUS
    "Looked for include/rocksdb/version.h in ${LOCAL_ROCKSDB_DIR} and ${WITH_ROCKSDB}")
  COULD_NOT_FIND_ROCKSDB()
ELSE()
  MESSAGE(STATUS "Found ${ROCKSDB_INCLUDE_DIR}/include/rocksdb/version.h ")
ENDIF()

# Verify version number. Version information looks like:
# //  ROCKSDB_PATCH  6 is the patch level
# //  ROCKSDB_MINOR  12 or 14  is the minor version
# //  ROCKSDB_MAJOR  6 is the major version

FILE(STRINGS "${ROCKSDB_INCLUDE_DIR}/include/rocksdb/version.h"
  ROCKSDB_MAJOR_VERSION_STR
  REGEX "^#define[\t ]+ROCKSDB_MAJOR[\t ][0-9]+.*"
)

FILE(STRINGS "${ROCKSDB_INCLUDE_DIR}/include/rocksdb/version.h"
  ROCKSDB_MINOR_VERSION_STR
  REGEX "^#define[\t ]+ROCKSDB_MINOR[\t ][0-9]+.*"
)

MESSAGE(STATUS "current ROCKSDB_VERSION_NUMBER is ${ROCKSDB_MAJOR_VERSION_STR}.${ROCKSDB_MINOR_VERSION_STR}")

STRING(REGEX REPLACE
  "^#define[\t ]+ROCKSDB_MAJOR[\t ]" ""
  ROCKSDB_MAJOR_VERSION "${ROCKSDB_MAJOR_VERSION_STR}")

STRING(REGEX REPLACE
  "^#define[\t ]+ROCKSDB_MINOR[\t ]" ""
  ROCKSDB_MINOR_VERSION "${ROCKSDB_MINOR_VERSION_STR}")

MESSAGE(STATUS "current ROCKSDB_MAJOR_VERSION is: ${ROCKSDB_MAJOR_VERSION}, ROCKSDB_MAJOR_VERSION:${ROCKSDB_MINOR_VERSION}")

IF(NOT ROCKSDB_MAJOR_VERSION EQUAL 6)
  COULD_NOT_FIND_ROCKSDB()
ENDIF()

IF(ROCKSDB_MINOR_VERSION LESS 12)
  MESSAGE(WARNING "rocksdb minor version found is ${ROCKSDB_MINOR_VERSION} "
    "we need minimum 12 "
    )
  COULD_NOT_FIND_ROCKSDB()
ENDIF()

MESSAGE(STATUS "ROCKSDB_INCLUDE_DIR ${ROCKSDB_INCLUDE_DIR}")

# Bug in sqrt(NaN) on 32bit platforms
IF(SIZEOF_VOIDP EQUAL 4)
  ADD_DEFINITIONS(-DROCKSDB_GEOMETRY_SQRT_CHECK_FINITENESS)
ENDIF()

IF(LOCAL_ROCKSDB_DIR OR LOCAL_ROCKSDB_ZIP)
  SET(USING_LOCAL_ROCKSDB 1)
ELSE()
  SET(USING_SYSTEM_ROCKSDB 1)
ENDIF()
