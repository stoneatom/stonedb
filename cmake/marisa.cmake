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

# We want marisa in order to build our stonedb code.
# The marisa tarball is fairly big, and takes several minutes
# to download. So we recommend downloading/unpacking it
# only once, in a place visible from any bzr sandbox.
# We use only header files, so there should be no binary dependencies.

# Invoke with -DWITH_MARISA=<directory> or set WITH_MARISA in environment.
# If WITH_MARISA is *not* set, or is set to the special value "system",
# we assume that the correct version (see below)
# is installed on the compile host in the standard location.

SET(MARISA_PACKAGE_NAME "marisa-trie")
SET(MARISA_DOWNLOAD_URL
  "https://github.com/s-yata/marisa-trie.gi"
  )
SET(MARISA_TARBALL "${MARISA_PACKAGE_NAME}")

SET(OLD_PACKAGE_NAMES "")

MACRO(RESET_MARISA_VARIABLES)
  UNSET(MARISA_INCLUDE_DIR)
  UNSET(MARISA_INCLUDE_DIR CACHE)
  UNSET(LOCAL_MARISA_DIR)
  UNSET(LOCAL_MARISA_DIR CACHE)
ENDMACRO()

MACRO(ECHO_MARISA_VARIABLES)
  MESSAGE(STATUS "MARISA_INCLUDE_DIR ${MARISA_INCLUDE_DIR}")
  MESSAGE(STATUS "LOCAL_MARISA_DIR ${LOCAL_MARISA_DIR}")
ENDMACRO()

MACRO(COULD_NOT_FIND_MARISA)
  ECHO_MARISA_VARIABLES()
  RESET_MARISA_VARIABLES()
  MESSAGE(STATUS "Could not find (the correct version of) marisa.")
  MESSAGE(STATUS "MySQL currently requires ${MARISA_PACKAGE_NAME}\n")
  MESSAGE(FATAL_ERROR
    "You can download it, install it, then specify the marisa path "
    "with -DWITH_MARISA=<director>\n"
    "This CMake script will look for marisa in <directory>. "
    )
ENDMACRO()

# Pick value from environment if not set on command line.
IF(DEFINED ENV{WITH_MARISA} AND NOT DEFINED WITH_MARISA)
  SET(WITH_MARISA "$ENV{WITH_MARISA}")
ENDIF()

# Pick value from environment if not set on command line.
IF(DEFINED ENV{MARISA_ROOT} AND NOT DEFINED WITH_MARISA)
  SET(WITH_MARISA "$ENV{MARISA_ROOT}")
ENDIF()

IF(WITH_MARISA AND WITH_MARISA STREQUAL "system")
  UNSET(WITH_MARISA)
  UNSET(WITH_MARISA CACHE)
ENDIF()

# Update the cache, to make it visible in cmake-gui.
SET(WITH_MARISA ${WITH_MARISA} CACHE PATH
  "Path to marisa sources: a directory, or a tarball to be unzipped.")

# If the value of WITH_MARISA changes, we must unset all dependent variables:
IF(OLD_WITH_MARISA)
  IF(NOT "${OLD_WITH_MARISA}" STREQUAL "${WITH_MARISA}")
    RESET_MARISA_VARIABLES()
  ENDIF()
ENDIF()

SET(OLD_WITH_MARISA ${WITH_MARISA} CACHE INTERNAL
  "Previous version of WITH_MARISA" FORCE)

IF (WITH_MARISA)
  ## Did we get a path name to an unzippped version?
  FIND_PATH(LOCAL_MARISA_DIR
            NAMES "include/marisa.h"
            PATHS ${WITH_MARISA}
            NO_DEFAULT_PATH
           )
  IF(LOCAL_MARISA_DIR)
    MESSAGE(STATUS "Local marisa dir ${LOCAL_MARISA_DIR}")
  ENDIF()
ENDIF()

# Search for the header file, first in LOCAL_MARISA_DIR or WITH_MARISA
FIND_PATH(MARISA_INCLUDE_DIR
  NAMES "include/marisa.h"
  NO_DEFAULT_PATH
  PATHS ${LOCAL_MARISA_DIR}
        ${LOCAL_MARISA_DIR}/${MARISA_PACKAGE_NAME}
        ${WITH_MARISA}
)
MESSAGE("found MARISA_INCLUDE_DIR:${MARISA_INCLUDE_DIR}")
# Then search in standard places (if not found above).
FIND_PATH(MARISA_INCLUDE_DIR
  NAMES include/marisa.h
)
IF(NOT MARISA_INCLUDE_DIR)
  MESSAGE(STATUS
    "Looked for include/marisa.h in ${LOCAL_MARISA_DIR} and ${WITH_MARISA}")
  COULD_NOT_FIND_MARISA()
ELSE()
  MESSAGE(STATUS "Found ${MARISA_INCLUDE_DIR}/include/marisa.h ")
ENDIF()

MESSAGE(STATUS "MARISA_INCLUDE_DIR ${MARISA_INCLUDE_DIR}")

# Bug in sqrt(NaN) on 32bit platforms
IF(SIZEOF_VOIDP EQUAL 4)
  ADD_DEFINITIONS(-DMARISA_GEOMETRY_SQRT_CHECK_FINITENESS)
ENDIF()

IF(LOCAL_MARISA_DIR OR LOCAL_MARISA_ZIP)
  SET(USING_LOCAL_MARISA 1)
ELSE()
  SET(USING_SYSTEM_MARISA 1)
ENDIF()
