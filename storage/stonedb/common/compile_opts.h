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
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335  USA
*/

#ifndef STONEDB_COMMON_COMPILE_OPTS_H_
#pragma once

namespace stonedb {

#ifdef __clang__
// in clang
#elif __GNUC__
//
//__GNUC__              // major
//__GNUC_MINOR__        // minor
//__GNUC_PATCHLEVEL__     // patch
// C++0x features
//
#if (__GNUC__ > 4 && __GUNC__ < 7) || (__GNUC__ == 4 && __GNUC_MINOR__ > 2)
// C++0x features are only enabled when -std=c++0x or -std=gnu++0x are
// passed on the command line, which in turn defines
// __GXX_EXPERIMENTAL_CXX0X__.
#define STONEDB_UNUSED [[gnu::unused]]
#elif __GNUC__ > 7
// support in c++17
#define STONEDB__UNUSED [[maybe_unused]]
#else
#define STONEDB_UNUSED __attribute__((unused))
#endif
#elif _MSC_VER
/*usually has the version number in _MSC_VER*/
/*code specific to MSVC compiler*/
#elif __BORLANDC__
#elif __MINGW32__
#endif

}  // namespace stonedb

#endif  //  STONEDB_COMMON_COMPILE_OPTS_H_
