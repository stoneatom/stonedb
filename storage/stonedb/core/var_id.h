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
#ifndef STONEDB_CORE_VAR_ID_H_
#define STONEDB_CORE_VAR_ID_H_
#pragma once

namespace stonedb {
namespace core {
struct VarID {
  int tab;
  int col;
  VarID() : tab(0), col(0) {}
  VarID(int t, int c) : tab(t), col(c) {}
  // this is necessary if VarID is used as a key in std::map
  bool operator<(const VarID &cd) const { return tab < cd.tab || (tab == cd.tab && col < cd.col); }
  bool operator==(const VarID &cd) const { return tab == cd.tab && col == cd.col; }
};

}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_VAR_ID_H_
