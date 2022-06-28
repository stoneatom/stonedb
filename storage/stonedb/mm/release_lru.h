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
#ifndef STONEDB_MM_RELEASE_LRU_H_
#define STONEDB_MM_RELEASE_LRU_H_
#pragma once

#include "mm/release_strategy.h"
#include "mm/release_tracker.h"

namespace stonedb {
namespace mm {

class TraceableObject;

class ReleaseLRU : public ReleaseStrategy {
  LRUTracker tracker;

 public:
  ReleaseLRU() : ReleaseStrategy() {}
  void Access(TraceableObject *) override;
  void Remove(TraceableObject *) override;

  void Release(unsigned) override;
  void ReleaseFull() override;

  unsigned long long getCount1() override { return tracker.size(); }
  unsigned long long getCount2() override { return 0; }
  unsigned long long getCount3() override { return 0; }
  unsigned long long getCount4() override { return 0; }
};

}  // namespace mm
}  // namespace stonedb

#endif  // STONEDB_MM_RELEASE_LRU_H_
