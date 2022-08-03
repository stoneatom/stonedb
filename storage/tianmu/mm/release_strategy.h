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
#ifndef TIANMU_MM_RELEASE_STRATEGY_H_
#define TIANMU_MM_RELEASE_STRATEGY_H_
#pragma once

#include "mm/release_tracker.h"
#include "mm/traceable_object.h"

namespace Tianmu {
namespace mm {

class ReleaseStrategy {
 protected:
  uint64_t m_reloaded;

  void Touch(TraceableObject *o) { o->tracker->touch(o); }
  void UnTrack(TraceableObject *o) { o->tracker->remove(o); }

 public:
  ReleaseStrategy() : m_reloaded(0) {}
  virtual ~ReleaseStrategy() {}
  virtual void Access(TraceableObject *) = 0;
  virtual void Remove(TraceableObject *) = 0;

  virtual void Release(unsigned) = 0;
  virtual void ReleaseFull() = 0;

  virtual unsigned long long getCount1() = 0;
  virtual unsigned long long getCount2() = 0;
  virtual unsigned long long getCount3() = 0;
  virtual unsigned long long getCount4() = 0;
  virtual unsigned long long getReloaded() { return m_reloaded; }
};

}  // namespace mm
}  // namespace Tianmu

#endif  // TIANMU_MM_RELEASE_STRATEGY_H_
