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
#ifndef TIANMU_MM_RELEASE2Q_H_
#define TIANMU_MM_RELEASE2Q_H_
#pragma once

#include <unordered_map>

#include "mm/reference_object.h"
#include "mm/release_strategy.h"
#include "mm/release_tracker.h"

namespace Tianmu {
namespace mm {

class TraceableObject;
/*
 * Classic 2Q algorithm from Johnson and Shasha
 *
 * Full version 2Q:
 *
 * On reclaiming for X:
 *  begin
 *      if there are free page slots then
 *          put X into a free page slot
 *      else if (|Alin| >Kin)
 *          page out the tail of Alin, call it Y
 *          add identifier of Y to the head of Alout
 *
 *          if (|Alout| >Kout)
 *              remove identifier of Z from the tail of Alout
 *          end if
 *
 *          put X into the reclaimed page slot
 *      else
 *          page out the tail of Am, call it Y
 *          // do not put it on Alout; it hasn’t been accessed for a while
 *          put X into the reclaimed page slot
 *      end if
 *  end
 *
 *
 * On accessing:
 *  begin
 *      if X is in Am then
 *          move X to the head of Am
 *      else if (X is in Alout) then
 *          reclaim
 *          add X to the head of Am
 *      else if (X is in Alin)
 *          // do nothing
 *      else
 *          reclaim
 *          add X to the head of Alin
 *      end if
 *  end
 *
 * Modifications: objects are released on request only and in such a way that
 *                |A1in| and |Am| become as close to equal as possible.
 */
class Release2Q : public ReleaseStrategy {
  // Using names from the original paper, maybe we shouldn't?
  FIFOTracker A1in, A1out;
  LRUTracker Am;
  unsigned Kin, Kout, KminAm;
  // needs to be a hash map pointing to an element on A1out
  std::unordered_map<core::TOCoordinate, ReferenceObject *, core::TOCoordinate> A1outLookup;

 public:
  Release2Q(unsigned kin, unsigned kout, unsigned kminam) : Kin(kin), Kout(kout), KminAm(kminam) {}
  ~Release2Q() {
    for (const auto &it : A1outLookup) {
      delete it.second;
    }
    A1outLookup.clear();
  }

  void Access(TraceableObject *o) override;
  void Remove(TraceableObject *o) override { UnTrack(o); }
  void Release(unsigned) override;
  void ReleaseFull() override;

  unsigned long long getCount1() override { return Am.size(); }
  unsigned long long getCount2() override { return A1in.size(); }
  unsigned long long getCount3() override { return A1out.size(); }
  unsigned long long getCount4() override { return A1outLookup.size(); }
};

}  // namespace mm
}  // namespace Tianmu

#endif  // TIANMU_MM_RELEASE2Q_H_
