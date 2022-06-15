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
#include "release2q.h"

#include <algorithm>

#include "core/data_cache.h"
#include "core/pack.h"
#include "mm/reference_object.h"
#include "mm/release_strategy.h"
#include "mm/release_tracker.h"

namespace stonedb {
namespace mm {

void Release2Q::Access(TraceableObject *o) {
  // ASSERT(o->TraceableType() == TO_TYPE::TO_PACK, "Access wrong type");
  if (!o->IsTracked()) {
    auto it = A1outLookup.find(o->GetCoordinate());
    if (it != A1outLookup.end()) {
      m_reloaded++;
      TraceableObject *d = it->second;
      Am.insert(o);
      A1out.remove(d);
      A1outLookup.erase(it);
      delete d;
    } else {
      A1in.insert(o);
    }
  } else {
    Touch(o);
  }
}

/*
 * Try to drop packs by keeping A1in and Am equal in size
 */
void Release2Q::Release(unsigned num_objs) {
  uint count = 0;
  for (int max_loop = std::max(A1in.size(), Am.size()); (max_loop > 0) && (count < num_objs); max_loop--) {
    if (A1in.size() >= Am.size()) {
      if (A1in.size() == 0) break;

      TraceableObject *o = A1in.removeTail();
      if (o->IsLocked()) {
        Am.insert(o);
      } else {
        ReferenceObject *to = new ReferenceObject(o->GetCoordinate());
        ASSERT(!to->IsTracked(), "Tracking error");
        // ASSERT(to->GetCoordinate().ID == sdb::COORD_TYPE::PACK,
        // "ReferenceObject improperly constructed");
        A1out.insert(to);
        A1outLookup.insert(std::make_pair(to->GetCoordinate(), to));
        o->Release();
        count++;
        if (A1out.size() > Kout) {
          TraceableObject *ao = A1out.removeTail();
          A1outLookup.erase(ao->GetCoordinate());
          delete ao;
        }
      }
    } else {
      TraceableObject *o = Am.removeMax();
      if (o->IsLocked()) {
        Am.insert(o);
        continue;
      }
      o->Release();
      count++;
    }
  }
}

void Release2Q::ReleaseFull() {
  while (A1in.size() > 0) {
    TraceableObject *o = A1in.removeTail();
    if (o->IsLocked()) {
      Am.insert(o);
    } else {
      A1out.insert(new ReferenceObject(o->GetCoordinate()));
      o->Release();
      if (A1out.size() > Kout) {
        TraceableObject *ao = A1out.removeTail();
        A1outLookup.erase(ao->GetCoordinate());
        delete ao;
      }
    }
  }

  while (Am.size() > 0) {
    TraceableObject *o = Am.removeMax();
    if (o->IsLocked()) {
      Am.insert(o);
      // not guaranteed that the rest of Am is locked, but close enough?
      break;
    } else {
      o->Release();
    }
  }
}

}  // namespace mm
}  // namespace stonedb
