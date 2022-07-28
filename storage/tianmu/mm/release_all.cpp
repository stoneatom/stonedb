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

#include "release_all.h"

#include "core/data_cache.h"
#include "mm/traceable_object.h"

namespace Tianmu {
namespace mm {

void ReleaseALL::Access(TraceableObject *o) { trackable.touch(o); }

void ReleaseALL::Remove(TraceableObject *o) { trackable.remove(o); }

void ReleaseALL::Release([[maybe_unused]] unsigned sz) {
  uint size = trackable.size();
  TraceableObject *o;
  for (uint i = 0; i < size; i++) {
    o = trackable.removeTail();
    if (o->IsLocked())
      trackable.touch(o);
    else
      o->Release();
  }
}

void ReleaseALL::ReleaseFull() { Release(0); }

}  // namespace mm
}  // namespace Tianmu
