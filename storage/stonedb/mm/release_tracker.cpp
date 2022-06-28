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

#include "mm/release_tracker.h"

#include "common/assert.h"
#include "mm/traceable_object.h"

namespace stonedb {
namespace mm {

void FIFOTracker::insert(TraceableObject *o) {
  ASSERT(GetRelTracker(o) == NULL, "Object has multiple trackers");
  ASSERT(GetRelPrev(o) == NULL, "Object was not removed or initialized properly");
  ASSERT(GetRelNext(o) == NULL, "Object was not removed or initialized properly");
  SetRelTracker(o, this);
  SetRelPrev(o, NULL);
  SetRelNext(o, head);
  if (head != NULL) SetRelPrev(head, o);
  head = o;
  if (tail == NULL) tail = head;
  _size++;
}

TraceableObject *FIFOTracker::removeTail() {
  TraceableObject *res = tail;
  if (res == NULL) return NULL;
  tail = GetRelPrev(tail);
  if (tail != NULL)
    SetRelNext(tail, NULL);
  else {
    ASSERT(_size == 1, "FIFOTracker size error");
    head = NULL;
  }
  SetRelTracker(res, NULL);
  SetRelPrev(res, NULL);
  SetRelNext(res, NULL);
  _size--;
  return res;
}

TraceableObject *FIFOTracker::removeHead() {
  TraceableObject *res = head;
  if (res == NULL) return NULL;
  head = GetRelNext(head);
  if (head != NULL)
    SetRelPrev(head, NULL);
  else {
    ASSERT(_size == 1, "FIFOTracker size error");
    tail = NULL;
  }
  SetRelTracker(res, NULL);
  SetRelPrev(res, NULL);
  SetRelNext(res, NULL);
  _size--;
  return res;
}

void FIFOTracker::remove(TraceableObject *o) {
  ASSERT(GetRelTracker(o) == this, "Removing object from wrong tracker");
  SetRelTracker(o, NULL);
  if ((o == head) && (o == tail)) {
    head = tail = NULL;
  } else if (o == head) {
    head = GetRelNext(o);
    SetRelPrev(head, NULL);
  } else if (o == tail) {
    tail = GetRelPrev(o);
    SetRelNext(tail, NULL);
  } else {
    TraceableObject *p = GetRelPrev(o), *n = GetRelNext(o);
    SetRelNext(p, n);
    SetRelPrev(n, p);
  }
  SetRelNext(o, NULL);
  SetRelPrev(o, NULL);
  _size--;
}

void FIFOTracker::touch(TraceableObject *) {
  // do nothing
}

void LRUTracker::touch(TraceableObject *o) {
  remove(o);
  insert(o);
}

}  // namespace mm
}  // namespace stonedb
