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
#ifndef STONEDB_MM_RELEASE_TRACKER_H_
#define STONEDB_MM_RELEASE_TRACKER_H_
#pragma once

#include "mm/traceable_object.h"

namespace stonedb {
namespace mm {

class ReleaseTracker {
 protected:
  unsigned _size;

  // Access the tracking structures inside Releasable objects from friend
  // relationship
  inline TraceableObject *GetRelNext(TraceableObject *o) { return o->next; }
  inline TraceableObject *GetRelPrev(TraceableObject *o) { return o->prev; }
  inline ReleaseTracker *GetRelTracker(TraceableObject *o) { return o->tracker; }
  inline void SetRelNext(TraceableObject *o, TraceableObject *v) { o->next = v; }
  inline void SetRelPrev(TraceableObject *o, TraceableObject *v) { o->prev = v; }
  inline void SetRelTracker(TraceableObject *o, ReleaseTracker *v) { o->tracker = v; }

 public:
  ReleaseTracker() : _size(0) {}
  virtual ~ReleaseTracker() {}
  virtual void insert(TraceableObject *) = 0;
  virtual void remove(TraceableObject *) = 0;
  virtual void touch(TraceableObject *) = 0;
  unsigned size() { return _size; }
};

class FIFOTracker : public ReleaseTracker {
  TraceableObject *head, *tail;

 public:
  FIFOTracker() : ReleaseTracker(), head(0), tail(0) {}
  void insert(TraceableObject *) override;
  void remove(TraceableObject *) override;
  void touch(TraceableObject *) override;

  TraceableObject *removeHead();
  TraceableObject *removeTail();
};

class LRUTracker : public FIFOTracker {
 public:
  TraceableObject *removeMin() { return removeHead(); }
  TraceableObject *removeMax() { return removeTail(); }
  void touch(TraceableObject *) override;
};

}  // namespace mm
}  // namespace stonedb

#endif  // STONEDB_MM_RELEASE_TRACKER_H_
