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

namespace Tianmu {
namespace mm {

void FIFOTracker::insert(TraceableObject *o) {
  ASSERT(GetRelTracker(o) == nullptr, "Object has multiple trackers");
  ASSERT(GetRelPrev(o) == nullptr, "Object was not removed or initialized properly");
  ASSERT(GetRelNext(o) == nullptr, "Object was not removed or initialized properly");
  SetRelTracker(o, this);
  SetRelPrev(o, nullptr);
  SetRelNext(o, head_);
  if (head_ != nullptr)
    SetRelPrev(head_, o);
  head_ = o;
  if (tail_ == nullptr)
    tail_ = head_;
  size_++;
}

TraceableObject *FIFOTracker::removeTail() {
  TraceableObject *res = tail_;
  if (res == nullptr)
    return nullptr;
  tail_ = GetRelPrev(tail_);
  if (tail_ != nullptr)
    SetRelNext(tail_, nullptr);
  else {
    ASSERT(size_ == 1, "FIFOTracker size error");
    head_ = nullptr;
  }
  SetRelTracker(res, nullptr);
  SetRelPrev(res, nullptr);
  SetRelNext(res, nullptr);
  size_--;
  return res;
}

TraceableObject *FIFOTracker::removeHead() {
  TraceableObject *res = head_;
  if (res == nullptr)
    return nullptr;
  head_ = GetRelNext(head_);
  if (head_ != nullptr)
    SetRelPrev(head_, nullptr);
  else {
    ASSERT(size_ == 1, "FIFOTracker size error");
    tail_ = nullptr;
  }
  SetRelTracker(res, nullptr);
  SetRelPrev(res, nullptr);
  SetRelNext(res, nullptr);
  size_--;
  return res;
}

void FIFOTracker::remove(TraceableObject *o) {
  ASSERT(GetRelTracker(o) == this, "Removing object from wrong tracker");
  SetRelTracker(o, nullptr);
  if ((o == head_) && (o == tail_)) {
    head_ = tail_ = nullptr;
  } else if (o == head_) {
    head_ = GetRelNext(o);
    SetRelPrev(head_, nullptr);
  } else if (o == tail_) {
    tail_ = GetRelPrev(o);
    SetRelNext(tail_, nullptr);
  } else {
    TraceableObject *p = GetRelPrev(o), *n = GetRelNext(o);
    SetRelNext(p, n);
    SetRelPrev(n, p);
  }
  SetRelNext(o, nullptr);
  SetRelPrev(o, nullptr);
  size_--;
}

void FIFOTracker::touch(TraceableObject *) {
  // do nothing
}

void LRUTracker::touch(TraceableObject *o) {
  remove(o);
  insert(o);
}

}  // namespace mm
}  // namespace Tianmu
