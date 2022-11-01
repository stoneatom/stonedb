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
#ifndef TIANMU_MM_MM_GUARD_H_
#define TIANMU_MM_MM_GUARD_H_
#pragma once
#include "mm/traceable_object.h"

namespace Tianmu {
namespace mm {

template <typename T>
class MMGuard;

class TraceableObject;

template <typename T>
class MMGuardRef {
 private:
  explicit MMGuardRef(T *data, TraceableObject &owner, bool call_delete = true)
      : data(data), owner(&owner), call_delete(call_delete) {}
  T *data;
  TraceableObject *owner;
  bool call_delete;
  friend class MMGuard<T>;
};

template <typename T>
struct debunk_void {
  typedef T &type;
};

template <>
struct debunk_void<void> {
  typedef void *type;
};

// Friend class of TraceableObject that can use MM functions that do accounting
class TraceableAccounting {
 public:
  TraceableAccounting(TraceableObject *o) : owner(o) {}
  virtual ~TraceableAccounting() {}

 protected:
  void dealloc(void *p) { owner->dealloc(p); }
  TraceableObject *owner;
};

template <typename T>
class MMGuard final : public TraceableAccounting {
 public:
  MMGuard() : TraceableAccounting(0), data(0), call_delete(false) {}
  explicit MMGuard(T *data, TraceableObject &owner, bool call_delete = true)
      : TraceableAccounting(&owner), data(data), call_delete(call_delete) {}

  MMGuard(MMGuard &mm_guard)
      : TraceableAccounting(mm_guard.owner), data(mm_guard.data), call_delete(mm_guard.call_delete) {
    mm_guard.call_delete = false;
  }

  virtual ~MMGuard() { reset(); }
  MMGuard &operator=(MMGuardRef<T> const &mm_guard) {
    reset();
    data = mm_guard.data;
    owner = mm_guard.owner;
    call_delete = mm_guard.call_delete;
    return *this;
  }

  MMGuard &operator=(MMGuard<T> &mm_guard) {
    if (&mm_guard != this) {
      reset();
      data = mm_guard.data;
      owner = mm_guard.owner;
      call_delete = mm_guard.call_delete;
      mm_guard.call_delete = false;
    }
    return *this;
  }

  operator MMGuardRef<T>() {
    MMGuardRef<T> mmr(data, *owner, call_delete);
    call_delete = false;
    return (mmr);
  }

  typename debunk_void<T>::type operator*() const { return *data; }
  T *get() const { return data; }
  T *operator->() const { return data; }
  typename debunk_void<T>::type operator[](uint i) const { return data[i]; }
  T *release() {
    T *tmp = data;
    data = 0;
    owner = 0;
    call_delete = false;
    return tmp;
  }

  void reset() {
    if (call_delete && data)
      dealloc(data);
    data = 0;
    owner = 0;
    call_delete = false;
  }

 private:
  T *data;
  bool call_delete;
};

}  // namespace mm
}  // namespace Tianmu

#endif  // TIANMU_MM_MM_GUARD_H_
