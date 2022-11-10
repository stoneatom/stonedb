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
      : data_(data), owner_(&owner), call_delete_(call_delete) {}
  T *data_;
  TraceableObject *owner_;
  bool call_delete_;
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
  TraceableAccounting(TraceableObject *o) : owner_(o) {}
  virtual ~TraceableAccounting() {}

 protected:
  void dealloc(void *p) { owner_->dealloc(p); }
  TraceableObject *owner_;
};

template <typename T>
class MMGuard final : public TraceableAccounting {
 public:
  MMGuard() : TraceableAccounting(0), data_(0), call_delete_(false) {}
  explicit MMGuard(T *data, TraceableObject &owner, bool call_delete = true)
      : TraceableAccounting(&owner), data_(data), call_delete_(call_delete) {}

  MMGuard(MMGuard &mm_guard)
      : TraceableAccounting(mm_guard.owner_), data_(mm_guard.data_), call_delete_(mm_guard.call_delete_) {
    mm_guard.call_delete_ = false;
  }

  virtual ~MMGuard() { reset(); }
  MMGuard &operator=(MMGuardRef<T> const &mm_guard) {
    reset();
    data_ = mm_guard.data_;
    owner_ = mm_guard.owner_;
    call_delete_ = mm_guard.call_delete_;
    return *this;
  }

  MMGuard &operator=(MMGuard<T> &mm_guard) {
    if (&mm_guard != this) {
      reset();
      data_ = mm_guard.data_;
      owner_ = mm_guard.owner_;
      call_delete_ = mm_guard.call_delete_;
      mm_guard.call_delete_ = false;
    }
    return *this;
  }

  operator MMGuardRef<T>() {
    MMGuardRef<T> mmr(data_, *owner_, call_delete_);
    call_delete_ = false;
    return (mmr);
  }

  typename debunk_void<T>::type operator*() const { return *data_; }
  T *get() const { return data_; }
  T *operator->() const { return data_; }
  typename debunk_void<T>::type operator[](uint i) const { return data_[i]; }
  T *release() {
    T *tmp = data_;
    data_ = 0;
    owner_ = 0;
    call_delete_ = false;
    return tmp;
  }

  void reset() {
    if (call_delete_ && data_)
      dealloc(data_);
    data_ = 0;
    owner_ = 0;
    call_delete_ = false;
  }

 private:
  T *data_;
  bool call_delete_;
};

}  // namespace mm
}  // namespace Tianmu

#endif  // TIANMU_MM_MM_GUARD_H_
