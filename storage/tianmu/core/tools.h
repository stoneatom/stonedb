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
#ifndef TIANMU_CORE_TOOLS_H_
#define TIANMU_CORE_TOOLS_H_
#pragma once

#include <cstdio>
#include <string>

#include "common/assert.h"

namespace Tianmu {
namespace core {
#if !defined(_countof)

template <typename _CountofType, size_t _SizeOfArray>
char (*__countof_helper(_CountofType (&_Array)[_SizeOfArray]))[_SizeOfArray];
#define _countof(_Array) sizeof(*__countof_helper(_Array))

#endif

namespace object_id_helper {
template <typename T, int const no_dims>
struct hasher {
  static size_t calc(T const *);
};

template <typename T>
struct hasher<T, 1> {
  static size_t calc(T const *v) { return (*v); }
};

template <typename T>
struct hasher<T, 2> {
  static size_t calc(T const *v) { return ((v[0] << 4) ^ v[1]); }
};

template <typename T>
struct hasher<T, 3> {
  static size_t calc(T const *v) { return ((v[0] << 6) ^ (v[1] << 4) ^ v[2]); }
};

template <typename T>
struct hasher<T, 4> {
  static size_t calc(T const *v) { return ((v[0] << 8) ^ (v[1] << 6) ^ (v[2] << 4) ^ v[3]); }
};

template <typename T>
struct hasher<T, 5> {
  static size_t calc(T const *v) { return ((v[0] << 8) ^ (v[1] << 6) ^ (v[2] << 4)); }
};
template <typename T>
struct range_eq {
  bool operator()(T f1, T l1, T f2) const { return (std::equal(f1, l1, f2)); }
};

template <typename T>
struct range_less {
  bool operator()(T f1, T l1, T f2) const {
    std::pair<T, T> m(std::mismatch(f1, l1, f2));
    return ((m.first != (l1)) && (*m.first < *m.second));
  }
};

struct empty {};
}  // namespace object_id_helper

enum class COORD_TYPE : int {
  PACK = 0,
  FILTER = 1,
  FTREE = 2,
  kTianmuAttr = 4,
  COORD_UNKNOWN = 999,
};

template <COORD_TYPE id, int const no_dims, typename U = object_id_helper::empty>
class ObjectId : public U {
  int _coord[no_dims];

 public:
  static COORD_TYPE const ID = id;
  ObjectId() : _coord() { std::fill(_coord, _coord + no_dims, 0); }
  ObjectId(int const &a0) : _coord{a0} { static_assert(no_dims == 1, "invalid no. of dims"); }
  ObjectId(int const &a0, int const &a1) : _coord{a0, a1} { static_assert(no_dims == 2, "invalid no. of dims"); }
  ObjectId(int const &a0, int const &a1, int const &a2) : _coord{a0, a1, a2} {
    static_assert(no_dims == 3, "invalid no. of dims");
  }
  ObjectId(int const &a0, int const &a1, int const &a2, int const &a3) : _coord{a0, a1, a2, a3} {
    static_assert(no_dims == 4, "invalid no. of dims");
  }
  ObjectId(int const &a0, int const &a1, int const &a2, int const &a3, int const &a4) : _coord{a0, a1, a2, a3, a4} {
    static_assert(no_dims == 5, "invalid no. of dims");
  }

  bool operator==(ObjectId const &oid) const {
    return object_id_helper::range_eq<int const *>()(_coord, _coord + no_dims, oid._coord);
  }
  bool operator<(ObjectId const &oid) const {
    return object_id_helper::range_less<int const *>()(_coord, _coord + no_dims, oid._coord);
  }
  size_t hash() const { return object_id_helper::hasher<int, no_dims>::calc(_coord); }
  bool operator()(ObjectId const &oid1, ObjectId const &oid2) const {
    return object_id_helper::range_eq<int const *>()(oid1._coord, oid1._coord + no_dims, oid2._coord);
  }
  int const &operator[](int const &idx_) const {
    DEBUG_ASSERT(idx_ < no_dims);
    return _coord[idx_];
  }
  int &operator[](int const &idx_) {
    DEBUG_ASSERT(idx_ < no_dims);
    return _coord[idx_];
  }
  size_t operator()(ObjectId const &oid) const { return oid.hash(); }
  std::string ToString() const {
    std::string str = "[";
    for (auto i : _coord) str += std::to_string(i) + ", ";
    str += "]";
    return str;
  }
};

using PackCoordinate = ObjectId<COORD_TYPE::PACK, 3>;
using FilterCoordinate = ObjectId<COORD_TYPE::FILTER, 5>;  // [ table, column, type, version1, version2 ]
using FTreeCoordinate = ObjectId<COORD_TYPE::FTREE, 3>;
using TianmuAttrCoordinate = ObjectId<COORD_TYPE::kTianmuAttr, 2>;

inline int pc_table(PackCoordinate const &pc) { return (pc[0]); }
inline int pc_column(PackCoordinate const &pc) { return (pc[1]); }
inline int pc_dp(PackCoordinate const &pc) { return (pc[2]); }

/* Create a coordinate type whereby objects of different logical
 * types can be compared and stored in the same datastructure.
 *
 * Short name of TraceableObjectCoordinate
 */
class TOCoordinate {
 public:
  COORD_TYPE ID;

  // since union is not possible.
  // maybe modify ObjectId<> to change its number of dimensions at run time
  // instead
  struct {
    PackCoordinate pack;
    TianmuAttrCoordinate rcattr;
  } co;

  TOCoordinate() : ID(COORD_TYPE::COORD_UNKNOWN) {}
  TOCoordinate(PackCoordinate &_p) : ID(COORD_TYPE::PACK) { co.pack = _p; }
  TOCoordinate(TianmuAttrCoordinate &_r) : ID(COORD_TYPE::kTianmuAttr) { co.rcattr = _r; }
  TOCoordinate(const TOCoordinate &_t) : ID(_t.ID) {
    co.pack = _t.co.pack;
    co.rcattr = _t.co.rcattr;
  }

  TOCoordinate &operator=(const TOCoordinate &) = default;

  bool operator==(TOCoordinate const &oid) const {
    if (oid.ID != ID)
      return false;
    switch (ID) {
      case COORD_TYPE::PACK:
        return co.pack == oid.co.pack;
      case COORD_TYPE::kTianmuAttr:
        return co.rcattr == oid.co.rcattr;
      default:
        ASSERT(false, "Undefined coordinate usage");
        return false;
    };
  }
  bool operator<(TOCoordinate const &oid) const {
    if (oid.ID < ID)
      return false;
    else if (oid.ID > ID)
      return true;
    else
      switch (ID) {
        case COORD_TYPE::PACK:
          return oid.co.pack < co.pack;
        case COORD_TYPE::kTianmuAttr:
          return oid.co.rcattr < co.rcattr;
        default:
          ASSERT(false, "Undefined coordinate usage");
          return false;
      };
  }

  size_t hash() const {
    switch (ID) {
      case COORD_TYPE::PACK:
        return co.pack.hash();
      case COORD_TYPE::kTianmuAttr:
        return co.rcattr.hash();
      default:
        ASSERT(false, "Undefined coordinate usage");
        return false;
    };
  }

  bool operator()(TOCoordinate const &oid1, TOCoordinate const &oid2) const { return oid1 < oid2; }
  int const &operator[](int const &idx_) const {
    switch (ID) {
      case COORD_TYPE::PACK:
        return co.pack[idx_];
      case COORD_TYPE::kTianmuAttr:
        return co.rcattr[idx_];
      default:
        ASSERT(false, "Undefined coordinate usage");
        return co.rcattr[idx_];
    };
  }

  int &operator[](int const &idx_) {
    switch (ID) {
      case COORD_TYPE::PACK:
        return co.pack[idx_];
      case COORD_TYPE::kTianmuAttr:
        return co.rcattr[idx_];
      default:
        ASSERT(false, "Undefined coordinate usage");
        return co.rcattr[idx_];
    };
  }

  size_t operator()(TOCoordinate const &oid) const {
    switch (oid.ID) {
      case COORD_TYPE::PACK:
        return oid.co.pack(oid.co.pack);
      case COORD_TYPE::kTianmuAttr:
        return oid.co.rcattr(oid.co.rcattr);
      default:
        ASSERT(false, "Undefined coordinate usage");
        return co.rcattr(oid.co.rcattr);
    };
  }
  std::string ToString() const {
    std::string str;
    switch (ID) {
      case COORD_TYPE::PACK:
        return "PACK [" + std::to_string(co.pack[0]) + ", " + std::to_string(co.pack[1]) + ", " +
               std::to_string(co.pack[2]) + "]";
      case COORD_TYPE::kTianmuAttr:
        return "Attr [" + std::to_string(co.rcattr[0]) + ", " + std::to_string(co.rcattr[1]) + "]";
      default:
        return "bad COORD type";
    };
  }
};

class FunctionExecutor {
  typedef std::function<void()> F;

 public:
  FunctionExecutor(F call_in_constructor, F call_in_deconstructor)
      : call_in_constructor(call_in_constructor), call_in_deconstructor(call_in_deconstructor) {
    if (call_in_constructor)
      call_in_constructor();
  }

  virtual ~FunctionExecutor() {
    if (call_in_deconstructor)
      call_in_deconstructor();
  }

 private:
  F call_in_constructor;
  F call_in_deconstructor;
};

// #define PROFILE_LOCK_WAITING
#ifdef PROFILE_LOCK_WAITING

class LockProfiler {
 public:
  LockProfiler() {}
  ~LockProfiler() {
    for (auto &it : lock_waiting) {
      cerr << it.first << "\t" << it.second << endl;
    }
    lock_waiting.clear();
  }
  void Record(string where, uint64_t usec) {
    std::scoped_lock guard(lock_profiler_mutex);
    lock_waiting[where] += usec;
  }

 private:
  std::mutex lock_profiler_mutex;
  std::map<string, uint64_t> lock_waiting;
};

extern LockProfiler lock_profiler;

#endif
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_TOOLS_H_