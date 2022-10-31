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
#ifndef TIANMU_CORE_DATA_CACHE_H_
#define TIANMU_CORE_DATA_CACHE_H_
#pragma once

#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "core/pack.h"

namespace Tianmu {
namespace core {
using TraceableObjectPtr = std::shared_ptr<mm::TraceableObject>;

class DataCache final {
 private:
  using PackContainer = std::unordered_map<PackCoordinate, TraceableObjectPtr, PackCoordinate>;
  using IOPackReqSet = std::unordered_set<PackCoordinate, PackCoordinate>;

  using FTreeContainer = std::unordered_map<FTreeCoordinate, TraceableObjectPtr, FTreeCoordinate>;
  using IOFTreeReqSet = std::unordered_set<FTreeCoordinate, FTreeCoordinate>;

  PackContainer _packs;
  FTreeContainer _ftrees;

  int64_t m_cacheHits = 0;
  int64_t m_cacheMisses = 0;
  int64_t m_objectsReleased = 0;

  IOPackReqSet _packPendingIO;
  IOFTreeReqSet _ftreePendingIO;

  std::condition_variable_any _packWaitIO;
  std::condition_variable_any _ftreeWaitIO;

  int64_t m_readWait = 0;
  int64_t m_falseWakeup = 0;
  int64_t m_readWaitInProgress = 0;
  int64_t m_packLoads = 0;
  int64_t m_packLoadInProgress = 0;
  int64_t m_loadErrors = 0;
  int64_t m_reDecompress = 0;

  std::recursive_mutex m_cache_mutex;

 public:
  int64_t getReadWait() { return m_readWait; }
  int64_t getReadWaitInProgress() { return m_readWaitInProgress; }
  int64_t getFalseWakeup() { return m_falseWakeup; }
  int64_t getPackLoads() { return m_packLoads; }
  int64_t getPackLoadInProgress() { return m_packLoadInProgress; }
  int64_t getLoadErrors() { return m_loadErrors; }
  int64_t getReDecompress() { return m_reDecompress; }
  int64_t getCacheHits() { return m_cacheHits; }
  int64_t getCacheMisses() { return m_cacheMisses; }
  int64_t getReleased() { return m_objectsReleased; }
  DataCache() = default;
  ~DataCache() = default;

  void ReleaseAll();

  template <typename T>
  void PutObject(T const &coord_, TraceableObjectPtr p) {
    TraceableObjectPtr old;
    {
      std::scoped_lock m_locking_guard(mm::TraceableObject::GetLockingMutex());
      std::scoped_lock lock(m_cache_mutex);

      p->SetOwner(this);
      auto &c(cache<T>());
      auto result = c.insert(std::make_pair(coord_, p));
      if (!result.second && result.first->second != p) {
        old = result.first->second;  // old object must be physically deleted
                                     // after mutex unlock
        DropObject(coord_);
        result = c.insert(std::make_pair(coord_, p));
      }
      if constexpr (T::ID == COORD_TYPE::PACK) {
        p->TrackAccess();
        // int objnum = c.size();
        // TIANMU_LOG(LogCtl_Level::DEBUG, "PutObject packs objnum %d
        // (table:%d,pack:%d,clounm:%d)", objnum,pc_table(coord_),
        // pc_dp(coord_), pc_column(coord_));
      }
    }
  }

  void ReleaseTable(int id);
  void ReleasePacks(int table);
  void ReleaseFTrees(int table);

  template <typename T>
  void DropObject(T const &coord_) {
    TraceableObjectPtr removed;
    {
      std::scoped_lock m_locking_guard(mm::TraceableObject::GetLockingMutex());
      std::scoped_lock lock(m_cache_mutex);
      auto &c(cache<T>());
      auto it = c.find(coord_);
      if (it != c.end()) {
        // if (T::ID == COORD_TYPE::PACK) {
        // int objnum = c.size();
        // TIANMU_LOG(LogCtl_Level::DEBUG, "DropObject packs objnum %d
        // (table:%d,pack:%d,clounm:%d)", objnum,pc_table(coord_),
        // pc_dp(coord_), pc_column(coord_));
        //}
        removed = it->second;
        if constexpr (T::ID == COORD_TYPE::PACK) {
          removed->Lock();
        }
        removed->SetOwner(nullptr);
        c.erase(it);
        ++m_objectsReleased;
      }
    }
  }

  template <typename T>
  void DropObjectByMM(T const &coord_) {
    TraceableObjectPtr removed;
    {
      std::scoped_lock lock(m_cache_mutex);
      auto &c(cache<T>());

      auto it = c.find(coord_);
      if (it != c.end()) {
        removed = it->second;
        removed->SetOwner(nullptr);
        c.erase(it);
        ++m_objectsReleased;
      }
    }
  }

  template <typename T, typename U>
  std::shared_ptr<T> GetLockedObject(U const &coord_) {
    std::scoped_lock m_locking_guard(mm::TraceableObject::GetLockingMutex());
    std::scoped_lock lock(m_cache_mutex);

    auto &c(cache<U>());
    auto it = c.find(coord_);
    if (it == c.end())
      //++ m_cacheMisses;
      return nullptr;

    //++ m_cacheHits;
    auto sp = std::static_pointer_cast<T>(it->second);
    sp->Lock();
    if constexpr (U::ID == COORD_TYPE::PACK)
      sp->TrackAccess();
    return sp;
  }

  template <typename T, typename U, typename V>
  std::shared_ptr<T> GetOrFetchObject(U const &coord_, V *fetcher_) {
    auto &c(cache<U>());
    auto &w(waitIO<U>());
    auto &cond(condition<U>());
    bool waited = false;
    /* a scope for mutex lock */
    {
      /* Lock is acquired inside */
      std::unique_lock<std::recursive_mutex> m_obj_guard(mm::TraceableObject::GetLockingMutex());

      std::scoped_lock lock(m_cache_mutex);

      auto it = c.find(coord_);

      if (it != c.end()) {
        if constexpr (U::ID == COORD_TYPE::PACK) {
          it->second->Lock();
          ++m_cacheHits;
          it->second->TrackAccess();
        }
        return std::static_pointer_cast<T>(it->second);
      }

      {
        if constexpr (U::ID == COORD_TYPE::PACK)
          m_cacheMisses++;
        auto rit = w.find(coord_);
        while (rit != w.end()) {
          m_readWaitInProgress++;
          if (waited)
            m_falseWakeup++;
          else
            m_readWait++;

          m_cache_mutex.unlock();
          cond.wait(m_obj_guard);
          m_cache_mutex.lock();

          waited = true;
          m_readWaitInProgress--;
          rit = w.find(coord_);
        }
        it = c.find(coord_);
        if (it != c.end()) {
          if constexpr (U::ID == COORD_TYPE::PACK) {
            it->second->Lock();
            it->second->TrackAccess();
          }
          return std::static_pointer_cast<T>(it->second);
        }
        // mm::TraceableObject::GetLockingMutex().Unlock();
        // if we get here the obj has been loaded, used, unlocked
        // and pushed out of memory before we should get to it after
        // waiting
      }
      w.insert(coord_);
      m_packLoadInProgress++;
    }

    std::shared_ptr<T> obj;
    try {
      obj = fetcher_->Fetch(coord_);
    } catch (...) {
      std::scoped_lock m_obj_guard(mm::TraceableObject::GetLockingMutex());
      std::scoped_lock lock(m_cache_mutex);
      m_loadErrors++;
      m_packLoadInProgress--;
      w.erase(coord_);
      cond.notify_all();
      throw;
    }

    {
      obj->SetOwner(this);
      std::scoped_lock m_obj_guard(mm::TraceableObject::GetLockingMutex());
      std::scoped_lock lock(m_cache_mutex);
      if constexpr (U::ID == COORD_TYPE::PACK) {
        m_packLoads++;
        obj->TrackAccess();
      }
      m_packLoadInProgress--;
      DEBUG_ASSERT(c.find(coord_) == c.end());
      c.insert(std::make_pair(coord_, obj));
      w.erase(coord_);
    }
    cond.notify_all();

    return obj;
  }

  template <typename T>
  std::unordered_map<T, TraceableObjectPtr, T> &cache();

  template <typename T>
  std::unordered_set<T, T> &waitIO();

  template <typename T>
  std::condition_variable_any &condition();

  template <typename T>
  std::unordered_map<T, TraceableObjectPtr, T> const &cache() const;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_DATA_CACHE_H_
