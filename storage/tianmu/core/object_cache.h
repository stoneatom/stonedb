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
#ifndef TIANMU_CORE_OBJECT_CACHE_H_
#define TIANMU_CORE_OBJECT_CACHE_H_
#pragma once

#include <mutex>
#include <unordered_map>
#include <utility>

#include "system/tianmu_system.h"

namespace Tianmu {
namespace core {
template <class Key, class T, class Hash = std::hash<Key>>
class ObjectCache final {
 private:
  std::unordered_map<Key, std::weak_ptr<T>, Hash> cache;
  std::mutex mtx;

 public:
  ObjectCache() = default;
  ~ObjectCache() = default;

  std::shared_ptr<T> Get(const Key &k, std::function<std::shared_ptr<T>(const Key &)> creator = {}) {
    std::scoped_lock lck(mtx);

    auto sp = cache[k].lock();
    if (sp)
      return sp;

    if (!creator)
      return nullptr;
    sp = creator(k);
    cache[k] = sp;
    return sp;
  }

  void RemoveIf(std::function<bool(const Key &)> predicate) {
    std::scoped_lock lck(mtx);

    // for (auto &i : cache) {
    //    rclog << lock << "[" << i.first.ToString() << "] " <<
    //    system::unlock;
    // }
    auto it = cache.begin();
    while (it != cache.end()) {
      if (predicate(it->first)) {
        // rclog << lock << "remove cache " << it->first.ToString() <<
        // system::unlock;
        it = cache.erase(it);
      } else {
        ++it;
      }
    }
  }
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_OBJECT_CACHE_H_
