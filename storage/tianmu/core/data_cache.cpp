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

#include "data_cache.h"

#include "core/tianmu_attr.h"

namespace Tianmu {
namespace core {
void DataCache::ReleaseAll() {
  _packs.clear();
  _ftrees.clear();
}

// release all data for table id
void DataCache::ReleaseTable(int id) {
  ReleasePacks(id);
  ReleaseFTrees(id);
}

void DataCache::ReleasePacks(int table) {
  std::vector<TraceableObjectPtr> packs_removed;
  {
    std::scoped_lock m_locking_guard(mm::TraceableObject::GetLockingMutex());
    std::scoped_lock lock(m_cache_mutex);
    auto it = _packs.begin();
    while (it != _packs.end()) {
      if (pc_table(it->first) == table) {
        auto tmp = it++;
        std::shared_ptr<Pack> pack = std::static_pointer_cast<Pack>(tmp->second);
        pack->Lock();
        pack->SetOwner(0);
        _packs.erase(tmp);
        packs_removed.push_back(pack);
        m_objectsReleased++;
      } else
        it++;
    }
  }
}

void DataCache::ReleaseFTrees(int table) {
  std::vector<TraceableObjectPtr> to_remove;
  {
    std::scoped_lock m_locking_guard(mm::TraceableObject::GetLockingMutex());
    std::scoped_lock lock(m_cache_mutex);
    auto it = _ftrees.begin();
    while (it != _ftrees.end()) {
      if (it->first[0] == table) {
        auto tmp = it++;
        auto sp = tmp->second;
        sp->Lock();
        sp->SetOwner(0);
        _ftrees.erase(tmp);
        to_remove.push_back(sp);
        m_objectsReleased++;
      } else
        it++;
    }
  }
}

template <>
DataCache::PackContainer &DataCache::cache() {
  return (_packs);
}

template <>
DataCache::PackContainer const &DataCache::cache() const {
  return (_packs);
}

template <>
DataCache::FTreeContainer &DataCache::cache() {
  return (_ftrees);
}

template <>
DataCache::FTreeContainer const &DataCache::cache() const {
  return (_ftrees);
}

template <>
DataCache::IOPackReqSet &DataCache::waitIO() {
  return (_packPendingIO);
}

template <>
DataCache::IOFTreeReqSet &DataCache::waitIO() {
  return (_ftreePendingIO);
}

template <>
std::condition_variable_any &DataCache::condition<PackCoordinate>() {
  return (_packWaitIO);
}

template <>
std::condition_variable_any &DataCache::condition<FTreeCoordinate>() {
  return (_ftreeWaitIO);
}
}  // namespace core
}  // namespace Tianmu
