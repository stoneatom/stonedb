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

#include "cached_buffer.h"

#include "core/transaction.h"

namespace Tianmu {
namespace core {
template <class T>
CachedBuffer<T>::CachedBuffer(uint page_size, uint _elem_size, Transaction *conn)
    : system::CacheableItem("PS", "CB"), page_size(page_size), elem_size(_elem_size), m_conn(conn) {
  if (!elem_size)
    elem_size = sizeof(T);
  CI_SetDefaultSize(page_size * elem_size);

  buf = (T *)alloc(sizeof(T) * (size_t)page_size, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
  std::memset(buf, 0, sizeof(T) * (size_t)page_size);
  loaded_page = 0;
  page_changed = false;
}

CachedBuffer<types::BString>::CachedBuffer(uint page_size, uint elem_size, Transaction *conn)
    : system::CacheableItem("PS", "CB"), page_size(page_size), elem_size(elem_size), m_conn(conn) {
  // DEBUG_ASSERT(elem_size);
  CI_SetDefaultSize(page_size * (elem_size + 4));

  size_t buf_size = sizeof(char) * (size_t)page_size * (elem_size + 4);
  if (buf_size) {
    buf = (char *)alloc(buf_size, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
    std::memset(buf, 0, buf_size);
  } else
    buf = nullptr;
  loaded_page = 0;
  page_changed = false;
}

template <class T>
CachedBuffer<T>::~CachedBuffer() {
  dealloc(buf);
}

CachedBuffer<types::BString>::~CachedBuffer() { dealloc(buf); }

template <class T>
T &CachedBuffer<T>::Get(uint64_t idx) {
  DEBUG_ASSERT(page_size > 0);
  if (idx / page_size != loaded_page)
    LoadPage((uint)(idx / page_size));
  return buf[idx % page_size];
}

void CachedBuffer<types::BString>::Get(types::BString &s, uint64_t idx) {
  if (idx / page_size != loaded_page)
    LoadPage((uint)(idx / page_size));
  uint64_t pos = (idx % page_size) * (elem_size + 4);
  uint size = *(uint *)&buf[pos];
  if (size == common::NULL_VALUE_U)  // 0 -nullptr, 1 -NOT nullptr
    s = types::BString();
  else if (size == 0)
    s = types::BString(ZERO_LENGTH_STRING, 0);
  else
    s = types::BString(&buf[pos + 4], size);
}

types::BString &CachedBuffer<types::BString>::Get(uint64_t idx) {
  Get(bufS, idx);
  return bufS;
}

template <class T>
void CachedBuffer<T>::Set(uint64_t idx, const T &value) {
  if (idx / page_size != loaded_page)
    LoadPage((uint)(idx / page_size));
  buf[idx % page_size] = value;
  page_changed = true;
}

void CachedBuffer<types::BString>::Set(uint64_t idx, const types::BString &value) {
  DEBUG_ASSERT(value.len_ <= elem_size);
  if (idx / page_size != loaded_page)
    LoadPage((uint)(idx / page_size));
  uint pos = (uint)(idx % page_size) * (elem_size + 4);
  uint *size = (uint *)&buf[pos];
  if (value.IsNull())
    *size = common::NULL_VALUE_U;
  else {
    *size = value.len_;
    std::memcpy(&buf[pos + 4], value.val_, value.len_);
  }
  page_changed = true;
}

template <class T>
void CachedBuffer<T>::LoadPage(uint page) {
  if (m_conn && m_conn->Killed())
    throw common::KilledException();  // cleaning is implemented below (we are
                                      // inside try{})
  if (page_changed) {                 // Save current page
    CI_Put(loaded_page, (unsigned char *)buf);
    page_changed = false;
  }
  // load new page to memory
  CI_Get(page, (unsigned char *)buf);
  loaded_page = page;
}

void CachedBuffer<types::BString>::LoadPage(uint page) {
  if (m_conn && m_conn->Killed())
    throw common::KilledException();  // cleaning is implemented below (we are
                                      // inside try{})
  if (page_changed) {                 // Save current page
    CI_Put(loaded_page, (unsigned char *)buf);
    page_changed = false;
  }
  // load new page to memory
  CI_Get(page, (unsigned char *)buf);
  loaded_page = page;
}

template <class T>
void CachedBuffer<T>::SetNewPageSize(uint new_page_size) {
  if (page_size == new_page_size)
    return;
  if (m_conn && m_conn->Killed())
    throw common::KilledException();  // cleaning is implemented below (we are
                                      // inside try{})
  page_size = new_page_size;
  CI_SetDefaultSize(page_size * elem_size);
  buf = (T *)rc_realloc(buf, sizeof(T) * (size_t)page_size, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
}

void CachedBuffer<types::BString>::SetNewPageSize(uint new_page_size) {
  if (page_size == new_page_size)
    return;
  if (m_conn && m_conn->Killed())
    throw common::KilledException();  // cleaning is implemented below (we are
                                      // inside try{})
  page_size = new_page_size;
  CI_SetDefaultSize(page_size * (elem_size + 4));
  size_t buf_size = (size_t)page_size * (elem_size + 4) * sizeof(char);
  buf = (char *)rc_realloc(buf, buf_size, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
}

template class CachedBuffer<short>;
template class CachedBuffer<int>;
template class CachedBuffer<int64_t>;
template class CachedBuffer<char>;
template class CachedBuffer<double>;

}  // namespace core
}  // namespace Tianmu
