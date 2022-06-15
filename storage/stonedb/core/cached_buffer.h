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
#ifndef STONEDB_CORE_CACHED_BUFFER_H_
#define STONEDB_CORE_CACHED_BUFFER_H_
#pragma once

#include "common/common_definitions.h"
#include "mm/traceable_object.h"
#include "system/cacheable_item.h"
#include "types/rc_data_types.h"

namespace stonedb {
namespace core {
/*
This is a general purpose class to store 64bit number of elements of any
built-in type in an array. One page of elements is kept in memory (2^25 = 33 554
432). If there are more elements they are cached on a disk (class
CacheableItem). Methods Get and Set are to read/write a value. NOTE: it is not
suitable to store structures containing pointers.
*/

class Transaction;

template <class T>
class CachedBuffer : public system::CacheableItem, public mm::TraceableObject {
 public:
  explicit CachedBuffer(uint page_size = 33554432, uint elem_size = 0, Transaction *conn = NULL);
  virtual ~CachedBuffer();

  uint64_t PageSize() { return page_size; }
  void SetNewPageSize(uint new_page_size);
  T &Get(uint64_t idx);
  void Get([[maybe_unused]] types::BString &s, [[maybe_unused]] uint64_t idx) {
    DEBUG_ASSERT("use only for BString" && 0);
  }
  void Set(uint64_t idx, const T &value);

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_CACHEDBUFFER; }

 protected:
  void LoadPage(uint n);     // load page n
  unsigned int loaded_page;  // number of page loaded to memory?
  bool page_changed;         // is current page changed and has to be saved?
  uint page_size;            // size of one page stored in memory (number of elements)
  uint elem_size;            // size of one element in BYTES; for most types computed by
                             // sizeof()
  T *buf;
  Transaction *m_conn;
};

/* buffers, in the case of BString, keep the char * only. first 4 bytes define
the length of entry or if it is null (common::NULL_VALUE_U) or not. must be of
equal size, thus elem_size must be set */

template <>
class CachedBuffer<types::BString> : public system::CacheableItem, public mm::TraceableObject {
 public:
  CachedBuffer(uint page_size = 33554432, uint elem_size = 0, Transaction *conn = NULL);
  CachedBuffer(uint64_t size, types::BString &value, uint page_size = 33554432, uint elem_size = 0,
               Transaction *conn = NULL);
  virtual ~CachedBuffer();

  uint64_t PageSize() { return page_size; }
  void SetNewPageSize(uint new_page_size);
  types::BString &Get(uint64_t idx);
  void Get(types::BString &s, uint64_t idx);
  void Set(uint64_t idx, const types::BString &value);

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_CACHEDBUFFER; }

 protected:
  void LoadPage(uint n);     // load page n
  unsigned int loaded_page;  // number of page loaded to memory?
  bool page_changed;         // is current page changed and has to be saved?
  uint page_size;            // size of one page stored in memory (number of elements)
  uint elem_size;            // size of one element in BYTES; for most types computed by
                             // sizeof()
  char *buf;
  types::BString bufS;
  Transaction *m_conn;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_CACHED_BUFFER_H_
