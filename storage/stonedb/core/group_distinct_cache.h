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
#ifndef STONEDB_CORE_GROUP_DISTINCT_CACHE_H_
#define STONEDB_CORE_GROUP_DISTINCT_CACHE_H_
#pragma once

#include "mm/traceable_object.h"
#include "system/cacheable_item.h"

namespace stonedb {
namespace core {
/*
 * Functionality:
 *   - remember byte vectors (of defined width) in a table, cached on disk,
 *   - provide one-by-one access for put/get value,
 *   - allow filtering values by marking some of them as "preserved",
 *   - allow switching to "preserved" values.
 * Assumption: values will be written once and read once (unless preserved).
 * */

class GroupDistinctCache : private system::CacheableItem, public mm::TraceableObject {
 public:
  GroupDistinctCache();
  ~GroupDistinctCache();

  void SetNoObj(int64_t max_no_obj) {
    no_obj = max_no_obj;
    orig_no_obj = max_no_obj;
  }
  void SetWidth(int w) { width = w; }  // w - byte size of a single object
  void Reset();                        // reset the number of objects, then rewind (used for reusing
                                       // the object)
  void Rewind();                       // rewind iterator, start from object 0
  bool NextRead();                     // go to the next position, return false if out of scope
  bool NextWrite();                    // go to the next position, return false if out of scope

  unsigned char *GetCurrentValue() { return cur_pos; }
  void SetCurrentValue(unsigned char *val);

  void MarkCurrentAsPreserved();   // move current value to beginning, or just
                                   // after previously preserved (reuse buffer)
  void SwitchToPreserved();        // switch to values marked as preserved
  void Omit(int64_t obj_to_omit);  // move a reading position forward by
                                   // obj_to_omit objects

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }

 private:
  void Initialize();

  unsigned char *t;         // value buffer
  unsigned char *t_write;   // value buffer for preserved objects, if different than t
  size_t upper_byte_limit;  // upper byte size of t, t_write, it is also the
                            // actual size for multi-block case
  unsigned char *cur_pos;   // current location in buffer; NULL - out of scope
  size_t cur_obj;           // current (virtual) object number
  size_t no_obj;            // a number of all (virtual) objects; current state (may be
                            // lowered by switching to preserved)
  int64_t orig_no_obj;      // a number of all objects (original value, no_obj will
                            // be switched to it on Reset())
  int width;                // number of bytes for one value

  unsigned char *cur_write_pos;  // current location for writing while reusage
  size_t cur_write_obj;          // current (virtual) object number, as above

  size_t buf_size;  // buffer size in objects
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_GROUP_DISTINCT_CACHE_H_
