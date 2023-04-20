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

#include "group_distinct_cache.h"

#include <cstring>

#include "system/configuration.h"

namespace Tianmu {
namespace core {
GroupDistinctCache::GroupDistinctCache() : system::CacheableItem("JW", "GDC") {
  t = nullptr;
  t_write = nullptr;
  cur_pos = nullptr;
  cur_obj = 0;
  buf_size = 0;
  no_obj = 0;
  orig_no_obj = 0;
  width = 0;
  upper_byte_limit = 0;
  cur_write_pos = nullptr;
}

GroupDistinctCache::~GroupDistinctCache() {
  dealloc(t);
  dealloc(t_write);
}

void GroupDistinctCache::Initialize() {
  DEBUG_ASSERT(no_obj > 0 && width > 0);
  upper_byte_limit = tianmu_sysvar_distcache_size * 1_MB;  // Default 64 MB - max size of buffer
  buf_size = no_obj;
  if (no_obj > 32_GB)
    no_obj = 32_GB;  // upper reasonable size: 32 bln rows (= up to 640 GB on disk)
  // this limitation should be in future released by actual disk limits
  if (buf_size * width > upper_byte_limit) {
    buf_size = upper_byte_limit / width;
    CI_SetDefaultSize(upper_byte_limit);
    t = (unsigned char *)alloc(upper_byte_limit, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
  } else
    t = (unsigned char *)alloc(buf_size * width,
                               mm::BLOCK_TYPE::BLOCK_TEMPORARY);  // no need to cache on disk
  cur_pos = t;
  cur_obj = 0;
  cur_write_pos = t;
  cur_write_obj = 0;
}

void GroupDistinctCache::SetCurrentValue(unsigned char *val) {
  if (t == nullptr)  // not initialized yet!
    Initialize();
  ASSERT(cur_pos,
         "cur_pos buffer overflow, GroupDistinctCache OOM");  // cur_pos==nullptr  =>
                                                              // no more place
  std::memcpy(cur_pos, val, width);
}

void GroupDistinctCache::Reset()  // rewind iterator, start from object 0
{
  no_obj = orig_no_obj;
  Rewind();
}

void GroupDistinctCache::Rewind()  // rewind iterator, start from object 0
{
  if (no_obj > 0 && buf_size > 0) {
    if (cur_obj >= buf_size) {
      CI_Put(int(cur_obj / buf_size), t);  // save the last block
      CI_Get(0, t);                        // load the first block
    }
    cur_pos = t;  // may be nullptr, will be initialized on the first SetCurrentValue
    cur_write_pos = t_write;
  } else {
    cur_pos = nullptr;  // invalid from the beginning
    cur_write_pos = nullptr;
  }
  cur_obj = 0;
  cur_write_obj = 0;
}

bool GroupDistinctCache::NextRead()  // go to the next position, return false if
                                     // out of scope
{
  cur_obj++;
  if (cur_obj >= no_obj) {
    cur_obj--;
    cur_pos = nullptr;  // indicator of buffer overflow
    return false;
  }
  if (cur_obj % buf_size == 0) {         // a boundary between buffers
    CI_Get(int(cur_obj / buf_size), t);  // load the current block
    cur_pos = t;
  } else
    cur_pos += width;
  return true;
}

bool GroupDistinctCache::NextWrite()  // go to the next position, return false
                                      // if out of scope
{
  cur_obj++;
  if (cur_obj > no_obj) {
    cur_obj--;
    cur_pos = nullptr;  // indicator of buffer overflow
    return false;
  }
  if (cur_obj % buf_size == 0) {               // a boundary between buffers
    CI_Put(int((cur_obj - 1) / buf_size), t);  // save the previous block
    cur_pos = t;
  } else
    cur_pos += width;
  return true;
}

void GroupDistinctCache::MarkCurrentAsPreserved() {
  DEBUG_ASSERT(cur_obj >= cur_write_obj);
  if (t_write == nullptr) {
    t_write = (unsigned char *)alloc(upper_byte_limit,
                                     mm::BLOCK_TYPE::BLOCK_TEMPORARY);  // switch writing to the new buffer
    cur_write_pos = t_write;
  }
  if (cur_obj > cur_write_obj) {
    std::memcpy(cur_write_pos, cur_pos, width);
  }
  cur_write_obj++;
  if (cur_write_obj % buf_size == 0) {  // a boundary between buffers
    CI_Put(int((cur_write_obj - 1) / buf_size),
           t_write);  // save the previous block
    cur_write_pos = t_write;
  } else
    cur_write_pos += width;
}

void GroupDistinctCache::SwitchToPreserved()  // switch to values marked as
                                              // preserved
{
  no_obj = cur_write_obj;
  if (cur_write_obj > buf_size) {                    // more than one buffer
    CI_Put(int(cur_write_obj / buf_size), t_write);  // save the last block
    CI_Get(0, t);                                    // load the first block
    cur_obj = 0;                                     // preventing reloading anything on Rewind()
  } else {
    unsigned char *p = t;  // switch buffer names
    t = t_write;
    t_write = p;
    cur_obj = cur_write_obj;
  }
}

void GroupDistinctCache::Omit(int64_t obj_to_omit) {
  auto prev_block = cur_obj / buf_size;
  cur_obj += obj_to_omit;
  if (cur_obj >= no_obj) {
    cur_obj = no_obj;
    cur_pos = nullptr;  // indicator of buffer overflow
  } else if (cur_obj / buf_size != prev_block) {
    CI_Get(int(cur_obj / buf_size), t);  // load the proper block
    cur_pos = t + width * (cur_obj % buf_size);
  } else
    cur_pos += width * obj_to_omit;
}
}  // namespace core
}  // namespace Tianmu
