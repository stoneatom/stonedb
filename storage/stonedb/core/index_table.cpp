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

#include "index_table.h"

#include "core/filter.h"
#include "core/transaction.h"
#include "system/rc_system.h"

namespace stonedb {
namespace core {
IndexTable::IndexTable(int64_t _size, int64_t _orig_size, [[maybe_unused]] int mem_modifier)
    : system::CacheableItem("JW", "INT"), m_conn(current_tx) {
  // Note: buffer size should be 2^n
  DEBUG_ASSERT(_orig_size >= 0);
  orig_size = _orig_size;
  bytes_per_value = ((orig_size + 1) > 0xFFFF ? ((orig_size + 1) > 0xFFFFFFFF ? 8 : 4) : 2);

  max_buffer_size_in_bytes = 32_MB;  // 32 MB = 2^25
  if (size_t(_size * bytes_per_value) > 32_MB) max_buffer_size_in_bytes = int(mm::TraceableObject::MaxBufferSize(-1));

  buffer_size_in_bytes = max_buffer_size_in_bytes;
  CI_SetDefaultSize((int)max_buffer_size_in_bytes);
  size = _size;
  if (size == 0) size = 1;
  uint values_per_block = uint(max_buffer_size_in_bytes / bytes_per_value);
  block_shift = CalculateBinSize(values_per_block) - 1;  // e.g. BinSize(16)==5, but shift by 4.
                                                         // WARNING: should it be (val...-1)? Now
                                                         // it works, because v... is only 2^25,
                                                         // 2^24, 2^23
  block_mask = (uint64_t(1) << block_shift) - 1;

  if (size * bytes_per_value < buffer_size_in_bytes)
    buffer_size_in_bytes = int(size * bytes_per_value);  // the whole table in one buffer
  buf = (unsigned char *)alloc(buffer_size_in_bytes, mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);
  if (!buf) {
    Unlock();
    STONEDB_LOG(LogCtl_Level::ERROR, "Could not allocate memory for IndexTable, size :%u", buffer_size_in_bytes);
    throw common::OutOfMemoryException();
  }
  std::memset(buf, 0, buffer_size_in_bytes);
  cur_block = 0;
  max_block_used = 0;
  block_changed = false;
  Unlock();
}

IndexTable::IndexTable(IndexTable &sec)
    : system::CacheableItem("JW", "INT"),
      mm::TraceableObject(sec),
      max_buffer_size_in_bytes(sec.max_buffer_size_in_bytes),
      bytes_per_value(sec.bytes_per_value),
      max_block_used(sec.max_block_used),
      buffer_size_in_bytes(sec.buffer_size_in_bytes),
      block_shift(sec.block_shift),
      block_mask(sec.block_mask),
      size(sec.size),
      orig_size(sec.orig_size),
      cur_block(0),
      block_changed(false),
      m_conn(sec.m_conn) {
  sec.Lock();
  CI_SetDefaultSize((int)max_buffer_size_in_bytes);
  buf = (unsigned char *)alloc(buffer_size_in_bytes, mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);
  if (!buf) {
    sec.Unlock();
    Unlock();
    STONEDB_LOG(LogCtl_Level::ERROR, "Could not allocate memory for IndexTable(sec)!");
    throw common::OutOfMemoryException();
  }
  int64_t used_size = size;
  if (used_size > ((int64_t(max_block_used) + 1) << block_shift))
    used_size = ((int64_t(max_block_used) + 1) << block_shift);
  if (max_block_used == 0 && size > 0) {
    std::memcpy(buf, sec.buf, std::min(uint64_t(buffer_size_in_bytes), size * bytes_per_value));
    block_changed = true;
  } else
    for (uint k = 0; k < used_size; k++) Set64(k, sec.Get64(k));
  sec.Unlock();
  Unlock();
}

IndexTable::~IndexTable() {
  DestructionLock();
  if (buf) dealloc(buf);
}

void IndexTable::LoadBlock(int b) {
  DEBUG_ASSERT(IsLocked());
  if (buf == NULL) {  // possible after block caching on disk
    buf = (unsigned char *)alloc(buffer_size_in_bytes, mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);
    if (!buf) {
      STONEDB_LOG(LogCtl_Level::ERROR, "Could not allocate memory for IndexTable(LoadBlock).");
      throw common::OutOfMemoryException();
    }
  } else if (block_changed)
    CI_Put(cur_block, buf);
  DEBUG_ASSERT(buf != NULL);
  CI_Get(b, buf);
  if (m_conn->Killed())  // from time to time...
    throw common::KilledException();
  max_block_used = std::max(max_block_used, b);
  cur_block = b;
  block_changed = false;
}

void IndexTable::ExpandTo(int64_t new_size) {
  DEBUG_ASSERT(IsLocked());
  if (new_size <= (int64_t)size) return;
  if (size * bytes_per_value == buffer_size_in_bytes) {  // the whole table was in one buffer
    if (buffer_size_in_bytes < 32_MB && size_t(new_size * bytes_per_value) > 32_MB) {
      max_buffer_size_in_bytes =
          int(mm::TraceableObject::MaxBufferSize(-1));   // recalculate, as it might not be done earlier
      CI_SetDefaultSize((int)max_buffer_size_in_bytes);  // redefine disk block sized
      uint values_per_block = uint(max_buffer_size_in_bytes / bytes_per_value);
      block_shift =
          CalculateBinSize(values_per_block) - 1;  // e.g. BinSize(16)==5, but shift by 4. WARNING: should it be
      // (val...-1)? Now it works, because v... is only 2^25, 2^24, 2^23
      block_mask = (uint64_t(1) << block_shift) - 1;
    }

    int new_buffer_size_in_bytes;
    if (new_size * bytes_per_value < (int64_t)max_buffer_size_in_bytes)
      new_buffer_size_in_bytes = int(new_size * bytes_per_value);
    else
      new_buffer_size_in_bytes = max_buffer_size_in_bytes;
    // TODO: check the rc_alloc status
    buf = (unsigned char *)rc_realloc(buf, new_buffer_size_in_bytes, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
    buffer_size_in_bytes = new_buffer_size_in_bytes;
  }
  // else: the table is buffered anyway, so we don't need to do anything
  size = new_size;
}

void IndexTable::SetByFilter(Filter *f, uint32_t power)  // add all positions where filter is one
{
  FilterOnesIterator it(f, power);
  ExpandTo(f->NoOnes());
  int64_t loc_obj = 0;
  while (it.IsValid()) {
    Set64(loc_obj++, *it + 1);
    ++it;
  }
}
}  // namespace core
}  // namespace stonedb
