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

#include "core/delta_record_head.h"
#include <cstring>
#include "delta_record_head.h"

namespace Tianmu {
namespace core {
DeltaRecordHeadForInsert::DeltaRecordHeadForInsert(char is_deleted, size_t &field_count, uint32_t load_num)
    : load_num_(load_num), is_deleted_(is_deleted), field_count_(field_count), null_mask_(field_count) {}

char *DeltaRecordHeadForInsert::recordEncode(char *ptr) {
  if (ptr == nullptr)
    return ptr;
  char *ptr_begin = ptr;
  // RecordType
  *(RecordType *)ptr = record_type_;
  ptr += sizeof(RecordType);
  // load num
  *(uint32_t *)ptr = load_num_;
  ptr += sizeof(uint32_t);
  // isDeleted
  *ptr = is_deleted_;
  ptr++;
  // field count
  *(size_t *)ptr = field_count_;
  ptr += sizeof(size_t);
  // null mask
  null_offset_ = ptr - ptr_begin;
  ptr += null_mask_.data_size();
  // field head
  field_len_ = (uint32_t *)ptr;
  std::memset(ptr, 0, sizeof(uint32_t) * field_count_);
  ptr += sizeof(uint32_t) * field_count_;
  field_offset_ = ptr - ptr_begin;
  return ptr;
}

const char *DeltaRecordHeadForInsert::recordDecode(const char *ptr) {
  if (ptr == nullptr)
    return ptr;
  record_type_ = *(RecordType *)(ptr);
  ptr += sizeof(RecordType);
  // load num
  load_num_ = *(uint32_t *)ptr;
  ptr += sizeof(uint32_t);
  // is delete
  is_deleted_ = *ptr;
  ptr++;
  // get existing field count
  field_count_ = *(size_t *)ptr;
  ptr += sizeof(size_t);
  // get existing null mask
  null_mask_.Init(field_count_, const_cast<char *>(ptr));
  ptr += null_mask_.data_size();
  // get existing field_head
  field_len_ = (uint32_t *)ptr;
  ptr += sizeof(uint32_t) * field_count_;
  return ptr;
}

DeltaRecordHeadForUpdate::DeltaRecordHeadForUpdate(size_t &field_count, uint32_t load_num)
    : load_num_(load_num), field_count_(field_count), update_mask_(field_count), null_mask_(field_count) {}

char *DeltaRecordHeadForUpdate::recordEncode(char *ptr) {
  if (ptr == nullptr)
    return ptr;
  char *ptr_begin = ptr;
  // RecordType
  *(RecordType *)ptr = record_type_;
  ptr += sizeof(RecordType);
  // load num
  *(uint32_t *)ptr = load_num_;
  ptr += sizeof(uint32_t);
  // field count
  *(size_t *)ptr = field_count_;
  ptr += sizeof(size_t);
  // update mask
  update_offset_ = ptr - ptr_begin;
  ptr += update_mask_.data_size();
  // null mask
  null_offset_ = ptr - ptr_begin;
  ptr += null_mask_.data_size();
  // field head
  field_len_ = (uint32_t *)ptr;
  std::memset(ptr, 0, sizeof(uint32_t) * field_count_);
  ptr += sizeof(uint32_t) * field_count_;
  field_offset_ = ptr - ptr_begin;
  return ptr;
}

const char *DeltaRecordHeadForUpdate::recordDecode(const char *ptr) {
  if (ptr == nullptr)
    return ptr;
  const char *ptr_begin = ptr;
  // type
  record_type_ = *(RecordType *)(ptr);
  ptr += sizeof(RecordType);
  // load num
  load_num_ = *(uint32_t *)ptr;
  ptr += sizeof(uint32_t);
  // get existing field count
  field_count_ = *(size_t *)ptr;
  ptr += sizeof(size_t);
  // update mask
  update_offset_ = ptr - ptr_begin;
  update_mask_.Init(field_count_, const_cast<char *>(ptr));
  ptr += update_mask_.data_size();
  // get existing null mask
  null_offset_ = ptr - ptr_begin;
  null_mask_.Init(field_count_, const_cast<char *>(ptr));
  ptr += null_mask_.data_size();
  // get existing field_head
  field_len_ = (uint32_t *)ptr;
  ptr += sizeof(uint32_t) * field_count_;
  field_offset_ = ptr - ptr_begin;
  return ptr;
}

DeltaRecordHeadForDelete::DeltaRecordHeadForDelete(uint32_t load_num) : load_num_(load_num) {}

char *DeltaRecordHeadForDelete::recordEncode(char *ptr) {
  if (ptr == nullptr)
    return ptr;
  char *ptr_begin = ptr;
  // RecordType
  *(RecordType *)ptr = record_type_;
  ptr += sizeof(RecordType);
  // load id
  *(uint32_t *)ptr = load_num_;
  ptr += sizeof(uint32_t);
  return ptr;
}

const char *DeltaRecordHeadForDelete::recordDecode(const char *ptr) {
  if (ptr == nullptr)
    return ptr;
  record_type_ = *(RecordType *)(ptr);
  ptr += sizeof(RecordType);
  // load id
  load_num_ = *(uint32_t *)ptr;
  ptr += sizeof(uint32_t);
  return ptr;
}

RecordType core::DeltaRecordHead::GetRecordType(const char *ptr) { return *(RecordType *)(ptr); }

}  // namespace core
}  // namespace Tianmu