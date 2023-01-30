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

#include <cstring>
#include "core/delta_record_head.h"

namespace Tianmu {
namespace core {
DeltaRecordHeadForInsert::DeltaRecordHeadForInsert(char is_deleted, int32_t &table_id, const std::string &table_path, 
                            size_t &field_count):null_mask_(field_count) {
  is_deleted_ = is_deleted;
  table_id_ = table_id;
  table_path_ = table_path;
  field_count_ = field_count;
}

char * DeltaRecordHeadForInsert::record_encode(char *ptr) {
  if(ptr == nullptr) return ptr;
  char *ptr_begin = ptr;
  // RecordType
  *(RecordType *)ptr = record_type_;
  ptr += sizeof(RecordType);
  // isDeleted
  *ptr = is_deleted_;
  ptr++;
  // table id
  *(int32_t *)ptr = table_id_;
  ptr += sizeof(int32_t);
  // table path
  int32_t path_len = table_path_.size();
  std::memcpy(ptr, table_path_.c_str(), path_len);
  ptr += path_len;
  *ptr++ = 0;  // end with \0
  // field count
  *(size_t *)ptr = field_count_;
  ptr += sizeof(size_t);
  // null mask
  null_offset_ = ptr - ptr_begin;
  ptr += null_mask_.data_size();
  // field head
  field_head_ = (uint32_t *)ptr;
  ptr += sizeof(int64_t) * field_count_;
  field_offset_ = ptr - ptr_begin;
  return ptr;
}

const char *DeltaRecordHeadForInsert::record_decode(const char *ptr) {
  if(ptr == nullptr) return ptr;
  record_type_ = *(RecordType *)(ptr);
  ptr += sizeof(RecordType);
  is_deleted_ = *ptr;
  ptr++;
  // parse existing value table id and table path
  table_id_ = *(int32_t *)ptr;
  ptr += sizeof(int32_t);
  // table path
  table_path_ = std::string(ptr);
  ptr += table_path_.length() + sizeof(char);
  // get existing field count
  field_count_ = *(size_t *)ptr;
  ptr += sizeof(size_t);
  // get existing null mask
  null_mask_.Init(field_count_, const_cast<char *>(ptr));
  ptr += null_mask_.data_size();
  // get existing field_head
  field_head_ = (uint32_t *)ptr;
  ptr += sizeof(int64_t) * field_count_;
  return ptr;
}

DeltaRecordHeadForUpdate::DeltaRecordHeadForUpdate(int32_t &table_id, const std::string &table_path, size_t &field_count)
                                                 :null_mask_(field_count),
                                                 update_mask_(field_count){
  table_id_ = table_id;
  table_path_ = table_path;
  field_count_ = field_count;
}

char *DeltaRecordHeadForUpdate::record_encode(char *ptr) {
  if(ptr == nullptr) return ptr;
  char *ptr_begin = ptr;
  // RecordType
  *(RecordType *)ptr = record_type_;
  ptr += sizeof(RecordType);
  // table id
  *(int32_t *)ptr = table_id_;
  ptr += sizeof(int32_t);
  // table path
  int32_t path_len = table_path_.size();
  std::memcpy(ptr, table_path_.c_str(), path_len);
  ptr += path_len;
  *ptr++ = 0;  // end with \0
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
  field_head_ = (uint32_t *)ptr;
  ptr += sizeof(int64_t) * field_count_;
  field_offset_ = ptr - ptr_begin;
  return ptr;
}

const char *DeltaRecordHeadForUpdate::record_decode(const char *ptr) {
  if(ptr == nullptr) return ptr;
  const char *ptr_begin = ptr;
  record_type_ = *(RecordType *)(ptr);
  ptr += sizeof(RecordType);
  // parse existing value table id and table path
  table_id_ = *(int32_t *)ptr;
  ptr += sizeof(int32_t);
  // table path
  table_path_ = std::string(ptr);
  ptr += table_path_.length() + sizeof(char);
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
  field_head_ = (uint32_t *)ptr;
  ptr += sizeof(uint32_t) * field_count_;
  field_offset_ = ptr - ptr_begin;
  return ptr;
}

DeltaRecordHeadForDelete::DeltaRecordHeadForDelete(int32_t &table_id, const std::string &table_path) {
    table_id_ = table_id;
    table_path_ = table_path;
}

char *DeltaRecordHeadForDelete::record_encode(char *ptr) {
  if(ptr == nullptr) return ptr;
  char *ptr_begin = ptr;
  // RecordType
  *(RecordType *)ptr = record_type_;
  ptr += sizeof(RecordType);
  // table id
  *(int32_t *)ptr = table_id_;
  ptr += sizeof(int32_t);
  // table path
  int32_t path_len = table_path_.size();
  std::memcpy(ptr, table_path_.c_str(), path_len);
  ptr += path_len;
  *ptr++ = 0;  // end with \0
  return ptr;
}

const char *DeltaRecordHeadForDelete::record_decode(const char *ptr) {
  if(ptr == nullptr) return ptr;
  record_type_ = *(RecordType *)(ptr);
  ptr += sizeof(RecordType);
  // parse existing value table id and table path
  table_id_ = *(int32_t *)ptr;
  ptr += sizeof(int32_t);
  // table path
  table_path_ = std::string(ptr);
  ptr += table_path_.length() + sizeof(char);
  return ptr;
}

}  // namespace core
}  // namespace Tianmu