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

//
// Created by dfx on 22-12-15.
//

#include "core/merge_operator.h"
#include "core/delta_record_head.h"
#include "core/delta_table.h"
#include "util/mapped_circular_buffer.h"

namespace Tianmu::core {

bool RecordMergeOperator::Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value,
                                const rocksdb::Slice &value, std::string *new_value, rocksdb::Logger *logger) const {
  if (existing_value == nullptr) {
    *new_value = value.ToString();
    return true;
  }
  // [existing value] ptr
  const char *e_ptr = existing_value->data();
  RecordType existing_type = *(RecordType *)(e_ptr);
  // [value] ptr
  const char *ptr = value.data();
  RecordType type = *(RecordType *)(ptr);
  // [new value] ptr
  uint64_t value_buff_size = existing_value->size() + value.size();
  std::unique_ptr<char[]> value_buff(new char[value_buff_size]);
  char *n_ptr = value_buff.get();

  if (existing_type == RecordType::kInsert) {
    DeltaRecordHeadForInsert e_insertRecord;
    e_ptr = e_insertRecord.recordDecode(e_ptr);

    if (e_insertRecord.is_deleted_ == DELTA_RECORD_DELETE) {
      *new_value = existing_value->ToString();
      return true;
    }
    // Insert data and update data are merged and combined. Overlapping records are subject to the latter
    if (type == RecordType::kUpdate) {
      // update record head
      DeltaRecordHeadForUpdate updateRecord;
      ptr = updateRecord.recordDecode(ptr);

      // new record head
      uint32_t n_load_num = e_insertRecord.load_num_ + updateRecord.load_num_;
      DeltaRecordHeadForInsert insertRecord(e_insertRecord.is_deleted_, e_insertRecord.field_count_, n_load_num);
      n_ptr = insertRecord.recordEncode(n_ptr);

      for (uint i = 0; i < updateRecord.field_count_; i++) {
        if (updateRecord.null_mask_[i]) {
          insertRecord.null_mask_.set(i);
          insertRecord.field_len_[i] = 0;
          if (!e_insertRecord.null_mask_[i]) {
            e_ptr += e_insertRecord.field_len_[i];
          }
        } else if (updateRecord.update_mask_[i]) {
          std::memcpy(n_ptr, ptr, updateRecord.field_len_[i]);
          n_ptr += updateRecord.field_len_[i];
          ptr += updateRecord.field_len_[i];
          insertRecord.field_len_[i] = updateRecord.field_len_[i];
          if (!e_insertRecord.null_mask_[i]) {
            e_ptr += e_insertRecord.field_len_[i];
          }
        } else if (e_insertRecord.null_mask_[i]) {
          insertRecord.null_mask_.set(i);
          insertRecord.field_len_[i] = 0;
        } else {
          std::memcpy(n_ptr, e_ptr, e_insertRecord.field_len_[i]);
          n_ptr += e_insertRecord.field_len_[i];
          e_ptr += e_insertRecord.field_len_[i];
          insertRecord.field_len_[i] = e_insertRecord.field_len_[i];
        }
      }
      std::memcpy(value_buff.get() + insertRecord.null_offset_, insertRecord.null_mask_.data(),
                  insertRecord.null_mask_.data_size());
      new_value->assign(value_buff.get(), n_ptr - value_buff.get());
      return true;
    } else if (type == RecordType::kDelete) {
      // Insert data is merged with delete data. After merging, the record type is still Insert, but the record has been
      // deleted
      DeltaRecordHeadForDelete deleteRecord;
      deleteRecord.recordDecode(ptr);
      uint32_t n_load_num = e_insertRecord.load_num_ + deleteRecord.load_num_;
      DeltaRecordHeadForInsert insertRecord(DELTA_RECORD_DELETE, e_insertRecord.field_count_, n_load_num);
      n_ptr = insertRecord.recordEncode(n_ptr);
      insertRecord.field_len_ = {0};
      new_value->assign(value_buff.get(), n_ptr - value_buff.get());
      return true;
    } else {
      return false;
    }
  } else if (existing_type == RecordType::kUpdate) {
    // exist update head
    DeltaRecordHeadForUpdate e_updateRecord;
    e_ptr = e_updateRecord.recordDecode(e_ptr);
    // The update data is merged with the update data, and the overlapping records are subject to the latter
    if (type == RecordType::kUpdate) {
      // update
      DeltaRecordHeadForUpdate updateRecord;
      ptr = updateRecord.recordDecode(ptr);

      uint32_t n_load_num = e_updateRecord.load_num_ + updateRecord.load_num_;
      DeltaRecordHeadForUpdate n_updateRecord(e_updateRecord.field_count_, n_load_num);
      n_ptr = n_updateRecord.recordEncode(n_ptr);

      for (uint i = 0; i < updateRecord.field_count_; i++) {
        if (updateRecord.null_mask_[i]) {
          n_updateRecord.null_mask_.set(i);
          n_updateRecord.update_mask_.set(i);
          n_updateRecord.field_len_[i] = 0;

          if (e_updateRecord.update_mask_[i]) {
            e_ptr += e_updateRecord.field_len_[i];
          }
        } else if (updateRecord.update_mask_[i]) {
          std::memcpy(n_ptr, ptr, updateRecord.field_len_[i]);
          n_ptr += updateRecord.field_len_[i];
          ptr += updateRecord.field_len_[i];
          n_updateRecord.update_mask_.set(i);
          n_updateRecord.field_len_[i] = updateRecord.field_len_[i];

          if (e_updateRecord.update_mask_[i]) {
            e_ptr += e_updateRecord.field_len_[i];
          }
        } else if (e_updateRecord.null_mask_[i]) {
          n_updateRecord.null_mask_.set(i);
          n_updateRecord.update_mask_.set(i);
          n_updateRecord.field_len_[i] = 0;
        } else if (e_updateRecord.update_mask_[i]) {
          std::memcpy(n_ptr, e_ptr, e_updateRecord.field_len_[i]);
          n_ptr += e_updateRecord.field_len_[i];
          e_ptr += e_updateRecord.field_len_[i];
          n_updateRecord.update_mask_.set(i);
          n_updateRecord.field_len_[i] = e_updateRecord.field_len_[i];
        }
      }
      std::memcpy(value_buff.get() + n_updateRecord.update_offset_, n_updateRecord.update_mask_.data(),
                  n_updateRecord.update_mask_.data_size());
      std::memcpy(value_buff.get() + n_updateRecord.null_offset_, n_updateRecord.null_mask_.data(),
                  n_updateRecord.null_mask_.data_size());
      *new_value = std::string(value_buff.get(), n_ptr - value_buff.get());
      return true;
    } else if (type == RecordType::kDelete) {
      // The update data is merged with the delete data. After merging, the record type is delete
      DeltaRecordHeadForDelete deleteRecord;
      deleteRecord.recordDecode(ptr);
      uint32_t n_load_num = e_updateRecord.load_num_ + deleteRecord.load_num_;
      DeltaRecordHeadForDelete n_deleteRecord(n_load_num);
      n_ptr = n_deleteRecord.recordEncode(n_ptr);
      *new_value = std::string(value_buff.get(), n_ptr - value_buff.get());
      return true;
    } else {
      return false;
    }
  } else if (existing_type == RecordType::kDelete) {
    DeltaRecordHeadForDelete e_deleteRecord;
    e_deleteRecord.recordDecode(e_ptr);

    DeltaRecordHeadForDelete deleteRecord;
    deleteRecord.recordDecode(ptr);

    uint32_t n_load_num = e_deleteRecord.load_num_ + deleteRecord.load_num_;
    DeltaRecordHeadForDelete n_deleteRecord(n_load_num);
    n_ptr = n_deleteRecord.recordEncode(n_ptr);
    *new_value = std::string(value_buff.get(), n_ptr - value_buff.get());
    return true;
  } else {
    return false;
  }
}
}  // namespace Tianmu::core
