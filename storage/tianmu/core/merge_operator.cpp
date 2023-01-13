//
// Created by dfx on 22-12-15.
//

#include "core/merge_operator.h"
#include "core/delta_table.h"
#include "core/delta_record_head.h"
#include "util/mapped_circular_buffer.h"

namespace Tianmu::core {

bool RecordMergeOperator::Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value,
                                const rocksdb::Slice &value, std::string *new_value, rocksdb::Logger *logger) const {
  if (existing_value == nullptr) {
    *new_value = value.ToString();
    return true;
  }
  // existing value ptr
  const char *e_ptr = existing_value->data();
  RecordType existing_type = *(RecordType *)(e_ptr);
  // value ptr
  const char *ptr = value.data();
  RecordType type = *(RecordType *)(ptr);
  // new value ptr
  uint64_t value_buff_size = existing_value->size();
  std::unique_ptr<char[]> value_buff(new char[value_buff_size]);
  char *n_ptr = value_buff.get();

  if (existing_type == RecordType::kInsert) {
    DeltaRecordHeadForInsert e_insertRecord;
    e_ptr = e_insertRecord.record_decode(e_ptr);

    if (e_insertRecord.is_deleted_ == DELTA_RECORD_DELETE) {
      *new_value = existing_value->ToString();
      return true;
    }

    if (type == RecordType::kUpdate) {
      DeltaRecordHeadForUpdate updateRecord;
      ptr = updateRecord.record_decode(ptr);

      DeltaRecordHeadForInsert insertRecord(e_insertRecord.is_deleted_,e_insertRecord.table_id_,
                                            e_insertRecord.table_path_,e_insertRecord.field_count_);
      n_ptr = insertRecord.record_encode(n_ptr);

      for (uint i = 0; i < updateRecord.field_count_; i++) {
        // resize
        size_t length = updateRecord.field_head_[i] > 0 ? updateRecord.field_head_[i] : e_insertRecord.field_head_[i] > 0 ? e_insertRecord.field_head_[i] : 0;
        size_t used = ptr - value_buff.get();
        if (value_buff_size - used < length) {
          while (value_buff_size - used < length) {
            value_buff_size *= 2;
            if (value_buff_size > utils::MappedCircularBuffer::MAX_BUF_SIZE)
              throw common::Exception(e_insertRecord.table_path_ + " INSERT data exceeds max buffer size " +
                                      std::to_string(utils::MappedCircularBuffer::MAX_BUF_SIZE));
          }
          auto old_value_buff = std::move(value_buff);
          value_buff = std::make_unique<char[]>(value_buff_size);
          std::memcpy(value_buff.get(), old_value_buff.get(), used);
          n_ptr = value_buff.get() + used;
        }
        if (updateRecord.null_mask_[i]) {
          insertRecord.null_mask_.set(i);
          insertRecord.field_head_[i] = -1;
          if (!e_insertRecord.null_mask_[i]) {
            e_ptr += e_insertRecord.field_head_[i];
          }
        } else if (updateRecord.update_mask_[i]) {
          std::memcpy(n_ptr, ptr, updateRecord.field_head_[i]);
          insertRecord.field_head_[i] = updateRecord.field_head_[i];
          n_ptr += updateRecord.field_head_[i];
          ptr += updateRecord.field_head_[i];
          if (!e_insertRecord.null_mask_[i]) {
            e_ptr += e_insertRecord.field_head_[i];
          }
        } else if (e_insertRecord.null_mask_[i]) {
          insertRecord.null_mask_.set(i);
          insertRecord.field_head_[i] = -1;
        } else {
          std::memcpy(n_ptr, e_ptr, e_insertRecord.field_head_[i]);
          insertRecord.field_head_[i] = e_insertRecord.field_head_[i];
          n_ptr += e_insertRecord.field_head_[i];
          e_ptr += e_insertRecord.field_head_[i];
        }
      }
      std::memcpy(value_buff.get() + insertRecord.null_offset_, updateRecord.null_mask_.data(), updateRecord.null_mask_.data_size());
      new_value->assign(value_buff.get(), n_ptr - value_buff.get());
      return true;
    } else if (type == RecordType::kDelete) {
      DeltaRecordHeadForInsert insertRecord(DELTA_RECORD_DELETE,e_insertRecord.table_id_,
                                            e_insertRecord.table_path_,e_insertRecord.field_count_);
      n_ptr = insertRecord.record_encode(n_ptr);

      new_value->assign(value_buff.get(), n_ptr - value_buff.get());
      return true;
    } else {
      return false;
    }
  } else if (existing_type == RecordType::kUpdate) {

    DeltaRecordHeadForUpdate e_updateRecord;
    e_ptr = e_updateRecord.record_decode(e_ptr);

    if (type == RecordType::kUpdate) {
      DeltaRecordHeadForUpdate updateRecord;
      ptr = updateRecord.record_decode(ptr);

      DeltaRecordHeadForUpdate n_updateRecord(e_updateRecord.table_id_,e_updateRecord.table_path_,e_updateRecord.field_count_);
      n_ptr = n_updateRecord.record_encode(n_ptr);

      for (uint i = 0; i < updateRecord.field_count_; i++) {
        // resize
        size_t length = updateRecord.field_head_[i] > 0 ? updateRecord.field_head_[i] : e_updateRecord.field_head_[i] > 0 ? e_updateRecord.field_head_[i] : 0;
        size_t used = ptr - value_buff.get();
        if (value_buff_size - used < length) {
          while (value_buff_size - used < length) {
            value_buff_size *= 2;
            if (value_buff_size > utils::MappedCircularBuffer::MAX_BUF_SIZE)
              throw common::Exception(e_updateRecord.table_path_ + " INSERT data exceeds max buffer size " +
                                      std::to_string(utils::MappedCircularBuffer::MAX_BUF_SIZE));
          }
          auto old_value_buff = std::move(value_buff);
          value_buff = std::make_unique<char[]>(value_buff_size);
          std::memcpy(value_buff.get(), old_value_buff.get(), used);
          n_ptr = value_buff.get() + used;
        }
        if (updateRecord.null_mask_[i]) {
          n_updateRecord.null_mask_.set(i);
          n_updateRecord.field_head_[i] = -1;
          if (e_updateRecord.update_mask_[i]) {
            e_ptr += e_updateRecord.field_head_[i];
          }
        } else if (updateRecord.update_mask_[i]) {
          std::memcpy(n_ptr, ptr, updateRecord.field_head_[i]);
          n_updateRecord.field_head_[i] = updateRecord.field_head_[i];
          n_ptr += updateRecord.field_head_[i];
          ptr += updateRecord.field_head_[i];
          if (e_updateRecord.update_mask_[i]) {
            n_updateRecord.field_head_[i] = -1;
            e_ptr += e_updateRecord.field_head_[i];
          }
        } else if (e_updateRecord.null_mask_[i]) {
          n_updateRecord.null_mask_.set(i);
        } else if (e_updateRecord.update_mask_[i]) {
          std::memcpy(n_ptr, e_ptr, e_updateRecord.field_head_[i]);
          n_updateRecord.field_head_[i] = e_updateRecord.field_head_[i];
          n_ptr += e_updateRecord.field_head_[i];
          e_ptr += e_updateRecord.field_head_[i];
        }
      }
      std::memcpy(value_buff.get() + n_updateRecord.update_offset_, updateRecord.update_mask_.data(), updateRecord.update_mask_.data_size());
      std::memcpy(value_buff.get() + n_updateRecord.null_offset_, updateRecord.null_mask_.data(), updateRecord.null_mask_.data_size());
      new_value->assign(value_buff.get(), n_ptr - value_buff.get());
      return true;
    } else if (type == RecordType::kDelete) {
      *new_value = value.ToString();
      return true;
    } else {
      return false;
    }
  } else if (existing_type == RecordType::kDelete) {
    *new_value = existing_value->ToString();
    return true;
  } else {
    return false;
  }
}
}  // namespace Tianmu::core
