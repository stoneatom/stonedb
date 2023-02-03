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
    e_ptr = e_insertRecord.record_decode(e_ptr);

    if (e_insertRecord.is_deleted_ == DELTA_RECORD_DELETE) {
      *new_value = existing_value->ToString();
      return true;
    }

    if (type == RecordType::kUpdate) {
      // update record head
      DeltaRecordHeadForUpdate updateRecord;
      ptr = updateRecord.record_decode(ptr);

      // new record head
      uint32_t n_load_num = e_insertRecord.load_num_ + updateRecord.load_num_;
      DeltaRecordHeadForInsert insertRecord(e_insertRecord.is_deleted_, e_insertRecord.table_id_,
                                            e_insertRecord.table_path_, e_insertRecord.field_count_, n_load_num);
      n_ptr = insertRecord.record_encode(n_ptr);

      for (uint i = 0; i < updateRecord.field_count_; i++) {
        // this field length
        insertRecord.field_len_[i] =
            updateRecord.field_len_[i] > 0 ? updateRecord.field_len_[i] : e_insertRecord.field_len_[i];
        if (updateRecord.null_mask_[i]) {
          insertRecord.null_mask_.set(i);
          if (!e_insertRecord.null_mask_[i]) {
            e_ptr += e_insertRecord.field_len_[i];
          }
        } else if (updateRecord.update_mask_[i]) {
          std::memcpy(n_ptr, ptr, updateRecord.field_len_[i]);
          n_ptr += updateRecord.field_len_[i];
          ptr += updateRecord.field_len_[i];
          if (!e_insertRecord.null_mask_[i]) {
            e_ptr += e_insertRecord.field_len_[i];
          }
        } else if (e_insertRecord.null_mask_[i]) {
          insertRecord.null_mask_.set(i);
        } else {
          std::memcpy(n_ptr, e_ptr, e_insertRecord.field_len_[i]);
          n_ptr += e_insertRecord.field_len_[i];
          e_ptr += e_insertRecord.field_len_[i];
        }
      }
      std::memcpy(value_buff.get() + insertRecord.null_offset_, insertRecord.null_mask_.data(),
                  insertRecord.null_mask_.data_size());
      new_value->assign(value_buff.get(), n_ptr - value_buff.get());
      return true;
    } else if (type == RecordType::kDelete) {
      DeltaRecordHeadForDelete deleteRecord;
      deleteRecord.record_decode(ptr);
      uint32_t n_load_num = e_insertRecord.load_num_ + deleteRecord.load_num_;
      DeltaRecordHeadForInsert insertRecord(DELTA_RECORD_DELETE, e_insertRecord.table_id_, e_insertRecord.table_path_,
                                            e_insertRecord.field_count_, n_load_num);
      n_ptr = insertRecord.record_encode(n_ptr);
      insertRecord.field_len_ = {0};
      new_value->assign(value_buff.get(), n_ptr - value_buff.get());
      return true;
    } else {
      return false;
    }
  } else if (existing_type == RecordType::kUpdate) {
    // exist update head
    DeltaRecordHeadForUpdate e_updateRecord;
    e_ptr = e_updateRecord.record_decode(e_ptr);

    if (type == RecordType::kUpdate) {
      // update
      DeltaRecordHeadForUpdate updateRecord;
      ptr = updateRecord.record_decode(ptr);

      uint32_t n_load_num = e_updateRecord.load_num_ + updateRecord.load_num_;
      DeltaRecordHeadForUpdate n_updateRecord(e_updateRecord.table_id_, e_updateRecord.table_path_,
                                              e_updateRecord.field_count_, n_load_num);
      n_ptr = n_updateRecord.record_encode(n_ptr);

      for (uint i = 0; i < updateRecord.field_count_; i++) {
        // this field length
        n_updateRecord.field_len_[i] =
            updateRecord.field_len_[i] > 0 ? updateRecord.field_len_[i] : e_updateRecord.field_len_[i];

        if (updateRecord.null_mask_[i]) {
          n_updateRecord.null_mask_.set(i);
          n_updateRecord.update_mask_.set(i);
          if (e_updateRecord.update_mask_[i]) {
            e_ptr += e_updateRecord.field_len_[i];
          }
        } else if (updateRecord.update_mask_[i]) {
          std::memcpy(n_ptr, ptr, updateRecord.field_len_[i]);
          n_ptr += updateRecord.field_len_[i];
          ptr += updateRecord.field_len_[i];
          n_updateRecord.update_mask_.set(i);

          if (e_updateRecord.update_mask_[i]) {
            e_ptr += e_updateRecord.field_len_[i];
          }
        } else if (e_updateRecord.null_mask_[i]) {
          n_updateRecord.null_mask_.set(i);
          n_updateRecord.update_mask_.set(i);
        } else if (e_updateRecord.update_mask_[i]) {
          std::memcpy(n_ptr, e_ptr, e_updateRecord.field_len_[i]);
          n_ptr += e_updateRecord.field_len_[i];
          e_ptr += e_updateRecord.field_len_[i];
        }
      }
      std::memcpy(value_buff.get() + n_updateRecord.update_offset_, n_updateRecord.update_mask_.data(),
                  n_updateRecord.update_mask_.data_size());
      std::memcpy(value_buff.get() + n_updateRecord.null_offset_, n_updateRecord.null_mask_.data(),
                  n_updateRecord.null_mask_.data_size());
      *new_value = std::string(value_buff.get(), n_ptr - value_buff.get());
      return true;
    } else if (type == RecordType::kDelete) {
      DeltaRecordHeadForDelete deleteRecord;
      deleteRecord.record_decode(ptr);
      uint32_t n_load_num = e_updateRecord.load_num_ + deleteRecord.load_num_;
      DeltaRecordHeadForDelete n_deleteRecord(e_updateRecord.table_id_, e_updateRecord.table_path_, n_load_num);
      n_ptr = n_deleteRecord.record_encode(n_ptr);
      *new_value = std::string(value_buff.get(), n_ptr - value_buff.get());
      return true;
    } else {
      return false;
    }
  } else if (existing_type == RecordType::kDelete) {
    DeltaRecordHeadForDelete e_deleteRecord;
    e_deleteRecord.record_decode(e_ptr);

    DeltaRecordHeadForDelete deleteRecord;
    deleteRecord.record_decode(ptr);

    uint32_t n_load_num = e_deleteRecord.load_num_ + deleteRecord.load_num_;
    DeltaRecordHeadForDelete n_deleteRecord(e_deleteRecord.table_id_, e_deleteRecord.table_path_, n_load_num);
    n_ptr = n_deleteRecord.record_encode(n_ptr);
    *new_value = std::string(value_buff.get(), n_ptr - value_buff.get());
    return true;
  } else {
    return false;
  }
}
}  // namespace Tianmu::core
