//
// Created by dfx on 22-12-15.
//

#include "merge_operator.h"
#include "delta_table.h"
#include "util/bitset.h"
#include "util/mapped_circular_buffer.h"

namespace Tianmu::core {

bool RecordMergeOperator::Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value,
                                const rocksdb::Slice &value, std::string *new_value, rocksdb::Logger *logger) const {
  // existing value ptr
  const char *e_ptr = existing_value->data();
  RecordType existing_type = *(RecordType *)(e_ptr);
  e_ptr += sizeof(RecordType);

  // value ptr
  const char *ptr = existing_value->data();
  RecordType type = *(RecordType *)(ptr);
  ptr += sizeof(RecordType);

  // new value ptr
  uint64_t value_buff_size = existing_value->size();
  std::unique_ptr<char[]> value_buff(new char[value_buff_size]);
  char *n_ptr = value_buff.get();

  RecordType new_type;
  if (existing_type == RecordType::kInsert) {
    if (type == RecordType::kUpdate) {
      new_type = RecordType::kInsert;
      // parse table id and table path
      ptr += sizeof(int32_t);
      std::string path(ptr);
      ptr += path.length() + 1;
      // get field count
      size_t field_count = *(size_t *)ptr;
      ptr += sizeof(size_t);
      // get update mask
      utils::BitSet update_mask(field_count, const_cast<char *>(ptr));
      ptr += update_mask.data_size();
      // get null mask
      utils::BitSet null_mask(field_count, const_cast<char *>(ptr));
      ptr += null_mask.data_size();
      // get field_head
      int64_t *field_head = (int64_t *)ptr;
      ptr += sizeof(int64_t) * field_count;

      // parse existing value table id and table path
      int32_t e_tid = *(int32_t *)e_ptr;
      e_ptr += sizeof(int32_t);
      std::string e_path(e_ptr);
      e_ptr += path.length() + 1;
      // get existing field count
      size_t e_field_count = *(size_t *)ptr;
      e_ptr += sizeof(size_t);
      assert(e_field_count == field_count);
      // get existing null mask
      utils::BitSet e_null_mask(e_field_count, const_cast<char *>(ptr));
      e_ptr += e_null_mask.data_size();
      // get existing field_head
      int64_t *e_field_head = (int64_t *)ptr;
      e_ptr += sizeof(int64_t) * e_field_count;

      // assemble new value
      *(RecordType *)n_ptr = new_type;  // new_type
      n_ptr += sizeof(RecordType);
      *(int32 *)n_ptr = e_tid;  // tid
      n_ptr += sizeof(int32_t);
      std::memcpy(n_ptr, e_path.c_str(), e_path.length());  // path
      n_ptr += e_path.length();
      *(size_t *)n_ptr = e_field_count;  // field count
      e_ptr += sizeof(size_t);
      utils::BitSet n_null_mask(field_count);
      auto n_null_offset = n_ptr - value_buff.get();  // null mask
      n_ptr += n_null_mask.data_size();
      int64_t *n_field_head = (int64_t *)n_ptr;  // field head
      n_ptr += sizeof(int64_t) * e_field_count;

      for (uint i = 0; i < field_count; i++) {
        // resize
        size_t length = field_head[i] > 0 ? field_head[i] : e_field_head[i] > 0 ? e_field_head[i] : 0;
        size_t used = ptr - value_buff.get();
        if (value_buff_size - used < length) {
          while (value_buff_size - used < length) {
            value_buff_size *= 2;
            if (value_buff_size > utils::MappedCircularBuffer::MAX_BUF_SIZE)
              throw common::Exception(e_path + " INSERT data exceeds max buffer size " +
                                      std::to_string(utils::MappedCircularBuffer::MAX_BUF_SIZE));
          }
          auto old_value_buff = std::move(value_buff);
          value_buff = std::make_unique<char[]>(value_buff_size);
          std::memcpy(value_buff.get(), old_value_buff.get(), used);
          n_ptr = value_buff.get() + used;
        }
        if (null_mask[i]) {
          n_null_mask.set(i);
          n_field_head[i] = -1;
          if (!e_null_mask[i]) {
            e_ptr += e_field_head[i];
          }
        } else if (update_mask[i]) {
          std::memcpy(n_ptr, ptr, field_head[i]);
          n_field_head[i] = field_head[i];
          n_ptr += field_head[i];
          ptr += field_head[i];
          if (!e_null_mask[i]) {
            e_ptr += e_field_head[i];
          }
        } else if (e_null_mask[i]) {
          n_null_mask.set(i);
          n_field_head[i] = -1;
        } else {
          std::memcpy(n_ptr, e_ptr, e_field_head[i]);
          n_field_head[i] = e_field_head[i];
          n_ptr += e_field_head[i];
          e_ptr += e_field_head[i];
        }
      }
      std::memcpy(value_buff.get() + n_null_offset, null_mask.data(), null_mask.data_size());
      new_value->assign(value_buff.get(), n_ptr - value_buff.get());
    } else if (type == RecordType::kDelete) {
      // todo(dfx): delete
    } else {
      return false;
    }
  } else if (existing_type == RecordType::kUpdate) {
    if (type == RecordType::kUpdate) {
      new_type = RecordType::kUpdate;
      // parse table id and table path
      ptr += sizeof(int32_t);
      std::string path(ptr);
      ptr += path.length() + 1;
      // get field count
      size_t field_count = *(size_t *)ptr;
      ptr += sizeof(size_t);
      // get update mask
      utils::BitSet update_mask(field_count, const_cast<char *>(ptr));
      ptr += update_mask.data_size();
      // get null mask
      utils::BitSet null_mask(field_count, const_cast<char *>(ptr));
      ptr += null_mask.data_size();
      // get update_head
      int64_t *field_head = (int64_t *)ptr;
      ptr += sizeof(int64_t) * field_count;

      // parse existing value table id and table path
      int32_t e_tid = *(int32_t *)e_ptr;
      e_ptr += sizeof(int32_t);
      std::string e_path(e_ptr);
      e_ptr += path.length() + 1;
      // get existing field count
      size_t e_field_count = *(size_t *)ptr;
      e_ptr += sizeof(size_t);
      assert(e_field_count == field_count);
      // get update mask
      utils::BitSet e_update_mask(field_count, const_cast<char *>(ptr));
      ptr += e_update_mask.data_size();
      // get existing null mask
      utils::BitSet e_null_mask(e_field_count, const_cast<char *>(ptr));
      e_ptr += e_null_mask.data_size();
      // get existing update_head
      int64_t *e_field_head = (int64_t *)ptr;
      e_ptr += sizeof(int64_t) * e_field_count;

      // assemble new value
      *(RecordType *)n_ptr = new_type;  // new_type
      n_ptr += sizeof(RecordType);
      *(int32 *)n_ptr = e_tid;  // tid
      n_ptr += sizeof(int32_t);
      std::memcpy(n_ptr, e_path.c_str(), e_path.length());  // path
      n_ptr += e_path.length();
      utils::BitSet n_update_mask(field_count);  // update mask
      auto n_update_offset = n_ptr - value_buff.get();
      n_ptr += n_update_mask.data_size();
      utils::BitSet n_null_mask(field_count);  // null mask
      auto n_null_offset = n_ptr - value_buff.get();
      n_ptr += n_null_mask.data_size();
      int64_t *n_field_head = (int64_t *)n_ptr;  // field head
      n_ptr += sizeof(int64_t) * e_field_count;

      for (uint i = 0; i < field_count; i++) {
        // resize
        size_t length = field_head[i] > 0 ? field_head[i] : e_field_head[i] > 0 ? e_field_head[i] : 0;
        size_t used = ptr - value_buff.get();
        if (value_buff_size - used < length) {
          while (value_buff_size - used < length) {
            value_buff_size *= 2;
            if (value_buff_size > utils::MappedCircularBuffer::MAX_BUF_SIZE)
              throw common::Exception(e_path + " INSERT data exceeds max buffer size " +
                                      std::to_string(utils::MappedCircularBuffer::MAX_BUF_SIZE));
          }
          auto old_value_buff = std::move(value_buff);
          value_buff = std::make_unique<char[]>(value_buff_size);
          std::memcpy(value_buff.get(), old_value_buff.get(), used);
          n_ptr = value_buff.get() + used;
        }
        if (null_mask[i]) {
          n_null_mask.set(i);
          n_field_head[i] = -1;
          if (e_update_mask[i]) {
            e_ptr += e_field_head[i];
          }
        } else if (update_mask[i]) {
          std::memcpy(n_ptr, ptr, field_head[i]);
          n_field_head[i] = field_head[i];
          n_ptr += field_head[i];
          ptr += field_head[i];
          if (e_update_mask[i]) {
            n_field_head[i] = -1;
            e_ptr += e_field_head[i];
          }
        } else if (e_null_mask[i]) {
          n_null_mask.set(i);
        } else if (e_update_mask[i]) {
          std::memcpy(n_ptr, e_ptr, e_field_head[i]);
          n_field_head[i] = e_field_head[i];
          n_ptr += e_field_head[i];
          e_ptr += e_field_head[i];
        }
      }
      std::memcpy(value_buff.get() + n_update_offset, update_mask.data(), update_mask.data_size());
      std::memcpy(value_buff.get() + n_null_offset, null_mask.data(), null_mask.data_size());
      new_value->assign(value_buff.get(), n_ptr - value_buff.get());
    } else if (type == RecordType::kDelete) {
      // todo(dfx): delete
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
