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
#ifndef TIANMU_CORE_DELTA_RECORD_HEAD_H_
#define TIANMU_CORE_DELTA_RECORD_HEAD_H_
#pragma once

#include <string>
#include "util/bitset.h"

namespace Tianmu {
namespace core {

#define DELTA_RECORD_DELETE 'd'
#define DELTA_RECORD_NORMAL 'n'

enum class RecordType { RecordType_min, kSchema, kInsert, kUpdate, kDelete, RecordType_max };

// Record header encoding and decoding
class DeltaRecordHead {
 public:
  virtual char *record_encode(char *ptr) = 0;
  virtual const char *record_decode(const char *ptr) = 0;
};

class DeltaRecordHeadForInsert : public DeltaRecordHead {
  // layout:
  //     (Insert)TypeFlag
  //     load_num [count of currently loaded data, which is 1 by default, increased when the merge operator is triggered.]
  //     isDeleted [normal:n deleted:d]
  //     table_id
  //     table_path
  //     fields_size
  //     fields_count
  //     null_mask
  //     field_head
  //     fields...
 public:
  DeltaRecordHeadForInsert() = default;
  ~DeltaRecordHeadForInsert() = default;
  DeltaRecordHeadForInsert(char is_deleted, int32_t &table_id, const std::string &table_path, size_t &field_count, uint32_t load_num = 1);
  char *record_encode(char *ptr) override;
  const char *record_decode(const char *ptr) override;

 public:
  RecordType record_type_ = RecordType::kInsert;

 public:
  uint32_t load_num_;
  char is_deleted_;
  int32_t table_id_;
  std::string table_path_;
  size_t field_count_;
  utils::BitSet null_mask_;

  uint32_t *field_head_;
  int null_offset_;
  int field_offset_;
};

class DeltaRecordHeadForUpdate : public DeltaRecordHead {
  // layout:
  //     (Update)TypeFlag
  //     load_id
  //     table_id
  //     table_path
  //     fields_count
  //     update_mask
  //     null_mask
  //     field_head
  //     update_fields...
 public:
  DeltaRecordHeadForUpdate() = default;
  ~DeltaRecordHeadForUpdate() = default;
  DeltaRecordHeadForUpdate(int32_t &table_id, const std::string &table_path, size_t &field_count, uint32_t load_num = 1);
  char *record_encode(char *ptr) override;
  const char *record_decode(const char *ptr) override;

 public:
  RecordType record_type_ = RecordType::kUpdate;

 public:
  uint32_t load_num_;
  int32_t table_id_;
  std::string table_path_;
  size_t field_count_;
  utils::BitSet update_mask_;
  utils::BitSet null_mask_;
  uint32_t *field_head_;
  int null_offset_;
  int update_offset_;
  int field_offset_;
};

class DeltaRecordHeadForDelete : public DeltaRecordHead {
 public:
  DeltaRecordHeadForDelete() = default;
  ~DeltaRecordHeadForDelete() = default;
  DeltaRecordHeadForDelete(int32_t &table_id, const std::string &table_path, uint32_t load_num = 1);
  char *record_encode(char *ptr) override;
  const char *record_decode(const char *ptr) override;

 public:
  RecordType record_type_ = RecordType::kDelete;

 public:
  uint32_t load_num_;
  int32_t table_id_;
  std::string table_path_;
};

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_DELTA_RECORD_HEAD_H_