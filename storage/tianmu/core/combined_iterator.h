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
// Created by dfx on 22-12-19.
//

#ifndef MYSQL_STORAGE_TIANMU_CORE_MERGE_ITERATOR_H_
#define MYSQL_STORAGE_TIANMU_CORE_MERGE_ITERATOR_H_

#include "delta_table.h"
#include "tianmu_attr_typeinfo.h"
#include "tianmu_table.h"

// new iterator not Begin() and End() function;
// only use Valid() to check iterator is valid.

namespace Tianmu::core {
class CombinedIterator {
 public:
  CombinedIterator() = default;
  ~CombinedIterator() = default;
  CombinedIterator(TianmuTable *base_table, const std::vector<bool> &attrs, const Filter &filter);
  CombinedIterator(TianmuTable *base_table, const std::vector<bool> &attrs);
  // check two iterator is same
  bool operator==(const CombinedIterator &other);
  bool operator!=(const CombinedIterator &other);
  // goto next row
  void Next();
  // get tianmu record col data
  std::shared_ptr<types::TianmuDataType> &GetBaseData(int col = -1);
  // get delta record row data
  std::string GetDeltaData();
  // move position to this row (:row_id)
  void SeekTo(int64_t row_id);
  // get the current row_id
  int64_t Position() const;
  // check the iterator is valid
  bool Valid() const;
  // check the iterator is delta || base
  bool IsBase() const;
  // Check whether the current line is invalid
  bool BaseCurrentRowIsInvalid() const;

  std::unordered_map<int64_t, bool> InDeltaUpdateRow;
  std::unordered_map<int64_t, bool> InDeltaDeletedRow;
  const TianmuTable *GetBaseTable() const { return base_table_; }

 private:
  TianmuTable *base_table_ = nullptr;
  std::vector<bool> attrs_;
  bool is_base_ = false;  // make sure that is_base_=true base_iter_ is Valid
  std::unique_ptr<DeltaIterator> delta_iter_;
  std::unique_ptr<TianmuIterator> base_iter_;
};
}  // namespace Tianmu::core
#endif  // MYSQL_STORAGE_TIANMU_CORE_MERGE_ITERATOR_H_
