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

#ifndef TIANMU_CORE_MERGE_OPERATOR_H_
#define TIANMU_CORE_MERGE_OPERATOR_H_

#include "rocksdb/merge_operator.h"

namespace Tianmu::core {
// The Merge function of this class is mainly used for the callback of RocksDB to merge the data of the same key
class RecordMergeOperator : public rocksdb::AssociativeMergeOperator {
 public:
  bool Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value, const rocksdb::Slice &value,
             std::string *new_value, rocksdb::Logger *logger) const override;

  [[nodiscard]] const char *Name() const override { return "RecordMergeOperator"; };
};
}  // namespace Tianmu::core
#endif  // TIANMU_CORE_MERGE_OPERATOR_H_
