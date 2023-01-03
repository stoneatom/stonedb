//
// Created by dfx on 22-12-15.
//

#ifndef TIANMU_CORE_MERGE_OPERATOR_H_
#define TIANMU_CORE_MERGE_OPERATOR_H_

#include "rocksdb/merge_operator.h"

namespace Tianmu::core {

class RecordMergeOperator : public rocksdb::AssociativeMergeOperator {
 public:
  bool Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value, const rocksdb::Slice &value,
                     std::string *new_value, rocksdb::Logger *logger) const override;

  [[nodiscard]] const char *Name() const override { return "RecordMergeOperator"; };
};
}  // namespace Tianmu::core
#endif  // TIANMU_CORE_MERGE_OPERATOR_H_
