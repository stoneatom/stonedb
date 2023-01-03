//
// Created by dfx on 22-12-19.
//

#ifndef MYSQL_STORAGE_TIANMU_CORE_MERGE_ITERATOR_H_
#define MYSQL_STORAGE_TIANMU_CORE_MERGE_ITERATOR_H_

#include "delta_store.h"
#include "tianmu_table.h"
#include "tianmu_attr_typeinfo.h"

namespace Tianmu::core {
class MergeIterator {
 public:
  MergeIterator() = default;

  bool operator==(const MergeIterator &iter);
  bool operator!=(const MergeIterator &iter) { return !(*this == iter); }
  void operator++(int);

  std::shared_ptr<types::TianmuDataType> &GetData(int col) {}

  void MoveToRow(int64_t row_id);
  int64_t GetCurrentRowId() const { return position; }
//  bool Inited() const { return table != nullptr; }

 private:
  TianmuTable::Iterator *base_iter;
  DeltaTable::Iterator *delta_iter;
  int64_t position = -1;
  core::Transaction *conn = nullptr;
  bool current_record_fetched = false;
  std::vector<std::shared_ptr<types::TianmuDataType>> record;

 private:
  static MergeIterator CreateBegin(TianmuTable &table);
  static MergeIterator CreateEnd();
};
}
#endif  // MYSQL_STORAGE_TIANMU_CORE_MERGE_ITERATOR_H_
