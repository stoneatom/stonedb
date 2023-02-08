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
  CombinedIterator(TianmuTable *table, const std::vector<bool> &attrs, const Filter &filter);
  CombinedIterator(TianmuTable *table, const std::vector<bool> &attrs);
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
  bool IsDelta() const;

 private:
  bool is_delta_ = true;
  std::unique_ptr<DeltaIterator> delta_iter_;
  std::unique_ptr<TianmuIterator> base_iter_;
};
}  // namespace Tianmu::core
#endif  // MYSQL_STORAGE_TIANMU_CORE_MERGE_ITERATOR_H_
