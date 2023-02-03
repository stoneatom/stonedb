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
  CombinedIterator(const CombinedIterator &) = delete;
  CombinedIterator &operator=(const CombinedIterator &) = delete;
  CombinedIterator(DeltaIterator delta, TianmuIterator base)
      : delta_iter_(std::move(delta)), base_iter_(std::move(base)) {
    is_delta_ = delta_iter_.Valid() ? true : false;  // first read delta, then read base
  }
  CombinedIterator(CombinedIterator &&other) noexcept;
  CombinedIterator &operator=(CombinedIterator &&other) noexcept;

  // check two iterator is same
  bool operator==(const CombinedIterator &other);
  bool operator!=(const CombinedIterator &other);
  // goto next row
  void operator++(int);
  // get tianmu record col data
  std::shared_ptr<types::TianmuDataType> &GetBaseData(int col = -1);
  // get delta record row data
  std::string GetDeltaData();
  // move position to this row (:row_id)
  void MoveTo(int64_t row_id);
  // get the current row_id
  int64_t Position() const;
  // check the iterator is valid
  bool Valid() const;
  // check the iterator is delta || base
  bool IsDelta() const;

 private:
  std::mutex lock_;
  bool is_delta_ = true;
  DeltaIterator delta_iter_;
  TianmuIterator base_iter_;

 public:
  DeltaIterator &GetDeltaIterator() { return delta_iter_; }
  static CombinedIterator Create(TianmuTable *table, const std::vector<bool> &attrs, const Filter &filter) {
    DeltaIterator delta_iter(table->GetDelta().get(), attrs);
    TianmuIterator base_iter(table, attrs, filter);
    return {std::move(delta_iter), std::move(base_iter)};
  }
  static CombinedIterator Create(TianmuTable *table, const std::vector<bool> &attrs) {
    DeltaIterator delta_iter(table->GetDelta().get(), attrs);
    TianmuIterator base_iter(table, attrs);
    return {std::move(delta_iter), std::move(base_iter)};
  }
};
}  // namespace Tianmu::core
#endif  // MYSQL_STORAGE_TIANMU_CORE_MERGE_ITERATOR_H_
