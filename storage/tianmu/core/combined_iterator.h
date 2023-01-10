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
  CombinedIterator(DeltaIterator delta, Tianmu::core::TianmuIterator base)
      : is_delta_(false), delta_iter_(std::move(delta)), base_iter_(std::move(base)){};
  CombinedIterator(CombinedIterator &&other) {
    is_delta_ = other.is_delta_;
    delta_iter_ = std::move(other.delta_iter_);
    base_iter_ = std::move(other.base_iter_);
  }
  CombinedIterator &operator=(CombinedIterator &&other) {
    is_delta_ = other.is_delta_;
    delta_iter_ = std::move(other.delta_iter_);
    base_iter_ = std::move(other.base_iter_);
    return *this;
  }

  void Inited();

  bool operator==(const CombinedIterator &other) {
    return is_delta_ == other.is_delta_ && is_delta_ ? delta_iter_ == other.delta_iter_
                                                     : base_iter_ == other.base_iter_;
  }
  bool operator!=(const CombinedIterator &other) { return !(*this == other); }
  void operator++(int) {
    std::lock_guard<std::mutex> guard(lock_);
    if (is_delta_) {
      delta_iter_++;
      if (!delta_iter_.Valid()) {
        is_delta_ = false;
      }
    } else {
      base_iter_++;
    }
  }

  // get tiannmu record col data
  std::shared_ptr<types::TianmuDataType> &GetBaseData(int col = -1) { return base_iter_.GetData(col); }
  // get delta record row data
  std::string GetDeltaData() { return delta_iter_.GetData(); }
  void MoveTo(int64_t row_id) {
    std::lock_guard<std::mutex> guard(lock_);
    int64_t delta_start_pos = delta_iter_.StartPosition();
    if (row_id >= delta_start_pos) {  // delta
      delta_iter_.MoveTo(row_id);
      is_delta_ = true;
    } else {  // base
      base_iter_.MoveTo(row_id);
      is_delta_ = false;
    }
  }
  int64_t Position() const { is_delta_ ? delta_iter_.Position() : base_iter_.Position(); }
  bool Valid() const { return Position() != -1; }
  bool IsDelta() const { return is_delta_; }

 private:
  std::mutex lock_;
  bool is_delta_ = true;
  DeltaIterator delta_iter_;
  Tianmu::core::TianmuIterator base_iter_;

 public:
  static CombinedIterator Create(TianmuTable *table, const std::vector<bool> &attrs, const Filter &filter) {
    DeltaIterator delta_iter(table->GetDelta().get(), attrs);
    Tianmu::core::TianmuIterator base_iter(table, attrs, filter);
    return {std::move(delta_iter), std::move(base_iter)};
  }
  static CombinedIterator Create(TianmuTable *table, const std::vector<bool> &attrs) {
    DeltaIterator delta_iter(table->GetDelta().get(), attrs);
    Tianmu::core::TianmuIterator base_iter(table, attrs);
    return {std::move(delta_iter), std::move(base_iter)};
  }
};
}  // namespace Tianmu::core
#endif  // MYSQL_STORAGE_TIANMU_CORE_MERGE_ITERATOR_H_
