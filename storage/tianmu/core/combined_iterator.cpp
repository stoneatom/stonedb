//
// Created by dfx on 22-12-19.
//

#include "combined_iterator.h"

namespace Tianmu::core {

CombinedIterator::CombinedIterator(TianmuTable *base_table, const std::vector<bool> &attrs, const Filter &filter)
    : base_table_(base_table), attrs_(attrs), is_base_(true) {
  base_iter_ = std::make_unique<TianmuIterator>(base_table, attrs, filter);
  // lazy create delta iter
}

CombinedIterator::CombinedIterator(TianmuTable *base_table, const std::vector<bool> &attrs)
    : base_table_(base_table), attrs_(attrs), is_base_(true) {
  base_iter_ = std::make_unique<TianmuIterator>(base_table, attrs);
  // lazy create delta iter
}

bool CombinedIterator::operator==(const CombinedIterator &o) {
  return is_base_ == o.is_base_ && (is_base_ ? base_iter_ == o.base_iter_ : delta_iter_ == o.delta_iter_);
}

bool CombinedIterator::operator!=(const CombinedIterator &other) { return !(*this == other); }

void CombinedIterator::Next() {
  if (is_base_) {
    if (base_iter_->Valid()) {
      base_iter_->Next();
      if (!base_iter_->Valid()) {  // switch to delta
        delta_iter_ = std::make_unique<DeltaIterator>(base_table_->GetDelta().get(), attrs_);
        is_base_ = false;
      }
    }
  } else {
    delta_iter_->Next();
  }
}

std::shared_ptr<types::TianmuDataType> &CombinedIterator::GetBaseData(int col) { return base_iter_->GetData(col); }

std::string CombinedIterator::GetDeltaData() { return delta_iter_->GetData(); }

void CombinedIterator::SeekTo(int64_t row_id) {
  int64_t base_max_row_id = base_table_->NumOfObj();
  if (row_id <= base_max_row_id) {
    base_iter_->SeekTo(row_id);
    is_base_ = true;
  } else {
    if (!delta_iter_) {  // lazy create
      delta_iter_ = std::make_unique<DeltaIterator>(base_table_->GetDelta().get(), attrs_);
    }
    delta_iter_->SeekTo(row_id);
    is_base_ = false;
  }
}

int64_t CombinedIterator::Position() const {
  if (is_base_) {
    return base_iter_->Position();
  } else {
    return delta_iter_->Position();
  }
}

bool CombinedIterator::Valid() const { return Position() != -1; }

bool CombinedIterator::IsBase() const { return is_base_; }

}  // namespace Tianmu::core
