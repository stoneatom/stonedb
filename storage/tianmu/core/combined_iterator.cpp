//
// Created by dfx on 22-12-19.
//

#include "combined_iterator.h"

namespace Tianmu::core {

CombinedIterator::CombinedIterator(TianmuTable *base_table, const std::vector<bool> &attrs, const Filter &filter)
    : base_table_(base_table), attrs_(attrs) {
  base_iter_ = std::make_unique<TianmuIterator>(base_table, attrs, filter);
  delta_iter_ = std::make_unique<DeltaIterator>(base_table_->GetDelta().get(), attrs_);
  is_base_ = !delta_iter_->Valid();
}

CombinedIterator::CombinedIterator(TianmuTable *base_table, const std::vector<bool> &attrs)
    : base_table_(base_table), attrs_(attrs) {
  base_iter_ = std::make_unique<TianmuIterator>(base_table, attrs);
  delta_iter_ = std::make_unique<DeltaIterator>(base_table_->GetDelta().get(), attrs_);
  is_base_ = !delta_iter_->Valid();
}

bool CombinedIterator::operator==(const CombinedIterator &o) {
  return is_base_ == o.is_base_ && (is_base_ ? base_iter_ == o.base_iter_ : delta_iter_ == o.delta_iter_);
}

bool CombinedIterator::operator!=(const CombinedIterator &other) { return !(*this == other); }

void CombinedIterator::Next() {
  if (!is_base_) {
    delta_iter_->Next();
    if (!delta_iter_->Valid()) {
      is_base_ = true;
    }
  } else {
    base_iter_->Next();
    if (base_iter_->Valid()) {
      TIANMU_LOG(LogCtl_Level::ERROR, "base pos: %d", base_iter_->Position());
    }
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

bool CombinedIterator::BaseCurrentRowIsDeleted() const {
  if (base_iter_->CurrentRowIsDeleted() ||
      delta_iter_->GetTable()->BaseRowIsDeleted(current_txn_, base_iter_->Position())) {
    return true;
  }
  return false;
}

}  // namespace Tianmu::core