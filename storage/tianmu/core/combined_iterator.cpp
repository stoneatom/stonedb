//
// Created by dfx on 22-12-19.
//

#include "combined_iterator.h"

namespace Tianmu::core {

CombinedIterator::CombinedIterator(TianmuTable *table, const std::vector<bool> &attrs, const Filter &filter) {
  delta_iter_ = std::make_unique<DeltaIterator>(table->GetDelta().get(), attrs);
  base_iter_ = std::make_unique<TianmuIterator>(table, attrs, filter);
  is_delta_ = delta_iter_->Valid() ? true : false;  // first read delta, then read base
}

CombinedIterator::CombinedIterator(TianmuTable *table, const std::vector<bool> &attrs) {
  delta_iter_ = std::make_unique<DeltaIterator>(table->GetDelta().get(), attrs);
  base_iter_ = std::make_unique<TianmuIterator>(table, attrs);
  is_delta_ = delta_iter_->Valid() ? true : false;
}

bool CombinedIterator::operator==(const CombinedIterator &other) {
  return is_delta_ == other.is_delta_ && is_delta_ ? delta_iter_ == other.delta_iter_ : base_iter_ == other.base_iter_;
}

bool CombinedIterator::operator!=(const CombinedIterator &other) { return !(*this == other); }

void CombinedIterator::Next() {
  if (is_delta_) {
    delta_iter_->Next();
    if (!delta_iter_->Valid()) {
      is_delta_ = false;
    }
  } else {
    base_iter_->Next();
  }
}

std::shared_ptr<types::TianmuDataType> &CombinedIterator::GetBaseData(int col) { return base_iter_->GetData(col); }

std::string CombinedIterator::GetDeltaData() { return delta_iter_->GetData(); }

void CombinedIterator::SeekTo(int64_t row_id) {
  int64_t delta_start_pos = delta_iter_->StartPosition();
  if (row_id >= delta_start_pos) {  // delta
    delta_iter_->SeekTo(row_id);
    is_delta_ = true;
  } else {  // base
    base_iter_->SeekTo(row_id);
    is_delta_ = false;
  }
}

int64_t CombinedIterator::Position() const {
  if (is_delta_ && delta_iter_) {
    return delta_iter_->Position();
  } else if (base_iter_) {
    return base_iter_->Position();
  } else {
    return -1;
  }
}

bool CombinedIterator::Valid() const { return Position() != -1; }

bool CombinedIterator::IsDelta() const { return is_delta_; }

}  // namespace Tianmu::core
