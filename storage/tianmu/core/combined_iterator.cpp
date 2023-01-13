//
// Created by dfx on 22-12-19.
//

#include "combined_iterator.h"

namespace Tianmu::core {
CombinedIterator::CombinedIterator(CombinedIterator &&other) noexcept {
  is_delta_ = other.is_delta_;
  delta_iter_ = std::move(other.delta_iter_);
  base_iter_ = std::move(other.base_iter_);
}

CombinedIterator &CombinedIterator::operator=(CombinedIterator &&other) noexcept {
  is_delta_ = other.is_delta_;
  delta_iter_ = std::move(other.delta_iter_);
  base_iter_ = std::move(other.base_iter_);
  return *this;
}

bool CombinedIterator::operator==(const CombinedIterator &other) {
  return is_delta_ == other.is_delta_ && is_delta_ ? delta_iter_ == other.delta_iter_ : base_iter_ == other.base_iter_;
}

bool CombinedIterator::operator!=(const CombinedIterator &other) { return !(*this == other); }

void CombinedIterator::operator++(int) {
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

std::shared_ptr<types::TianmuDataType> &CombinedIterator::GetBaseData(int col) { return base_iter_.GetData(col); }

std::string CombinedIterator::GetDeltaData() { return delta_iter_.GetData(); }

void CombinedIterator::MoveTo(int64_t row_id) {
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

int64_t CombinedIterator::Position() const { is_delta_ ? delta_iter_.Position() : base_iter_.Position(); }

bool CombinedIterator::Valid() const { return Position() != -1; }

bool CombinedIterator::IsDelta() const { return is_delta_; }

}  // namespace Tianmu::core
