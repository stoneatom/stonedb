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

#include "mi_step_iterator.h"

namespace stonedb {
namespace core {
// MIStepIterator
MIStepIterator::MIStepIterator(int start, int step, const MIIterator &sec) : MIIterator(sec), step_(step) {
  RewindToPack(start);
}

// MIPackStepIterator
MIPackStepIterator::MIPackStepIterator(int start, int step, const MIIterator &sec) : MIStepIterator(start, step, sec) {}

MIIterator &MIPackStepIterator::Increment() {
  MIIterator &iter = this->operator++();
  if (next_pack_started) {
    step_already_++;
    if (step_already_ == step_) {
      valid = false;
    }
  }
  return iter;
}

// MIRowsStepIterator
MIRowsStepIterator::MIRowsStepIterator(int64_t start, int step, int64_t origin_size, const MIIterator &sec)
    : MIIterator(sec) {
  do {
    RewindToRow(start);
    start++;
  } while (!IsValid() && start < origin_size);

  rows_finish_ = (start - 1) + step;
}

MIIterator &MIRowsStepIterator::Increment() {
  MIIterator &iter = this->operator++();
  if (iter.IsValid()) {
    DEBUG_ASSERT(GetOneFilterDim() > -1);
    int64_t cur_pos = iter[iter.GetOneFilterDim()];
    if (cur_pos >= rows_finish_) {
      valid = false;
    }
  }
  return iter;
}

}  // namespace core
}  // namespace stonedb