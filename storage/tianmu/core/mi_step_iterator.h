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
#ifndef TIANMU_CORE_MI_STEP_ITERATOR_H_
#define TIANMU_CORE_MI_STEP_ITERATOR_H_
#pragma once

#include "core/mi_iterator.h"

namespace Tianmu {
namespace core {
class MIStepIterator : public MIIterator {
 public:
  MIStepIterator(int start, int step, const MIIterator &sec);

 protected:
  int step_ = 1;
};

class MIPackStepIterator : public MIStepIterator {
 public:
  // If step is 1, MIPackStepIterator is same as MIInpackIterator.
  MIPackStepIterator(int start, int step, const MIIterator &sec);

  MIIterator &Increment() override;

 private:
  int step_already_ = 0;
};

class MIRowsStepIterator : public MIIterator {
 public:
  MIRowsStepIterator(int64_t start, int step, int64_t origin_size, const MIIterator &sec);

  MIIterator &Increment() override;

 private:
  int64_t rows_finish_ = 0;
};

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_MI_STEP_ITERATOR_H_