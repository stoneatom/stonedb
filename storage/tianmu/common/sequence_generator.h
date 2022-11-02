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
#ifndef TIANMU_COMMON_SEQUENCE_GENERATOR_H_
#define TIANMU_COMMON_SEQUENCE_GENERATOR_H_
#pragma once

#include <atomic>
#include <chrono>

namespace Tianmu {
namespace common {

class SequenceGenerator {
 public:
  SequenceGenerator(const SequenceGenerator &) = delete;
  SequenceGenerator &operator=(const SequenceGenerator &) = delete;
  SequenceGenerator() {
    using namespace std::chrono;
    sequence_.store(duration_cast<seconds>(system_clock::now().time_since_epoch()).count() << 32);
    sequence_ += 10000;  // in case the server restarts into service in less than
                         // one second
  }

  uint64_t NextID() { return ++sequence_; }

 private:
  std::atomic<std::uint64_t> sequence_;
};

}  // namespace common
}  // namespace Tianmu

#endif  // TIANMU_COMMON_SEQUENCE_GENERATOR_H_
