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

#include "util/timer.h"

#include "system/tianmu_system.h"

namespace Tianmu {
namespace utils {

void Timer::DoPrint(const std::string &msg) const {
  using namespace std::chrono;
  auto diff = duration_cast<duration<float>>(high_resolution_clock::now() - start_);
  TIANMU_LOG(LogCtl_Level::INFO, "Timer %f seconds: %s", diff.count(), msg.c_str());
}

}  // namespace utils
}  // namespace Tianmu
