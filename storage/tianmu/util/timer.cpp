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

#include "system/rc_system.h"

namespace Tianmu {
namespace utils {

void Timer::DoPrint(const std::string &msg) const {
  using namespace std::chrono;
  auto diff = duration_cast<duration<float>>(high_resolution_clock::now() - start_);
  TIANMU_LOG(LogCtl_Level::INFO, "Timer %f seconds: %s", diff.count(), msg.c_str());
}

KillTimer::KillTimer(THD *thd [[maybe_unused]], long secs) {
  if (secs == 0) return;
  struct sigevent sev;
  sev.sigev_notify = SIGEV_THREAD_ID;
  sev.sigev_signo = SIGRTMIN;
  sev._sigev_un._tid = syscall(SYS_gettid);
  sev.sigev_value.sival_ptr = thd;
  if (timer_create(CLOCK_MONOTONIC, &sev, &id)) {
    TIANMU_LOG(LogCtl_Level::INFO, "Failed to create timer. error =%d[%s]", errno, std::strerror(errno));
    return;
  }

  struct itimerspec interval;
  std::memset(&interval, 0, sizeof(interval));
  interval.it_value.tv_sec = secs;
  if (timer_settime(id, 0, &interval, NULL)) {
    TIANMU_LOG(LogCtl_Level::INFO, "Failed to set up timer. error =%d[%s]", errno, std::strerror(errno));
    return;
  }
  armed = true;
}

}  // namespace utils
}  // namespace Tianmu
