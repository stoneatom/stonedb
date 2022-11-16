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

#include <sys/syscall.h>
#include <ctime>
#include <mutex>

#include "channel.h"
#include "core/tools.h"
#include "system/tianmu_system.h"

namespace Tianmu {
namespace system {

Channel::Channel(bool time_stamp_at_lock) : time_stamp_at_lock_(time_stamp_at_lock) {}

Channel::Channel(ChannelOut *output, bool time_stamp_at_lock) : time_stamp_at_lock_(time_stamp_at_lock) {
  addOutput(output);
}

Channel::~Channel() {
  for (auto &out : channel_outputs_) delete out;
}

void Channel::addOutput(ChannelOut *output) { channel_outputs_.push_back(output); }

bool Channel::isOn() { return enabled_; }

Channel &Channel::lock(uint optional_sess_id) {
  channel_mutex_.lock();
  if (time_stamp_at_lock_ && enabled_) {
    time_t curtime = time(nullptr);
    struct tm *cdt = localtime(&curtime);
    char sdatetime[32] = "";
    struct timespec time;
    clock_gettime(CLOCK_REALTIME, &time);
    std::sprintf(sdatetime, "[%4d-%02d-%02d %02d:%02d:%02d.%06d]", cdt->tm_year + 1900, cdt->tm_mon + 1, cdt->tm_mday,
                 cdt->tm_hour, cdt->tm_min, cdt->tm_sec, static_cast<int>(time.tv_nsec / 1000));
    (*this) << sdatetime << ' ';
    if (optional_sess_id != 0xFFFFFFFF)
      (*this) << '[' << optional_sess_id << "] ";
    else
      (*this) << '[' << syscall(SYS_gettid) << "] ";
    // Uncomment this to see mm::TraceableObject report
    //		(*this) << "[freeable=" <<
    // mm::TraceableObject::GetFreeableSize()/1000000
    //<< "M,
    // unf.=" << mm::TraceableObject::GetUnFreeableSize()/1000000 << "M] ";
  }

  return *this;
}

Channel &Channel::unlock() {
  channel_mutex_.unlock();
  return *this;
}

Channel &Channel::operator<<(short value) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << value;
  return *this;
}

Channel &Channel::operator<<(int value) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << value;
  return *this;
}

Channel &Channel::operator<<(float value) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << value;
  return *this;
}

Channel &Channel::operator<<(double value) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << value;
  return *this;
}

Channel &Channel::operator<<(long double value) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << value;
  return *this;
}

Channel &Channel::operator<<(unsigned short value) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << value;
  return *this;
}

Channel &Channel::operator<<(unsigned int value) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << value;
  return *this;
}

Channel &Channel::operator<<(unsigned long value) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << value;
  return *this;
}

Channel &Channel::operator<<(int long value) { return operator<<(static_cast<int long long>(value)); }

Channel &Channel::operator<<(int long long value) {
  if (enabled_)
    for (auto &out : channel_outputs_) {
      if (value == common::NULL_VALUE_64)
        (*out) << "null";
      else if (value == common::PLUS_INF_64)
        (*out) << "+inf";
      else if (value == common::MINUS_INF_64)
        (*out) << "-inf";
      else
        (*out) << value;
    }
  return *this;
}

Channel &Channel::operator<<(unsigned long long value) {
  if (enabled_)
    for (auto &out : channel_outputs_) {
      if (value == (unsigned long long)common::NULL_VALUE_64)
        (*out) << "null";
      else if (value == common::PLUS_INF_64)
        (*out) << "+inf";
      else if (value == (unsigned long long)common::MINUS_INF_64)
        (*out) << "-inf";
      else
        (*out) << value;
    }
  return *this;
}

Channel &Channel::operator<<(const char *buffer) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << buffer;
  return *this;
}

Channel &Channel::operator<<(const wchar_t *buffer) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << buffer;
  return *this;
}

Channel &Channel::operator<<(char c) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << c;
  return *this;
}
Channel &Channel::operator<<(const std::string &str) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << str;
  return *this;
}

Channel &Channel::operator<<(const std::exception &exc) {
  if (enabled_)
    for (auto &out : channel_outputs_) (*out) << exc.what();
  return *this;
}

Channel &Channel::flush() {
  if (enabled_)
    for (auto &out : channel_outputs_) out->flush();
  return *this;
}

}  // namespace system
}  // namespace Tianmu
