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
#ifndef STONEDB_SYSTEM_CHANNEL_H_
#define STONEDB_SYSTEM_CHANNEL_H_
#pragma once

#include <exception>
#include <mutex>

#include "channel_out.h"

namespace stonedb {
namespace system {
class Channel {
 public:
  Channel(bool time_stamp_at_lock = false);
  Channel(ChannelOut *output, bool time_stamp_at_lock = false);
  ~Channel();

  void addOutput(ChannelOut *output);
  void setOn() { m_bEnabled = true; };
  void setOff() { m_bEnabled = false; };
  void setTimeStamp(bool _on = true) { m_bTimeStampAtLock = _on; };
  bool isOn();
  Channel &lock(uint optional_sess_id = 0xFFFFFFFF);
  Channel &unlock();

  Channel &operator<<(short value);
  Channel &operator<<(int value);

  Channel &operator<<(float value);
  Channel &operator<<(double value);
  Channel &operator<<(long double value);

  Channel &operator<<(unsigned short value);
  Channel &operator<<(unsigned int value);
  Channel &operator<<(unsigned long value);

  Channel &operator<<(int long value);
  Channel &operator<<(int long long value);
  Channel &operator<<(unsigned long long value);

  Channel &operator<<(const char *buffer);
  Channel &operator<<(const wchar_t *buffer);
  Channel &operator<<(char c);
  Channel &operator<<(const std::string &str);

  Channel &operator<<(const std::exception &exc);
  Channel &operator<<(Channel &(*_Pfn)(Channel &)) { return _Pfn(*this); };
  Channel &flush();

 private:
  std::vector<ChannelOut *> m_Outputs;

  bool m_bEnabled = true;
  std::mutex channel_mutex;

  bool m_bTimeStampAtLock;
};

inline Channel &lock(Channel &report) { return report.lock(); }

inline Channel &unlock(Channel &report) {
  report << '\n';
  report.flush();
  return report.unlock();
}

inline Channel &flush(Channel &report) { return report.flush(); }
}  // namespace system
}  // namespace stonedb

#endif  // STONEDB_SYSTEM_CHANNEL_H_
