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
#ifndef STONEDB_SYSTEM_CHANNEL_OUT_H_
#define STONEDB_SYSTEM_CHANNEL_OUT_H_
#pragma once

#include <iomanip>

#include "common/common_definitions.h"
#include "types/rc_data_types.h"

namespace stonedb {
namespace system {
class ChannelOut {
 public:
  virtual ChannelOut &operator<<(short value) = 0;
  virtual ChannelOut &operator<<(int value) = 0;
  virtual ChannelOut &operator<<(long value) = 0;

  virtual ChannelOut &operator<<(float value) = 0;
  virtual ChannelOut &operator<<(double value) = 0;
  virtual ChannelOut &operator<<(long double value) = 0;

  virtual ChannelOut &operator<<(unsigned short value) = 0;
  virtual ChannelOut &operator<<(unsigned int value) = 0;
  virtual ChannelOut &operator<<(unsigned long value) = 0;

  virtual ChannelOut &operator<<(int long long value) = 0;
  virtual ChannelOut &operator<<(int long long unsigned value) = 0;

  virtual ChannelOut &operator<<(char c) = 0;
  virtual ChannelOut &operator<<(const char *buffer) = 0;
  virtual ChannelOut &operator<<(const wchar_t *buffer) = 0;
  virtual ChannelOut &operator<<(const std::string &str) = 0;
  ChannelOut &operator<<(types::BString &rcbs) {
    for (ushort i = 0; i < rcbs.len; i++) (*this) << (char)(rcbs[i]);
    return *this;
  }
  ChannelOut &operator<<(const std::exception &exc) {
    (*this) << exc.what();
    return *this;
  };

  virtual void setf(std::ios_base::fmtflags _Mask) = 0;
  virtual void precision(std::streamsize prec) = 0;

  virtual ChannelOut &flush() = 0;
  virtual ChannelOut &fixed() = 0;
  virtual void close() = 0;

  virtual ChannelOut &operator<<(ChannelOut &(*_Pfn)(ChannelOut &)) = 0;
  virtual ~ChannelOut(){};
};

inline ChannelOut &endl(ChannelOut &inout) {
  inout << '\n';
  return inout.flush();
}

inline ChannelOut &fixed(ChannelOut &inout) { return inout.fixed(); }
}  // namespace system
}  // namespace stonedb

#endif  // STONEDB_SYSTEM_CHANNEL_OUT_H_
