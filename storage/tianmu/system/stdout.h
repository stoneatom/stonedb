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
#ifndef TIANMU_SYSTEM_STDOUT_H_
#define TIANMU_SYSTEM_STDOUT_H_
#pragma once

#include "system/channel_out.h"

namespace Tianmu {
namespace system {

class StdOut : public ChannelOut {
 public:
  ChannelOut &operator<<(short value) override {
    std::cout << value;
    return *this;
  }

  ChannelOut &operator<<(int value) override {
    std::cout << value;
    return *this;
  }

  ChannelOut &operator<<(long value) override {
    std::cout << value;
    return *this;
  }

  ChannelOut &operator<<(float value) override {
    std::cout << value;
    return *this;
  }

  ChannelOut &operator<<(double value) override {
    std::cout << value;
    return *this;
  }

  ChannelOut &operator<<(long double value) override {
    std::cout << value;
    return *this;
  }

  ChannelOut &operator<<(unsigned short value) override {
    std::cout << value;
    return *this;
  }

  ChannelOut &operator<<(unsigned int value) override {
    std::cout << value;
    return *this;
  }

  ChannelOut &operator<<(unsigned long value) override {
    std::cout << value;
    return *this;
  }

  ChannelOut &operator<<(int long long value) override {
    std::cout << value;
    return *this;
  }

  ChannelOut &operator<<(int long long unsigned value) override {
    std::cout << value;
    return *this;
  }

  ChannelOut &operator<<(char c) override {
    std::cout << c;
    return *this;
  }

  ChannelOut &operator<<(const char *buffer) override {
    std::cout << buffer;
    return *this;
  }

  ChannelOut &operator<<(const wchar_t *buffer) override {
    std::wcout << buffer;
    return *this;
  }

  ChannelOut &operator<<(const std::string &str) override {
    std::cout << str;
    return *this;
  }

  void setf(std::ios_base::fmtflags _Mask) override { std::cout.setf(_Mask); }
  void precision(std::streamsize prec) override { std::cout.precision(prec); }

  ChannelOut &flush() override {
    std::cout.flush();
    return *this;
  }

  ChannelOut &fixed() override {
    std::cout.setf(std::ios_base::fixed, std::ios_base::floatfield);
    return *this;
  }

  void close() override {}

  ChannelOut &operator<<(ChannelOut &(*_Pfn)(ChannelOut &)) override { return _Pfn(*this); }
};

}  // namespace system
}  // namespace Tianmu

#endif  // TIANMU_SYSTEM_STDOUT_H_
