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
#ifndef TIANMU_SYSTEM_STREAM_H_
#define TIANMU_SYSTEM_STREAM_H_
#pragma once

#include <string>

namespace Tianmu {
namespace system {
class Stream {
 public:
  Stream() = default;
  virtual ~Stream() = default;

  virtual bool IsOpen() const = 0;
  virtual int Close() = 0;
  virtual int OpenReadOnly(std::string const &filename) = 0;
  virtual int OpenReadWrite(std::string const &filename) = 0;
  virtual int OpenCreateEmpty(std::string const &filename) = 0;
  virtual void WriteExact(const void *buf, size_t count) = 0;
  virtual size_t Read(void *buf, size_t count) = 0;
  virtual void ReadExact(void *buf, size_t count) = 0;
  void ThrowError(int errnum);
  void ThrowError(std::string serror);
  const std::string &Name() const { return name_; }

 protected:
  std::string name_;
};

}  // namespace system
}  // namespace Tianmu

#endif  // TIANMU_SYSTEM_STREAM_H_
