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
#ifndef STONEDB_SYSTEM_NET_STREAM_H_
#define STONEDB_SYSTEM_NET_STREAM_H_
#pragma once

#include <string>

#include "common/exception.h"
#include "system/file_system.h"
#include "system/io_parameters.h"
#include "system/stream.h"

namespace stonedb {
namespace system {
class NetStream : public Stream {
 public:
  NetStream(const IOParameters &iop);
  virtual ~NetStream() { Close(); }
  bool IsOpen() const override;
  int Open();
  int Close() override;
  size_t Read(void *buf, size_t count) override;

  int OpenReadOnly(const std::string &) override { return 0; }
  int OpenReadWrite(const std::string &) override { return 0; }
  int OpenCreateEmpty(const std::string &) override { return 0; }
  void WriteExact(const void *, size_t) override {}
  void ReadExact(void *, size_t) override {}

 private:
  NET *net;
  bool opened;
  ulong cached_size;
  ulong cached_offset;
  bool can_read;
};
}  // namespace system
}  // namespace stonedb

#endif  // STONEDB_SYSTEM_NET_STREAM_H_
