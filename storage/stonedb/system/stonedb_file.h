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
#ifndef STONEDB_SYSTEM_STONEDB_FILE_H_
#define STONEDB_SYSTEM_STONEDB_FILE_H_
#pragma once

#include "common/exception.h"
#include "system/file_system.h"
#include "system/stream.h"

namespace stonedb {
namespace system {

#define O_BINARY 0

#define ib_umask S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP

/* A portable wrapper class for low level file I/O.
  Note - Most of the functions throws exception if it fails. So caller must
  handle exception appropriately.
*/

class StoneDBFile : public Stream {
  int fd;

 public:
  StoneDBFile() { fd = -1; }
  ~StoneDBFile() {
    if (fd != -1) Close();
  }
  off_t Seek(off_t pos, int whence);
  off_t Tell();
  int Flush();

  int Open(std::string const &file, int flags, mode_t mode);
  int OpenCreate(std::string const &file);
  int OpenCreateNotExists(std::string const &file);
  int OpenCreateEmpty(std::string const &file) override;
  int OpenReadOnly(std::string const &file) override;
  int OpenReadWrite(std::string const &file) override;
  size_t Read(void *buf, size_t count) override;
  void ReadExact(void *buf, size_t count) override;
  void WriteExact(const void *buf, size_t count) override;

  bool IsOpen() const override;
  int Close() override;
};

}  // namespace system
}  // namespace stonedb

#endif  // STONEDB_SYSTEM_STONEDB_FILE_H_
