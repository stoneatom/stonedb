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

#include "stonedb_file.h"

#include "common/assert.h"

namespace stonedb {
namespace system {

int StoneDBFile::Open(std::string const &file, int flags, mode_t mode) {
  DEBUG_ASSERT(file.length());
  name = file;

  fd = open(file.c_str(), flags, mode);
  if (fd == -1) ThrowError(errno);
  return fd;
}

size_t StoneDBFile::Read(void *buf, size_t count) {
  DEBUG_ASSERT(fd != -1);
  auto read_bytes = read(fd, buf, count);
  if (read_bytes == -1) ThrowError(errno);
  return read_bytes;
}

int StoneDBFile::Flush() {
  int ret;
#ifdef HAVE_FDATASYNC
  ret = fdatasync(fd);
#else
  ret = fsync(fd);
#endif
  return ret;
}

int StoneDBFile::OpenCreate(std::string const &file) {
  return Open(file, O_CREAT | O_RDWR | O_LARGEFILE | O_BINARY, ib_umask);
}

int StoneDBFile::OpenCreateNotExists(std::string const &file) {
  return Open(file, O_CREAT | O_EXCL | O_RDWR | O_LARGEFILE | O_BINARY, ib_umask);
}

int StoneDBFile::OpenCreateEmpty(std::string const &file) {
  return Open(file, O_CREAT | O_RDWR | O_TRUNC | O_LARGEFILE | O_BINARY, ib_umask);
}

int StoneDBFile::OpenReadOnly(std::string const &file) {
  return Open(file, O_RDONLY | O_LARGEFILE | O_BINARY, ib_umask);
}

int StoneDBFile::OpenReadWrite(std::string const &file) {
  return Open(file, O_RDWR | O_LARGEFILE | O_BINARY, ib_umask);
}

void StoneDBFile::ReadExact(void *buf, size_t count) {
  size_t read_bytes = 0;
  while (read_bytes < count) {
    auto rb = Read((char *)buf + read_bytes, count - read_bytes);
    if (rb == 0) break;
    read_bytes += rb;
  }
  if (read_bytes != count) {
    ThrowError("Failed to read " + std::to_string(count) + " bytes from " + name + ". returned " +
               std::to_string(read_bytes));
  }
}

void StoneDBFile::WriteExact(const void *buf, size_t count) {
  DEBUG_ASSERT(fd != -1);
  size_t total_writen_bytes = 0;
  while (total_writen_bytes < count) {
    auto writen_bytes = write(fd, ((char *)buf) + total_writen_bytes, count - total_writen_bytes);
    if (writen_bytes == -1) ThrowError(errno);
    total_writen_bytes += writen_bytes;
  }
}

off_t StoneDBFile::Seek(off_t pos, int whence) {
  off_t new_pos = -1;
  DEBUG_ASSERT(fd != -1);

  new_pos = lseek(fd, pos, whence);

  if (new_pos == (off_t)-1) ThrowError(errno);
  return new_pos;
}

off_t StoneDBFile::Tell() {
  off_t pos;
  DEBUG_ASSERT(fd != -1);
  pos = lseek(fd, 0, SEEK_CUR);
  if (pos == (off_t)-1) ThrowError(errno);
  return pos;
}

bool StoneDBFile::IsOpen() const { return (fd != -1 ? true : false); }

int StoneDBFile::Close() {
  int sdb_err = 0;
  if (fd != -1) {
    sdb_err = close(fd);
    name = "";
    fd = -1;
  }
  return sdb_err;
}
}  // namespace system
}  // namespace stonedb
