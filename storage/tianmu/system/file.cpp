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

#include "tianmu_file.h"

#include "common/assert.h"

namespace Tianmu {
namespace system {

int TianmuFile::Open(std::string const &file, int flags, mode_t mode) {
  DEBUG_ASSERT(file.length());
  name_ = file;

  fd_ = open(file.c_str(), flags, mode);
  if (fd_ == -1)
    throw common::TianmuError(common::ErrorCode::FAILED,
                              "ErrorCode: " + std::to_string(errno) + " - " + strerror(errno));
  return fd_;
}

size_t TianmuFile::Read(void *buf, size_t count) {
  DEBUG_ASSERT(fd_ != -1);
  auto read_bytes = read(fd_, buf, count);
  if (read_bytes == -1)
    ThrowError(errno);
  return read_bytes;
}

int TianmuFile::Flush() {
  int ret;
#ifdef HAVE_FDATASYNC
  ret = fdatasync(fd_);
#else
  ret = fsync(fd_);
#endif
  return ret;
}

int TianmuFile::OpenCreate(std::string const &file) {
  return Open(file, O_CREAT | O_RDWR | O_LARGEFILE | O_BINARY, tianmu_umask);
}

int TianmuFile::OpenCreateNotExists(std::string const &file) {
  return Open(file, O_CREAT | O_EXCL | O_RDWR | O_LARGEFILE | O_BINARY, tianmu_umask);
}

int TianmuFile::OpenCreateEmpty(std::string const &file) {
  return Open(file, O_CREAT | O_RDWR | O_TRUNC | O_LARGEFILE | O_BINARY, tianmu_umask);
}

int TianmuFile::OpenReadOnly(std::string const &file) {
  return Open(file, O_RDONLY | O_LARGEFILE | O_BINARY, tianmu_umask);
}

int TianmuFile::OpenReadWrite(std::string const &file) {
  return Open(file, O_RDWR | O_LARGEFILE | O_BINARY, tianmu_umask);
}

void TianmuFile::ReadExact(void *buf, size_t count) {
  size_t read_bytes = 0;
  while (read_bytes < count) {
    auto rb = Read((char *)buf + read_bytes, count - read_bytes);
    if (rb == 0)
      break;
    read_bytes += rb;
  }
  if (read_bytes != count) {
    ThrowError("Failed to read " + std::to_string(count) + " bytes from " + name_ + ". returned " +
               std::to_string(read_bytes));
  }
}

void TianmuFile::WriteExact(const void *buf, size_t count) {
  DEBUG_ASSERT(fd_ != -1);
  size_t total_writen_bytes = 0;
  while (total_writen_bytes < count) {
    auto writen_bytes = write(fd_, ((char *)buf) + total_writen_bytes, count - total_writen_bytes);
    if (writen_bytes == -1)
      ThrowError(errno);
    total_writen_bytes += writen_bytes;
  }
}

off_t TianmuFile::Seek(off_t pos, int whence) {
  off_t new_pos = -1;
  DEBUG_ASSERT(fd_ != -1);

  new_pos = lseek(fd_, pos, whence);

  if (new_pos == (off_t)-1)
    ThrowError(errno);
  return new_pos;
}

off_t TianmuFile::Tell() {
  off_t pos;
  DEBUG_ASSERT(fd_ != -1);
  pos = lseek(fd_, 0, SEEK_CUR);
  if (pos == (off_t)-1)
    ThrowError(errno);
  return pos;
}

bool TianmuFile::IsOpen() const { return (fd_ != -1 ? true : false); }

int TianmuFile::Close() {
  int tianmu_err = 0;
  if (fd_ != -1) {
    tianmu_err = close(fd_);
    name_ = "";
    fd_ = -1;
  }
  return tianmu_err;
}

}  // namespace system
}  // namespace Tianmu
