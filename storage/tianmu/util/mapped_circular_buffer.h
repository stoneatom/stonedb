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
#ifndef TIANMU_UTIL_MAPPED_CIRCULAR_BUFFER_H_
#define TIANMU_UTIL_MAPPED_CIRCULAR_BUFFER_H_
#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <exception>
#include <mutex>

#include "util/log_ctl.h"

namespace Tianmu {
namespace utils {

// a memory-mapped circular buffer, support single-read, multiple write
class MappedCircularBuffer {
  static inline size_t round_up(size_t n, size_t multiple) { return ((n + multiple - 1) / multiple) * multiple; }

 public:
  MappedCircularBuffer() = delete;
  MappedCircularBuffer(const MappedCircularBuffer &) = delete;
  ~MappedCircularBuffer() { ::close(fd_); }

  enum class TAG : int32_t {
    INVALID = 0,
    INSERT_RECORD = 1,
  };

  static constexpr size_t MAX_BUF_SIZE = 32_MB;

  MappedCircularBuffer(const std::string &file, size_t sz_MB)
      : hdr_size_(sysconf(_SC_PAGE_SIZE)), buf_size_(round_up(sz_MB * 1_MB, hdr_size_)) {
    bool file_exists = false;
    fd_ = ::open(file.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd_ < 0) {
      throw std::invalid_argument("failed to open file " + file + ". error " + std::to_string(errno) + ": " +
                                  std::strerror(errno));
    }

    struct stat sb;
    if (::fstat(fd_, &sb) == -1) {
      throw std::invalid_argument("failed to stat file " + file + ". error " + std::to_string(errno) + ": " +
                                  std::strerror(errno));
    }

    if (sb.st_size == 0) {
      if (::ftruncate(fd_, FileSize()) == -1) {
        throw std::invalid_argument("failed to truncate " + file + ". error " + std::to_string(errno) + ": " +
                                    std::strerror(errno));
      }
      TIANMU_LOG(LogCtl_Level::INFO, "created delayed buffer file %s with size %ldMB", file.c_str(), buf_size_ / 1_MB);
    } else {
      // file already exists
      if (sb.st_size == static_cast<decltype(sb.st_size)>(FileSize())) {
        file_exists = true;
        TIANMU_LOG(LogCtl_Level::INFO, "use delayed buffer file %s with size %ldMB", file.c_str(), buf_size_ / 1_MB);
      } else {
        // for now just drop the old file.
        if (::ftruncate(fd_, 0) == -1 || ::ftruncate(fd_, FileSize()) == -1) {
          throw std::invalid_argument("failed to truncate " + file + ". error " + std::to_string(errno) + ": " +
                                      std::strerror(errno));
        }
        TIANMU_LOG(LogCtl_Level::WARN, "old buffer file (size %ldM) purged!!! ", sb.st_size / 1_MB);
      }
    }
    MapFile(file);

    if (file_exists && hdr->version != current_version) {
      throw std::invalid_argument("file " + file + " size " + std::to_string(sb.st_size) + " is unexpeceted!");
    }
    hdr->version = current_version;
    GetStat();
  }

  void Write(TAG tag, const void *data, size_t sz) {
    if (sz > MAX_BUF_SIZE) {
      throw std::invalid_argument("write data buffer size (" + std::to_string(sz) + ") exceeds system defined size");
    }

    size_t total_len = sz + sizeof(TAG) + sizeof(uint32_t) + sizeof(uint64_t);

    std::scoped_lock g(write_mutex_);

    uint64_t snap_read_offset = hdr->read_offset % buf_size_;

    uint64_t used;
    if (snap_read_offset == hdr->write_offset) {
      used = 0;
    } else if (snap_read_offset > hdr->write_offset) {
      used = hdr->write_offset + buf_size_ - snap_read_offset;
    } else {
      used = hdr->write_offset - snap_read_offset;
    }

    if (total_len >= buf_size_ - used) {
      throw std::length_error("buffer not sufficient to store " + std::to_string(sz) + " bytes");
    }

    uchar *ptr = start_addr_ + hdr->write_offset;
    *(TAG *)ptr = tag;
    ptr += sizeof(TAG);
    *(uint32_t *)ptr = sz;
    ptr += sizeof(uint32_t);
    *(uint64_t *)ptr = 0;
    ptr += sizeof(uint64_t);
    std::memcpy(ptr, data, sz);
    hdr->write_offset += total_len;
    hdr->write_offset %= buf_size_;

    stat.write_cnt++;
    stat.write_bytes += sz;
  }

  std::pair<std::unique_ptr<char[]>, size_t> Read() {
    uint64_t snap_write_offset = hdr->write_offset % buf_size_;

    if (hdr->read_offset == snap_write_offset) {
      std::unique_ptr<char[]> nptr;
      return std::make_pair(std::move(nptr), 0);
    }

    uint64_t total;
    if (hdr->read_offset > snap_write_offset) {
      total = snap_write_offset + buf_size_ - hdr->read_offset;
    } else {
      total = snap_write_offset - hdr->read_offset;
    }

    uchar *ptr = start_addr_ + hdr->read_offset;
    // TAG    tag = *(TAG *)ptr;
    ptr += sizeof(TAG);
    size_t sz = *(uint32_t *)ptr;
    ptr += sizeof(uint32_t);
    ptr += sizeof(uint64_t);  // skip timestamp
    if (sz > MAX_BUF_SIZE) {
      throw std::length_error("read buffer size (" + std::to_string(sz) + ") exceeds system defined size");
    }
    std::unique_ptr<char[]> buf(new char[sz]);
    std::memcpy(buf.get(), ptr, sz);
    uint64_t consumed = sz + sizeof(TAG) + sizeof(uint32_t) + sizeof(uint64_t);
    ASSERT(consumed <= total, "delayed insert buffer corrupted!!");
    hdr->read_offset += consumed;
    hdr->read_offset %= buf_size_;

    stat.read_cnt++;
    stat.read_bytes += sz;

    return std::make_pair(std::move(buf), sz);
  }

  void Sync(bool async = false) { ::msync(hdr, FileSize(), async ? MS_ASYNC : MS_ASYNC); }
  size_t TotalSize() const { return buf_size_ * 2 + hdr_size_; }
  size_t FileSize() const { return buf_size_ + hdr_size_; }

  std::string Status() {
    return "w:" + std::to_string(stat.write_cnt.load()) + "/" + std::to_string(stat.write_bytes.load()) +
           ", r:" + std::to_string(stat.read_cnt.load()) + "/" + std::to_string(stat.read_bytes.load()) +
           " delta: " + std::to_string(stat.write_cnt.load() - stat.read_cnt.load()) + "/" +
           std::to_string(stat.write_bytes.load() - stat.read_bytes.load());
  }

  size_t Usage() { return (stat.write_bytes.load() - stat.read_bytes.load()) * 100 / buf_size_; }

 private:
  void MapFile(const std::string &file) {
    file_name_ = file;
    // reserve the address space
    void *addr = ::mmap(0, TotalSize(), PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (addr == MAP_FAILED) {
      throw std::runtime_error("failed to mmap ANONYMOUS file. error " + std::to_string(errno) + ": " +
                               std::strerror(errno));
    }

    hdr = static_cast<header *>(addr);
    start_addr_ = static_cast<uchar *>(addr) + hdr_size_;

    if (::mmap(hdr, FileSize(), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd_, 0) == MAP_FAILED) {
      throw std::runtime_error("failed to mmap file " + file + ". error " + std::to_string(errno) + ": " +
                               std::strerror(errno));
    }
    if (::mmap(start_addr_ + buf_size_, buf_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd_, hdr_size_) ==
        MAP_FAILED) {
      throw std::runtime_error("failed to mmap file " + file + ". error " + std::to_string(errno) + ": " +
                               std::strerror(errno));
    }
    TIANMU_LOG(LogCtl_Level::INFO, "insert buffer address %p", start_addr_);
  }

  void GetStat() {
    if (hdr->read_offset == hdr->write_offset) {
      return;
    }

    uint64_t total;
    if (hdr->read_offset > hdr->write_offset) {
      total = hdr->write_offset + buf_size_ - hdr->read_offset;
    } else {
      total = hdr->write_offset - hdr->read_offset;
    }

    uchar *ptr = start_addr_ + hdr->read_offset;

    size_t total_data = 0;
    unsigned long cnt = 0;
    while (total != 0) {
      ptr += sizeof(TAG);
      size_t sz = *(uint32_t *)ptr;
      total_data += sz;
      ptr += sizeof(uint32_t);
      ptr += sizeof(uint64_t);  // skip timestamp
      if (sz > MAX_BUF_SIZE) {
        ::remove(file_name_.c_str());
        throw std::length_error("read buffer size (" + std::to_string(sz) +
                                ") exceeds system defined size. File removed!");
      }
      uint64_t consumed = sz + sizeof(TAG) + sizeof(uint32_t) + sizeof(uint64_t);
      ASSERT(consumed <= total, "delayed insert buffer corrupted!!");
      total -= consumed;
      ptr += sz;
      cnt++;
    }
    stat.write_bytes = total_data;
    stat.write_cnt = cnt;
  }

  const static uint64_t current_version = 0x31765348;  // "TIANMUv1"

  struct header {
    uint64_t version;
    volatile uint64_t write_offset;
    volatile uint64_t read_offset;
  } * hdr;

  struct Stat {
    std::atomic_ulong write_cnt{0};
    std::atomic_ulong write_bytes{0};
    std::atomic_ulong read_cnt{0};
    std::atomic_ulong read_bytes{0};
  } stat;

 private:
  uchar *start_addr_;
  std::mutex write_mutex_;
  int fd_;
  std::string file_name_;
  size_t hdr_size_;
  const size_t buf_size_;  // should be a multiple of page size
};

}  // namespace utils
}  // namespace Tianmu

#endif  // TIANMU_UTIL_MAPPED_CIRCULAR_BUFFER_H_
