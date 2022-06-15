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
#ifndef STONEDB_UTIL_MAPPED_CIRCULAR_BUFFER_H_
#define STONEDB_UTIL_MAPPED_CIRCULAR_BUFFER_H_
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

namespace stonedb {
namespace utils {
// a memory-mapped circular buffer, support single-read, multiple write
class MappedCircularBuffer {
  static inline size_t round_up(size_t n, size_t multiple) { return ((n + multiple - 1) / multiple) * multiple; }

 public:
  enum class TAG : int32_t {
    INVALID = 0,
    INSERT_RECORD = 1,
  };
  static const size_t MAX_BUF_SIZE = 32_MB;
  MappedCircularBuffer(const std::string &file, size_t sz_MB)
      : hdr_size(sysconf(_SC_PAGE_SIZE)), buf_size(round_up(sz_MB * 1_MB, hdr_size)) {
    bool file_exists = false;
    fd = ::open(file.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0) {
      throw std::invalid_argument("failed to open file " + file + ". error " + std::to_string(errno) + ": " +
                                  std::strerror(errno));
    }

    struct stat sb;
    if (::fstat(fd, &sb) == -1) {
      throw std::invalid_argument("failed to stat file " + file + ". error " + std::to_string(errno) + ": " +
                                  std::strerror(errno));
    }

    if (sb.st_size == 0) {
      if (::ftruncate(fd, FileSize()) == -1) {
        throw std::invalid_argument("failed to truncate " + file + ". error " + std::to_string(errno) + ": " +
                                    std::strerror(errno));
      }
      STONEDB_LOG(LogCtl_Level::INFO, "created delayed buffer file %s with size %ldMB", file.c_str(), buf_size / 1_MB);
    } else {
      // file already exists
      if (sb.st_size == static_cast<decltype(sb.st_size)>(FileSize())) {
        file_exists = true;
        STONEDB_LOG(LogCtl_Level::INFO, "use delayed buffer file %s with size %ldMB", file.c_str(), buf_size / 1_MB);
      } else {
        // for now just drop the old file.  TODO: handle resize
        if (::ftruncate(fd, 0) == -1 || ::ftruncate(fd, FileSize()) == -1) {
          throw std::invalid_argument("failed to truncate " + file + ". error " + std::to_string(errno) + ": " +
                                      std::strerror(errno));
        }
        STONEDB_LOG(LogCtl_Level::WARN, "old buffer file (size %ldM) purged!!! ", sb.st_size / 1_MB);
      }
    }
    MapFile(file);

    if (file_exists && hdr->version != current_version) {
      throw std::invalid_argument("file " + file + " size " + std::to_string(sb.st_size) + " is unexpeceted!");
    }
    hdr->version = current_version;
    GetStat();
  }
  MappedCircularBuffer() = delete;
  MappedCircularBuffer(const MappedCircularBuffer &) = delete;
  ~MappedCircularBuffer() { ::close(fd); }

  void Write(TAG tag, const void *data, size_t sz) {
    if (sz > MAX_BUF_SIZE) {
      throw std::invalid_argument("write data buffer size (" + std::to_string(sz) + ") exceeds system defined size");
    }

    size_t total_len = sz + sizeof(TAG) + sizeof(uint32_t) + sizeof(uint64_t);

    std::scoped_lock g(write_mtx);

    uint64_t snap_read_offset = hdr->read_offset % buf_size;

    uint64_t used;
    if (snap_read_offset == hdr->write_offset) {
      used = 0;
    } else if (snap_read_offset > hdr->write_offset) {
      used = hdr->write_offset + buf_size - snap_read_offset;
    } else {
      used = hdr->write_offset - snap_read_offset;
    }

    if (total_len >= buf_size - used) {
      throw std::length_error("buffer not sufficient to store " + std::to_string(sz) + " bytes");
    }

    uchar *ptr = start_addr + hdr->write_offset;
    *(TAG *)ptr = tag;
    ptr += sizeof(TAG);
    *(uint32_t *)ptr = sz;
    ptr += sizeof(uint32_t);
    *(uint64_t *)ptr = 0;  // TODO: timestamp
    ptr += sizeof(uint64_t);
    std::memcpy(ptr, data, sz);
    hdr->write_offset += total_len;
    hdr->write_offset %= buf_size;

    stat.write_cnt++;
    stat.write_bytes += sz;
  }

  std::pair<std::unique_ptr<char[]>, size_t> Read() {
    uint64_t snap_write_offset = hdr->write_offset % buf_size;

    if (hdr->read_offset == snap_write_offset) {
      std::unique_ptr<char[]> nptr;
      return std::make_pair(std::move(nptr), 0);
    }

    uint64_t total;
    if (hdr->read_offset > snap_write_offset) {
      total = snap_write_offset + buf_size - hdr->read_offset;
    } else {
      total = snap_write_offset - hdr->read_offset;
    }

    uchar *ptr = start_addr + hdr->read_offset;
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
    hdr->read_offset %= buf_size;

    stat.read_cnt++;
    stat.read_bytes += sz;

    return std::make_pair(std::move(buf), sz);
  }

  void Sync(bool async = false) { ::msync(hdr, FileSize(), async ? MS_ASYNC : MS_ASYNC); }
  size_t TotalSize() const { return buf_size * 2 + hdr_size; }
  size_t FileSize() const { return buf_size + hdr_size; }
  std::string Status() {
    return "w:" + std::to_string(stat.write_cnt.load()) + "/" + std::to_string(stat.write_bytes.load()) +
           ", r:" + std::to_string(stat.read_cnt.load()) + "/" + std::to_string(stat.read_bytes.load()) +
           " delta: " + std::to_string(stat.write_cnt.load() - stat.read_cnt.load()) + "/" +
           std::to_string(stat.write_bytes.load() - stat.read_bytes.load());
  }
  size_t Usage() { return (stat.write_bytes.load() - stat.read_bytes.load()) * 100 / buf_size; }

 private:
  void MapFile(const std::string &file) {
    file_name = file;
    // reserve the address space
    void *addr = ::mmap(0, TotalSize(), PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (addr == MAP_FAILED) {
      throw std::runtime_error("failed to mmap ANONYMOUS file. error " + std::to_string(errno) + ": " +
                               std::strerror(errno));
    }

    hdr = static_cast<header *>(addr);
    start_addr = static_cast<uchar *>(addr) + hdr_size;

    if (::mmap(hdr, FileSize(), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) == MAP_FAILED) {
      throw std::runtime_error("failed to mmap file " + file + ". error " + std::to_string(errno) + ": " +
                               std::strerror(errno));
    }
    if (::mmap(start_addr + buf_size, buf_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, hdr_size) ==
        MAP_FAILED) {
      throw std::runtime_error("failed to mmap file " + file + ". error " + std::to_string(errno) + ": " +
                               std::strerror(errno));
    }
    STONEDB_LOG(LogCtl_Level::INFO, "insert buffer address %p", start_addr);
  }
  void GetStat() {
    if (hdr->read_offset == hdr->write_offset) {
      return;
    }

    uint64_t total;
    if (hdr->read_offset > hdr->write_offset) {
      total = hdr->write_offset + buf_size - hdr->read_offset;
    } else {
      total = hdr->write_offset - hdr->read_offset;
    }

    uchar *ptr = start_addr + hdr->read_offset;

    size_t total_data = 0;
    unsigned long cnt = 0;
    while (total != 0) {
      ptr += sizeof(TAG);
      size_t sz = *(uint32_t *)ptr;
      total_data += sz;
      ptr += sizeof(uint32_t);
      ptr += sizeof(uint64_t);  // skip timestamp
      if (sz > MAX_BUF_SIZE) {
        ::remove(file_name.c_str());
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
  const static uint64_t current_version = 0x31765348;  // "STONEDBv1"
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
  uchar *start_addr;
  std::mutex write_mtx;
  int fd;
  std::string file_name;
  size_t hdr_size;
  const size_t buf_size;  // should be a multiple of page size
};
}  // namespace utils
}  // namespace stonedb

#endif  // STONEDB_UTIL_MAPPED_CIRCULAR_BUFFER_H_
