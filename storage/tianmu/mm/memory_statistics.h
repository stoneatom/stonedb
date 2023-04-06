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
#ifndef TIANMU_MEMORY_STATICS_H_
#define TIANMU_MEMORY_STATICS_H_

#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/assert.h"
#include "util/log_ctl.h"

namespace Tianmu {

static const char *filename_self_statm = "/proc/self/statm";
static const char *filename_meminfo = "/proc/meminfo";

class MemoryStatisticsOS {
 public:
  // In number of bytes.
  struct DataSelfStatm {
    uint64_t virt;
    uint64_t resident;
    uint64_t shared;
    uint64_t code;
    uint64_t data_and_stack;
  };

  struct DataMemInfo {
    uint64_t mem_total;
    uint64_t mem_free;
    uint64_t mem_available;
    uint64_t buffers;
    uint64_t cached;
    uint64_t swap_cached;
    uint64_t swap_total;
    uint64_t swap_free;
    uint64_t swap_used;
    uint64_t free_total;
  };

  // Thread-safe.
  DataSelfStatm GetSelfStatm() const {
    auto SplitString = [](const std::string &str, const char pattern, std::vector<std::string> &res) {
      std::stringstream input(str);
      std::string temp;
      while (getline(input, temp, pattern)) {
        res.push_back(temp);
      }
    };

    DataSelfStatm data;

    constexpr size_t buf_size = 1024;
    char buf[buf_size] = {0};

    ssize_t res = 0;

    do {
      res = ::pread(fd_self_statm, buf, buf_size, 0);

      if (-1 == res) {
        if (errno == EINTR)
          continue;

        TIANMU_LOG(LogCtl_Level::ERROR, "MemoryStatisticsOS pread fail, errno: %d strerrno: %s", errno,
                   strerror(errno));
        return data;
      }

      if (res < 0) {
        TIANMU_LOG(LogCtl_Level::ERROR, "MemoryStatisticsOS pread fail, errno: %d strerrno: %s", errno,
                   strerror(errno));
        return data;
      }

      break;
    } while (true);

    std::vector<std::string> vec_str;
    SplitString(buf, ' ', vec_str);
    ASSERT(vec_str.size() == 7);

    data.virt = std::atoll(vec_str[0].c_str());
    data.resident = std::atoll(vec_str[1].c_str());
    data.shared = std::atoll(vec_str[2].c_str());
    data.code = std::atoll(vec_str[3].c_str());
    data.data_and_stack = std::atoll(vec_str[5].c_str());

    return data;
  }

  // Thread-safe.
  DataMemInfo GetMemInfo() {
    DataMemInfo data;

    std::ifstream in(filename_meminfo, std::ios_base::in);

    constexpr size_t line_num = 16;
    std::vector<std::string> vec_buf;
    vec_buf.reserve(line_num);
    char name[20] = {0};

    for (size_t i = 0; i < line_num; ++i) {
      std::string line;
      if (!std::getline(in, line)) {
        TIANMU_LOG(LogCtl_Level::ERROR, "GetMemInfo fgets fail, line: %d errno: %d strerrno: %s", i, errno,
                   strerror(errno));
        return data;
      }

      vec_buf.emplace_back(line);
    }

    sscanf(vec_buf[0].c_str(), "%s%lu", name, &data.mem_total);
    sscanf(vec_buf[1].c_str(), "%s%lu", name, &data.mem_free);
    sscanf(vec_buf[2].c_str(), "%s%lu", name, &data.mem_available);
    sscanf(vec_buf[3].c_str(), "%s%lu", name, &data.buffers);
    sscanf(vec_buf[4].c_str(), "%s%lu", name, &data.cached);
    sscanf(vec_buf[5].c_str(), "%s%lu", name, &data.swap_cached);

    sscanf(vec_buf[14].c_str(), "%s%lu", name, &data.swap_total);
    sscanf(vec_buf[15].c_str(), "%s%lu", name, &data.swap_free);

    data.free_total = data.mem_free + data.buffers + data.cached;
    data.swap_used = data.swap_total - data.swap_free;

    return data;
  }

 public:
  static MemoryStatisticsOS *Instance() {
    if (!instance) {
      instance = new MemoryStatisticsOS();
    }

    return instance;
  }

 private:
  MemoryStatisticsOS() {
    auto open_file = ([&](const char *filename, int &fd) {
      fd = ::open(filename, O_RDONLY | O_CLOEXEC);
      if (-1 == fd_self_statm) {
        TIANMU_LOG(LogCtl_Level::ERROR, "MemoryStatisticsOS open fail, errno: %d strerrno: %s", errno, strerror(errno));
        ASSERT(0, "MemoryStatisticsOS open fail " + std::string(filename));
      }
    });

    open_file(filename_self_statm, fd_self_statm);
  }

  ~MemoryStatisticsOS() {
    auto close_file = ([&](int fd) {
      if (0 != ::close(fd)) {
        TIANMU_LOG(LogCtl_Level::ERROR, "MemoryStatisticsOS close fail, errno: %d strerrno: %s", errno,
                   strerror(errno));
      }
    });

    close_file(fd_self_statm);
    fd_self_statm = -1;
  }

 private:
  int fd_self_statm = -1;
  static MemoryStatisticsOS *instance;
};

void memory_statistics_record(const char *model, const char *deal);

}  // namespace Tianmu

#endif  // TIANMU_MEMORY_STATICS_H_