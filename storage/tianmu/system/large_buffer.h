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
#ifndef TIANMU_SYSTEM_LARGE_BUFFER_H_
#define TIANMU_SYSTEM_LARGE_BUFFER_H_
#pragma once

#include <condition_variable>
#include <memory>
#include <thread>

#include "common/common_definitions.h"
#include "mm/traceable_object.h"

namespace Tianmu {
namespace system {

class IOParameters;
class Stream;

#define WRITE_TO_PIPE_NO_BYTES PIPE_BUF

class LargeBuffer final : public mm::TraceableObject {
 public:
  LargeBuffer(int no_of_bufs = 3, int size = 8_MB);
  virtual ~LargeBuffer();

  bool BufOpen(const IOParameters &iop);
  void FlushAndClose();
  char *BufAppend(unsigned int len);
  char *SeekBack(uint len);

  // direct access to the buffer (declared part only):
  char *Buf(int n) {
    // Note: we allow n=buf_used, although it is out of declared limits.
    // Fortunately "buf" is one character longer.
    DEBUG_ASSERT(n >= 0 && n <= buf_used_);
    return buf_ + n;
  }

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }
  int WriteIfNonzero(uchar c) {
    if (c != 0) {
      char *ptr = BufAppend(1);
      if (ptr) {
        *ptr = c;
        return 1;
      }
    }
    return -1;
  }

 private:
  void BufFlush();  // save the data to the file (in writing mode)
  void BufClose();  // close the buffer; warning: does not flush data in writing mode
  void UseNextBuf();
  int FindUnusedBuf();
  void BufFlushThread(Stream *file, char *buf_ptr, int len, bool *failed);

 private:
  int buf_used_;  // number of bytes loaded or reserved so far
  char *buf_;     // current buf in bufs
  int size_;
  std::vector<std::unique_ptr<char[]>> bufs_;
  int curr_buf_num_;  // buf = bufs + currBufNo
  char *buf2next_;    // buf to be used next
  int curr_buf2next_num_;

  std::unique_ptr<Stream> tianmu_stream_;

  std::mutex mutex_;
  std::condition_variable cond_var_;
  std::thread flush_thread_;
  bool failed_;
};
}  // namespace system
}  // namespace Tianmu
#endif  // TIANMU_SYSTEM_LARGE_BUFFER_H_
