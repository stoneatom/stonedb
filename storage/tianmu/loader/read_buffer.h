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
#ifndef TIANMU_LOADER_READ_BUFFER_H_
#define TIANMU_LOADER_READ_BUFFER_H_
#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "mm/traceable_object.h"
#include "system/stream.h"

namespace Tianmu {
namespace loader {

class ReadBuffer final : public mm::TraceableObject {
 public:
  ReadBuffer(int no_of_bufs = 3, int size = 32_MB);
  ~ReadBuffer();

  bool BufOpen(std::unique_ptr<system::Stream> &s);
  int BufFetch(int unused_bytes);  // load the next data block into buffer
  int BufSize() const { return buf_used_; }
  char *Buf() const { return buf_; }
  int Read(char *buffer, int bytes_to_read);
  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }

 private:
  int buf_used_;  // number of bytes loaded or reserved so far
  char *buf_;     // current buf in bufs
  int size_;
  int curr_buf_no_;  // buf_ = bufs_ + currBufNo
  char *buf2_;       // buf to be used next
  int curr_buf2_no_;
  int bytes_in_read_thread_buffer_;
  bool buf_incomplete_;

  std::vector<std::unique_ptr<char[]>> bufs_;
  std::unique_ptr<char[]> read_thread_buffer_;
  std::unique_ptr<system::Stream> tianmu_stream_;

  std::mutex read_mutex_;

  std::mutex mtx_;
  std::condition_variable cv_;

  std::thread read_thread_;
  bool stop_reading_thread_;
  THD *thd_;

  void ReadThread();
  void UseNextBuf();
  int FindUnusedBuf();
  bool StartReadingThread();
};

}  // namespace loader
}  // namespace Tianmu

#endif  // TIANMU_LOADER_READ_BUFFER_H_
