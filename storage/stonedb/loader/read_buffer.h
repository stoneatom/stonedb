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
#ifndef STONEDB_LOADER_READ_BUFFER_H_
#define STONEDB_LOADER_READ_BUFFER_H_
#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "mm/traceable_object.h"
#include "system/stream.h"

namespace stonedb {
namespace loader {

class ReadBuffer final : public mm::TraceableObject {
 public:
  ReadBuffer(int no_of_bufs = 3, int size = 32_MB);
  ~ReadBuffer();

  bool BufOpen(std::unique_ptr<system::Stream> &s);
  int BufFetch(int unused_bytes);  // load the next data block into buffer
  int BufSize() const { return buf_used; }
  char *Buf() const { return buf; }
  int Read(char *buffer, int bytes_to_read);
  mm::TO_TYPE TraceableType() const override { return mm::TO_TEMPORARY; }

 private:
  int buf_used;  // number of bytes loaded or reserved so far
  char *buf;     // current buf in bufs
  int size;
  int curr_buf_no;  // buf = bufs + currBufNo
  char *buf2;       // buf to be used next
  int curr_buf2_no;
  int bytes_in_read_thread_buffer;
  bool buf_incomplete;

  std::vector<std::unique_ptr<char[]>> bufs;
  std::unique_ptr<char[]> read_thread_buffer;

  std::unique_ptr<system::Stream> ib_stream;

  std::mutex read_mutex;

  std::mutex mtx;
  std::condition_variable cv;

  std::thread read_thread;
  bool stop_reading_thread;
  THD *thd;

  void ReadThread();
  void UseNextBuf();
  int FindUnusedBuf();
  bool StartReadingThread();
};

}  // namespace loader
}  // namespace stonedb

#endif  // STONEDB_LOADER_READ_BUFFER_H_
