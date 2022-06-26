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

#include "large_buffer.h"

#include <algorithm>

#include "common/assert.h"
#include "core/tools.h"
#include "system/io_parameters.h"
#include "system/rc_system.h"
#include "system/stonedb_file.h"

namespace stonedb {
namespace system {
static int const D_OVERRUN_GUARDIAN = -66;

LargeBuffer::LargeBuffer(int num, int requested_size) : size(requested_size), bufs(num) {
  failed = false;

  for (auto &buf : bufs) {
    buf = std::unique_ptr<char[]>(new char[size + 2]);  // additional 2 bytes for safety and terminating '\0';
    buf[size] = '\0';                                   // maybe unnecessary
    buf[size + 1] = D_OVERRUN_GUARDIAN;                 // maybe unnecessary
  }

  curr_buf_no = 0;
  buf = bufs[curr_buf_no].get();
  curr_buf2_no = 1;
  buf2 = bufs[curr_buf2_no].get();

  buf_used = 0;
}

LargeBuffer::~LargeBuffer() {
  if (flush_thread.joinable()) flush_thread.join();
  if (ib_stream) ib_stream->Close();
}

bool LargeBuffer::BufOpen(const IOParameters &iop) {
  try {
    ib_stream = std::unique_ptr<Stream>(new StoneDBFile());
    ib_stream->OpenCreateEmpty(iop.Path());
    if (!ib_stream || !ib_stream->IsOpen()) {
      BufClose();
      return false;
    }
  } catch (...) {
    return false;
  }

  buf_used = 0;
  chmod(iop.Path(), 0666);
  return true;
}

void LargeBuffer::BufFlush() {
  if (buf_used > size) STONEDB_LOG(LogCtl_Level::ERROR, "LargeBuffer error: Buffer overrun (Flush)");
  if (flush_thread.joinable()) {
    flush_thread.join();
  }
  if (failed) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Write operation to file or pipe failed.");
    throw common::FileException("Write operation to file or pipe failed.");
  }
  flush_thread = std::thread(std::bind(&LargeBuffer::BufFlushThread, this, ib_stream.get(), buf, buf_used, &failed));
  UseNextBuf();
}

void LargeBuffer::BufFlushThread(Stream *s, char *buf_ptr, int len, bool *failed) {
  if (s->IsOpen()) {
    int start = 0, end = 0;
    while (end != len) {
      start = end;
      end += WRITE_TO_PIPE_NO_BYTES;
      if (end > len) end = len;
      try {
        s->WriteExact(buf_ptr + start, end - start);
      } catch (common::DatabaseException &) {
        *failed = true;
        break;
      }
    }
  }
  return;
}

void LargeBuffer::BufClose()  // close the buffer; warning: does not flush data
                              // on disk
{
  if (flush_thread.joinable()) flush_thread.join();

  if (ib_stream) ib_stream->Close();

  buf_used = 0;
  if (buf[size + 1] != D_OVERRUN_GUARDIAN) {
    STONEDB_LOG(LogCtl_Level::WARN, "buffer overrun detected in LargeBuffer::BufClose.");
    DEBUG_ASSERT(0);
  }
}

void LargeBuffer::FlushAndClose() {
  BufFlush();
  BufClose();
}

char *LargeBuffer::BufAppend(unsigned int len) {
  if ((int)len > size) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Error: LargeBuffer buffer overrun (BufAppend)");
    return NULL;
  }
  int buf_pos = buf_used;
  if (size > (int)(buf_used + len))
    buf_used += len;
  else {
    BufFlush();
    buf_pos = 0;
    buf_used = len;
  }
  return this->Buf(buf_pos);  // NOTE: will point to the first undeclared byte
                              // in case of len=0.
  // Fortunately we always assume one additional byte for '\0' to be insertable
  // at the end of system buffer
}

char *LargeBuffer::SeekBack(uint len) {
  DEBUG_ASSERT((uint)buf_used >= len);
  buf_used -= len;
  return this->Buf(buf_used);
}

void LargeBuffer::UseNextBuf() {
  int tmp = curr_buf2_no;
  curr_buf2_no = FindUnusedBuf();
  buf2 = bufs[curr_buf2_no].get();  // to be loaded with data
  curr_buf_no = tmp;
  buf = bufs[curr_buf_no].get();  // to be parsed
}

int LargeBuffer::FindUnusedBuf() {
  std::unique_lock<std::mutex> lk(mtx);
  while (true) {
    for (size_t i = 0; i < bufs.size(); i++) {
      if (int(i) != curr_buf2_no) {  // not used and not just loaded
        return i;
      }
    }
    cv.wait(lk);
  }
  return -1;
}
}  // namespace system
}  // namespace stonedb
