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
#include "system/tianmu_file.h"
#include "system/tianmu_system.h"

namespace Tianmu {
namespace system {
static int const D_OVERRUN_GUARDIAN = -66;

LargeBuffer::LargeBuffer(int num, int requested_size) : size_(requested_size), bufs_(num) {
  failed_ = false;

  for (auto &buf : bufs_) {
    buf = std::unique_ptr<char[]>(new char[size_ + 2]);  // additional 2 bytes for safety and terminating '\0';
    buf[size_] = '\0';                                   // maybe unnecessary
    buf[size_ + 1] = D_OVERRUN_GUARDIAN;                 // maybe unnecessary
  }

  curr_buf_num_ = 0;
  buf_ = bufs_[curr_buf_num_].get();
  curr_buf2next_num_ = 1;
  buf2next_ = bufs_[curr_buf2next_num_].get();

  buf_used_ = 0;
}

LargeBuffer::~LargeBuffer() {
  if (flush_thread_.joinable())
    flush_thread_.join();
  if (tianmu_stream_)
    tianmu_stream_->Close();
}

bool LargeBuffer::BufOpen(const IOParameters &iop) {
  try {
    tianmu_stream_ = std::unique_ptr<Stream>(new TianmuFile());
    tianmu_stream_->OpenCreateEmpty(iop.Path());
    if (!tianmu_stream_ || !tianmu_stream_->IsOpen()) {
      BufClose();
      return false;
    }
  } catch (...) {
    return false;
  }

  buf_used_ = 0;
  chmod(iop.Path(), 0666);
  return true;
}

void LargeBuffer::BufFlush() {
  if (buf_used_ > size_)
    TIANMU_LOG(LogCtl_Level::ERROR, "LargeBuffer error: Buffer overrun (Flush)");
  if (flush_thread_.joinable()) {
    flush_thread_.join();
  }
  if (failed_) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Write operation to file or pipe failed_.");
    throw common::FileException("Write operation to file or pipe failed_.");
  }
  flush_thread_ =
      std::thread(std::bind(&LargeBuffer::BufFlushThread, this, tianmu_stream_.get(), buf_, buf_used_, &failed_));
  UseNextBuf();
}

void LargeBuffer::BufFlushThread(Stream *s, char *buf_ptr, int len, bool *failed_) {
  if (s->IsOpen()) {
    int start = 0, end = 0;
    while (end != len) {
      start = end;
      end += WRITE_TO_PIPE_NO_BYTES;
      if (end > len)
        end = len;
      try {
        s->WriteExact(buf_ptr + start, end - start);
      } catch (common::DatabaseException &) {
        *failed_ = true;
        break;
      }
    }
  }
  return;
}

void LargeBuffer::BufClose()  // close the buffer; warning: does not flush data
                              // on disk
{
  if (flush_thread_.joinable())
    flush_thread_.join();

  if (tianmu_stream_)
    tianmu_stream_->Close();

  buf_used_ = 0;
  if (buf_[size_ + 1] != D_OVERRUN_GUARDIAN) {
    TIANMU_LOG(LogCtl_Level::WARN, "buffer overrun detected in LargeBuffer::BufClose.");
    DEBUG_ASSERT(0);
  }
}

void LargeBuffer::FlushAndClose() {
  BufFlush();
  BufClose();
}

char *LargeBuffer::BufAppend(unsigned int len) {
  if ((int)len > size_) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Error: LargeBuffer buffer overrun (BufAppend)");
    return nullptr;
  }
  int buf_pos = buf_used_;
  if (size_ > (int)(buf_used_ + len))
    buf_used_ += len;
  else {
    BufFlush();
    buf_pos = 0;
    buf_used_ = len;
  }
  return this->Buf(buf_pos);  // NOTE: will point to the first undeclared byte
                              // in case of len=0.
  // Fortunately we always assume one additional byte for '\0' to be insertable
  // at the end of system buffer
}

char *LargeBuffer::SeekBack(uint len) {
  DEBUG_ASSERT((uint)buf_used_ >= len);
  buf_used_ -= len;
  return this->Buf(buf_used_);
}

void LargeBuffer::UseNextBuf() {
  int tmp = curr_buf2next_num_;
  curr_buf2next_num_ = FindUnusedBuf();
  buf2next_ = bufs_[curr_buf2next_num_].get();  // to be loaded with data
  curr_buf_num_ = tmp;
  buf_ = bufs_[curr_buf_num_].get();  // to be parsed
}

int LargeBuffer::FindUnusedBuf() {
  std::unique_lock<std::mutex> lk(mutex_);
  while (true) {
    for (size_t i = 0; i < bufs_.size(); i++) {
      if (int(i) != curr_buf2next_num_) {  // not used and not just loaded
        return i;
      }
    }
    cond_var_.wait(lk);
  }
  return -1;
}
}  // namespace system
}  // namespace Tianmu
