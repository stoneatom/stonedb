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

#include "loader/read_buffer.h"

#include "core/transaction.h"

namespace stonedb {
namespace loader {

ReadBuffer::ReadBuffer(int num, int requested_size) : size_(requested_size), bufs_(num) {
  thd_ = current_tx->Thd();

  for (auto &b : bufs_) {
    b = std::unique_ptr<char[]>(new char[size_]);
    memset(b.get(), 0x0, size_);
  }

  curr_buf_no_ = 0;
  buf_ = bufs_[curr_buf_no_].get();
  curr_buf2_no_ = 1;
  buf2_ = bufs_[curr_buf2_no_].get();

  buf_used_ = 0;
  buf_incomplete_ = false;
  bytes_in_read_thread_buffer_ = 0;
  stop_reading_thread_ = true;
}

ReadBuffer::~ReadBuffer() {
  if (read_thread_.joinable()) {
    read_mutex_.lock();
    stop_reading_thread_ = true;
    read_mutex_.unlock();
    read_thread_.join();
  }

  if (ib_stream_) ib_stream_->Close();
}

bool ReadBuffer::BufOpen(std::unique_ptr<system::Stream> &s) {
  ib_stream_ = std::move(s);

  auto r = Read(buf_ + buf_used_, size_);
  if (r == -1) {
    ib_stream_->Close();
    buf_incomplete_ = false;
    return false;
  }

  if (r == size_) {
    if (!StartReadingThread()) {
      return false;
    }
    buf_incomplete_ = true;
  } else {
    ib_stream_->Close();
    buf_incomplete_ = false;
  }

  buf_used_ += r;
  return true;
}

bool ReadBuffer::StartReadingThread() {
  if (!read_thread_buffer_) {
    read_thread_buffer_ = std::unique_ptr<char[]>(new char[size_]);
    if (!read_thread_buffer_) {
      STONEDB_LOG(LogCtl_Level::ERROR, "out of memory (%d bytes failed). (42)", size_);
      return false;
    }
  }

  bytes_in_read_thread_buffer_ = 0;

  stop_reading_thread_ = false;
  read_thread_ = std::thread(std::bind(&ReadBuffer::ReadThread, this));

  return true;
}

int ReadBuffer::BufFetch(int unused_bytes) {
  int to_read = size_;
  {
    std::scoped_lock guard(read_mutex_);

    if (bytes_in_read_thread_buffer_ == -1) {
      ib_stream_->Close();
      buf_incomplete_ = false;
      throw common::FileException("Unable to read from the input file.");
    }

    if ((!ib_stream_->IsOpen() && bytes_in_read_thread_buffer_ == 0) || to_read == unused_bytes) return 0;

    for (int i = 0; i < unused_bytes; i++) buf2_[i] = *(buf_ + ((buf_used_ - unused_bytes) + i));
    to_read -= unused_bytes;
    int to_read_from_th_buf = std::min(to_read, bytes_in_read_thread_buffer_);

    std::memcpy(buf2_ + unused_bytes, read_thread_buffer_.get(), to_read_from_th_buf);

    for (int i = 0; i < bytes_in_read_thread_buffer_ - to_read_from_th_buf; i++)
      read_thread_buffer_[i] = read_thread_buffer_[i + to_read_from_th_buf];

    bytes_in_read_thread_buffer_ -= to_read_from_th_buf;
    unused_bytes += to_read_from_th_buf;
    to_read -= to_read_from_th_buf;

    buf_used_ = unused_bytes;
    if (ib_stream_->IsOpen()) {
      ushort no_steps = 0;
      int r = 0;
      while (no_steps < 15 && to_read > 0) {
        try {
          r = ib_stream_->Read(buf2_ + unused_bytes, to_read);
        } catch (common::DatabaseException &) {
          r = -1;
        }
        if (r != -1) {
          buf_used_ += r;
          if (r == 0) {
            ib_stream_->Close();
            buf_incomplete_ = false;
            break;
          } else {
            buf_incomplete_ = true;
          }
          if (r != to_read) {
            unused_bytes += r;
            to_read -= r;
          } else
            break;
        } else {
          no_steps++;
          STONEDB_LOG(LogCtl_Level::WARN, "Reading from the input file error: %s", std::strerror(errno));
          std::this_thread::sleep_for(std::chrono::milliseconds(500 + no_steps * 500));
        }
      }
      if (r == -1) {
        ib_stream_->Close();
        buf_incomplete_ = false;
        throw common::FileException("Unable to read from the input file.");
      }
    }
  }
  UseNextBuf();
  return buf_used_;
}

int ReadBuffer::Read(char *buffer, int bytes_to_read) {
  int requested_size = bytes_to_read;
  int no_attempts = 0;
  int bytes_read = 0;
  int read_b = -1;
  bytes_read = 0;
  while (no_attempts < 16 && bytes_read != requested_size) {
    no_attempts++;
    bool do_stop = false;
    while (!do_stop) {
      try {
        read_b = ib_stream_->Read(buffer + bytes_read, std::min(bytes_to_read, requested_size - bytes_read));
      } catch (common::DatabaseException &) {
        read_b = -1;
      }
      if (read_b == -1)
        do_stop = true;
      else if (read_b == 0)
        return bytes_read;
      else
        bytes_read += read_b;
    }
    bytes_to_read /= 2;
  }
  return read_b != -1 ? bytes_read : read_b;
}

void ReadBuffer::ReadThread() {
  common::SetMySQLTHD(thd_);

  int to_read = 0;
  int to_read_in_one_loop = size_ / 2;
  bool do_sleep = false;
  bool do_stop = false;

  int no_read_bytes = -1;

  while (!do_stop) {
    no_read_bytes = 0;
    {
      std::scoped_lock guard(read_mutex_);
      if (!buf_incomplete_ || !ib_stream_->IsOpen() || stop_reading_thread_) {
        break;
      }

      if (bytes_in_read_thread_buffer_ != size_) {
        to_read = to_read_in_one_loop;
        if (size_ - bytes_in_read_thread_buffer_ < to_read_in_one_loop) to_read = size_ - bytes_in_read_thread_buffer_;
        ushort no_steps = 0;
        while (no_steps < 15 && to_read > 0) {
          try {
            no_read_bytes = ib_stream_->Read(read_thread_buffer_.get() + bytes_in_read_thread_buffer_, to_read);
          } catch (common::DatabaseException &) {
            no_read_bytes = -1;
          }

          if (no_read_bytes == -1) {
            no_steps++;
            STONEDB_LOG(LogCtl_Level::WARN, "Reading from the input file error: %s", std::strerror(errno));
            std::this_thread::sleep_for(std::chrono::milliseconds(500 + no_steps * 500));
            do_stop = true;
          } else {
            do_stop = false;
            bytes_in_read_thread_buffer_ += no_read_bytes;
            if (no_read_bytes != 0)
              buf_incomplete_ = true;
            else {
              ib_stream_->Close();
              buf_incomplete_ = false;
              do_stop = true;
            }
            break;
          }
        }  // while

        if (no_read_bytes == -1) {
          bytes_in_read_thread_buffer_ = -1;
          do_stop = true;
        }
      }  // if

      (bytes_in_read_thread_buffer_ == size_) ? do_sleep = true : do_sleep = false;
    }

    if (do_sleep || no_read_bytes != -1) std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }  // while (!do_stop)

  return;
}

void ReadBuffer::UseNextBuf() {
  int tmp = curr_buf2_no_;
  curr_buf2_no_ = FindUnusedBuf();
  buf2_ = bufs_[curr_buf2_no_].get();  // to be loaded with data
  curr_buf_no_ = tmp;
  buf_ = bufs_[curr_buf_no_].get();  // to be parsed
}

int ReadBuffer::FindUnusedBuf() {
  std::unique_lock<std::mutex> lk(mtx_);
  while (true) {
    for (size_t i = 0; i < bufs_.size(); i++) {
      if (int(i) != curr_buf2_no_) {  // not used and not just loaded
        return i;
      }
    }
    cv_.wait(lk);
  }
  return -1;
}

}  // namespace loader
}  // namespace stonedb
