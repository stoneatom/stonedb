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

ReadBuffer::ReadBuffer(int num, int requested_size) : size(requested_size), bufs(num) {
  thd = current_tx->Thd();

  for (auto &b : bufs) {
    b = std::unique_ptr<char[]>(new char[size]);
  }

  curr_buf_no = 0;
  buf = bufs[curr_buf_no].get();
  curr_buf2_no = 1;
  buf2 = bufs[curr_buf2_no].get();

  buf_used = 0;
  buf_incomplete = false;
  bytes_in_read_thread_buffer = 0;
  stop_reading_thread = true;
}

ReadBuffer::~ReadBuffer() {
  if (read_thread.joinable()) {
    read_mutex.lock();
    stop_reading_thread = true;
    read_mutex.unlock();
    read_thread.join();
  }

  if (ib_stream) ib_stream->Close();
}

bool ReadBuffer::BufOpen(std::unique_ptr<system::Stream> &s) {
  ib_stream = std::move(s);

  auto r = Read(buf + buf_used, size);
  if (r == -1) {
    ib_stream->Close();
    buf_incomplete = false;
    return false;
  }
  if (r == size) {
    if (!StartReadingThread()) {
      return false;
    }
    buf_incomplete = true;
  } else {
    ib_stream->Close();
    buf_incomplete = false;
  }
  buf_used += r;
  return true;
}

bool ReadBuffer::StartReadingThread() {
  if (!read_thread_buffer) {
    read_thread_buffer = std::unique_ptr<char[]>(new char[size]);
    if (!read_thread_buffer) {
      STONEDB_LOG(ERROR, "out of memory (%d bytes failed). (42)", size);
      return false;
    }
  }
  bytes_in_read_thread_buffer = 0;

  stop_reading_thread = false;
  read_thread = std::thread(std::bind(&ReadBuffer::ReadThread, this));

  return true;
}

int ReadBuffer::BufFetch(int unused_bytes) {
  int to_read = size;
  {
    std::scoped_lock guard(read_mutex);

    if (bytes_in_read_thread_buffer == -1) {
      ib_stream->Close();
      buf_incomplete = false;
      throw common::FileException("Unable to read from the input file.");
    }

    if ((!ib_stream->IsOpen() && bytes_in_read_thread_buffer == 0) || to_read == unused_bytes) return 0;

    for (int i = 0; i < unused_bytes; i++) buf2[i] = *(buf + ((buf_used - unused_bytes) + i));
    to_read -= unused_bytes;
    int to_read_from_th_buf = std::min(to_read, bytes_in_read_thread_buffer);

    std::memcpy(buf2 + unused_bytes, read_thread_buffer.get(), to_read_from_th_buf);

    for (int i = 0; i < bytes_in_read_thread_buffer - to_read_from_th_buf; i++)
      read_thread_buffer[i] = read_thread_buffer[i + to_read_from_th_buf];

    bytes_in_read_thread_buffer -= to_read_from_th_buf;
    unused_bytes += to_read_from_th_buf;
    to_read -= to_read_from_th_buf;

    buf_used = unused_bytes;
    if (ib_stream->IsOpen()) {
      ushort no_steps = 0;
      int r = 0;
      while (no_steps < 15 && to_read > 0) {
        try {
          r = ib_stream->Read(buf2 + unused_bytes, to_read);
        } catch (common::DatabaseException &) {
          r = -1;
        }
        if (r != -1) {
          buf_used += r;
          if (r == 0) {
            ib_stream->Close();
            buf_incomplete = false;
            break;
          } else {
            buf_incomplete = true;
          }
          if (r != to_read) {
            unused_bytes += r;
            to_read -= r;
          } else
            break;
        } else {
          no_steps++;
          STONEDB_LOG(WARN, "Reading from the input file error: %s", std::strerror(errno));
          std::this_thread::sleep_for(std::chrono::milliseconds(500 + no_steps * 500));
        }
      }
      if (r == -1) {
        ib_stream->Close();
        buf_incomplete = false;
        throw common::FileException("Unable to read from the input file.");
      }
    }
  }
  UseNextBuf();
  return buf_used;
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
        read_b = ib_stream->Read(buffer + bytes_read, std::min(bytes_to_read, requested_size - bytes_read));
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
  common::SetMySQLTHD(thd);

  int to_read = 0;
  int to_read_in_one_loop = size / 2;
  bool do_sleep = false;
  bool do_stop = false;

  int no_read_bytes = -1;

  while (!do_stop) {
    no_read_bytes = 0;
    {
      std::scoped_lock guard(read_mutex);
      if (!buf_incomplete || !ib_stream->IsOpen() || stop_reading_thread) {
        break;
      }

      if (bytes_in_read_thread_buffer != size) {
        to_read = to_read_in_one_loop;
        if (size - bytes_in_read_thread_buffer < to_read_in_one_loop) to_read = size - bytes_in_read_thread_buffer;
        ushort no_steps = 0;
        while (no_steps < 15 && to_read > 0) {
          try {
            no_read_bytes = ib_stream->Read(read_thread_buffer.get() + bytes_in_read_thread_buffer, to_read);
          } catch (common::DatabaseException &) {
            no_read_bytes = -1;
          }
          if (no_read_bytes == -1) {
            no_steps++;
            STONEDB_LOG(WARN, "Reading from the input file error: %s", std::strerror(errno));
            std::this_thread::sleep_for(std::chrono::milliseconds(500 + no_steps * 500));
            do_stop = true;
          } else {
            do_stop = false;
            bytes_in_read_thread_buffer += no_read_bytes;
            if (no_read_bytes != 0)
              buf_incomplete = true;
            else {
              ib_stream->Close();
              buf_incomplete = false;
              do_stop = true;
            }
            break;
          }
        }

        if (no_read_bytes == -1) {
          bytes_in_read_thread_buffer = -1;
          do_stop = true;
        }
      }

      if (bytes_in_read_thread_buffer == size)
        do_sleep = true;
      else
        do_sleep = false;
    }
    if (do_sleep) std::this_thread::sleep_for(std::chrono::milliseconds(20));
    if (no_read_bytes != -1) std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return;
}

void ReadBuffer::UseNextBuf() {
  int tmp = curr_buf2_no;
  curr_buf2_no = FindUnusedBuf();
  buf2 = bufs[curr_buf2_no].get();  // to be loaded with data
  curr_buf_no = tmp;
  buf = bufs[curr_buf_no].get();  // to be parsed
}

int ReadBuffer::FindUnusedBuf() {
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

}  // namespace loader
}  // namespace stonedb
