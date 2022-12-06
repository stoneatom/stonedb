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

#include "system/net_stream.h"

#include <chrono>

#include "common/assert.h"
#include "core/transaction.h"
#include "system/tianmu_system.h"

namespace Tianmu {
namespace system {
NetStream::NetStream(const IOParameters &iop) : net_(&current_txn_->Thd()->net), cached_size_(0), cached_offset_(0) {
  // net_request_file(net_, iop.Path());
  net_write_command(net_, 251, (uchar *)iop.Path(), strlen(iop.Path()), (uchar *)"", 0);
  opened_ = true;
}

bool NetStream::IsOpen() const { return opened_; }

int NetStream::Close() {
  if (opened_) {
    while (my_net_read(net_) > 0)
      ;
  }
  opened_ = false;
  return 0;
}

size_t NetStream::Read(void *buf, size_t count) {
  if (!opened_)
    return 0;

  if (cached_size_ >= count) {
    std::memcpy(buf, net_->read_pos + cached_offset_, count);
    cached_size_ -= count;
    cached_offset_ += count;
    return count;
  }

  size_t copied = 0;

  // copy the cached first
  if (cached_size_) {
    std::memcpy(buf, net_->read_pos + cached_offset_, cached_size_);
    copied = cached_size_;
    // now all cached data has been taken
    cached_size_ = 0;
  }

  auto start = std::chrono::system_clock::now();

  ulong len = my_net_read(net_);
  if (len == packet_error) {
    opened_ = false;
    std::chrono::duration<double> diff = std::chrono::system_clock::now() - start;
    TIANMU_LOG(LogCtl_Level::INFO, "Failed to read from network: %s. Used %f seconds", std::strerror(errno),
               diff.count());
    return copied;
  }
  if (len == 0) {
    /* End of file from client */
    opened_ = false;
    return copied;
  }

  // we got more than expected
  if (len >= count - copied) {
    std::memcpy((char *)buf + copied, net_->read_pos, count - copied);
    cached_size_ = len - (count - copied);
    cached_offset_ = count - copied;
    return count;
  }

  // there are less data than expected
  std::memcpy((char *)buf + copied, net_->read_pos, len);
  copied += len;

  return copied;
}
}  // namespace system
}  // namespace Tianmu
