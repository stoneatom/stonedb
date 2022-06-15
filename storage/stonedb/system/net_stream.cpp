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
#include "system/rc_system.h"

namespace stonedb {
namespace system {
NetStream::NetStream(const IOParameters &iop) : net(&current_tx->Thd()->net), cached_size(0), cached_offset(0) {
  net_request_file(net, iop.Path());
  opened = true;
}

bool NetStream::IsOpen() const { return opened; }

int NetStream::Close() {
  if (opened) {
    while (my_net_read(net) > 0)
      ;
  }
  opened = false;
  return 0;
}

size_t NetStream::Read(void *buf, size_t count) {
  if (!opened) return 0;

  if (cached_size >= count) {
    std::memcpy(buf, net->read_pos + cached_offset, count);
    cached_size -= count;
    cached_offset += count;
    return count;
  }

  size_t copied = 0;

  // copy the cached first
  if (cached_size) {
    std::memcpy(buf, net->read_pos + cached_offset, cached_size);
    copied = cached_size;
    // now all cached data has been taken
    cached_size = 0;
  }

  auto start = std::chrono::system_clock::now();

  ulong len = my_net_read(net);
  if (len == packet_error) {
    opened = false;
    std::chrono::duration<double> diff = std::chrono::system_clock::now() - start;
    STONEDB_LOG(LogCtl_Level::INFO, "Failed to read from network: %s. Used %f seconds", std::strerror(errno),
                diff.count());
    return copied;
  }
  if (len == 0) {
    /* End of file from client */
    opened = false;
    return copied;
  }

  // we got more than expected
  if (len >= count - copied) {
    std::memcpy((char *)buf + copied, net->read_pos, count - copied);
    cached_size = len - (count - copied);
    cached_offset = count - copied;
    return count;
  }

  // there are less data than expected
  std::memcpy((char *)buf + copied, net->read_pos, len);
  copied += len;

  return copied;
}
}  // namespace system
}  // namespace stonedb
