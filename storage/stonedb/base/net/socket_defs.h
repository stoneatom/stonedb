/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2016 ScyllaDB.
 * Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
 */
#ifndef STONEDB_BASE_SOCKET_DEFS_H_
#define STONEDB_BASE_SOCKET_DEFS_H_
#pragma once

#include <netinet/ip.h>
#include <sys/socket.h>
#include <iosfwd>

#include <boost/algorithm/string.hpp>
#include <boost/asio/ip/address_v4.hpp>

#include "base/net/byteorder.h"

namespace stonedb {
namespace base {

struct ipv4_addr;

class socket_address {
 public:
  union {
    ::sockaddr_storage sas;
    ::sockaddr sa;
    ::sockaddr_in in;
  } u;

  socket_address(sockaddr_in sa) { u.in = sa; }
  socket_address(ipv4_addr);
  socket_address() = default;

  ::sockaddr &as_posix_sockaddr() { return u.sa; }
  ::sockaddr_in &as_posix_sockaddr_in() { return u.in; }
  const ::sockaddr &as_posix_sockaddr() const { return u.sa; }
  const ::sockaddr_in &as_posix_sockaddr_in() const { return u.in; }

  bool operator==(const socket_address &) const;
};

std::ostream &operator<<(std::ostream &, const socket_address &);

enum class transport {
  TCP = IPPROTO_TCP,
#if 0
    SCTP = IPPROTO_SCTP
#endif
};

struct listen_options {
  transport proto = transport::TCP;
  bool reuse_address = false;
  listen_options(bool rua = false) : reuse_address(rua) {}
};

struct ipv4_addr {
  uint32_t ip;
  uint16_t port;

  ipv4_addr() : ip(0), port(0) {}
  ipv4_addr(uint32_t ip, uint16_t port) : ip(ip), port(port) {}
  ipv4_addr(uint16_t port) : ip(0), port(port) {}
  ipv4_addr(const std::string &addr) {
    std::vector<std::string> items;
    boost::split(items, addr, boost::is_any_of(":"));

    if (items.size() == 1) {
      ip = boost::asio::ip::address_v4::from_string(addr).to_ulong();
      port = 0;
    } else if (items.size() == 2) {
      ip = boost::asio::ip::address_v4::from_string(items[0]).to_ulong();
      port = std::stoul(items[1]);
    } else {
      throw std::invalid_argument("invalid format: " + addr);
    }
  }

  ipv4_addr(const std::string &addr, uint16_t port_)
      : ip(boost::asio::ip::address_v4::from_string(addr).to_ulong()), port(port_) {}

  ipv4_addr(const socket_address &sa) {
    ip = net::ntoh(sa.u.in.sin_addr.s_addr);
    port = net::ntoh(sa.u.in.sin_port);
  }

  ipv4_addr(socket_address &&sa) : ipv4_addr(sa) {}
};

}  // namespace base
}  // namespace stonedb

#endif  // STONEDB_BASE_SOCKET_DEFS_H_
