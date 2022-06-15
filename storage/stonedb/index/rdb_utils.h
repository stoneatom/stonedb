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
#ifndef STONEDB_INDEX_RDB_UTILS_H_
#define STONEDB_INDEX_RDB_UTILS_H_
#pragma once

#include <chrono>
#include <string>
#include <vector>

#include "common/assert.h"
#include "common/mysql_gate.h"
#include "rocksdb/db.h"
#include "util/fs.h"

namespace stonedb {
namespace index {

enum class Separator { LE_SPACES = 1, EQ_SPACES, GE_SPACES };

constexpr uint INTSIZE = 8;
constexpr uint CHUNKSIZE = 9;

// global index id
struct GlobalId {
  uint32_t cf_id;
  uint32_t index_id;
};

struct ColAttr {
  uint16_t col_no;
  uchar col_type;
  uchar col_flag;
};

inline void be_store_uint64(uchar *const dst_netbuf, const uint64_t &n) {
  ASSERT(dst_netbuf != nullptr, "dst_netbuf is NULL");
  uint64_t net_val = htobe64(n);
  memcpy(dst_netbuf, &net_val, sizeof(net_val));
}

inline void be_store_uint32(uchar *const dst_netbuf, const uint32_t &n) {
  ASSERT(dst_netbuf != nullptr, "dst_netbuf is NULL");
  uint32_t net_val = htobe32(n);
  memcpy(dst_netbuf, &net_val, sizeof(net_val));
}

inline void be_store_uint16(uchar *const dst_netbuf, const uint16_t &n) {
  ASSERT(dst_netbuf != nullptr, "dst_netbuf is NULL");
  uint16_t net_val = htobe16(n);
  memcpy(dst_netbuf, &net_val, sizeof(net_val));
}

inline void be_store_byte(uchar *const dst_netbuf, const uchar &c) { *dst_netbuf = c; }

inline void be_store_index(uchar *const dst_netbuf, const uint32_t &number) { be_store_uint32(dst_netbuf, number); }

inline uint64_t be_to_uint64(const uchar *const netbuf) {
  ASSERT(netbuf != nullptr, "netbuf is NULL");
  uint64_t net_val;
  memcpy(&net_val, netbuf, sizeof(net_val));

  return be64toh(net_val);
}

inline uint32_t be_to_uint32(const uchar *const netbuf) {
  uint32_t net_val;
  memcpy(&net_val, netbuf, sizeof(net_val));
  return be32toh(net_val);
}

inline uint16_t be_to_uint16(const uchar *const netbuf) {
  ASSERT(netbuf != nullptr, "netbuf is NULL");

  uint16_t net_val;
  memcpy(&net_val, netbuf, sizeof(net_val));
  return be16toh(net_val);
}

inline uchar be_to_byte(const uchar *const netbuf) {
  ASSERT(netbuf != nullptr, "netbuf is NULL");
  return (uchar)netbuf[0];
}

inline uint32_t be_read_uint32(const uchar **netbuf_ptr) {
  ASSERT(netbuf_ptr != nullptr, "netbuf_ptr is NULL");
  const uint32_t host_val = be_to_uint32(*netbuf_ptr);
  *netbuf_ptr += sizeof(host_val);

  return host_val;
}

inline uint16_t be_read_uint16(const uchar **netbuf_ptr) {
  const uint16_t host_val = be_to_uint16(*netbuf_ptr);
  *netbuf_ptr += sizeof(host_val);

  return host_val;
}

inline void be_read_gl_index(const uchar **netbuf_ptr, GlobalId *const gl_index_id) {
  ASSERT(gl_index_id != nullptr, "gl_index_id is NULL");
  ASSERT(netbuf_ptr != nullptr, "netbuf_ptr is NULL");

  gl_index_id->cf_id = be_read_uint32(netbuf_ptr);
  gl_index_id->index_id = be_read_uint32(netbuf_ptr);
}

class StringReader {
 private:
  const char *m_ptr;
  uint m_len;

 private:
  StringReader &operator=(const StringReader &) = default;

 public:
  StringReader(const StringReader &) = default;
  explicit StringReader(const std::string_view &str) {
    m_len = str.length();
    if (m_len) {
      m_ptr = &str.at(0);
    } else {
      m_ptr = nullptr;
    }
  }

  const char *read(const uint &size) {
    const char *res;
    if (m_len < size) {
      res = nullptr;
    } else {
      res = m_ptr;
      m_ptr += size;
      m_len -= size;
    }
    return res;
  }

  void read_uint8(uchar *const res) {
    const uchar *p;
    if (!(p = reinterpret_cast<const uchar *>(read(1))))
      STONEDB_ERROR("bad read_uint8");
    else {
      *res = *p;
    }
  }

  void read_uint16(uint16_t *const res) {
    const uchar *p;
    if (!(p = reinterpret_cast<const uchar *>(read(2))))
      STONEDB_ERROR("bad read_uint16");
    else {
      *res = be_to_uint16(p);
    }
  }

  void read_uint32(uint32_t *const res) {
    const uchar *p;
    if (!(p = reinterpret_cast<const uchar *>(read(4))))
      STONEDB_ERROR("bad read_uint32");
    else {
      *res = be_to_uint32(p);
    }
  }

  void read_uint64(uint64_t *const res) {
    const uchar *p;
    if (!(p = reinterpret_cast<const uchar *>(read(sizeof(uint64_t))))) {
      STONEDB_ERROR("bad read_uint64");
    } else {
      *res = be_to_uint64(p);
    }
  }

  uint remain_len() const { return m_len; }
  const char *current_ptr() const { return m_ptr; }
};

class StringWriter {
 private:
  std::vector<uchar> m_data;

 public:
  StringWriter(const StringWriter &) = delete;
  StringWriter &operator=(const StringWriter &) = delete;
  StringWriter() = default;

  void clear() { m_data.clear(); }
  void write_uint8(const uint &val) { m_data.push_back(static_cast<uchar>(val)); }

  void write_uint16(const uint &val) {
    const auto size = m_data.size();
    m_data.resize(size + 2);
    be_store_uint16(m_data.data() + size, val);
  }

  void write_uint32(const uint &val) {
    const auto size = m_data.size();
    m_data.resize(size + 4);
    be_store_uint32(m_data.data() + size, val);
  }

  void write_uint64(const uint64_t &val) {
    const auto size = m_data.size();
    m_data.resize(size + 8);
    be_store_uint64(m_data.data() + size, val);
  }

  void write(const uchar *const new_data, const size_t &len) {
    ASSERT(new_data != nullptr, "new_data is NULL");
    m_data.insert(m_data.end(), new_data, new_data + len);
  }

  uchar *ptr() { return m_data.data(); }
  size_t length() const { return m_data.size(); }

  void write_uint8_at(const size_t &pos, const uint &new_val) {
    // overwrite what was written
    ASSERT(pos < length(), "pos error");
    m_data.data()[pos] = new_val;
  }

  void write_uint16_at(const size_t &pos, const uint &new_val) {
    ASSERT(pos < length() && (pos + 1) < length(), "pos error");
    be_store_uint16(m_data.data() + pos, new_val);
  }
};

inline bool NormalizeName(const std::string &path, std::string &name) {
  std::string db_name, tab_name;
  if (path.size() < 2 || path[0] != '.' || path[1] != '/') {
    return false;
  }
  auto pa = fs::path(path);
  std::tie(db_name, tab_name) = std::make_tuple(pa.parent_path().filename().native(), pa.filename().native());
  name = db_name + "." + tab_name;
  return true;
}

}  // namespace index
}  // namespace stonedb

#endif  // STONEDB_INDEX_RDB_UTILS_H_
