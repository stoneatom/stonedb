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
#ifndef STONEDB_CORE_COLUMN_SHARE_H_
#define STONEDB_CORE_COLUMN_SHARE_H_
#pragma once

#include <list>

#include "common/assert.h"
#include "common/common_definitions.h"
#include "common/mysql_gate.h"
#include "core/column_type.h"
#include "core/dpn.h"
#include "util/fs.h"

namespace stonedb {
namespace core {
class TableShare;

struct COL_META {
  uint32_t magic;
  uint32_t ver;         // file version
  uint8_t pss;          // pack size shift
  common::CT type;      // type
  common::PackFmt fmt;  // data format: LZ4, snappy, lookup, raw, etc
  uint8_t flag;
  uint32_t precision;
  uint32_t scale;
};

struct alignas(128) COL_VER_HDR_V3 {
  uint64_t nr;  // no. of records
  uint64_t nn;  // no. of nulls
  uint64_t np;  // no. of packs
  uint64_t auto_inc_next;
  int64_t min;
  int64_t max;
  uint32_t dict_ver;  // dict file version name. 0 means n/a
  uint32_t unique : 1;
  uint32_t unique_updated : 1;
  uint64_t natural_size;
  uint64_t compressed_size;
};

using COL_VER_HDR = COL_VER_HDR_V3;

class ColumnShare final {
  friend class RCAttr;

 public:
  ColumnShare() = delete;
  ~ColumnShare();
  ColumnShare(ColumnShare const &) = delete;
  void operator=(ColumnShare const &x) = delete;
  ColumnShare(TableShare *owner, common::TX_ID ver, uint32_t i, const fs::path &p, const Field *f)
      : owner(owner), m_path(p), col_id(i) {
    ct.SetCollation({f->charset(), f->derivation()});
    ct.SetAutoInc(f->flags & AUTO_INCREMENT_FLAG);
    Init(ver);
  }

  DPN *get_dpn_ptr(common::PACK_INDEX i) {
    ASSERT(i < common::COL_DN_FILE_SIZE / sizeof(DPN), "bad dpn index: " + std::to_string(i));
    return &start[i];
  }
  const DPN *get_dpn_ptr(common::PACK_INDEX i) const {
    ASSERT(i < common::COL_DN_FILE_SIZE / sizeof(DPN), "bad dpn index: " + std::to_string(i));
    return &start[i];
  }

  int alloc_dpn(common::TX_ID xid, const DPN *dpn = nullptr);
  void init_dpn(DPN &dpn, const common::TX_ID xid, const DPN *from);
  void sync_dpns();
  void alloc_seg(DPN *dpn);

  const ColumnType &ColType() const { return ct; }
  std::string DataFile() const { return m_path / common::COL_DATA_FILE; }
  uint8_t pss;
  common::PACK_INDEX GetPackIndex(DPN *dpn) const {
    auto i = std::distance(start, dpn);
    ASSERT(i >= 0 && size_t(i) < cap, "bad index " + std::to_string(i));
    return i;
  }

 private:
  void Init(common::TX_ID xid);
  void map_dpn();
  void read_meta();
  void scan_dpn(common::TX_ID xid);

  TableShare *owner;
  const fs::path m_path;
  ColumnType ct;
  int dn_fd{-1};
  DPN *start;
  size_t cap{0};  // current capacity of the dn array
  common::PackType pt;
  uint32_t col_id;

  struct seg {
    uint64_t offset;
    uint64_t len;
    common::PACK_INDEX idx;
  };
  std::list<seg> segs;  // only used by write session so no mutex is needed

  bool has_filter_cmap = false;
  bool has_filter_hist = false;
  bool has_filter_bloom = false;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_COLUMN_SHARE_H_
