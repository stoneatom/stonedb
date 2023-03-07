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

#include <sys/mman.h>
#include <unistd.h>
#include <cstring>

#include "common/exception.h"
#include "core/column_share.h"
#include "core/engine.h"
#include "system/tianmu_file.h"
#include "system/tianmu_system.h"

namespace Tianmu {
namespace core {

/*
  The size of the current DPN structure object is used to prevent the structure of
  the DPN from being changed arbitrarily.
  If modification is required,
  please consider the size of PAGT_CNT and COL_DN_FILE_SIZE to
  prevent space waste and give consideration to IO efficiency
*/
static constexpr size_t DPN_SIZE = 88;
// make sure the struct is not modified by mistake
static_assert(sizeof(DPN) == DPN_SIZE, "Bad struct size of DPN");

// Operating system page size
static constexpr size_t PAGE_SIZE = 4096;
// Number of pages per allocation
static constexpr size_t PAGE_CNT = 11;
// Size of DPN memory allocation
static constexpr size_t ALLOC_UNIT = PAGE_CNT * PAGE_SIZE;

// Ensure that the allocated memory is an integer multiple of the DPN size
static_assert(ALLOC_UNIT % sizeof(DPN) == 0);

// Number of dpns allocated each time
static constexpr size_t DPN_INC_CNT = ALLOC_UNIT / sizeof(DPN);

ColumnShare::~ColumnShare() {
  if (start != nullptr) {
    if (::munmap(start, common::COL_DN_FILE_SIZE) != 0) {
      // DO NOT throw in dtor!
      TIANMU_LOG(LogCtl_Level::WARN, "Failed to unmap DPN file. Error %d(%s)", errno, std::strerror(errno));
    }
  }
  if (dn_fd >= 0)
    ::close(dn_fd);
}

void ColumnShare::Init(common::TX_ID xid) {
  map_dpn();

  read_meta();

  scan_dpn(xid);
}

void ColumnShare::map_dpn() {
  auto dpn_file = m_path / common::COL_DN_FILE;
  dn_fd = ::open(dpn_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (dn_fd < 0)
    throw std::system_error(errno, std::system_category(), "open() " + dpn_file.string());

  struct stat sb;
  if (::fstat(dn_fd, &sb) != 0) {
    throw std::system_error(errno, std::system_category(), "stat() " + dpn_file.string());
  }

  ASSERT(sb.st_size % sizeof(DPN) == 0);
  capacity = sb.st_size / sizeof(DPN);

  auto addr = ::mmap(0, common::COL_DN_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, dn_fd, 0);
  if (addr == MAP_FAILED) {
    throw std::system_error(errno, std::system_category(), "mmap() " + dpn_file.string());
  }

  // TODO: should we mlock(addr, common::COL_DN_FILE_SIZE)?

  start = static_cast<DPN *>(addr);
}

void ColumnShare::read_meta() {
  auto fname = m_path / common::COL_META_FILE;
  system::TianmuFile file;
  file.OpenReadOnly(fname);

  COL_META meta;
  file.ReadExact(&meta, sizeof(meta));
  file.Close();

  if (meta.magic != common::COL_FILE_MAGIC)
    throw common::DatabaseException("Invalid column data file " + fname.string());

  ASSERT(meta.ver == common::COL_FILE_VERSION, "Invalid version in column file " + m_path.string());

  pss = meta.pss;
  ct.SetTypeName(meta.type);
  ct.SetFmt(meta.fmt);
  ct.SetFlag(meta.flag);
  ct.SetPrecision(meta.precision);
  ct.SetScale(meta.scale);

  auto type = ct.GetTypeName();
  if (ct.Lookup() || ATI::IsNumericType(type) || ATI::IsDateTimeType(type))
    pt = common::PackType::INT;
  else
    pt = common::PackType::STR;

  if (pt == common::PackType::INT) {
    has_filter_hist = true;
  } else {
    if (!types::RequiresUTFConversions(ct.GetCollation()))
      has_filter_cmap = true;

    if (ct.HasFilter())
      has_filter_bloom = true;
  }
}

void ColumnShare::scan_dpn(common::TX_ID xid) {
  COL_VER_HDR hdr{};
  system::TianmuFile fv;
  fv.OpenReadOnly(m_path / common::COL_VERSION_DIR / xid.ToString());
  fv.ReadExact(&hdr, sizeof(hdr));

  ASSERT(hdr.numOfPacks <= capacity, "bad dpn index");

  // get column saved auto inc
  auto_inc_.store(hdr.auto_inc);

  if (hdr.numOfPacks == 0) {
    for (uint32_t i = 0; i < capacity; i++) {
      start[i].reset();
    }
    return;
  }

  auto arr = std::make_unique<common::PACK_INDEX[]>(hdr.numOfPacks);
  fv.ReadExact(arr.get(), hdr.numOfPacks * sizeof(common::PACK_INDEX));
  auto end = arr.get() + hdr.numOfPacks;

  for (uint32_t i = 0; i < capacity; i++) {
    auto found = std::find(arr.get(), end, i);
    if (found == end) {
      start[i].reset();
    } else {
      start[i].SetPackPtr(0);
      start[i].used = 1;
      start[i].base = common::INVALID_PACK_INDEX;
      start[i].synced = 1;
      start[i].xmax = common::MAX_XID;
      if (start[i].local) {
        TIANMU_LOG(LogCtl_Level::WARN, "uncommited pack found: %s %d", m_path.c_str(), i);
        start[i].local = 0;
      }
      if (start[i].dataAddress != DPN_INVALID_ADDR) {
        segs.push_back({start[i].dataAddress, start[i].dataLength, i});
      } else {
      }
    }
  }

  segs.sort([](const auto &a, const auto &b) { return a.offset < b.offset; });

  // make sure the data is good
  auto second = segs.cbegin();
  for (auto first = second++; second != segs.cend(); ++first, ++second) {
    if (second->offset < first->offset + first->len) {
      TIANMU_LOG(LogCtl_Level::ERROR, "sorted beg: -------------------");
      for (auto &it : segs) {
        TIANMU_LOG(LogCtl_Level::ERROR, "     %u  [%ld, %ld]", it.idx, it.offset, it.len);
      }
      TIANMU_LOG(LogCtl_Level::ERROR, "sorted end: -------------------");
      throw common::DatabaseException("bad DPN index file: " + m_path.string());
    }
  }
}

void ColumnShare::init_dpn(DPN &dpn, const common::TX_ID xid, const DPN *from) {
  if (from != nullptr) {
    dpn = *from;
  } else {
    dpn.reset();
    dpn.dataAddress = DPN_INVALID_ADDR;
    if (pt == common::PackType::INT) {
      dpn.min_i = common::PLUS_INF_64;
      dpn.max_i = common::MINUS_INF_64;
    } else {
      dpn.min_i = 0;
      dpn.max_i = -1;
    }
  }
  dpn.used = 1;
  dpn.local = 1;   // a new allocated dpn is __always__ owned by write session
  dpn.synced = 1;  // would be reset by Pack when there is update
  if (from != nullptr)
    dpn.base = GetPackIndex(const_cast<DPN *>(from));
  else
    dpn.base = common::INVALID_PACK_INDEX;
  dpn.xmin = xid;
  dpn.xmax = common::MAX_XID;
  dpn.SetPackPtr(0);
}

int ColumnShare::alloc_dpn(common::TX_ID xid, const DPN *from) {
  for (uint32_t i = 0; i < capacity; i++) {
    if (start[i].used == 1) {
      if (!(start[i].xmax < ha_tianmu_engine_->MinXID()))
        continue;
      ha_tianmu_engine_->cache.DropObject(PackCoordinate(owner->TabID(), col_id, i));
      segs.remove_if([i](const auto &s) { return s.idx == i; });
    }
    init_dpn(start[i], xid, from);
    return i;
  }

  ASSERT((capacity + DPN_INC_CNT) <= (common::COL_DN_FILE_SIZE / sizeof(DPN)),
         "Failed to allocate new DN: " + m_path.string());
  capacity += DPN_INC_CNT;

  //  NOTICE:
  // It is not portable to enlarge the file size after mmapping, but it seems to
  // work well on Linux. Otherwise we'll need to remmap the file, which in turn
  // requires locking because other threads may read simultaneously.
  ftruncate(dn_fd, capacity * sizeof(DPN));
  init_dpn(start[capacity - 1], xid, from);
  return capacity - 1;
}

void ColumnShare::alloc_seg(DPN *dpn) {
  auto i = GetPackIndex(dpn);

  uint64_t prev = 0;
  for (auto it = segs.cbegin(); it != segs.cend(); ++it) {
    if (it->offset - prev > dpn->dataLength) {
      segs.insert(it, {prev, dpn->dataLength, i});
      dpn->dataAddress = prev;
      return;
    }
    prev = it->offset + it->len;
  }
  segs.push_back({prev, dpn->dataLength, i});
  dpn->dataAddress = prev;
}

void ColumnShare::sync_dpns() {
  int ret = ::msync(start, common::COL_DN_FILE_SIZE, MS_SYNC);
  if (ret != 0)
    throw std::system_error(errno, std::system_category(), "msync() " + m_path.string());
}
}  // namespace core
}  // namespace Tianmu
