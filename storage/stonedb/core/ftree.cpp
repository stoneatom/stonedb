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

#include "ftree.h"

#include <fstream>
#include <map>

#include "system/rc_system.h"
#include "system/stonedb_file.h"

namespace stonedb {
namespace core {
static const size_t BUF_SIZE_LIMIT = 1_GB;

FTree::FTree(const FTree &ft)
    : mm::TraceableObject(ft),
      total_dic_size(ft.total_dic_size),
      total_buf_size(ft.total_buf_size),
      last_code(ft.last_code),
      max_value_size(ft.max_value_size),
      changed(ft.changed),
      hash_size(ft.hash_size),
      hdr(ft.hdr) {
  mem = (char *)alloc(total_buf_size, mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);
  len = (uint16_t *)alloc(total_dic_size * sizeof(uint16_t), mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);
  value_offset = (uint32_t *)alloc(total_dic_size * sizeof(uint32_t), mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);

  if (total_buf_size > 0 && (!mem || !len || !value_offset)) {
    Destroy();
    STONEDB_LOG(LogCtl_Level::ERROR, "FTree, out of memory.");
    throw common::OutOfMemoryException();
  }

  std::memcpy(mem, ft.mem, total_buf_size);
  std::memcpy(len, ft.len, hdr.size * sizeof(uint16_t));
  std::memcpy(value_offset, ft.value_offset, hdr.size * sizeof(uint32_t));

  if (ft.hash_table) hash_table = (int *)alloc(hash_size * sizeof(int), mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);

  if (ft.hash_table && !hash_table) {
    Destroy();
    STONEDB_LOG(LogCtl_Level::ERROR, "FTree, out of memory.");
    throw common::OutOfMemoryException();
  }

  if (ft.hash_table) std::memcpy(hash_table, ft.hash_table, hash_size * sizeof(int));
}

void FTree::Destroy() {
  if (mem) {
    dealloc(mem);
    mem = NULL;
  }

  if (len) {
    dealloc(len);
    len = NULL;
  }

  if (value_offset) {
    dealloc(value_offset);
    value_offset = NULL;
  }

  if (hash_table) {
    dealloc(hash_table);
    hash_table = NULL;
  }
}

void FTree::Release() {
  // if(owner) owner->DropObjectByMM(_coord);
}

FTree::~FTree() { Destroy(); }

std::unique_ptr<FTree> FTree::Clone() const { return std::unique_ptr<FTree>(new FTree(*this)); }

types::BString FTree::GetRealValue(int v) {
  if (v >= 0 && v < hdr.size) {
    if (len[v] == 0) return types::BString("", 0);
    return types::BString((mem + value_offset[v]), len[v]);
  }
  return types::BString();
}

char *FTree::GetBuffer(int v) {
  if (v >= 0 && v < hdr.size) return (mem + value_offset[v]);
  return NULL;
}

int FTree::GetEncodedValue(const char *str, size_t sz) {
  if (mem == NULL) return -1;
  int local_last_code = last_code;  // for multithread safety

  if (local_last_code > -1 && sz == len[local_last_code] &&
      std::memcmp(str, (mem + value_offset[local_last_code]), sz) == 0)
    return local_last_code;
  return HashFind(str, sz);
}

void FTree::Init(int width) {
  ASSERT(width > 0);
  hdr.max_len = width;
  hdr.size = 0;
  total_dic_size = 10;  // minimal dictionary size
  total_buf_size = hdr.max_len * 10 + 10;
  if (mem) dealloc(mem);
  mem = (char *)alloc(total_buf_size, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
  if (len == NULL) len = (uint16_t *)alloc(total_dic_size * sizeof(uint16_t), mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);
  if (value_offset == NULL)
    value_offset = (uint32_t *)alloc(total_dic_size * sizeof(uint32_t), mm::BLOCK_TYPE::BLOCK_TEMPORARY, true);

  std::memset(mem, 0, total_buf_size);
  last_code = -1;
  changed = false;
}

int FTree::Add(const char *str, size_t sz) {
  ASSERT(hdr.max_len > 0);
  if (size_t(hdr.size) > stonedb_sysvar_lookup_max_size) {
    throw common::OutOfMemoryException("Too many lookup values. Dictionary size is " +
                                       std::to_string(stonedb_sysvar_lookup_max_size));
  }

  int code = HashFind(str, sz, hdr.size);
  ASSERT(code == hdr.size);  // should not add if the item already exists

  // Not found? The first unused number returned.
  changed = true;
  if (total_dic_size < size_t(hdr.size + 1)) {  // Enlarge tables, if required
    int new_dic_size = int((total_dic_size + 10) * 1.2);
    if (new_dic_size > 536870910) new_dic_size = total_dic_size + 10;
    len = (uint16_t *)rc_realloc(len, new_dic_size * sizeof(uint16_t), mm::BLOCK_TYPE::BLOCK_TEMPORARY);
    value_offset =
        (uint32_t *)rc_realloc(value_offset, new_dic_size * sizeof(uint32_t), mm::BLOCK_TYPE::BLOCK_TEMPORARY);
    if (len == NULL || value_offset == NULL) throw common::OutOfMemoryException("Too many lookup values");
    for (int i = total_dic_size; i < new_dic_size; i++) {
      len[i] = 0;
      value_offset[i] = common::NULL_VALUE_32;
    }
    total_dic_size = new_dic_size;
  }
  size_t new_value_offset = (hdr.size == 0 ? 0 : value_offset[hdr.size - 1] + len[hdr.size - 1]);
  if (total_buf_size < new_value_offset + sz) {  // Enlarge tables, if required
    size_t new_buf_size = (total_buf_size + sz + 10) * 1.2;
    if (new_buf_size > BUF_SIZE_LIMIT) new_buf_size = int64_t(total_buf_size) + sz;
    if (new_buf_size > BUF_SIZE_LIMIT) throw common::OutOfMemoryException("Too many lookup values");
    mem = (char *)rc_realloc(mem, new_buf_size, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
    std::memset(mem + total_buf_size, 0, new_buf_size - total_buf_size);
    total_buf_size = int(new_buf_size);
  }
  if (sz > 0) std::memcpy(mem + new_value_offset, str, sz);
  value_offset[hdr.size] = new_value_offset;
  len[hdr.size] = sz;
  if (sz > (uint)max_value_size) max_value_size = sz;
  hdr.size++;
  return hdr.size - 1;
}

int FTree::MaxValueSize(int start,
                        int end)  // max. value size for an interval of codes
{
  if (end - start > hdr.size / 2 || end - start > 100000) return max_value_size;
  unsigned short max_size = 0;
  for (int i = start; i <= end; i++)
    if (max_size < len[i]) max_size = len[i];
  return max_size;
}

int FTree::ByteSize() {
  int sz = sizeof(hdr) + sizeof(uint16_t) * hdr.size;
  int len_sum = 0;
  for (int i = 0; i < hdr.size; i++) len_sum += len[i];
  return sz + len_sum;
}

void FTree::SaveData(const fs::path &p) {
  system::StoneDBFile f;
  f.OpenCreate(p);
  std::vector<char> v(ByteSize());

  auto buf = &v[0];

  *(HDR *)buf = hdr;
  buf += sizeof(hdr);

  if (hdr.size != 0) {
    for (int i = 0; i < hdr.size; i++) {
      if (len[i] > hdr.max_len) throw common::InternalException("Invalid length of a lookup value");
      *((uint16_t *)buf) = len[i];
      buf += sizeof(uint16_t);
    }
    int len_sum = (hdr.size > 0 ? value_offset[hdr.size - 1] + len[hdr.size - 1] : 0);
    std::memcpy(buf, mem, len_sum);
    if (STONEDB_LOGCHECK(LogCtl_Level::DEBUG)) {
      if (CheckConsistency() != 0) {
        STONEDB_LOG(LogCtl_Level::DEBUG, "FTree CheckConsistency fail");
      }
    }
  }
  f.WriteExact(&v[0], v.size());
  f.Flush();
  changed = false;
}

void FTree::LoadData(const fs::path &p) {
  auto size = fs::file_size(p);
  if (size > BUF_SIZE_LIMIT) {
    throw common::InternalException("Dictionary is too large");
  }

  auto ptr = std::make_unique<char[]>(size);
  char *buf = ptr.get();

  std::ifstream ifs(p, std::ios::binary);
  ifs.read(buf, size);
  if (!ifs) {
    throw common::Exception("Failed to read from file " + p.string());
  }

  hdr = *(HDR *)buf;
  buf += sizeof(HDR);

  if (hdr.ver != DICT_FILE_VERSION) {
    STONEDB_LOG(LogCtl_Level::ERROR, "bad dictionary version %d", hdr.ver);
    return;
  }

  total_dic_size = hdr.size;

  dealloc(len);
  len = (uint16_t *)alloc(total_dic_size * sizeof(uint16_t), mm::BLOCK_TYPE::BLOCK_TEMPORARY);

  dealloc(value_offset);
  value_offset = (uint32_t *)alloc(total_dic_size * sizeof(uint32_t), mm::BLOCK_TYPE::BLOCK_TEMPORARY);

  max_value_size = 0;
  for (int i = 0; i < hdr.size; i++) {
    auto loc_len = *(uint16_t *)buf;
    len[i] = loc_len;
    if (loc_len > max_value_size) max_value_size = loc_len;
    value_offset[i] = (i == 0 ? 0 : value_offset[i - 1] + len[i - 1]);
    buf += 2;
  }

  int len_sum = (hdr.size > 0 ? value_offset[hdr.size - 1] + len[hdr.size - 1] : 0);

  if (hdr.size < 3)
    total_buf_size = hdr.max_len * 10 + 10;
  else
    total_buf_size = len_sum;

  dealloc(mem);
  mem = (char *)alloc(total_buf_size, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
  std::memset(mem, 0, total_buf_size);
  std::memcpy(mem, buf, len_sum);
  last_code = -1;
  changed = false;
}

// Hash part

void FTree::InitHash() {
  // Note: from the limitation below, we cannot use more than
  // stonedb_sysvar_lookup_max_size
  hash_size = int(hdr.size < 30 ? 97 : (hdr.size + 10) * 2.5);
  while (hash_size % 2 == 0 || hash_size % 3 == 0 || hash_size % 5 == 0 || hash_size % 7 == 0) hash_size++;
  dealloc(hash_table);
  hash_table = (int *)alloc(hash_size * sizeof(int), mm::BLOCK_TYPE::BLOCK_TEMPORARY);  // 2 GB max.
  if (hash_table == NULL) throw common::OutOfMemoryException("Too many lookup values");

  std::memset(hash_table, 0xFF,
              hash_size * sizeof(int));  // set -1 for all positions
  for (int i = 0; i < hdr.size; i++) HashFind(mem + value_offset[i], len[i], i);
}

int FTree::HashFind(const char *v, int v_len, int position_if_not_found) {
  if (hash_table == NULL) InitHash();
  unsigned int crc_code = (v_len == 0 ? 0 : HashValue((const unsigned char *)v, v_len));
  int row = crc_code % hash_size;
  int step = 3 + crc_code % 8;  // step: 3, 4, 5, 6, 7, 8, 9, 10  => hash_size
                                // should not be dividable by 2, 3, 5, 7
  while (step != 0) {           // infinite loop
    int hash_pos = hash_table[row];
    if (hash_pos == -1) {
      if (position_if_not_found > -1) {
        if (hdr.size > hash_size * 0.9) {  // too many values, hash table should be rebuilt
          InitHash();
          HashFind(v, v_len,
                   position_if_not_found);  // find again, as hash_size changed
        } else
          hash_table[row] = position_if_not_found;
        return position_if_not_found;
      }
      return -1;
    }
    if (len[hash_pos] == v_len && (v_len == 0 || std::memcmp(mem + value_offset[hash_pos], v, v_len) == 0))
      return hash_pos;
    row = (row + step) % hash_size;
  }
  return -1;  // should never reach this line
}

int FTree::CheckConsistency() {
  if (hdr.size == 0) return 0;
  for (int i = 0; i < hdr.size; i++)
    if (len[i] > hdr.max_len) return 1;
  std::map<uint, std::set<int>> crc_to_pos;
  for (int i = 0; i < hdr.size; i++)
    crc_to_pos[HashValue((const unsigned char *)(mem + value_offset[i]), len[i])].insert(i);

  for (auto const &map_it : crc_to_pos) {
    const auto &pos_set = map_it.second;
    for (const auto &it1 : pos_set)
      for (const auto &it2 : pos_set)
        if (it1 != it2 && len[it1] == len[it2])
          if (std::memcmp(mem + value_offset[it1], mem + value_offset[it2], len[it1]) == 0) return 2;
  }
  return 0;
}
}  // namespace core
}  // namespace stonedb
