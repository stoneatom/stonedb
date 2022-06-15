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
#ifndef STONEDB_CORE_PACK_STR_H_
#define STONEDB_CORE_PACK_STR_H_
#pragma once

#include <unordered_set>

#include "core/pack.h"
#include "marisa.h"

namespace stonedb {

namespace loader {
class ValueCache;
}

namespace core {

class PackStr final : public Pack {
 public:
  PackStr(DPN *dpn, PackCoordinate pc, ColumnShare *s);
  ~PackStr() {
    DestructionLock();
    Destroy();
  }

  // overrides
  std::unique_ptr<Pack> Clone(const PackCoordinate &pc) const override;
  void LoadDataFromFile(system::Stream *fcurfile) override;
  void UpdateValue(size_t i, const Value &v) override;
  void Save() override;

  types::BString GetValueBinary(int i) const override;

  void LoadValues(const loader::ValueCache *vc);
  bool IsTrie() const { return state_ == PackStrtate::PACK_TRIE; }
  bool Lookup(const types::BString &pattern, uint16_t &id);
  bool LikePrefix(const types::BString &pattern, std::size_t prefixlen, std::unordered_set<uint16_t> &ids);
  bool IsNotMatched(int row, uint16_t &id);
  bool IsNotMatched(int row, const std::unordered_set<uint16_t> &ids);

 protected:
  std::pair<UniquePtr, size_t> Compress() override;
  void CompressTrie();
  void Destroy() override;

 private:
  PackStr() = delete;
  PackStr(const PackStr &, const PackCoordinate &pc);

  void Prepare(int no_nulls);
  void AppendValue(const char *value, uint size) {
    if (size == 0) {
      SetPtrSize(dpn->nr, nullptr, 0);
    } else {
      SetPtrSize(dpn->nr, Put(value, size), size);
      data.sum_len += size;
    }
    dpn->nr++;
  }

  size_t CalculateMaxLen() const;
  types::BString GetStringValueTrie(int ono) const;
  size_t GetSize(int ono) {
    if (data.len_mode == sizeof(ushort))
      return data.lens16[ono];
    else
      return data.lens32[ono];
  }
  void SetSize(int ono, uint size) {
    if (data.len_mode == sizeof(uint16_t))
      data.lens16[ono] = (ushort)size;
    else
      data.lens32[ono] = (uint)size;
  }
  void SetMinS(const types::BString &s);
  void SetMaxS(const types::BString &s);

  // Make sure this is larger than the max length of CHAR/TEXT field of mysql.
  static const size_t DEFAULT_BUF_SIZE = 64_KB;

  struct buf {
    char *ptr;
    const size_t len;
    size_t pos;

    size_t capacity() const { return len - pos; }
    void *put(const void *src, size_t length) {
      ASSERT(length <= capacity());
      auto ret = std::memcpy(ptr + pos, src, length);
      pos += length;
      return ret;
    }
  };

  char *GetPtr(int i) const { return data.index[i]; }
  void SetPtr(int i, void *addr) { data.index[i] = reinterpret_cast<char *>(addr); }
  void SetPtrSize(int i, void *addr, uint size) {
    SetPtr(i, addr);
    SetSize(i, size);
  }

  enum class PackStrtate { PACK_ARRAY, PACK_TRIE };
  PackStrtate state_ = PackStrtate::PACK_ARRAY;
  marisa::Trie marisa_trie_;
  UniquePtr compressed_data_;
  uint16_t *ids_array_;
  struct {
    std::vector<buf> v;
    size_t sum_len;
    char **index;
    union {
      void *lens;
      uint32_t *lens32;
      uint16_t *lens16;
    };
    uint8_t len_mode;
  } data{};

  void *Put(const void *src, size_t length) {
    if (data.v.empty() || length > data.v.back().capacity()) {
      auto sz = length * 2;
      data.v.push_back({(char *)alloc(sz, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED), sz, 0});
    }
    return data.v.back().put(src, length);
  }

  void SaveUncompressed(system::Stream *fcurfile);
  void LoadUncompressed(system::Stream *fcurfile);
  void LoadCompressed(system::Stream *fcurfile);
  void LoadCompressedTrie(system::Stream *fcurfile);
  void TransformIntoArray();

  int GetCompressBufferSize(size_t size);
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_PACK_STR_H_
