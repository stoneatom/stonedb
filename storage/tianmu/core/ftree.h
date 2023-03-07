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

// Ftree - dictionary for strings(can be persisted to disk)
#ifndef TIANMU_CORE_FTREE_H_
#define TIANMU_CORE_FTREE_H_
#pragma once

#include "core/bin_tools.h"
#include "mm/traceable_object.h"
#include "types/tianmu_data_types.h"
#include "util/fs.h"

namespace Tianmu {
namespace core {
class FTree final : public mm::TraceableObject {
 public:
  FTree() = default;
  ~FTree();
  std::unique_ptr<FTree> Clone() const;

  void Init(int width);

  types::BString GetRealValue(int v);
  char *GetBuffer(int v);  // the pointer to the memory place of the real value

  int GetEncodedValue(const char *str,
                      size_t sz);       // return -1 when value not found
  int Add(const char *str, size_t sz);  // return -1 when len=0
  int CountOfUniqueValues() const { return hdr.size; }
  void SaveData(const fs::path &p);
  void LoadData(const fs::path &p);

  int ValueSize(int i) { return len[i]; }
  int MaxValueSize(int start,
                   int end);  // max. value size for an interval of codes
  int CheckConsistency();     // 0 = dictionary consistent, otherwise corrupted

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_FTREE; }
  void Release() override;
  bool Changed() const { return changed; }

 private:
  FTree(const FTree &ft);
  void InitHash();
  int HashFind(const char *v, int v_len,
               int position_if_not_found = -1);  // return a position or -1 if not found;
  void Destroy();
  int ByteSize();  // number of bytes required to store the dictionary

 private:
  char *mem = nullptr;               // dictionary: size*value_size bytes. Values are NOT terminated
                                     // by anything and may contain any character (incl. 0)
  uint16_t *len = nullptr;           // table of lengths of values
  uint32_t *value_offset = nullptr;  // mem + value_offset[v] is an address of value v

  size_t total_dic_size{0};  // may be more than dict size, because we reserve
                             // some place for new values
  size_t total_buf_size{0};  // in bytes, the current size of value buffer
  int last_code{-1};         // last code used
  int max_value_size{0};     // max. length of a string found in dictionary

  bool changed = false;

  // Hash section
  int *hash_table = nullptr;
  int hash_size{0};

  const static int32_t DICT_FILE_VERSION = 1;
#pragma pack(push, 1)
  struct HDR final {
    int32_t ver;
    int32_t size;     // number of different values
    int32_t max_len;  // max. length of one string (limit)
  };
#pragma pack(pop)
  HDR hdr{DICT_FILE_VERSION, 0, 0};
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_FTREE_H_
