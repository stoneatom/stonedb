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
#ifndef TIANMU_SYSTEM_CACHEABLE_ITEM_H_
#define TIANMU_SYSTEM_CACHEABLE_ITEM_H_
#pragma once

#include <vector>

#include "system/tianmu_file.h"

namespace Tianmu {
namespace system {
//////////////////////////////////////////////////////////////////////////////////////////
///////////////////  CacheableItem:
///////////////////////////////////////////////////////
//
//	Base class for objects that need to store large memory blocks in disk
// cache.
//
// Constructor parameters:
//    owner_name  - an identifier of the child class manager (up to 3 char.)
//    object_id   - an identifier of the child class (up to 3 char.)
//    _default_block_size - if set, the size parameter in CI_Put may be omitted

class CacheableItem {
 protected:
  CacheableItem(char const *owner_name, char const *object_id,
                int _default_block_size = -1);  // names up to 3 characters
  virtual ~CacheableItem();

  void CI_Put(int block, unsigned char *data,
              int size = -1);  // put the data block to disk (size
                               // in bytes, default value used if
                               // not specified)
  int CI_Get(int block, unsigned char *data, int size = -1, int off = 0);
  // Get the data block (when 'size'=-1, the size is stored in the class)
  // or its subrange from position 'off', of length 'size' bytes.
  // Returns 0 if successfully got the (sub)block, -1 otherwise

  void CI_SetDefaultSize(uint64_t size)  // If the value is set (here, or in constructor),
  {
    default_block_size_ = size;
  }  // then the size parameter in CI_Put may be omitted.

 private:
  void SetFilename(int i);     // set the current filename to the i-th file
  char *file_name_ = nullptr;  // the current filename with the full path
  // the filename template is as follows:
  // "..path../XXXXXXnnnnnnAAAAAAAABBBBBBBB.tianmu_tmp" where X..X is an id. of
  // object manager and object type (filled with "_") n..n is the number of file
  // (0..999999), A..A is the random session number (hex), B..B is the
  // semi-random object number (its memory address while creating, hex)
  size_t filename_n_position_ = 0;  // the position of the first character of
                                    // "nnnnnn" section of the file name
  int max_file_id_ = 0;             // maximal used file number, start with 0
  int max_file_pos_ = 0;            // the end of the last file used (here we will append)
  int no_block_ = 0;                // the number of registered (saved) data blocks
  std::vector<int> file_number_;    // a number of file where the i-th data block is stored
  std::vector<int> file_start_;     // an address in the file where the i-th data block starts
  std::vector<int> file_size_;      // a size of the i-th data block

  uint64_t default_block_size_ = 0;
  TianmuFile cur_file_handle_;
  int cur_file_number_ = -1;  // the number of currently opened file
  void *cur_map_addr_ = nullptr;
};
}  // namespace system
}  // namespace Tianmu

#endif  // TIANMU_SYSTEM_CACHEABLE_ITEM_H_
