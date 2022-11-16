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

#include "cacheable_item.h"

#include "core/tools.h"
#include "system/fet.h"
#include "system/tianmu_system.h"

namespace Tianmu {
namespace system {
CacheableItem::CacheableItem(char const *owner_name, char const *object_id, int default_block_size)
    : default_block_size_(default_block_size) {
  DEBUG_ASSERT(owner_name != nullptr);
  DEBUG_ASSERT(object_id != nullptr);

  constexpr size_t kMinOwnerNameLen = 3;
  constexpr size_t kOwnerAndObjectLen = 6;
  constexpr size_t kNumberFillLen = 6;
  constexpr size_t kRandomLen = 8;
  constexpr size_t kObjectPtrLen = 8;
  constexpr size_t kSuffixLen = 11;
  size_t filename_offset = 0;

  // read the configuration parameter
  std::string temp_filename = tianmu_sysvar_cachefolder;
  char last_char = temp_filename[temp_filename.size() - 1];
  if (last_char != '/' && last_char != '\\') {
    temp_filename += "/";
  }

  // copy the temporary folder first
  // "...path.../XXXXXXnnnnnnAAAAAAAABBBBBBBB.tianmu_tmp"
  size_t total_length =
      temp_filename.length() + kOwnerAndObjectLen + kNumberFillLen + kRandomLen + kObjectPtrLen + kSuffixLen + 1;
  file_name_ = new char[total_length];
  file_name_[total_length - 1] = 0;
  std::strcpy(file_name_, temp_filename.c_str());
  filename_offset = temp_filename.length();

  // fill the file name
  size_t i = 0, j = 0;

  while (owner_name[j] != 0 && i < kOwnerAndObjectLen) file_name_[filename_offset + (i++)] = owner_name[j++];
  while (i < kMinOwnerNameLen) file_name_[filename_offset + (i++)] = '_';
  j = 0;
  while (object_id[j] != 0 && i < kOwnerAndObjectLen) file_name_[filename_offset + (i++)] = object_id[j++];
  while (i < kOwnerAndObjectLen) file_name_[filename_offset + (i++)] = '_';
  filename_offset += kOwnerAndObjectLen;

  filename_n_position_ = filename_offset;

  snprintf(file_name_ + filename_offset, kNumberFillLen + 1, "%s", "000000");
  filename_offset += kNumberFillLen;

  char buf[30] = {};
  unsigned int random_number = 0;
  random_number |= ((rand() % 1024) << 21);
  random_number |= ((rand() % 1024) << 11);
  random_number |= (rand() % 2048);
  std::snprintf(file_name_ + filename_offset, kRandomLen + 1, "%08X", random_number);
  filename_offset += kRandomLen;

  std::snprintf(buf, sizeof(buf), "%p", this);
  auto object_len = std::strlen(buf);
  if (object_len >= kObjectPtrLen)
    std::memcpy(file_name_ + filename_offset, buf, kObjectPtrLen);
  else {
    std::strcpy(file_name_ + filename_offset + (kObjectPtrLen - object_len), buf);
    std::memset(file_name_ + filename_offset, '0', kObjectPtrLen - object_len);
  }
  filename_offset += kObjectPtrLen;

  std::snprintf(file_name_ + filename_offset, kSuffixLen + 1, "%s", ".tianmu_tmp");
  filename_offset += kSuffixLen;

  DEBUG_ASSERT(file_name_[filename_offset] == 0);
}

CacheableItem::~CacheableItem() {
  cur_file_handle_.Close();
  for (int i = 0; i <= max_file_id_; i++) {
    SetFilename(i);  // delete all files
    RemoveFile(file_name_);
  }
  delete[] file_name_;
}

void CacheableItem::CI_Put(int block, unsigned char *data, int size) {
  if (block == -1)
    return;
  if (size == -1)
    size = default_block_size_;
  if (size <= 0)
    return;
  for (int i = no_block_; i < block; i++) {  // rare case: the block numbering is not continuous
    // create empty blocks
    file_number_.push_back(-1);
    file_size_.push_back(0);
    file_start_.push_back(0);
  }

  try {
    if (block >= no_block_ || size != file_size_[block]) {
      // create a new block or reallocate the existing one
      if ((long long)(size) + (long long)(max_file_pos_) > 2000000000) {  // the file size limit: 2 GB
        // file becomes too large, start the next one!
        max_file_id_++;
        max_file_pos_ = 0;
      }
      if (block >= no_block_) {
        file_number_.push_back(max_file_id_);
        file_size_.push_back(size);
        file_start_.push_back(max_file_pos_);
        no_block_ = block + 1;
      } else {
        file_number_[block] = max_file_id_;
        file_size_[block] = size;
        file_start_[block] = max_file_pos_;
      }
      cur_file_handle_.Close();
      SetFilename(file_number_[block]);
      if (max_file_pos_ == 0)  // the new file
        cur_file_handle_.OpenCreateEmpty(file_name_);
      else
        cur_file_handle_.OpenReadWrite(file_name_);
      DEBUG_ASSERT(cur_file_handle_.IsOpen());
      cur_file_number_ = file_number_[block];
      max_file_pos_ += size;
    }
    // save the block
    if (file_number_[block] != cur_file_number_) {
      // open the block file
      cur_file_number_ = file_number_[block];
      cur_file_handle_.Close();
      SetFilename(cur_file_number_);
      cur_file_handle_.OpenReadWrite(file_name_);
      DEBUG_ASSERT(cur_file_handle_.IsOpen());
    }

#ifdef FUNCTIONS_EXECUTION_TIMES
    char str[100];
    if (file_size_[block] >= 1_MB)
      std::sprintf(str, "CacheableItem::CI_Put,write(%dMB)", (int)(file_size_[block] / 1_MB));
    else
      std::sprintf(str, "CacheableItem::CI_Put,write(%dKB)", (int)(file_size_[block] / 1_KB));
    FETOperator feto(str);
#endif
    cur_file_handle_.Seek(file_start_[block], SEEK_SET);
    cur_file_handle_.WriteExact((char *)data, file_size_[block]);
  } catch (common::DatabaseException &e) {
    throw common::OutOfMemoryException(e.what());
  }
}

int CacheableItem::CI_Get(int block, uchar *data, int size, int off) {
  if (block >= no_block_ || file_size_[block] <= 0 || (size >= 0 && off + size > file_size_[block]))
    return -1;

  try {
    // open file containing the block
    if (file_number_[block] != cur_file_number_) {
      // open the block file
      cur_file_number_ = file_number_[block];
      cur_file_handle_.Close();
      SetFilename(cur_file_number_);
      cur_file_handle_.OpenReadWrite(file_name_);
      DEBUG_ASSERT(cur_file_handle_.IsOpen());
    }

    // load the block
    if (size < 0) {
      size = file_size_[block];
      off = 0;
    }

#ifdef FUNCTIONS_EXECUTION_TIMES
    char str[100];
    if (size >= 1_MB)
      std::sprintf(str, "CacheableItem::CI_Get,read(%dMB)", (int)(size / 1_MB));
    else
      std::sprintf(str, "CacheableItem::CI_Get,read(%dKB)", (int)(size / 1_KB));
    FETOperator feto(str);
#endif

    cur_file_handle_.Seek(file_start_[block] + off, SEEK_SET);
    cur_file_handle_.Read((char *)data, size);
  } catch (common::DatabaseException &e) {
    throw common::OutOfMemoryException(e.what());
  }
  return 0;
}

void CacheableItem::SetFilename(int i)  // char -> void temporary change to enable compilation
{
  file_name_[filename_n_position_ + 5] = (char)('0' + i % 10);
  file_name_[filename_n_position_ + 4] = (char)('0' + (i / 10) % 10);
  file_name_[filename_n_position_ + 3] = (char)('0' + (i / 100) % 10);
  file_name_[filename_n_position_ + 2] = (char)('0' + (i / 1000) % 10);
  file_name_[filename_n_position_ + 1] = (char)('0' + (i / 10000) % 10);
  file_name_[filename_n_position_] = (char)('0' + (i / 100000) % 10);
}

}  // namespace system
}  // namespace Tianmu
