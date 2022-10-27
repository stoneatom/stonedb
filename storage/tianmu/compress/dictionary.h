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
#ifndef TIANMU_COMPRESS_DICTIONARY_H_
#define TIANMU_COMPRESS_DICTIONARY_H_
#pragma once

#include "common/assert.h"
#include "compress/defs.h"
#include "compress/range_code.h"

namespace Tianmu {
namespace compress {

template <typename T>
struct Dictionary_Helper {
  static constexpr size_t MAX_KEYS_ = 4096;
  static constexpr size_t N_BUCK_ = 65536;
};

template <>
struct Dictionary_Helper<uchar> {
  static constexpr size_t MAX_KEYS_ = 256;
  static constexpr size_t N_BUCK_ = 256;
};
template <>
struct Dictionary_Helper<ushort> {
  static constexpr size_t MAX_KEYS_ = 1024;
  static constexpr size_t N_BUCK_ = 65536;
};

// Data structure which holds a dictionary of numerical values used for
// compression and realises the mappings:
//   key --> range (using a hash map)
//   count --> key (using an array for every count)
// NBUCK usually should be a prime.
// MAXKEYS must be smaller than SHRT_MAX = 32767.
template <class T = uint64_t>
class Dictionary final {
 public:
  static constexpr uint MAX_TOTAL_ = RangeCoder::MAX_TOTAL_;
  static constexpr ushort MAX_KEYS_ = Dictionary_Helper<T>::MAX_KEYS_;

  struct KeyRange {
    T key;
    uint count;
    uint low;  // lows are set when all keys_ are inserted
  };

 private:
  KeyRange keys_[MAX_KEYS_];
  short n_keys_;

  // Hash table to index 'keys_' array according to the 'key' field
  short buckets_[Dictionary_Helper<T>::N_BUCK_];  // indices into 'keys_'; -1 means
                                                  // empty bucket
  short next_[MAX_KEYS_];                         // next_[k] is the next_ element in a bucket after key no.
                                                  // k, or -1
  KeyRange *order_[MAX_KEYS_];

  // For decompression
  short cnt2val_[MAX_TOTAL_];  // cnt2val_[c] is an index of the key for cumulative
                               // count 'c'
  uint tot_shift_;             // total = 1 << tot_shift_

  static int compare(const void *p1,
                     const void *p2);  // for sorting keys_ by descending 'count'
  bool compress_, decompress_;         // says if internal structures are set to perform
                                       // compression or decompression
  void Clear();
  uint hash(T key) { return (ushort)key; }

 public:
  Dictionary();
  ~Dictionary() = default;

  // Insert(): if 'key' is already in dictionary, increase its count by 'count'.
  // Otherwise insert the key and set count to 'count'.
  void InitInsert() { Clear(); }
  // returns false if too many keys_
  bool Insert(T key, uint count = 1) {
    uint b = hash(key);
    short k = buckets_[b];
    while ((k >= 0) && (keys_[k].key != key)) k = next_[k];

    if (k < 0) {
      if (n_keys_ >= MAX_KEYS_)
        return false;
      keys_[n_keys_].key = key;
      keys_[n_keys_].count = count;
      next_[n_keys_] = buckets_[b];  // TODO: time - insert new keys_ at the END of the list
      buckets_[b] = n_keys_++;
    } else
      keys_[k].count += count;
    return true;
  }

  KeyRange *GetKeys(short &n) {
    n = n_keys_;
    return keys_;
  }
  void SetLows();  // set lows/highs of keys_

  void Save(RangeCoder *dest,
            T maxkey);  // maxkey - the largest key or something bigger
  void Load(RangeCoder *src,
            T maxkey);  // maxkey must be the same as for Save()

  // returns true when ESC was encoded ('key' is not in dictionary)
  bool Encode(RangeCoder *dest, T key) {
    DEBUG_ASSERT(compress_);

    // find the 'key' in the hash
    uint b = hash(key);
    short k = buckets_[b];
    while ((k >= 0) && (keys_[k].key != key)) k = next_[k];
    DEBUG_ASSERT(k >= 0);  // TODO: handle ESC encoding

    dest->EncodeShift(keys_[k].low, keys_[k].count, tot_shift_);
    return false;
  }
  bool Decode(RangeCoder *src, T &key) {
    DEBUG_ASSERT(decompress_);
    uint count = src->GetCountShift(tot_shift_);
    short k = cnt2val_[count];  // TODO: handle ESC decoding
    key = keys_[k].key;
    src->DecodeShift(keys_[k].low, keys_[k].count, tot_shift_);
    return false;
  }
};

template <>
inline uint Dictionary<uchar>::hash(uchar key) {
  return key;
}

}  // namespace compress
}  // namespace Tianmu

#endif  // TIANMU_COMPRESS_DICTIONARY_H_
