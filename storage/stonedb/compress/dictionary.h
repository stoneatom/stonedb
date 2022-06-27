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
#ifndef STONEDB_COMPRESS_DICTIONARY_H_
#define STONEDB_COMPRESS_DICTIONARY_H_
#pragma once

#include "common/assert.h"
#include "compress/defs.h"
#include "compress/range_code.h"

namespace stonedb {
namespace compress {

template <typename T>
struct Dictionary_Helper {
  static constexpr size_t MAXKEYS = 4096;
  static constexpr size_t NBUCK = 65536;
};

template <>
struct Dictionary_Helper<uchar> {
  static constexpr size_t MAXKEYS = 256;
  static constexpr size_t NBUCK = 256;
};
template <>
struct Dictionary_Helper<ushort> {
  static constexpr size_t MAXKEYS = 1024;
  static constexpr size_t NBUCK = 65536;
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
  static constexpr uint MAXTOTAL = RangeCoder::MAX_TOTAL;
  static constexpr ushort MAXKEYS = Dictionary_Helper<T>::MAXKEYS;

  struct KeyRange {
    T key;
    uint count;
    uint low;  // lows are set when all keys are inserted
  };

 private:
  KeyRange keys[MAXKEYS];
  short nkeys;

  // Hash table to index 'keys' array according to the 'key' field
  short buckets[Dictionary_Helper<T>::NBUCK];  // indices into 'keys'; -1 means
                                               // empty bucket
  short next[MAXKEYS];                         // next[k] is the next element in a bucket after key no.
                                               // k, or -1
  KeyRange *order[MAXKEYS];

  // For decompression
  short cnt2val[MAXTOTAL];  // cnt2val[c] is an index of the key for cumulative
                            // count 'c'
  uint tot_shift;           // total = 1 << tot_shift

  static int compare(const void *p1,
                     const void *p2);  // for sorting keys by descending 'count'
  bool compress, decompress;           // says if internal structures are set to perform
                                       // compression or decompression
  void Clear();
  uint hash(T key) { return (ushort)key; }

 public:
  Dictionary();
  ~Dictionary() = default;

  // Insert(): if 'key' is already in dictionary, increase its count by 'count'.
  // Otherwise insert the key and set count to 'count'.
  void InitInsert() { Clear(); }
  // returns false if too many keys
  bool Insert(T key, uint count = 1) {
    uint b = hash(key);
    short k = buckets[b];
    while ((k >= 0) && (keys[k].key != key)) k = next[k];

    if (k < 0) {
      if (nkeys >= MAXKEYS) return false;
      keys[nkeys].key = key;
      keys[nkeys].count = count;
      next[nkeys] = buckets[b];  // TODO: time - insert new keys at the END of the list
      buckets[b] = nkeys++;
    } else
      keys[k].count += count;
    return true;
  }

  KeyRange *GetKeys(short &n) {
    n = nkeys;
    return keys;
  }
  void SetLows();  // set lows/highs of keys

  void Save(RangeCoder *dest,
            T maxkey);  // maxkey - the largest key or something bigger
  void Load(RangeCoder *src,
            T maxkey);  // maxkey must be the same as for Save()

  // returns true when ESC was encoded ('key' is not in dictionary)
  bool Encode(RangeCoder *dest, T key) {
    DEBUG_ASSERT(compress);

    // find the 'key' in the hash
    uint b = hash(key);
    short k = buckets[b];
    while ((k >= 0) && (keys[k].key != key)) k = next[k];
    DEBUG_ASSERT(k >= 0);  // TODO: handle ESC encoding

    dest->EncodeShift(keys[k].low, keys[k].count, tot_shift);
    return false;
  }
  bool Decode(RangeCoder *src, T &key) {
    DEBUG_ASSERT(decompress);
    uint count = src->GetCountShift(tot_shift);
    short k = cnt2val[count];  // TODO: handle ESC decoding
    key = keys[k].key;
    src->DecodeShift(keys[k].low, keys[k].count, tot_shift);
    return false;
  }
};

template <>
inline uint Dictionary<uchar>::hash(uchar key) {
  return key;
}

}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_DICTIONARY_H_
