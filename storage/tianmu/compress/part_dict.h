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
#ifndef TIANMU_COMPRESS_PART_DICT_H_
#define TIANMU_COMPRESS_PART_DICT_H_
#pragma once

#include <cstring>

#include "common/assert.h"
#include "compress/data_filt.h"
#include "compress/defs.h"
#include "compress/range_code.h"

namespace Tianmu {
namespace compress {

template <class T>
struct PartDict_Helper {
  static constexpr size_t nbuck = 0x20000;
  static constexpr unsigned int mask = 0x20000 - 1;
};

template <>
struct PartDict_Helper<unsigned short> {
  static constexpr size_t nbuck = 65536;
  static constexpr unsigned int mask = 65536 - 1;
};

template <>
struct PartDict_Helper<unsigned char> {
  static constexpr size_t nbuck = 256;
  static constexpr unsigned int mask = 256 - 1;
};

// Partial Dictionary model of distribution of uint64_t values
// Only the most frequent values are stored in the dictionary, so as to optimize
// the length of compressed data plus dictionary.
template <class T>
class PartDict : public DataFilt<T> {
 public:
  static const uint MAXLEN = DICMAP_MAX;
  static const uint MAXTOTAL = RangeCoder::MAX_TOTAL_;

  static const uint MINOCCUR = 4;                          // how many times a value must occur to be frequent value
  static const uint MAXFREQ = (MAXTOTAL + 20) / MINOCCUR;  // max no. of frequent values = max
                                                           // size of dictionary; must be smaller
                                                           // than USHRT_MAX
 private:
  struct HashTab {
    static constexpr uint topbit = 15;
    static constexpr uint mask = PartDict_Helper<T>::mask;
    struct AKey {
      T key;
      uint count;  // no. of occurences of the 'key'; 0 for empty bucket
      uint low;    // low=(uint)-1  when the key is outside dictionary (ESC)
      int next;    // next element in the bucket or -1
    };

    AKey keys[MAXLEN];
    int buckets[PartDict_Helper<T>::nbuck];  // indices into 'keys'; -1 means
                                             // empty bucket
    int nkeys;                               // no. of elements (keys) currently in the table 'keys'
    int nusebuck;                            // no. of currently used buckets (just for statistics)

    // hash function
    uint fun(T key);
    void insert(T key) {
      uint b = fun(key);
      int k = buckets[b];
      if (k < 0)
        nusebuck++;
      else
        while ((k >= 0) && (keys[k].key != key)) k = keys[k].next;

      if (k < 0) {
        ASSERT(nkeys < (int)PartDict::MAXLEN, "should be 'nkeys < PartDict::MAXLEN'");
        AKey &ak = keys[nkeys];
        ak.key = key;
        ak.count = 1;
        ak.low = (uint)-1;
        ak.next = buckets[b];  // TODO: time - insert new keys at the END of the list
        buckets[b] = nkeys++;
      } else
        keys[k].count++;
    }
    // returns index of 'key' in 'keys' or -1
    int find(T key) {
      uint b = fun(key);
      int k = buckets[b];
      while ((k >= 0) && (keys[k].key != key)) k = keys[k].next;
      return k;
    }

    void Clear() {
      nkeys = nusebuck = 0;
      std::memset(buckets, -1, sizeof(buckets));
    }

    HashTab() { Clear(); }
  };

  // structures for compression
  HashTab hash;
  typename HashTab::AKey *freqkey[MAXLEN];  // pointers to frequent values (occuring more than once)
                                            // in array 'hash.keys'; sorted in decending order
  uint nfreq;                               // current size of 'freqkey'; = size of dictionary
  static int compare(const void *p1,
                     const void *p2);  // for sorting of 'freqkey' array

  // for decompression
  struct ValRange {
    T val;
    uint low, count;
  };
  ValRange freqval[MAXFREQ];  // 8K * 16B = 128 KB
  ushort cnt2val[MAXTOTAL];   // cnt2val[c] is an index (into freqval) of
                              // frequent value for count 'c'

  // for merging data during decompression
  T decoded[MAXLEN];
  bool isesc[MAXLEN];
  uint lenall, lenrest;

  uint esc_count, esc_usecnt, esc_low, esc_high;

  void Clear();

  // returns true when ESC (then low and count are of ESC)
  bool GetRange(T val, uint &low, uint &count) {
    int k = hash.find(val);
    // OK under assumption that PartDict is built on the whole data to be
    // compressed
    ASSERT(k >= 0, "should be 'k >= 0'");
    low = hash.keys[k].low;
    if (low == (uint)-1) {  // ESCape
      low = esc_low;
      count = esc_usecnt;
      return true;
    }
    count = hash.keys[k].count;
    return false;
  }
  // returns true when ESC (then low and count are of ESC)
  bool GetVal(uint c, T &val, uint &low, uint &count) {
    if (c >= esc_low) {  // ESCape
      low = esc_low;
      count = esc_usecnt;
      return true;
    }
    ValRange &vr = freqval[cnt2val[c]];
    val = vr.val;
    low = vr.low;
    count = vr.count;
    return false;
  }

  void Create(DataSet<T> *dataset);

  // prediction of compressed data size, in BITS (rare symbols are assumed to be
  // encoded uniformly)
  uint Predict(DataSet<T> *ds);

  void Save(RangeCoder *coder,
            T maxval);  // saves frequent values and their ranges
  void Load(RangeCoder *coder, T maxval);

 public:
  PartDict() = default;
  virtual ~PartDict() = default;
  char const *GetName() override { return "dict"; }
  bool Encode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Decode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Merge(DataSet<T> *dataset) override;
};

template <>
inline unsigned int PartDict<unsigned long>::HashTab::fun(unsigned long key) {
  uint x = ((uint)key ^ *((uint *)&key + 1));
  ASSERT(topbit < sizeof(uint) * 8, "should be 'topbit < sizeof(uint)*8'");
  return (x & mask) ^ (x >> topbit);
}
template <>
inline unsigned int PartDict<unsigned int>::HashTab::fun(unsigned int key) {
  ASSERT(topbit < sizeof(uint) * 8, "should be 'topbit < sizeof(uint)*8'");
  return (key & mask) ^ (key >> topbit);
}
template <>
inline unsigned int PartDict<unsigned short>::HashTab::fun(unsigned short key) {
  return key;
}
template <>
inline unsigned int PartDict<unsigned char>::HashTab::fun(unsigned char key) {
  return key;
}

}  // namespace compress
}  // namespace Tianmu

#endif  // TIANMU_COMPRESS_PART_DICT_H_
