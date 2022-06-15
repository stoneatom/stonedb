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

#include "part_dict.h"

#include <cstring>

#include "core/quick_math.h"
#include "util/qsort.h"

namespace stonedb {
namespace compress {

// The smallest no. of bits of x which must be shifted right to make x <= limit
static inline uint GetShift(uint x, uint limit) {
  if (x <= limit) return 0;
  uint shift = core::GetBitLen(x) - core::GetBitLen(limit);
  if ((x _SHR_ shift) > limit) shift++;
  return shift;
}

template <class T>
void PartDict<T>::Create(DataSet<T> *dataset) {
  T *data = dataset->data;
  uint len = dataset->nrec;
  ASSERT(len <= MAXLEN, "should be 'len <= MAXLEN'");

  // put the data into hash table - to count no. of occurences of each value
  nfreq = 0;
  hash.Clear();
  for (uint i = 0; i < len; i++) hash.insert(data[i]);

  for (int k = 0; k < hash.nkeys; k++)
    if (hash.keys[k].count >= MINOCCUR) freqkey[nfreq++] = &hash.keys[k];

  // sort the array of pointers to frequent values in descending order
  if (nfreq > 0) qsort_ib(freqkey, nfreq, sizeof(*freqkey), compare);

  if (nfreq > MAXTOTAL / 2)
    nfreq = MAXTOTAL / 2;  // there is no sense to hold big dictionary with all
                           // counts small and equal
  ASSERT(nfreq <= MAXFREQ, "should be 'nfreq <= MAXFREQ'");

  // compute lows and highs
  if (len > MAXTOTAL) {
    // how much the counts must be shifted to fit into 'maxtotal'
    uint shift = GetShift(len, MAXTOTAL - nfreq - 1);
    uint low = 0, scount;
    uint sumcnt = 0;
    for (uint f = 0; f < nfreq; f++) {
      scount = freqkey[f]->count _SHR_ shift;
      if (scount == 0) scount = 1;
      freqkey[f]->low = low;
      freqkey[f]->count = scount;
      low += scount;
      sumcnt += freqkey[f]->count;
    }
    ASSERT(sumcnt <= len, "should be 'sumcnt <= len'");

    esc_count = len - sumcnt;
    scount = esc_count _SHR_ shift;
    if (((esc_count > 0) && (scount == 0)) || (sumcnt == 0)) scount = 1;
    esc_low = low;
    esc_high = low + scount;
    ASSERT(esc_high <= MAXTOTAL, "should be 'esc_high <= MAXTOTAL'");
  } else {
    uint sumcnt = 0;
    for (uint f = 0; f < nfreq; f++) {
      freqkey[f]->low = sumcnt;
      sumcnt += freqkey[f]->count;
      // freqkey[f]->high = (sumcnt += freqkey[f]->count);
    }
    ASSERT(sumcnt <= len, "should be 'sumcnt <= len'");
    esc_count = len - sumcnt;
    esc_low = sumcnt;
    esc_high = len;
  }

  // TODO CAUTION: difference between 2^n and real total is put fully to ESC
  // count
  // -- this may lead to much worse compression for partially filled packs (e.g.
  // with nulls)
  esc_high = 1u _SHL_ core::GetBitLen(esc_high - 1);
  esc_usecnt = esc_high - esc_low;
  ASSERT(esc_high <= MAXTOTAL, "should be 'esc_high <= MAXTOTAL'");
}

template <class T>
int PartDict<T>::compare(const void *p1, const void *p2) {
  using AK = struct PartDict<T>::HashTab::AKey;
  if (((*(AK **)p1)->count) < ((*(AK **)p2)->count))
    return 1;
  else if (((*(AK **)p1)->count) > ((*(AK **)p2)->count))
    return -1;
  else
    return 0;
}

template <class T>
void PartDict<T>::Save(RangeCoder *coder, T maxval) {
  // save no. of frequent values
  coder->EncodeUniform(nfreq, MAXFREQ);

  // save frequent values
  uint bitmax = core::GetBitLen(maxval);
  uint c, prevc = MAXTOTAL - 1;
  for (uint f = 0; f < nfreq; f++) {
    // save frequent value and its short count-1 (which is not greater than the
    // previous count-1)
    coder->EncodeUniform(freqkey[f]->key, maxval, bitmax);
    coder->EncodeUniform(c = freqkey[f]->count - 1, prevc);
    prevc = c;
  }

  coder->EncodeUniform(esc_high, MAXTOTAL);
}

template <class T>
void PartDict<T>::Load(RangeCoder *coder, T maxval) {
  // load no. of frequent values
  coder->DecodeUniform(nfreq, MAXFREQ);

  // load frequent values, their 'lows' and 'highs'; fill 'cnt2val' array
  uint bitmax = core::GetBitLen(maxval);
  uint c, prevc = MAXTOTAL - 1;
  uint low = 0;
  for (ushort f = 0; f < nfreq; f++) {
    coder->DecodeUniform(freqval[f].val, maxval, bitmax);
    coder->DecodeUniform(c, prevc);
    prevc = c++;
    freqval[f].low = low;
    freqval[f].count = c;
    if (low + c > MAXTOTAL) throw CprsErr::CPRS_ERR_COR;
    for (; c > 0; c--) cnt2val[low++] = f;
  }

  // load range of ESC
  esc_low = low;
  coder->DecodeUniform(esc_high, MAXTOTAL);
  if (esc_low > esc_high) throw CprsErr::CPRS_ERR_COR;
  esc_usecnt = esc_high - esc_low;
}

template <class T>
uint PartDict<T>::Predict(DataSet<T> *ds) {
  core::QuickMath math;
  double x = 0;
  double logmax = math.log2(ds->maxval + 1.0);  // no. of bits to encode a value uniformly

  // dictionary:  nfreq + vals_and_counts + esc_high
  x += 16 + nfreq * (logmax + math.log2(MAXTOTAL) - math.log2(nfreq)) + 16;

  // encoding of data:  frequent values + ESC + uniform model for rare values
  double data = ds->nrec * math.log2(esc_high);
  if (esc_count > 0) data -= esc_count * (math.log2(esc_usecnt) - logmax);
  ASSERT(MAXTOTAL >= ds->nrec, "should be 'MAXTOTAL >= ds->nrec'");
  for (uint f = 0; f < nfreq; f++) data -= math.nlog2n(freqkey[f]->count);
  // data -= freqkey[f]->count * math.log2(freqkey[f]->high - freqkey[f]->low);

  x += data;
  return (uint)x + 20;  // cost of the use of arithmetic coder
}

template <class T>
bool PartDict<T>::Encode(RangeCoder *coder, DataSet<T> *dataset) {
  // build dictionary
  Create(dataset);
  if (Predict(dataset) > 0.98 * this->PredictUni(dataset)) return false;

  // save version of this routine, using 3 bits (7 = 2^3-1)
  coder->EncodeUniform((uchar)0, (uchar)7);

  // save dictionary
  Save(coder, dataset->maxval);

  // encode data
  T *data = dataset->data;
  uint len = dataset->nrec;
  uint low, cnt;
  bool esc;
  uint shift = core::GetBitLen(esc_high - 1);

  lenrest = 0;
  for (uint i = 0; i < len; i++) {
    esc = GetRange(data[i], low, cnt);  // encode frequent symbol or ESC
    coder->EncodeShift(low, cnt, shift);
    if (esc) {  // ESC -> encode rare symbol
      data[lenrest++] = data[i];
    }
  }
  dataset->nrec = lenrest;
  // maxval is not changed
  return true;
}

template <class T>
void PartDict<T>::Decode(RangeCoder *coder, DataSet<T> *dataset) {
  // read version
  uchar ver;
  coder->DecodeUniform(ver, (uchar)7);
  if (ver > 0) throw CprsErr::CPRS_ERR_COR;

  // load dictionary
  Load(coder, dataset->maxval);

  // decode data
  uint len = dataset->nrec;
  uint c, low, count;
  uint shift = core::GetBitLen(esc_high - 1);

  lenall = len;
  lenrest = 0;
  for (uint i = 0; i < len; i++) {
    c = coder->GetCountShift(shift);
    isesc[i] = GetVal(c, decoded[i], low, count);
    if (isesc[i]) lenrest++;
    coder->DecodeShift(low, count, shift);
  }
  dataset->nrec = lenrest;
}

template <class T>
void PartDict<T>::Merge(DataSet<T> *dataset) {
  T *data = dataset->data;
  ASSERT(dataset->nrec == lenrest, "should be 'dataset->nrec == lenrest'");
  uint i = lenall;
  while (i-- > 0)
    if (isesc[i])
      data[i] = data[--lenrest];
    else
      data[i] = decoded[i];
  ASSERT(lenrest == 0, "should be 'lenrest == 0'");
  dataset->nrec = lenall;
}

TEMPLATE_CLS(PartDict)

}  // namespace compress
}  // namespace stonedb
