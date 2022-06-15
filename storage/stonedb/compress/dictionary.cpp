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

#include "dictionary.h"

#include <cstring>

#include "core/bin_tools.h"
#include "util/qsort.h"

namespace stonedb {
namespace compress {

template <class T>
void Dictionary<T>::Clear() {
  static_assert(MAXTOTAL > MAXKEYS + 1, "should be 'MAXTOTAL > MAXKEYS+1'");
  nkeys = 0;
  std::memset(buckets, -1, sizeof(buckets));
  compress = decompress = false;
}

template <class T>
Dictionary<T>::Dictionary() {
  static_assert(MAXKEYS < SHRT_MAX, "should be 'MAXKEYS < SHRT_MAX'");
  Clear();
}

//-------------------------------------------------------------------------------------

template <class T>
void Dictionary<T>::SetLows() {
  // sort keys by descending 'count'
  uint sumcnt = 0, total = 0;
  for (short i = 0; i < nkeys; i++) {
    order[i] = &keys[i];
    sumcnt += keys[i].count;
    DEBUG_ASSERT(keys[i].count > 0);
  }
  qsort_ib(order, nkeys, sizeof(*order), compare);

  ASSERT(sumcnt <= MAXTOTAL, "should be 'sumcnt <= MAXTOTAL'");
  // set short counts
  //	if(sumcnt > MAXTOTAL) {
  //		DEBUG_ASSERT(0);
  //		uint shift = GetShift(sumcnt, MAXTOTAL - nkeys);
  //		for(short i = 0; i < nkeys; i++) {
  //			if((order[i]->count _SHR_ASSIGN_ shift) == 0)
  // order[i]->count = 1; 			total += order[i]->count;
  //		}
  //	}
  //	else total = sumcnt;

  total = sumcnt;

  tot_shift = core::GetBitLen(total - 1);
  ASSERT((total <= MAXTOTAL) && (total > 0) && (1u _SHL_ tot_shift) >= total,
         "should be '(total <= MAXTOTAL) && (total > 0) && (1u _SHL_ "
         "tot_shift) >= total'");

  // correct counts to sum up to power of 2; set lows
  uint high = (1u _SHL_ tot_shift), rest = high - total, d;
  for (short i = nkeys; i > 0;) {
    rest -= (d = rest / i--);
    order[i]->low = (high -= (order[i]->count += d));
  }
  ASSERT(high == 0, "should be 'high == 0'");

  compress = true;
}

template <class T>
int Dictionary<T>::compare(const void *p1, const void *p2) {
  using KR = KeyRange;
  if (((*(KR **)p1)->count) < ((*(KR **)p2)->count))
    return 1;
  else if (((*(KR **)p1)->count) > ((*(KR **)p2)->count))
    return -1;
  else
    return 0;
}

template <class T>
void Dictionary<T>::Save(RangeCoder *dest, T maxkey) {
  ASSERT(compress, "'compress' should be true");

  // save no. of keys
  dest->EncodeUniform(nkeys, (short)MAXKEYS);

  uint bitmax = core::GetBitLen(maxkey);
  uint c, prevc = MAXTOTAL - 1;
  for (short i = 0; i < nkeys; i++) {
    // save the key and its short count-1 (which is not greater than the
    // previous count-1)
    dest->EncodeUniform(order[i]->key, maxkey, bitmax);
    dest->EncodeUniform(c = order[i]->count - 1, prevc);
    prevc = c;
  }
}

template <class T>
void Dictionary<T>::Load(RangeCoder *src, T maxkey) {
  compress = decompress = false;

  // load no. of keys
  src->DecodeUniform(nkeys, (short)MAXKEYS);

  // load keys, their 'lows' and 'highs'; fill 'cnt2val' array
  uint bitmax = core::GetBitLen(maxkey);
  uint c, prevc = MAXTOTAL - 1;
  uint total = 0;
  for (short i = 0; i < nkeys; i++) {
    src->DecodeUniform(keys[i].key, maxkey, bitmax);
    src->DecodeUniform(c, prevc);
    prevc = c++;
    keys[i].count = c;
    keys[i].low = total;
    if (total + c > MAXTOTAL) throw CprsErr::CPRS_ERR_COR;
    for (; c > 0; c--) cnt2val[total++] = i;
  }

  tot_shift = core::GetBitLen(total - 1);
  ASSERT((total <= MAXTOTAL) && (total > 0) && (1u _SHL_ tot_shift) >= total,
         "should be '(total <= MAXTOTAL) && (total > 0) && (1u _SHL_ "
         "tot_shift) >= total'");

  decompress = true;
}

template class Dictionary<uchar>;
template class Dictionary<ushort>;
template class Dictionary<uint>;
template class Dictionary<uint64_t>;

}  // namespace compress
}  // namespace stonedb
