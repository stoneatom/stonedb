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

namespace Tianmu {
namespace compress {

template <class T>
void Dictionary<T>::Clear() {
  static_assert(MAX_TOTAL_ > MAX_KEYS_ + 1, "should be 'MAX_TOTAL_ > MAX_KEYS_+1'");
  n_keys_ = 0;
  std::memset(buckets_, -1, sizeof(buckets_));
  compress_ = decompress_ = false;
}

template <class T>
Dictionary<T>::Dictionary() {
  static_assert(MAX_KEYS_ < SHRT_MAX, "should be 'MAX_KEYS_ < SHRT_MAX'");
  Clear();
}

//-------------------------------------------------------------------------------------

template <class T>
void Dictionary<T>::SetLows() {
  // sort keys_ by descending 'count'
  uint sumcnt = 0, total = 0;
  for (short i = 0; i < n_keys_; i++) {
    order_[i] = &keys_[i];
    sumcnt += keys_[i].count;
    DEBUG_ASSERT(keys_[i].count > 0);
  }
  qsort_tianmu(order_, n_keys_, sizeof(*order_), compare);

  ASSERT(sumcnt <= MAX_TOTAL_, "should be 'sumcnt <= MAX_TOTAL_'");
  // set short counts
  //	if(sumcnt > MAX_TOTAL_) {
  //		DEBUG_ASSERT(0);
  //		uint shift = GetShift(sumcnt, MAX_TOTAL_ - n_keys_);
  //		for(short i = 0; i < n_keys_; i++) {
  //			if((order_[i]->count _SHR_ASSIGN_ shift) == 0)
  // order_[i]->count = 1; 			total += order_[i]->count;
  //		}
  //	}
  //	else total = sumcnt;

  total = sumcnt;

  tot_shift_ = core::GetBitLen(total - 1);
  ASSERT((total <= MAX_TOTAL_) && (total > 0) && (1u _SHL_ tot_shift_) >= total,
         "should be '(total <= MAX_TOTAL_) && (total > 0) && (1u _SHL_ "
         "tot_shift_) >= total'");

  // correct counts to sum up to power of 2; set lows
  uint high = (1u _SHL_ tot_shift_), rest = high - total, d;
  for (short i = n_keys_; i > 0;) {
    rest -= (d = rest / i--);
    order_[i]->low = (high -= (order_[i]->count += d));
  }
  ASSERT(high == 0, "should be 'high == 0'");

  compress_ = true;
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
  ASSERT(compress_, "'compress_' should be true");

  // save no. of keys_
  dest->EncodeUniform(n_keys_, (short)MAX_KEYS_);

  uint bitmax = core::GetBitLen(maxkey);
  uint c, prevc = MAX_TOTAL_ - 1;
  for (short i = 0; i < n_keys_; i++) {
    // save the key and its short count-1 (which is not greater than the
    // previous count-1)
    dest->EncodeUniform(order_[i]->key, maxkey, bitmax);
    dest->EncodeUniform(c = order_[i]->count - 1, prevc);
    prevc = c;
  }
}

template <class T>
void Dictionary<T>::Load(RangeCoder *src, T maxkey) {
  compress_ = decompress_ = false;

  // load no. of keys_
  src->DecodeUniform(n_keys_, (short)MAX_KEYS_);

  // load keys_, their 'lows' and 'highs'; fill 'cnt2val_' array
  uint bitmax = core::GetBitLen(maxkey);
  uint c, prevc = MAX_TOTAL_ - 1;
  uint total = 0;
  for (short i = 0; i < n_keys_; i++) {
    src->DecodeUniform(keys_[i].key, maxkey, bitmax);
    src->DecodeUniform(c, prevc);
    prevc = c++;
    keys_[i].count = c;
    keys_[i].low = total;
    if (total + c > MAX_TOTAL_)
      throw CprsErr::CPRS_ERR_COR;
    for (; c > 0; c--) cnt2val_[total++] = i;
  }

  tot_shift_ = core::GetBitLen(total - 1);
  ASSERT((total <= MAX_TOTAL_) && (total > 0) && (1u _SHL_ tot_shift_) >= total,
         "should be '(total <= MAX_TOTAL_) && (total > 0) && (1u _SHL_ "
         "tot_shift_) >= total'");

  decompress_ = true;
}

template class Dictionary<uchar>;
template class Dictionary<ushort>;
template class Dictionary<uint>;
template class Dictionary<uint64_t>;

}  // namespace compress
}  // namespace Tianmu
