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

#include "top_bit_dict.h"

#include "common/assert.h"
#include "core/quick_math.h"

namespace Tianmu {
namespace compress {

template <class T>
const double TopBitDict<T>::MIN_PREDICT_ = 0.97;

template <class T>
uint TopBitDict<T>::FindOptimum(DataSet<T> *dataset, uint nbit, uint &opt_bit, Dictionary<T> *&opt_dict) {
  if (nbit <= 2)
    return INF_;

  T *data = dataset->data;
  uint nrec = dataset->nrec;
  T maxval = dataset->maxval;

  core::QuickMath math;
  opt_bit = 0;
  opt_dict = &counters_[0];
  double uni_pred = nrec * math.log2(maxval + 1.0);
  double opt_pred = uni_pred;
  double min_pred = MIN_PREDICT_ * uni_pred;
  double max_pred = 0.99 * uni_pred;

  uint bitstart = (nbit / 2 < BIT_START_) ? nbit / 2 : BIT_START_;
  uint bit = bitstart;
  Dictionary<T> *dict = &counters_[1];
  double pred;
  uint maxkey = 1u _SHL_ bitstart;  // maximum no. of keys in the next loop
  uint skiprec, opt_skip = 0;
  // uint skiprec = (nrec >> bitstart) / KEY_OCCUR_;	// when skipping, each
  // possible key occurs KEY_OCCUR_ times on avg.

  while (bit <= nbit) {
    skiprec = nrec / (maxkey * KEY_OCCUR_);
    if (!skiprec)
      skiprec = 1;

    // insert truncated bits to dictionary (counter)
    if (!Insert(dict, data, nbit, bit, nrec, skiprec))
      break;

    // make prediction
    DEBUG_ASSERT(nrec <= MAX_TOTAL_);
    double uplen = 0.0;
    short nkey;
    auto keys = dict->GetKeys(nkey);
    for (short i = 0; i < nkey; i++) uplen -= math.nlog2n(keys[i].count);
    if (skiprec > 1)
      uplen = skiprec * uplen - nrec * math.log2(skiprec);
    uplen += math.nlog2n(nrec);
    double cntlen = math.log2(MAX_TOTAL_) - math.log2((uint)nkey);
    pred = (nbit - bit) * nrec       // bits encoded uniformly
           + uplen                   // bits encoded with dictionary
           + (bit + cntlen) * nkey;  // dictionary

    if ((pred > max_pred) || (pred > 1.03 * opt_pred))
      break;
    if (pred < opt_pred) {
      // if((skiprec > 1) && (pred < min_pred)) {
      //	skiprec = 1;			// recalculate dictionary
      // without skipping 	continue;
      //}
      opt_pred = pred;
      opt_bit = bit;
      opt_skip = skiprec;
      if (opt_skip == 1) {
        Dictionary<T> *tmp = dict;
        dict = opt_dict;
        opt_dict = tmp;
      }
    }
    // skiprec >>= BIT_STEP_;
    // if(!skiprec) skiprec = 1;
    // if((skiprec == 1) && (opt_pred > min_pred)) break;
    bit += BIT_STEP_;
    maxkey = (uint)nkey << BIT_STEP_;  // upper bound for the no. of keys in the next loop
  }

  if (!opt_bit || (opt_pred >= min_pred))
    return INF_;
  if (opt_skip > 1) {
    bool ok = Insert(opt_dict, data, nbit, opt_bit, nrec, 1);
    // DEBUG_ASSERT(ok);
    if (ok == false)
      return INF_;
    // don't recalculate prediction
  }
  return (uint)opt_pred + 1;
}

template <class T>
inline bool TopBitDict<T>::Insert(Dictionary<T> *dict, T *data, uint nbit, uint bit, uint nrec, uint skiprec) {
  dict->InitInsert();
  if (top_bottom_ == TopBottom::tbTop) {  // top bits
    uchar bitlow = (uchar)(nbit - bit);
    DEBUG_ASSERT(bitlow < sizeof(T) * 8);
    for (uint i = 0; i < nrec; i += skiprec)
      if (!dict->Insert(data[i] >> bitlow))
        return false;
  } else {  // low bits
    T mask = (T)1 _SHL_(uchar) bit;
    mask--;
    for (uint i = 0; i < nrec; i += skiprec)
      if (!dict->Insert(data[i] & mask))
        return false;
  }
  return true;
}

template <class T>
bool TopBitDict<T>::Encode(RangeCoder *coder, DataSet<T> *dataset) {
  T maxval = dataset->maxval;
  if (maxval == 0)
    return false;

  // find optimum dictionary
  Dictionary<T> *dict;
  uint nbit = core::GetBitLen(maxval), bitdict;
  if (FindOptimum(dataset, nbit, bitdict, dict) >= 0.98 * this->PredictUni(dataset))
    return false;
  DEBUG_ASSERT(bitdict);

  dict->SetLows();

  // save version of this routine, using 3 bits (7 = 2^3-1)
  IFSTAT(uint pos0 = coder->GetPos());
  coder->EncodeUniform((uchar)0, (uchar)7);

  // save no. of lower bits
  bitlow_ = (top_bottom_ == TopBottom::tbTop) ? (T)(nbit - bitdict) : (T)bitdict;
  coder->EncodeUniform(bitlow_, (T)64);

  // save dictionary
  DEBUG_ASSERT(bitlow_ < sizeof(maxval) * 8);
  T maxhigh = maxval >> bitlow_, maxlow = ((T)1 _SHL_ bitlow_) - (T)1;
  T dictmax = (top_bottom_ == TopBottom::tbTop) ? maxhigh : maxlow;
  dict->Save(coder, dictmax);

  IFSTAT(uint pos1 = coder->GetPos());
  IFSTAT(codesize_[0] = pos1 - pos0);
  T *data = dataset->data;
  uint nrec = dataset->nrec;
  bool esc;

  // encode data
  DEBUG_ASSERT(bitlow_ < sizeof(T) * 8);
  if (top_bottom_ == TopBottom::tbTop)
    for (uint i = 0; i < nrec; i++) {
      esc = dict->Encode(coder, data[i] >> bitlow_);
      ASSERT(!esc, "TOP encode failed");
      data[i] &= maxlow;
    }
  else
    for (uint i = 0; i < nrec; i++) {
      esc = dict->Encode(coder, data[i] & maxlow);
      ASSERT(!esc, "BOTTOM encode failed");
      data[i] >>= bitlow_;
    }

  IFSTAT(codesize_[1] = coder->GetPos() - pos1);
  dataset->maxval = (top_bottom_ == TopBottom::tbTop) ? maxlow : maxhigh;
  return true;
}

template <class T>
void TopBitDict<T>::Decode(RangeCoder *coder, DataSet<T> *dataset) {
  // read version
  uchar ver;
  coder->DecodeUniform(ver, (uchar)7);
  if (ver > 0)
    throw CprsErr::CPRS_ERR_COR;

  // read no. of lower bits
  coder->DecodeUniform(bitlow_, (T)64);

  // load dictionary
  Dictionary<T> *dict = counters_;
  DEBUG_ASSERT(bitlow_ < sizeof(dataset->maxval) * 8);
  T maxhigh = dataset->maxval >> bitlow_, maxlow = ((T)1 _SHL_ bitlow_) - (T)1;
  T dictmax = (top_bottom_ == TopBottom::tbTop) ? maxhigh : maxlow;
  dict->Load(coder, dictmax);

  // decode data
  bool esc;
  uint nrec = dataset->nrec;
  for (uint i = 0; i < nrec; i++) {
    esc = dict->Decode(coder, decoded_[i]);
    ASSERT(!esc, "decode failed");
  }

  maxval_merge_ = dataset->maxval;
  dataset->maxval = (top_bottom_ == TopBottom::tbTop) ? maxlow : maxhigh;
}

template <class T>
void TopBitDict<T>::Merge(DataSet<T> *dataset) {
  T *data = dataset->data;
  uint nrec = dataset->nrec;
  DEBUG_ASSERT(bitlow_ < sizeof(T) * 8);
  if (top_bottom_ == TopBottom::tbTop)
    for (uint i = 0; i < nrec; i++) data[i] |= decoded_[i] << bitlow_;
  else
    for (uint i = 0; i < nrec; i++) (data[i] <<= bitlow_) |= decoded_[i];
  dataset->maxval = maxval_merge_;  // recover original maxval
}

//-------------------------------------------------------------------------------------

TEMPLATE_CLS(TopBitDict)

}  // namespace compress
}  // namespace Tianmu
