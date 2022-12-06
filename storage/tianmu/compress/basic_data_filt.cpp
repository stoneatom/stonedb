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

#include "basic_data_filt.h"

#include <cstring>

#include "common/assert.h"

namespace Tianmu {
namespace compress {

template <class T>
bool DataFilt_RLE<T>::Encode(RangeCoder *coder, DataSet<T> *dataset) {
  T *data = dataset->data;
  uint nrec = dataset->nrec;
  ASSERT(MAXBLEN_ < 65536, "should be 'MAXBLEN_ < 65536'");

  // use RLE?
  dict_.InitInsert();
  uint nrep = 0, nsamp = 0;
  for (uint i = 1; i < nrec; i += 5)
    if (dict_.Insert(data[i - 1])) {
      nsamp++;
      if (data[i] == data[i - 1])
        nrep++;
    } else
      break;

  ASSERT(nsamp <= 65535, "should be 'nsamp <= 65535'");
  short nkey;
  uint sum2 = 0;
  auto keys = dict_.GetKeys(nkey);
  for (short k = 0; k < nkey; k++) sum2 += keys[k].count * keys[k].count;
  if (nrep * nsamp < 5 * sum2)
    return false;

  // make blocks
  Clear();
  ushort len = 1;
  T last = data[0];
  for (uint i = 1; i < nrec; i++)
    if ((data[i] == last) && (len < MAXBLEN_))
      len++;
    else {
      AddLen(len);
      len = 1;
      last = data[nblk_] = data[i];
    }
  AddLen(len);
  dataset->nrec = nblk_;

  // save version using 2 bits
  coder->EncodeUniShift((uchar)0, 2);

  // calculate and save cum counts
  // TODO: histogram alignment to power of 2
  lencnt_[0] = 0;
  uint bitmax = core::GetBitLen(nrec);
  for (ushort i = 1; i <= MAXBLEN_; i++) coder->EncodeUniShift(lencnt_[i] += lencnt_[i - 1], bitmax);
  uint total = lencnt_[MAXBLEN_];

  // encode block lengths
  for (uint b = 0; b < nblk_; b++) {
    len = lens_[b];
    coder->Encode(lencnt_[len - 1], lencnt_[len] - lencnt_[len - 1],
                  total);  // TODO: EncodeShift
  }

  return true;
}
template <class T>
void DataFilt_RLE<T>::Decode(RangeCoder *coder, DataSet<T> *dataset) {
  uchar ver;
  coder->DecodeUniShift(ver, 2);
  if (ver > 0)
    throw CprsErr::CPRS_ERR_COR;

  // read cum counts
  lencnt_[0] = 0;
  merge_nrec_ = dataset->nrec;
  uint bitmax = core::GetBitLen(merge_nrec_);
  for (ushort i = 1; i <= MAXBLEN_; i++) coder->DecodeUniShift(lencnt_[i], bitmax);
  uint total = lencnt_[MAXBLEN_];

  // decode block lengths
  nblk_ = 0;
  uint sumlen = 0;
  while (sumlen < merge_nrec_) {
    uint c = coder->GetCount(total);
    ushort len = 1;
    while (c >= lencnt_[len]) len++;
    coder->Decode(lencnt_[len - 1], lencnt_[len] - lencnt_[len - 1], total);
    sumlen += (lens_[nblk_++] = len);
  }
  if (sumlen > merge_nrec_)
    throw CprsErr::CPRS_ERR_COR;
  dataset->nrec = nblk_;
}
template <class T>
void DataFilt_RLE<T>::Merge(DataSet<T> *dataset) {
  T *data = dataset->data;
  uint nrec = merge_nrec_;
  nblk_ = dataset->nrec;
  while (nblk_ > 0) {
    T val = data[--nblk_];
    for (ushort i = lens_[nblk_]; i > 0; i--) {
      DEBUG_ASSERT(nrec > 0);
      data[--nrec] = val;
    }
  }
  dataset->nrec = merge_nrec_;
}

template <class T>
bool DataFilt_Min<T>::Encode(RangeCoder *coder, DataSet<T> *dataset) {
  // TODO: if(dataset->maxval < 100) return false;
  T *data = dataset->data;
  uint nrec = dataset->nrec;

  // find minimum
  T minval = (T)0 - (T)1;
  for (uint i = 0; i < nrec; i++)
    // if(data[i] < minval) minval = data[i];
    if ((data[i] < minval) && ((minval = data[i]) == 0))
      break;
  if (minval == 0)
    return false;
  if (minval > dataset->maxval)
    throw CprsErr::CPRS_ERR_PAR;

  // save 'minval'
  coder->EncodeUniform(minval, dataset->maxval);

  // subtract minimum
  for (uint i = 0; i < nrec; i++) data[i] -= minval;
  dataset->maxval -= minval;

  return true;
}
template <class T>
void DataFilt_Min<T>::Decode(RangeCoder *coder, DataSet<T> *dataset) {
  coder->DecodeUniform(minval_, dataset->maxval);
  if (minval_ == 0)
    throw CprsErr::CPRS_ERR_COR;
  dataset->maxval -= minval_;
}
template <class T>
void DataFilt_Min<T>::Merge(DataSet<T> *dataset) {
  ASSERT(minval_ > 0, "should be 'minval_ > 0'");
  T *data = dataset->data;
  uint nrec = dataset->nrec;
  for (uint i = 0; i < nrec; i++) data[i] += minval_;
  dataset->maxval += minval_;
}

//--------------------------------------------------------------------------------------------
// Find Greatest Common Divisor of 2 integers with Euclid's algorithm.
// It's slightly faster if you set a <= b
template <class T>
inline T GCD(T a, T b) {
  T t;
  if (a > b) {  // force a <= b
    t = a;
    a = b;
    b = t;
  }
  while (a != 0) {
    t = a;
    a = b % a;
    b = t;
  }
  return b;
}

template <class T>
bool DataFilt_GCD<T>::Encode(RangeCoder *coder, DataSet<T> *dataset) {
  // TODO: if(dataset->maxval < 100) return false;
  T *data = dataset->data;
  uint nrec = dataset->nrec;

  // find GCD
  T gcd = 0;
  for (uint i = 0; i < nrec; i++)
    if ((gcd = GCD(gcd, data[i])) == 1)
      break;
  if (gcd <= 1)
    return false;
  if (gcd > dataset->maxval)
    throw CprsErr::CPRS_ERR_PAR;

  // save
  coder->EncodeUniform(gcd, dataset->maxval);

  // divide by GCD
  for (uint i = 0; i < nrec; i++) data[i] /= gcd;
  dataset->maxval /= gcd;

  return true;
}
template <class T>
void DataFilt_GCD<T>::Decode(RangeCoder *coder, DataSet<T> *dataset) {
  coder->DecodeUniform(gcd_, dataset->maxval);
  if (gcd_ <= 1)
    throw CprsErr::CPRS_ERR_COR;
  dataset->maxval /= gcd_;
}
template <class T>
void DataFilt_GCD<T>::Merge(DataSet<T> *dataset) {
  ASSERT(gcd_ > 1, "should be 'gcd_ > 1'");
  T *data = dataset->data;
  uint nrec = dataset->nrec;
  for (uint i = 0; i < nrec; i++) data[i] *= gcd_;
  dataset->maxval *= gcd_;
}

//--------------------------------------------------------------------------------------------

template <class T>
double DataFilt_Diff<T>::Entropy(T *data, uint nrec, [[maybe_unused]] uchar bitdict, uchar bitlow, bool top) {
  uchar mask = ((uchar)1 _SHL_ bitlow) - 1;
  ASSERT(bitlow < sizeof(T) * 8, "should be 'bitlow < sizeof(T)*8'");
  dict_.InitInsert();
  if (top)
    for (uint i = 0; i < nrec; i++) dict_.Insert((uchar)(data[i] _SHR_ bitlow));
  else
    for (uint i = 0; i < nrec; i++) dict_.Insert((uchar)data[i] & mask);

  double len = core::QuickMath::nlog2n(nrec);
  short nkey;
  auto keys = dict_.GetKeys(nkey);
  for (short k = 0; k < nkey; k++) len -= core::QuickMath::nlog2n(keys[k].count);
  return len;
}
template <class T>
double DataFilt_Diff<T>::Measure(DataSet<T> *dataset, bool diff) {
  T *data = dataset->data;
  uint nrec = dataset->nrec;
  uint nsamp = (nrec - 1 < MAXSAMP_) ? nrec - 1 : MAXSAMP_;
  uint step = (nrec - 1) / nsamp;
  uint i = 1, j = 0;

  if (diff) {
    T maxval1 = dataset->maxval + 1;
    for (; j < nsamp; i += step, j++) {
      DEBUG_ASSERT(i < nrec);
      if ((sample_[j] = data[i] - data[i - 1]) > data[i])
        sample_[j] += maxval1;
    }
  } else
    for (; j < nsamp; i += step, j++) {
      DEBUG_ASSERT(i < nrec);
      sample_[j] = data[i];
    }

  ASSERT(j <= MAXSAMP_, "should be 'j <= MAXSAMP_'");

  uint nbit = core::GetBitLen(dataset->maxval);
  if (nbit <= BITDICT_)
    return Entropy(sample_, j, nbit, 0, true);

  double x = Entropy(sample_, j, BITDICT_, nbit - BITDICT_, true);
  if (nbit <= 2 * BITDICT_)
    x += Entropy(sample_, j, nbit - BITDICT_, nbit - BITDICT_, false);
  else
    x += Entropy(sample_, j, BITDICT_, BITDICT_, false);
  return x;
}

template <class T>
bool DataFilt_Diff<T>::Encode(RangeCoder *coder, DataSet<T> *dataset) {
  uint nrec = dataset->nrec;
  if (nrec < 100)
    return false;

  double m_orig = Measure(dataset, false);
  double m_diff = Measure(dataset, true);

  if (m_diff >= 0.99 * m_orig)
    return false;

  // save version of this routine, using 2 bits (3 = 2^2-1)
  coder->EncodeUniform((uchar)0, (uchar)3);

  T *data = dataset->data;
  T last = 0, curr, maxval1 = dataset->maxval + 1;
  // newmin = newmax = data[0];

  // make earlyEnd_directive::full differencing
  for (uint i = 0; i < nrec; i++) {
    data[i] = (curr = data[i]) - last;
    if (last > curr)
      data[i] += maxval1;
    DEBUG_ASSERT(data[i] <= dataset->maxval);
    last = curr;
  }

  return true;
}

template <class T>
void DataFilt_Diff<T>::Decode(RangeCoder *coder, [[maybe_unused]] DataSet<T> *dataset) {
  uchar ver;
  coder->DecodeUniform(ver, (uchar)3);
  if (ver > 0)
    throw CprsErr::CPRS_ERR_COR;
}

template <class T>
void DataFilt_Diff<T>::Merge(DataSet<T> *dataset) {
  T *data = dataset->data;
  uint nrec = dataset->nrec;

  // integrate
  T last = 0, curr;
  T maxval = dataset->maxval, maxval1 = maxval + 1;
  for (uint i = 0; i < nrec; i++) {
    curr = last + data[i];
    if ((curr > maxval) || (curr < last))
      curr -= maxval1;
    DEBUG_ASSERT(curr <= maxval);
    last = data[i] = curr;
  }
}

//--------------------------------------------------------------------------------------------

template <class T>
bool DataFilt_Uniform<T>::Encode(RangeCoder *coder, DataSet<T> *dataset) {
  // save version of this routine, using 2 bits (3 = 2^2-1)
  coder->EncodeUniform((uchar)0, (uchar)3);

  T *data = dataset->data;
  T maxval = dataset->maxval;
  uint nrec = dataset->nrec;

  if (maxval) {
    uint bitmax = core::GetBitLen(maxval);
    if (maxval == ((T)1 _SHL_ bitmax) - (T)1)
      for (uint i = 0; i < nrec; i++) coder->EncodeUniShift(data[i], bitmax);
    else
      for (uint i = 0; i < nrec; i++) coder->EncodeUniform(data[i], maxval, bitmax);
  }

  dataset->nrec = 0;  // no other compression can be used after Uniform
  return true;
}
template <class T>
void DataFilt_Uniform<T>::Decode(RangeCoder *coder, DataSet<T> *dataset) {
  // read version
  uchar ver;
  coder->DecodeUniform(ver, (uchar)3);
  if (ver > 0)
    throw CprsErr::CPRS_ERR_COR;

  T *data = dataset->data;
  T maxval = dataset->maxval;
  uint nrec = dataset->nrec;

  // decompress data directly to 'dataset->data' - because Uniform is always the
  // last stage of compression
  if (maxval) {
    uint bitmax = core::GetBitLen(maxval);
    if (maxval == ((T)1 _SHL_ bitmax) - (T)1)
      for (uint i = 0; i < nrec; i++) coder->DecodeUniShift(data[i], bitmax);
    else
      for (uint i = 0; i < nrec; i++) coder->DecodeUniform(data[i], maxval, bitmax);
  } else
    std::memset(data, 0, nrec * sizeof(*data));
}
template <class T>
void DataFilt_Uniform<T>::Merge([[maybe_unused]] DataSet<T> *dataset) {}

//--------------------------------------------------------------------------------------------

TEMPLATE_CLS(DataFilt_RLE)
TEMPLATE_CLS(DataFilt_Min)
TEMPLATE_CLS(DataFilt_GCD)
TEMPLATE_CLS(DataFilt_Diff)
TEMPLATE_CLS(DataFilt_Uniform)

}  // namespace compress
}  // namespace Tianmu
