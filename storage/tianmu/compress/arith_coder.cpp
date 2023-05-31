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

#include "arith_coder.h"

#include "common/assert.h"

namespace Tianmu {
namespace compress {

void ArithCoder::InitCompress() {
  low_ = 0;
  high_ = 0xffff;
  underflow_bits_ = 0;
}

// if possible, make normalization and send bits to the 'dest'
CprsErr ArithCoder::ScaleRange(BitStream *dest, BaseT s_low, BaseT s_high, BaseT total) {
  range_ = (WideT)(high_ - low_) + 1;
  high_ = low_ + (BaseT)((range_ * s_high) / total - 1);
  low_ = low_ + (BaseT)((range_ * s_low) / total);
  if (high_ < low_)
    return CprsErr::CPRS_ERR_SUM;

  for (;;) {
    // the same MS bits
    if ((high_ & 0x8000) == (low_ & 0x8000)) {
      dest->PutBit(high_ >> 15);
      while (underflow_bits_ > 0) {
        dest->PutBit((~high_ & 0x8000) >> 15);
        underflow_bits_--;
      }
    }
    // low_=01... high_=10...
    else if ((low_ & 0x4000) && !(high_ & 0x4000)) {
      underflow_bits_++;
      low_ &= 0x3fff;
      high_ |= 0x4000;
    } else
      return CprsErr::CPRS_SUCCESS;

    low_ <<= 1;
    high_ <<= 1;
    high_ |= 1;
  }
}

template <class T>
CprsErr ArithCoder::EncodeUniform(BitStream *dest, T val, T maxval, uint bitmax) {
  DEBUG_ASSERT((val <= maxval) && (val >= 0));
  if (maxval == 0)
    return CprsErr::CPRS_SUCCESS;
  DEBUG_ASSERT((maxval _SHR_ bitmax) == 0);

  // encode groups of 'uni_nbit' bits, from the least significant
  BaseT v;
  CprsErr err;
  DEBUG_ASSERT(uni_total_ <= MAX_TOTAL_);
  while (bitmax > uni_nbit_) {
    v = (BaseT)(val & uni_mask_);
    err = ScaleRange(dest, v, v + (BaseT)1, uni_total_);
    if (static_cast<int>(err))
      return err;
    val >>= uni_nbit_;
    maxval >>= uni_nbit_;
    bitmax -= uni_nbit_;
  }
  // encode the most significant group
  DEBUG_ASSERT(maxval < MAX_TOTAL_);
  err = ScaleRange(dest, (BaseT)val, (BaseT)val + (BaseT)1, (BaseT)maxval + (BaseT)1);
  if (static_cast<int>(err))
    return err;

  return CprsErr::CPRS_SUCCESS;
}

// TODO: it was
void ArithCoder::EndCompress(BitStream *dest) {
  dest->PutBit((low_ & 0x4000) > 0);
  underflow_bits_++;
  while (underflow_bits_-- > 0) dest->PutBit(((~low_) & 0x4000) > 0);
}

CprsErr ArithCoder::CompressBytes(BitStream *dest, char *src, int slen, BaseT *sum, BaseT total) {
  if (!dest || !src || !sum || (slen < 1) || (total <= 0))
    return CprsErr::CPRS_ERR_PAR;

  InitCompress();

  // loop over symbols to encode
  int c;
  CprsErr err;
  for (; slen > 0; slen--) {
    c = *(src++);
    err = ScaleRange(dest, sum[c], sum[c + 1],
                     total);  // rescale high_ and low_, send bits to dest
    if (static_cast<int>(static_cast<int>(err)))
      return err;
  }

  EndCompress(dest);
  return CprsErr::CPRS_SUCCESS;
}

CprsErr ArithCoder::CompressBits(BitStream *dest, BitStream *src, BaseT *sum, BaseT total) {
  if (!dest || !src || !sum || (total <= 0))
    return CprsErr::CPRS_ERR_PAR;

  InitCompress();

  // loop over symbols to encode
  int c;
  CprsErr err;
  while (src->CanRead()) {
    c = src->GetBit();
    err = ScaleRange(dest, sum[c], sum[c + 1],
                     total);  // rescale high_ and low_, send bits to dest
    if (static_cast<int>(static_cast<int>(err)))
      return err;
  }

  EndCompress(dest);
  return CprsErr::CPRS_SUCCESS;
}

void ArithCoder::InitDecompress(BitStream *src) {
  low_ = 0;
  high_ = 0xffff;
  code_ = 0;
  added_ = 0;

  for (int i = 0; i < 16; i++) {
    code_ <<= 1;
    if (src->CanRead())
      code_ |= src->GetBit();
    else
      added_++;
  }
}

// remove the symbol from the input
CprsErr ArithCoder::RemoveSymbol(BitStream *src, BaseT s_low, BaseT s_high, BaseT total) {
  high_ = low_ + (BaseT)((range_ * s_high) / total - 1);  // TODO: optimize for decompression of bits
  low_ = low_ + (BaseT)((range_ * s_low) / total);
  for (;;) {
    // the same MS bits
    if ((high_ & 0x8000) == (low_ & 0x8000)) {
    }
    // low_=01... high_=10...
    else if ((low_ & 0x4000) && !(high_ & 0x4000)) {
      code_ ^= 0x4000;
      low_ &= 0x3fff;
      high_ |= 0x4000;
    } else
      return CprsErr::CPRS_SUCCESS;

    low_ <<= 1;
    high_ <<= 1;
    high_ |= 1;

    code_ <<= 1;
    if (src->CanRead())
      code_ |= src->GetBit();
    else if (++added_ > sizeof(BaseT) * 8)
      return CprsErr::CPRS_ERR_BUF;
  }
}

template <class T>
CprsErr ArithCoder::DecodeUniform(BitStream *src, T &val, T maxval, uint bitmax) {
  val = 0;
  if (maxval == 0)
    return CprsErr::CPRS_SUCCESS;
  DEBUG_ASSERT((maxval _SHR_ bitmax) == 0);

  // decode groups of 'uni_nbit' bits, from the least significant
  BaseT v;
  CprsErr err;
  DEBUG_ASSERT(uni_total_ <= MAX_TOTAL_);
  uint shift = 0;
  while (shift + uni_nbit_ < bitmax) {
    v = GetCount(uni_total_);
    err = RemoveSymbol(src, v, v + (BaseT)1, uni_total_);
    if (static_cast<int>(err))
      return err;
    DEBUG_ASSERT(shift < 64);
    val |= (uint64_t)v << shift;
    shift += uni_nbit_;
  }

  // decode the most significant group
  BaseT total = (BaseT)(maxval _SHR_ shift) + (BaseT)1;
  DEBUG_ASSERT(total <= MAX_TOTAL_);
  v = GetCount(total);
  err = RemoveSymbol(src, v, v + (BaseT)1, total);
  if (static_cast<int>(err))
    return err;
  DEBUG_ASSERT(shift < 64);
  val |= (uint64_t)v << shift;
  DEBUG_ASSERT(val <= maxval);

  return CprsErr::CPRS_SUCCESS;
}

CprsErr ArithCoder::DecompressBytes(char *dest, int dlen, BitStream *src, BaseT *sum, BaseT total) {
  if (!dest || !src || !sum || (dlen < 1))
    return CprsErr::CPRS_ERR_PAR;

  BaseT count;
  int c;
  CprsErr err;

  InitDecompress(src);

  // loop over decoded symbols
  for (; dlen > 0; dlen--) {
    // compute 'count' of the next symbol
    count = GetCount(total);

    // decode the symbol using 'sum' table (naive, slow method)
    c = 0;
    while (sum[++c] <= count)
      ;
    *(dest++) = (char)(--c);

    // remove the symbol from the input
    err = RemoveSymbol(src, sum[c], sum[c + 1], total);
    if (static_cast<int>(static_cast<int>(err)))
      return err;
  }

  return CprsErr::CPRS_SUCCESS;
}

CprsErr ArithCoder::DecompressBits(BitStream *dest, BitStream *src, BaseT *sum, BaseT total) {
  if (!dest || !src || !sum)
    return CprsErr::CPRS_ERR_PAR;

  BaseT count, sum0 = sum[0], sum1 = sum[1];
  CprsErr err;

  InitDecompress(src);

  // loop over decoded symbols
  while (dest->CanWrite()) {
    // compute 'count' of the next symbol
    count = GetCount(total);

    if (sum1 <= count) {
      dest->PutBit1();
      err = RemoveSymbol(src, sum1, total, total);
    } else {
      dest->PutBit0();
      err = RemoveSymbol(src, sum0, sum1, total);
    }
    if (static_cast<int>(static_cast<int>(err)))
      return err;
  }

  return CprsErr::CPRS_SUCCESS;
}

ArithCoder::BaseT ArithCoder::GetCount(BaseT total) {
  range_ = (WideT)(high_ - low_) + 1;
  return (BaseT)((((WideT)(code_ - low_) + 1) * total - 1) / range_);
}

template <class T>
CprsErr ArithCoder::EncodeUniform(BitStream *dest, T val, T maxval) {
  return EncodeUniform<T>(dest, val, maxval, GetBitLen(maxval));
}

template <class T>
CprsErr ArithCoder::DecodeUniform(BitStream *src, T &val, T maxval) {
  return DecodeUniform<T>(src, val, maxval, GetBitLen(maxval));
}

//-------------------------------------------------------------------------------------

template CprsErr ArithCoder::EncodeUniform<uint64_t>(BitStream *, uint64_t, uint64_t, uint);
template CprsErr ArithCoder::DecodeUniform<uint64_t>(BitStream *, uint64_t &, uint64_t, uint);
template CprsErr ArithCoder::EncodeUniform<uint>(BitStream *, uint, uint, uint);
template CprsErr ArithCoder::DecodeUniform<uint>(BitStream *, uint &, uint, uint);
template CprsErr ArithCoder::EncodeUniform<ushort>(BitStream *, ushort, ushort, uint);
template CprsErr ArithCoder::DecodeUniform<ushort>(BitStream *, ushort &, ushort, uint);
template CprsErr ArithCoder::EncodeUniform<short>(BitStream *, short, short, uint);
template CprsErr ArithCoder::DecodeUniform<short>(BitStream *, short &, short, uint);

}  // namespace compress
}  // namespace Tianmu
