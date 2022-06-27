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
#ifndef STONEDB_COMPRESS_RANGE_CODE_H_
#define STONEDB_COMPRESS_RANGE_CODE_H_
#pragma once

#include <typeinfo>

#include "common/assert.h"
#include "common/exception.h"
#include "compress/defs.h"
#include "core/bin_tools.h"

namespace stonedb {
namespace compress {

/* Decoder always reads exactly the same no. of bytes as the encoder saves */
class RangeCoder {
  static const uint TOP = 1u << 24;
  static const uint BOT = 1u << 16;
  // static const uint BOT = 1u << 15;

  uint low, code, range, _tot, _sh;
  uchar *buf, *pos, *stop;

  // constants for uniform encoding:
  static const uint uni_nbit = 16;
  static const uint uni_mask = (1u << uni_nbit) - 1;
  static const uint uni_total = 1u << uni_nbit;

  uchar InByte() {
    if (pos >= stop) throw CprsErr::CPRS_ERR_BUF;
    return *pos++;
  }
  void OutByte(uchar c) {
    if (pos >= stop) throw CprsErr::CPRS_ERR_BUF;
    *pos++ = c;
  }

 public:
  static const uint MAX_TOTAL = BOT;

  RangeCoder() { buf = pos = stop = 0; }
  uint GetPos() { return (uint)(pos - buf); }  // !!! position in BYTES !!!
  void InitCompress(void *b, uint len, uint p = 0) {
    buf = (uchar *)b;
    pos = buf + p;
    stop = buf + len;
    low = 0;
    range = (uint)-1;
  }
  void EndCompress() {
    for (int i = 0; i < 4; i++) OutByte(low >> 24), low <<= 8;
  }
  void InitDecompress(void *b, uint len, uint p = 0) {
    buf = (uchar *)b;
    pos = buf + p;
    stop = buf + len;
    low = code = 0;
    range = (uint)-1;
    for (int i = 0; i < 4; i++) code = code << 8 | InByte();
  }

  void Encode(uint cumFreq, uint freq, uint total) {
    DEBUG_ASSERT(freq && cumFreq + freq <= total && total <= MAX_TOTAL);
    DEBUG_ASSERT(range >= BOT && low + range - 1 >= low);
    low += (range /= total) * cumFreq;
    range *= freq;
    while (((low ^ (low + range)) < TOP) || ((range < BOT) && ((range = -low & (BOT - 1)), 1)))
      OutByte(low >> 24), low <<= 8, range <<= 8;
  }

  uint GetCount(uint total) {
#if defined(_DEBUG) || !defined(NDEBUG)
    _tot = total;
#endif
    DEBUG_ASSERT(range >= BOT && low + range - 1 >= code && code >= low);
    uint tmp = (code - low) / (range /= total);
    DEBUG_ASSERT(tmp < total);
    if (tmp >= total) throw CprsErr::CPRS_ERR_COR;
    return tmp;
  }

  void Decode(uint cumFreq, uint freq, [[maybe_unused]] uint total) {
    DEBUG_ASSERT(_tot == total && freq && cumFreq + freq <= total && total <= MAX_TOTAL);
    low += range * cumFreq;
    range *= freq;
    while (((low ^ (low + range)) < TOP) || ((range < BOT) && ((range = -low & (BOT - 1)), 1)))
      code = code << 8 | InByte(), low <<= 8, range <<= 8;
    // NOTE: after range<BOT we might check for data corruption
  }

  void EncodeShift(uint cumFreq, uint freq, uint shift) {
    DEBUG_ASSERT(cumFreq + freq <= (1u _SHL_ shift) && freq && (1u _SHL_ shift) <= MAX_TOTAL);
    DEBUG_ASSERT(range >= BOT && low + range - 1 >= low);
    low += (range _SHR_ASSIGN_ shift)*cumFreq;
    range *= freq;
    while ((low ^ (low + range)) < TOP || (range < BOT && ((range = -low & (BOT - 1)), 1)))
      OutByte(low >> 24), low <<= 8, range <<= 8;
  }

  uint GetCountShift(uint shift) {
#if defined(_DEBUG) || !defined(NDEBUG)
    _sh = shift;
#endif
    DEBUG_ASSERT(range >= BOT && low + range - 1 >= code && code >= low);
    uint tmp = (code - low) / (range _SHR_ASSIGN_ shift);
    if (tmp >= (1u << shift)) throw CprsErr::CPRS_ERR_COR;
    return tmp;
  }

  void DecodeShift(uint cumFreq, uint freq, [[maybe_unused]] uint shift) {
    DEBUG_ASSERT(_sh == shift && cumFreq + freq <= (1u _SHL_ shift) && freq && (1u _SHL_ shift) <= MAX_TOTAL);
    low += range * cumFreq;
    range *= freq;
    while (((low ^ (low + range)) < TOP) || ((range < BOT) && ((range = -low & (BOT - 1)), 1)))
      code = code << 8 | InByte(), low <<= 8, range <<= 8;
  }

  // uniform compression and decompression (must be: val <= maxval)
  template <class T>
  void EncodeUniform(T val, T maxval, uint bitmax) {
    DEBUG_ASSERT((val <= maxval));
    DEBUG_ASSERT((((uint64_t)maxval >> bitmax) == 0) || bitmax >= 64);
    if (maxval == 0) return;

    // encode groups of 'uni_nbit' bits, from the least significant
    DEBUG_ASSERT(uni_total <= MAX_TOTAL);
    while (bitmax > uni_nbit) {
      EncodeShift((uint)(val & uni_mask), 1, uni_nbit);
      DEBUG_ASSERT(uni_nbit < sizeof(T) * 8);
      val >>= uni_nbit;
      maxval >>= uni_nbit;
      bitmax -= uni_nbit;
    }

    // encode the most significant group
    // ASSERT(maxval < MAX_TOTAL, "should be 'maxval < MAX_TOTAL'"); // compiler
    // figure out as allways true
    Encode((uint)val, 1, (uint)maxval + 1);
  }

  template <class T>
  void EncodeUniform(T val, T maxval) {
    EncodeUniform<T>(val, maxval, core::GetBitLen((uint64_t)maxval));
  }

  template <class T>
  void DecodeUniform(T &val, T maxval, uint bitmax) {
    val = 0;
    if (maxval == 0) return;
    DEBUG_ASSERT((((uint64_t)maxval >> bitmax) == 0) || bitmax >= 64);

    // decode groups of 'uni_nbit' bits, from the least significant
    DEBUG_ASSERT(uni_total <= MAX_TOTAL);
    uint v, shift = 0;
    while (shift + uni_nbit < bitmax) {
      v = GetCountShift(uni_nbit);
      DecodeShift(v, 1, uni_nbit);
      DEBUG_ASSERT(shift < 64);
      val |= (uint64_t)v << shift;
      shift += uni_nbit;
    }

    // decode the most significant group
    DEBUG_ASSERT(shift < sizeof(maxval) * 8);
    uint total = (uint)(maxval >> shift) + 1;
    DEBUG_ASSERT(total <= MAX_TOTAL);
    v = GetCount(total);
    Decode(v, 1, total);
    val |= (uint64_t)v << shift;
    DEBUG_ASSERT(val <= maxval);
  }

  template <class T>
  void DecodeUniform(T &val, T maxval) {
    DecodeUniform<T>(val, maxval, core::GetBitLen((uint64_t)maxval));
  }

  // uniform compression by shifting
  template <class T>
  void EncodeUniShift(T x, uint shift) {
    if (shift <= uni_nbit)
      EncodeShift((uint)x, 1, shift);
    else {
      EncodeShift((uint)x & uni_mask, 1, uni_nbit);
      EncodeUniShift(x >> uni_nbit, shift - uni_nbit);
    }
  }

  template <class T>
  void DecodeUniShift(T &x, uint shift) {
    if (shift <= uni_nbit) {
      x = (T)GetCountShift(shift);
      DecodeShift((uint)x, 1, shift);
    } else {
      uint tmp = GetCountShift(uni_nbit);
      DecodeShift(tmp, 1, uni_nbit);
      DecodeUniShift(x, shift - uni_nbit);
      DEBUG_ASSERT(uni_nbit < sizeof(x) * 8);
      x = (x << uni_nbit) | (T)tmp;
    }
  }
};

template <>
inline void RangeCoder::EncodeUniShift<uchar>(uchar x, uint shift) {
  DEBUG_ASSERT(shift <= uni_nbit);
  EncodeShift((uint)x, 1, shift);
}

template <>
inline void RangeCoder::EncodeUniShift<ushort>(ushort x, uint shift) {
  DEBUG_ASSERT(shift <= uni_nbit);
  EncodeShift((uint)x, 1, shift);
}

}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_RANGE_CODE_H_
