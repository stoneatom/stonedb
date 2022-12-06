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
#ifndef TIANMU_COMPRESS_RANGE_CODE_H_
#define TIANMU_COMPRESS_RANGE_CODE_H_
#pragma once

#include <typeinfo>

#include "common/assert.h"
#include "common/exception.h"
#include "compress/defs.h"
#include "core/bin_tools.h"

namespace Tianmu {
namespace compress {

/* Decoder always reads exactly the same no. of bytes as the encoder saves */
class RangeCoder {
  static const uint TOP_ = 1u << 24;
  static const uint BOT_ = 1u << 16;
  // static const uint BOT_ = 1u << 15;

  uint low_, code_, range_, _tot_, _sh_;
  uchar *buf_, *pos_, *stop_;

  // constants for uniform encoding:
  static const uint uni_nbit_ = 16;
  static const uint uni_mask_ = (1u << uni_nbit_) - 1;
  static const uint uni_total_ = 1u << uni_nbit_;

  uchar InByte() {
    if (pos_ >= stop_)
      throw CprsErr::CPRS_ERR_BUF;
    return *pos_++;
  }
  void OutByte(uchar c) {
    if (pos_ >= stop_)
      throw CprsErr::CPRS_ERR_BUF;
    *pos_++ = c;
  }

 public:
  static const uint MAX_TOTAL_ = BOT_;

  RangeCoder() { buf_ = pos_ = stop_ = 0; }
  uint GetPos() { return (uint)(pos_ - buf_); }  // !!! position in BYTES !!!
  void InitCompress(void *b, uint len, uint p = 0) {
    buf_ = (uchar *)b;
    pos_ = buf_ + p;
    stop_ = buf_ + len;
    low_ = 0;
    range_ = (uint)-1;
  }
  void EndCompress() {
    for (int i = 0; i < 4; i++) OutByte(low_ >> 24), low_ <<= 8;
  }
  void InitDecompress(void *b, uint len, uint p = 0) {
    buf_ = (uchar *)b;
    pos_ = buf_ + p;
    stop_ = buf_ + len;
    low_ = code_ = 0;
    range_ = (uint)-1;
    for (int i = 0; i < 4; i++) code_ = code_ << 8 | InByte();
  }

  void Encode(uint cumFreq, uint freq, uint total) {
    DEBUG_ASSERT(freq && cumFreq + freq <= total && total <= MAX_TOTAL_);
    DEBUG_ASSERT(range_ >= BOT_ && low_ + range_ - 1 >= low_);
    low_ += (range_ /= total) * cumFreq;
    range_ *= freq;
    while (((low_ ^ (low_ + range_)) < TOP_) || ((range_ < BOT_) && ((range_ = -low_ & (BOT_ - 1)), 1)))
      OutByte(low_ >> 24), low_ <<= 8, range_ <<= 8;
  }

  uint GetCount(uint total) {
#if defined(_DEBUG) || !defined(NDEBUG)
    _tot_ = total;
#endif
    DEBUG_ASSERT(range_ >= BOT_ && low_ + range_ - 1 >= code_ && code_ >= low_);
    uint tmp = (code_ - low_) / (range_ /= total);
    DEBUG_ASSERT(tmp < total);
    if (tmp >= total)
      throw CprsErr::CPRS_ERR_COR;
    return tmp;
  }

  void Decode(uint cumFreq, uint freq, [[maybe_unused]] uint total) {
    DEBUG_ASSERT(_tot_ == total && freq && cumFreq + freq <= total && total <= MAX_TOTAL_);
    low_ += range_ * cumFreq;
    range_ *= freq;
    while (((low_ ^ (low_ + range_)) < TOP_) || ((range_ < BOT_) && ((range_ = -low_ & (BOT_ - 1)), 1)))
      code_ = code_ << 8 | InByte(), low_ <<= 8, range_ <<= 8;
    // NOTE: after range_<BOT_ we might check for data corruption
  }

  void EncodeShift(uint cumFreq, uint freq, uint shift) {
    DEBUG_ASSERT(cumFreq + freq <= (1u _SHL_ shift) && freq && (1u _SHL_ shift) <= MAX_TOTAL_);
    DEBUG_ASSERT(range_ >= BOT_ && low_ + range_ - 1 >= low_);
    low_ += (range_ _SHR_ASSIGN_ shift)*cumFreq;
    range_ *= freq;
    while ((low_ ^ (low_ + range_)) < TOP_ || (range_ < BOT_ && ((range_ = -low_ & (BOT_ - 1)), 1)))
      OutByte(low_ >> 24), low_ <<= 8, range_ <<= 8;
  }

  uint GetCountShift(uint shift) {
#if defined(_DEBUG) || !defined(NDEBUG)
    _sh_ = shift;
#endif
    DEBUG_ASSERT(range_ >= BOT_ && low_ + range_ - 1 >= code_ && code_ >= low_);
    uint tmp = (code_ - low_) / (range_ _SHR_ASSIGN_ shift);
    if (tmp >= (1u << shift))
      throw CprsErr::CPRS_ERR_COR;
    return tmp;
  }

  void DecodeShift(uint cumFreq, uint freq, [[maybe_unused]] uint shift) {
    DEBUG_ASSERT(_sh_ == shift && cumFreq + freq <= (1u _SHL_ shift) && freq && (1u _SHL_ shift) <= MAX_TOTAL_);
    low_ += range_ * cumFreq;
    range_ *= freq;
    while (((low_ ^ (low_ + range_)) < TOP_) || ((range_ < BOT_) && ((range_ = -low_ & (BOT_ - 1)), 1)))
      code_ = code_ << 8 | InByte(), low_ <<= 8, range_ <<= 8;
  }

  // uniform compression and decompression (must be: val <= maxval)
  template <class T>
  void EncodeUniform(T val, T maxval, uint bitmax) {
    DEBUG_ASSERT((val <= maxval));
    DEBUG_ASSERT((((uint64_t)maxval >> bitmax) == 0) || bitmax >= 64);
    if (maxval == 0)
      return;

    // encode groups of 'uni_nbit_' bits, from the least significant
    DEBUG_ASSERT(uni_total_ <= MAX_TOTAL_);
    while (bitmax > uni_nbit_) {
      EncodeShift((uint)(val & uni_mask_), 1, uni_nbit_);
      DEBUG_ASSERT(uni_nbit_ < sizeof(T) * 8);
      val >>= uni_nbit_;
      maxval >>= uni_nbit_;
      bitmax -= uni_nbit_;
    }

    // encode the most significant group
    // ASSERT(maxval < MAX_TOTAL_, "should be 'maxval < MAX_TOTAL_'"); // compiler
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
    if (maxval == 0)
      return;
    DEBUG_ASSERT((((uint64_t)maxval >> bitmax) == 0) || bitmax >= 64);

    // decode groups of 'uni_nbit_' bits, from the least significant
    DEBUG_ASSERT(uni_total_ <= MAX_TOTAL_);
    uint v, shift = 0;
    while (shift + uni_nbit_ < bitmax) {
      v = GetCountShift(uni_nbit_);
      DecodeShift(v, 1, uni_nbit_);
      DEBUG_ASSERT(shift < 64);
      val |= (uint64_t)v << shift;
      shift += uni_nbit_;
    }

    // decode the most significant group
    DEBUG_ASSERT(shift < sizeof(maxval) * 8);
    uint total = (uint)(maxval >> shift) + 1;
    DEBUG_ASSERT(total <= MAX_TOTAL_);
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
    if (shift <= uni_nbit_)
      EncodeShift((uint)x, 1, shift);
    else {
      EncodeShift((uint)x & uni_mask_, 1, uni_nbit_);
      EncodeUniShift(x >> uni_nbit_, shift - uni_nbit_);
    }
  }

  template <class T>
  void DecodeUniShift(T &x, uint shift) {
    if (shift <= uni_nbit_) {
      x = (T)GetCountShift(shift);
      DecodeShift((uint)x, 1, shift);
    } else {
      uint tmp = GetCountShift(uni_nbit_);
      DecodeShift(tmp, 1, uni_nbit_);
      DecodeUniShift(x, shift - uni_nbit_);
      DEBUG_ASSERT(uni_nbit_ < sizeof(x) * 8);
      x = (x << uni_nbit_) | (T)tmp;
    }
  }
};

template <>
inline void RangeCoder::EncodeUniShift<uchar>(uchar x, uint shift) {
  DEBUG_ASSERT(shift <= uni_nbit_);
  EncodeShift((uint)x, 1, shift);
}

template <>
inline void RangeCoder::EncodeUniShift<ushort>(ushort x, uint shift) {
  DEBUG_ASSERT(shift <= uni_nbit_);
  EncodeShift((uint)x, 1, shift);
}

}  // namespace compress
}  // namespace Tianmu

#endif  // TIANMU_COMPRESS_RANGE_CODE_H_
