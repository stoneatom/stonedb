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
#ifndef STONEDB_COMPRESS_ARITH_CODER_H_
#define STONEDB_COMPRESS_ARITH_CODER_H_
#pragma once

#include "compress/data_stream.h"
#include "compress/defs.h"

namespace stonedb {
namespace compress {

// The functions in ArithCoder throw ErrBufOverrun() or return CprsErr::CPRS_ERR_BUF,
// when the buffer is too short (during reading or writing).
//
// NOTE: cumulative count of symbols ('total') must be smaller than MAX_BaseT /
// 4 (=16383) !!!

class ArithCoder {
 public:
  using BaseT = unsigned short;
  using WideT = unsigned long;
  static const BaseT MAX_TOTAL = USHRT_MAX / 4;

  // Saves encoded data to 'dest'.
  //   sum - array of cumulated counts, starting with value 0.
  //      Its size = highest_index_of_character_to_encode + 2 (1 for total)
  //      (contains 'low' for every character plus total),
  //		e.g. for characters 0 and 1 it is: { low0, low1, total }
  //   total - total sum of counts
  CprsErr CompressBytes(BitStream *dest, char *src, int slen, BaseT *sum, BaseT total);

  // dlen - the number of characters to decode ('dest' must be at least 'dlen'
  // large)
  CprsErr DecompressBytes(char *dest, int dlen, BitStream *src, BaseT *sum, BaseT total);

  // For de/compression of bit strings.
  //   sum[] = { low0, low1, total } = { 0, count0, count0 + count1 }
  CprsErr CompressBits(BitStream *dest, BitStream *src, BaseT *sum, BaseT total);
  // dest->len is the number of bits to decode
  CprsErr DecompressBits(BitStream *dest, BitStream *src, BaseT *sum, BaseT total);

  // De/compression of bits with dynamic probabilities
  // CprsErr CompressBitsDyn(BitStream* dest, BitStream* src, uint num0, uint
  // num1); CprsErr DecompressBitsDyn(BitStream* dest, BitStream* src, uint
  // num0, uint num1);

 private:
  BaseT low;
  BaseT high;
  BaseT code;
  WideT range;
  int underflow_bits;
  unsigned int added;  // number of '0' bits virtually "added" to the source
                       // during decompr. (added > 16, means error)

  // constants for uniform encoding:
  static const uint uni_nbit = 13;
  static const BaseT uni_mask = (1 << uni_nbit) - 1;
  static const BaseT uni_total = 1 << uni_nbit;

 public:
  ArithCoder() {
    low = high = code = 0;
    range = 0;
    underflow_bits = added = 0;
  }
  ~ArithCoder() {}
  // compression methods
  void InitCompress();
  CprsErr ScaleRange(BitStream *dest, BaseT s_low, BaseT s_high, BaseT total);
  void EndCompress(BitStream *dest);

  // decompression methods
  void InitDecompress(BitStream *src);
  BaseT GetCount(BaseT total);
  CprsErr RemoveSymbol(BitStream *src, BaseT s_low, BaseT s_high, BaseT total);

  // uniform compression and decompression
  // bitmax - bit length of maxval
  template <class T>
  CprsErr EncodeUniform(BitStream *dest, T val, T maxval, uint bitmax);
  template <class T>
  CprsErr EncodeUniform(BitStream *dest, T val, T maxval);
  template <class T>
  CprsErr DecodeUniform(BitStream *src, T &val, T maxval, uint bitmax);
  template <class T>
  CprsErr DecodeUniform(BitStream *src, T &val, T maxval);
};

}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_ARITH_CODER_H_
