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
#ifndef STONEDB_COMPRESS_BIT_STREAM_COMPRESSOR_H_
#define STONEDB_COMPRESS_BIT_STREAM_COMPRESSOR_H_
#pragma once

#include "compress/arith_coder.h"

namespace stonedb {
namespace compress {

//////////////////////////////////////////////////////////////////////
//////  Bitstream Compressor (optimized by the number of 0 and 1) ////
// Compress() and Decompress() return the error identifier (see defs.h).
// This should be checked, especially against the CprsErr::CPRS_ERR_BUF value,
// which indicates buffer overflow (output buf for Compress, input buf for
// Decompress).
//
// For compression, the size of destination buffer >= (1.1 * source_len + 32)
// should be enough in 99.9% of cases.
class BitstreamCompressor {
  double Entropy(double p);

  // fill the array of cumulative counts
  void GetSumTable(unsigned short *sum, unsigned int num0, unsigned int num1);

  // compute 'differential' stream of bits; return number of ones in the new
  // stream
  uint Differ(BitStream *dest, BitStream *src);
  // compute 'integral' stream of bits
  void Integrt(BitStream *dest, BitStream *src);

  // the main part of de/compression (after performing data preprocessing and
  // writing the header)
  CprsErr CompressData(BitStream *dest, BitStream *src, unsigned int numbits, unsigned int num1);
  CprsErr DecompressData(BitStream *dest, BitStream *src, unsigned int numbits, unsigned int num1);

  // public:
  // De/compress using BitStream:

  // src->len is the number of bits to encode, so must hold:  src->len ==
  // numbits. After compression, dest->GetPos() is what should be passed to
  // Decompress() in its src->len (assuming that src->pos in Decompress() is the
  // same as initial dest->pos in Compress())
  CprsErr Compress(BitStream *dest, BitStream *src, unsigned int numbits, unsigned int num1);
  // src - encoded data, dest - decoded data;
  // dest->len must be the number of bits to decode (NOT the total size of the
  // buffer!!! that's a little trap :)
  CprsErr Decompress(BitStream *dest, BitStream *src, unsigned int numbits, unsigned int num1);

 public:
  // The following interface is perhaps the simplest:

  // src - original data, dest - buffer for encoded data
  // len - length of 'dest', in BYTES
  // After compression, 'len' is the length of compressed data, in BYTES. This
  // must be passed to Decompress().
  CprsErr Compress(char *dest, uint &len, char *src, uint numbits, uint num1);
  // src - encoded data, dest - buffer for decoded data (of size at least
  // 'numbits' bits) len - the 'len' returned from Compress()
  CprsErr Decompress(char *dest, uint len, char *src, uint numbits, uint num1);

  // Another interface:

  //   outpos - number of bits of 'out' already used (can be >= 32, that's not a
  //   problem) outlen - length of 'out' array, in BITS (so outlen is divisible
  //   by 32),
  // After compression, out and outpos are changed, so that outpos < 32, and
  // returned by reference. 'outlen' is set to the length of encoded data +
  // original 'outpos' (and should be passed to Decompress() as 'inlen')
  CprsErr Compress(unsigned int *in, unsigned int *&out, int &outpos, int &outlen, int numbits, int num1);
  //   in - encoded data, out - decoded data
  //   inpos - no. of bits of 'in' already used
  //   inlen - such that (inlen - inpos) is the length of encoded data, in bits;
  //		   If 'in' and 'inpos' are the same as 'out' and 'outpos' passed
  // to
  // Compress(), 		   then 'inlen' should be the same as 'outlen'
  // returned from Compress().
  CprsErr Decompress(unsigned int *in, int inpos, int inlen, unsigned int *out, int numbits, int num1);
};

}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_BIT_STREAM_COMPRESSOR_H_
