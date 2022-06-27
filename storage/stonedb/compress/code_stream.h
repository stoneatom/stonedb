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
#ifndef STONEDB_COMPRESS_CODE_STREAM_H_
#define STONEDB_COMPRESS_CODE_STREAM_H_
#pragma once

#include "compress/arith_coder.h"
#include "compress/defs.h"

namespace stonedb {
namespace compress {

// All the methods may throw exceptions of type CprsErr or ErrBufOverrun
class CoderStream : protected ArithCoder {
  BitStream my_stream;
  BitStream *str;

 public:
  void Reset(char *buf, uint len, uint pos = 0) {
    my_stream.Reset(buf, len, pos);
    str = &my_stream;
  }
  void Reset(BitStream *str_ = 0) { str = str_; }
  CoderStream() { Reset(); }
  CoderStream(char *buf, uint len, uint pos = 0) { Reset(buf, len, pos); }
  CoderStream(BitStream *str_) { Reset(str_); }
  virtual ~CoderStream() {}
  using ArithCoder::BaseT;
  using ArithCoder::MAX_TOTAL;

  // stream access methods
  uint GetPos() { return str->GetPos(); }
  // compression methods
  void InitCompress() { ArithCoder::InitCompress(); }
  void Encode(BaseT low, BaseT high, BaseT total) {
    CprsErr err = ArithCoder::ScaleRange(str, low, high, total);
    if (static_cast<int>(err)) throw err;
  }
  void EndCompress() { ArithCoder::EndCompress(str); }
  // decompression methods
  void InitDecompress() { ArithCoder::InitDecompress(str); }
  BaseT GetCount(BaseT total) { return ArithCoder::GetCount(total); }
  void Decode(BaseT low, BaseT high, BaseT total) {
    CprsErr err = ArithCoder::RemoveSymbol(str, low, high, total);
    if (static_cast<int>(err)) throw err;
  }

  // uniform compression and decompression
  template <class T>
  void EncodeUniform(T val, T maxval, uint bitmax) {
    CprsErr err = ArithCoder::EncodeUniform<T>(str, val, maxval, bitmax);
    if (static_cast<int>(err)) throw err;
  }

  template <class T>
  void EncodeUniform(T val, T maxval) {
    CprsErr err = ArithCoder::EncodeUniform<T>(str, val, maxval);
    if (static_cast<int>(err)) throw err;
  }

  template <class T>
  void DecodeUniform(T &val, T maxval, uint bitmax) {
    CprsErr err = ArithCoder::DecodeUniform<T>(str, val, maxval, bitmax);
    if (static_cast<int>(err)) throw err;
  }

  template <class T>
  void DecodeUniform(T &val, T maxval) {
    CprsErr err = ArithCoder::DecodeUniform<T>(str, val, maxval);
    if (static_cast<int>(err)) throw err;
  }
};

}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_CODE_STREAM_H_
