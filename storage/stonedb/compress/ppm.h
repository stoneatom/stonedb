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
#ifndef STONEDB_COMPRESS_PPM_H_
#define STONEDB_COMPRESS_PPM_H_
#pragma once

#include <iostream>

#include "compress/arith_coder.h"
#include "compress/ppm_defs.h"

namespace stonedb {
namespace compress {
class PPM {
  using Symb = uchar;

  std::unique_ptr<PPMModel> model;

  // compression and decompression using ArithCoder
  CprsErr CompressArith(char *dest, int &dlen, Symb *src, int slen);
  CprsErr DecompressArith(Symb *dest, int dlen, char *src, int slen);

 public:
  enum class ModelType { ModelNull, ModelSufTree, ModelWordGraph };
  // enum CoderType { CoderArith, CoderRange };

  static FILE *dump;
  static bool printstat;

  // if data=NULL or dlen=0, the model will be null - compression will simply
  // copy the data; 'method' - which version of compression will be used in
  // Compress/Decompress
  PPM(const Symb *data, int dlen, ModelType mt, PPMParam param = PPMParam(), uchar method = 2);
  ~PPM() = default;

  // 'dlen' - max size of 'dest'; upon exit: actual size of 'dest'.
  // 'dlen' should be at least slen+1 - in this case buffer overflow will never
  // occur Otherwise the return value should be checked against CprsErr::CPRS_ERR_BUF.
  // The last symbol of 'src' must be '\0'.
  CprsErr Compress(char *dest, int &dlen, Symb *src, int slen);

  // 'dlen' - actual size of decompressed data ('slen' from Compress())
  // 'slen' - size of compressed data ('dlen' returned from Compress())
  // (so both sizes - of compressed and uncompressed data - must be stored
  // outside of these routines)
  CprsErr Decompress(Symb *dest, int dlen, char *src, int slen);

  void PrintInfo(std::ostream &str);
};
}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_PPM_H_
