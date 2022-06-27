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
#ifndef STONEDB_COMPRESS_TEXT_COMPRESSOR_H_
#define STONEDB_COMPRESS_TEXT_COMPRESSOR_H_
#pragma once

#include <vector>

#include "compress/inc_wgraph.h"
#include "compress/ppm.h"

namespace stonedb {
namespace compress {

// TODO: safety - check model sizes and stop building the next model if the size
// is too large
// TODO: memory - don't make a copy of the data for encoding
// TODO: time - in Compress() and Decompress(), don't encode if ver=0
// TODO: ratio - test other values of PERMSTEP
// TODO: ratio - separate recognition if a record contains only upper-case or
// only lower-case letters
// TODO: ratio - different algorithm for compression of short columns

class TextCompressor {
  // array of split positions, at which PPM models are built;
  // its size is (no_of_models + 1) and is >= 2;
  // split[0]=0 and split[size-1]=data_len
  std::vector<int> split;

  // set splits so that split[i+1]/split[i] is approx. the same for all 'i' and
  // is >= BLD_RATIO
  void SetSplit(int len);

  // simple permutation of strings to compress; PermNext will loop through
  // permuted elements 0..nrec-1, beginning from PermFirst() and finishing in
  // PermFirst()
  static const int PERMSTEP = 2048;
  int PermFirst([[maybe_unused]] int nrec) { return 0; }
  void PermNext(int &i, int nrec) {
    i += PERMSTEP;
    if (i >= nrec) i = (i + 1) % PERMSTEP % nrec;
  }

  // parameters of the procedure of building a sequence of models
  int BLD_START;     // no. of bytes used to build the first model (they must be
                     // simply copied during compression)
  double BLD_RATIO;  // no. of bytes used to build the next model is min.
                     // BLD_RATIO * no_bytes_to_build_previous_model

  void SetParams(PPMParam &p, int ver, int lev,
                 int len);  // sets 'p' and BLD_...; then invokes SetSplit()

  IncWGraph graph;

  CprsErr CompressCopy(char *dest, int &dlen, char *src,
                       int slen);  // stores ver=0 at the beginning
  CprsErr CompressCopy(char *dest, int &dlen, char **index, const uint *lens,
                       int nrec);  // stores ver=0
  CprsErr DecompressCopy(char *dest, int dlen, char *src, int slen, char **index, const uint *lens, int nrec);

  // Used for versions 1 and 2
  // Note: this routine does mainly 2 things:
  //  - permutes strings (so that models are built on the part of data which is
  //  representative for the rest)
  //  - and encodes them as null-separated concatenation (each string _begins_
  //  with '\0') Then it runs CompressSimple.
  CprsErr CompressVer2(char *dest, int &dlen, char **index, const uint *lens, int nrec, int ver = VER, int lev = LEV);
  CprsErr DecompressVer2(char *dest, int dlen, char *src, int slen, char **index, const uint * /*lens*/, int nrec);

  CprsErr CompressVer4(char *dest, int &dlen, char **index, const uint *lens, int nrec, int ver, int lev, uint packlen);
  CprsErr DecompressVer4(char *dest, int dlen, char *src, int slen, char **index, const uint *lens, int nrec);
  CprsErr CompressZlib(char *dest, int &dlen, char **index, const uint *lens, int nrec, uint packlen);
  CprsErr DecompressZlib(char *dest, int dlen, char *src, int slen, char **index, const uint *lens, int nrec);

 public:
  TextCompressor();
  ~TextCompressor() = default;

  static const int MAXVER = static_cast<int>(common::PackFmt::ZLIB);
  static const int VER = 3;
  static const int LEV = 7;

  // 'ver' - version of algorithm to use:
  //    0 - no compression,
  //    1 - PPM using SuffixTree
  //    2 - PPM using WordGraph
  //    3 - PPM using IncWGraph
  // 'lev' - integer from 1 to 9 - compression level (currently NOT used):
  //    1 - fast and low memory requirement, but low compression ratio
  //    9 - high compression ratio, but slow and high memory requirements

  //---------- Simple interface ----------//

  // Input data 'src' of size 'slen' is compressed into buffer 'dest', which is
  // 'dlen' bytes long. Upon exit, 'dlen' contains the actual size of compressed
  // data, which must be passed to DecompressPlain() as 'slen'. Size of
  // compressed data is ALWAYS <= slen+1, so dlen >= slen+1 is enough (if the
  // compressed data is larger than the original ones, compression method is
  // automatically
  //  switched to ver=0, that is simple copy)
  // This routine doesn't work for ver=3.
  //
  // Note 1: 'src' is treated as concatenation of null-terminated strings
  // (symbol '\0' has special meaning!) Note 2: strings used to build PPM model
  // are taken simply from the beginning of 'src',
  //         so they can be non-representative for the next strings
  CprsErr CompressPlain(char *dest, int &dlen, char *src, int slen, int ver = VER, int lev = LEV);

  // 'dlen' - actual size of decompressed data ('slen' from CompressPlain())
  // 'slen' - size of compressed data ('dlen' returned from CompressPlain())
  // (so both sizes - of compressed and uncompressed data - must be stored
  // outside of these routines)
  CprsErr DecompressPlain(char *dest, int dlen, char *src, int slen);

  //---------- Interface for strings containing '\0' as a usual symbol
  //----------//

  // nrec - no. of strings
  // index[i] - pointer to the i'th string
  // lens[i] - length of the i'th string (can be 0 - for empty string)
  // dlen - length of 'dest' buffer. Upon exit, 'dlen' contains the actual size
  // of compressed data. packlen - upon exit will hold minimum size of 'dest'
  // buffer which must be passed to Decompress. Size of compressed data is
  // ALWAYS <= total_length_of_records + 2.
  CprsErr Compress(char *dest, int &dlen, char **index, const uint *lens, int nrec, uint &packlen, int ver = VER,
                   int lev = LEV);

  // dlen - length of buffer 'dest'; must be >= 'packlen' returned from Compress
  //        (which is <= cumulative length of decoded strings)
  // slen - size of compressed data ('dlen' returned from Compress())
  // lens - expected lengths of records
  // nrec - expected no. of records
  // Decompressed and decoded strings are put into 'dest', sometimes in
  // different order than in 'lens'! (this is the case in ver 1 and 2; in
  // version 3 the order is kept the same)
  CprsErr Decompress(char *dest, int dlen, char *src, int slen, char **index, const uint *lens, int nrec);
};

}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_TEXT_COMPRESSOR_H_
