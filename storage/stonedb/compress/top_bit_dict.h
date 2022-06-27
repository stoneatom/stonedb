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
#ifndef STONEDB_COMPRESS_TOP_BIT_DICT_H_
#define STONEDB_COMPRESS_TOP_BIT_DICT_H_
#pragma once

#include "compress/data_filt.h"
#include "compress/dictionary.h"
#include "compress/range_code.h"

namespace stonedb {
namespace compress {

// Full dictionary of highest bits
template <class T>
class TopBitDict : public DataFilt<T> {
 public:
  static const uint MAXTOTAL = RangeCoder::MAX_TOTAL;
  static const uint MAXLEN = DICMAP_MAX;
  static const uint BITSTART = 7;
  static const uint BITSTEP = 2;
  static const uint KEYOCCUR = 8;
  static const double MINPREDICT;
  enum class TopBottom { tbTop, tbBottom };  // which part of bits is compressed

 private:
  const TopBottom topbottom;

  // uint* levels[2];		// for temporary use in FindOptimum()
  // T* datasort;

  // struct KeyRange {
  //	uint64_t key;
  //	uint count, low, high;
  //	void Set(uint64_t k, uint c, uint l, uint h)	{ key=k; count=c; low=l;
  // high=h; }
  //};
  // uint total;

  Dictionary<T> counters[2];
  T bitlow;  // no. of bits in lower part

  // for merging data during decompression
  T decoded[MAXLEN];  // decoded 'highs' of values

  T maxval_merge;

  // Finds optimum no. of bits to store in the dictionary.
  uint FindOptimum(DataSet<T> *dataset, uint nbit, uint &opt_bit, Dictionary<T> *&opt_dict);
  bool Insert(Dictionary<T> *dict, T *data, uint nbit, uint bit, uint nrec, uint skiprec);
  static const uint INF = UINT_MAX;

  virtual void LogCompress(FILE *f) { std::fprintf(f, "%u %u", this->codesize[0], this->codesize[1]); }

 public:
  TopBitDict(bool top) : topbottom(top ? TopBottom::tbTop : TopBottom::tbBottom) {}
  virtual ~TopBitDict() = default;
  char const *GetName() override { return topbottom == TopBottom::tbTop ? (char *)"top" : (char *)"low"; }
  bool Encode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Decode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Merge(DataSet<T> *dataset) override;
};

}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_TOP_BIT_DICT_H_
