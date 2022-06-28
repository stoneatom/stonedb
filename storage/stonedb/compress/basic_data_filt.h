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
#ifndef STONEDB_COMPRESS_BASIC_DATA_FILT_H_
#define STONEDB_COMPRESS_BASIC_DATA_FILT_H_
#pragma once

#include <cstring>

#include "compress/data_filt.h"
#include "compress/dictionary.h"
#include "core/bin_tools.h"
#include "core/quick_math.h"

namespace stonedb {
namespace compress {

// Maximum no. of records allowed in compression routines
#define CPRS_MAXREC 65536

/* RLE */
template <class T>
class DataFilt_RLE : public DataFilt<T> {
  static const ushort MAXBLEN = 16;
  ushort lens[CPRS_MAXREC];
  uint nblk;
  uint lencnt[MAXBLEN + 1];
  uint merge_nrec;
  Dictionary<T> dict;

  void Clear() {
    std::memset(lencnt, 0, sizeof(lencnt));
    nblk = 0;
  }
  void AddLen(ushort len) {
    lens[nblk++] = len;
    lencnt[len]++;
  }

 public:
  DataFilt_RLE() = default;
  virtual ~DataFilt_RLE() = default;

  char const *GetName() override { return "rle"; }
  bool Encode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Decode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Merge(DataSet<T> *dataset) override;
};

/* Subtracting minimum from data */
template <class T>
class DataFilt_Min : public DataFilt<T> {
  T minval;

 public:
  char const *GetName() override { return "min"; }
  bool Encode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Decode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Merge(DataSet<T> *dataset) override;
};

/* Dividing data by GCD */
template <class T>
class DataFilt_GCD : public DataFilt<T> {
  T gcd;

 public:
  char const *GetName() override { return "gcd"; }
  bool Encode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Decode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Merge(DataSet<T> *dataset) override;
};

/* Data differencing */
template <class T>
class DataFilt_Diff : public DataFilt<T> {
  static const uint MAXSAMP = 65536 / 20;
  static const uchar BITDICT = 8;
  T sample[MAXSAMP];
  Dictionary<uchar> dict;
  // T newmin, newmax, merge_maxval;
  // static int compare(const void* p1, const void* p2);		// for
  // sorting array in increasing order
  double Entropy(T *data, uint nrec, uchar bitdict, uchar bitlow, bool top);
  double Measure(DataSet<T> *dataset, bool diff);

 public:
  DataFilt_Diff() = default;
  virtual ~DataFilt_Diff() = default;
  char const *GetName() override { return "diff"; }
  bool Encode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Decode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Merge(DataSet<T> *dataset) override;
};

/* Uniform compression */
template <class T>
class DataFilt_Uniform : public DataFilt<T> {
 public:
  char const *GetName() override { return "uni"; }
  bool Encode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Decode(RangeCoder *coder, DataSet<T> *dataset) override;
  void Merge(DataSet<T> *dataset) override;
};

}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_BASIC_DATA_FILT_H_
