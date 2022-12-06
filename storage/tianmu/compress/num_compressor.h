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
#ifndef TIANMU_COMPRESS_NUM_COMPRESSOR_H_
#define TIANMU_COMPRESS_NUM_COMPRESSOR_H_
#pragma once

#include "common/assert.h"
#include "compress/basic_data_filt.h"
#include "compress/defs.h"
#include "compress/part_dict.h"
#include "compress/range_code.h"
#include "compress/top_bit_dict.h"
#include "core/quick_math.h"
#include "core/tools.h"
#include "system/fet.h"

namespace Tianmu {
namespace compress {

// NOTE: if possible, REUSE the same NumCompressor object instead of destroying
// and allocating a new one!!! This is more efficient, as it prevents memory
// reallocation (every object allocates the same amount of memory - several MB -
// during creation)

// Non-templated interface of NumCompressor
class NumCompressorBase {
 public:
  virtual ~NumCompressorBase() {}
  static const uint N_FILTERS_ = 7;
  FILE *dump_;

  // statistics
  struct Stat {
    clock_t tc, td;  // times of compression and decomp.
    void Clear() { std::memset(this, 0, sizeof(Stat)); }
    Stat() { Clear(); }
    Stat &operator+=(Stat &s) {
      tc += s.tc;
      td += s.td;
      return *this;
    }
  };
  struct Stats : public std::vector<Stat> {
    Stats() : std::vector<Stat>(NumCompressorBase::N_FILTERS_) {}
    // Stats(size_type cnt): std::vector<Stat>(cnt) {}
    Stats &operator+=(Stats &s) {
      ASSERT(size() == s.size(), "should be 'size() == s.size()'");
      for (size_type i = 0; i < size(); i++) (*this)[i] += s[i];
      return *this;
    }
    Stats &operator=(Stats &s) {
      assign(s.begin(), s.end());
      return *this;
    }
    void Clear() {
      for (size_type i = 0; i < size(); i++) (*this)[i].Clear();
    }
  };
  Stats stats_;

  // non-templated type-safe compression methods
  virtual CprsErr Compress([[maybe_unused]] char *dest, [[maybe_unused]] uint &len, [[maybe_unused]] const uchar *src,
                           [[maybe_unused]] uint nrec, [[maybe_unused]] uchar maxval,
                           [[maybe_unused]] CprsAttrType cat = CprsAttrType::CAT_OTHER) {
    TIANMU_ERROR("method should not be invoked");
    return CprsErr::CPRS_ERR_OTH;
  }

  virtual CprsErr Compress([[maybe_unused]] char *dest, [[maybe_unused]] uint &len, [[maybe_unused]] const ushort *src,
                           [[maybe_unused]] uint nrec, [[maybe_unused]] ushort maxval,
                           [[maybe_unused]] CprsAttrType cat = CprsAttrType::CAT_OTHER) {
    TIANMU_ERROR("method should not be invoked");
    return CprsErr::CPRS_ERR_OTH;
  }

  virtual CprsErr Compress([[maybe_unused]] char *dest, [[maybe_unused]] uint &len, [[maybe_unused]] const uint *src,
                           [[maybe_unused]] uint nrec, [[maybe_unused]] uint maxval,
                           [[maybe_unused]] CprsAttrType cat = CprsAttrType::CAT_OTHER) {
    TIANMU_ERROR("method should not be invoked");
    return CprsErr::CPRS_ERR_OTH;
  }

  virtual CprsErr Compress([[maybe_unused]] char *dest, [[maybe_unused]] uint &len,
                           [[maybe_unused]] const uint64_t *src, [[maybe_unused]] uint nrec,
                           [[maybe_unused]] uint64_t maxval,
                           [[maybe_unused]] CprsAttrType cat = CprsAttrType::CAT_OTHER) {
    TIANMU_ERROR("method should not be invoked");
    return CprsErr::CPRS_ERR_OTH;
  }

  virtual CprsErr Decompress([[maybe_unused]] uchar *dest, [[maybe_unused]] char *src, [[maybe_unused]] uint len,
                             [[maybe_unused]] uint nrec, [[maybe_unused]] uchar maxval,
                             [[maybe_unused]] CprsAttrType cat = CprsAttrType::CAT_OTHER) {
    TIANMU_ERROR("method should not be invoked");
    return CprsErr::CPRS_ERR_OTH;
  }

  virtual CprsErr Decompress([[maybe_unused]] ushort *dest, [[maybe_unused]] char *src, [[maybe_unused]] uint len,
                             [[maybe_unused]] uint nrec, [[maybe_unused]] ushort maxval,
                             [[maybe_unused]] CprsAttrType cat = CprsAttrType::CAT_OTHER) {
    TIANMU_ERROR("method should not be invoked");
    return CprsErr::CPRS_ERR_OTH;
  }

  virtual CprsErr Decompress([[maybe_unused]] uint *dest, [[maybe_unused]] char *src, [[maybe_unused]] uint len,
                             [[maybe_unused]] uint nrec, [[maybe_unused]] uint maxval,
                             [[maybe_unused]] CprsAttrType cat = CprsAttrType::CAT_OTHER) {
    TIANMU_ERROR("method should not be invoked");
    return CprsErr::CPRS_ERR_OTH;
  }

  virtual CprsErr Decompress([[maybe_unused]] uint64_t *dest, [[maybe_unused]] char *src, [[maybe_unused]] uint len,
                             [[maybe_unused]] uint nrec, [[maybe_unused]] uint64_t maxval,
                             [[maybe_unused]] CprsAttrType cat = CprsAttrType::CAT_OTHER) {
    TIANMU_ERROR("method should not be invoked");
    return CprsErr::CPRS_ERR_OTH;
  }

  // non-templated NON-type-safe compression methods - use them carefully!
  // make sure that you invoke them for object of appropriate type
  virtual CprsErr Compress(char *dest, uint &len, const void *src, uint nrec, uint64_t maxval,
                           CprsAttrType cat = CprsAttrType::CAT_OTHER) = 0;
  virtual CprsErr Decompress(void *dest, char *src, uint len, uint nrec, uint64_t maxval,
                             CprsAttrType cat = CprsAttrType::CAT_OTHER) = 0;
};

// THE NumCompressor (templated)
template <class T>
class NumCompressor : public NumCompressorBase {
 private:
  bool copy_only_;

  // compress by simple copying the data
  CprsErr CopyCompress(char *dest, uint &len, const T *src, uint nrec);
  CprsErr CopyDecompress(T *dest, char *src, uint len, uint nrec);

  void DumpData(DataSet<T> *, uint);

 public:
  // Filters - compression algorithms:
  std::vector<std::unique_ptr<DataFilt<T>>> filters_;

  NumCompressor(bool copy_only = false);
  virtual ~NumCompressor();

  // 'len' - length of 'dest' in BYTES; upon exit contains actual size of
  // compressed data,
  //         which is at most size_of_original_data+20
  // 'nrec' must be at most CPRS_MAXREC
  // 'maxval' - maximum value in the data
  // Data in 'src' are NOT changed.
  CprsErr CompressT(char *dest, uint &len, const T *src, uint nrec, T maxval,
                    CprsAttrType cat = CprsAttrType::CAT_OTHER);

  // 'len' - the value of 'len' returned from Compress()
  // 'dest' must be able to hold at least 'nrec' elements of type T
  CprsErr DecompressT(T *dest, char *src, uint len, uint nrec, T maxval, CprsAttrType cat = CprsAttrType::CAT_OTHER);

  using NumCompressorBase::Compress;
  using NumCompressorBase::Decompress;
  CprsErr Compress(char *dest, uint &len, const T *src, uint nrec, T maxval,
                   CprsAttrType cat = CprsAttrType::CAT_OTHER) override {
    MEASURE_FET("NumCompressor::Compress(...)");
    return CompressT(dest, len, src, nrec, maxval, cat);
  }

  CprsErr Decompress(T *dest, char *src, uint len, uint nrec, T maxval,
                     CprsAttrType cat = CprsAttrType::CAT_OTHER) override {
    MEASURE_FET("NumCompressor::Decompress(...)");
    return DecompressT(dest, src, len, nrec, maxval, cat);
  }

  CprsErr Compress(char *dest, uint &len, const void *src, uint nrec, uint64_t maxval,
                   CprsAttrType cat = CprsAttrType::CAT_OTHER) override {
    MEASURE_FET("NumCompressor::Compress(...)");
    return CompressT(dest, len, (T *)src, nrec, (T)maxval, cat);
  }

  CprsErr Decompress(void *dest, char *src, uint len, uint nrec, uint64_t maxval,
                     CprsAttrType cat = CprsAttrType::CAT_OTHER) override {
    MEASURE_FET("NumCompressor::Decompress(...)");
    return DecompressT((T *)dest, src, len, nrec, (T)maxval, cat);
  }
};

//-------------------------------------------------------------------------

// The same routines as in NumCompressor, but without the need to manually
// create an object of appropriate type T
template <class T>
inline CprsErr NumCompress(char *dest, uint &len, const T *src, uint nrec, uint64_t maxval) {
  NumCompressor<T> nc;
  return nc.Compress(dest, len, src, nrec, (T)maxval);
}
template <class T>
inline CprsErr NumDecompress(T *dest, char *src, uint len, uint nrec, uint64_t maxval) {
  NumCompressor<T> nc;
  return nc.Decompress(dest, src, len, nrec, (T)maxval);
}

//-------------------------------------------------------------------------

template <class T>
NumCompressor<T>::NumCompressor(bool copy_only) : copy_only_(copy_only) {
  dump_ = nullptr;

  // Create filters_
  filters_.reserve(N_FILTERS_);
  // filters_.emplace_back(new DataFilt_RLE<T>);
  filters_.emplace_back(new DataFilt_Min<T>);
  filters_.emplace_back(new DataFilt_GCD<T>);
  filters_.emplace_back(new DataFilt_Diff<T>);
  filters_.emplace_back(new PartDict<T>);
  filters_.emplace_back(new TopBitDict<T>(true));   // top bits
  filters_.emplace_back(new TopBitDict<T>(false));  // low bits
  filters_.emplace_back(new DataFilt_Uniform<T>);
  ASSERT(filters_.size() == N_FILTERS_, "should be 'filters_.size() == N_FILTERS_'");
  IFSTAT(stats_.resize(filters_.size()));
}

template <class T>
NumCompressor<T>::~NumCompressor() {}

//-------------------------------------------------------------------------

template <class T>
void NumCompressor<T>::DumpData(DataSet<T> *ds, uint f) {
  if (!dump_)
    return;
  uint nbit = core::GetBitLen(ds->maxval);
  std::fprintf(dump_, "%u:  %u %I64u %u ;  ", f, ds->nrec, (uint64_t)ds->maxval, nbit);
  if (f)
    filters_[f - 1]->LogCompress(dump_);
  std::fprintf(dump_, "\n");
}
//-------------------------------------------------------------------------

template <class T>
CprsErr NumCompressor<T>::CopyCompress(char *dest, uint &len, const T *src, uint nrec) {
  uint datalen = nrec * sizeof(T);
  if (len < 1 + datalen)
    return CprsErr::CPRS_ERR_BUF;
  *dest = 0;  // ID of copy compression
  std::memcpy(dest + 1, src, datalen);
  len = 1 + datalen;
  return CprsErr::CPRS_SUCCESS;
}

template <class T>
CprsErr NumCompressor<T>::CopyDecompress(T *dest, char *src, uint len, uint nrec) {
  uint datalen = nrec * sizeof(T);
  if (len < 1 + datalen)
    throw CprsErr::CPRS_ERR_BUF;
  std::memcpy(dest, src + 1, datalen);
  return CprsErr::CPRS_SUCCESS;
}

template <class T>
CprsErr NumCompressor<T>::CompressT(char *dest, uint &len, const T *src, uint nrec, T maxval,
                                    [[maybe_unused]] CprsAttrType cat) {
  // Format:    <ver>[1B] <compressID>[1B] <compressed_data>[...]
  // <ver>=0  - copy compression
  // <ver>=1  - current version

  if (!dest || !src || (len < 3))
    return CprsErr::CPRS_ERR_BUF;
  if ((nrec == 0) || (maxval == 0))
    return CprsErr::CPRS_ERR_PAR;

  if (copy_only_)
    return CopyCompress(dest, len, src, nrec);

  *dest = 1;                // version
  uint posID = 1, pos = 3;  // 1 byte reserved for compression ID
  ushort ID = 0;
  ASSERT(filters_.size() <= 8 * sizeof(ID), "should be 'filters_.size() <= 8*sizeof(ID)'");

  std::vector<T> buf(src, src + nrec);
  DataSet<T> dataset = {&buf[0], maxval, nrec};

  CprsErr err = CprsErr::CPRS_SUCCESS;
  try {
    RangeCoder coder;
    coder.InitCompress(dest, len, pos);
    IFSTAT(stats_.Clear());

    // main loop: apply all compression algorithms from 'filters_'
    uint f = 0;
    IFSTAT(DumpData(&dataset, f));
    for (; (f < filters_.size()) && dataset.nrec; f++) {
      IFSTAT(clock_t t1 = clock());
      if (filters_[f]->Encode(&coder, &dataset))
        ID |= 1 << f;
      IFSTAT(stats_[f].tc += clock() - t1);
      IFSTAT(if (ID & (1 << f)) DumpData(&dataset, f + 1));
    }

    // finish and store compression ID
    coder.EndCompress();
    pos = coder.GetPos();
    *(ushort *)(dest + posID) = ID;
  } catch (CprsErr &err_) {
    err = err_;
  }

  // if compression failed or the size is bigger than the raw data, use copy
  // compression
  if (static_cast<int>(err) || (pos >= 0.98 * nrec * sizeof(T)))
    return CopyCompress(dest, len, src, nrec);

  len = pos;
  return CprsErr::CPRS_SUCCESS;
}

template <class T>
CprsErr NumCompressor<T>::DecompressT(T *dest, char *src, uint len, uint nrec, T maxval,
                                      [[maybe_unused]] CprsAttrType cat) {
  if (!src || (len < 1))
    return CprsErr::CPRS_ERR_BUF;
  if ((nrec == 0) || (maxval == 0))
    return CprsErr::CPRS_ERR_PAR;

  uchar ver = (uchar)src[0];
  if (ver == 0)
    return CopyDecompress(dest, src, len, nrec);
  if (len < 3)
    return CprsErr::CPRS_ERR_BUF;
  ushort ID = *(ushort *)(src + 1);
  uint pos = 3;

  DataSet<T> dataset = {dest, maxval, nrec};

  try {
    RangeCoder coder;
    coder.InitDecompress(src, len, pos);
    uint f = 0, nfilt = (uint)filters_.size();
    IFSTAT(stats_.Clear());

    // 1st stage of decompression
    for (; (f < nfilt) && dataset.nrec; f++) {
      IFSTAT(clock_t t1 = clock());
      if (ID & (1 << f))
        filters_[f]->Decode(&coder, &dataset);
      IFSTAT(stats_[f].td += clock() - t1);
    }

    // 2nd stage
    for (; f > 0;) {
      IFSTAT(clock_t t1 = clock());
      if (ID & (1 << --f))
        filters_[f]->Merge(&dataset);
      IFSTAT(stats_[f].td += clock() - t1);
    }
  } catch (CprsErr &err) {
    return err;
  }

  return CprsErr::CPRS_SUCCESS;
}

}  // namespace compress
}  // namespace Tianmu

#endif  // TIANMU_COMPRESS_NUM_COMPRESSOR_H_
