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

#include "text_compressor.h"

#include "zconf.h"
#include "zlib.h"

#include "common/assert.h"
#include "compress/inc_wgraph.h"
#include "lz4.h"
#include "system/fet.h"

namespace Tianmu {
namespace compress {

// Compressed data format:		<head_ver> <body>
//
// <head_ver>:		<encoded>[1B] [<size_encod>[4B]] <version>[1B]
// [<level>[1B]]
//		<encoded> = 1 - data is encoded (CompressVer2 routine, only
// versions <= 2), 0 - not
// encoded;
//				<encoded> is stored only in CompressVer2 and
// Compress; 				CompressPlain and CompressCopy start
// from <version> 		<level> exists only if version > 0
//
// <body> in versions 1 and 2:
// <pos_part1>[4B]<pos_part2>[4B]...<pos_part_n>[4B] <part1><part2>...<part_n>
//		<pos_part_i> - position of i'th part, from the beginning of the
// buffer
//

TextCompressor::TextCompressor() : BLD_START_(0), BLD_RATIO_(0.0), graph_() {}

void TextCompressor::SetSplit(int len) {
  DEBUG_ASSERT(len > 0);
  DEBUG_ASSERT(BLD_RATIO_ > 1.01);
  double ratio = len / (double)BLD_START_;
  if (ratio < 1.0)
    ratio = 1.0;

  int n = (int)(log(ratio) / log(BLD_RATIO_)) + 1;  // no. of models (rounded downwards)
  if (n < 1)
    n = 1;

  double bld_ratio = 0.0;
  if (n > 1)
    bld_ratio = pow(ratio, 1.0 / (n - 1));  // quotient of 2 consecutive splits

  split_.resize(n + 1);
  split_[0] = 0;
  double next = BLD_START_;
  for (int i = 1; i < n; i++) {
    split_[i] = (int)next;
    DEBUG_ASSERT(split_[i] > split_[i - 1]);
    next *= bld_ratio;
  }
  split_[n] = len;
  DEBUG_ASSERT(split_[n] > split_[n - 1]);
}

void TextCompressor::SetParams(PPMParam &p, int ver, [[maybe_unused]] int lev, int len) {
  p.SetDefault();
  BLD_START_ = 64;

  switch (ver) {
    case 1:
      p.esc_count = 25;
      BLD_RATIO_ = 2.5;
      break;
    case 2:
      p.esc_count = 70;
      BLD_RATIO_ = 2.0;
      break;
    default:
      TIANMU_ERROR("not implemented");
  }

  SetSplit(len);
}

CprsErr TextCompressor::CompressCopy(char *dest, int &dlen, char *src, int slen) {
  if (dlen <= slen)
    return CprsErr::CPRS_ERR_BUF;
  dest[0] = 0;  // method: no compression
  std::memcpy(dest + 1, src, slen);
  dlen = slen + 1;
  return CprsErr::CPRS_SUCCESS;
}

CprsErr TextCompressor::CompressCopy(char *dest, int &dlen, char **index, const uint *lens, int nrec) {
  dest[0] = 0;  // method: no compression
  int dpos = 1;
  for (int i = 0; i < nrec; i++) {
    if (dpos + lens[i] > (uint)dlen)
      return CprsErr::CPRS_ERR_BUF;
    std::memcpy(dest + dpos, index[i], lens[i]);
    dpos += lens[i];
  }
  dlen = dpos;
  return CprsErr::CPRS_SUCCESS;
}

CprsErr TextCompressor::DecompressCopy(char *dest, int dlen, char *src, [[maybe_unused]] int slen, char **index,
                                       const uint *lens, int nrec) {
  // version number is already read
  int sumlen = 0;
  for (int i = 0; i < nrec; i++) {
    index[i] = dest + sumlen;
    sumlen += lens[i];
  }
  if (sumlen > dlen)
    return CprsErr::CPRS_ERR_BUF;
  std::memcpy(dest, src, sumlen);
  return CprsErr::CPRS_SUCCESS;
}

//---------------------------------------------------------------------------------------------------------

CprsErr TextCompressor::CompressPlain(char *dest, int &dlen, char *src, int slen, int ver, int lev) {
  if ((dest == nullptr) || (src == nullptr) || (dlen <= 0) || (slen <= 0))
    return CprsErr::CPRS_ERR_PAR;
  if ((ver < 0) || (ver > 2) || (lev < 1) || (lev > 9))
    return CprsErr::CPRS_ERR_VER;

  if (ver == 0)
    return CompressCopy(dest, dlen, src, slen);

  // save version and level of compression
  dest[0] = (char)ver;
  dest[1] = (char)lev;
  int dpos = 2;
  CprsErr err = CprsErr::CPRS_SUCCESS;

  // PPM
  if ((ver == 1) || (ver == 2)) {
    int clen;
    PPMParam param;
    SetParams(param, ver, lev, slen);

    // leave place in 'dest' for the array of 'dpos' of data parts
    int n = (int)split_.size() - 1;
    int *dpos_tab = (int *)(dest + dpos);
    dpos += n * sizeof(int);

    PPM::ModelType mt = ((ver == 1) ? PPM::ModelType::ModelSufTree : PPM::ModelType::ModelWordGraph);

    // loop: build next PPM model, compress next part of the data
    for (int i = 0; i < n; i++) {
      PPM ppm((uchar *)src, split_[i], mt, param);
      clen = dlen - dpos;
      err = ppm.Compress(dest + dpos, clen, (uchar *)src + split_[i], split_[i + 1] - split_[i]);
      if (static_cast<int>(err))
        break;

      dpos_tab[i] = dpos;
      dpos += clen;
    }
  }

  // is it better to simply copy the source data?
  if (((err == CprsErr::CPRS_ERR_BUF) || (dpos >= slen)) && (dlen >= slen + 1))
    return CompressCopy(dest, dlen, src, slen);

  dlen = dpos;
  return err;
}

CprsErr TextCompressor::DecompressPlain(char *dest, int dlen, char *src, int slen) {
  if ((dest == nullptr) || (src == nullptr) || (dlen <= 0) || (slen <= 0))
    return CprsErr::CPRS_ERR_PAR;
  if (slen < 2)
    return CprsErr::CPRS_ERR_BUF;

  char ver = src[0], lev = (ver > 0) ? src[1] : 1;
  int spos = (ver > 0) ? 2 : 1;

  if ((ver < 0) || (ver > 2) || (lev < 1) || (lev > 9))
    return CprsErr::CPRS_ERR_VER;

  // are the data simply copied, without compression?
  if (ver == 0) {
    if (dlen != slen - 1)
      return CprsErr::CPRS_ERR_PAR;
    std::memcpy(dest, src + 1, dlen);
    return CprsErr::CPRS_SUCCESS;
  }

  PPMParam param;
  SetParams(param, ver, lev, dlen);
  int n = (int)split_.size() - 1;

  // read array of parts' positions in 'src'
  std::vector<int> parts;
  parts.resize(n + 1);
  for (int j = 0; j < n; j++, spos += sizeof(int)) parts[j] = *(int *)(src + spos);
  parts[n] = slen;
  if (parts[n] < parts[n - 1])
    return CprsErr::CPRS_ERR_BUF;

  PPM::ModelType mt = ((ver == 1) ? PPM::ModelType::ModelSufTree : PPM::ModelType::ModelWordGraph);
  CprsErr err;

  // loop: build next PPM model, decompress next part of the data
  for (int i = 0; i < n; i++) {
    PPM ppm((uchar *)dest, split_[i], mt, param, (uchar)src[parts[i]]);
    err = ppm.Decompress((uchar *)dest + split_[i], split_[i + 1] - split_[i], src + parts[i], parts[i + 1] - parts[i]);
    if (static_cast<int>(err))
      return err;
  }

  return CprsErr::CPRS_SUCCESS;
}

CprsErr TextCompressor::CompressVer2(char *dest, int &dlen, char **index, const uint *lens, int nrec, int ver,
                                     int lev) {
  // encoding:
  //  '0' -> '1'     (zero may occur frequently and should be encoded as a
  //  single symbol) '1' -> '2' '1' '2' -> '2' '2'

  if ((!dest) || (!index) || (!lens) || (dlen <= 0) || (nrec <= 0))
    return CprsErr::CPRS_ERR_PAR;
  if (dlen < 5)
    return CprsErr::CPRS_ERR_BUF;

  // how much memory to allocate for encoded data
  int mem = 0, /*stop,*/ i;
  uint j;
  char s;
  /*for(i = 0; i < nrec; i++) {
      stop = index[i] + lens[i];
      for(j = index[i]; j < stop; j++) {
              s = src[j];
              if((s == '\1') || (s == '\2'))
                      mem += 2;
              else mem++;
      }
      mem++;		// separating '\0'
  }*/
  for (i = 0; i < nrec; i++) {
    for (j = 0; j < lens[i]; j++) {
      s = index[i][j];
      if ((s == '\1') || (s == '\2'))
        mem += 2;
      else
        mem++;
    }
    mem++;  // separating '\0'
  }

  auto data = std::make_unique<char[]>(mem);
  int sdata = 0;

  // encode, with permutation
  i = PermFirst(nrec);
  /*for(int p = 0; p < nrec; p++) {
      data[sdata++] = '\0';
      stop = index[i] + lens[i];
      for(j = index[i]; j < stop; j++) {
              s = src[j];
              if((uint)s > 2)
                      data[sdata++] = s;
              else switch(s) {
                      case '\0':   data[sdata++] = '\1'; break;
                      case '\1':   data[sdata++] = '\2'; data[sdata++] = '\1';
  break; case '\2':   data[sdata++] = '\2'; data[sdata++] = '\2';
              }
      }
      PermNext(i, nrec);
  }*/
  for (int p = 0; p < nrec; p++) {
    data[sdata++] = '\0';
    for (j = 0; j < lens[i]; j++) {
      s = index[i][j];
      if ((uint)s > 2)
        data[sdata++] = s;
      else
        switch (s) {
          case '\0':
            data[sdata++] = '\1';
            break;
          case '\1':
            data[sdata++] = '\2';
            data[sdata++] = '\1';
            break;
          case '\2':
            data[sdata++] = '\2';
            data[sdata++] = '\2';
        }
    }
    PermNext(i, nrec);
  }
  DEBUG_ASSERT(sdata == mem);
  DEBUG_ASSERT(i == PermFirst(nrec));

  // header
  dest[0] = 1;                 // data is encoded
  *(int *)(dest + 1) = sdata;  // store size of encoded data
  int dpos = 5;

  // compress
  dlen -= dpos;
  CprsErr err = CompressPlain(dest + dpos, dlen, data.get(), sdata, ver, lev);
  dlen += dpos;

  return err;
}

CprsErr TextCompressor::DecompressVer2(char *dest, int dlen, char *src, int slen, char **index, const uint * /*lens*/,
                                       int nrec) {
  if ((!dest) || (!src) /*|| (!index) */ || (slen <= 0) || (nrec <= 0))
    return CprsErr::CPRS_ERR_PAR;
  if (slen < 5)
    return CprsErr::CPRS_ERR_BUF;

  // is data encoded?
  if (src[0] != 1)
    return CprsErr::CPRS_ERR_VER;
  int spos = 1;

  // get size of encoded data
  if (slen < spos + 4)
    return CprsErr::CPRS_ERR_BUF;
  int sdata = *(int *)(src + spos);
  spos += 4;

  auto data = std::make_unique<char[]>(sdata);

  // decompress
  CprsErr err = DecompressPlain(data.get(), sdata, src + spos, slen - spos);
  if (static_cast<int>(err)) {
    return err;
  }

  // decode
  int i = PermFirst(nrec);
  int pdat = 0, pdst = 0;
  char s;

  while (pdat < sdata) {
    s = data[pdat++];
    if (s == '\0') {
      index[i] = dest + pdst;
      PermNext(i, nrec);
      continue;
    }

    if (pdst >= dlen) {
      err = CprsErr::CPRS_ERR_BUF;
      break;
    }
    if ((uint)s > 2)
      dest[pdst++] = s;
    else if (s == '\1')
      dest[pdst++] = '\0';
    else {
      if (pdat >= sdata) {
        err = CprsErr::CPRS_ERR_OTH;
        break;
      }
      s = data[pdat++];
      if ((s == '\1') || (s == '\2'))
        dest[pdst++] = s;
      else {
        err = CprsErr::CPRS_ERR_OTH;
        break;
      }
    }
  }

  if (static_cast<int>(err))
    return err;
  if (i != PermFirst(nrec))
    return CprsErr::CPRS_ERR_OTH;
  return CprsErr::CPRS_SUCCESS;
}

CprsErr TextCompressor::CompressVer4(char *dest, int &dlen, char **index, const uint *lens, int nrec,
                                     [[maybe_unused]] int ver, [[maybe_unused]] int lev, uint packlen) {
  if ((!dest) || (!index) || (!lens) || (dlen <= 0) || (nrec <= 0) || (packlen <= 0))
    return CprsErr::CPRS_ERR_PAR;
  int length = 0;
  int pos = 0;
  auto srcdata = std::make_unique<char[]>(packlen);
  for (int i = 0; i < nrec; i++) {
    std::memcpy(srcdata.get() + length, index[i], lens[i]);
    length += lens[i];
  }
  pos += 4;  // store size of encoded data

  const int cmpBytes = LZ4_compress_default(srcdata.get(), dest + pos, length, dlen - pos);

  if (cmpBytes <= 0 || cmpBytes > dlen - pos) {
    TIANMU_LOG(LogCtl_Level::ERROR, "CompressVer4 error,cmpBytes: %d dlen - pos = %d", cmpBytes, dlen - pos);
    return CprsErr::CPRS_ERR_OTH;
  }
  *(int *)(dest) = (int)cmpBytes;  // store size of encoded data
  dlen = cmpBytes + 4;

  return CprsErr::CPRS_SUCCESS;
}

CprsErr TextCompressor::DecompressVer4(char *dest, int dlen, char *src, int slen, char **index, const uint *lens,
                                       int nrec) {
  int spos = 0;
  if ((!dest) || (!src) || (slen <= 0) || (nrec <= 0))
    return CprsErr::CPRS_ERR_PAR;
  int datalen = *(int *)(src);
  spos += 4;

  const int decBytes = LZ4_decompress_safe(src + spos, dest, datalen, dlen);
  if (decBytes <= 0 || decBytes > dlen) {
    TIANMU_LOG(LogCtl_Level::ERROR, "DecompressVer4 error,decBytes: %d dlen = %d", decBytes, dlen);
    return CprsErr::CPRS_ERR_OTH;
  }

  size_t sumlen = 0;
  for (int i = 0; i < nrec; i++) {
    index[i] = dest + sumlen;
    sumlen += lens[i];
  }
  return CprsErr::CPRS_SUCCESS;
}

CprsErr TextCompressor::CompressZlib(char *dest, int &dlen, char **index, const uint *lens, int nrec, uint packlen) {
  if ((!dest) || (!index) || (!lens) || (dlen <= 4) || (nrec <= 0) || (packlen == 0))
    return CprsErr::CPRS_ERR_PAR;
  uint64_t srclen = 0;
  std::unique_ptr<unsigned char> srcdata(new unsigned char[packlen]);
  for (int i = 0; i < nrec; i++) {
    memcpy(srcdata.get() + srclen, index[i], lens[i]);
    srclen += lens[i];
  }

  uint32_t pos = 4;  // reserve to store compressed buffer len
  uint64_t destlen = dlen - pos;
  int ret = compress2(reinterpret_cast<Bytef *>(dest + pos), &destlen, srcdata.get(), srclen, Z_DEFAULT_COMPRESSION);
  if (ret != Z_OK) {
    TIANMU_LOG(LogCtl_Level::ERROR, "compress2 failure %d, destlen: %lu, srclen %lu, packlen %u", ret, destlen, srclen,
               packlen);
    return CprsErr::CPRS_ERR_OTH;
  }
  *(reinterpret_cast<uint32_t *>(dest)) = static_cast<uint32_t>(destlen);
  dlen = destlen + pos;
  return CprsErr::CPRS_SUCCESS;
}

CprsErr TextCompressor::DecompressZlib(char *dest, int dlen, char *src, int slen, char **index, const uint *lens,
                                       int nrec) {
  if ((!dest) || (!src) || (slen <= 0) || (nrec <= 0))
    return CprsErr::CPRS_ERR_PAR;
  uint64_t srclen = *(reinterpret_cast<uint32_t *>(src));
  uint32_t spos = 4;
  uint64_t destlen = dlen;
  const int ret =
      uncompress(reinterpret_cast<Bytef *>(dest), &destlen, reinterpret_cast<const Bytef *>(src + spos), srclen);
  if (ret != Z_OK) {
    TIANMU_LOG(LogCtl_Level::ERROR, "uncompress error: %d, srclen: %lu destlen = %d", ret, srclen, dlen);
    return CprsErr::CPRS_ERR_OTH;
  }
  size_t sumlen = 0;
  for (int i = 0; i < nrec; i++) {
    index[i] = dest + sumlen;
    sumlen += lens[i];
  }
  return CprsErr::CPRS_SUCCESS;
}

CprsErr TextCompressor::Compress(char *dest, int &dlen, char **index, const uint *lens, int nrec, uint &packlen,
                                 int ver, int lev) {
  if ((!dest) || (!index) || (!lens) || (dlen <= 0) || (nrec <= 0))
    return CprsErr::CPRS_ERR_PAR;
  if ((ver < 0) || (ver > MAXVER_) || (lev < 1) || (lev > 9))
    return CprsErr::CPRS_ERR_VER;

  int slen = packlen;

  if ((ver == 1) || (ver == 2))
    return CompressVer2(dest, dlen, index, lens, nrec, ver, lev);

  dest[0] = 0;  // not encoded
  int dpos = 1;
  CprsErr err = CprsErr::CPRS_SUCCESS;

  if (ver == 0) {
    dlen -= dpos;
    err = CompressCopy(dest + dpos, dlen, index, lens, nrec);
    dlen += dpos;
    return err;
  }
  dest[dpos++] = (char)ver;  // version and level of compression
  dest[dpos++] = (char)lev;
  // add lz4 compress ver=4
  if (ver == 4) {
    dlen -= dpos;
    err = CompressVer4(dest + dpos, dlen, index, lens, nrec, ver, lev, packlen);
    dlen += dpos;
    return err;
  } else if (ver == static_cast<int>(common::PackFmt::ZLIB)) {
    dlen -= dpos;
    err = CompressZlib(dest + dpos, dlen, index, lens, nrec, packlen);
    dlen += dpos;
    return err;
  }

  // Version 3
  // compress with IncWGraph
  try {
    RangeCoder coder;
    coder.InitCompress(dest, dlen, dpos);
    graph_.Encode(&coder, index, lens, nrec, packlen);
    coder.EndCompress();
    dpos = coder.GetPos();
  } catch (CprsErr &e) {
    err = e;
  }

  // check if copy compression is better
  if (((err == CprsErr::CPRS_ERR_BUF) || (dpos >= slen)) && (dlen >= slen + 2)) {
    dpos = 1;  // leave first byte of the header
    dlen -= dpos;
    err = CompressCopy(dest + dpos, dlen, index, lens, nrec);
    dlen += dpos;
    packlen = (uint)slen;
  } else
    dlen = dpos;

  return err;
}

CprsErr TextCompressor::Decompress(char *dest, int dlen, char *src, int slen, char **index, const uint *lens,
                                   int nrec) {
  MEASURE_FET("TextCompressor::Decompress(...)");
  if ((!dest) || (!src) || (!index) || (slen <= 0) || (nrec <= 0))
    return CprsErr::CPRS_ERR_PAR;
  if (slen < 2)
    return CprsErr::CPRS_ERR_BUF;

  // old versions
  if (src[0])
    return DecompressVer2(dest, dlen, src, slen, index, lens, nrec);
  int spos = 1;

  // copy compress
  char ver = src[spos++];
  if (ver == 0)
    return DecompressCopy(dest, dlen, src + spos, slen - spos, index, lens, nrec);

  // Version 3
  if (slen <= spos)
    return CprsErr::CPRS_ERR_BUF;
  char lev = src[spos++];
  // add lz4 decompress
  if (ver == 4)
    return DecompressVer4(dest, dlen, src + spos, slen - spos, index, lens, nrec);
  else if (ver == static_cast<char>(common::PackFmt::ZLIB))
    return DecompressZlib(dest, dlen, src + spos, slen - spos, index, lens, nrec);

  if ((ver != 3) || (lev < 1) || (lev > 9))
    return CprsErr::CPRS_ERR_VER;

  // check if 'dlen' is large enough
  // int sumlen = 0;
  // for(int i = 0; i < nrec; i++)
  //	sumlen += lens[i];
  // if(sumlen > dlen) return CprsErr::CPRS_ERR_BUF;

  // decompress with IncWGraph
  try {
    RangeCoder coder;
    coder.InitDecompress(src, slen, spos);
    graph_.Decode(&coder, index, lens, nrec, dest, dlen);
  } catch (CprsErr &e) {
    return e;
  }
  return CprsErr::CPRS_SUCCESS;
}

}  // namespace compress
}  // namespace Tianmu
