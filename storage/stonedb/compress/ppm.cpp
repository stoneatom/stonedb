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

#include "ppm.h"

#include "common/assert.h"
#include "compress/data_stream.h"
#include "compress/range_code.h"
#include "compress/suffix_tree.h"
#include "compress/word_graph.h"

namespace stonedb {
namespace compress {

FILE *PPM::dump = NULL;
bool PPM::printstat = false;

PPM::PPM(const Symb *data, int dlen, ModelType mt, PPMParam param, uchar method) {
  if ((data == NULL) || (dlen <= 0) || (mt == ModelType::ModelNull)) return;

  switch (mt) {
    case ModelType::ModelSufTree:
      model.reset(new SuffixTree<>(data, dlen));
      break;
    case ModelType::ModelWordGraph:
      model.reset(new WordGraph(data, dlen, method == 2));
      break;
    default:
      STONEDB_ERROR("not implemented");
  }
  model->TransformForPPM(param);

  static int _i_ = 0;
  if (printstat && dump && (++_i_ == 18)) model->PrintStat(dump);
}

CprsErr PPM::CompressArith(char *dest, int &dlen, Symb *src, int slen) {
  // Data format:
  //  <compr_method>[1B] <compressed_data>[?]

  // null PPM model
  if (!model) {
    if (dlen < slen + 1) return CprsErr::CPRS_ERR_BUF;
    dest[0] = 0;  // method: no compression
    std::memcpy(dest + 1, src, slen);
    dlen = slen + 1;
    return CprsErr::CPRS_SUCCESS;
  }
  WordGraph *wg = NULL;
  try {
    wg = dynamic_cast<WordGraph *>(model.get());
  } catch (...) {
    wg = NULL;
  }
  if (wg) ASSERT(wg->insatend == false, "should be 'wg->insatend == false'");

  // DEBUG_ASSERT(src[slen-1] == 0);
  ArithCoder coder;
  BitStream bs_dest(dest, dlen * 8);
  bool overflow = false;
  int clen = 0;

  // try to compress
  try {
    bs_dest.PutByte(1);  // compression method [1 byte], currently 1; 0 means no
                         // compression (data are copied)

    // SufTree::State stt = model->InitState();
    // SufTree::Edge edge;
    Range rng;
    Count total;

    model->InitPPM();
    model->logfile = dump;
    coder.InitCompress();

    // int esc = 0;
    int len;
    for (int i = 0; i < slen;) {
      // if(dump) std::fprintf(dump, "%d %d %d\n", _n_++, i, stt);
      // if(_n_ >= 14860)//314712)
      //	i = i;

      len = slen - i;
      model->Move(src + i, len, rng, total);
      i += len;

      // model->FindEdgeS(stt, edge, src + i, slen - i);
      // model->GetRange(stt, edge, rng);
      // total = model->GetTotal(stt);
      // DEBUG_ASSERT(rng.high <= total);
      // len = model->GetLen(stt, edge);

      // encode 'rng'
      coder.ScaleRange(&bs_dest, rng.low, rng.high, total);

      // #			ifdef SUFTREE_STAT
      //			//if(edge.n == 0) esc += bs_dest.GetPos() -
      // pos1;		// count bits used to
      //  encode ESC symbols
      //			if(makedump && dump) {
      //				std::fprintf(dump, "%d\t",
      // model->GetDep(stt)); 				if(edge.n == 0)
      // std::fprintf(dump, "<-"); 				else {
      // for(int j
      //= i; j < i+len; j++) {
      // char s = (char)src[j];
      // if((s == 0) || (s == 10)
      //|| (s == 13) || (s == '\t')) s =
      //'#';
      //						fputc(s, dump);
      //					}
      //				}
      //				std::fprintf(dump, "\t%d\n",
      // bs_dest.GetPos() - pos1);
      //			}
      // #			endif
      //
      //			i += len;
      //			model->Move(stt, edge);
    }
    // if(dump) std::fprintf(dump, "Bytes used to encode ESC symbols: %d\n",
    // (esc+7)/8);

    coder.EndCompress(&bs_dest);

    clen = (bs_dest.GetPos() + 7) / 8;
  } catch (ErrBufOverrun &) {
    overflow = true;
  }

  // should we simply copy the source data?
  if ((overflow || (clen >= slen)) && (dlen >= slen + 1)) {
    dest[0] = 0;  // method: no compression
    std::memcpy(dest + 1, src, slen);
    clen = slen + 1;
  } else if (overflow)
    return CprsErr::CPRS_ERR_BUF;

  dlen = clen;
  return CprsErr::CPRS_SUCCESS;
}

CprsErr PPM::DecompressArith(Symb *dest, int dlen, char *src, int slen) {
  // if(slen < 1) return CprsErr::CPRS_ERR_BUF;
  // uchar method = (uchar) src[0];
  //
  //// are the data simply copied, without compression?
  // if(method == 0) {
  //	if(dlen != slen - 1) return CprsErr::CPRS_ERR_PAR;
  //	std::memcpy(dest, src + 1, dlen);
  //	return CprsErr::CPRS_SUCCESS;
  //}
  // if(method != 1) return CprsErr::CPRS_ERR_VER;

  WordGraph *wg = NULL;
  try {
    wg = dynamic_cast<WordGraph *>(model.get());
  } catch (...) {
    wg = NULL;
  }
  if (wg) ASSERT(wg->insatend == false, "should be 'wg->insatend == false'");

  ArithCoder coder;
  BitStream bs_src(src, slen * 8, 8);  // 1 byte already read
  try {
    // SufTree::State stt = model->InitState();
    // SufTree::Edge edge;
    Range rng;
    Count c, total;
    int len;
    CprsErr err;

    model->InitPPM();
    coder.InitDecompress(&bs_src);

    // int _n_ = 0;
    for (int i = 0; i < dlen;) {
      // if(dump) std::fprintf(dump, "%d %d %d\n", _n_++, i, stt);

      // find the next edge to move
      total = model->GetTotal();
      c = coder.GetCount(total);

      len = dlen - i;
      err = model->Move(c, dest + i, len, rng);
      if (static_cast<int>(err)) return err;
      i += len;

      // model->FindEdgeC(stt, edge, c);

      // remove the decoded data from the source
      // model->GetRange(stt, edge, rng);
      // DEBUG_ASSERT(rng.high <= total);
      coder.RemoveSymbol(&bs_src, rng.low, rng.high, total);

      // get label of the edge, put it into 'dest'
      // if(_n_ >= 14860)//314712)
      //	i = i;

      // len = dlen - i;
      // err = model->GetLabel(stt, edge, dest + i, len);
      // if(static_cast<int>(err))
      //	return err;
      // i += len;

      // model->Move(stt, edge);
    }
  } catch (ErrBufOverrun &) {
    return CprsErr::CPRS_ERR_BUF;
  }

  return CprsErr::CPRS_SUCCESS;
}

CprsErr PPM::Compress(char *dest, int &dlen, Symb *src, int slen) {
  // return CompressArith(dest, dlen, src, slen);

  // null PPM model
  if (!model) {
    if (dlen < slen + 1) return CprsErr::CPRS_ERR_BUF;
    dest[0] = 0;  // method: no compression
    std::memcpy(dest + 1, src, slen);
    dlen = slen + 1;
    return CprsErr::CPRS_SUCCESS;
  }
  // try {
  //	WordGraph* wg = dynamic_cast<WordGraph*>(model);
  //	if(wg) wg->insatend = true;
  //} catch(...){}

  if (dlen < 1) return CprsErr::CPRS_ERR_BUF;
  dest[0] = 2;  // compression method: with RangeCoder

  WordGraph *wg = NULL;
  try {
    wg = dynamic_cast<WordGraph *>(model.get());
  } catch (...) {
    wg = NULL;
  }

  if (wg) ASSERT(wg->insatend, "'wg->insatend' should be true");

  RangeCoder coder;
  coder.InitCompress(dest + 1, dlen - 1);
  bool overflow = false;
  int clen = 0;

  // try to compress
  try {
    Range rng;
    Count total;

    model->InitPPM();
    model->logfile = dump;

    int len;
    for (int i = 0; i < slen;) {
      len = slen - i;
      model->Move(src + i, len, rng, total);
      i += len;
      coder.Encode(rng.low, rng.high - rng.low, total);
    }
    coder.EndCompress();
    clen = 1 + (int)coder.GetPos();
  } catch (CprsErr &e) {
    if (e == CprsErr::CPRS_ERR_BUF)
      overflow = true;
    else
      throw;
  }
  // catch(BufOverException) { overflow = true; }

  // should we simply copy the source data?
  if ((overflow || (clen >= slen)) && (dlen >= slen + 1)) {
    dest[0] = 0;  // method: no compression
    std::memcpy(dest + 1, src, slen);
    clen = slen + 1;
  } else if (overflow)
    return CprsErr::CPRS_ERR_BUF;

  dlen = clen;
  return CprsErr::CPRS_SUCCESS;
}

CprsErr PPM::Decompress(Symb *dest, int dlen, char *src, int slen) {
  if (slen < 1) return CprsErr::CPRS_ERR_BUF;
  uchar method = (uchar)src[0];

  // are the data simply copied, without compression?
  if (method == 0) {
    if (dlen != slen - 1) return CprsErr::CPRS_ERR_PAR;
    std::memcpy(dest, src + 1, dlen);
    return CprsErr::CPRS_SUCCESS;
  } else if (method == 1)
    return DecompressArith(dest, dlen, src, slen);
  if (method != 2) return CprsErr::CPRS_ERR_VER;

  WordGraph *wg = NULL;
  try {
    wg = dynamic_cast<WordGraph *>(model.get());
  } catch (...) {
    wg = NULL;
  }

  if (wg) ASSERT(wg->insatend, "'wg->insatend' should be true");

  RangeCoder coder;
  coder.InitDecompress(src + 1, slen - 1);
  try {
    Range rng;
    Count c, total;
    int len;
    CprsErr err;

    model->InitPPM();

    for (int i = 0; i < dlen;) {
      // find the next edge to move
      total = model->GetTotal();
      c = coder.GetCount(total);

      len = dlen - i;
      err = model->Move(c, dest + i, len, rng);
      if (static_cast<int>(err)) return err;
      i += len;

      coder.Decode(rng.low, rng.high - rng.low, total);
    }
  } catch (CprsErr &e) {
    return e;
  }
  // catch(BufOverException) { return CprsErr::CPRS_ERR_BUF; }

  return CprsErr::CPRS_SUCCESS;
}

void PPM::PrintInfo(std::ostream &str) { str << "No. of all nodes in the model: " << model->GetNNodes() << std::endl; }

}  // namespace compress
}  // namespace stonedb
