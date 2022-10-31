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
#ifndef TIANMU_COMPRESS_INC_WGRAPH_H_
#define TIANMU_COMPRESS_INC_WGRAPH_H_
#pragma once

#include <bitset>
#include <iostream>

#include "common/assert.h"
#include "compress/inc_alloc.h"
#include "compress/ppm_defs.h"
#include "compress/range_code.h"

namespace Tianmu {
namespace compress {

class IncWGraph {
  struct Edge;
  class Mask;
  struct Node {
    static const uint MAX_TOTAL = RangeCoder::MAX_TOTAL_;
    static const Count init_count = 1;  // initial value of a count
    static const Count updt_count = 1;  // how much a count is updated in a single step
    // static const uchar max_count_last = 2;
    // static const uchar propag_count = 1;

    uchar *endpos;  // position of the first symbol _after_ the solid edge label
                    // (no. of symbols passed?)
    Node *suf;
    Edge *edge;   // array of outgoing edges; 0 for a leaf
    Count total;  // total count of outgoing edges, incl. ESC
    uchar nedge;  // size of 'edge'; 0 means 256

    bool IsNIL() { return suf == 0; }  // risky method for detecting NIL_ node
    ushort GetNEdge() { return edge ? (nedge ? nedge : 256) : 0; }
    static ushort RoundNEdge(ushort n);
    bool FindEdge(uchar s, Edge *&e, Count &low, Mask *mask);
    bool FindEdge(Count c, Edge *&e, Count &low, Mask *mask);
    Edge *FindEdge(uchar s);
    Edge *AddEdge(uchar s, ushort len, bool solid, Node *final, Count esc, ushort e = 1);
    Edge *AddEdge(uchar s, ushort len, bool solid, Node *final, IncAlloc &mem);
    ushort AllocEdge(IncAlloc &mem);  // extend array 'edge' so that it can hold one more
                                      // element; nedge MUST be incremented afterwards

    Node *Duplicate(Node *base, Edge *e,
                    IncAlloc &mem);  // duplicates itself; returns the duplicate
    static Node *Canonize(Node *n, Edge *&e, ushort &proj, uchar *s, bool canonlast);

    Count GetMaskTotal(Mask *mask);
    uchar EscCount() { return 3; }  // edge ? 2+nedge/4 : 1; }
    void EncodeEsc(RangeCoder *cod, Count low, Count tot) {
      DEBUG_ASSERT(low + EscCount() == tot);
      cod->Encode(low, EscCount(), tot);
    }
    void DecodeEsc(RangeCoder *cod, Count low, Count tot) {
      ASSERT(low + EscCount() == tot, "should be 'low+EscCount()==tot'");
      cod->Decode(low, EscCount(), tot);
    }
    void Rescale(uchar shift = 1);  // scales all counts shifting right by 'shift'
    void UpdateCount(Edge *&e, Count update = updt_count);
  };

  struct Edge {
   private:
    // uint len;		// edge length is used also to mark if the edge is
    // solid (set upper bit) or not static const uint top = (1u << 31); static
    // const uint bot = top - 1;
    ushort len;
    uchar solid;

   public:
    uchar fsym;
    Count count;   // scaled count
    Node *target;  // target node of the edge
    // uchar count_last;	// recently added count, waiting for propagation

    // Edge()						{ lastvisit =
    // (TimeStamp)-1;
    // }
    void Init(uchar fs, ushort l, bool s, Node *t, Count c);
    void SetLen(ushort a) { len = a; }
    void SetSolid(bool s) { solid = (s ? (uchar)1 : (uchar)0); }
    ushort GetLen() { return len; }
    bool IsSolid() { return solid != 0; }
    // void SetLen(ushort a)		{ (len &= top) |= (a & bot); }
    // void SetSolid(bool s)		{ if(s) len |= top; else len &= bot; }
    // ushort GetLen()				{ return (ushort)len; }
    // bool IsSolid()				{ return (len & top) != 0; }

    ushort NumMatch(uchar *s, ushort maxlen);
  };

  class Mask {
    std::bitset<255> map_;
    uint n_set_;

   public:
    bool Masked(uchar s) { return map_.test(s); }
    void Add(uchar s) {
      DEBUG_ASSERT(!Masked(s));
      map_.set(s);
      n_set_++;
    }
    void Reset() {
      map_.reset();
      n_set_ = 0;
    }  //{ std::memset(b_map,0,sizeof(map_)); }
    uint NumSet() { return n_set_; }
    // Mask()			{ Reset(); }
  };

  struct MatchLenCoder {
    static const uint c2[];  // = {120,128}     = {proj==edgelen [15/16], proj==1 [1/16]}
    static const uint c3[];  // = {102,119,128} = {proj==edgelen [102/128],
                             // proj==1 [17/128], other [9/128]}
    static const uint shift = 7;
    static void Encode(RangeCoder *coder, ushort proj, ushort edgelen, FILE *dump);
    static void Decode(RangeCoder *coder, ushort &proj, ushort edgelen);
  };

  IncAlloc memory_;  // all elements of the graph are allocated with IncAlloc
  Node *ROOT_, *NIL_, *START_;
  std::vector<Edge *> recent_;  // edges with 'fsym' fields waiting for
                                // initialization (during decoding)
  Mask _mask_, *p_mask_;

  RangeCoder *coder_;
  uchar **records_;
  const uint *reclen_;

  uint nfinals_;

  Node *InsertNode(Node *base, Edge *edge,
                   ushort proj);  // insert new node, in the middle of the 'edge'
  Node *NewFinal(uchar *endpos);
  void Traverse(Node *&base, Edge *&edge, ushort &proj, uchar *s, ushort restlen, Node *&final, bool encode);

  void LabelCopy(uchar *&dest, const uchar *src, ushort len);
  void EncodeRec(ushort rec, bool &repeated);
  void DecodeRec(ushort rec, uint dlen, bool &repeated);

  void Init();
  void Clear();  // clear graph structure withOUT memory_ deallocation

 public:
  IncWGraph();
  ~IncWGraph() = default;

  void Encode(RangeCoder *cod, char **index, const uint *lens, int nrec, uint &packlen);

  // Upon exit, 'index' will contain indices into 'dest'.
  // 'dlen' - size of 'dest'; must be >= 'packlen' returned from Encode during
  // compression (which is <= total length of records_).
  void Decode(RangeCoder *cod, char **index, const uint *lens, int nrec, char *dest, uint dlen);

  void Print(std::ostream &str = std::cout, uint flags = 1, Node *n = 0);
  void PrintLbl(std::ostream &str, Edge *e);

  // for gathering statistics
  FILE *dump_;
  uint matchlen_cost_, esc_cost_;  // length of code produced in MatchLen::Encode
                                   // and Node::EncodeEsc
  void PrintMemUsg(FILE *f) { memory_.PrintMemUsg(f); }
  void PrintMemUsg(std::ostream &str) { memory_.PrintMemUsg(str); }
};

}  // namespace compress
}  // namespace Tianmu

#endif  // TIANMU_COMPRESS_INC_WGRAPH_H_
