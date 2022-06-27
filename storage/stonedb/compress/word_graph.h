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
#ifndef STONEDB_COMPRESS_WORD_GRAPH_H_
#define STONEDB_COMPRESS_WORD_GRAPH_H_
#pragma once

#include <cstring>
#include <iostream>
#include <vector>

#include "common/assert.h"
#include "compress/ppm_defs.h"

namespace stonedb {
namespace compress {

class WordGraph : public PPMModel {
  using PNode = int;
  using PEdge = int;
  using PSEdge = int;

#pragma pack(push, 1)
  struct Node {
    int endpos;  // position of the first symbol _after_ the solid edge label
    int stop;    // no. of occurences of representative of this node as a suffix
                 // (so terminating in this node)
    PNode suf;
    PEdge edge;  // pointer to the first edge leaving this node (or ENIL if no
                 // edge leaves the node)

    int count;    // 'stop' is included in 'count'
    Count total;  // total count for distribution of outgoing edges + ESC

    // bool istab;		// "edge" points to an array of 256 edges

#ifdef SHADOW_EDGES
    Symb nshadow;  // no. of shadow edges
    PSEdge sedge;  // first shadow edge
    Count stotal;  // total count for distribution of shadow edges + ESC
#endif

    Node() { std::memset(this, 0, sizeof *this); }
  };
  struct Edge {
   private:
    int len;  // edge length is used also to mark if the edge is solid (len>0)
              // or not (len<0)
   public:
    Symb fsym;
    PNode n;     // target node of the edge
    PEdge next;  // next edge in the list

    Count sum;  // lower bound of the edge's range

    Edge() { std::memset(this, 0, sizeof *this); }
    void SetLen(int a) {
      if (len >= 0)
        len = abs(a);
      else
        len = -abs(a);
    }
    void SetSolid(bool s) {
      if (s)
        len = abs(len);
      else
        len = -abs(len);
    }
    uint GetLen() { return (len > 0) ? (uint)len : (uint)(-len); }
    bool IsSolid() { return len > 0; }
  };
  struct SEdge {  // shadow edge
    Symb fsym;
    Count sum;
  };
#pragma pack(pop)

  static const PNode ROOT1 = 1;
  static const PNode NIL = 0;
  static const PEdge ENIL = 0;
  static const PSEdge SENIL = 0;

  std::vector<Node> nodes;
  std::vector<Edge> edges;
  std::vector<PNode> finals;  // list of final nodes
  std::vector<SEdge> sedges;

  // original string - for reference; this is a pointer to _original_ data, not
  // a copy!!!
  const Symb *data;
  int dlen;  // length of 'data', INcluding the possible terminating symbol
             // ('\0')

  PPMParam param;

  //-------------------------------------------------------------------------

  Node &GN(PNode n) { return nodes[n]; }
  Edge &GE(PEdge e) { return edges[e]; }
  SEdge &GSE(PSEdge e) { return sedges[e]; }
  PEdge NxtEdge(PEdge e) { return GE(e).next; }
  PEdge FindEdge(PNode n, Symb s);
  PEdge AddEdge(PNode n, Symb s, int len, bool solid, PNode m);
  void CopyEdge(PNode e1, PNode e2) { std::memcpy(&GE(e2), &GE(e1), sizeof(Edge)); }  // copy from e1 to e2
  PSEdge FindSEdge(PNode n, Symb s);

  PNode NewNode() {
    int s = (int)nodes.size();
    nodes.resize(s + 1);
    return s;
  }
  PEdge NewEdge() {
    int s = (int)edges.size();
    edges.resize(s + 1);
    return s;
  }
  // PSEdge NewSEdge()		{ int s = (int)sedges.size();
  // sedges.resize(s+1); return s;
  // }
  PNode NewFinal(int endpos);

  // void SortEdges(Node& n);

  //-------------------------------------------------------------------------

  void Init();
  void Create();
  void Clear();

  // Point exactly at node 'x' may be represented in 2 ways:
  //  (1) { n = x, proj = 0, edge = undefined }
  //  (2) { n = parent_of_x, proj = len_of_edge, edge = edge_to_x }
  // The latter (not fully canonized) is required during duplication, to find
  // non-solid edges for redirection.
  struct Point {
    PNode n;
    uint proj;   // number of symbols passed from 'n' along 'edge'
    PEdge edge;  // current edge
  };

  void MoveDown(Point &p, const uchar *&s, const uchar *sstop);
  void Duplicate(Point &p);

  // if canonlast=true, 'p' upon exit is in form (1); otherwise it's in form (2)
  void MoveSuffix(Point &p, const uchar *s, bool canonlast);

  void Insert(Point p, int pos, int endpos, PNode &last, PNode &final);

  //-------------------------------------------------------------------------
  // transformation for PPM

  // needed for derecursivation
  struct StackElem {
    PNode n;
    bool proc;  // are children of 'n' already processed?
    StackElem(PNode n_, bool proc_) {
      n = n_;
      proc = proc_;
    }
  };

#ifdef SHADOW_EDGES
  int shlen[NSymb];  // used in SetShadow() to store lenghts of edges of the
                     // longer-suffix node
#endif

  void PropagateStop();
  void SetCounts();
  // void SortEdges();
  void SetSums();
  void SetShadow(PNode pn);
  int Shift(int c);  // c - cumulative count of outgoing edges (without ESC and
                     // 'stop' of the node)

  Count GetEscCount(PNode n, int c = 0);
  Count GetShEscCount(PNode n, int c = 0);
  // Count GetEscCount(PNode n)	{ DEBUG_ASSERT(n != NIL); return
  // param.esc_count; }
  // Count GetShEscCount(PNode n)	{ DEBUG_ASSERT(n != NIL); return
  // param.esc_count; }

  //-------------------------------------------------------------------------
  // PPM compression/decompression

#ifdef SHADOW_EDGES
  bool NoShadow(PNode n) { return GN(n).stotal == 0; }
#else
  bool NoShadow([[maybe_unused]] PNode n) { return true; }
#endif

  // if the last transition was forward or the last node doesn't have shadow
  // edges, prev == NIL; otherwise it's the previous node
  struct State {
    bool lastesc;  // the last transition was ESC
    PNode prev, n;
  };
  State state;
  // PNode state;

  // when "escape", e := ENIL
  void FindEdge(PEdge &e, PSEdge &se, Symb *str, int len);  // compression
  void FindEdge(PEdge &e, PSEdge &se, Count c);             // decompression

  CprsErr GetLabel(PEdge e, Symb *lbl,
                   int &len);  // 'len' - max size of lbl; upon exit: length of lbl
  void GetRange(PEdge e, PSEdge se,
                Range &r);  // 'e' must be an edge of the 'state'; 'se' is
                            // shadow of 'e' or SENIL

#ifdef SHADOW_EDGES
  Count GetTotal_() { return state.prev == NIL ? GN(state.n).total : GN(state.prev).stotal; }
#else
  Count GetTotal_() { return GN(state.n).total; }
#endif

  void Move(PEdge e);
  void MakeLog(PNode stt, PEdge e);  // print into 'logfile' information about
                                     // current state and transition

  //-----------------------------------------------------------------------------
 public:
  // CAUTION: the 'data_' array is NOT physically copied, only its pointer.
  // So the data must not change outside this class during lifetime of this
  // object.
  WordGraph(const Symb *data_, int dlen_ = -1, bool insatend_ = true);
  virtual ~WordGraph() { Clear(); }
  bool insatend;  // insert new child at the end of the children list?

  //-------------------------------------------------------------------------
  // information and statistics

  int GetNNodes() override { return (int)nodes.size(); }
  void PrintStat(FILE *f) override;
  int GetMemUsage() override;  // real number of bytes used, without wasted space in 'vector'
  int GetMemAlloc() override;  // total number of bytes used
  void Print(std::ostream &str = std::cout, uint flags = 1);
  void PrintLbl(std::ostream &str, Edge &e);

  //-------------------------------------------------------------------------
  // definitions for PPM

  void TransformForPPM(PPMParam param_ = PPMParam()) override;
  void InitPPM() override;

  // compression: [str,len_total] -> [len_of_edge,rng,total]
  void Move(Symb *str, int &len, Range &rng, Count &total) override;

  // decompression: [c,str,len_max] -> [str,len_of_edge,rng]+returned_error
  CprsErr Move(Count c, Symb *str, int &len, Range &rng) override;
  Count GetTotal() override { return GetTotal_(); }
};

}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_WORD_GRAPH_H_
