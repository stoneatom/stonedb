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
#ifndef TIANMU_COMPRESS_SUFFIX_TREE_H_
#define TIANMU_COMPRESS_SUFFIX_TREE_H_
#pragma once

#include <cstring>
#include <iostream>
#include <vector>

#include "compress/ppm_defs.h"

namespace Tianmu {
namespace compress {

// this macro tells that some additional statistics should be computed and
// stored during suffix tree construction - which takes more time and memory, so
// finally should be switched off
#if defined(_DEBUG) || !defined(NDEBUG)
#define SUFTREE_STAT
#endif
// #define SUFTREE_CDAWG_SIZE

//------------------------------------------------------------------------

/*
        Ukkonen/McCreight's algorithm of suffix tree construction.
        The tree will need aprox. 20 times as much memory as the input data.
*/
template <class Symb = uchar, int NSymb = 256>
class SuffixTree : public PPMModel {
 public:
  // node identifier (pointer);
  // 0 - nil, 1 - root, positive - internal nodes_, negative - leafs (-1 means
  // the first leaf, i.e. number 0)
  using PNode = int;

 public:
  using Bool = uchar;

  // internal node or leaf
  struct Node {
    int pos;  // int-1, leaf-2	// start of the edge label
    int len;  // int-2, leaf-2

    PNode suf;  // int-1, leaf-2	// suffix link; for PPM it's changed
                // into _long_ suffix link

    int count;  // int-2, leaf-1
    Count sum;  // int-2, leaf-2	// upper bound of the node's range
    Bool cut;   // int-2, leaf-2	// true if the node was cut off during tree
                // transformation for PPM

    Symb fsym;    // int-2, leaf-2
    PNode child;  // int-1, leaf-NO	// first child (NIL_ only for a leaf)
    PNode next;   // int-1, leaf-1	// sibling of this node (NIL_ for the last child)
    PNode prev;   // int-2, leaf-2	// previous sibling; cyclic list (first
                  // child points to the last)

#ifdef SUFTREE_STAT
    int dep;
    int nvisit;  // how many times GetChild() was run for this node during tree
                 // construction
#endif

    Node() { std::memset(this, 0, sizeof *this); }
  };

  // all nodes_
  std::vector<Node> nodes_;
  PNode NIL_, ROOT_;

  int dlen_;                      // length of 'data_', INcluding the terminating symbol ('\0')
  std::unique_ptr<Symb[]> data_;  // original string - for reference
  std::unique_ptr<int[]> lens_;   // lens_[i] - length of substring of 'data_' from
                                  // 'i' to the next '\0' (incl. '\0')

  // global parameters and statistics
  int nleaves_;
  bool iscreating_;  // = true during tree construction, false otherwise
  PPMParam param_;

  //-------------------------------------------------------------------------

  Node &GetNode(PNode n) { return nodes_[n]; }
  PNode NxtChild(PNode ch) { return GetNode(ch).next; }
  PNode PrvChild(PNode ch) { return GetNode(ch).prev; }
  PNode GetChild(PNode n, Symb s) {
#ifdef SUFTREE_STAT
    if (iscreating_)
      GetNode(n).nvisit++;
#endif
    if (n == NIL_)
      return ROOT_;
    PNode ch = GetNode(n).child;
    while (ch != NIL_)
      if (GetNode(ch).fsym == s)
        return ch;
      else
        ch = GetNode(ch).next;
    return NIL_;
  }
  void AddChild(Node &n, Symb s, PNode m) {
    Node &mm = GetNode(m);
    mm.fsym = s;
    mm.next = n.child;
    if (n.child == NIL_)
      mm.prev = m;
    else {
      PNode &prev = GetNode(n.child).prev;
      mm.prev = prev;
      prev = m;
    }
    n.child = m;
  }
  void AddChild(PNode n, Symb s, PNode m) { AddChild(GetNode(n), s, m); }
  void ChgChild(Node &n, Symb s, PNode m) {
    // find a child with symbol 's'
    PNode *ch = &n.child;           // it's a pointer because we'll have to relink
    while (GetNode(*ch).fsym != s)  // the child must exist!!!
      ch = &GetNode(*ch).next;
    // replace the found child with node 'm'
    Node &mm = GetNode(m);
    mm.fsym = s;
    mm.next = GetNode(*ch).next;

    mm.prev = GetNode(*ch).prev;
    if (mm.prev == *ch)
      mm.prev = m;  // 'mm' is the only child
    else if (mm.next == NIL_)
      GetNode(n.child).prev = m;  // 'mm' is the last child out of 2 or more children
    else
      GetNode(mm.next).prev = m;  // 'mm' is not the last child

    *ch = m;
  }

  // int GetLeafPos(PNode par, PNode leaf)	{ return GetNode(leaf).dep +
  // GetNode(par).dep; }

  bool IsLeaf(PNode n) { return GetNode(n).child == NIL_; }
  bool IsInternal(PNode n) { return GetNode(n).child != NIL_; }
  struct Point {
    PNode n;
    uint proj;   // number of symbols passed from 'n' down to 'next' (if Point at
                 // node, proj=0)
    PNode next;  // child of 'n' representing the current edge (if Point is at
                 // node, 'next' is undefined)
  };

  void Init();
  void Create();
  void Clear();

  // Moves the point 'p' along the string 's', as far as *s is the same as
  // tree[p] (modifies 'p'). If the '\0' symbol was read, terminates and returns
  // true. Otherwise returns false. Assumption: at the beginning, 'p' is at a
  // node (p.proj = 0).
  void MoveDown(Point &p, const uchar *&s);

  // Add new leaf below point 'p'; doesn't change p (even if a new node is
  // created)
  void AddLeaf(Point p, int pos, PNode &last_int, PNode &last_leaf);

  // s - pointer to the first UNread character
  void MoveSuffix(Point &p, const uchar *s);
  void Canonize(Point &p, const uchar *s);

  //-------------------------------------------------------------------------
  // transformation of the tree for PPM

  // needed for derecursivation
  struct StackElem {
    PNode n;
    bool proc;  // are children of 'n' already processed?
    StackElem(PNode n_, bool proc_) {
      n = n_;
      proc = proc_;
    }
  };

  bool GetCut(PNode n) { return GetNode(n).cut == 1; }
  void SetCut(PNode n, bool v) { GetNode(n).cut = (v ? 1 : 0); }
  Count GetEscCount([[maybe_unused]] PNode n) { return param_.esc_count; }
  void SetCounts();

  // sets 'cut'=true for unnecessary nodes_; physically breaks edges between
  // pruned and non-pruned nodes_
  void Prune(PNode n, bool cut);
  bool IsValid(PNode n);  // is node 'n' useful for prediction? If not, its
                          // children will be cut off

  // 'sum' will be set only for not-cut-off nodes_
  void SetSums();
  int Shift(int c);

  // after pruning some 'suf' links may be invalid and should be advanced to
  // shorter suffixes; advancing suf-links may be good also for compression
  // performance
  void SetSuffix();
  bool Similar(PNode n, PNode suf);  // are distributions in 'n' and 'suf'
                                     // similar? (suf is some suffix of n)

  //-------------------------------------------------------------------------
  // PPM compression/decompression

  PNode state_;
  struct Edge {
    PNode n;  // e.n = NIL_ means "escape"
    Symb s;   // this is needed to identify exactly the edges from NIL_ to ROOT_
              // (otherwise is undefined)
  };

  void FindEdge(Edge &e, Symb *str, int len);  // compression
  void FindEdge(Edge &e, Count c);             // decompression

  CprsErr GetLabel(Edge e, Symb *lbl,
                   int &len);  // 'len' - max size of lbl; upon exit: length of lbl
  int GetLen(Edge e) {
    if (e.n == NIL_)
      return 0;
    return GetNode(e.n).len;
  }
  void GetRange(PNode stt, Edge e,
                Range &r);  // 'stt' must be the parent of 'e'
  Count GetTotal(PNode stt) {
    if (stt == NIL_)
      return N_Symb_;
    Node &node = GetNode(stt);
    if (node.child == NIL_)
      return param_.esc_count;
    return GetNode(GetNode(node.child).prev).sum;
  }

  void Move(Edge e) {
    if (e.n == NIL_)
      state_ = GetNode(state_).suf;
    else
      state_ = e.n;
  }

  //-----------------------------------------------------------------------------
 public:
  // data_ - a sequence of concatenated null-terminated strings,
  // containing 'dlen_' symbols in total (INcluding the '\0' symbols!)
  // The resultant tree is a sum of trees constructed for each of the strings,
  // with every leaf storing the number of its repetitions.
  // If dlen_=-1, it's assumed that data_ contains only one string.
  SuffixTree(const Symb *data, int dlen = -1) {
    Init();
    if (dlen < 0)
      dlen_ = (int)std::strlen((const char *)data) + 1;
    else
      dlen_ = dlen;
    dlen_ += 2;  // add '\0' at the beginning and at the end
    data_.reset(new uchar[dlen_]);
    std::memcpy(data_.get() + 1, data, dlen_ - 2);
    data_[0] = data_[dlen_ - 1] = '\0';
    Create();
  }
  virtual ~SuffixTree() { Clear(); }
  //-------------------------------------------------------------------------
  // information and statistics

  int GetNNodes() override { return (int)nodes_.size(); }
  int GetNLeaves() { return nleaves_; }
  int GetMemUsage() override;  // real number of bytes used, without wasted space in 'vector'
  int GetMemAlloc() override;  // total number of bytes used

  // branch[256] - array of counts of branching factors
  // sumcnt[256] - array of summary subtree sizes (counts) for nodes_ of a given
  // branching factor sumvis[256] - array of summary number of visits (invoking
  // GetChild) for a branching factor vld - number of valid internal nodes_, i.e.
  // with large count and small branching - good for prediction void
  // GetStat(int* branch, int* sumcnt, int* sumvis, int& vld, int* cnt = 0,
  // PNode n = 0, PNode prev = 0);

  // returns number of edges that would be removed if the tree were turned into
  // Compact Directed Acyclic Word Graph
  int GetSizeCDAWG(PNode n = 1, bool cut = false);

  // flags (OR):
  //  1 - print label text
  //  2 - print lengths of labels
  //  4 - print depths of nodes_
  //  8 - print counts
  void Print(std::ostream &str = std::cout, uint flags = 1 + 8, PNode n = 0, PNode prev = 0, int ind = 0);
  void PrintSub(std::ostream &str, int start,
                int stop = -1);  // stop=-1: print to '\0' symbol
  void PrintInd(std::ostream &str, int ind);

  //-------------------------------------------------------------------------
  // definitions for PPM

  void TransformForPPM(PPMParam param = PPMParam()) override;
  void InitPPM() override { state_ = ROOT_; }
  // compression: [str,len_total] -> [len_of_edge,rng,total]
  void Move(Symb *str, int &len, Range &rng, Count &total) override;

  // decompression: [c,str,len_max] -> [str,len_of_edge,rng]+returned_error
  CprsErr Move(Count c, Symb *str, int &len, Range &rng) override;
  Count GetTotal() override { return GetTotal(state_); }
#ifdef SUFTREE_STAT
  int GetDep(PNode s) { return GetNode(s).dep; }
#endif
};

}  // namespace compress
}  // namespace Tianmu

#endif  // TIANMU_COMPRESS_SUFFIX_TREE_H_
