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

#include "suffix_tree.h"

namespace Tianmu {
namespace compress {

#define VECT_INIT 16384
#define STACK_INIT 16384

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::Init() {
  dlen_ = -1;
  NIL_ = 0;
  ROOT_ = 1;
  nleaves_ = 0;
  nodes_.reserve(VECT_INIT);
  nodes_.resize(1);
#ifdef SUFTREE_STAT
  nodes_[NIL_].dep = -1;
#endif
  iscreating_ = false;
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::Create() {
  iscreating_ = true;

  lens_.reset(new int[dlen_]);
  for (int z = 0, i = 0; z < dlen_; z++)
    if (data_[z] == '\0')
      for (; i <= z; i++) lens_[i] = z + 1 - i;

  // make root node
  nodes_.resize(2);
  Node &root = GetNode(ROOT_);
  root = Node();
  root.len = 1;
  root.prev = ROOT_;

  // active point
  Point active = {ROOT_, 0, 0};

  const uchar *s = data_.get();
  PNode last_int = NIL_,
        last_leaf = NIL_;  // index of the recently created internal and leaf node;
                           // stored for initialization of their 'suf' links

  // add leaf representing string "\0"
  AddLeaf(active, 0, last_int, last_leaf);
  GetNode(last_leaf).suf = ROOT_;
  GetNode(last_leaf).count = 0;
  last_leaf = NIL_;

  int nsuf = 1;
  while (nsuf < dlen_) {
    // only if active point is at node, it's worth invoking MoveDown()
    if (active.proj == 0) {
      // after following a suffix link, we reached a node, so we already know
      // how to initialize the suf link of the recently created node
      if (last_int != NIL_) {
        GetNode(last_int).suf = active.n;
        last_int = NIL_;
      }

      // maybe we can move further down the tree
      MoveDown(active, s);

      // Did we passed the end of the next string? (symbol '\0')
      // If so, we don't add new leaves, but walk along the border path,
      // to the root, and update counts of the passed leaves. Finally, we skip
      // to the next string.
      if (IsLeaf(active.n)) {
        if (last_leaf != NIL_) {
          GetNode(last_leaf).suf = active.n;
          last_leaf = NIL_;
        }
        do {
          DEBUG_ASSERT(*(s - 1) == '\0');
          DEBUG_ASSERT(IsLeaf(active.n));
          DEBUG_ASSERT(active.proj == 0);
          GetNode(active.n).count++;
          MoveSuffix(active, s);
        } while (active.n != ROOT_);  // TODO: time - optimize the leaf counting

        // GetNode(active.n).count++;
        nsuf = (int)(s - data_.get());
        continue;
      }
    }

    AddLeaf(active, (int)(s - data_.get()), last_int, last_leaf);
    MoveSuffix(active, s);

    nsuf++;
  }

  iscreating_ = false;
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::MoveDown(Point &p, const uchar *&s) {
  DEBUG_ASSERT(p.proj == 0);
  uchar *lbl;
  uint len;
  uint &proj = p.proj;

  for (;;) {
    // read the first symbol of the next edge
    p.next = GetChild(p.n, *s);
    if (p.next == NIL_)
      return;
    proj = 1;
    s++;

    // if((*s == '\0') && (p.n != NIL_)) {
    //	s++;
    //	p.n = p.next;
    //	p.proj = 0;
    //	DEBUG_ASSERT(GetNode(p.n).len == 1);
    //	return true;
    //}
    // s++;

    // read all next symbols of the edge
    lbl = data_.get() + GetNode(p.next).pos;  // text of the edge label
    len = GetNode(p.next).len;
    while ((proj < len) && (*s == lbl[proj])) {
      proj++;
      s++;
    }
    if (proj < len)
      return;

    p.n = p.next;
    proj = 0;
  }
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::AddLeaf(Point p, int pos, PNode &last_int, PNode &last_leaf) {
  // allocate a new leaf
  PNode nleaf = (int)nodes_.size();
  nodes_.resize(nleaf + 1);
  Node &leaf = GetNode(nleaf);
  leaf.child = NIL_;
  leaf.pos = pos;
  leaf.len = lens_[pos];
  leaf.count = 1;
  uchar s = data_[pos];

  // add leaf below an existing node...
  if (p.proj == 0) {
    AddChild(p.n, s, nleaf);

    if (last_int != NIL_)
      GetNode(last_int).suf = p.n;
    last_int = NIL_;

#ifdef SUFTREE_STAT
    GetNode(nleaf).dep = GetNode(p.n).dep + GetNode(nleaf).len;
#endif
  }

  // ...or split the edge, add new node and a leaf
  else {
    int nint = (int)nodes_.size();  // index of new node
    nodes_.resize(nint + 1);        // TODO: memory, time - allocate 'nodes_' on the basis of prediction

    Node &n = GetNode(nint);
    Node &prev = GetNode(p.n);
    Node &next = GetNode(p.next);

    n.pos = next.pos;
    n.len = p.proj;
    next.pos += n.len;
    next.len -= n.len;

    ChgChild(prev, data_[n.pos],
             nint);  // replace child 'next' of 'prev' with 'n'
    AddChild(n, data_[next.pos], p.next);
    AddChild(n, s, nleaf);

    // now we can initialize the 'suf' link of the previously created node
    if (last_int != NIL_)
      GetNode(last_int).suf = nint;
    last_int = nint;

#ifdef SUFTREE_STAT
    n.dep = prev.dep + n.len;
    GetNode(nleaf).dep = n.dep + GetNode(nleaf).len;
#endif
  }

  if (last_leaf != NIL_)
    GetNode(last_leaf).suf = nleaf;
  last_leaf = nleaf;

  // #	ifdef SUFTREE_STAT
  nleaves_++;
  // #	endif
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::MoveSuffix(Point &p, const uchar *s) {
  p.n = GetNode(p.n).suf;
  Canonize(p, s);
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::Canonize(Point &p, const uchar *s) {
  PNode ch;
  int len;
  while (p.proj > 0) {
    ch = GetChild(p.n, *(s - p.proj));
    DEBUG_ASSERT(ch != NIL_);
    len = GetNode(ch).len;
    if (p.proj < (uint)len) {
      p.next = ch;
      return;
    }
    p.n = ch;
    p.proj -= len;
  }
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::Clear() {
  nodes_.clear();
  lens_.reset();
  data_.reset();
}

template <class Symb, int NSymb>
int SuffixTree<Symb, NSymb>::GetSizeCDAWG(PNode n, bool cut) {
  int ret = 0;
  Node &nd = GetNode(n);
  if (cut) {
    ret = 1;
    ASSERT(nd.count == GetNode(nd.suf).count, "should be 'nd.count == GetNode(nd.suf).count'");
    // if(nd.count != GetNode(nd.suf).count) { cout << "ASSERT FAILED\n"; return
    // 0; }
  } else if ((n != ROOT_) && (nd.count == GetNode(nd.suf).count))
    cut = true;

  PNode ch = nd.child;
  while (ch != NIL_) {
    ret += GetSizeCDAWG(ch, cut);
    ch = GetNode(ch).next;
  }
  return ret;
}

//------------------------------------------------------------------------

template <class Symb, int NSymb>
int SuffixTree<Symb, NSymb>::GetMemUsage() {
  size_t x = nodes_.size() * sizeof(Node);
  if (data_)
    x += dlen_ * sizeof(data_[0]);
  return (int)x;
}

template <class Symb, int NSymb>
int SuffixTree<Symb, NSymb>::GetMemAlloc() {
  size_t x = nodes_.capacity() * sizeof(Node);
  if (data_)
    x += dlen_ * sizeof(data_[0]);
  return (int)x;
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::PrintInd(std::ostream &str, int ind) {
  for (int i = 0; i < ind; i++) str << ' ';
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::PrintSub(std::ostream &str, int start, int stop) {
  if (stop < 0)
    str << (char *)data_.get() + start << '#';
  else {
    char c;
    for (; start < stop; start++) str << ((c = (char)data_[start]) ? c : '#');
  }
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::Print(std::ostream &str, uint flags, PNode n, PNode prev, int ind) {
  if (n == NIL_) {
    n = ROOT_;
    prev = NIL_;
  }

  Node &node = GetNode(n);
  // if(IsInternal(n)) {
  int len = (n == ROOT_) ? 0 : node.len;

  PrintInd(str, ind);
  if (flags & 1) {
    PrintSub(str, node.pos, node.pos + len);
    str << ' ';
  }
  if (flags & 2)
    str << len << ' ';
  // if(flags & 4)
  //	str << node.dep << ' ';
  if (flags & 8)
    str << node.count << ' ';
  str << " [" << n << "]->[" << node.suf << "]" << std::endl;

  ind += len;
  for (int i = 1; i <= NSymb; i++) {
    PNode ch = GetChild(n, i % NSymb);
    if (ch != NIL_)
      Print(str, flags, ch, n, ind);
  }
  (void)prev;  // FIXME
               //}
               // else {
               //	int pos = GetLeafPos(prev, n);
               //
               //	PrintInd(str, ind);
               //	if(flags & 1) {
               //		PrintSub(str, pos);
               //		str << ' ';
               //	}
               //	if(flags & 2)
               //		str << (dlen_ - pos) << ' ';
               //	if(flags & 4)
               //		str << dlen_ - node.dep << ' ';
               //	if(flags & 8)
               //		str << node.count << ' ';
               //	str << " (" << node.dep << ")" << endl;
               //}
}

//------------------------------------------------------------------------

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::TransformForPPM(PPMParam param) {
  param_ = param;

  // compute counts of all nodes_ in the 'tree'
  SetCounts();

#ifdef SUFTREE_CDAWG_SIZE
  cout << "No. of nodes_: " << GetNNodes() << endl;
  cout << "No. of edges to remove if the tree were turned into CDAWG: " << GetSizeCDAWG() << endl;
#endif

  // cut off unnecessary branches
  // if(param_.valid_count > 1.0)
  //	Prune(ROOT_, false);

  // set cumulative counts of nodes_ - field 'sum'
  // TODO: time - merge SetSums() and SetSuffix()
  SetSums();

  // set 'suf' links
  SetSuffix();
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::SetCounts() {
  std::vector<StackElem> stack;
  stack.reserve(STACK_INIT);
  stack.push_back(StackElem(ROOT_, false));

  PNode n, ch;
  bool proc;
  while (!stack.empty()) {
    n = stack.back().n;
    proc = stack.back().proc;
    stack.pop_back();
    Node &node = GetNode(n);
    ch = node.child;
    if (ch == NIL_)
      continue;

    if (proc) {  // children are already processed (their counts are correct)
      int &count = node.count;
      while (ch != NIL_) {
        count += GetNode(ch).count;
        ch = NxtChild(ch);
      }
    } else {  // insert children onto the stack
      stack.push_back(StackElem(n, true));
      while (ch != NIL_) {
        stack.push_back(StackElem(ch, false));
        ch = NxtChild(ch);
      }
    }
  }
}
// template <class Symb, int NSymb>
// int SuffixTree<Symb,NSymb>::SetCounts(PNode n)
//{
//	// TODO: safety - remove recursion in this and other methods
//	Node& node = GetNode(n);
//	PNode ch = node.child;
//	if(ch == NIL) return node.count;
//	int c = 0;
//	do {
//		c += SetCounts(ch);
//		ch = NxtChild(ch);
//	} while(ch != NIL);
//	return (node.count = c);
//}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::Prune(PNode n, bool cut) {
  SetCut(n, cut);
  PNode ch = GetNode(n).child;
  if (ch == NIL_)
    return;

  bool invalid = false;
  if (!cut) {
    invalid = !IsValid(n);
    cut = invalid;
  }

  // TODO: safety - remove recursion in this and other methods
  // recursion
  do {
    Prune(ch, cut);
    ch = NxtChild(ch);
  } while (ch != NIL_);

  if (invalid)
    GetNode(n).child = NIL_;  // physically cut off children (but without destroying them)
}

template <class Symb, int NSymb>
bool SuffixTree<Symb, NSymb>::IsValid(PNode n) {
  // TODO: more sophisticated validity ckecking of nodes_
  // count children
  int num = 0;
  PNode ch = GetNode(n).child;
  DEBUG_ASSERT(ch != NIL_);
  do {
    num++;
    ch = NxtChild(ch);
  } while (ch != NIL_);

  return (GetNode(n).count >= param_.valid_count * num);
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::SetSums() {
  GetNode(ROOT_).sum = 0;  // root's count shouldn't be needed

  std::vector<PNode> stack;
  stack.reserve(STACK_INIT);
  stack.push_back(ROOT_);

  // int _xxx_ = 0;	// no. of suf links where both nodes_ have the same count
  PNode n, ch;
  int shift, cnt, scnt;
  while (!stack.empty()) {
    n = stack.back();
    stack.pop_back();
    Node &node = GetNode(n);
    // if(node.count == GetNode(node.suf).count) _xxx_++;
    ch = node.child;
    if (ch == NIL_)
      continue;

    shift = Shift(node.count);  // how to shift children's counts
    Count sum = GetEscCount(n);
    do {
      Node &child = GetNode(ch);
      cnt = child.count;       // real count, wide-represented
      scnt = cnt _SHR_ shift;  // short count, reduced
      DEBUG_ASSERT(cnt > 0);
      if (scnt == 0)
        scnt = 1;

      DEBUG_ASSERT(scnt <= COUNT_MAX);
      DEBUG_ASSERT(sum <= sum + (Count)scnt);
      sum += (Count)scnt;
      child.sum = sum;

      stack.push_back(ch);
      ch = NxtChild(ch);
    } while (ch != NIL_);
  }
  // cout << "No. of unnecessary sufixes: " << _xxx_ << endl;
}
// template <class Symb, int NSymb>
// Count SuffixTree<Symb,NSymb>::SetSums(PNode n, Count prev, int shift)
//{
//	DEBUG_ASSERT(n != NIL);
//
//	// set 'sum' of this node
//	Node& node = GetNode(n);
//	int cnt = node.count;			// real count, wide-represented
//	int scnt = cnt >> shift;		// short count, reduced
//	DEBUG_ASSERT(cnt > 0);
//	if(scnt == 0) scnt = 1;
//
//	DEBUG_ASSERT(scnt <= COUNT_MAX);
//	node.sum = prev + (Count)scnt;
//	DEBUG_ASSERT(prev <= node.sum);
//
//	// recursively set 'sum' of descendants
//	PNode ch = node.child;
//	if(ch != NIL) {
//		shift = Shift(cnt);			// how to shift
// children's
// counts 		Count sum = GetEscCount(n); 		do {
// sum = SetSums(ch, sum, shift); 			ch = NxtChild(ch);
// } while(ch != NIL);
//	}
//
//	return node.sum;
//}

template <class Symb, int NSymb>
int SuffixTree<Symb, NSymb>::Shift(int c) {
  int s = 0;
  int limit = COUNT_MAX / 4 - MAX_ESC_COUNT - NSymb;
  while ((c _SHR_ s) > limit) s++;
  return s;
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::SetSuffix() {
  // recursive procedure SetSuffix(n) processed nodes_ in post-order;
  // this one processes in pre-order (it is easier to implement iteratively)
  // TODO: ratio - more sophisticated choose of long suffix link

  std::vector<PNode> stack;
  stack.reserve(STACK_INIT);
  stack.push_back(ROOT_);

  PNode n, ch;
  while (!stack.empty()) {
    n = stack.back();
    stack.pop_back();
    Node &node = GetNode(n);
    DEBUG_ASSERT(n != NIL_);

    // put children on stack
    for (ch = node.child; ch != NIL_; ch = NxtChild(ch)) stack.push_back(ch);

    if (n == ROOT_)
      continue;
    PNode &suf = node.suf;
    if (data_[node.pos + node.len - 1] == '\0')
      // if the node's edge label ends with '\0', link the suffix directly to
      // the root
      suf = ROOT_;
    else
      // find suffix which is not cut off and has signifficantly different
      // distribution than 'n'
      // TODO: time - optimize searching for the best suffix link
      while ((suf != NIL_) && (GetCut(suf) || Similar(n, suf))) suf = GetNode(suf).suf;
    // if(suf == NIL_) suf = ROOT_;
  }
}
// template <class Symb, int NSymb>
// void SuffixTree<Symb,NSymb>::SetSuffix(PNode n)
//{
//	// TODO: more sophisticated choose of long suffix link
//	DEBUG_ASSERT(n != NIL);
//	Node& node = GetNode(n);
//
//	// recursion
//	PNode ch = node.child;
//	while(ch != NIL) {
//		SetSuffix(ch);
//		ch = NxtChild(ch);
//	}
//
//	// if the node's edge label ends with '\0', link the suffix directly to
// the root 	if(n == ROOT) return; 	PNode& suf = node.suf; if(data[node.pos
// + node.len - 1] == '\0') { 		suf = ROOT; 		return;
//	}
//
//	// find suffix which is not cut off and has signifficantly different
// distribution than 'n'
//	// TODO: time - optimize searching for the best suffix link
//	while((suf != NIL) && (GetCut(suf) || Similar(n, suf)))
//		suf = GetNode(suf).suf;
//	//if(suf == NIL) suf = ROOT;
//}

template <class Symb, int NSymb>
bool SuffixTree<Symb, NSymb>::Similar(PNode n, PNode suf) {
  int c1 = GetNode(n).count, c2 = GetNode(suf).count;
  return (c2 < param_.suf_ratio * c1);
}

//------------------------------------------------------------------------

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::FindEdge(Edge &e, Symb *str, int len) {
  if (state_ == NIL_) {
    e.n = ROOT_;
    e.s = str[0];
    return;
  }

  // find the edge beginning with symbol str[0]
  e.n = GetChild(state_, str[0]);
  if (e.n == NIL_)
    return;

  // check if the rest of the edge label is the same as 'str'
  Node &child = GetNode(e.n);
  if ((child.len > len) || (std::memcmp(str + 1, data_.get() + child.pos + 1, child.len - 1) != 0))
    e.n = NIL_;
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::FindEdge(Edge &e, Count c) {
  if (state_ == NIL_) {
    DEBUG_ASSERT(c < NSymb);
    e.n = ROOT_;
    e.s = (Symb)c;
    return;
  }
  if (c < GetEscCount(state_)) {
    e.n = NIL_;
    return;
  }

  // find the edge with count 'c'; linear search
  // TODO: time - binary search given count; or sort children by descending
  // count
  PNode ch = GetNode(state_).child;
  for (;;) {
    DEBUG_ASSERT(ch != NIL_);
    if (c < GetNode(ch).sum) {
      e.n = ch;
      return;
    }
    ch = NxtChild(ch);
  }
}

template <class Symb, int NSymb>
CprsErr SuffixTree<Symb, NSymb>::GetLabel(Edge e, Symb *lbl, int &len) {
  if (e.n == NIL_) {
    len = 0;
    return CprsErr::CPRS_SUCCESS;
  }
  if (e.n == ROOT_) {
    if (len < 1)
      return CprsErr::CPRS_ERR_BUF;
    *lbl = e.s;
    len = 1;
    return CprsErr::CPRS_SUCCESS;
  }

  Node &n = GetNode(e.n);
  if (len < n.len)
    return CprsErr::CPRS_ERR_BUF;
  len = n.len;
  std::memcpy(lbl, data_.get() + n.pos, len);
  return CprsErr::CPRS_SUCCESS;
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::GetRange(PNode stt, Edge e, Range &r) {
  if (e.n == ROOT_) {
    r.low = e.s;
    r.high = e.s + 1;
    return;
  }
  if (e.n == NIL_) {
    r.low = 0;
    r.high = GetEscCount(stt);
    return;
  }

  if (e.n == GetNode(stt).child)  // e.n is the first child?
    r.low = GetEscCount(stt);
  else
    r.low = GetNode(PrvChild(e.n)).sum;
  r.high = GetNode(e.n).sum;

  DEBUG_ASSERT(r.low < r.high);
}

template <class Symb, int NSymb>
void SuffixTree<Symb, NSymb>::Move(Symb *str, int &len, Range &rng, Count &total) {
  Edge e{NIL_, 0};
  FindEdge(e, str, len);
  len = GetLen(e);
  GetRange(state_, e, rng);
  total = GetTotal(state_);
  DEBUG_ASSERT(rng.high <= total);
  Move(e);
}

template <class Symb, int NSymb>
CprsErr SuffixTree<Symb, NSymb>::Move(Count c, Symb *str, int &len, Range &rng) {
  Edge e{NIL_, 0};
  FindEdge(e, c);
  CprsErr err = GetLabel(e, str, len);
  if (static_cast<int>(err))
    return err;
  GetRange(state_, e, rng);
  Move(e);
  return CprsErr::CPRS_SUCCESS;
}

template class SuffixTree<uchar, 256>;

}  // namespace compress
}  // namespace Tianmu
