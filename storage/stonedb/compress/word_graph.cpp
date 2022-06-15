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

#include "word_graph.h"

#include <cstring>

namespace stonedb {
namespace compress {

constexpr size_t VECT_INIT = 128_KB;
constexpr size_t STACK_INIT = VECT_INIT / 2;

WordGraph::WordGraph(const Symb *data_, int dlen_, bool insatend_) {
  Init();

  if (dlen_ < 0)
    dlen = (int)std::strlen((const char *)data_) + 1;
  else
    dlen = dlen_;
  DEBUG_ASSERT(dlen > 0);

  data = data_;  // the data are NOT copied!
  insatend = insatend_;
  Create();
}

void WordGraph::Init() {
  data = NULL;
  dlen = -1;
  // NIL = 0; ROOT = 1;
  // nleaves = 0;

  nodes.reserve(VECT_INIT);
  nodes.resize(1);  // nodes[0] - NIL

  edges.reserve(VECT_INIT);
  edges.resize(1);  // edges[0] - ENIL (it is not used, by the space is left for safety)

  finals.reserve(VECT_INIT);
#ifdef SHADOW_EDGES
  sedges.reserve(VECT_INIT);
  sedges.resize(1);
// std::memset(shlen, 0, NSymb*sizeof(*shlen));
#endif

  // #	ifdef SUFTREE_STAT
  //	nodes[NIL].dep = -1;
  // #	endif
  //  iscreating = false;
  //  for(int i = 0; i < 256; i++)
  //	nodes[NIL].child[i] = ROOT;
  //  lens = NULL;
}

void WordGraph::Create() {
  // make root node
  nodes.resize(2);
  Node &root = GN(ROOT1);
  root = Node();
  root.suf = NIL;
  root.edge = ENIL;

  // create edges from NIL to ROOT, for every possible symbol as a label
  for (int fsym = 0; fsym < NSymb; fsym++) AddEdge(NIL, (Symb)fsym, 1, true, ROOT1);

  const uchar *s = data, *dstop = data + dlen, *sstop;
  while (s != dstop) {
    // set 'sstop' = beginning of the next string; note that the last string
    // doesn't need to be null-terminated!
    sstop = s;
    while ((sstop != dstop) && *sstop) sstop++;
    if (sstop != dstop) sstop++;  // strings inserted into the graph will include '\0' at the end
    // sstop = s + std::strlen((char*)s) + 1;

    Point active = {ROOT1, 0, ENIL};  // active point
    PNode last = NIL;                 // recently created node; stored for initialization of
                                      // its 'suf' link
    PNode final = NIL;                // final node for the current string

    while ((s != sstop) || (active.proj > 0)) {
      // only if active point is at node, it's worth invoking MoveDown()
      if (active.proj == 0) MoveDown(active, s, sstop);

      Insert(active, (int)(s - data), (int)(sstop - data), last, final);
      MoveSuffix(active, s, true);

      // after following a suffix link, we reached a node, so we already know
      // how to initialize the suf link of the recently created node
      if ((last != NIL) && (active.proj == 0)) {
        GN(last).suf = active.n;
        last = NIL;
      }
    }
    DEBUG_ASSERT(last == NIL);
  }
}

WordGraph::PNode WordGraph::NewFinal(int endpos) {
  PNode pfinal = NewNode();
  Node &final = GN(pfinal);
  final.endpos = endpos;
  final.stop = 1;
  final.suf = NIL;
  final.edge = ENIL;
  finals.push_back(pfinal);
  return pfinal;
}

void WordGraph::MoveDown(Point &p, const uchar *&s, const uchar *sstop) {
  DEBUG_ASSERT(p.proj == 0);
  DEBUG_ASSERT(s != sstop);
  const uchar *lbl;
  uint len;
  uint &proj = p.proj;

  for (;;) {
    // read the first symbol of the next edge
    if (s == sstop) return;
    p.edge = FindEdge(p.n, *s);
    if (p.edge == ENIL) return;
    proj = 1;
    s++;

    // read all next symbols of the edge
    Edge &e = GE(p.edge);
    len = e.GetLen();
    lbl = data + GN(e.n).endpos - len;  // text of the edge label
    while ((proj < len) && (s != sstop) && (*s == lbl[proj])) {
      proj++;
      s++;
    }
    if (proj < len) return;

    // check whether the passed edge was non-solid - in that case the target
    // node must be duplicated, part of incoming edges redirected and active
    // point 'p' accordingly updated
    if (!e.IsSolid()) Duplicate(p);

    p.n = GE(p.edge).n;
    proj = 0;
  }
}

void WordGraph::Duplicate(Point &p) {
  // allocate a new node
  PNode pn1 = GE(p.edge).n;
  PNode pn2 = NewNode();
  Node &n1 = GN(pn1);  // caution: keeping references may be unsafe
  Node &n2 = GN(pn2);

  // set fields
  n2.endpos = n1.endpos;
  // n2.stop = 0;
  // n2.edge = ENIL;		// should be 0 after construction
  n2.suf = n1.suf;
  n1.suf = pn2;

  // copy outgoing edges
  PEdge pe2, prev = ENIL;
  for (PEdge pe1 = n1.edge; pe1 != ENIL; pe1 = GE(pe1).next) {
    pe2 = NewEdge();
    CopyEdge(pe1, pe2);
    GE(pe2).SetSolid(false);
    if (prev == ENIL)
      n2.edge = pe2;
    else
      GE(prev).next = pe2;
    prev = pe2;
  }

  // redirect incoming edges
  GE(p.edge).SetSolid(true);
  const Symb *s = data + n1.endpos;
  Point q = p;
  do {
    DEBUG_ASSERT(q.proj == GE(q.edge).GetLen());
    DEBUG_ASSERT(q.proj > 0);
    GE(q.edge).n = pn2;
    MoveSuffix(q, s,
               false);  // move 'q' along suffix link, don't canonize the last edge
  } while (GE(q.edge).n == pn1);
}

void WordGraph::MoveSuffix(Point &p, const uchar *s, bool canonlast) {
  p.n = GN(p.n).suf;

  PEdge e;
  uint len;
  while (p.proj > 0) {
    e = FindEdge(p.n, *(s - p.proj));
    DEBUG_ASSERT(e != ENIL);

    len = GE(e).GetLen();
    if ((p.proj < len) || (!canonlast && (p.proj == len))) {
      p.edge = e;
      return;
    }
    p.n = GE(e).n;
    p.proj -= len;
  }
}

void WordGraph::Insert(Point p, int pos, int endpos, PNode &last, PNode &final) {
  uchar s = data[pos];
  bool isfirst = (final == NIL);

  // 'p' is at a node, fully canonized -> add leaf below the node
  if (p.proj == 0) {
    if (last != NIL) GN(last).suf = p.n;
    last = NIL;

    if (pos < endpos) {
      if (final == NIL) final = NewFinal(endpos);
      AddEdge(p.n, s, endpos - pos, isfirst, final);
    } else {
      // GN(p.n).stop ++;
      if (final == NIL) {
        final = p.n;
        if (GN(final).stop++ == 0) finals.push_back(final);
      } else if (GN(final).suf == NIL)
        GN(final).suf = p.n;
    }
  }

  // 'p' is on a solid edge or there was no node recently created -> split the
  // edge, add new node
  else if (GE(p.edge).IsSolid() || (last == NIL)) {
    PNode pn = NewNode();

    // edge from 'n' to 'final'
    if (pos < endpos) {
      if (final == NIL) final = NewFinal(endpos);  // caution: new node is added
      AddEdge(pn, s, endpos - pos, isfirst, final);
    } else if (final == NIL) {
      final = pn;
      if (GN(final).stop++ == 0) finals.push_back(final);
    } else if (GN(final).suf == NIL)
      GN(final).suf = pn;

    // edge from 'n' to 'next'
    PEdge pe = p.edge;
    Node &n = GN(pn);  // Caution: keeping references is risky if a new node is
                       // added! (array can be reallocated)
    Node &next = GN(GE(pe).n);
    n.endpos = next.endpos - GE(pe).GetLen() + p.proj;
    AddEdge(pn, data[n.endpos], next.endpos - n.endpos, GE(pe).IsSolid(), GE(pe).n);

    // edge from 'prev' to 'n'
    Edge &e = GE(pe);
    e.n = pn;
    e.SetLen(p.proj);
    e.SetSolid(true);

    // now we can initialize the 'suf' link of the previously created node
    if (last != NIL) GN(last).suf = pn;
    last = pn;
  }

  // 'p' is on a non-solid edge and there was a node recently created ->
  // redirect the edge
  else {
    Edge &e = GE(p.edge);
    e.n = last;
    e.SetLen(p.proj);
  }
}

void WordGraph::Clear() {
  finals.clear();
  edges.clear();
  nodes.clear();
  data = NULL;
}

//------------------------------------------------------------------------

int WordGraph::GetMemUsage() {
  size_t x = nodes.size() * sizeof(Node);
  x += edges.size() * sizeof(Edge);
  x += finals.size() * sizeof(PNode);
  x += sedges.size() * sizeof(SEdge);
  // if(data) x += dlen * sizeof(*data);
  return (int)x;
}

int WordGraph::GetMemAlloc() {
  size_t x = nodes.capacity() * sizeof(Node);
  x += edges.capacity() * sizeof(Edge);
  x += finals.capacity() * sizeof(PNode);
  x += sedges.capacity() * sizeof(SEdge);
  // if(data) x += dlen * sizeof(*data);
  return (int)x;
}

// TODO commented out
void WordGraph::PrintStat([[maybe_unused]] FILE *f) {
  /*	// walk through the tree and print no. of children
      std::vector<PNode> stack;
      stack.reserve(STACK_INIT);

      stack.push_back(ROOT1);

      PNode pn; PEdge pe;
      while(!stack.empty()) {
              pn = stack.back();
              stack.pop_back();
              Node& n = GN(pn);
              int nchild = 0;
              pe = n.edge;
              while(pe != ENIL) {
                      nchild++;
                      stack.push_back(GE(pe).n);
                      pe = GE(pe).next;
              }
              std::fprintf(f, "%d\n", nchild);
      }
  */
}

void WordGraph::PrintLbl(std::ostream &str, Edge &e) {
  int stop = GN(e.n).endpos;
  char c;
  for (int i = stop - e.GetLen(); i < stop; i++) str << (((c = (char)data[i]) > 13) ? c : '#');
}

void WordGraph::Print(std::ostream &str, [[maybe_unused]] uint flags) {
  for (int n = ROOT1; n < (int)nodes.size(); n++) {
    Node &nd = GN(n);
    str << n << ": ";
    str << "  endpos = " << nd.endpos;
    str << "  suf = " << nd.suf;
    str << "  stop = " << nd.stop;
    str << "  count = " << nd.count;
    str << "  total = " << nd.total;

    for (PEdge e = nd.edge; e != NIL; e = GE(e).next) {
      Edge &ed = GE(e);
      str << "\n   " << n << " -> " << ed.n << ":  ";
      str << "\"";
      PrintLbl(str, ed);
      str << "\"  ";
      str << (ed.IsSolid() ? "solid" : "non-solid");
      str << "  sum = " << ed.sum;
    }
    str << std::endl;
  }
}

//------------------------------------------------------------------------
// transformation for PPM
//------------------------------------------------------------------------

void WordGraph::TransformForPPM(PPMParam param_) {
  param = param_;

  // propagate 'stop' numbers from final nodes to all border-path nodes
  PropagateStop();

  // compute counts of all nodes (not only on the border path)
  SetCounts();
  // SortEdges(GN(ROOT));

  // set cumulative counts of nodes - field 'sum';
  // create shadow edges, which are used after "escape" transition during
  // compress
  SetSums();
}

void WordGraph::PropagateStop() {
  // propagate 'stop' through the suffix path, temporarily accumulating 'stop'
  // values in 'count' fields (this is necessary because some final node may be
  // a suffix of another final node)
  for (uint i = 0; i < finals.size(); i++) {
    int s = GN(finals[i]).stop;
    PNode n = finals[i];

    while (n != ROOT1) {
      GN(n).count += s;
      n = GN(n).suf;
    }
  }

  // move values from 'count' to 'stop'
  for (uint i = 0; i < finals.size(); i++) {
    PNode n = finals[i];
    while (n != ROOT1) {
      Node &nd = GN(n);
      if (nd.count == 0) break;
      nd.stop = nd.count;
      nd.count = 0;
      n = nd.suf;
    }
  }
}

void WordGraph::SetCounts() {
  std::vector<StackElem> stack;
  stack.reserve(STACK_INIT);
  // TODO:uncomment ROOT
  // stack.push_back(StackElem(ROOT,false));

  PNode n;
  PEdge e;
  bool proc;
  while (!stack.empty()) {
    n = stack.back().n;
    proc = stack.back().proc;
    stack.pop_back();

    Node &node = GN(n);
    int &count = node.count;
    e = node.edge;

    if (proc) {  // children are already processed (their counts are correct)
      count = node.stop;
      while (e != NIL) {
        count += GN(GE(e).n).count;
        e = NxtEdge(e);
      }
      // SortEdges(node);
    } else if (count == 0) {  // insert children onto the stack
      stack.push_back(StackElem(n, true));
      int nedges = 0;
      while (e != NIL) {
        stack.push_back(StackElem(GE(e).n, false));
        e = NxtEdge(e);
        nedges++;
      }
    }
  }
}

// void WordGraph::SortEdges()
//{
//	std::vector<PNode> stack;
//	stack.reserve(1000); //STACK_INIT);
//	stack.push_back(ROOT);
//
//	PNode pn; PEdge pe;
//	while(!stack.empty()) {
//		Node& node = GN(pn = stack.back());
//		stack.pop_back();
//
//		if(SortEdges(node)) {		// insert children onto the
// stack 			pe = node.edge; 			while(pe != ENIL)
// { stack.push_back(GE(pe).n); 				pe =
// GE(pe).next;
//			}
//		}
//	}
//}

void WordGraph::SetSums() {
  // set sums of edges from NIL to ROOT
  PNode pn;
  PEdge pe;
  int s = 0;
  pe = GN(NIL).edge;
  while (pe != ENIL) {
    GE(pe).sum = s++;
    pe = GE(pe).next;
  }
  GN(NIL).total = s;

  std::vector<PNode> stack;  // holds nodes which outgoing edges' sums must be set
  stack.reserve(STACK_INIT);
  // TODO:uncomment ROOT
  // stack.push_back(ROOT);

  int shift, cnt, scnt;
  while (!stack.empty()) {
    pn = stack.back();
    stack.pop_back();
    Node &node = GN(pn);
    if (node.total > 0) continue;  // we already visited this node

#ifdef SHIFT_ESC
    int c = node.count - node.stop;
    int esc = GetEscCount(pn, c);
    shift = Shift(c + esc);
    Count sum = esc _SHR_ shift;
    if (sum == 0) sum = 1;
#else
    int c = node.count - node.stop;
    shift = Shift(c);  // how to shift children's counts
    Count sum = GetEscCount(pn, c);
#endif

    pe = node.edge;
    while (pe != ENIL) {
      Edge &e = GE(pe);
      e.sum = sum;  // 'e.sum' is the lower bound

      cnt = GN(e.n).count;     // real count, wide-represented
      scnt = cnt _SHR_ shift;  // short count, reduced
      DEBUG_ASSERT(cnt > 0);
      if (scnt == 0) scnt = 1;

      DEBUG_ASSERT(scnt <= COUNT_MAX / 4);
      DEBUG_ASSERT(sum <= sum + (Count)scnt);
      sum += (Count)scnt;

      stack.push_back(e.n);
      pe = e.next;
    }
    node.total = sum;

#ifdef SHADOW_EDGES
    SetShadow(pn);
#endif
  }
}

void WordGraph::SetShadow([[maybe_unused]] PNode pn) {
#ifdef SHADOW_EDGES
  Node &n1 = GN(pn);
  PNode pe = n1.edge;
  if (pe == ENIL) return;  // don't make shadow edges for leaves

  // fill in the 'shlen' array of edge lengths
  DEBUG_ASSERT(sizeof(shlen) == NSymb * sizeof(*shlen));
  std::memset(shlen, 0, sizeof(shlen));  // isn't it faster to clear only the
                                         // used cells of 'shlen' at the end?
  do {
    Edge &e = GE(pe);
    shlen[e.fsym] = e.GetLen();
    pe = e.next;
  } while (pe != ENIL);

  // compute total count of shadow edges (not including ESC)
  // TODO: try to do it approximately but faster
  Node &n2 = GN(n1.suf);
  pe = n2.edge;
  DEBUG_ASSERT(pe != ENIL);
  int totcnt = 0;
  do {
    Edge &e = GE(pe);
    if (shlen[e.fsym] != e.GetLen()) totcnt += GN(e.n).count;
    pe = e.next;
  } while (pe != ENIL);

  n1.sedge = sedges.size();
  // int shift = Shift(n2.count - n1.count);		// this is an upper
  // approximation of the shift; it could be lower
  int shift = Shift(totcnt);
  int cnt, scnt;
  Count &stotal = n1.stotal = GetShEscCount(pn);

  // loop through edges of suffix-link node and create shadow edges when
  // necessary; set shadow sums and total count
  pe = n2.edge;
  do {
    Edge &e = GE(pe);
    if (shlen[e.fsym] != e.GetLen()) {
      DEBUG_ASSERT((shlen[e.fsym] == 0) || (shlen[e.fsym] > e.GetLen()));
      sedges.resize(n1.sedge + ++n1.nshadow);
      SEdge &se = sedges.back();
      se.fsym = e.fsym;
      se.sum = stotal;

      cnt = GN(e.n).count;     // real count, wide-represented
      scnt = cnt _SHR_ shift;  // short count, reduced
      DEBUG_ASSERT(cnt > 0);
      if (scnt == 0) scnt = 1;

      DEBUG_ASSERT(scnt <= COUNT_MAX / 4);
      DEBUG_ASSERT(stotal <= stotal + (Count)scnt);
      stotal += (Count)scnt;
    }
    pe = e.next;
  } while (pe != ENIL);

#endif
}

int WordGraph::Shift(int c) {
  int s = 0;
#ifdef SHIFT_ESC
  int limit = COUNT_MAX / 4 - NSymb - 1;
#else
  int limit = COUNT_MAX / 4 - NSymb - MAX_ESC_COUNT;
#endif
  while ((c _SHR_ s) > limit) s++;
  return s;
}

//------------------------------------------------------------------------
// PPM compression/decompression
//------------------------------------------------------------------------

void WordGraph::FindEdge(PEdge &e, PSEdge &se, Symb *str, int len) {
  // find the edge beginning with symbol str[0]
  DEBUG_ASSERT(len > 0);
  if (state.prev == NIL) {  // the last transition was forward
    se = SENIL;
    e = FindEdge(state.n, str[0]);
    if (e == ENIL) return;
  } else {  // the last transition was ESC
    se = FindSEdge(state.prev, str[0]);
    if (se == SENIL) {
      e = ENIL;
      return;
    }
    e = FindEdge(state.n, str[0]);
  }
  DEBUG_ASSERT(e != ENIL);

  // check if the rest of the edge label is the same as 'str'
  PNode n = GE(e).n;
  int elen = GE(e).GetLen();
  if ((elen > len) || (std::memcmp(str + 1, data + GN(n).endpos - elen + 1, elen - 1) != 0)) {
    e = ENIL;
    se = SENIL;
  }
}

void WordGraph::FindEdge(PEdge &e, PSEdge &se, Count c) {
  // find the edge with count 'c'; linear search
  if (state.prev == NIL) {  // the last transition was forward
    e = ENIL;
    se = SENIL;
    PEdge next = GN(state.n).edge;
    while ((next != ENIL) && (c >= GE(next).sum)) {
      e = next;
      next = GE(e).next;
    }
  } else {  // the last transition was ESC
#ifdef SHADOW_EDGES

    // find a shadow edge 'se'
    Node &n = GN(state.prev);
    se = n.sedge;
    PSEdge stop = se + n.nshadow;
    while ((se != stop) && (c >= GSE(se).sum)) se++;
    if (--se < n.sedge) {
      se = SENIL;
      e = ENIL;
    } else {
      e = FindEdge(state.n, GSE(se).fsym);
      DEBUG_ASSERT(e != ENIL);
    }

#else
    STONEDB_ERROR("not implemented");
#endif
  }
}

CprsErr WordGraph::GetLabel(PEdge e, [[maybe_unused]] Symb *lbl, int &len) {
  if (e == ENIL) {
    len = 0;
    return CprsErr::CPRS_SUCCESS;
  }  // "escape"

  Edge &edge = GE(e);
  if (len < (int)edge.GetLen()) return CprsErr::CPRS_ERR_BUF;
  len = (int)edge.GetLen();

  // TODO:uncomment ROOT
  // if(edge.n == ROOT) lbl[0] = edge.fsym;			// edges from
  // NIL consider separately
  //	else std::memcpy(lbl, data + GN(edge.n).endpos - len, len);

  return CprsErr::CPRS_SUCCESS;
}

void WordGraph::GetRange(PEdge e, [[maybe_unused]] PSEdge se, Range &r) {
  r.low = 0;
  if (state.prev == NIL) {  // the last transition was forward
    PEdge next;
    Node &n = GN(state.n);
    if (e == ENIL)
      next = n.edge;
    else {
      r.low = GE(e).sum;
      next = GE(e).next;
    }
    r.high = (next == ENIL ? n.total : GE(next).sum);
  } else {  // the last transition was ESC
#ifdef SHADOW_EDGES
    PSEdge next;
    Node &n = GN(state.prev);
    if (se == SENIL)
      next = n.sedge;
    else {
      r.low = GSE(se).sum;
      next = se + 1;
    }
    r.high = (next >= n.sedge + n.nshadow ? n.stotal : GSE(next).sum);
#else
    STONEDB_ERROR("not implemented");
#endif
  }

  DEBUG_ASSERT(r.low < r.high);
}

void WordGraph::Move(PEdge e) {
  if (e == ENIL) {
    state.lastesc = true;
    if (NoShadow(state.n))
      state.prev = NIL;
    else
      state.prev = state.n;
    state.n = GN(state.n).suf;
  } else {
    state.lastesc = false;
    state.prev = NIL;
    state.n = GE(e).n;
  }
  // #	ifndef SHADOW_EDGES
  //	state.prev = NIL;
  // #	endif
}

void WordGraph::MakeLog(PNode stt, PEdge e) {
  if (logfile == NULL) return;
  // if(e != ENIL) {
  //	std::fprintf(logfile, "F\n");	// forward transition
  //	return;
  //}

  // compute no. of transitions from n1 and n2
  Node &n1 = GN(stt);
  int t1 = 0;
  for (PEdge e1 = n1.edge; e1 != ENIL; e1 = GE(e1).next) t1++;
  // for(e = n2.edge; e != ENIL; e = GE(e).next) t2++;

  std::fprintf(logfile, "%d %d %d %d\n", (int)state.lastesc, (int)(e == ENIL), t1, n1.count - n1.stop);
  // std::fprintf(logfile, "E %d:%d %d:%d\n", t1, t2, n1.count, n2.count);
}

void WordGraph::InitPPM() {
  state.lastesc = false;
  state.prev = NIL;
  // TODO:uncomment ROOT
  // state.n = ROOT;
}

void WordGraph::Move(Symb *str, int &len, Range &rng, Count &total) {
  PEdge e;
  PSEdge se;
  FindEdge(e, se, str, len);
  len = (e == ENIL ? 0 : GE(e).GetLen());
  GetRange(e, se, rng);
  total = GetTotal_();
  DEBUG_ASSERT(rng.high <= total);

#ifdef MAKELOG
  MakeLog(state.n, e);
#endif

  Move(e);
}

CprsErr WordGraph::Move(Count c, Symb *str, int &len, Range &rng) {
  PEdge e;
  PSEdge se;
  FindEdge(e, se, c);
  CprsErr err = GetLabel(e, str, len);
  if (static_cast<int>(err)) return err;
  GetRange(e, se, rng);
  Move(e);
  return CprsErr::CPRS_SUCCESS;
}

// TODO: time - use incremental allocator IncAlloc
// TODO: time - sort edges by descending count
// TODO: ratio - shouldn't we shift ESC count like other counts?
// TODO: time - don't rebuild models, but build it incrementally; preferably
// during compress/decompress
// TODO: memory - don't create shadow edges if they have similar entropy as the
// suffix node
// TODO: ratio - remember the last path and assign to it higher probability
// TODO: ratio - use in parallel a model which predicts on the basis of position
// from the beginning
// TODO: ratio - when sending count to a coder, increase it by a constant - to
// make probablts more equal

// DONE: memory - don't copy referential data

//-------------------------------------------------------------------------

WordGraph::PEdge WordGraph::FindEdge(PNode n, Symb s) {
  PEdge e = GN(n).edge;
  while (e != ENIL)
    if (GE(e).fsym == s)
      return e;
    else
      e = GE(e).next;
  return ENIL;
}

WordGraph::PSEdge WordGraph::FindSEdge([[maybe_unused]] PNode n, [[maybe_unused]] Symb s) {
#ifdef SHADOW_EDGES
  Node &nd = GN(n);
  PSEdge e1 = nd.sedge, e2 = e1 + nd.nshadow;
  while (e1 != e2)
    if (GSE(e1).fsym == s)
      return e1;
    else
      e1++;
#endif
  return SENIL;
}

WordGraph::PEdge WordGraph::AddEdge(PNode n, Symb s, int len, bool solid, PNode m) {
  PEdge pe = NewEdge();
  Edge &e = GE(pe);
  e.n = m;
  e.fsym = s;
  e.SetLen(len);
  e.SetSolid(solid);

  if (!insatend) {
    // add the edge at the beginning of the list
    e.next = GN(n).edge;
    return (GN(n).edge = pe);
  } else {
    // add the edge at the end
    e.next = ENIL;
    PEdge last = GN(n).edge;
    if (last == ENIL) return (GN(n).edge = pe);
    while (GE(last).next != ENIL) last = GE(last).next;
    return (GE(last).next = pe);
  }
}

// void WordGraph::SortEdges(Node& n)
//{
//	PEdge& head = n.edge;
//	PEdge rest = head;
//	head = ENIL;
//
//	PEdge *p, tmp;
//	int count;
//	while(rest != ENIL) {
//		p = &head;
//		count = GN(GE(rest).n).count;
//		while((*p != ENIL) && (GN(GE(*p).n).count > count))
//			p = &GE(*p).next;
//		tmp = rest;
//		rest = GE(rest).next;
//		GE(tmp).next = *p;
//		*p = tmp;
//	}
//}

Count WordGraph::GetEscCount([[maybe_unused]] PNode n, [[maybe_unused]] int c) {
  DEBUG_ASSERT(c >= 0);
#ifdef EXP_ESC_COUNT
  return (int)(param.esc_coef * pow(c, param.esc_exp)) + param.esc_count;
#else
  return param.esc_count;
#endif
}

Count WordGraph::GetShEscCount([[maybe_unused]] PNode n, [[maybe_unused]] int c) {
  DEBUG_ASSERT(c >= 0);
#ifdef EXP_ESC_COUNT
  return (int)(param.esc_coef * pow(c, param.esc_exp)) + param.esc_count;
#else
  return param.esc_count;
#endif
}

}  // namespace compress
}  // namespace stonedb
