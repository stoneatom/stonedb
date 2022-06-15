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

#include "inc_wgraph.h"

#include <cstring>

namespace stonedb {
namespace compress {

// #define MAKESTAT

const uint IncWGraph::MatchLenCoder::c2[] = {120, 128};
const uint IncWGraph::MatchLenCoder::c3[] = {102, 119, 128};

IncWGraph::IncWGraph() : memory() {
  dump = NULL;
  ROOT = NIL = START = NULL;
  mask = NULL;
  coder = NULL;
  reclen = NULL;
  records = NULL;
}

void IncWGraph::Init() {
  nfinals = 0;
  matchlen_cost = esc_cost = 0;
  // mask = &_mask_;		// uncomment if masking should be used
  mask = NULL;

  // create ROOT and NIL nodes (and START)
  memory.alloc(ROOT);
  memory.alloc(NIL);
  memory.alloc(START);

  ROOT->edge = 0;
  ROOT->nedge = 0;
  ROOT->endpos = 0;
  ROOT->suf = NIL;
  ROOT->total = ROOT->EscCount();

  *START = *ROOT;

  memory.alloc(NIL->edge, 256);
  NIL->nedge = 0;  // means 256
  NIL->endpos = 0;
  NIL->suf = 0;
  NIL->total = 256;

  // create edges from NIL to ROOT, for every possible symbol as a label
  for (uint s = 0; s < 256; s++) NIL->edge[s].Init((uchar)s, 1, true, ROOT, 1);
}
void IncWGraph::Clear() {
  memory.freeall();
  recent.clear();
  ROOT = NIL = START = 0;
}

//-------------------------------------------------------------------------------------------

void IncWGraph::Encode(RangeCoder *cod, char **index, const uint *lens, int nrec, uint &packlen) {
  if ((nrec < 1) || (nrec > 65536) || (!cod)) throw CprsErr::CPRS_ERR_PAR;
  Clear();
  Init();
  coder = cod;
  records = (uchar **)index;
  reclen = lens;

  packlen = 0;
  bool repeated;

  // bool repetitions = true;		// start: assume optimistically that
  // repetitions exist
  uint rep = 1, total = 2;  // start: rep=1, nonrep=1
  for (int rec = 0; rec < nrec; rec++) {
    if (lens[rec] == 0) continue;
    repeated = true;

    if (/*repetitions &&*/ rec && (lens[rec] == lens[rec - 1])) {
      // is the previous record repeated?
      if (std::memcmp(records[rec], records[rec - 1], lens[rec]) == 0)
        coder->Encode(0, rep++, total);
      else {
        coder->Encode(rep, total - rep, total);
        EncodeRec((ushort)rec, repeated);
      }
      if (++total > coder->MAX_TOTAL) {
        rep >>= 1;
        total >>= 1;
      }

      // are the repetitions indeed statistically significant?
      // if(((total & 63) == 0) && (total > 1000) && (rep*(nfinals+2) <
      // 3*total)) repetitions = false; 1/nfinals - prob. of repetition when
      // records are random, independent, uniform distribution (lower
      // approximation)
    } else
      EncodeRec((ushort)rec, repeated);

    if (!repeated) packlen += lens[rec];
  }
  // packlen += lens[nrec-1];
}

void IncWGraph::Decode(RangeCoder *cod, char **index, const uint *lens, int nrec, char *dest, uint dlen) {
  if ((nrec < 1) || (nrec > 65536) || (!cod)) throw CprsErr::CPRS_ERR_PAR;
  Clear();
  Init();
  coder = cod;
  records = (uchar **)index;
  reclen = lens;
  bool repeated;

  // bool repetitions = true;		// start: assume optimistically that
  // repetitions exist
  uint rep = 1, total = 2, cnt;  // start: rep=1, nonrep=1
  uint sum = 0;
  for (int rec = 0; rec < nrec; rec++) {
    records[rec] = (uchar *)dest + sum;
    if (lens[rec] == 0) continue;
    repeated = true;

    if (/*repetitions &&*/ rec && (lens[rec] == lens[rec - 1])) {
      // is the previous record repeated?
      if ((cnt = coder->GetCount(total)) < rep) {
        coder->Decode(0, rep++, total);
        records[rec] = records[rec - 1];
        // std::memcpy(records[rec], records[rec-1], lens[rec]);
      } else {
        coder->Decode(rep, total - rep, total);
        DecodeRec((ushort)rec, dlen - sum, repeated);
      }
      if (++total > coder->MAX_TOTAL) {
        rep >>= 1;
        total >>= 1;
      }

      // are the repetitions indeed statistically significant?
      // if(((total & 63) == 0) && (total > 1000) && (rep*(nfinals+2) <
      // 3*total)) repetitions = false;
    } else
      DecodeRec((ushort)rec, dlen - sum, repeated);

    if (!repeated) sum += lens[rec];
  }
}

void IncWGraph::EncodeRec(ushort rec, bool &repeated) {
  ASSERT(reclen[rec] < 65536, "bad length for word graph");
  ushort proj = 0, edgelen, maxlen;
  ushort restlen = reclen[rec];
  uchar *s = records[rec];
  Node *base = START, *final = 0;
  Edge *edge;
  Count low, total;
  bool solid;

  if (mask) mask->Reset();

  // loop over consecutive substrings to encode:
  // (1) escape to the first node with transition starting with symbol *s,
  // adding edges along the way (2) encode the next substring, (3) if in the
  // middle of an edge, go by suffix links, adding nodes if necessary, to obtain
  // active.proj==0
  while (restlen) {
    DEBUG_ASSERT(proj == 0);

    // (1)
    while (1) {
      total = base->GetMaskTotal(mask);
      if (base->FindEdge(*s, edge, low, mask))
        break;
      else {
        repeated = false;
        if (!final) {
          final = NewFinal(s + restlen);
          solid = true;
        } else
          solid = false;
#ifdef MAKESTAT
        uint pos = coder->GetPos();
#endif
        base->EncodeEsc(coder, low, total);
#ifdef MAKESTAT
        esc_cost += coder->GetPos() - pos;
#endif
        base->AddEdge(*s, restlen, solid, final, memory);
        base = base->suf;
      }
    }

    // (2)
    coder->Encode(low, edge->count, total);  // encode the edge choice

    // find and encode the no. of matching symbols (incl. the first one)
    edgelen = edge->GetLen();
    maxlen = (edgelen < restlen ? edgelen : restlen);  // min(edgelen,restlen)
    proj = edge->NumMatch(s, maxlen);
    s += proj;
    restlen -= proj;
    if ((proj < edge->GetLen()) && restlen) repeated = false;
#ifdef MAKESTAT
    uint pos = coder->GetPos();
#endif
    MatchLenCoder::Encode(coder, proj, maxlen, dump);
#ifdef MAKESTAT
    matchlen_cost += coder->GetPos() - pos;
#endif

    // (3)
    Traverse(base, edge, proj, s, restlen, final, true);
  }
  if (final && !final->suf) final->suf = base;
}

void IncWGraph::DecodeRec(ushort rec, uint dlen, bool &repeated) {
  ushort proj = 0, declen, edgelen, maxlen;
  ASSERT(reclen[rec] < 65536, "bad length for word graph");
  ushort len = reclen[rec];
  ushort restlen = len;
  uchar *s = records[rec];
  uchar fsym = 0;
  Node *base = START, *final = 0;
  Edge *edge;
  Count low, count, total;
  bool solid;
  if (mask) mask->Reset();

  while (restlen) {
    DEBUG_ASSERT(proj == 0);

    // (1)
    while (1) {
      total = base->GetMaskTotal(mask);
      count = (Count)coder->GetCount(total);
      if (base->FindEdge(count, edge, low, mask))
        break;
      else {
        if (repeated) {
          if (len > dlen) throw CprsErr::CPRS_ERR_BUF;
          declen = len - restlen;
          LabelCopy(s, base->endpos - declen,
                    declen);  // copy at once all labels passed till now
          repeated = false;
        }
        if (!final) {
          final = NewFinal(s + restlen);
          solid = true;
        } else
          solid = false;
        base->DecodeEsc(coder, low, total);
        recent.push_back(base->AddEdge(0, restlen, solid, final, memory));
        base = base->suf;
      }
    }

    // (2)
    coder->Decode(low, edge->count, total);  // decode the edge choice

    // decode the no. of matching symbols (incl. the first one)
    edgelen = edge->GetLen();
    maxlen = (edgelen < restlen ? edgelen : restlen);  // min(edgelen,restlen)
    MatchLenCoder::Decode(coder, proj, maxlen);

    // copy decoded symbols to 's'
    restlen -= proj;
    if (!repeated) {
      *s++ = fsym = edge->fsym;  // needed when base=NIL
      LabelCopy(s, edge->target->endpos - edgelen + 1, proj - 1);
    } else if (restlen == 0) {
      s = edge->target->endpos - edgelen + proj;
      records[rec] = s - len;            // set correct index for repeated record
    } else if (proj < edge->GetLen()) {  // it appears that the record is not a
                                         // repetition
      DEBUG_ASSERT(base != NIL);
      if (len > dlen) throw CprsErr::CPRS_ERR_BUF;
      declen = len - restlen;
      LabelCopy(s, edge->target->endpos - edgelen + proj - declen,
                declen);  // copy at once all labels passed till now
      repeated = false;
      DEBUG_ASSERT(recent.empty());
    }

    // set 'fsym' of recently created edges
    for (std::vector<Edge *>::size_type i = recent.size(); i;) recent[--i]->fsym = fsym;
    recent.clear();

    // (3)
    Traverse(base, edge, proj, s, restlen, final, false);
  }
  if (final && !final->suf) final->suf = base;
  DEBUG_ASSERT(recent.empty());
}

//-------------------------------------------------------------------------------------------

inline void IncWGraph::Traverse(Node *&base, Edge *&edge, ushort &proj, uchar *s, ushort restlen, Node *&final,
                                bool encode) {
  if (mask) mask->Reset();
  if (proj == edge->GetLen()) {  // we reached a node
    // update count, perhaps exchange edges to keep them sorted, make scaling if
    // necessary
    if (base != NIL) base->UpdateCount(edge);

    // node duplication
    if (!edge->IsSolid())
      base = edge->target->Duplicate(base, edge, memory);
    else
      base = edge->target;  // canonize
    proj = 0;
  } else {  // we reached a point on the edge
    bool solid = false;
    if (restlen && !final) {
      final = NewFinal(s + restlen);
      solid = true;
    }

    Node *last = 0;
    while (proj) {
      // active point is on a solid edge or no node was recently created ->
      // split the edge, add new node
      if (!last || edge->IsSolid()) {
        Node *node = InsertNode(base, edge, proj);
        if (restlen) {  // edge from 'node' to 'final'
          if (encode)
            node->AddEdge(*s, restlen, solid, final, node->EscCount());
          else
            recent.push_back(node->AddEdge(0, restlen, solid, final, node->EscCount()));
          solid = false;
        } else if (final && !final->suf) {
          final->suf = node;
          DEBUG_ASSERT(!last);
        }
        if (last)
          last->suf = node;  // initialize 'suf' link of the previously created node
        else
          base->UpdateCount(edge);  // only now we can update count of the 'edge'
        last = node;
      } else {  // point is on a non-solid edge and a node was recently created
                // -> redirect the edge
        edge->target = last;
        edge->SetLen(proj);
      }
      base = Node::Canonize(base->suf, edge, proj, s, true);
    }
    last->suf = base;
    if (mask) mask->Add(last->edge[0].fsym);  // assumption: edge[0] is the edge that was split
  }
}

ushort IncWGraph::Node::AllocEdge(IncAlloc &mem) {
  ushort n = GetNEdge(), m = RoundNEdge(n), m1 = RoundNEdge(n + 1);
  DEBUG_ASSERT((n <= 255) && (n <= m) && (m1 <= 256));
  if (m == m1) return n;

  // allocate new array and fill it
  if (m) {
    Edge *e;
    mem.alloc(e, m1);
    std::memcpy(e, edge, n * sizeof(*e));
    mem.freemem(edge, m);
    edge = e;
  } else
    mem.alloc(edge, m1);
  return n;
}

IncWGraph::Node *IncWGraph::Node::Duplicate(Node *base, Edge *e, IncAlloc &mem) {
  // allocate a new node 'n'; copy all fields
  Node *n;
  mem.alloc(n);
  *n = *this;
  suf = n;

  // copy outgoing edges
  if (edge) {
    ushort i = GetNEdge();
    mem.alloc(n->edge, RoundNEdge(i));
    Edge *nedge = n->edge;
    std::memcpy(nedge, edge, i * sizeof(*edge));
    while (i > 0) nedge[--i].SetSolid(false);
  }
  // n->Rescale(4);
  // base->UpdateCount(e, total/4);

  // redirect incoming edges
  e->SetSolid(true);
  ushort proj = e->GetLen();
  do {
    DEBUG_ASSERT(proj && (proj == e->GetLen()));
    e->target = n;
    base = Canonize(base->suf, e, proj, endpos,
                    false);  // move 'q' along suffix link, don't canonize the last edge
  } while (e->target == this);
  DEBUG_ASSERT(e->target == n->suf);

  return n;
}

inline IncWGraph::Node *IncWGraph::Node::Canonize(Node *n, Edge *&e, ushort &proj, uchar *s, bool canonlast) {
  ushort len;
  while (proj) {
    e = n->FindEdge(*(s - proj));
    DEBUG_ASSERT(e);
    // if(canonlast) n->UpdateCount(e, 1);
    len = e->GetLen();
    if ((proj < len) || (!canonlast && (proj == len))) return n;
    n = e->target;
    proj -= len;
  }
  return n;
}

// this is for the FIXME line
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wtype-limits"
void IncWGraph::Node::Rescale(uchar shift) {
  uint t = EscCount(), n = GetNEdge();
  for (uint i = 0; i < n; i++) {
    if (!(edge[i].count _SHR_ASSIGN_ shift)) edge[i].count = 1;
    t += edge[i].count;
  }
  total = t;
  // the compare is reasonable when "MAX_TOTAL = ArithCoder::MAX_TOTAL;"
  // now, we use MAX_TOTAL = RangeCoder::MAX_TOTAL, so the compiler warning
  // 'always false'
  // FIXME: always false
#if 0
    if (total > MAX_TOTAL)
        Rescale();
#endif
}
#pragma GCC diagnostic pop

IncWGraph::Node *IncWGraph::InsertNode([[maybe_unused]] Node *base, Edge *edge, ushort proj) {
  Node *node;
  memory.alloc(node);
  memory.alloc(node->edge, node->RoundNEdge(node->nedge = 1));
  DEBUG_ASSERT(node->RoundNEdge(1) == node->RoundNEdge(2));

  // edge from 'node' to 'edge->target'
  Node *target = edge->target;
  ushort len = edge->GetLen() - proj;
  node->endpos = target->endpos - len;
  node->edge->Init(*(node->endpos), len, edge->IsSolid(), target, node->total = edge->count);
  node->total += node->EscCount();

  // edge from 'base' to 'node'
  edge->target = node;
  edge->SetLen(proj);
  edge->SetSolid(true);

  return node;
}

//-------------------------------------------------------------------------------------------

inline ushort IncWGraph::Node::RoundNEdge(ushort n) {
  // 'edge' array is allocated with sizes: 0,2,4,8,16,... Returns 'n' rounded up
  // to a value of this type.
  if (n == 0)
    return 0;
  else if (n == 1)
    return 2;
  else
    return (ushort)1 _SHL_ core::GetBitLen((uint)(n - 1));
}

inline bool IncWGraph::Node::FindEdge(uchar s, Edge *&e, Count &low, Mask *mask) {
  low = 0;
  if (!edge) return false;
  Edge *stop = edge + GetNEdge();
  e = edge;
  if (mask)
    while (e != stop) {
      if (e->fsym == s) return true;
      if (!mask->Masked(e->fsym)) {
        mask->Add(e->fsym);
        low += e->count;
      }
      e++;
    }
  else
    while (e != stop)
      if (e->fsym == s)
        return true;
      else
        low += (e++)->count;
  return false;
}
inline bool IncWGraph::Node::FindEdge(Count c, Edge *&e, Count &low, Mask *mask) {
  DEBUG_ASSERT(c < total);
  low = 0;
  if (!edge) return false;
  Edge *stop = edge + GetNEdge();
  e = edge;
  if (mask)
    while (e != stop)
      if (mask->Masked(e->fsym))
        e++;
      else if (low + e->count > c)
        return true;
      else {
        mask->Add(e->fsym);
        low += (e++)->count;
      }
  else
    while (e != stop)
      if (low + e->count > c)
        return true;
      else
        low += (e++)->count;
  return false;
}
inline IncWGraph::Edge *IncWGraph::Node::FindEdge(uchar s) {
  Edge *e = edge, *stop = edge + GetNEdge();
  while (e != stop)
    if (e->fsym == s)
      return e;
    else
      e++;
  return 0;
}

inline IncWGraph::Edge *IncWGraph::Node::AddEdge(uchar s, ushort len, bool solid, Node *final, Count esc, ushort e) {
  DEBUG_ASSERT(e <= 255);
  Count t = total;
  total -= esc;
  edge[e].Init(s, len, solid, final, init_count);
  nedge = (uchar)(e + 1);
  total += EscCount() + init_count;
  if ((total <= t)
      // || (total > MAX_TOTAL) FIXME: always false
  )
    Rescale();
  return edge + e;
}
inline IncWGraph::Edge *IncWGraph::Node::AddEdge(uchar s, ushort len, bool solid, Node *final, IncAlloc &mem) {
  Count esc = EscCount();
  return AddEdge(s, len, solid, final, esc, AllocEdge(mem));
}

inline Count IncWGraph::Node::GetMaskTotal(Mask *mask) {
  if (!mask) return total;
  Count mtot = total;
  uint nset = mask->NumSet();
  Edge *e = edge, *stop = edge + GetNEdge();
  for (; nset && e != stop; e++)
    if (mask->Masked(e->fsym)) {
      mtot -= e->count;
      nset--;
    }
  return mtot;
}

inline void IncWGraph::Node::UpdateCount(Edge *&e, Count update) {
  // increase count and total, do rescaling if necessary
  DEBUG_ASSERT((e >= edge) && (e - edge < GetNEdge()));
  Count t = total + update;
  if ((t <= total)
      //  || (t > MAX_TOTAL)) FIXME: always false
  )
    Rescale();
  Count c = (e->count += update);
  total += update;

  // keep edge counts in descending order
  while ((e != edge) && ((e - 1)->count < c)) {
    Edge tmp = *e;
    *e = *(e - 1);
    *(--e) = tmp;
  }

  // propagate count update
  // if(suf->IsNIL() || ((e->count_last += update) < max_count_last)) return;
  // e->count_last = 0;
  // Edge* esuf = suf->FindEdge(e->fsym);
  // suf->UpdateCount(esuf, propag_count);
}

inline void IncWGraph::Edge::Init(uchar fs, ushort l, bool s, Node *t, Count c) {
  fsym = fs;
  target = t;
  count = c;  // count_last = 0;
  SetLen(l);
  SetSolid(s);
}

inline ushort IncWGraph::Edge::NumMatch(uchar *s, ushort maxlen) {
  DEBUG_ASSERT(maxlen && (maxlen <= GetLen()));
  if (maxlen == 1) return 1;
  uchar *q = target->endpos - GetLen();
  DEBUG_ASSERT(*s == *q);
  ushort m = 1;
  while (m < maxlen)
    if (s[m] != q[m])
      return m;
    else
      m++;
  return m;
}

// inline bool IncWGraph::Mask::Add(uchar s)
//{
//	uchar i = s>>3, b = (uchar)1<<(s&3);
//	if(map[i]&b) return false;
//	map[i] |= b;
//	return true;
//}

inline void IncWGraph::MatchLenCoder::Encode(RangeCoder *coder, ushort proj, ushort edgelen,
                                             [[maybe_unused]] FILE *dump) {
  if (edgelen < 2) return;
#ifdef MAKESTAT
  if (dump) std::fprintf(dump, "%d %d\n", (int)proj, (int)edgelen);
#endif
  const uint *c = (edgelen == 2 ? c2 : c3);
  if (proj == edgelen)
    coder->EncodeShift(0, c[0], shift);
  else if (proj == 1)
    coder->EncodeShift(c[0], c[1] - c[0], shift);
  else {
    coder->EncodeShift(c[1], c[2] - c[1], shift);
    coder->EncodeUniform((ushort)(proj - 2), (ushort)(edgelen - 3));
  }
}
inline void IncWGraph::MatchLenCoder::Decode(RangeCoder *coder, ushort &proj, ushort edgelen) {
  if (edgelen < 2) {
    proj = 1;
    return;
  }
  const uint *c = (edgelen == 2 ? c2 : c3);
  Count count = (Count)coder->GetCountShift(shift);
  if (count < c[0]) {
    coder->DecodeShift(0, c[0], shift);
    proj = edgelen;
  } else if (count < c[1]) {
    coder->DecodeShift(c[0], c[1] - c[0], shift);
    proj = 1;
  } else {
    coder->DecodeShift(c[1], c[2] - c[1], shift);
    coder->DecodeUniform(proj, (ushort)(edgelen - 3));
    proj += 2;
  }
}

inline IncWGraph::Node *IncWGraph::NewFinal(uchar *endpos) {
  Node *f;
  memory.alloc(f);
  f->endpos = endpos;
  f->edge = 0;
  f->suf = 0;
  f->total = f->EscCount();
  nfinals++;
  return f;
}

inline void IncWGraph::LabelCopy(uchar *&dest, const uchar *src, ushort len) {
  while (len) {
    *dest++ = *src++;
    len--;
  }
}

//-------------------------------------------------------------------------------------------

void IncWGraph::Print(std::ostream &str, uint flags, Node *n) {
  if (!n) {
    if (START && (START != ROOT)) Print(str, flags, START);
    Print(str, flags, ROOT);
    return;
  }
  str << "- " << n << ": ";
  str << "  suf = " << n->suf;
  str << "  total = " << n->total;

  std::vector<Node *> next;
  int ne = n->GetNEdge();
  for (int e = 0; e < ne; e++) {
    Edge &eg = n->edge[e];
    str << "\n  " << n << " -> " << eg.target << ":  ";
    str << (eg.IsSolid() ? "solid" : "nosld");
    str << "  count = " << eg.count;
    str << "  \"";
    PrintLbl(str, &eg);
    str << "\"  ";
    // str << "  fsym = " << eg.fsym;
    if (eg.IsSolid()) next.push_back(eg.target);
  }
  str << std::endl;

  for (uint i = 0; i < next.size(); i++) Print(str, flags, next[i]);
}
void IncWGraph::PrintLbl(std::ostream &str, Edge *e) {
  uchar *stop = e->target->endpos;
  char c;
  for (uchar *s = stop - e->GetLen(); s != stop; s++) str << (((c = (char)*s) > 13) ? c : '#');
}

}  // namespace compress
}  // namespace stonedb
