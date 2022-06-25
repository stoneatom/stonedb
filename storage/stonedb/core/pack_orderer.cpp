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

#include "pack_orderer.h"

#include "core/mi_iterator.h"

namespace stonedb {
namespace core {
float PackOrderer::basic_sorted_percentage = 10.0;  // ordering on just sorted_percentage% of packs

PackOrderer::PackOrderer() {
  curvc = 0;
  ncols = 0;
  packs.push_back(std::vector<PackPair>());
  otype.push_back(OrderType::NotSpecified);
  packs_ordered_up_to = 0;
  packs_passed = 0;
}

PackOrderer::PackOrderer(const PackOrderer &p) {
  if (visited.get()) visited.reset(new Filter(*p.visited.get()));
  packs = p.packs;
  curndx = p.curndx;
  prevndx = p.prevndx;
  curvc = p.curvc;
  ncols = p.ncols;
  natural_order = p.natural_order;
  dimsize = p.dimsize;
  lastly_left = p.lastly_left;
  mmtype = p.mmtype;
  otype = p.otype;
  packs_ordered_up_to = p.packs_ordered_up_to;
  packs_passed = p.packs_passed;
}

PackOrderer &PackOrderer::operator=(const PackOrderer &p) {
  visited.reset(new Filter(*p.visited.get()));
  packs = p.packs;
  curndx = p.curndx;
  prevndx = p.prevndx;
  curvc = p.curvc;
  ncols = p.ncols;
  natural_order = p.natural_order;
  dimsize = p.dimsize;
  lastly_left = p.lastly_left;
  mmtype = p.mmtype;
  otype = p.otype;
  packs_ordered_up_to = p.packs_ordered_up_to;
  packs_passed = p.packs_passed;
  return *this;
}

PackOrderer::PackOrderer(vcolumn::VirtualColumn *vc, common::RSValue *r_filter, OrderType order) {
  Init(vc, order, r_filter);
}

bool PackOrderer::Init(vcolumn::VirtualColumn *vc, OrderType order, common::RSValue *r_filter) {
  DEBUG_ASSERT(vc->GetDim() != -1);  // works only for vcolumns based on a single table

  if (Initialized()) return false;

  packs.clear();
  otype.clear();

  curvc = 0;

  InitOneColumn(vc, order, r_filter, OrderStat(0, 1));

  return true;
}

void PackOrderer::InitOneColumn(vcolumn::VirtualColumn *vc, OrderType ot, common::RSValue *r_filter,
                                struct OrderStat os) {
  ++ncols;
  MinMaxType mmt;
  if (vc->Type().IsFixed() || vc->Type().IsDateTime()) {
    mmt = MinMaxType::MMT_Fixed;
  } else
    mmt = MinMaxType::MMT_String;

  mmtype.push_back(mmt);
  otype.push_back(ot);
  packs.push_back(std::vector<PackPair>());
  lastly_left.push_back(true);
  prevndx.push_back(static_cast<int>(State::INIT_VAL));
  curndx.push_back(static_cast<int>(State::INIT_VAL));

  auto &packs_one_col = packs[ncols - 1];
  int d = vc->GetDim();
  MIIterator mit(vc->GetMultiIndex(), d, true);

  MMTU mid(0);
  while (mit.IsValid()) {
    int pack = mit.GetCurPackrow(d);
    if (!r_filter || r_filter[pack] != common::RSValue::RS_NONE) {
      if (mmt == MinMaxType::MMT_Fixed) {
        if (vc->GetNumOfNulls(mit) == mit.GetPackSizeLeft()) {
          mid.i = common::PLUS_INF_64;
        } else {
          int64_t min = vc->GetMinInt64(mit);
          int64_t max = vc->GetMaxInt64(mit);
          switch (ot) {
            case OrderType::RangeSimilarity:
              mid.i = (min == common::NULL_VALUE_64 ? common::MINUS_INF_64 : (max - min) / 2);
              break;
            case OrderType::MinAsc:
            case OrderType::MinDesc:
            case OrderType::Covering:
              mid.i = min;
              break;
            case OrderType::MaxAsc:
            case OrderType::MaxDesc:
              mid.i = max;
              break;
            case OrderType::NotSpecified:
              break;
          }
        }
        packs_one_col.push_back(std::make_pair(mid, pack));
      } else {
        // not implemented for strings & doubles/floats
        // keep the original order
        if (r_filter) {
          // create a list with natural order. For !r_filter just use ++ to
          // visit each pack
          mid.i = mit.GetCurPackrow(d);
          packs_one_col.push_back(std::make_pair(mid, pack));
        } else
          break;  // no need for iterating mit
      }
    }
    mit.NextPackrow();
  }

  if (packs_one_col.size() == 0 && !r_filter)
    natural_order.push_back(true);
  else
    natural_order.push_back(false);

  if (mmt == MinMaxType::MMT_Fixed) {
    switch (ot) {
      case OrderType::RangeSimilarity:
      case OrderType::MinAsc:
      case OrderType::MaxAsc:
        sort(packs_one_col.begin(), packs_one_col.end(), [](const auto &v1, const auto &v2) {
          return v1.first.i < v2.first.i || (v1.first.i == v2.first.i && v1.second < v2.second);
        });
        break;
      case OrderType::Covering:
        ReorderForCovering(packs_one_col, vc);
        break;
      case OrderType::MinDesc:
      case OrderType::MaxDesc:
        sort(packs_one_col.begin(), packs_one_col.end(), [](const auto &v1, const auto &v2) {
          return v1.first.i > v2.first.i || (v1.first.i == v2.first.i && v1.second < v2.second);
        });
        break;
      case OrderType::NotSpecified:
        break;
    }
  }

  // Resort in natural order, leaving only the beginning ordered
  auto packs_one_col_begin = packs_one_col.begin();
  float sorted_percentage =
      basic_sorted_percentage / (os.neutral + os.ordered ? os.neutral + os.ordered : 1);  // todo: using os.sorted
  packs_ordered_up_to = (int64_t)(packs_one_col.size() * (sorted_percentage / 100.0));
  if (packs_ordered_up_to > 1000)  // do not order too much packs (for large cases)
    packs_ordered_up_to = 1000;
  packs_one_col_begin += packs_ordered_up_to;  // ordering on just sorted_percentage% of packs
  sort(packs_one_col_begin, packs_one_col.end(), [](const auto &v1, const auto &v2) { return v1.second < v2.second; });
}

void PackOrderer::ReorderForCovering(std::vector<PackPair> &packs_one_col, vcolumn::VirtualColumn *vc) {
  sort(packs_one_col.begin(), packs_one_col.end(), [](const auto &v1, const auto &v2) {
    return v1.first.i < v2.first.i || (v1.first.i == v2.first.i && v1.second < v2.second);
  });

  int d = vc->GetDim();
  MIDummyIterator mit(vc->GetMultiIndex());
  // Algorithm: store at the beginning part all the packs which covers the whole
  // scope
  uint j = 0;  // j is a position of the last swapped pack (the end of
               // "beginning" part)
  int i_max;   // i_max is a position of the best pack found, i.e. min(i_max) <=
               // max(j), max(i_max)->max
  uint i = 1;  // i is the current seek position
  while (i < packs_one_col.size()) {
    mit.SetPack(d, packs_one_col[j].second);       // vector of pairs: <mid, pack>
    int64_t max_up_to_now = vc->GetMaxInt64(mit);  // max of the last pack from the beginning
    if (max_up_to_now == common::PLUS_INF_64) break;
    // Find max(max) of packs for which min<=max_up_to_now
    i_max = -1;
    int64_t best_local_max = max_up_to_now;
    while (i < packs_one_col.size() && packs_one_col[i].first.i <= max_up_to_now) {
      mit.SetPack(d, packs_one_col[i].second);
      int64_t local_max = vc->GetMaxInt64(mit);
      if (local_max > best_local_max) {
        best_local_max = local_max;
        i_max = i;
      }
      i++;
    }
    if (i_max == -1 && i < packs_one_col.size())  // suitable pack not found
      i_max = i;                                  // just get the next good one
    j++;
    if (i_max > int(j)) {
      std::swap(packs_one_col[j].first, packs_one_col[i_max].first);
      std::swap(packs_one_col[j].second, packs_one_col[i_max].second);
    }
  }
  // Order the rest of packs in a semi-covering way
  auto step = (packs_one_col.size() - j) / 2;
  while (step > 1 && j < packs_one_col.size() - 1) {
    for (i = j + step; i < packs_one_col.size(); i += step) {
      j++;
      std::swap(packs_one_col[j].first, packs_one_col[i].first);
      std::swap(packs_one_col[j].second, packs_one_col[i].second);
    }
    step = step / 2;
  }
}

void PackOrderer::NextPack() {
  packs_passed++;
  if (natural_order[curvc]) {
    // natural order traversing all packs
    if (curndx[curvc] < dimsize - 1 && curndx[curvc] != static_cast<int>(State::END))
      ++curndx[curvc];
    else
      curndx[curvc] = static_cast<int>(State::END);
  } else
    switch (otype[curvc]) {
      case OrderType::RangeSimilarity: {
        if (lastly_left[curvc]) {
          if (size_t(prevndx[curvc]) < packs[curvc].size() - 1) {
            lastly_left[curvc] = !lastly_left[curvc];
            int tmp = curndx[curvc];
            curndx[curvc] = prevndx[curvc] + 1;
            prevndx[curvc] = tmp;
          } else {
            if (curndx[curvc] > 0)
              --curndx[curvc];
            else
              curndx[curvc] = static_cast<int>(State::END);
          }
        } else if (prevndx[curvc] > 0) {
          lastly_left[curvc] = !lastly_left[curvc];
          int tmp = curndx[curvc];
          curndx[curvc] = prevndx[curvc] - 1;
          prevndx[curvc] = tmp;
        } else {
          if (size_t(curndx[curvc]) < packs[curvc].size() - 1)
            ++curndx[curvc];
          else
            curndx[curvc] = static_cast<int>(State::END);
        }
        break;
      }
      default:
        // go along packs from 0 to packs[curvc].size() - 1
        if (curndx[curvc] != static_cast<int>(State::END)) {
          if (curndx[curvc] < (int)packs[curvc].size() - 1)
            ++curndx[curvc];
          else {
            curndx[curvc] = static_cast<int>(State::END);
          }
        }
        break;
    }
}

PackOrderer &PackOrderer::operator++() {
  DEBUG_ASSERT(Initialized());
  if (ncols == 1) {
    NextPack();
  } else {
    curvc = (curvc + 1) % ncols;

    do {
      NextPack();
    } while (curndx[curvc] != static_cast<int>(State::END) &&
             visited->Get(natural_order[curvc] ? curndx[curvc] : packs[curvc][curndx[curvc]].second));

    if (curndx[curvc] != static_cast<int>(State::END)) {
      visited->Set(natural_order[curvc] ? curndx[curvc] : packs[curvc][curndx[curvc]].second);
    }
  }
  return *this;
}

void PackOrderer::Rewind() {
  packs_passed = 0;
  for (curvc = 0; curvc < ncols; curvc++) RewindCol();
  curvc = 0;
  if (visited.get()) visited->Reset();
}

void PackOrderer::RewindCol() {
  if (!natural_order[curvc] && packs[curvc].size() == 0)
    curndx[curvc] = static_cast<int>(State::END);
  else
    curndx[curvc] = prevndx[curvc] = static_cast<int>(State::INIT_VAL);
}

void PackOrderer::RewindToMatch(vcolumn::VirtualColumn *vc, MIIterator &mit) {
  DEBUG_ASSERT(vc->GetDim() != -1);
  DEBUG_ASSERT(otype[curvc] == OrderType::RangeSimilarity);
  DEBUG_ASSERT(ncols == 1);  // not implemented otherwise

  if (mmtype[curvc] == MinMaxType::MMT_Fixed) {
    int64_t mid = common::MINUS_INF_64;
    if (vc->GetNumOfNulls(mit) != mit.GetPackSizeLeft()) {
      int64_t min = vc->GetMinInt64(mit);
      int64_t max = vc->GetMaxInt64(mit);
      mid = (max - min) / 2;
    }
    auto it = std::lower_bound(packs[curvc].begin(), packs[curvc].end(), PackPair(mid, 0),
                               [](const auto &v1, const auto &v2) {
                                 return v1.first.i < v2.first.i || (v1.first.i == v2.first.i && v1.second < v2.second);
                               });
    if (it == packs[curvc].end())
      curndx[curvc] = int(packs[curvc].size() - 1);
    else
      curndx[curvc] = int(distance(packs[curvc].begin(), it));
  } else
    // not implemented for strings & doubles/floats
    curndx[curvc] = 0;

  if (packs[curvc].size() == 0 && !natural_order[curvc]) curndx[curvc] = static_cast<int>(State::END);
  prevndx[curvc] = curndx[curvc];
}
}  // namespace core
}  // namespace stonedb
