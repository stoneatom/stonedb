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

namespace Tianmu {
namespace core {

float PackOrderer::basic_sorted_percentage_ = 10.0;  // ordering on just sorted_percentage% of packs

PackOrderer::PackOrderer() {
  cur_vc_ = 0;
  n_cols_ = 0;
  packs_.push_back(std::vector<PackPair>());
  order_type_.push_back(OrderType::kNotSpecified);
  packs_ordered_up_to_ = 0;
  packs_passed_ = 0;
}

PackOrderer::PackOrderer(const PackOrderer &p) {
  if (visited_.get())
    visited_.reset(new Filter(*p.visited_.get()));
  packs_ = p.packs_;
  cur_ndx_ = p.cur_ndx_;
  prev_ndx_ = p.prev_ndx_;
  cur_vc_ = p.cur_vc_;
  n_cols_ = p.n_cols_;
  natural_order_ = p.natural_order_;
  dim_size_ = p.dim_size_;
  lastly_left_ = p.lastly_left_;
  min_max_type_ = p.min_max_type_;
  order_type_ = p.order_type_;
  packs_ordered_up_to_ = p.packs_ordered_up_to_;
  packs_passed_ = p.packs_passed_;
}

PackOrderer &PackOrderer::operator=(const PackOrderer &p) {
  visited_.reset(new Filter(*p.visited_.get()));
  packs_ = p.packs_;
  cur_ndx_ = p.cur_ndx_;
  prev_ndx_ = p.prev_ndx_;
  cur_vc_ = p.cur_vc_;
  n_cols_ = p.n_cols_;
  natural_order_ = p.natural_order_;
  dim_size_ = p.dim_size_;
  lastly_left_ = p.lastly_left_;
  min_max_type_ = p.min_max_type_;
  order_type_ = p.order_type_;
  packs_ordered_up_to_ = p.packs_ordered_up_to_;
  packs_passed_ = p.packs_passed_;
  return *this;
}

PackOrderer::PackOrderer(vcolumn::VirtualColumn *vc, common::RoughSetValue *r_filter, OrderType order) {
  Init(vc, order, r_filter);
}

bool PackOrderer::Init(vcolumn::VirtualColumn *vc, OrderType order, common::RoughSetValue *r_filter) {
  DEBUG_ASSERT(vc->GetDim() != -1);  // works only for vcolumns based on a single table

  if (Initialized())
    return false;

  packs_.clear();
  order_type_.clear();

  cur_vc_ = 0;

  InitOneColumn(vc, order, r_filter, OrderStat(0, 1));

  return true;
}

void PackOrderer::InitOneColumn(vcolumn::VirtualColumn *vc, OrderType ot, common::RoughSetValue *r_filter,
                                struct OrderStat os) {
  ++n_cols_;
  MinMaxType mmt;
  if (vc->Type().IsFixed() || vc->Type().IsDateTime()) {
    mmt = MinMaxType::kMMTFixed;
  } else
    mmt = MinMaxType::kMMTString;

  min_max_type_.push_back(mmt);
  order_type_.push_back(ot);
  packs_.push_back(std::vector<PackPair>());
  lastly_left_.push_back(true);
  prev_ndx_.push_back(static_cast<int>(State::kInitVal));
  cur_ndx_.push_back(static_cast<int>(State::kInitVal));

  auto &packs_one_col = packs_[n_cols_ - 1];
  int d = vc->GetDim();
  MIIterator mit(vc->GetMultiIndex(), d, true);

  MMTU mid(0);
  while (mit.IsValid()) {
    int pack = mit.GetCurPackrow(d);
    if (!r_filter || r_filter[pack] != common::RoughSetValue::RS_NONE) {
      if (mmt == MinMaxType::kMMTFixed) {
        if (vc->GetNumOfNulls(mit) == mit.GetPackSizeLeft()) {
          mid.i = common::PLUS_INF_64;
        } else {
          int64_t min = vc->GetMinInt64(mit);
          int64_t max = vc->GetMaxInt64(mit);
          switch (ot) {
            case OrderType::kRangeSimilarity:
              mid.i = (min == common::NULL_VALUE_64 ? common::MINUS_INF_64 : (max - min) / 2);
              break;
            case OrderType::kMinAsc:
            case OrderType::kMinDesc:
            case OrderType::kCovering:
              mid.i = min;
              break;
            case OrderType::kMaxAsc:
            case OrderType::kMaxDesc:
              mid.i = max;
              break;
            case OrderType::kNotSpecified:
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
    natural_order_.push_back(true);
  else
    natural_order_.push_back(false);

  if (mmt == MinMaxType::kMMTFixed) {
    switch (ot) {
      case OrderType::kRangeSimilarity:
      case OrderType::kMinAsc:
      case OrderType::kMaxAsc:
        sort(packs_one_col.begin(), packs_one_col.end(), [](const auto &v1, const auto &v2) {
          return v1.first.i < v2.first.i || (v1.first.i == v2.first.i && v1.second < v2.second);
        });
        break;
      case OrderType::kCovering:
        ReorderForCovering(packs_one_col, vc);
        break;
      case OrderType::kMinDesc:
      case OrderType::kMaxDesc:
        sort(packs_one_col.begin(), packs_one_col.end(), [](const auto &v1, const auto &v2) {
          return v1.first.i > v2.first.i || (v1.first.i == v2.first.i && v1.second < v2.second);
        });
        break;
      case OrderType::kNotSpecified:
        break;
    }
  }

  // Resort in natural order, leaving only the beginning ordered
  auto packs_one_col_begin = packs_one_col.begin();
  float sorted_percentage =
      basic_sorted_percentage_ / (os.neutral + os.ordered ? os.neutral + os.ordered : 1);  // todo: using os.sorted
  packs_ordered_up_to_ = (int64_t)(packs_one_col.size() * (sorted_percentage / 100.0));
  if (packs_ordered_up_to_ > 1000)  // do not order too much packs (for large cases)
    packs_ordered_up_to_ = 1000;
  packs_one_col_begin += packs_ordered_up_to_;  // ordering on just sorted_percentage% of packs
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
    if (max_up_to_now == common::PLUS_INF_64)
      break;
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
  packs_passed_++;
  if (natural_order_[cur_vc_]) {
    // natural order traversing all packs
    if (cur_ndx_[cur_vc_] < dim_size_ - 1 && cur_ndx_[cur_vc_] != static_cast<int>(State::kEnd))
      ++cur_ndx_[cur_vc_];
    else
      cur_ndx_[cur_vc_] = static_cast<int>(State::kEnd);
  } else
    switch (order_type_[cur_vc_]) {
      case OrderType::kRangeSimilarity: {
        if (lastly_left_[cur_vc_]) {
          if (size_t(prev_ndx_[cur_vc_]) < packs_[cur_vc_].size() - 1) {
            lastly_left_[cur_vc_] = !lastly_left_[cur_vc_];
            int tmp = cur_ndx_[cur_vc_];
            cur_ndx_[cur_vc_] = prev_ndx_[cur_vc_] + 1;
            prev_ndx_[cur_vc_] = tmp;
          } else {
            if (cur_ndx_[cur_vc_] > 0)
              --cur_ndx_[cur_vc_];
            else
              cur_ndx_[cur_vc_] = static_cast<int>(State::kEnd);
          }
        } else if (prev_ndx_[cur_vc_] > 0) {
          lastly_left_[cur_vc_] = !lastly_left_[cur_vc_];
          int tmp = cur_ndx_[cur_vc_];
          cur_ndx_[cur_vc_] = prev_ndx_[cur_vc_] - 1;
          prev_ndx_[cur_vc_] = tmp;
        } else {
          if (size_t(cur_ndx_[cur_vc_]) < packs_[cur_vc_].size() - 1)
            ++cur_ndx_[cur_vc_];
          else
            cur_ndx_[cur_vc_] = static_cast<int>(State::kEnd);
        }
        break;
      }
      default:
        // go along packs from 0 to packs[cur_vc_].size() - 1
        if (cur_ndx_[cur_vc_] != static_cast<int>(State::kEnd)) {
          if (cur_ndx_[cur_vc_] < (int)packs_[cur_vc_].size() - 1)
            ++cur_ndx_[cur_vc_];
          else {
            cur_ndx_[cur_vc_] = static_cast<int>(State::kEnd);
          }
        }
        break;
    }
}

PackOrderer &PackOrderer::operator++() {
  DEBUG_ASSERT(Initialized());
  if (n_cols_ == 1) {
    NextPack();
  } else {
    cur_vc_ = (cur_vc_ + 1) % n_cols_;

    do {
      NextPack();
    } while (cur_ndx_[cur_vc_] != static_cast<int>(State::kEnd) &&
             visited_->Get(natural_order_[cur_vc_] ? cur_ndx_[cur_vc_] : packs_[cur_vc_][cur_ndx_[cur_vc_]].second));

    if (cur_ndx_[cur_vc_] != static_cast<int>(State::kEnd)) {
      visited_->Set(natural_order_[cur_vc_] ? cur_ndx_[cur_vc_] : packs_[cur_vc_][cur_ndx_[cur_vc_]].second);
    }
  }
  return *this;
}

void PackOrderer::Rewind() {
  packs_passed_ = 0;
  for (cur_vc_ = 0; cur_vc_ < n_cols_; cur_vc_++) RewindCol();
  cur_vc_ = 0;
  if (visited_.get())
    visited_->Reset();
}

void PackOrderer::RewindCol() {
  if (!natural_order_[cur_vc_] && packs_[cur_vc_].size() == 0)
    cur_ndx_[cur_vc_] = static_cast<int>(State::kEnd);
  else
    cur_ndx_[cur_vc_] = prev_ndx_[cur_vc_] = static_cast<int>(State::kInitVal);
}

void PackOrderer::RewindToMatch(vcolumn::VirtualColumn *vc, MIIterator &mit) {
  DEBUG_ASSERT(vc->GetDim() != -1);
  DEBUG_ASSERT(order_type_[cur_vc_] == OrderType::kRangeSimilarity);
  DEBUG_ASSERT(n_cols_ == 1);  // not implemented otherwise

  if (min_max_type_[cur_vc_] == MinMaxType::kMMTFixed) {
    int64_t mid = common::MINUS_INF_64;
    if (vc->GetNumOfNulls(mit) != mit.GetPackSizeLeft()) {
      int64_t min = vc->GetMinInt64(mit);
      int64_t max = vc->GetMaxInt64(mit);
      mid = (max - min) / 2;
    }
    auto it = std::lower_bound(packs_[cur_vc_].begin(), packs_[cur_vc_].end(), PackPair(mid, 0),
                               [](const auto &v1, const auto &v2) {
                                 return v1.first.i < v2.first.i || (v1.first.i == v2.first.i && v1.second < v2.second);
                               });
    if (it == packs_[cur_vc_].end())
      cur_ndx_[cur_vc_] = int(packs_[cur_vc_].size() - 1);
    else
      cur_ndx_[cur_vc_] = int(distance(packs_[cur_vc_].begin(), it));
  } else
    // not implemented for strings & doubles/floats
    cur_ndx_[cur_vc_] = 0;

  if (packs_[cur_vc_].size() == 0 && !natural_order_[cur_vc_])
    cur_ndx_[cur_vc_] = static_cast<int>(State::kEnd);
  prev_ndx_[cur_vc_] = cur_ndx_[cur_vc_];
}

}  // namespace core
}  // namespace Tianmu
