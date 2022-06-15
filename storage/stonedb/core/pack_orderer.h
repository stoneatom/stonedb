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
#ifndef STONEDB_CORE_PACK_ORDERER_H_
#define STONEDB_CORE_PACK_ORDERER_H_
#pragma once

#include <vector>

#include "common/common_definitions.h"
#include "vc/virtual_column.h"

namespace stonedb {
namespace core {
class PackOrderer {
 public:
  enum class OrderType {
    RangeSimilarity,  // ...
    MinAsc,           // ascending by pack minimum
    MinDesc,          // descending by pack minimum
    MaxAsc,           // ascending by pack maximum
    MaxDesc,          // descending by pack maximum
    Covering,         // start with packs which quickly covers the whole attribute
                      // domain (MinAsc, then find the next minimally overlapping pack)
    NotSpecified
  };
  enum class State { INIT_VAL = -1, END = -2 };

  struct OrderStat {
    OrderStat(int n, int o) : neutral(n), ordered(o){};
    int neutral;
    int ordered;
  };

  //! Uninitialized object, use Init() before usage
  PackOrderer();
  PackOrderer(const PackOrderer &);
  PackOrderer &operator=(const PackOrderer &);

  /*!
   * Create an orderer for given column,
   * \param vc datapacks of this column will be ordered
   * \param r_filter use only pack not eliminated by this rough filter
   * \param order how to order datapacks
   */
  PackOrderer(vcolumn::VirtualColumn *vc, common::RSValue *r_filter, OrderType order);

  ~PackOrderer() = default;

  //! Initialized Orderer constructed with a default constructor,
  //! ignored if used on a initialized orderer
  //! \return true if successful, otherwise false
  bool Init(vcolumn::VirtualColumn *vc, OrderType order, common::RSValue *r_filter = NULL);

  /*!
   * Reset the iterator, so it will start from the first pack in the given sort
   * order
   */
  void Rewind();

  /*!
   * Reset the iterator to the position associated with given datapack from the
   * given column
   */
  void RewindToMatch(vcolumn::VirtualColumn *vc, MIIterator &mit);

  /*!
   * the current datapack number in the ordered sequence
   * \return pack number or -1 if end of sequence reached
   */
  int Current() {
    return curndx[curvc] < 0 ? -1 : natural_order[curvc] ? curndx[curvc] : packs[curvc][curndx[curvc]].second;
  }

  /*!
   * Advance to the next position in the ordered sequence, return the next
   * datapack number
   */
  PackOrderer &operator++();

  //! Is the end of sequence reached?
  bool IsValid() { return curndx[curvc] >= 0; }
  bool Initialized() { return ncols > 0; }
  // true if the current position and the rest of packs are in ascending order
  bool NaturallyOrdered() { return packs_passed >= packs_ordered_up_to; }

 private:
  enum class MinMaxType { MMT_Fixed, MMT_Float, MMT_Double, MMT_String };

  union MMTU {
    int64_t i;
    double d;
    float f;
    char c[8];
    MMTU(int64_t i) : i(i) {}
  };

  using PackPair = std::pair<MMTU, int>;

  void InitOneColumn(vcolumn::VirtualColumn *vc, OrderType otype, common::RSValue *r_filter, struct OrderStat os);
  void NextPack();
  void RewindCol();
  void ReorderForCovering(std::vector<PackPair> &packs_one_col, vcolumn::VirtualColumn *vc);

  static float basic_sorted_percentage;

  std::vector<std::vector<PackPair>> packs;
  std::vector<int> curndx;   // Current() == packs[curvc][curndx[curvc]]
  std::vector<int> prevndx;  // i = o.Current(); ++o; prevndx[curvc] = i ;
  int curvc;                 //
  int ncols;
  std::vector<bool> natural_order;
  int dimsize;
  std::vector<bool> lastly_left;  // if the last ++ went to the left
  std::vector<MinMaxType> mmtype;
  std::vector<OrderType> otype;

  std::unique_ptr<Filter> visited;  // which pack was visited already; can be
                                    // implemented by Filter class to save space
  int64_t packs_ordered_up_to;      // how many packs are actually reordered (the
                                    // rest of them is left in natural order
  int64_t packs_passed;             // how many packs are already processed
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_PACK_ORDERER_H_