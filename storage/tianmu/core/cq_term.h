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
#ifndef TIANMU_CORE_CQ_TERM_H_
#define TIANMU_CORE_CQ_TERM_H_
#pragma once

#include "common/common_definitions.h"
#include "types/tianmu_data_types.h"

namespace Tianmu {

namespace vcolumn {
class VirtualColumn;
}

namespace utils {
class Hash64;
}  // namespace utils

namespace core {
struct AttrID {
  int n;
  AttrID() : n(common::NULL_VALUE_32) {}
  explicit AttrID(int _n) : n(_n) {}
};

struct TabID {
  int n;
  TabID() : n(common::NULL_VALUE_32) {}
  explicit TabID(int _n) : n(_n) {}
  bool IsNullID() const { return (n == common::NULL_VALUE_32); }
  bool operator==(const TabID &other) const { return (n == other.n) && (!IsNullID()); }
  bool operator<(const TabID &other) const { return (n < other.n) && (!IsNullID()); }
  bool operator!=(const TabID &other) const { return !(operator==(other)); }
};

struct CondID {
  int n;
  CondID() : n(common::NULL_VALUE_32) {}
  explicit CondID(int _n) { n = _n; }
  bool IsNull() const { return (n == common::NULL_VALUE_32); }
  bool IsInvalid() const { return (n < 0); }
  bool operator==(const CondID &other) const { return (n == other.n) && (!IsNull()); }
};

enum class JoinType { JO_INNER, JO_LEFT, JO_RIGHT, JO_FULL };
enum class TMParameter { TM_DISTINCT, TM_TOP, TM_EXISTS };  // Table Mode Parameter
enum class CondType {
  UNKOWN_COND,
  WHERE_COND,
  HAVING_COND,
  ON_INNER_FILTER,
  ON_LEFT_FILTER,
  ON_RIGHT_FILTER,
  OR_SUBTREE,
  AND_SUBTREE
};

/**
  Interpretation of CQTerm depends on which parameters are used.
  All unused parameters must be set to NULL_VALUE (int), nullptr (pointers),
  SF_NONE (SimpleFunction), common::NULL_VALUE_64 (Tint64_t).

  When these parameters are set:            then the meaning is:
        attr_id                     attribute value of the current
 table/TempTable attr_id, tab_id             attribute value of a given
 table/TempTable func, attr_id               attribute value of the current
 table/TempTable with a simple function applied func, attr_id, tab_id attribute
 value of a given table/TempTable with a simple function applied tab_id value of
 a TempTable (execution of a subquery) param_id                    parameter
 value c_term                      complex expression, defined elsewhere (?)

  Note: if all above are not used, it is interpreted as a constant (even if
 null) val_n                       numerical (fixed precision) constant val_t
 numerical (fixed precision) constant
 * note that all values may be safely copied (pointers are managed outside the
 class)
 */
struct CQTerm {
  common::ColumnType type;  // type of constant
  vcolumn::VirtualColumn *vc;
  types::CondArray cond_value;
  std::shared_ptr<utils::Hash64> cond_numvalue;

  int vc_id;         // virt column number set at compilation, = -1 for legacy cases
                     // (not using virt column)
  bool is_vc_owner;  // indicator if vc should be it deleted in destructor
  Item *item;

  CQTerm();  // null
  explicit CQTerm(int v, Item *item_arg = nullptr);
  explicit CQTerm(int64_t v, common::ColumnType t = common::ColumnType::INT, Item *item_arg = nullptr);
  CQTerm(const CQTerm &);
  ~CQTerm();

  bool IsNull() const { return (vc_id == common::NULL_VALUE_32 && vc == nullptr); }
  CQTerm &operator=(const CQTerm &);
  bool operator==(const CQTerm &) const;
  char *ToString(char *buf, int tab_id) const;
  char *ToString(char p_buf[], size_t buf_ct, int tab_id) const;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_CQ_TERM_H_
