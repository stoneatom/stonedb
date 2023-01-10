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
#ifndef TIANMU_CORE_JUST_A_TABLE_H_
#define TIANMU_CORE_JUST_A_TABLE_H_
#pragma once

#include <memory>
#include <vector>

#include "core/mysql_expression.h"

namespace Tianmu {
namespace types {
class BString;
}  // namespace types
namespace core {
enum class TType { TABLE, TEMP_TABLE };
class Transaction;
class PhysicalColumn;
class Filter;

class JustATable : public std::enable_shared_from_this<JustATable> {
 public:
  static unsigned PackIndex(int64_t obj, uint32_t power) {
    return (obj == common::NULL_VALUE_64 ? 0xFFFFFFFF : (unsigned)(obj >> power));
  }  // null pack number (interpreted properly)
  virtual void LockPackForUse(unsigned attr, unsigned pack_no) = 0;
  virtual void UnlockPackFromUse(unsigned attr, unsigned pack_no) = 0;
  virtual int64_t NumOfObj() const = 0;
  virtual uint NumOfAttrs() const = 0;
  virtual uint NumOfDisplaybleAttrs() const = 0;
  virtual TType TableType() const = 0;

  virtual int64_t GetTable64(int64_t obj, int attr) = 0;
  virtual void GetTable_S(types::BString &s, int64_t obj, int attr) = 0;

  virtual bool IsNull(int64_t obj, int attr) = 0;

  virtual uint MaxStringSize(int n_a, Filter *f = nullptr) = 0;

  virtual std::vector<AttributeTypeInfo> GetATIs(bool orig = false) = 0;

  virtual PhysicalColumn *GetColumn(int col_no) = 0;

  virtual const ColumnType &GetColumnType(int n_a) = 0;
  virtual uint32_t Getpackpower() const = 0;
  //! Returns column value in the form required by complex expressions
  ValueOrNull GetComplexValue(const int64_t obj, const int attr);

  virtual ~JustATable(){};
};

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_JUST_A_TABLE_H_
