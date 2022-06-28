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
#ifndef STONEDB_CORE_COLUMN_TYPE_H_
#define STONEDB_CORE_COLUMN_TYPE_H_
#pragma once

#include "common/common_definitions.h"
#include "core/rc_attr_typeinfo.h"

namespace stonedb {
namespace core {
struct DataType;

struct ColumnType {
  enum class enumCT {
    NOT_NULL = 0,
    AUTO_INC = 1,
    BLOOM_FILTER = 2,
  };

 public:
  ColumnType() : type(common::CT::INT), internal_size(4), display_size(11) {}
  ColumnType(common::CT t, bool notnull = false, common::PackFmt fmt = common::PackFmt::DEFAULT, int prec = 0,
             int sc = 0, DTCollation collation = DTCollation())
      : type(t),
        precision(prec),
        scale(sc),
        display_size(ATI::TextSize(t, prec, sc, collation)),
        collation(collation),
        fmt(fmt) {
    flag[static_cast<int>(enumCT::NOT_NULL)] = notnull;
    internal_size = InternalSize();
  }

  void Initialize(common::CT t, bool notnull, common::PackFmt f, uint prec, int sc,
                  DTCollation collation = DTCollation()) {
    type = t;
    flag[static_cast<int>(enumCT::NOT_NULL)] = notnull;
    fmt = f;
    precision = prec;
    scale = sc;
    this->collation = collation;
    Recalculate();
  }

  explicit ColumnType(const DataType &dt);

  bool operator==(const ColumnType &) const;

  common::CT GetTypeName() const { return type; }
  void SetTypeName(common::CT t) { type = t; }
  // column width, as X in CHAR(X) or DEC(X,*)
  uint GetPrecision() const { return precision; }
  /*! \brief Set column width, as X in DEC(X,*)
   *
   * Used in type conversions in Attr. Cannot be used as
   * Column::Type().setPrecision(p) because Type() returns const, so can be used
   * only by Column subclasses and friends
   */
  void SetPrecision(uint prec) {
    precision = prec;
    Recalculate();
  }

  // number of decimal places after decimal point, as Y in DEC(*,Y)
  int GetScale() const { return scale; }
  /*! \brief number of decimal places after decimal point, as Y in DEC(*,Y)
   *
   * Used in type conversions in Attr. Cannot be used as
   * Column::Type().SetScale(p) because Type() returns const, so can be used
   * only by Column subclasses and friends
   */
  void SetScale(int sc) {
    scale = sc;
    Recalculate();
  }

  // maximal number of bytes occupied in memory by a value of this type
  uint GetInternalSize() const { return internal_size; }
  // Use in cases where actual string length is less then declared, before
  // materialization of Attr
  void OverrideInternalSize(uint size) { internal_size = size; };
  int GetDisplaySize() const { return display_size; }
  bool IsLookup() const { return fmt == common::PackFmt::LOOKUP; }
  ColumnType RemovedLookup() const;

  bool IsNumeric() const {
    switch (type) {
      case common::CT::BIN:
      case common::CT::BYTE:
      case common::CT::VARBYTE:
      case common::CT::STRING:
      case common::CT::VARCHAR:
      case common::CT::LONGTEXT:
        return false;
      default:
        return true;
    }
  }

  bool IsKnown() const { return type != common::CT::UNK; };
  bool IsFixed() const { return ATI::IsFixedNumericType(type); };
  bool IsFloat() const { return ATI::IsRealType(type); };
  bool IsInt() const { return IsFixed() && scale == 0; }
  bool IsString() const { return ATI::IsStringType(type); }
  bool IsDateTime() const { return ATI::IsDateTimeType(type); }
  const DTCollation &GetCollation() const { return collation; }
  void SetCollation(DTCollation _collation) { collation = _collation; }
  bool IsNumComparable(const ColumnType &sec) const {
    if (IsLookup() || sec.IsLookup() || IsString() || sec.IsString()) return false;
    if (scale != sec.scale) return false;
    if (IsFixed() && sec.IsFixed()) return true;
    if (IsFloat() && sec.IsFloat()) return true;
    if (IsDateTime() && sec.IsDateTime()) return true;
    return false;
  }
  common::PackFmt GetFmt() const { return fmt; }
  void SetFmt(common::PackFmt f) { fmt = f; }
  unsigned char GetFlag() const { return flag.to_ulong(); }
  void SetFlag(unsigned char v) { flag = std::bitset<std::numeric_limits<unsigned char>::digits>(v); }
  bool NotNull() const { return flag[static_cast<int>(enumCT::NOT_NULL)]; }
  bool Nullable() const { return !NotNull(); }
  bool GetAutoInc() const { return flag[static_cast<int>(enumCT::AUTO_INC)]; }
  void SetAutoInc(bool inc) { flag[static_cast<int>(enumCT::AUTO_INC)] = inc; }
  bool HasFilter() const { return flag[static_cast<int>(enumCT::BLOOM_FILTER)]; }

 private:
  common::CT type;
  uint precision = 0;
  int scale = 0;
  uint internal_size;
  int display_size;
  DTCollation collation;
  common::PackFmt fmt = common::PackFmt::DEFAULT;

  std::bitset<std::numeric_limits<unsigned char>::digits> flag;

  // calculate maximal number of bytes a value of a given type can take in
  // memory
  uint InternalSize();

  void Recalculate() {
    internal_size = InternalSize();
    display_size = ATI::TextSize(type, precision, scale, collation);
  }
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_COLUMN_TYPE_H_
