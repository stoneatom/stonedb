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
#ifndef TIANMU_CORE_CONDITION_ENCODER_H_
#define TIANMU_CORE_CONDITION_ENCODER_H_
#pragma once

#include "common/common_definitions.h"
#include "core/column_type.h"
#include "core/tianmu_attr.h"

namespace Tianmu {
namespace vcolumn {
class MultiValColumn;
}  // namespace vcolumn

namespace core {
class Descriptor;

class ConditionEncoder {
 public:
  ConditionEncoder(bool additional_nulls, uint32_t power);
  virtual ~ConditionEncoder();

  void operator()(Descriptor &desc);

 private:
  void DoEncode();
  bool IsTransformationNeeded();
  void TransformINs();
  void TransformOtherThanINsOnNotLookup();
  void TransformLIKEs();
  void TextTransformation();

  void EncodeConditionOnStringColumn();
  void EncodeConditionOnNumerics();
  void TransformWithRespectToNulls();
  void DescriptorTransformation();

  void PrepareValueSet(vcolumn::MultiValColumn &mvc);
  void TransformINsOnLookup();
  void TransformLIKEsPattern();
  void TransformLIKEsIntoINsOnLookup();
  void TransformIntoINsOnLookup();
  void TransformOtherThanINsOnNumerics();
  void LookupExpressionTransformation();

  inline common::ColumnType AttrTypeName() const { return attr->TypeName(); }

 public:
  static void EncodeIfPossible(Descriptor &desc, bool for_rough_query, bool additional_nulls);

 private:
  bool additional_nulls;
  ColumnType in_type;
  bool sharp;
  bool encoding_done;
  TianmuAttr *attr;
  Descriptor *desc;
  uint32_t pack_power;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_CONDITION_ENCODER_H_
