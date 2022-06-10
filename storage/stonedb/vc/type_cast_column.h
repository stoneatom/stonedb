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
#ifndef STONEDB_VC_TYPE_CAST_COLUMN_H_
#define STONEDB_VC_TYPE_CAST_COLUMN_H_
#pragma once

#include "core/mi_iterator.h"
#include "vc/virtual_column.h"

namespace stonedb {
namespace vcolumn {
class TypeCastColumn : public VirtualColumn {
 public:
  TypeCastColumn(VirtualColumn *from, core::ColumnType const &to);
  TypeCastColumn(const TypeCastColumn &c);

  const std::vector<VarMap> &GetVarMap() const { return vc->GetVarMap(); }
  //	void SetParam(VarID var, core::ValueOrNull val) {vc->SetParam(var,
  // val);}
  void LockSourcePacks(const core::MIIterator &mit) override { vc->LockSourcePacks(mit); }
  void LockSourcePacks(const core::MIIterator &mit, int th_no);
  void UnlockSourcePacks() override { vc->UnlockSourcePacks(); }
  void MarkUsedDims(core::DimensionVector &dims_usage) override { vc->MarkUsedDims(dims_usage); }
  std::set<int> GetDimensions() override { return vc->GetDimensions(); }
  int64_t NoTuples() override { return vc->NoTuples(); }
  bool IsConstExpression(core::MysqlExpression *expr, int temp_table_alias, const std::vector<int> *aliases) {
    return vc->IsConstExpression(expr, temp_table_alias, aliases);
  }
  int GetDim() override { return vc->GetDim(); }
  void RequestEval(const core::MIIterator &mit, const int tta) override { vc->RequestEval(mit, tta); }
  bool IsConst() const override { return vc->IsConst(); }
  void GetNotNullValueString(types::BString &s, const core::MIIterator &m) override {
    return vc->GetNotNullValueString(s, m);
  }
  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override { return vc->GetNotNullValueInt64(mit); }
  bool IsDistinctInTable() override { return vc->IsDistinctInTable(); }  // sometimes overriden in subclasses
  bool IsDeterministic() override { return vc->IsDeterministic(); }
  bool IsParameterized() const override { return vc->IsParameterized(); }
  std::vector<VarMap> &GetVarMap() override { return vc->GetVarMap(); }
  bool IsTypeCastColumn() const override { return true; }
  const core::MysqlExpression::sdbfields_cache_t &GetSDBItems() const override { return vc->GetSDBItems(); }
  bool CanCopy() const override { return vc->CanCopy(); }
  bool IsThreadSafe() override { return vc->IsThreadSafe(); }
  std::vector<VirtualColumn *> GetChildren() const override { return std::vector<VirtualColumn *>(1, vc); }

 protected:
  int64_t DoGetValueInt64(const core::MIIterator &mit) override { return vc->GetValueInt64(mit); }
  bool DoIsNull(const core::MIIterator &mit) override { return vc->IsNull(mit); }
  void DoGetValueString(types::BString &s, const core::MIIterator &m) override { return vc->GetValueString(s, m); }
  double DoGetValueDouble(const core::MIIterator &m) override { return vc->GetValueDouble(m); }
  types::RCValueObject DoGetValue(const core::MIIterator &m, bool lookup_to_num) override {
    return vc->GetValue(m, lookup_to_num);
  }
  int64_t DoGetMinInt64([[maybe_unused]] const core::MIIterator &m) override { return common::MINUS_INF_64; }
  int64_t DoGetMaxInt64([[maybe_unused]] const core::MIIterator &m) override { return common::PLUS_INF_64; }
  types::BString DoGetMinString([[maybe_unused]] const core::MIIterator &m) override { return types::BString(); }
  types::BString DoGetMaxString([[maybe_unused]] const core::MIIterator &m) override { return types::BString(); }
  int64_t DoRoughMin() override { return common::MINUS_INF_64; }
  int64_t DoRoughMax() override { return common::PLUS_INF_64; }
  int64_t DoGetNoNulls([[maybe_unused]] const core::MIIterator &m, [[maybe_unused]] bool val_nulls_possible) override {
    return vc->GetNoNulls(m);
  }
  bool DoRoughNullsOnly() const override { return vc->RoughNullsOnly(); }
  bool DoNullsPossible([[maybe_unused]] bool val_nulls_possible) override { return vc->NullsPossible(); }
  int64_t DoGetSum(const core::MIIterator &m, bool &nonnegative) override { return vc->GetSum(m, nonnegative); }
  bool DoIsDistinct() override { return false; }
  int64_t DoGetApproxDistVals(bool incl_nulls, core::RoughMultiIndex *rough_mind) override {
    return vc->GetApproxDistVals(incl_nulls, rough_mind);
  }
  size_t DoMaxStringSize() override { return vc->MaxStringSize(); }  // maximal byte string length in column
  core::PackOntologicalStatus DoGetPackOntologicalStatus(const core::MIIterator &m) override {
    return vc->GetPackOntologicalStatus(m);
  }
  common::RSValue DoRoughCheck(const core::MIIterator &m, core::Descriptor &d) override { return vc->RoughCheck(m, d); }
  void DoEvaluatePack(core::MIUpdatingIterator &mit, core::Descriptor &d) override { return vc->EvaluatePack(mit, d); }
  common::ErrorCode DoEvaluateOnIndex(core::MIUpdatingIterator &mit, core::Descriptor &d, int64_t limit) override {
    return vc->EvaluateOnIndex(mit, d, limit);
  }
  bool full_const;
  VirtualColumn *vc;
};

//////////////////////////////////////////////////////
class String2NumCastColumn : public TypeCastColumn {
 public:
  String2NumCastColumn(VirtualColumn *from, core::ColumnType const &to);
  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override;
  virtual bool IsDistinctInTable() override { return false; }  // cast may make distinct strings equal
 protected:
  int64_t DoGetValueInt64(const core::MIIterator &mit) override;
  types::RCValueObject DoGetValue(const core::MIIterator &, bool lookup_to_num = true) override;
  int64_t DoGetMinInt64(const core::MIIterator &m) override;
  int64_t DoGetMaxInt64(const core::MIIterator &m) override;
  double DoGetValueDouble(const core::MIIterator &mit) override;

 private:
  mutable int64_t val;
  mutable types::RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class String2DateTimeCastColumn : public TypeCastColumn {
 public:
  String2DateTimeCastColumn(VirtualColumn *from, core::ColumnType const &to);
  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override;
  bool IsDistinctInTable() override { return false; }  // cast may make distinct strings equal
 protected:
  int64_t DoGetValueInt64(const core::MIIterator &mit) override;
  types::RCValueObject DoGetValue(const core::MIIterator &, bool lookup_to_num = true) override;
  void DoGetValueString(types::BString &s, const core::MIIterator &m) override;
  int64_t DoGetMinInt64(const core::MIIterator &m) override;
  int64_t DoGetMaxInt64(const core::MIIterator &m) override;

 private:
  int64_t val;
  mutable types::RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class Num2DateTimeCastColumn : public String2DateTimeCastColumn {
 public:
  Num2DateTimeCastColumn(VirtualColumn *from, core::ColumnType const &to);

 protected:
  int64_t DoGetValueInt64(const core::MIIterator &mit) override;
  types::RCValueObject DoGetValue(const core::MIIterator &, bool lookup_to_num = true) override;

 private:
  int64_t val;
  mutable types::RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class DateTime2VarcharCastColumn : public TypeCastColumn {
 public:
  DateTime2VarcharCastColumn(VirtualColumn *from, core::ColumnType const &to);
  void GetNotNullValueString(types::BString &s, const core::MIIterator &m) override {
    s = DoGetValue(m).ToBString();
    return;
  }

 protected:
  types::RCValueObject DoGetValue(const core::MIIterator &, bool lookup_to_num = true) override;

 private:
  mutable types::RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class Num2VarcharCastColumn : public TypeCastColumn {
 public:
  Num2VarcharCastColumn(VirtualColumn *from, core::ColumnType const &to);
  void GetNotNullValueString(types::BString &s, const core::MIIterator &m) override { DoGetValueString(s, m); }

 protected:
  types::RCValueObject DoGetValue(const core::MIIterator &, bool lookup_to_num = true) override;
  void DoGetValueString(types::BString &s, const core::MIIterator &m) override;

 private:
  mutable types::RCValueObject rcv;
};

//////////////////////////////////////////////////////
class DateTime2NumCastColumn : public TypeCastColumn {
 public:
  DateTime2NumCastColumn(VirtualColumn *from, core::ColumnType const &to);
  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override;

 protected:
  int64_t DoGetValueInt64(const core::MIIterator &mit) override;
  types::RCValueObject DoGetValue(const core::MIIterator &, bool lookup_to_num = true) override;
  double DoGetValueDouble(const core::MIIterator &mit) override;

 private:
  mutable int64_t val;
  mutable types::RCValueObject rcv;
};

//////////////////////////////////////////////////////
class TimeZoneConversionCastColumn : public TypeCastColumn {
 public:
  TimeZoneConversionCastColumn(VirtualColumn *from);
  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override;

 protected:
  int64_t DoGetValueInt64(const core::MIIterator &mit) override;
  types::RCValueObject DoGetValue(const core::MIIterator &, bool lookup_to_num = true) override;
  double DoGetValueDouble(const core::MIIterator &m) override;
  void DoGetValueString(types::BString &s, const core::MIIterator &m) override;

 private:
  mutable int64_t val;
  mutable types::RCValueObject rcv;
};

class StringCastColumn : public TypeCastColumn {
 public:
  StringCastColumn(VirtualColumn *from, core::ColumnType const &to) : TypeCastColumn(from, to) {}

 protected:
  void DoGetValueString(types::BString &s, const core::MIIterator &m) override { return vc->GetValueString(s, m); }
  types::RCValueObject DoGetValue(const core::MIIterator &, bool lookup_to_num = true) override;
  size_t DoMaxStringSize() override { return ct.GetPrecision(); }
};
}  // namespace vcolumn
}  // namespace stonedb

#endif  // STONEDB_VC_TYPE_CAST_COLUMN_H_
