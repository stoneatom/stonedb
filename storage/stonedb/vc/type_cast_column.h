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
  virtual ~TypeCastColumn() {}

  TypeCastColumn& operator = (const TypeCastColumn&) = delete;

  const std::vector<VarMap> &GetVarMap() const { return vc_->GetVarMap(); }

  void LockSourcePacks(const core::MIIterator &mit) override { vc_->LockSourcePacks(mit); }
  void LockSourcePacks(const core::MIIterator &mit, int th_no);
  void UnlockSourcePacks() override { vc_->UnlockSourcePacks(); }

  void MarkUsedDims(core::DimensionVector &dims_usage) override { vc_->MarkUsedDims(dims_usage); }
  std::set<int> GetDimensions() override { return vc_->GetDimensions(); }
  int GetDim() override { return vc_->GetDim(); }

  int64_t NoTuples() override { return vc_->NoTuples(); }

  bool IsConstExpression(core::MysqlExpression *expr, int temp_table_alias, const std::vector<int> *aliases) {
    return vc_->IsConstExpression(expr, temp_table_alias, aliases);
  }

  void RequestEval(const core::MIIterator &mit, const int tta) override { vc_->RequestEval(mit, tta); }
  bool IsConst() const override { return vc_->IsConst(); }

  void GetNotNullValueString(types::BString &s, const core::MIIterator &m) override {
    return vc_->GetNotNullValueString(s, m);
  }

  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override { return vc_->GetNotNullValueInt64(mit); }
  std::vector<VarMap> &GetVarMap() override { return vc_->GetVarMap(); }
  const core::MysqlExpression::sdbfields_cache_t &GetSDBItems() const override { return vc_->GetSDBItems(); }

  bool IsDistinctInTable() override { return vc_->IsDistinctInTable(); }  // sometimes overriden in subclasses
  bool IsDeterministic() override { return vc_->IsDeterministic(); }
  bool IsParameterized() const override { return vc_->IsParameterized(); }
  bool IsTypeCastColumn() const override { return true; }

  bool CanCopy() const override { return vc_->CanCopy(); }
  bool IsThreadSafe() override { return vc_->IsThreadSafe(); }
  std::vector<VirtualColumn *> GetChildren() const override { return std::vector<VirtualColumn *>(1, vc_); }

 protected:
  int64_t GetValueInt64Impl(const core::MIIterator &mit) override { return vc_->GetValueInt64(mit); }
  bool IsNullImpl(const core::MIIterator &mit) override { return vc_->IsNull(mit); }
  void GetValueStringImpl(types::BString &s, const core::MIIterator &m) override { return vc_->GetValueString(s, m); }
  double GetValueDoubleImpl(const core::MIIterator &m) override { return vc_->GetValueDouble(m); }
  types::RCValueObject GetValueImpl(const core::MIIterator &m, bool lookup_to_num) override {
    return vc_->GetValue(m, lookup_to_num);
  }

  int64_t GetMinInt64Impl([[maybe_unused]] const core::MIIterator &m) override { return common::MINUS_INF_64; }
  int64_t GetMaxInt64Impl([[maybe_unused]] const core::MIIterator &m) override { return common::PLUS_INF_64; }

  types::BString GetMinStringImpl([[maybe_unused]] const core::MIIterator &m) override { return types::BString(); }
  types::BString GetMaxStringImpl([[maybe_unused]] const core::MIIterator &m) override { return types::BString(); }

  int64_t RoughMinImpl() override { return common::MINUS_INF_64; }
  int64_t RoughMaxImpl() override { return common::PLUS_INF_64; }

  int64_t GetNumOfNullsImpl([[maybe_unused]] const core::MIIterator &m,
                            [[maybe_unused]] bool val_nulls_possible) override {
    return vc_->GetNumOfNulls(m);
  }

  bool IsRoughNullsOnlyImpl() const override { return vc_->IsRoughNullsOnly(); }
  bool IsNullsPossibleImpl([[maybe_unused]] bool val_nulls_possible) override { return vc_->IsNullsPossible(); }
  int64_t GetSumImpl(const core::MIIterator &m, bool &nonnegative) override { return vc_->GetSum(m, nonnegative); }
  bool IsDistinctImpl() override { return false; }

  int64_t GetApproxDistValsImpl(bool incl_nulls, core::RoughMultiIndex *rough_mind) override {
    return vc_->GetApproxDistVals(incl_nulls, rough_mind);
  }

  size_t MaxStringSizeImpl() override { return vc_->MaxStringSize(); }  // maximal byte string length in column
  core::PackOntologicalStatus GetPackOntologicalStatusImpl(const core::MIIterator &m) override {
    return vc_->GetPackOntologicalStatus(m);
  }

  common::RSValue RoughCheckImpl(const core::MIIterator &m, core::Descriptor &d) override {
    return vc_->RoughCheck(m, d);
  }

  void EvaluatePackImpl(core::MIUpdatingIterator &mit, core::Descriptor &d) override {
    return vc_->EvaluatePack(mit, d);
  }

  common::ErrorCode EvaluateOnIndexImpl(core::MIUpdatingIterator &mit, core::Descriptor &d, int64_t limit) override {
    return vc_->EvaluateOnIndex(mit, d, limit);
  }

  bool is_full_const_;
  VirtualColumn *vc_;
};

//////////////////////////////////////////////////////
class String2NumCastColumn : public TypeCastColumn {
 public:
  String2NumCastColumn(VirtualColumn *from, core::ColumnType const &to);
  ~String2NumCastColumn() final {}  

  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override;
  virtual bool IsDistinctInTable() override { return false; }  // cast may make distinct strings equal
 protected:
  int64_t GetValueInt64Impl(const core::MIIterator &mit) override;
  types::RCValueObject GetValueImpl(const core::MIIterator &, bool lookup_to_num = true) override;
  int64_t GetMinInt64Impl(const core::MIIterator &m) override;
  int64_t GetMaxInt64Impl(const core::MIIterator &m) override;
  double GetValueDoubleImpl(const core::MIIterator &mit) override;

 private:
  mutable int64_t value_;
  mutable types::RCValueObject rcv_;
};

//////////////////////////////////////////////////////////
class String2DateTimeCastColumn : public TypeCastColumn {
 public:
  String2DateTimeCastColumn(VirtualColumn *from, core::ColumnType const &to);
  virtual ~String2DateTimeCastColumn() {}

  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override;
  bool IsDistinctInTable() override { return false; }  // cast may make distinct strings equal
 protected:
  int64_t GetValueInt64Impl(const core::MIIterator &mit) override;
  types::RCValueObject GetValueImpl(const core::MIIterator &, bool lookup_to_num = true) override;
  void GetValueStringImpl(types::BString &s, const core::MIIterator &m) override;
  int64_t GetMinInt64Impl(const core::MIIterator &m) override;
  int64_t GetMaxInt64Impl(const core::MIIterator &m) override;

 private:
  int64_t value_;
  mutable types::RCValueObject rcv_;
};

//////////////////////////////////////////////////////////
class Num2DateTimeCastColumn : public String2DateTimeCastColumn {
 public:
  Num2DateTimeCastColumn(VirtualColumn *from, core::ColumnType const &to);
  ~Num2DateTimeCastColumn() final {}
 
 protected:
  int64_t GetValueInt64Impl(const core::MIIterator &mit) override;
  types::RCValueObject GetValueImpl(const core::MIIterator &, bool lookup_to_num = true) override;

 private:
  int64_t value_;
  mutable types::RCValueObject rcv_;
};

//////////////////////////////////////////////////////////
class DateTime2VarcharCastColumn : public TypeCastColumn {
 public:
  DateTime2VarcharCastColumn(VirtualColumn *from, core::ColumnType const &to);
  ~DateTime2VarcharCastColumn() final {}   

  void GetNotNullValueString(types::BString &s, const core::MIIterator &m) override {
    s = GetValueImpl(m).ToBString();
    return;
  }

 protected:
  types::RCValueObject GetValueImpl(const core::MIIterator &, bool lookup_to_num = true) override;

 private:
  mutable types::RCValueObject rcv_;
};

//////////////////////////////////////////////////////////
class Num2VarcharCastColumn : public TypeCastColumn {
 public:
  Num2VarcharCastColumn(VirtualColumn *from, core::ColumnType const &to);
  ~Num2VarcharCastColumn() final {}

  void GetNotNullValueString(types::BString &s, const core::MIIterator &m) override { GetValueStringImpl(s, m); }

 protected:
  types::RCValueObject GetValueImpl(const core::MIIterator &, bool lookup_to_num = true) override;
  void GetValueStringImpl(types::BString &s, const core::MIIterator &m) override;

 private:
  mutable types::RCValueObject rcv_;
};

//////////////////////////////////////////////////////
class DateTime2NumCastColumn : public TypeCastColumn {
 public:
  DateTime2NumCastColumn(VirtualColumn *from, core::ColumnType const &to);
  ~DateTime2NumCastColumn() final {}

  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override;

 protected:
  int64_t GetValueInt64Impl(const core::MIIterator &mit) override;
  types::RCValueObject GetValueImpl(const core::MIIterator &, bool lookup_to_num = true) override;
  double GetValueDoubleImpl(const core::MIIterator &mit) override;

 private:
  mutable int64_t value_;
  mutable types::RCValueObject rcv_;
};

//////////////////////////////////////////////////////
class TimeZoneConversionCastColumn : public TypeCastColumn {
 public:
  TimeZoneConversionCastColumn(VirtualColumn *from);
  ~TimeZoneConversionCastColumn() final {}

  int64_t GetNotNullValueInt64(const core::MIIterator &mit) override;

 protected:
  int64_t GetValueInt64Impl(const core::MIIterator &mit) override;
  types::RCValueObject GetValueImpl(const core::MIIterator &, bool lookup_to_num = true) override;
  double GetValueDoubleImpl(const core::MIIterator &m) override;
  void GetValueStringImpl(types::BString &s, const core::MIIterator &m) override;

 private:
  mutable int64_t value_;
  mutable types::RCValueObject rcv_;
};

class StringCastColumn : public TypeCastColumn {
 public:
  StringCastColumn(VirtualColumn *from, core::ColumnType const &to) : TypeCastColumn(from, to) {}
  ~StringCastColumn() final {}

 protected:
  void GetValueStringImpl(types::BString &s, const core::MIIterator &m) override { return vc_->GetValueString(s, m); }
  types::RCValueObject GetValueImpl(const core::MIIterator &, bool lookup_to_num = true) override;
  size_t MaxStringSizeImpl() override { return ct.GetPrecision(); }
};

}  // namespace vcolumn
}  // namespace stonedb

#endif  // STONEDB_VC_TYPE_CAST_COLUMN_H_
