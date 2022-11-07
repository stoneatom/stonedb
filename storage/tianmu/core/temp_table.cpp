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

#include <algorithm>

#include "core/aggregation_algorithm.h"
#include "core/cached_buffer.h"
#include "core/column_bin_encoder.h"
#include "core/condition_encoder.h"
#include "core/engine.h"
#include "core/group_distinct_table.h"
#include "core/mysql_expression.h"
#include "core/parameterized_filter.h"
#include "core/query.h"
#include "core/rsi_cmap.h"
#include "core/temp_table.h"
#include "core/transaction.h"
#include "core/value_set.h"
#include "system/fet.h"
#include "vc/const_column.h"
#include "vc/const_expr_column.h"
#include "vc/expr_column.h"
#include "vc/in_set_column.h"
#include "vc/single_column.h"
#include "vc/subselect_column.h"
#include "vc/type_cast_column.h"

namespace Tianmu {
namespace core {

template <class T>
class AttrBuffer : public CachedBuffer<T> {
 public:
  explicit AttrBuffer(uint page_size_, uint elem_size = 0, Transaction *conn = nullptr)
      : CachedBuffer<T>(page_size_, elem_size, conn) {}
  ~AttrBuffer() = default;

  void GetString(types::BString &s, int64_t idx) { CachedBuffer<T>::Get(s, idx); }
  // Warning: read only operator!!!
  T &operator[](int64_t idx) { return CachedBuffer<T>::Get(idx); }
  void Set(int64_t idx, T value) { CachedBuffer<T>::Set(idx, value); }
};

template class AttrBuffer<char>;
template class AttrBuffer<short>;
template class AttrBuffer<int>;
template class AttrBuffer<int64_t>;
template class AttrBuffer<double>;
template class AttrBuffer<types::BString>;

TempTable::Attr::Attr(const Attr &a) : PhysicalColumn(a) {
  oper_type_ = a.oper_type_;
  is_distinct_ = a.is_distinct_;
  num_of_materialized_ = a.num_of_materialized_;
  term_ = a.term_;

  if (term_.vc)
    term_.vc->ResetLocalStatistics();

  dimension_ = a.dimension_;
  // DEBUG_ASSERT(a.buffer == nullptr); // otherwise we cannot copy Attr !
  buffer_ = nullptr;
  num_of_obj_ = a.num_of_obj_;
  num_of_power_ = a.num_of_power_;

  if (a.alias_) {
    alias_ = new char[std::strlen(a.alias_) + 1];
    std::strcpy(alias_, a.alias_);
  } else
    alias_ = nullptr;

  page_size_ = a.page_size_;
  orig_precision_ = a.orig_precision_;
  not_complete_ = a.not_complete_;
  si_ = a.si_;
}

TempTable::Attr::Attr(CQTerm t, common::ColOperation m, uint32_t power, bool dis, char *a, int dim,
                      common::ColumnType type, uint scale, uint no_digits, bool notnull, DTCollation collation, SI *si1)
    : oper_type_(m), is_distinct_(dis), term_(t), dimension_(dim), not_complete_(true) {
  ct.Initialize(type, notnull, common::PackFmt::DEFAULT, no_digits, scale, collation);

  orig_precision_ = no_digits;
  buffer_ = nullptr;
  num_of_obj_ = 0;
  num_of_power_ = power;
  num_of_materialized_ = 0;

  if (a) {
    alias_ = new char[std::strlen(a) + 1];
    std::strcpy(alias_, a);
  } else
    alias_ = nullptr;

  if (m == common::ColOperation::GROUP_CONCAT)
    si_ = (*si1);
  else
    si_.order = ORDER_NOT_RELEVANT;

  page_size_ = 1;
}

TempTable::Attr::~Attr() {
  DeleteBuffer();
  if (alias_) {
    delete[] alias_;
    alias_ = nullptr;
  }
}

TempTable::Attr &TempTable::Attr::operator=(const TempTable::Attr &a) {
  oper_type_ = a.oper_type_;
  is_distinct_ = a.is_distinct_;
  num_of_materialized_ = a.num_of_materialized_;
  term_ = a.term_;

  if (term_.vc)
    term_.vc->ResetLocalStatistics();

  dimension_ = a.dimension_;
  // DEBUG_ASSERT(a.buffer == nullptr); // otherwise we cannot copy Attr !
  buffer_ = nullptr;
  num_of_obj_ = a.num_of_obj_;
  num_of_power_ = a.num_of_power_;
  if (alias_) {
    delete[] alias_;
    alias_ = nullptr;
  }

  if (a.alias_) {
    alias_ = new char[std::strlen(a.alias_) + 1];
    std::strcpy(alias_, a.alias_);
  } else
    alias_ = nullptr;

  page_size_ = a.page_size_;
  orig_precision_ = a.orig_precision_;
  not_complete_ = a.not_complete_;
  return (*this);
}

int TempTable::Attr::operator==(const TempTable::Attr &sec) {
  return oper_type_ == sec.oper_type_ && is_distinct_ == sec.is_distinct_ && term_ == sec.term_ &&
         Type() == sec.Type() && dimension_ == sec.dimension_;
}

void TempTable::Attr::CreateBuffer(uint64_t size, Transaction *conn, bool not_c) {
  // do not create larger buffer than size
  not_complete_ = not_c;
  if (size < page_size_)
    page_size_ = (uint)size;

  num_of_obj_ = size;
  switch (TypeName()) {
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      if (!buffer_)
        buffer_ = new AttrBuffer<int>(page_size_, sizeof(int), conn);
      break;
    case common::ColumnType::BYTEINT:
      if (!buffer_)
        buffer_ = new AttrBuffer<char>(page_size_, sizeof(char), conn);
      break;
    case common::ColumnType::SMALLINT:
      if (!buffer_)
        buffer_ = new AttrBuffer<short>(page_size_, sizeof(short), conn);
      break;
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
      DeleteBuffer();
      buffer_ = new AttrBuffer<types::BString>(page_size_, Type().GetInternalSize(), conn);
      break;
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
      if (!buffer_)
        buffer_ = new AttrBuffer<int64_t>(page_size_, sizeof(int64_t), conn);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      if (!buffer_)
        buffer_ = new AttrBuffer<double>(page_size_, sizeof(double), conn);
      break;
    default:
      break;
  }
}

void TempTable::Attr::FillValue(const MIIterator &mii, size_t idx) {
  types::BString vals;
  switch (TypeName()) {
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
      term_.vc->GetValueString(vals, mii);
      SetValueString(idx, vals);
      break;
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
      if (!term_.vc->IsNull(mii))
        term_.vc->GetNotNullValueString(vals, mii);
      else
        vals = types::BString();
      SetValueString(idx, vals);
      break;
    default:
      SetValueInt64(idx, term_.vc->GetValueInt64(mii));
      break;
  }
}

size_t TempTable::Attr::FillValues(MIIterator &mii, size_t start, size_t count) {
  size_t n = 0;
  bool first_row_for_vc = true;
  while (mii.IsValid() && n < count) {
    if (mii.PackrowStarted() || first_row_for_vc) {
      term_.vc->LockSourcePacks(mii);
      first_row_for_vc = false;
    }

    FillValue(mii, start + n);
    ++mii;
    ++n;
  };

  term_.vc->UnlockSourcePacks();
  return n;
}

void TempTable::Attr::DeleteBuffer() {
  switch (TypeName()) {
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      delete (AttrBuffer<int> *)(buffer_);
      break;
    case common::ColumnType::BYTEINT:
      delete (AttrBuffer<char> *)(buffer_);
      break;
    case common::ColumnType::SMALLINT:
      delete (AttrBuffer<short> *)(buffer_);
      break;
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
      delete (AttrBuffer<types::BString> *)(buffer_);
      break;
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
      delete (AttrBuffer<int64_t> *)(buffer_);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      delete (AttrBuffer<double> *)(buffer_);
      break;
    default:
      break;
  }

  buffer_ = nullptr;
  num_of_obj_ = 0;
}

void TempTable::Attr::SetValueInt64(int64_t obj, int64_t val) {
  num_of_materialized_ = obj + 1;
  num_of_obj_ = obj >= num_of_obj_ ? obj + 1 : num_of_obj_;

  switch (TypeName()) {
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:  // 64-bit
      ((AttrBuffer<int64_t> *)buffer_)->Set(obj, val);
      break;
    case common::ColumnType::INT:        // 32-bit
    case common::ColumnType::MEDIUMINT:  // 24-bit
      if (val == common::NULL_VALUE_64)
        ((AttrBuffer<int> *)buffer_)->Set(obj, common::NULL_VALUE_32);
      else if (val == common::PLUS_INF_64)
        ((AttrBuffer<int> *)buffer_)->Set(obj, std::numeric_limits<int>::max());
      else if (val == common::MINUS_INF_64)
        ((AttrBuffer<int> *)buffer_)->Set(obj, TIANMU_INT_MIN);
      else
        ((AttrBuffer<int> *)buffer_)->Set(obj, (int)val);
      break;
    case common::ColumnType::SMALLINT:  // 16-bit
      if (val == common::NULL_VALUE_64)
        ((AttrBuffer<short> *)buffer_)->Set(obj, common::NULL_VALUE_SH);
      else if (val == common::PLUS_INF_64)
        ((AttrBuffer<short> *)buffer_)->Set(obj, TIANMU_SMALLINT_MAX);
      else if (val == common::MINUS_INF_64)
        ((AttrBuffer<short> *)buffer_)->Set(obj, TIANMU_SMALLINT_MIN);
      else
        ((AttrBuffer<short> *)buffer_)->Set(obj, (short)val);
      break;
    case common::ColumnType::BYTEINT:  // 8-bit
      if (val == common::NULL_VALUE_64)
        ((AttrBuffer<char> *)buffer_)->Set(obj, common::NULL_VALUE_C);
      else if (val == common::PLUS_INF_64)
        ((AttrBuffer<char> *)buffer_)->Set(obj, TIANMU_TINYINT_MAX);
      else if (val == common::MINUS_INF_64)
        ((AttrBuffer<char> *)buffer_)->Set(obj, TIANMU_TINYINT_MIN);
      else
        ((AttrBuffer<char> *)buffer_)->Set(obj, (char)val);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      if (val == common::NULL_VALUE_64)
        ((AttrBuffer<double> *)buffer_)->Set(obj, NULL_VALUE_D);
      else if (val == common::PLUS_INF_64)
        ((AttrBuffer<double> *)buffer_)->Set(obj, common::PLUS_INF_DBL);
      else if (val == common::MINUS_INF_64)
        ((AttrBuffer<double> *)buffer_)->Set(obj, common::MINUS_INF_DBL);
      else
        ((AttrBuffer<double> *)buffer_)->Set(obj, *(double *)&val);
      break;
    default:
      DEBUG_ASSERT(0);
      break;
  }
}

void TempTable::Attr::InvalidateRow([[maybe_unused]] int64_t obj) {
  DEBUG_ASSERT(obj + 1 == num_of_materialized_);
  num_of_obj_--;
  num_of_materialized_--;
}

void TempTable::Attr::SetNull(int64_t obj) {
  num_of_materialized_ = obj + 1;
  num_of_obj_ = obj >= num_of_obj_ ? obj + 1 : num_of_obj_;
  switch (TypeName()) {
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
      ((AttrBuffer<int64_t> *)buffer_)->Set(obj, common::NULL_VALUE_64);
      break;
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      ((AttrBuffer<int> *)buffer_)->Set(obj, common::NULL_VALUE_32);
      break;
    case common::ColumnType::SMALLINT:
      ((AttrBuffer<short> *)buffer_)->Set(obj, common::NULL_VALUE_SH);
      break;
    case common::ColumnType::BYTEINT:
      ((AttrBuffer<char> *)buffer_)->Set(obj, common::NULL_VALUE_C);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      ((AttrBuffer<double> *)buffer_)->Set(obj, NULL_VALUE_D);
      break;
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
      ((AttrBuffer<types::BString> *)buffer_)->Set(obj, types::BString());
      break;
    default:
      DEBUG_ASSERT(0);
      break;
  }
}

void TempTable::Attr::SetMinusInf(int64_t obj) { SetValueInt64(obj, common::MINUS_INF_64); }

void TempTable::Attr::SetPlusInf(int64_t obj) { SetValueInt64(obj, common::PLUS_INF_64); }

void TempTable::Attr::SetValueString(int64_t obj, const types::BString &val) {
  num_of_materialized_ = obj + 1;
  num_of_obj_ = obj >= num_of_obj_ ? obj + 1 : num_of_obj_;

  int64_t val64 = 0;
  double valD = 0.0;

  switch (TypeName()) {
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      ((AttrBuffer<int> *)buffer_)->Set(obj, std::atoi(val.GetDataBytesPointer()));
      break;
    case common::ColumnType::BYTEINT:
      ((AttrBuffer<char> *)buffer_)->Set(obj, val.GetDataBytesPointer()[0]);
      break;
    case common::ColumnType::SMALLINT:
      ((AttrBuffer<short> *)buffer_)->Set(obj, (short)std::atoi(val.GetDataBytesPointer()));
      break;
    case common::ColumnType::BIGINT:
      ((AttrBuffer<int64_t> *)buffer_)->Set(obj, std::strtoll(val.GetDataBytesPointer(), nullptr, 10));
      break;
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::LONGTEXT:
      ((AttrBuffer<types::BString> *)buffer_)->Set(obj, val);
      break;
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
      val64 = *(int64_t *)(val.GetDataBytesPointer());
      ((AttrBuffer<int64_t> *)buffer_)->Set(obj, val64);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      if (!val.IsNullOrEmpty())
        valD = *(double *)(val.GetDataBytesPointer());
      else
        valD = NULL_VALUE_D;
      ((AttrBuffer<double> *)buffer_)->Set(obj, valD);
      break;
    default:
      break;
  }
}

types::RCValueObject TempTable::Attr::GetValue(int64_t obj, [[maybe_unused]] bool lookup_to_num) {
  if (obj == common::NULL_VALUE_64)
    return types::RCValueObject();
  types::RCValueObject ret;
  if (ATI::IsStringType(TypeName())) {
    types::BString s;
    GetValueString(s, obj);
    ret = s;
  } else if (ATI::IsIntegerType(TypeName()))
    ret = types::RCNum(GetValueInt64(obj), 0, false, common::ColumnType::NUM);
  else if (ATI::IsDateTimeType(TypeName()))
    ret = types::RCDateTime(this->GetValueInt64(obj), TypeName() /*, precision*/);
  else if (ATI::IsRealType(TypeName()))
    ret = types::RCNum(this->GetValueInt64(obj), 0, true);
  else if (TypeName() == common::ColumnType::NUM)
    ret = types::RCNum((int64_t)GetValueInt64(obj), Type().GetScale());
  return ret;
}

void TempTable::Attr::GetValueString(types::BString &s, int64_t obj) {
  if (obj == common::NULL_VALUE_64) {
    s = types::BString();
    return;
  }
  double *d_p = nullptr;
  switch (TypeName()) {
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::LONGTEXT:
      (*(AttrBuffer<types::BString> *)buffer_).GetString(s, obj);
      break;
    case common::ColumnType::BYTEINT: {
      types::RCNum rcn((int64_t)(*(AttrBuffer<char> *)buffer_)[obj], -1, false, TypeName());
      s = rcn.ToBString();
      break;
    }
    case common::ColumnType::SMALLINT: {
      types::RCNum rcn((int64_t)(*(AttrBuffer<short> *)buffer_)[obj], -1, false, TypeName());
      s = rcn.ToBString();
      break;
    }
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT: {
      types::RCNum rcn((int)(*(AttrBuffer<int> *)buffer_)[obj], -1, false, TypeName());
      s = rcn.ToBString();
      break;
    }
    case common::ColumnType::BIGINT: {
      types::RCNum rcn((int64_t)(*(AttrBuffer<int64_t> *)buffer_)[obj], -1, false, TypeName());
      s = rcn.ToBString();
      break;
    }
    case common::ColumnType::NUM: {
      types::RCNum rcn((*(AttrBuffer<int64_t> *)buffer_)[obj], Type().GetScale());
      s = rcn.ToBString();
      break;
    }
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP: {
      types::RCDateTime rcdt((*(AttrBuffer<int64_t> *)buffer_)[obj], TypeName());
      if (TypeName() == common::ColumnType::TIMESTAMP) {
        types::RCDateTime::AdjustTimezone(rcdt);
      }
      s = rcdt.ToBString();
      break;
    }
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT: {
      d_p = &(*(AttrBuffer<double> *)buffer_)[obj];
      types::RCNum rcn(*(int64_t *)d_p, 0, true, TypeName());
      s = rcn.ToBString();
      break;
    }
    default:
      break;
  }
}

int64_t TempTable::Attr::GetSum(int pack, bool &nonnegative) {
  DEBUG_ASSERT(ATI::IsNumericType(ct.GetTypeName()));
  int64_t start = pack * (1 << num_of_power_);
  int64_t stop = (1 << num_of_power_);
  stop = stop > num_of_obj_ ? num_of_obj_ : stop;

  if (not_complete_ || num_of_materialized_ < stop)
    return common::NULL_VALUE_64;

  int64_t sum{0}, val{0};
  double sum_d{0.0};
  bool not_null_exists{false};

  nonnegative = true;

  for (int64_t i = start; i < stop; i++) {
    if (!IsNull(i)) {
      not_null_exists = true;
      val = GetNotNullValueInt64(i);
      nonnegative = nonnegative && (val >= 0);

      if (ATI::IsRealType(ct.GetTypeName())) {
        sum_d += *(double *)&val;
      } else
        sum += val;
    }
  }

  if (ATI::IsRealType(ct.GetTypeName()))
    sum = *(int64_t *)&sum_d;

  return not_null_exists ? sum : common::NULL_VALUE_64;
}

int64_t TempTable::Attr::GetNumOfNulls(int pack) {
  if (not_complete_)
    return common::NULL_VALUE_64;

  int64_t start{0}, stop{0};
  if (pack == -1) {
    start = 0;
    stop = num_of_obj_;
  } else {
    start = pack * (1 << num_of_power_);
    stop = (pack + 1) * (1 << num_of_power_);
    stop = stop > num_of_obj_ ? num_of_obj_ : stop;
  }

  if (num_of_materialized_ < stop)
    return common::NULL_VALUE_64;

  int64_t no_nulls{0};
  for (int64_t i = start; i < stop; i++) {
    if (IsNull(i))
      no_nulls++;
  }

  return no_nulls;
}

types::BString TempTable::Attr::GetMinString([[maybe_unused]] int pack) {
  return types::BString();  // it should not touch data
}

types::BString TempTable::Attr::GetMaxString([[maybe_unused]] int pack) {
  return types::BString();  // it should not touch data
}

int64_t TempTable::Attr::GetMaxInt64(int pack) {
  if (not_complete_)
    return common::PLUS_INF_64;

  int64_t start = (1 << num_of_power_);
  int64_t stop = (pack + 1) * (1 << num_of_power_);
  stop = stop > num_of_obj_ ? num_of_obj_ : stop;

  if (num_of_materialized_ < stop)
    return common::NULL_VALUE_64;

  int64_t val, max = common::TIANMU_BIGINT_MIN;
  for (int64_t i = start; i < stop; i++) {
    if (!IsNull(i)) {
      val = GetNotNullValueInt64(i);
      if ((ATI::IsRealType(ct.GetTypeName()) &&
           (max == common::TIANMU_BIGINT_MIN || *(double *)&val > *(double *)&max)) ||
          (!ATI::IsRealType(ct.GetTypeName()) && val > max))
        max = val;
    }
  }

  return (max == common::TIANMU_BIGINT_MIN ? common::PLUS_INF_64 : max);
}

int64_t TempTable::Attr::GetMinInt64(int pack) {
  if (not_complete_)
    return common::MINUS_INF_64;

  int64_t start = pack * (1 << num_of_power_);
  int64_t stop = (pack + 1) * (1 << num_of_power_);
  stop = stop > num_of_obj_ ? num_of_obj_ : stop;

  if (num_of_materialized_ < stop)
    return common::NULL_VALUE_64;

  int64_t val, min = common::TIANMU_BIGINT_MAX;
  for (int64_t i = start; i < stop; i++) {
    if (!IsNull(i)) {
      val = GetNotNullValueInt64(i);
      if ((ATI::IsRealType(ct.GetTypeName()) &&
           (min == common::TIANMU_BIGINT_MAX || *(double *)&val < *(double *)&min)) ||
          (!ATI::IsRealType(ct.GetTypeName()) && val < min))
        min = val;
    }
  }

  return (min == common::TIANMU_BIGINT_MAX ? common::MINUS_INF_64 : min);
}

int64_t TempTable::Attr::GetValueInt64(int64_t obj) const {
  int64_t res = common::NULL_VALUE_64;

  if (obj == common::NULL_VALUE_64)
    return res;

  switch (TypeName()) {
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
      if (!IsNull(obj))
        res = (*(AttrBuffer<int64_t> *)buffer_)[obj];
      break;
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      if (!IsNull(obj))
        res = (*(AttrBuffer<int> *)buffer_)[obj];
      break;
    case common::ColumnType::SMALLINT:
      if (!IsNull(obj))
        res = (*(AttrBuffer<short> *)buffer_)[obj];
      break;
    case common::ColumnType::BYTEINT:
      if (!IsNull(obj))
        res = (*(AttrBuffer<char> *)buffer_)[obj];
      break;
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
      DEBUG_ASSERT(0);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      if (!IsNull(obj))
        res = *(int64_t *)&(*(AttrBuffer<double> *)buffer_)[obj];
      else
        res = *(reinterpret_cast<const int64_t *>(&NULL_VALUE_D));
      break;
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
      DEBUG_ASSERT(0);
      break;
    default:
      break;
  }

  return res;
}

int64_t TempTable::Attr::GetNotNullValueInt64(int64_t obj) const {
  int64_t res = common::NULL_VALUE_64;
  switch (TypeName()) {
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      res = (*(AttrBuffer<int> *)buffer_)[obj];
      break;
    case common::ColumnType::BYTEINT:
      res = (*(AttrBuffer<char> *)buffer_)[obj];
      break;
    case common::ColumnType::SMALLINT:
      res = (*(AttrBuffer<short> *)buffer_)[obj];
      break;
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
      res = (*(AttrBuffer<int64_t> *)buffer_)[obj];
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      res = *(int64_t *)&(*(AttrBuffer<double> *)buffer_)[obj];
      break;
    default:
      DEBUG_ASSERT(0);
      break;
  }
  return res;
}

bool TempTable::Attr::IsNull(const int64_t obj) const {
  if (obj == common::NULL_VALUE_64)
    return true;
  bool res = false;
  switch (TypeName()) {
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      res = (*(AttrBuffer<int> *)buffer_)[obj] == common::NULL_VALUE_32;
      break;
    case common::ColumnType::BYTEINT:
      res = (*(AttrBuffer<char> *)buffer_)[obj] == common::NULL_VALUE_C;
      break;
    case common::ColumnType::SMALLINT:
      res = (*(AttrBuffer<short> *)buffer_)[obj] == common::NULL_VALUE_SH;
      break;
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::LONGTEXT:
      res = (*(AttrBuffer<types::BString> *)buffer_)[obj].IsNull();
      break;
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
      res = (*(AttrBuffer<int64_t> *)buffer_)[obj] == common::NULL_VALUE_64;
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT: {
      common::double_int_t v((*(AttrBuffer<double> *)buffer_)[obj]);
      res = v.i == common::NULL_VALUE_64;
      break;
    }
    default:
      break;
  }
  return res;
}

void TempTable::Attr::ApplyFilter(MultiIndex &mind, int64_t offset, int64_t last_index) {
  DEBUG_ASSERT(mind.NumOfDimensions() == 1);

  if (mind.NumOfDimensions() != 1)
    throw common::NotImplementedException("MultiIndex has too many dimensions.");
  if (mind.ZeroTuples() || num_of_obj_ == 0 || offset >= mind.NumOfTuples()) {
    DeleteBuffer();
    return;
  }

  if (last_index > mind.NumOfTuples())
    last_index = mind.NumOfTuples();

  void *old_buffer = buffer_;
  buffer_ = nullptr;
  CreateBuffer(last_index - offset, mind.m_conn);

  MIIterator mit(&mind, mind.ValueOfPower());
  for (int64_t i = 0; i < offset; i++) ++mit;
  uint64_t idx = 0;
  for (int64_t i = 0; i < last_index - offset; i++, ++mit) {
    idx = mit[0];
    DEBUG_ASSERT(idx != static_cast<uint64_t>(common::NULL_VALUE_64));  // null object should never appear
                                                                        // in a materialized_ temp. table
    switch (TypeName()) {
      case common::ColumnType::INT:
      case common::ColumnType::MEDIUMINT:
        ((AttrBuffer<int> *)buffer_)->Set(i, (*(AttrBuffer<int> *)old_buffer)[idx]);
        break;
      case common::ColumnType::BYTEINT:
        ((AttrBuffer<char> *)buffer_)->Set(i, (*(AttrBuffer<char> *)old_buffer)[idx]);
        break;
      case common::ColumnType::SMALLINT:
        ((AttrBuffer<short> *)buffer_)->Set(i, (*(AttrBuffer<short> *)old_buffer)[idx]);
        break;
      case common::ColumnType::STRING:
      case common::ColumnType::VARCHAR:
      case common::ColumnType::BIN:
      case common::ColumnType::BYTE:
      case common::ColumnType::VARBYTE:
      case common::ColumnType::LONGTEXT:
        ((AttrBuffer<types::BString> *)buffer_)->Set(i, (*(AttrBuffer<types::BString> *)old_buffer)[idx]);
        break;
      case common::ColumnType::BIGINT:
      case common::ColumnType::NUM:
      case common::ColumnType::YEAR:
      case common::ColumnType::TIME:
      case common::ColumnType::DATE:
      case common::ColumnType::DATETIME:
      case common::ColumnType::TIMESTAMP:
        ((AttrBuffer<int64_t> *)buffer_)->Set(i, (*(AttrBuffer<int64_t> *)old_buffer)[idx]);
        break;
      case common::ColumnType::REAL:
      case common::ColumnType::FLOAT:
        ((AttrBuffer<double> *)buffer_)->Set(i, (*(AttrBuffer<double> *)old_buffer)[idx]);
        break;
      default:
        DEBUG_ASSERT(0);
        break;
    }
  }

  switch (TypeName()) {
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      delete (AttrBuffer<int> *)old_buffer;
      break;
    case common::ColumnType::BYTEINT:
      delete (AttrBuffer<char> *)old_buffer;
      break;
    case common::ColumnType::SMALLINT:
      delete (AttrBuffer<short> *)old_buffer;
      break;
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
      delete (AttrBuffer<types::BString> *)old_buffer;
      break;
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
      delete (AttrBuffer<int64_t> *)old_buffer;
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      delete (AttrBuffer<double> *)old_buffer;
      break;
    default:
      DEBUG_ASSERT(0);
      break;
  }
}

// provide the best upper approximation of number of diff. values (incl. null)
uint64_t TempTable::Attr::ApproxDistinctVals([[maybe_unused]] bool incl_nulls, Filter *f,
                                             [[maybe_unused]] common::RoughSetValue *rf,
                                             [[maybe_unused]] bool outer_nulls_possible) {
  // TODO: can it be done better?
  if (f)
    return f->NumOfOnes();
  return num_of_obj_;
}

size_t TempTable::Attr::MaxStringSize([[maybe_unused]] Filter *f)  // maximal byte std::string length in column
{
  uint res = ct.GetPrecision();

  if (ct.IsFixed())
    res = std::max(res, 21u);  // max. numeric size ("-aaa.bbb")
  else if (!ct.IsString())
    res = std::max(res, 23u);  // max. size of datetime/double etc.

  return res;
}

void TempTable::Attr::SetNewPageSize(uint new_page_size) {
  if (buffer_) {
    switch (TypeName()) {
      case common::ColumnType::INT:
      case common::ColumnType::MEDIUMINT:
        static_cast<CachedBuffer<int> *>(buffer_)->SetNewPageSize(new_page_size);
        break;
      case common::ColumnType::BYTEINT:
        static_cast<CachedBuffer<char> *>(buffer_)->SetNewPageSize(new_page_size);
        break;
      case common::ColumnType::SMALLINT:
        static_cast<CachedBuffer<short> *>(buffer_)->SetNewPageSize(new_page_size);
        break;
      case common::ColumnType::STRING:
      case common::ColumnType::VARCHAR:
      case common::ColumnType::BIN:
      case common::ColumnType::BYTE:
      case common::ColumnType::VARBYTE:
      case common::ColumnType::LONGTEXT:
        static_cast<CachedBuffer<types::BString> *>(buffer_)->SetNewPageSize(new_page_size);
        break;
      case common::ColumnType::BIGINT:
      case common::ColumnType::NUM:
      case common::ColumnType::YEAR:
      case common::ColumnType::TIME:
      case common::ColumnType::DATE:
      case common::ColumnType::DATETIME:
      case common::ColumnType::TIMESTAMP:
        static_cast<CachedBuffer<int64_t> *>(buffer_)->SetNewPageSize(new_page_size);
        break;
      case common::ColumnType::REAL:
      case common::ColumnType::FLOAT:
        static_cast<CachedBuffer<double> *>(buffer_)->SetNewPageSize(new_page_size);
        break;
      default:
        ASSERT(0, "Attr::SetNewPageSize: unrecognized type");
        break;
    }
  }

  page_size_ = new_page_size;
}

TempTable::TempTable(const TempTable &t, bool is_vc_owner_)
    : filter_(t.filter_), output_multi_index_(t.output_multi_index_), is_vc_owner_(is_vc_owner_), m_conn_(t.m_conn_) {
  num_of_obj_ = t.num_of_obj_;
  materialized_ = t.materialized_;
  aliases_ = t.aliases_;
  group_by_ = t.group_by_;
  num_of_columns_ = t.num_of_columns_;
  table_mode_ = t.table_mode_;

  virtual_columns_ = t.virtual_columns_;
  virtual_columns_for_having_ = t.virtual_columns_for_having_;

  having_conds_ = t.having_conds_;
  order_by_ = t.order_by_;

  src_tables_ = t.src_tables_;
  join_types_ = t.join_types_;

  has_temp_table_ = t.has_temp_table_;
  lazy_ = t.lazy_;

  force_full_materialize_ = t.force_full_materialize_;
  num_of_materialized_ = t.num_of_materialized_;
  num_of_global_virtual_columns_ = int(t.virtual_columns_.size());

  for (auto t_attr : t.attrs_) {
    attrs_.push_back(new Attr(*t_attr));
  }

  is_sent_ = t.is_sent_;
  mem_scale_ = t.mem_scale_;
  rough_is_empty_ = t.rough_is_empty_;
  p_power_ = t.p_power_;
  size_of_one_record_ = 0;
}

TempTable::~TempTable() {
  MEASURE_FET("TempTable::~TempTable(...)");
  // remove all temporary tables used by *this
  // but only if *this should manage those tables

  if (is_vc_owner_) {
    for (auto virtual_col : virtual_columns_) delete virtual_col;
  } else {
    TranslateBackVCs();

    for (uint i = num_of_global_virtual_columns_; i < virtual_columns_.size(); i++) delete virtual_columns_[i];
  }

  for (auto attr : attrs_) {
    delete attr;
  }
}

void TempTable::TranslateBackVCs() {
  for (int i = 0; i < num_of_global_virtual_columns_; i++)
    if (virtual_columns_[i] && static_cast<int>(virtual_columns_[i]->IsSingleColumn()))
      static_cast<vcolumn::SingleColumn *>(virtual_columns_[i])->TranslateSourceColumns(attr_back_translation);
}

std::shared_ptr<TempTable> TempTable::CreateMaterializedCopy(bool translate_order,
                                                             bool in_subq)  // move all buffers to a newly created
                                                                            // TempTable, make this VC-pointing to
                                                                            // it; the new table must be deleted
                                                                            // by DeleteMaterializedCopy()
{
  MEASURE_FET("TempTable::CreateMaterializedCopy(...)");
  // Copy buffers in a safe place for a moment

  std::vector<void *> copy_buf;
  std::map<PhysicalColumn *, PhysicalColumn *> attr_translation;  // new attr (this), old attr (working_copy)

  for (auto attr : attrs_) {
    copy_buf.push_back(attr->buffer_);
    attr->buffer_ = nullptr;
  }

  if (num_of_global_virtual_columns_ != -1) {
    // this is a TempTable copy
    for (uint i = num_of_global_virtual_columns_; i < virtual_columns_.size(); i++) {
      delete virtual_columns_[i];
      virtual_columns_[i] = nullptr;
    }
    virtual_columns_.resize(num_of_global_virtual_columns_);
  }

  std::shared_ptr<TempTable> working_copy = Create(*this, in_subq);  // Original VCs of this will be copied to
                                                                     // working_copy, and then deleted in its
                                                                     // destructor
  for (auto attr : attrs_) {
    attr->oper_type_ = common::ColOperation::LISTING;
  }

  working_copy->table_mode_ = TableMode();  // reset all flags (distinct, limit) on copy
  // working_copy->for_subq = this->for_subq;
  // working_copy->num_of_global_virtual_columns_ = this->num_of_global_virtual_columns_;
  for (uint i = 0; i < attrs_.size(); i++) {
    working_copy->attrs_[i]->buffer_ = copy_buf[i];  // copy the data buffers
    attrs_[i]->num_of_obj_ = 0;
    attr_translation[attrs_[i]] = working_copy->attrs_[i];
  }

  // VirtualColumns are copied, and we should replace them by references to the
  // temporary source
  delete filter_.mind;
  filter_.mind = new MultiIndex(p_power_);
  filter_.mind->AddDimension_cross(num_of_obj_);

  if (virtual_columns_.size() < attrs_.size())
    virtual_columns_.resize(attrs_.size());

  fill(virtual_columns_.begin(), virtual_columns_.end(), (vcolumn::VirtualColumn *)nullptr);

  for (uint i = 0; i < attrs_.size(); i++) {
    vcolumn::VirtualColumn *new_vc =
        new vcolumn::SingleColumn(working_copy->attrs_[i], filter_.mind, 0, 0, working_copy.get(), 0);

    virtual_columns_[i] = new_vc;
    attrs_[i]->term_.vc = new_vc;
    attrs_[i]->dimension_ = 0;
  }

  if (translate_order) {
    order_by_.clear();  // translation needed: virtual_columns_ should point to working_copy as a data source
    for (uint i = 0; i < working_copy->virtual_columns_.size(); i++) {
      vcolumn::VirtualColumn *orig_vc = working_copy->virtual_columns_[i];
      // if(in_subq && orig_vc->IsSingleColumn())
      //	working_copy->virtual_columns_[i] =
      // CreateVCCopy(working_copy->virtual_columns_[i]);
      for (uint j = 0; j < working_copy->order_by_.size(); j++) {
        if (working_copy->order_by_[j].vc == orig_vc) {
          working_copy->order_by_[j].vc = working_copy->virtual_columns_[i];
        }
      }
      // TODO: redesign it for more universal solution
      vcolumn::VirtualColumn *vc = working_copy->virtual_columns_[i];
      if (vc) {
        if (static_cast<int>(vc->IsSingleColumn()))
          static_cast<vcolumn::SingleColumn *>(vc)->TranslateSourceColumns(attr_translation);
        else {
          auto &var_maps = vc->GetVarMap();
          for (uint i = 0; i < var_maps.size(); i++) {
            if (var_maps[i].just_a_table.lock().get() == this)
              var_maps[i].just_a_table = working_copy;
          }
        }
      }
    }

    for (uint i = 0; i < working_copy->order_by_.size(); i++) {
      SortDescriptor sord;
      sord.vc = working_copy->order_by_[i].vc;
      sord.dir = working_copy->order_by_[i].dir;
      order_by_.push_back(sord);
    }
  } else {  // Create() did move vc's used in order_by_ into working_copy, so we should bring them back
    for (uint i = 0; i < order_by_.size(); i++) {
      // find which working_copy->vc is used in ordering
      for (uint j = 0; j < working_copy->virtual_columns_.size(); j++) {
        if (/*working_copy->*/ order_by_[i].vc == working_copy->virtual_columns_[j]) {
          // order_by_[i].vc = working_copy->virtual_columns_[j];
          MoveVC(j, working_copy->virtual_columns_,
                 virtual_columns_);  // moving vc - now it is back in this
          break;
        }
      }
    }
  }

  num_of_global_virtual_columns_ = (int)virtual_columns_.size();

  return working_copy;  // must be deleted by DeleteMaterializedCopy()
}

void TempTable::DeleteMaterializedCopy(std::shared_ptr<TempTable> &old_t)  // delete the external table and remove
                                                                           // VC pointers, make this fully
                                                                           // materialized_
{
  MEASURE_FET("TempTable::DeleteMaterializedCopy(...)");

  for (uint i = 0; i < attrs_.size(); i++) {  // Make sure VCs are deleted before the source table is deleted
    attrs_[i]->term_.vc = nullptr;
    delete virtual_columns_[i];
    virtual_columns_[i] = nullptr;
  }

  old_t.reset();
}

void TempTable::MoveVC(int colnum, std::vector<vcolumn::VirtualColumn *> &from,
                       std::vector<vcolumn::VirtualColumn *> &to) {
  vcolumn::VirtualColumn *vc = from[colnum];
  to.push_back(vc);
  from[colnum] = nullptr;
  std::vector<vcolumn::VirtualColumn *> vv = vc->GetChildren();

  for (size_t i = 0; i < vv.size(); i++) MoveVC(vv[i], from, to);
}

void TempTable::MoveVC(vcolumn::VirtualColumn *vc, std::vector<vcolumn::VirtualColumn *> &from,
                       std::vector<vcolumn::VirtualColumn *> &to) {
  for (int k = 0; (size_t)k < from.size(); k++)
    if (vc == from[k]) {
      MoveVC(k, from, to);
      break;
    }
}

void TempTable::ReserveVirtColumns(int no) {
  DEBUG_ASSERT((no == 0 && virtual_columns_.size() == 0) || static_cast<size_t>(no) > virtual_columns_.size());
  num_of_global_virtual_columns_ = -1;  // usable value only in TempTable copies and in subq
  virtual_columns_.resize(no);
  virtual_columns_for_having_.resize(no);
}

void TempTable::CreateDisplayableAttrP() {
  if (attrs_.size() == 0)
    return;

  displayable_attr_.clear();
  displayable_attr_.resize(attrs_.size());
  int idx = 0;
  for (size_t i = 0; i < attrs_.size(); i++) {
    if (attrs_[i]->alias_) {
      displayable_attr_[idx] = attrs_[i];
      idx++;
    }
  }

  for (uint i = idx; i < attrs_.size(); i++) displayable_attr_[i] = nullptr;
}

uint TempTable::GetDisplayableAttrIndex(uint attr) {
  uint idx = -1;
  uint i;
  for (i = 0; i < attrs_.size(); i++) {
    if (attrs_[i]->alias_) {
      idx++;
      if (idx == attr)
        break;
    }
  }

  DEBUG_ASSERT(i < attrs_.size());

  return i;
}

void TempTable::AddConds(Condition *cond, CondType type) {
  MEASURE_FET("TempTable::AddConds(...)");

  if (type == CondType::WHERE_COND) {
    // need to add one by one since where_conds can be non-empty
    filter_.AddConditions(cond);
  } else if (type == CondType::HAVING_COND) {
    DEBUG_ASSERT((*cond)[0].tree);
    having_conds_.AddDescriptor((*cond)[0].tree, this, (*cond)[0].left_dims.Size());
  } else
    DEBUG_ASSERT(0);
}

void TempTable::AddInnerConds(Condition *cond, std::vector<TableID> &dim_aliases) {
  for (uint i = 0; i < cond->Size(); i++)
    for (uint j = 0; j < dim_aliases.size(); j++) (*cond)[i].left_dims[GetDimension(dim_aliases[j])] = true;
  filter_.AddConditions(cond);
}

void TempTable::AddLeftConds(Condition *cond, std::vector<TableID> &left_aliases, std::vector<TableID> &right_aliases) {
  for (uint i = 0; i < cond->Size(); i++) {
    for (int d = 0; d < (*cond)[i].left_dims.Size(); d++) (*cond)[i].left_dims[d] = (*cond)[i].right_dims[d] = false;
    for (uint j = 0; j < left_aliases.size(); j++) (*cond)[i].left_dims[GetDimension(left_aliases[j])] = true;
    for (uint j = 0; j < right_aliases.size(); j++) (*cond)[i].right_dims[GetDimension(right_aliases[j])] = true;
  }

  filter_.AddConditions(cond);
}

void TempTable::SetMode(TMParameter mode, int64_t mode_param1, int64_t mode_param2) {
  switch (mode) {
    case TMParameter::TM_DISTINCT:
      this->table_mode_.distinct_ = true;
      break;
    case TMParameter::TM_TOP:
      this->table_mode_.top_ = true;
      this->table_mode_.param1_ = mode_param1;  // offset
      this->table_mode_.param2_ = mode_param2;  // limit
      break;
    case TMParameter::TM_EXISTS:
      this->table_mode_.exists_ = true;
      break;
    default:
      DEBUG_ASSERT(false);
      break;
  }
}

int TempTable::AddColumn(CQTerm e, common::ColOperation mode, char *alias, bool distinct, SI si) {
  if (alias)
    num_of_columns_++;

  common::ColumnType type = common::ColumnType::UNK;  // type of column
  bool notnull{false};                                // NULLs are allowed
  uint scale{0};                                      // number of decimal places
  uint precision{0};                                  // number of all places

  // new code for vcolumn::VirtualColumn
  if (e.vc_id != common::NULL_VALUE_32) {
    DEBUG_ASSERT(e.vc);
    // enum common::ColOperation {LISTING, COUNT, SUM, MIN, MAX, AVG, GROUP_BY};
    if (mode == common::ColOperation::COUNT) {
      type = common::ColumnType::NUM;  // 64 bit, Decimal(18,0)
      precision = 18;
      notnull = true;
    } else if (mode == common::ColOperation::BIT_AND || mode == common::ColOperation::BIT_OR ||
               mode == common::ColOperation::BIT_XOR) {
      type = common::ColumnType::BIGINT;
      precision = 19;
      notnull = true;
    } else if (mode == common::ColOperation::AVG || mode == common::ColOperation::VAR_POP ||
               mode == common::ColOperation::VAR_SAMP || mode == common::ColOperation::STD_POP ||
               mode == common::ColOperation::STD_SAMP) {
      type = common::ColumnType::REAL;  // 64 bit
      precision = 18;
    } else {
      scale = e.vc->Type().GetScale();
      precision = e.vc->Type().GetPrecision();
      type = e.vc->TypeName();
      notnull = e.vc->Type().NotNull();
      if (mode == common::ColOperation::SUM) {
        switch (e.vc->TypeName()) {
          case common::ColumnType::INT:
          case common::ColumnType::NUM:
          case common::ColumnType::SMALLINT:
          case common::ColumnType::BYTEINT:
          case common::ColumnType::MEDIUMINT:
            type = common::ColumnType::NUM;
            precision = 18;
            break;
          case common::ColumnType::BIGINT:
            type = common::ColumnType::BIGINT;
            precision = 19;
            break;
          case common::ColumnType::REAL:
          case common::ColumnType::FLOAT:
          default:  // all other types summed up as double (parsed from text)
            type = common::ColumnType::REAL;
            break;
            //					default:
            //						throw
            // common::NotImplementedException("SUM
            // performed on non-numerical column.");
        }
      } else if (mode == common::ColOperation::MIN || mode == common::ColOperation::MAX) {
        ;  // do nothing.
      } else if (mode == common::ColOperation::LISTING || mode == common::ColOperation::GROUP_BY) {
        if (mode == common::ColOperation::GROUP_BY)
          group_by_ = true;

        notnull = e.vc->Type().NotNull();
      }
    }
  } else if (mode == common::ColOperation::COUNT) {
    type = common::ColumnType::NUM;
    precision = 18;
    notnull = true;
  } else {
    // illegal execution path: neither VC is set nor mode is
    // common::ColOperation::COUNT
    DEBUG_ASSERT(!"wrong execution path");
    // throw common::NotImplementedException("Invalid column on SELECT list.");
  }

  attrs_.push_back(new Attr(e, mode, p_power_, distinct, alias, -2, type, scale, precision, notnull,
                            e.vc ? e.vc->GetCollation() : DTCollation(), &si));

  return -(int)attrs_.size();  // attributes are counted from -1, -2, ...
}

void TempTable::AddOrder(vcolumn::VirtualColumn *vc, int direction) {
  // don't sort by const expressions
  if (vc->IsConst())
    return;
  SortDescriptor d;
  d.vc = vc;
  d.dir = direction;
  bool already_added = false;
  for (uint j = 0; j < order_by_.size(); j++) {
    if (order_by_[j] == d) {
      already_added = true;
      break;
    }
  }
  if (!already_added)
    order_by_.push_back(d);
}

void TempTable::Union(TempTable *t, int all) {
  MEASURE_FET("TempTable::UnionOld(...)");
  if (!t) {  // trivial union with single select and external order by
    this->Materialize();
    return;
  }
  DEBUG_ASSERT(NumOfDisplaybleAttrs() == t->NumOfDisplaybleAttrs());
  if (NumOfDisplaybleAttrs() != t->NumOfDisplaybleAttrs())
    throw common::NotImplementedException("UNION of tables with different number of columns.");
  if (this->IsParametrized() || t->IsParametrized())
    throw common::NotImplementedException("Materialize: not implemented union of parameterized queries.");
  rc_control_.lock(m_conn_->GetThreadID()) << "UNION: materializing components." << system::unlock;
  this->Materialize();
  t->Materialize();
  if ((!t->NumOfObj() && all) || (!this->NumOfObj() && !t->NumOfObj()))  // no objects = no union
    return;

  Filter first_f(NumOfObj(), p_power_),
      first_mask(NumOfObj(), p_power_);  // mask of objects to be added to the final result set
  Filter sec_f(t->NumOfObj(), p_power_), sec_mask(t->NumOfObj(), p_power_);
  first_mask.Set();
  sec_mask.Set();

  if (!all) {
    rc_control_.lock(m_conn_->GetThreadID()) << "UNION: excluding repetitions." << system::unlock;
    Filter first_f(NumOfObj(), p_power_);
    first_f.Set();
    Filter sec_f(t->NumOfObj(), p_power_);
    sec_f.Set();

    GroupDistinctTable dist_table(p_power_);
    using vc_ptr_t = std::shared_ptr<vcolumn::VirtualColumn>;
    std::vector<vc_ptr_t> first_vcs;
    std::vector<vc_ptr_t> sec_vcs;
    uchar *input_buf = nullptr;

    {
      // block to ensure deleting encoder before deleting first_vcs, sec_vcs
      int size = 0;
      std::vector<ColumnBinEncoder> encoder;
      for (int i = 0; i < (int)NumOfDisplaybleAttrs(); i++) {
        first_vcs.push_back(
            vc_ptr_t(new vcolumn::SingleColumn(GetDisplayableAttrP(i), &output_multi_index_, 0, -i - 1, this, 0)));
        sec_vcs.push_back(
            vc_ptr_t(new vcolumn::SingleColumn(t->GetDisplayableAttrP(i), t->GetOutputMultiIndexP(), 1, -i - 1, t, 0)));
        encoder.push_back(ColumnBinEncoder());

        bool encoder_created{false};
        if (NumOfObj() == 0)
          encoder_created = encoder[i].PrepareEncoder(sec_vcs[i].get());
        else if (t->NumOfObj() == 0)
          encoder_created = encoder[i].PrepareEncoder(first_vcs[i].get());
        else
          encoder_created = encoder[i].PrepareEncoder(first_vcs[i].get(), sec_vcs[i].get());

        if (!encoder_created) {
          std::stringstream ss;
          ss << "UNION of non-matching columns (column no " << i << ") .";
          throw common::NotImplementedException(ss.str());
        }

        encoder[i].SetPrimaryOffset(size);
        size += encoder[i].GetPrimarySize();
      }

      input_buf = new uchar[size];
      memset(input_buf, 0x0, size);
      dist_table.InitializeB(size, NumOfObj() + t->NumOfObj() / 2);  // optimization assumption: a
                                                                     // half of values in the second
                                                                     // table will be repetitions
      MIIterator first_mit(&output_multi_index_, p_power_);
      MIIterator sec_mit(t->GetOutputMultiIndexP(), p_power_);
      // check all objects from the first table
      do {
        first_mit.Rewind();
        dist_table.Clear();

        while (first_mit.IsValid()) {
          int64_t pos = **first_mit;
          if (first_f.Get(pos)) {
            for (uint i = 0; i < encoder.size(); i++) encoder[i].Encode(input_buf, first_mit);

            GDTResult res = dist_table.Add(input_buf);
            if (res == GDTResult::GDT_EXISTS)
              first_mask.ResetDelayed(pos);

            if (res != GDTResult::GDT_FULL)  // note: if v is omitted here, it will also be omitted in sec!
              first_f.ResetDelayed(pos);
          }

          ++first_mit;
          if (m_conn_->Killed())
            throw common::KilledException();

        }  // while

        sec_mit.Rewind();
        while (sec_mit.IsValid()) {
          int64_t pos = **sec_mit;
          if (sec_f.Get(pos)) {
            for (uint i = 0; i < encoder.size(); i++) encoder[i].Encode(input_buf, sec_mit, sec_vcs[i].get());

            GDTResult res = dist_table.Add(input_buf);
            if (res == GDTResult::GDT_EXISTS)
              sec_mask.ResetDelayed(pos);
            if (res != GDTResult::GDT_FULL)
              sec_f.ResetDelayed(pos);
          }

          ++sec_mit;
          if (m_conn_->Killed())
            throw common::KilledException();
        }

        first_f.Commit();
        sec_f.Commit();
        first_mask.Commit();
        sec_mask.Commit();

      } while (!first_f.IsEmpty() || !sec_f.IsEmpty());
    }

    delete[] input_buf;
    input_buf = nullptr;
  }

  int64_t first_no_obj = first_mask.NumOfOnes();
  int64_t sec_no_obj = sec_mask.NumOfOnes();
  int64_t new_no_obj = first_no_obj + sec_no_obj;
  rc_control_.lock(m_conn_->GetThreadID()) << "UNION: generating result (" << new_no_obj << " rows)." << system::unlock;
  uint new_page_size = CalculatePageSize(new_no_obj);

  for (uint i = 0; i < NumOfDisplaybleAttrs(); i++) {
    Attr *first_attr = GetDisplayableAttrP(i);
    Attr *sec_attr = t->GetDisplayableAttrP(i);
    ColumnType new_type = GetUnionType(first_attr->Type(), sec_attr->Type());

    if (first_attr->Type() == sec_attr->Type() && first_mask.IsFull() && first_no_obj && sec_no_obj &&
        first_attr->Type().GetPrecision() >= sec_attr->Type().GetPrecision()) {
      // fast path of execution: do not modify first buffer
      SetPageSize(new_page_size);
      FilterOnesIterator sec_fi(&sec_mask, p_power_);
      for (int64_t j = 0; j < sec_no_obj; j++) {
        types::BString s;
        if (ATI::IsStringType(first_attr->TypeName())) {
          sec_attr->GetValueString(s, *sec_fi);
          first_attr->SetValueString(first_no_obj + j, s);
        } else {
          first_attr->SetValueInt64(first_no_obj + j, sec_attr->GetValueInt64(*sec_fi));
        }
        ++sec_fi;
      }
      continue;
    }

    Attr *new_attr = new Attr(CQTerm(), common::ColOperation::LISTING, p_power_, false, first_attr->alias_, 0,
                              new_type.GetTypeName(), new_type.GetScale(), new_type.GetPrecision(), new_type.NotNull(),
                              new_type.GetCollation());
    new_attr->page_size_ = new_page_size;
    new_attr->CreateBuffer(new_no_obj, m_conn_);

    if (first_attr->TypeName() == common::ColumnType::NUM && sec_attr->TypeName() == common::ColumnType::NUM &&
        first_attr->Type().GetScale() != sec_attr->Type().GetScale()) {
      uint max_scale = new_attr->Type().GetScale();
      // copy attr from first table to new_attr
      double multiplier = types::PowOfTen(max_scale - first_attr->Type().GetScale());
      FilterOnesIterator first_fi(&first_mask, p_power_);

      for (int64_t j = 0; j < first_no_obj; j++) {
        int64_t pos = *first_fi;
        ++first_fi;
        if (!first_attr->IsNull(pos))
          new_attr->SetValueInt64(j, first_attr->GetNotNullValueInt64(pos) * (int64_t)multiplier);
        else
          new_attr->SetValueInt64(j, common::NULL_VALUE_64);
      }

      // copy attr from second table to new_attr
      multiplier = types::PowOfTen(max_scale - sec_attr->Type().GetScale());
      FilterOnesIterator sec_fi(&sec_mask, p_power_);

      for (int64_t j = 0; j < sec_no_obj; j++) {
        int64_t pos = *sec_fi;
        ++sec_fi;
        if (!sec_attr->IsNull(pos))
          new_attr->SetValueInt64(first_no_obj + j, sec_attr->GetNotNullValueInt64(pos) * (int64_t)multiplier);
        else
          new_attr->SetValueInt64(first_no_obj + j, common::NULL_VALUE_64);
      }
    } else if (ATI::IsStringType(new_attr->TypeName())) {
      types::BString s;
      FilterOnesIterator first_fi(&first_mask, p_power_);

      for (int64_t j = 0; j < first_no_obj; j++) {
        first_attr->GetValueString(s, *first_fi);
        new_attr->SetValueString(j, s);
        ++first_fi;
      }

      FilterOnesIterator sec_fi(&sec_mask, p_power_);
      for (int64_t j = 0; j < sec_no_obj; j++) {
        sec_attr->GetValueString(s, *sec_fi);
        new_attr->SetValueString(first_no_obj + j, s);
        ++sec_fi;
      }
    } else if (new_attr->Type().IsFloat()) {
      FilterOnesIterator first_fi(&first_mask, p_power_);

      for (int64_t j = 0; j < first_no_obj; j++) {
        int64_t pos = *first_fi;
        ++first_fi;
        if (first_attr->IsNull(pos))
          new_attr->SetValueInt64(j, common::NULL_VALUE_64);
        else if (first_attr->Type().IsFloat())
          new_attr->SetValueInt64(j, first_attr->GetNotNullValueInt64(pos));
        else {
          double v = (double)first_attr->GetNotNullValueInt64(pos) / types::PowOfTen(first_attr->Type().GetScale());
          new_attr->SetValueInt64(j, *(int64_t *)&v);
        }
      }

      FilterOnesIterator sec_fi(&sec_mask, p_power_);
      for (int64_t j = 0; j < sec_no_obj; j++) {
        int64_t pos = *sec_fi;
        ++sec_fi;
        if (sec_attr->IsNull(pos))
          new_attr->SetValueInt64(first_no_obj + j, common::NULL_VALUE_64);
        else if (sec_attr->Type().IsFloat())
          new_attr->SetValueInt64(first_no_obj + j, sec_attr->GetNotNullValueInt64(pos));
        else {
          double v = (double)sec_attr->GetNotNullValueInt64(pos) / types::PowOfTen(sec_attr->Type().GetScale());
          new_attr->SetValueInt64(first_no_obj + j, *(int64_t *)&v);
        }
      }
    } else {
      // copy attr from first table to new_attr
      double multiplier = types::PowOfTen(new_attr->Type().GetScale() - first_attr->Type().GetScale());
      FilterOnesIterator first_fi(&first_mask, p_power_);
      for (int64_t j = 0; j < first_no_obj; j++) {
        int64_t pos = *first_fi;
        ++first_fi;
        if (first_attr->IsNull(pos))
          new_attr->SetValueInt64(j, common::NULL_VALUE_64);
        else if (multiplier == 1.0)  // do not multiply by 1.0, as it causes
                                     // precision problems on bigint
          new_attr->SetValueInt64(j, first_attr->GetNotNullValueInt64(pos));
        else
          new_attr->SetValueInt64(j, (int64_t)(first_attr->GetNotNullValueInt64(pos) * multiplier));
      }
      multiplier = types::PowOfTen(new_attr->Type().GetScale() - sec_attr->Type().GetScale());
      FilterOnesIterator sec_fi(&sec_mask, p_power_);
      for (int64_t j = 0; j < sec_no_obj; j++) {
        int64_t pos = *sec_fi;
        ++sec_fi;
        if (sec_attr->IsNull(pos))
          new_attr->SetValueInt64(first_no_obj + j, common::NULL_VALUE_64);
        else if (multiplier == 1.0)  // do not multiply by 1.0, as it causes
                                     // precision problems on bigint
          new_attr->SetValueInt64(first_no_obj + j, sec_attr->GetNotNullValueInt64(pos));
        else
          new_attr->SetValueInt64(first_no_obj + j, (int64_t)(sec_attr->GetNotNullValueInt64(pos) * multiplier));
      }
    }

    attrs_[GetDisplayableAttrIndex(i)] = new_attr;
    displayable_attr_[i] = new_attr;
    delete first_attr;
    first_attr = nullptr;
  }

  SetNumOfMaterialized(new_no_obj);
  output_multi_index_.Clear();
  output_multi_index_.AddDimension_cross(num_of_obj_);
}

void TempTable::Union(TempTable *t, [[maybe_unused]] int all, ResultSender *sender, int64_t &g_offset,
                      int64_t &g_limit) {
  MEASURE_FET("TempTable::UnionSender(...)");

  DEBUG_ASSERT(NumOfDisplaybleAttrs() == t->NumOfDisplaybleAttrs());
  if (NumOfDisplaybleAttrs() != t->NumOfDisplaybleAttrs())
    throw common::NotImplementedException("UNION of tables with different number of columns.");
  if (this->IsParametrized() || t->IsParametrized())
    throw common::NotImplementedException("Materialize: not implemented union of parameterized queries.");

  if (table_mode_.top_) {
    if (g_offset != 0 || g_limit != -1) {
      table_mode_.param2_ = std::min(g_offset + g_limit, table_mode_.param2_);
    }
  } else if (g_offset != 0 || g_limit != -1) {
    table_mode_.top_ = true;
    table_mode_.param1_ = 0;
    table_mode_.param2_ = g_limit + g_offset;
  }

  if (g_offset != 0 || g_limit != -1)
    sender->SetLimits(&g_offset, &g_limit);

  this->Materialize(false, sender);

  if (this->IsMaterialized() && !this->IsSent())
    sender->Send(this);

  if (t->table_mode_.top_) {
    if (g_offset != 0 || g_limit != -1) {
      t->table_mode_.param2_ = std::min(g_offset + g_limit, t->table_mode_.param2_);
    }
  } else if (g_offset != 0 || g_limit != -1) {
    t->table_mode_.top_ = true;
    t->table_mode_.param1_ = 0;
    t->table_mode_.param2_ = g_limit + g_offset;
  }

  // second query might want to switch to mysql but the first is already sent
  // we have to prevent switching to mysql otherwise Malformed Packet error may
  // occur
  try {
    t->Materialize(false, sender);
  } catch (common::Exception &e) {
    std::string msg(e.what());
    msg.append(" Can't switch to MySQL execution path");
    throw common::InternalException(msg);
  }
  if (t->IsMaterialized() && !t->IsSent())
    sender->Send(t);
}

void TempTable::Display(std::ostream &out) {
  out << "No obj.:" << num_of_obj_ << ", No attrs.:" << this->NumOfDisplaybleAttrs() << system::endl
      << "-----------" << system::endl;

  for (int64_t i = 0; i < num_of_obj_; i++) {
    for (uint j = 0; j < this->NumOfDisplaybleAttrs(); j++) {
      if (!attrs_[j]->alias_)
        continue;

      if (!IsNull(i, j)) {
        types::BString s;
        GetTable_S(s, i, j);
        out << s << " ";
      } else
        out << "nullptr"
            << " ";
    }

    out << system::endl;
  }

  out << "-----------" << system::endl;
}

int TempTable::GetDimension(TableID alias) {
  for (uint i = 0; i < aliases_.size(); i++) {
    if (aliases_[i] == alias.n)
      return i;
  }
  return common::NULL_VALUE_32;
}

int64_t TempTable::GetTable64(int64_t obj, int attr) {
  if (num_of_obj_ == 0)
    return common::NULL_VALUE_64;

  DEBUG_ASSERT(obj < num_of_obj_ && (uint)attr < attrs_.size());

  return attrs_[attr]->GetValueInt64(obj);
}

void TempTable::GetTable_S(types::BString &s, int64_t obj, int _attr) {
  if (num_of_obj_ == 0) {
    s = types::BString();
    return;
  }

  uint attr = (uint)_attr;

  DEBUG_ASSERT(obj < num_of_obj_ && attr < attrs_.size());

  attrs_[attr]->GetValueString(s, obj);
}

bool TempTable::IsNull(int64_t obj, int attr) {
  if (num_of_obj_ == 0)
    return true;

  DEBUG_ASSERT(obj < num_of_obj_ && (uint)attr < attrs_.size());

  return attrs_[attr]->IsNull(obj);
}

void TempTable::SuspendDisplay() { m_conn_->SuspendDisplay(); }

void TempTable::ResumeDisplay() { m_conn_->ResumeDisplay(); }

uint64_t TempTable::ApproxAnswerSize([[maybe_unused]] int attr,
                                     [[maybe_unused]] Descriptor &d)  // provide the most probable
                                                                      // approximation of number of objects
                                                                      // matching the condition
{
  // TODO: can it be done better?
  return num_of_obj_ / 2;
}

void TempTable::GetTableString(types::BString &s, int64_t obj, uint attr) {
  if (num_of_obj_ == 0)
    s = types::BString();

  DEBUG_ASSERT(obj < num_of_obj_ && (uint)attr < attrs_.size());

  attrs_[attr]->GetValueString(s, obj);
}

types::RCValueObject TempTable::GetValueObject(int64_t obj, uint attr) {
  if (num_of_obj_ == 0)
    return types::RCValueObject();

  DEBUG_ASSERT(obj < num_of_obj_ && (uint)attr < attrs_.size());

  return attrs_[attr]->GetValue(obj);
}

uint TempTable::CalculatePageSize(int64_t _no_obj) {
  int64_t new_no_obj = _no_obj == -1 ? num_of_obj_ : _no_obj;
  uint size_of_one_record = 0;

  for (auto attr : attrs_) {
    if (attr->TypeName() == common::ColumnType::BIN || attr->TypeName() == common::ColumnType::BYTE ||
        attr->TypeName() == common::ColumnType::VARBYTE || attr->TypeName() == common::ColumnType::LONGTEXT ||
        attr->TypeName() == common::ColumnType::STRING || attr->TypeName() == common::ColumnType::VARCHAR)
      size_of_one_record += attr->Type().GetInternalSize() + 4;  // 4 bytes describing length
    else
      size_of_one_record += attr->Type().GetInternalSize();
  }

  uint raw_size = (uint)new_no_obj;
  if (size_of_one_record < 1)
    size_of_one_record = 1;

  SetOneOutputRecordSize(size_of_one_record);
  uint64_t cache_size{0};
  if (mem_scale_ == -1)
    mem_scale_ = mm::TraceableObject::MemorySettingsScale();

  //???????????????????????
  switch (mem_scale_) {
    case 0:
    case 1:
      cache_size = 1l << 26;
      break;  // 64MB
    case 2:
      cache_size = 1l << 27;
      break;  // 128MB
    case 3:
    case 4:
      cache_size = 1l << 28;
      break;  // 256MB
    case 5:
      cache_size = 1l << 29;
      break;  // 512MB
    case 6:
      cache_size = 1l << 30;
      break;  // 1GB
    case 7:
    case 8:
      cache_size = 1l << 31;
      break;  // 2GB
    default:
      cache_size = 1l << 31;
      break;  // heap larger than 40GB-> 2GB Cache size
  }

  if (cache_size / size_of_one_record <= (uint64_t)new_no_obj) {
    raw_size = uint((cache_size - 1) / size_of_one_record);  // minus 1 to avoid overflow
  }

  if (raw_size < 1)
    raw_size = 1;

  for (auto attr : attrs_) attr->page_size_ = raw_size;

  return raw_size;
}

void TempTable::SetPageSize(int64_t new_page_size) {
  for (auto attr : attrs_) attr->SetNewPageSize((uint)new_page_size);
}

/*! Filter out rows from the given multiindex according to the tree of
 * descriptors
 *
 */

void TempTable::DisplayRSI() {
  for (auto src_table : src_tables_) {
    if (src_table->TableType() == TType::TABLE)
      ((RCTable *)src_table)->DisplayRSI();
  }
}

// Refactoring: extracted methods
void TempTable::RemoveFromManagedList(const RCTable *tab) {
  src_tables_.erase(std::remove(src_tables_.begin(), src_tables_.end(), tab), src_tables_.end());
}

void TempTable::ApplyOffset(int64_t limit, int64_t offset) {
  // filter out all unwanted values from buffers
  num_of_obj_ = limit;

  // for (uint i = 0; i < attrs.size(); i++) {
  for (auto attr : attrs_) {
    if (attr->alias_)
      attr->ApplyFilter(output_multi_index_, offset, offset + limit);
    else
      attr->DeleteBuffer();
  }
}

void TempTable::ProcessParameters(const MIIterator &mit, const int alias) {
  MEASURE_FET("TempTable::ProcessParameters(...)");

  for (uint i = 0; i < virtual_columns_.size(); i++) virtual_columns_[i]->RequestEval(mit, alias);

  filter_.ProcessParameters();
  // filter.Prepare();
  if (table_mode_.top_ && LimitMayBeAppliedToWhere())  // easy case - apply limits
    filter_.UpdateMultiIndex(false, (table_mode_.param2_ >= 0 ? table_mode_.param2_ : 0));
  else
    filter_.UpdateMultiIndex(false, -1);
}

void TempTable::RoughProcessParameters(const MIIterator &mit, const int alias) {
  MEASURE_FET("TempTable::RoughProcessParameters(...)");

  for (auto vc : virtual_columns_) vc->RequestEval(mit, alias);

  filter_.ProcessParameters();
  filter_.RoughUpdateParamFilter();
}

bool TempTable::IsParametrized() {
  for (auto vc : virtual_columns_) {
    if (vc && vc->IsParameterized())
      return true;
  }

  return false;
}

int TempTable::DimInDistinctContext() {
  // return a dimension number if it is used only in contexts where row
  // repetitions may be omitted, e.g. distinct
  int d = -1;
  if (HasHavingConditions() || filter_.mind->NumOfDimensions() == 1)  // having or no joins
    return -1;

  bool group_by_exists{false};
  bool aggregation_exists{false};

  // for (uint i = 0; i < attrs_.size(); i++){
  for (auto attr : attrs_) {
    if (attr) {
      if (attr->oper_type_ == common::ColOperation::GROUP_BY)
        group_by_exists = true;

      if (attr->oper_type_ != common::ColOperation::GROUP_BY && attr->oper_type_ != common::ColOperation::LISTING &&
          attr->oper_type_ != common::ColOperation::DELAYED)
        aggregation_exists = true;

      if (!attr->term_.vc || attr->term_.vc->IsConst())
        continue;

      int local_dim = attr->term_.vc->GetDim();

      if (local_dim == -1 || (d != -1 && d != local_dim))
        return -1;

      d = local_dim;
    }
  }

  // all columns are based on the same dimension
  if (group_by_exists && aggregation_exists)  // group by with aggregations - not distinct context
    return -1;

  if (group_by_exists ||
      (table_mode_.distinct_ && !aggregation_exists))  // select distinct a,b,c...; select a,b,c group by a,b,c
    return d;

  // aggregations exist - check their type
  for (auto attr : attrs_) {
    if (attr && attr->oper_type_ != common::ColOperation::MIN && attr->oper_type_ != common::ColOperation::MAX &&
        !attr->is_distinct_)
      return -1;
  }

  return d;
}

bool TempTable::LimitMayBeAppliedToWhere() {
  if (order_by_.size() > 0)  // ORDER BY => false
    return false;

  if (table_mode_.distinct_ || HasHavingConditions())  // DISTINCT or HAVING  => false
    return false;

  for (auto attr : attrs_)  // GROUP BY or other aggregation => false
    if (attr->oper_type_ != common::ColOperation::LISTING)
      return false;

  return true;
}

int TempTable::AddVirtColumn(vcolumn::VirtualColumn *vc) {
  virtual_columns_.push_back(vc);
  virtual_columns_for_having_.push_back(vc->GetMultiIndex() == &output_multi_index_);

  return (int)virtual_columns_.size() - 1;
}

int TempTable::AddVirtColumn(vcolumn::VirtualColumn *vc, int no) {
  DEBUG_ASSERT(static_cast<size_t>(no) < virtual_columns_.size());

  virtual_columns_for_having_[no] = (vc->GetMultiIndex() == &output_multi_index_);
  virtual_columns_[no] = vc;

  return no;
}

void TempTable::ResetVCStatistics() {
  for (auto vc : virtual_columns_) {
    if (vc)
      vc->ResetLocalStatistics();
  }
}

void TempTable::SetVCDistinctVals(int dim, int64_t val) {
  for (auto vc : virtual_columns_) {
    if (vc && vc->GetDim() == dim)
      vc->SetLocalDistVals(val);
  }
}

std::shared_ptr<TempTable> TempTable::Create(const TempTable &t, bool in_subq) {
  MEASURE_FET("TempTable::Create(...)");

  bool _is_vc_owner = !in_subq;
  std::shared_ptr<TempTable> tnew = std::shared_ptr<TempTable>(new TempTable(t, _is_vc_owner));

  if (in_subq) {
    // map<PhysicalColumn *, PhysicalColumn *> attr_translation;	//
    // SingleColumns should refer to columns of tnew
    for (uint i = 0; i < t.attrs_.size(); i++) {
      // attr_translation[t.attrs[i]] = tnew->attrs[i];
      tnew->attr_back_translation[tnew->attrs_[i]] = t.attrs_[i];
    }
    // for(uint i = 0; i< tnew->virtual_columns_.size(); i++) {
    //	if(tnew->virtual_columns_[i]->IsSingleColumn())
    //		static_cast<vcolumn::SingleColumn*>(tnew->virtual_columns_[i])->TranslateSourceColumns(attr_translation);
    //	if(tnew->virtual_columns_for_having_[i]) {
    //		tnew->virtual_columns_[i]->SetMultiIndex(&tnew->output_multi_index_, tnew);
    //	} else
    //		tnew->virtual_columns_[i]->SetMultiIndex(tnew->filter.mind);
    //}
  }
  return tnew;
}

std::shared_ptr<TempTable> TempTable::Create(JustATable *const t, int alias, Query *q, bool for_subq) {
  if (for_subq)
    return std::shared_ptr<TempTable>(new TempTableForSubquery(t, alias, q));
  else
    return std::shared_ptr<TempTable>(new TempTable(t, alias, q));
}

ColumnType TempTable::GetUnionType(ColumnType type1, ColumnType type2) {
  if (type1.IsFloat() || type2.IsFloat())
    return type1.IsFloat() ? type1 : type2;

  if (type1.IsString() && type1.GetPrecision() != 0 && !type2.IsString()) {
    ColumnType t(type1);

    if (t.GetPrecision() < type2.GetPrecision())
      t.SetPrecision(type2.GetPrecision());
    return t;
  }

  if (type2.IsString() && type2.GetPrecision() != 0 && !type1.IsString()) {
    ColumnType t(type2);
    if (t.GetPrecision() < type1.GetPrecision())
      t.SetPrecision(type1.GetPrecision());
    return t;
  }

  if (type1.IsFixed() && type2.IsFixed() && type1.GetScale() != type2.GetScale()) {
    uint max_scale = type1.GetScale() > type2.GetScale() ? type1.GetScale() : type2.GetScale();
    uint max_ints = (type1.GetPrecision() - type1.GetScale()) > (type2.GetPrecision() - type2.GetScale())
                        ? (type1.GetPrecision() - type1.GetScale())
                        : (type2.GetPrecision() - type2.GetScale());
    if (max_ints + max_scale > 18) {
      std::stringstream ss;
      ss << "UNION of non-matching columns (column no " << 0 << ") .";
      throw common::NotImplementedException(ss.str());
    }

    return type1.GetScale() > type2.GetScale() ? type1 : type2;
  }

  if (type1.GetInternalSize() < type2.GetInternalSize())
    return type2;
  else if (type1.GetInternalSize() == type2.GetInternalSize())
    return type1.GetPrecision() > type2.GetPrecision() ? type1 : type2;
  else
    return type1;
}

bool TempTable::SubqueryInFrom() {
  for (auto src_table : src_tables_) {
    if (src_table->TableType() == TType::TEMP_TABLE)
      return true;
  }

  return false;
}

void TempTable::LockPackForUse([[maybe_unused]] unsigned attr, unsigned pack_no) {
  while (lazy_ && num_of_materialized_ < std::min(((int64_t)pack_no << p_power_) + (1 << p_power_), num_of_obj_))
    Materialize(false, nullptr, true);
}

bool TempTable::CanOrderSources() {
  for (auto ob : order_by_) {
    if (ob.vc->GetMultiIndex() != filter_.mind)
      return false;
  }

  return true;
}

void TempTable::Materialize(bool in_subq, ResultSender *sender, bool lazy) {
  MEASURE_FET("TempTable::Materialize()");
  if (sender)
    sender->SetAffectRows(num_of_obj_);

  CreateDisplayableAttrP();
  CalculatePageSize();
  int64_t offset = 0;  // controls the first object to be materialized_
  int64_t limit = -1;  // if(limit>=0)  ->  for(row = offset; row < offset +
                       // limit; row++) ....
  bool limits_present = table_mode_.top_;
  bool exists_only = table_mode_.exists_;

  if (limits_present) {
    offset = (table_mode_.param1_ >= 0 ? table_mode_.param1_ : 0);
    limit = (table_mode_.param2_ >= 0 ? table_mode_.param2_ : 0);
    // applied => reset to default values
    table_mode_.top_ = false;
    table_mode_.param2_ = -1;
    table_mode_.param1_ = 0;
  }

  int64_t local_offset = 0;  // controls the first object to be materialized_ in a given algorithm
  int64_t local_limit = -1;

  if (materialized_ && (order_by_.size() > 0 || limits_present) && num_of_obj_) {
    // if TempTable is already materialized_ but some additional constraints were
    // specified, e.g., order by or limit this is typical case for union, where
    // constraints are specified for the result of union after materialization
    if (limits_present) {
      local_offset = offset;
      local_limit = std::min(limit, (int64_t)num_of_obj_ - offset);
      local_limit = local_limit < 0 ? 0 : local_limit;
    } else
      local_limit = num_of_obj_;
    if (exists_only) {
      if (local_limit == 0)  // else no change needed
        num_of_obj_ = 0;
      return;
    }

    if (order_by_.size() != 0 && num_of_obj_ > 1) {
      std::shared_ptr<TempTable> temporary_source_table =
          CreateMaterializedCopy(true, in_subq);  // true: translate definition of ordering
      OrderByAndMaterialize(order_by_, local_limit, local_offset);
      DeleteMaterializedCopy(temporary_source_table);
    } else if (limits_present)
      ApplyOffset(local_limit, local_offset);

    order_by_.clear();
    return;
  }

  if ((materialized_ && !this->lazy_) || (this->lazy_ && num_of_obj_ == num_of_materialized_)) {
    return;
  }

  bool table_distinct = this->table_mode_.distinct_;
  bool distinct_on_materialized = false;

  for (auto attr : attrs_) {
    if (attr->oper_type_ != common::ColOperation::LISTING)
      group_by_ = true;
  }

  if (table_distinct && group_by_) {
    distinct_on_materialized = true;
    table_distinct = false;
  }

  if (order_by_.size() > 0 || group_by_ || this->table_mode_.distinct_ || force_full_materialize_)
    lazy = false;

  this->lazy_ = lazy;

  bool no_rows_too_large = filter_.mind->TooManyTuples();
  num_of_obj_ = -1;    // num_of_obj_ not calculated yet - wait for better moment
  VerifyAttrsSizes();  // resize attr[i] buffers basing on the current
                       // multiindex state

  // the case when there is no grouping of attributes, check also DISTINCT
  // modifier of TT
  if (!group_by_ && !table_distinct) {
    DEBUG_ASSERT(!distinct_on_materialized);  // should by false here, otherwise must be
                                              // added to conditions below

    if (limits_present) {
      if (no_rows_too_large && order_by_.size() == 0)
        num_of_obj_ = offset + limit;  // offset + limit in the worst case
      else
        num_of_obj_ = filter_.mind->NumOfTuples();

      if (num_of_obj_ <= offset) {
        num_of_obj_ = 0;
        materialized_ = true;
        order_by_.clear();
        return;
      }

      local_offset = offset;
      local_limit = std::min(limit, (int64_t)num_of_obj_ - offset);
      local_limit = local_limit < 0 ? 0 : local_limit;
    } else {
      num_of_obj_ = filter_.mind->NumOfTuples();
      local_limit = num_of_obj_;
    }

    if (exists_only) {
      order_by_.clear();
      return;
    }

    output_multi_index_.Clear();
    output_multi_index_.AddDimension_cross(local_limit);  // an artificial dimension for result

    CalculatePageSize();  // recalculate, as num_of_obj_ might changed
    // perform order by: in this case it can be done on source tables, not on
    // the result
    bool materialized_by_ordering = false;
    if (CanOrderSources())
      // false if no sorting used
      materialized_by_ordering = this->OrderByAndMaterialize(order_by_, local_limit, local_offset, sender);
    if (!materialized_by_ordering) {  // not materialized_ yet?
      // materialize without aggregations. If ordering then do not send result
      if (order_by_.size() == 0)
        FillMaterializedBuffers(local_limit, local_offset, sender, lazy);
      else  // in case of order by we need to materialize all rows to be next
            // ordered
        FillMaterializedBuffers(num_of_obj_, 0, nullptr, lazy);
    }
  } else {
    // GROUP BY or DISTINCT -  compute aggregations
    if (limits_present && !distinct_on_materialized && order_by_.size() == 0) {
      local_offset = offset;
      local_limit = limit;
      if (exists_only)
        local_limit = 1;
    }

    if (HasHavingConditions() && in_subq)
      having_conds_[0].tree->Simplify(true);

    ResultSender *local_sender = (distinct_on_materialized || order_by_.size() > 0 ? nullptr : sender);
    AggregationAlgorithm aggr(this);
    aggr.Aggregate(table_distinct, local_limit, local_offset, local_sender);  // this->tree (HAVING) used inside

    if (num_of_obj_ == 0) {
      order_by_.clear();
      return;
    }

    output_multi_index_.Clear();
    output_multi_index_.AddDimension_cross(num_of_obj_);  // an artificial dimension for result
  }

  local_offset = 0;
  local_limit = -1;

  // DISTINCT after grouping
  if (distinct_on_materialized && num_of_obj_ > 1) {
    if (limits_present && order_by_.size() == 0) {
      local_offset = std::min((int64_t)num_of_obj_, offset);
      local_limit = std::min(limit, (int64_t)num_of_obj_ - local_offset);
      local_limit = local_limit < 0 ? 0 : local_limit;
      if (num_of_obj_ <= offset) {
        num_of_obj_ = 0;
        materialized_ = true;
        order_by_.clear();
        return;
      }
    } else
      local_limit = num_of_obj_;

    if (exists_only)
      local_limit = 1;

    std::shared_ptr<TempTable> temporary_source_table = CreateMaterializedCopy(false, in_subq);
    ResultSender *local_sender = (order_by_.size() > 0 ? nullptr : sender);

    AggregationAlgorithm aggr(this);
    aggr.Aggregate(true, local_limit, local_offset,
                   local_sender);  // true => select-level distinct
    DeleteMaterializedCopy(temporary_source_table);
    output_multi_index_.Clear();
    output_multi_index_.AddDimension_cross(num_of_obj_);  // an artificial dimension for result
  }                                                       // end of distinct part
  // ORDER BY, if not sorted until now
  if (order_by_.size() != 0) {
    if (limits_present) {
      local_offset = std::min((int64_t)num_of_obj_, offset);
      local_limit = std::min(limit, (int64_t)num_of_obj_ - local_offset);
      local_limit = local_limit < 0 ? 0 : local_limit;

      if (num_of_obj_ <= offset) {
        num_of_obj_ = 0;
        materialized_ = true;
        order_by_.clear();
        return;
      }
    } else
      local_limit = num_of_obj_;

    if (num_of_obj_ > 1 && !exists_only) {
      std::shared_ptr<TempTable> temporary_source_table =
          CreateMaterializedCopy(true, in_subq);  // true: translate definition of ordering
      OrderByAndMaterialize(order_by_, local_limit, local_offset, sender);
      DeleteMaterializedCopy(temporary_source_table);
    }

    order_by_.clear();
    output_multi_index_.Clear();
    output_multi_index_.AddDimension_cross(num_of_obj_);  // an artificial dimension for result
  }

  materialized_ = true;
}

void TempTable::RecordIterator::PrepareValues() {
  if (_currentRNo < uint64_t(table->NumOfObj())) {
    uint no_disp_attr = table->NumOfDisplaybleAttrs();
    for (uint att = 0; att < no_disp_attr; ++att) {
      common::ColumnType attrt_tmp = table->GetDisplayableAttrP(att)->TypeName();
      if (attrt_tmp == common::ColumnType::INT || attrt_tmp == common::ColumnType::MEDIUMINT) {
        int &v = (*(AttrBuffer<int> *)table->GetDisplayableAttrP(att)->buffer_)[_currentRNo];
        if (v == common::NULL_VALUE_32)
          dataTypes[att]->SetToNull();
        else
          ((types::RCNum *)dataTypes[att].get())->Assign(v, 0, false, attrt_tmp);
      } else if (attrt_tmp == common::ColumnType::SMALLINT) {
        short &v = (*(AttrBuffer<short> *)table->GetDisplayableAttrP(att)->buffer_)[_currentRNo];
        if (v == common::NULL_VALUE_SH)
          dataTypes[att]->SetToNull();
        else
          ((types::RCNum *)dataTypes[att].get())->Assign(v, 0, false, attrt_tmp);
      } else if (attrt_tmp == common::ColumnType::BYTEINT) {
        char &v = (*(AttrBuffer<char> *)table->GetDisplayableAttrP(att)->buffer_)[_currentRNo];
        if (v == common::NULL_VALUE_C)
          dataTypes[att]->SetToNull();
        else
          ((types::RCNum *)dataTypes[att].get())->Assign(v, 0, false, attrt_tmp);
      } else if (ATI::IsRealType(attrt_tmp)) {
        double &v = (*(AttrBuffer<double> *)table->GetDisplayableAttrP(att)->buffer_)[_currentRNo];
        if (v == NULL_VALUE_D)
          dataTypes[att]->SetToNull();
        else
          ((types::RCNum *)dataTypes[att].get())->Assign(v);
      } else if (attrt_tmp == common::ColumnType::NUM || attrt_tmp == common::ColumnType::BIGINT) {
        int64_t &v = (*(AttrBuffer<int64_t> *)table->GetDisplayableAttrP(att)->buffer_)[_currentRNo];
        if (v == common::NULL_VALUE_64)
          dataTypes[att]->SetToNull();
        else
          ((types::RCNum *)dataTypes[att].get())
              ->Assign(v, table->GetDisplayableAttrP(att)->Type().GetScale(), false, attrt_tmp);
      } else if (ATI::IsDateTimeType(attrt_tmp)) {
        int64_t &v = (*(AttrBuffer<int64_t> *)table->GetDisplayableAttrP(att)->buffer_)[_currentRNo];
        if (v == common::NULL_VALUE_64)
          dataTypes[att]->SetToNull();
        else
          ((types::RCDateTime *)dataTypes[att].get())->Assign(v, attrt_tmp);
      } else {
        ASSERT(ATI::IsStringType(attrt_tmp), "not all possible attr_types checked");
        (*(AttrBuffer<types::BString> *)table->GetDisplayableAttrP(att)->buffer_)
            .GetString(*(types::BString *)dataTypes[att].get(), _currentRNo);
      }
    }
  }
}

TempTable::RecordIterator &TempTable::RecordIterator::operator++() {
  DEBUG_ASSERT(_currentRNo < uint64_t(table->NumOfObj()));
  is_prepared = false;
  ++_currentRNo;
  return (*this);
}

TempTable::RecordIterator TempTable::begin(Transaction *conn) { return (RecordIterator(this, conn, 0)); }

TempTable::RecordIterator TempTable::end(Transaction *conn) { return (RecordIterator(this, conn, NumOfObj())); }

TempTable::RecordIterator::RecordIterator() : table(nullptr), _currentRNo(0), _conn(nullptr), is_prepared(false) {}

TempTable::RecordIterator::RecordIterator(TempTable *table_, Transaction *conn_, uint64_t rowNo_)
    : table(table_), _currentRNo(rowNo_), _conn(conn_), is_prepared(false) {
  DEBUG_ASSERT(table != 0);
  DEBUG_ASSERT(_currentRNo <= uint64_t(table->NumOfObj()));
  for (uint att = 0; att < table->NumOfDisplaybleAttrs(); att++) {
    common::ColumnType att_type = table->GetDisplayableAttrP(att)->TypeName();
    if (att_type == common::ColumnType::INT || att_type == common::ColumnType::MEDIUMINT ||
        att_type == common::ColumnType::SMALLINT || att_type == common::ColumnType::BYTEINT ||
        ATI::IsRealType(att_type) || att_type == common::ColumnType::NUM || att_type == common::ColumnType::BIGINT)
      dataTypes.emplace_back(new types::RCNum());
    else if (ATI::IsDateTimeType(att_type))
      dataTypes.emplace_back(new types::RCDateTime());
    else {
      ASSERT(ATI::IsStringType(att_type), "not all possible attr_types checked");
      dataTypes.emplace_back(new types::BString());
    }
  }
}

TempTable::RecordIterator::RecordIterator(RecordIterator const &it)
    : table(it.table), _currentRNo(it._currentRNo), _conn(it._conn), is_prepared(false) {
  for (uint att = 0; att < it.dataTypes.size(); att++)
    if (it.dataTypes[att])
      dataTypes.emplace_back(it.dataTypes[att]->Clone());
    else
      dataTypes.emplace_back(nullptr);
}

bool TempTable::RecordIterator::operator==(RecordIterator const &it) const {
  DEBUG_ASSERT((!(table || it.table)) || (table == it.table));
  return (_currentRNo == it._currentRNo);
}

bool TempTable::RecordIterator::operator!=(RecordIterator const &it) const {
  DEBUG_ASSERT((!(table || it.table)) || (table == it.table));
  return (_currentRNo != it._currentRNo);
}

TempTableForSubquery::~TempTableForSubquery() {
  if (num_of_global_virtual_columns_ != -1) {  // otherwise never evaluated
    for (uint i = num_of_global_virtual_columns_; i < virtual_columns_.size(); i++) {
      delete virtual_columns_[i];
      virtual_columns_[i] = nullptr;
    }

    virtual_columns_.resize(num_of_global_virtual_columns_);
    virtual_columns_ = template_virtual_columns_;
  }

  delete template_filter_;
  template_filter_ = nullptr;

  SetAttrsForExact();
  for (auto it : attrs_for_rough_) delete it;

  for (auto it : template_attrs_) delete it;
}

void TempTableForSubquery::ResetToTemplate(bool rough) {
  if (!template_filter_)
    return;

  for (uint i = num_of_global_virtual_columns_; i < virtual_columns_.size(); i++) delete virtual_columns_[i];

  virtual_columns_.resize(template_virtual_columns_.size());

  for (uint i = 0; i < template_virtual_columns_.size(); i++) virtual_columns_[i] = template_virtual_columns_[i];

  for (uint i = 0; i < attrs_.size(); i++) {
    void *orig_buf = (*attrs_[i]).buffer_;
    *attrs_[i] = *template_attrs_[i];
    (*attrs_[i]).buffer_ = orig_buf;
  }

  filter_ = std::move(*template_filter_);  // shallow
  filter_shallow_memory_ = true;

  for (int i = 0; i < num_of_global_virtual_columns_; i++)
    if (!virtual_columns_for_having_[i])
      virtual_columns_[i]->SetMultiIndex(filter_.mind);

  having_conds_ = template_having_conds_;
  order_by_ = template_order_by_;
  table_mode_ = template_mode_;
  num_of_obj_ = 0;

  if (rough)
    rough_materialized_ = false;
  else
    materialized_ = false;

  num_of_materialized_ = 0;
  rough_is_empty_ = common::Tribool();
}

void TempTableForSubquery::SetAttrsForRough() {
  if (!is_attr_for_rough_) {
    for (uint i = attrs_for_exact_.size(); i < attrs_.size(); i++) {
      delete attrs_[i];
      attrs_[i] = nullptr;
    }

    attrs_ = attrs_for_rough_;
    is_attr_for_rough_ = true;
  }
}

void TempTableForSubquery::SetAttrsForExact() {
  if (is_attr_for_rough_) {
    for (uint i = (int)attrs_for_rough_.size(); i < attrs_.size(); i++) {
      delete attrs_[i];
      attrs_[i] = nullptr;
    }

    attrs_ = attrs_for_exact_;
    is_attr_for_rough_ = false;
  }
}

void TempTableForSubquery::Materialize(bool in_subq, [[maybe_unused]] ResultSender *sender,
                                       [[maybe_unused]] bool lazy) {
  CreateTemplateIfNotExists();
  SetAttrsForExact();
  TempTable::Materialize(in_subq);
}

void TempTableForSubquery::RoughMaterialize(bool in_subq, [[maybe_unused]] ResultSender *sender,
                                            [[maybe_unused]] bool lazy) {
  CreateTemplateIfNotExists();
  SetAttrsForRough();

  if (rough_materialized_)
    return;

  TempTable::RoughMaterialize(in_subq);
  materialized_ = false;
  rough_materialized_ = true;
}

void TempTableForSubquery::CreateTemplateIfNotExists() {
  if (attrs_for_rough_.size() == 0) {
    attrs_for_exact_ = attrs_;
    is_attr_for_rough_ = false;

    for (auto attr : attrs_) attrs_for_rough_.push_back(new Attr(*attr));
  }

  if (!template_filter_ && IsParametrized()) {
    template_filter_ = new ParameterizedFilter(filter_);

    for (auto attr : attrs_) template_attrs_.push_back(new Attr(*attr));

    num_of_global_virtual_columns_ = int(virtual_columns_.size());
    template_having_conds_ = having_conds_;
    template_order_by_ = order_by_;
    template_mode_ = table_mode_;
    template_virtual_columns_ = virtual_columns_;
  }
}

}  // namespace core
}  // namespace Tianmu
