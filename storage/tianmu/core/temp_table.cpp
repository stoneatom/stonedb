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
  explicit AttrBuffer(uint page_size, uint elem_size = 0, Transaction *conn = nullptr)
      : CachedBuffer<T>(page_size, elem_size, conn) {}
  ~AttrBuffer() = default;

  void GetString(types::BString &s, int64_t idx) { CachedBuffer<T>::Get(s, idx); }
  // Warning: read only operator!!!
  T &operator[](int64_t idx) { return CachedBuffer<T>::Get(idx); }
  void Set(int64_t idx, T value) { CachedBuffer<T>::Set(idx, value); }
};

// here we stored data both signed/unsigned, the exact values will be converted on send results phase.
template class AttrBuffer<char>;
template class AttrBuffer<short>;
template class AttrBuffer<int>;
template class AttrBuffer<int64_t>;
template class AttrBuffer<double>;
template class AttrBuffer<types::BString>;

TempTable::Attr::Attr(const Attr &a) : PhysicalColumn(a) {
  mode = a.mode;
  distinct = a.distinct;
  no_materialized = a.no_materialized;
  term = a.term;
  if (term.vc)
    term.vc->ResetLocalStatistics();
  dim = a.dim;
  // DEBUG_ASSERT(a.buffer == nullptr); // otherwise we cannot copy Attr !
  buffer = nullptr;
  no_obj = a.no_obj;
  no_power = a.no_power;
  if (a.alias) {
    alias = new char[std::strlen(a.alias) + 1];
    std::strcpy(alias, a.alias);
  } else
    alias = nullptr;

  page_size = a.page_size;
  orig_precision = a.orig_precision;
  not_complete = a.not_complete;
  si = a.si;
}

TempTable::Attr::Attr(CQTerm t, common::ColOperation m, uint32_t power, bool dis, char *a, int dim,
                      common::ColumnType type, uint scale, uint no_digits, bool notnull, DTCollation collation, SI *si1)
    : mode(m), distinct(dis), term(t), dim(dim), not_complete(true) {
  ct.Initialize(type, notnull, common::PackFmt::DEFAULT, no_digits, scale, collation);
  orig_precision = no_digits;
  buffer = nullptr;
  no_obj = 0;
  no_power = power;
  no_materialized = 0;
  if (a) {
    alias = new char[std::strlen(a) + 1];
    std::strcpy(alias, a);
  } else
    alias = nullptr;
  if (m == common::ColOperation::GROUP_CONCAT)
    si = (*si1);
  else
    si.order = ORDER::ORDER_NOT_RELEVANT;

  page_size = 1;
}

TempTable::Attr::~Attr() {
  DeleteBuffer();
  delete[] alias;
}

TempTable::Attr &TempTable::Attr::operator=(const TempTable::Attr &a) {
  mode = a.mode;
  distinct = a.distinct;
  no_materialized = a.no_materialized;
  term = a.term;
  if (term.vc)
    term.vc->ResetLocalStatistics();
  dim = a.dim;
  // DEBUG_ASSERT(a.buffer == nullptr); // otherwise we cannot copy Attr !
  buffer = nullptr;
  no_obj = a.no_obj;
  no_power = a.no_power;
  delete[] alias;
  if (a.alias) {
    alias = new char[std::strlen(a.alias) + 1];
    std::strcpy(alias, a.alias);
  } else
    alias = nullptr;

  page_size = a.page_size;
  orig_precision = a.orig_precision;
  not_complete = a.not_complete;
  return *this;
}

int TempTable::Attr::operator==(const TempTable::Attr &sec) {
  return mode == sec.mode && distinct == sec.distinct && term == sec.term && Type() == sec.Type() && dim == sec.dim;
}

void TempTable::Attr::CreateBuffer(uint64_t size, Transaction *conn, bool not_c) {
  // do not create larger buffer than size
  not_complete = not_c;
  if (size < page_size)
    page_size = (uint)size;
  no_obj = size;
  switch (TypeName()) {
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      if (!buffer)
        buffer = new AttrBuffer<int>(page_size, sizeof(int), conn);
      break;
    case common::ColumnType::BYTEINT:
      if (!buffer)
        buffer = new AttrBuffer<char>(page_size, sizeof(char), conn);
      break;
    case common::ColumnType::SMALLINT:
      if (!buffer)
        buffer = new AttrBuffer<short>(page_size, sizeof(short), conn);
      break;
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
      DeleteBuffer();
      buffer = new AttrBuffer<types::BString>(page_size, Type().GetInternalSize(), conn);
      break;
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
    case common::ColumnType::BIT:
      if (!buffer)
        buffer = new AttrBuffer<int64_t>(page_size, sizeof(int64_t), conn);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      if (!buffer)
        buffer = new AttrBuffer<double>(page_size, sizeof(double), conn);
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
      term.vc->GetValueString(vals, mii);
      SetValueString(idx, vals);
      break;
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
      if (!term.vc->IsNull(mii))
        term.vc->GetNotNullValueString(vals, mii);
      else
        vals = types::BString();
      SetValueString(idx, vals);
      break;
    default:
      SetValueInt64(idx, term.vc->GetValueInt64(mii));
      break;
  }
}

size_t TempTable::Attr::FillValues(MIIterator &mii, size_t start, size_t count) {
  size_t n = 0;
  bool first_row_for_vc = true;
  while (mii.IsValid() && n < count) {
    if (mii.PackrowStarted() || first_row_for_vc) {
      term.vc->LockSourcePacks(mii);
      first_row_for_vc = false;
    }
    FillValue(mii, start + n);
    ++mii;
    ++n;
  };
  term.vc->UnlockSourcePacks();
  return n;
}

void TempTable::Attr::DeleteBuffer() {
  switch (TypeName()) {
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      delete (AttrBuffer<int> *)(buffer);
      break;
    case common::ColumnType::BYTEINT:
      delete (AttrBuffer<char> *)(buffer);
      break;
    case common::ColumnType::SMALLINT:
      delete (AttrBuffer<short> *)(buffer);
      break;
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
      delete (AttrBuffer<types::BString> *)(buffer);
      break;
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
    case common::ColumnType::BIT:
      delete (AttrBuffer<int64_t> *)(buffer);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      delete (AttrBuffer<double> *)(buffer);
      break;
    default:
      break;
  }
  buffer = nullptr;
  no_obj = 0;
}

// here we stored data both signed/unsigned, the exact values will be converted on send results phase.
void TempTable::Attr::SetValueInt64(int64_t obj, int64_t val) {
  no_materialized = obj + 1;
  no_obj = obj >= no_obj ? obj + 1 : no_obj;
  switch (TypeName()) {
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::BIT:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:  // 64-bit
      ((AttrBuffer<int64_t> *)buffer)->Set(obj, val);
      break;
    case common::ColumnType::INT:        // 32-bit
    case common::ColumnType::MEDIUMINT:  // 24-bit
      if (val == common::NULL_VALUE_64)
        ((AttrBuffer<int> *)buffer)->Set(obj, common::NULL_VALUE_32);
      else if (val == common::PLUS_INF_64)
        ((AttrBuffer<int> *)buffer)->Set(obj, std::numeric_limits<int>::max());
      else if (val == common::MINUS_INF_64)
        ((AttrBuffer<int> *)buffer)->Set(obj, TIANMU_INT_MIN);
      else
        ((AttrBuffer<int> *)buffer)->Set(obj, (int)val);
      break;
    case common::ColumnType::SMALLINT:  // 16-bit
      if (val == common::NULL_VALUE_64)
        ((AttrBuffer<short> *)buffer)->Set(obj, common::NULL_VALUE_SH);
      else if (val == common::PLUS_INF_64)
        ((AttrBuffer<short> *)buffer)->Set(obj, TIANMU_SMALLINT_MAX);
      else if (val == common::MINUS_INF_64)
        ((AttrBuffer<short> *)buffer)->Set(obj, TIANMU_SMALLINT_MIN);
      else
        ((AttrBuffer<short> *)buffer)->Set(obj, (short)val);
      break;
    case common::ColumnType::BYTEINT:  // 8-bit
      if (val == common::NULL_VALUE_64)
        ((AttrBuffer<char> *)buffer)->Set(obj, common::NULL_VALUE_C);
      else if (val == common::PLUS_INF_64)
        ((AttrBuffer<char> *)buffer)->Set(obj, TIANMU_TINYINT_MAX);
      else if (val == common::MINUS_INF_64)
        ((AttrBuffer<char> *)buffer)->Set(obj, TIANMU_TINYINT_MIN);
      else
        ((AttrBuffer<char> *)buffer)->Set(obj, (char)val);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      if (val == common::NULL_VALUE_64)
        ((AttrBuffer<double> *)buffer)->Set(obj, NULL_VALUE_D);
      else if (val == common::PLUS_INF_64)
        ((AttrBuffer<double> *)buffer)->Set(obj, common::PLUS_INF_DBL);
      else if (val == common::MINUS_INF_64)
        ((AttrBuffer<double> *)buffer)->Set(obj, common::MINUS_INF_DBL);
      else
        ((AttrBuffer<double> *)buffer)->Set(obj, *(double *)&val);
      break;
    default:
      DEBUG_ASSERT(0);
      break;
  }
}

void TempTable::Attr::InvalidateRow([[maybe_unused]] int64_t obj) {
  DEBUG_ASSERT(obj + 1 == no_materialized);
  no_obj--;
  no_materialized--;
}

void TempTable::Attr::SetNull(int64_t obj) {
  no_materialized = obj + 1;
  no_obj = obj >= no_obj ? obj + 1 : no_obj;
  switch (TypeName()) {
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
    case common::ColumnType::BIT:
      ((AttrBuffer<int64_t> *)buffer)->Set(obj, common::NULL_VALUE_64);
      break;
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      ((AttrBuffer<int> *)buffer)->Set(obj, common::NULL_VALUE_32);
      break;
    case common::ColumnType::SMALLINT:
      ((AttrBuffer<short> *)buffer)->Set(obj, common::NULL_VALUE_SH);
      break;
    case common::ColumnType::BYTEINT:
      ((AttrBuffer<char> *)buffer)->Set(obj, common::NULL_VALUE_C);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      ((AttrBuffer<double> *)buffer)->Set(obj, NULL_VALUE_D);
      break;
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::LONGTEXT:
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
      ((AttrBuffer<types::BString> *)buffer)->Set(obj, types::BString());
      break;
    default:
      DEBUG_ASSERT(0);
      break;
  }
}

void TempTable::Attr::SetMinusInf(int64_t obj) { SetValueInt64(obj, common::MINUS_INF_64); }

void TempTable::Attr::SetPlusInf(int64_t obj) { SetValueInt64(obj, common::PLUS_INF_64); }

void TempTable::Attr::SetValueString(int64_t obj, const types::BString &val) {
  no_materialized = obj + 1;
  no_obj = obj >= no_obj ? obj + 1 : no_obj;
  int64_t val64 = 0;
  double valD = 0.0;

  switch (TypeName()) {
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      ((AttrBuffer<int> *)buffer)->Set(obj, std::atoi(val.GetDataBytesPointer()));
      break;
    case common::ColumnType::BYTEINT:
      ((AttrBuffer<char> *)buffer)->Set(obj, val.GetDataBytesPointer()[0]);
      break;
    case common::ColumnType::SMALLINT:
      ((AttrBuffer<short> *)buffer)->Set(obj, (short)std::atoi(val.GetDataBytesPointer()));
      break;
    case common::ColumnType::BIGINT:
      ((AttrBuffer<int64_t> *)buffer)->Set(obj, std::strtoll(val.GetDataBytesPointer(), nullptr, 10));
      break;
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::LONGTEXT:
      ((AttrBuffer<types::BString> *)buffer)->Set(obj, val);
      break;
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
    case common::ColumnType::BIT:
      val64 = *(int64_t *)(val.GetDataBytesPointer());
      ((AttrBuffer<int64_t> *)buffer)->Set(obj, val64);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      if (!val.IsNullOrEmpty())
        valD = *(double *)(val.GetDataBytesPointer());
      else
        valD = NULL_VALUE_D;
      ((AttrBuffer<double> *)buffer)->Set(obj, valD);
      break;
    default:
      break;
  }
}

types::TianmuValueObject TempTable::Attr::GetValue(int64_t obj, [[maybe_unused]] bool lookup_to_num) {
  if (obj == common::NULL_VALUE_64)
    return types::TianmuValueObject();
  types::TianmuValueObject ret;
  if (ATI::IsStringType(TypeName())) {
    types::BString s;
    GetValueString(s, obj);
    ret = s;
  } else if (ATI::IsIntegerType(TypeName()))
    ret = types::TianmuNum(GetValueInt64(obj), 0, false, common::ColumnType::NUM);
  else if (ATI::IsDateTimeType(TypeName()))
    ret = types::TianmuDateTime(this->GetValueInt64(obj), TypeName() /*, precision*/);
  else if (ATI::IsRealType(TypeName()))
    ret = types::TianmuNum(this->GetValueInt64(obj), 0, true);
  else if (TypeName() == common::ColumnType::NUM)
    ret = types::TianmuNum((int64_t)GetValueInt64(obj), Type().GetScale());
  else if (TypeName() == common::ColumnType::BIT)
    ret = types::TianmuNum((int64_t)GetValueInt64(obj), Type().GetScale(), false,
                           TypeName());  // TODO(check prec & scale)
  return ret;
}

void TempTable::Attr::GetValueString(types::BString &value, int64_t obj) {
  if (obj == common::NULL_VALUE_64) {
    value = types::BString();
    return;
  }

  switch (TypeName()) {
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::LONGTEXT:
      (*(AttrBuffer<types::BString> *)buffer).GetString(value, obj);
      break;
    case common::ColumnType::BYTEINT: {
      types::TianmuNum tianmu_n(static_cast<int64_t>((*(AttrBuffer<char> *)buffer)[obj]), -1, false, TypeName());
      value = tianmu_n.GetValueInt64() == common::NULL_VALUE_64 ? types::BString() : tianmu_n.ToBString();
      break;
    }
    case common::ColumnType::SMALLINT: {
      types::TianmuNum tianmu_n(static_cast<int64_t>((*(AttrBuffer<short> *)buffer)[obj]), -1, false, TypeName());
      value = tianmu_n.GetValueInt64() == common::NULL_VALUE_64 ? types::BString() : tianmu_n.ToBString();
      break;
    }
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT: {
      types::TianmuNum tianmu_n(static_cast<int>((*(AttrBuffer<int> *)buffer)[obj]), -1, false, TypeName());
      value = tianmu_n.ValueInt() == common::NULL_VALUE_32 ? types::BString() : tianmu_n.ToBString();
      break;
    }
    case common::ColumnType::BIGINT: {
      types::TianmuNum tianmu_n(static_cast<int64_t>((*(AttrBuffer<int64_t> *)buffer)[obj]), -1, false, TypeName());
      value = tianmu_n.GetValueInt64() == common::NULL_VALUE_64 ? types::BString() : tianmu_n.ToBString();
      break;
    }
    case common::ColumnType::NUM: {
      types::TianmuNum tianmu_n((*(AttrBuffer<int64_t> *)buffer)[obj], Type().GetScale());
      value = tianmu_n.GetValueInt64() == common::NULL_VALUE_64 ? types::BString() : tianmu_n.ToBString();
      break;
    }
    case common::ColumnType::BIT: {
      types::TianmuNum tianmu_n((*(AttrBuffer<int64_t> *)buffer)[obj], Type().GetScale(), false, TypeName());
      value = tianmu_n.GetValueInt64() == common::NULL_VALUE_64 ? types::BString() : tianmu_n.ToBString();
      break;
    }
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP: {
      types::TianmuDateTime tianmu_dt((*(AttrBuffer<int64_t> *)buffer)[obj], TypeName());
      if (TypeName() == common::ColumnType::TIMESTAMP) {
        types::TianmuDateTime::AdjustTimezone(tianmu_dt);
      }
      if (tianmu_dt.GetInt64() == common::NULL_VALUE_64) {
        value = types::BString();
      } else {
        const types::BString &tianmu_dt_str = tianmu_dt.ToBString();
        size_t len = tianmu_dt_str.len_ > Type().GetPrecision() ? Type().GetPrecision() : tianmu_dt_str.len_;
        value = types::BString(tianmu_dt_str.GetDataBytesPointer(), len, tianmu_dt_str.IsPersistent());
      }
      break;
    }
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT: {
      double *d_p = &(*(AttrBuffer<double> *)buffer)[obj];
      types::TianmuNum tianmu_n(static_cast<int64_t>(*d_p), 0, true, TypeName());
      value = tianmu_n.GetValueInt64() == common::NULL_VALUE_64 ? types::BString() : tianmu_n.ToBString();
      break;
    }
    default:
      break;
  }
}

int64_t TempTable::Attr::GetSum(int pack, bool &nonnegative) {
  DEBUG_ASSERT(ATI::IsNumericType(ct.GetTypeName()));
  int64_t start = pack * (1 << no_power);
  int64_t stop = (1 << no_power);
  stop = stop > no_obj ? no_obj : stop;
  if (not_complete || no_materialized < stop)
    return common::NULL_VALUE_64;
  int64_t sum = 0, val;
  double sum_d = 0.0;
  bool not_null_exists = false;
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
  if (not_complete)
    return common::NULL_VALUE_64;
  int64_t start;
  int64_t stop;
  if (pack == -1) {
    start = 0;
    stop = no_obj;
  } else {
    start = pack * (1 << no_power);
    stop = (pack + 1) * (1 << no_power);
    stop = stop > no_obj ? no_obj : stop;
  }
  if (no_materialized < stop)
    return common::NULL_VALUE_64;
  int64_t no_nulls = 0;
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
  if (not_complete)
    return common::PLUS_INF_64;
  int64_t start = (1 << no_power);
  int64_t stop = (pack + 1) * (1 << no_power);
  stop = stop > no_obj ? no_obj : stop;
  if (no_materialized < stop)
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
  if (not_complete)
    return common::MINUS_INF_64;
  int64_t start = pack * (1 << no_power);
  int64_t stop = (pack + 1) * (1 << no_power);
  stop = stop > no_obj ? no_obj : stop;
  if (no_materialized < stop)
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
    case common::ColumnType::BIT:
      if (!IsNull(obj))
        res = (*(AttrBuffer<int64_t> *)buffer)[obj];
      break;
    case common::ColumnType::INT:
    case common::ColumnType::MEDIUMINT:
      if (!IsNull(obj))
        res = (*(AttrBuffer<int> *)buffer)[obj];
      break;
    case common::ColumnType::SMALLINT:
      if (!IsNull(obj))
        res = (*(AttrBuffer<short> *)buffer)[obj];
      break;
    case common::ColumnType::BYTEINT:
      if (!IsNull(obj))
        res = (*(AttrBuffer<char> *)buffer)[obj];
      break;
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
      DEBUG_ASSERT(0);
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      if (!IsNull(obj))
        res = *(int64_t *)&(*(AttrBuffer<double> *)buffer)[obj];
      else
        res = *(int64_t *)&NULL_VALUE_D;
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
      res = (*(AttrBuffer<int> *)buffer)[obj];
      break;
    case common::ColumnType::BYTEINT:
      res = (*(AttrBuffer<char> *)buffer)[obj];
      break;
    case common::ColumnType::SMALLINT:
      res = (*(AttrBuffer<short> *)buffer)[obj];
      break;
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
    case common::ColumnType::BIT:
      res = (*(AttrBuffer<int64_t> *)buffer)[obj];
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT:
      res = *(int64_t *)&(*(AttrBuffer<double> *)buffer)[obj];
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
      res = (*(AttrBuffer<int> *)buffer)[obj] == common::NULL_VALUE_32;
      break;
    case common::ColumnType::BYTEINT:
      res = (*(AttrBuffer<char> *)buffer)[obj] == common::NULL_VALUE_C;
      break;
    case common::ColumnType::SMALLINT:
      res = (*(AttrBuffer<short> *)buffer)[obj] == common::NULL_VALUE_SH;
      break;
    case common::ColumnType::BIN:
    case common::ColumnType::BYTE:
    case common::ColumnType::VARBYTE:
    case common::ColumnType::STRING:
    case common::ColumnType::VARCHAR:
    case common::ColumnType::LONGTEXT:
      res = (*(AttrBuffer<types::BString> *)buffer)[obj].IsNull();
      break;
    case common::ColumnType::BIGINT:
    case common::ColumnType::NUM:
    case common::ColumnType::YEAR:
    case common::ColumnType::TIME:
    case common::ColumnType::DATE:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
    case common::ColumnType::BIT:
      res = (*(AttrBuffer<int64_t> *)buffer)[obj] == common::NULL_VALUE_64;
      break;
    case common::ColumnType::REAL:
    case common::ColumnType::FLOAT: {
      common::double_int_t v((*(AttrBuffer<double> *)buffer)[obj]);
      res = v.i == common::NULL_VALUE_64;
      break;
    }
    default:
      break;
  }
  return res;
}

void TempTable::Attr::ApplyFilter(MultiIndex &mind_, int64_t offset, int64_t last_index) {
  DEBUG_ASSERT(mind_.NumOfDimensions() == 1);

  if (mind_.NumOfDimensions() != 1)
    throw common::NotImplementedException("MultiIndex has too many dimensions.");
  if (mind_.ZeroTuples() || no_obj == 0 || offset >= mind_.NumOfTuples()) {
    DeleteBuffer();
    return;
  }

  if (last_index > mind_.NumOfTuples())
    last_index = mind_.NumOfTuples();

  void *old_buffer = buffer;
  buffer = nullptr;
  CreateBuffer(last_index - offset, mind_.m_conn);

  MIIterator mit(&mind_, mind_.ValueOfPower());
  for (int64_t i = 0; i < offset; i++) ++mit;
  uint64_t idx = 0;
  for (int64_t i = 0; i < last_index - offset; i++, ++mit) {
    idx = mit[0];
    DEBUG_ASSERT(idx != static_cast<uint64_t>(common::NULL_VALUE_64));  // null object should never appear
                                                                        // in a materialized temp. table
    switch (TypeName()) {
      case common::ColumnType::INT:
      case common::ColumnType::MEDIUMINT:
        ((AttrBuffer<int> *)buffer)->Set(i, (*(AttrBuffer<int> *)old_buffer)[idx]);
        break;
      case common::ColumnType::BYTEINT:
        ((AttrBuffer<char> *)buffer)->Set(i, (*(AttrBuffer<char> *)old_buffer)[idx]);
        break;
      case common::ColumnType::SMALLINT:
        ((AttrBuffer<short> *)buffer)->Set(i, (*(AttrBuffer<short> *)old_buffer)[idx]);
        break;
      case common::ColumnType::STRING:
      case common::ColumnType::VARCHAR:
      case common::ColumnType::BIN:
      case common::ColumnType::BYTE:
      case common::ColumnType::VARBYTE:
      case common::ColumnType::LONGTEXT:
        ((AttrBuffer<types::BString> *)buffer)->Set(i, (*(AttrBuffer<types::BString> *)old_buffer)[idx]);
        break;
      case common::ColumnType::BIGINT:
      case common::ColumnType::NUM:
      case common::ColumnType::YEAR:
      case common::ColumnType::TIME:
      case common::ColumnType::DATE:
      case common::ColumnType::DATETIME:
      case common::ColumnType::TIMESTAMP:
      case common::ColumnType::BIT:
        ((AttrBuffer<int64_t> *)buffer)->Set(i, (*(AttrBuffer<int64_t> *)old_buffer)[idx]);
        break;
      case common::ColumnType::REAL:
      case common::ColumnType::FLOAT:
        ((AttrBuffer<double> *)buffer)->Set(i, (*(AttrBuffer<double> *)old_buffer)[idx]);
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
    case common::ColumnType::BIT:
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

  return no_obj;
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
  if (buffer) {
    switch (TypeName()) {
      case common::ColumnType::INT:
      case common::ColumnType::MEDIUMINT:
        static_cast<CachedBuffer<int> *>(buffer)->SetNewPageSize(new_page_size);
        break;
      case common::ColumnType::BYTEINT:
        static_cast<CachedBuffer<char> *>(buffer)->SetNewPageSize(new_page_size);
        break;
      case common::ColumnType::SMALLINT:
        static_cast<CachedBuffer<short> *>(buffer)->SetNewPageSize(new_page_size);
        break;
      case common::ColumnType::STRING:
      case common::ColumnType::VARCHAR:
      case common::ColumnType::BIN:
      case common::ColumnType::BYTE:
      case common::ColumnType::VARBYTE:
      case common::ColumnType::LONGTEXT:
        static_cast<CachedBuffer<types::BString> *>(buffer)->SetNewPageSize(new_page_size);
        break;
      case common::ColumnType::BIGINT:
      case common::ColumnType::NUM:
      case common::ColumnType::YEAR:
      case common::ColumnType::TIME:
      case common::ColumnType::DATE:
      case common::ColumnType::DATETIME:
      case common::ColumnType::TIMESTAMP:
        static_cast<CachedBuffer<int64_t> *>(buffer)->SetNewPageSize(new_page_size);
        break;
      case common::ColumnType::REAL:
      case common::ColumnType::FLOAT:
        static_cast<CachedBuffer<double> *>(buffer)->SetNewPageSize(new_page_size);
        break;
      default:
        ASSERT(0, "Attr::SetNewPageSize: unrecognized type");
        break;
    }
  }
  page_size = new_page_size;
}

TempTable::TempTable(const TempTable &t, bool is_vc_owner)
    : filter(t.filter), output_mind(t.output_mind), is_vc_owner(is_vc_owner), m_conn(t.m_conn) {
  no_obj = t.no_obj;
  materialized = t.materialized;
  aliases = t.aliases;
  group_by = t.group_by;
  no_cols = t.no_cols;
  mode = t.mode;
  virt_cols = t.virt_cols;
  virt_cols_for_having = t.virt_cols_for_having;
  having_conds = t.having_conds;
  tables = t.tables;
  join_types = t.join_types;
  order_by = t.order_by;
  has_temp_table = t.has_temp_table;
  lazy = t.lazy;
  force_full_materialize = t.force_full_materialize;
  no_materialized = t.no_materialized;
  no_global_virt_cols = int(t.virt_cols.size());
  for (uint i = 0; i < t.attrs.size(); i++) {
    attrs.push_back(new Attr(*t.attrs[i]));
  }
  is_sent = t.is_sent;
  mem_scale = t.mem_scale;
  rough_is_empty = t.rough_is_empty;
  p_power = t.p_power;
  size_of_one_record = 0;
}

TempTable::~TempTable() {
  MEASURE_FET("TempTable::~TempTable(...)");
  // remove all temporary tables used by *this
  // but only if *this should manage those tables
  if (is_vc_owner) {
    for (uint i = 0; i < virt_cols.size(); i++) delete virt_cols[i];
  } else {
    TranslateBackVCs();
    for (uint i = no_global_virt_cols; i < virt_cols.size(); i++) delete virt_cols[i];
  }

  for (uint i = 0; i < attrs.size(); i++) {
    delete attrs[i];
  }
}

void TempTable::TranslateBackVCs() {
  for (int i = 0; i < no_global_virt_cols; i++)
    if (virt_cols[i] && static_cast<int>(virt_cols[i]->IsSingleColumn()))
      static_cast<vcolumn::SingleColumn *>(virt_cols[i])->TranslateSourceColumns(attr_back_translation);
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
  for (uint i = 0; i < attrs.size(); i++) {
    copy_buf.push_back(attrs[i]->buffer);
    attrs[i]->buffer = nullptr;
  }
  if (no_global_virt_cols != -1) {
    // this is a TempTable copy
    for (uint i = no_global_virt_cols; i < virt_cols.size(); i++) {
      delete virt_cols[i];
      virt_cols[i] = nullptr;
    }
    virt_cols.resize(no_global_virt_cols);
  }
  std::shared_ptr<TempTable> working_copy = Create(*this, in_subq);  // Original VCs of this will be copied to
                                                                     // working_copy, and then deleted in its
                                                                     // destructor
  for (uint i = 0; i < attrs.size(); i++) {
    attrs[i]->mode = common::ColOperation::LISTING;
  }
  working_copy->mode = TableMode();  // reset all flags (distinct, limit) on copy
  // working_copy->for_subq = this->for_subq;
  // working_copy->no_global_virt_cols = this->no_global_virt_cols;
  for (uint i = 0; i < attrs.size(); i++) {
    working_copy->attrs[i]->buffer = copy_buf[i];  // copy the data buffers
    attrs[i]->no_obj = 0;
    attr_translation[attrs[i]] = working_copy->attrs[i];
  }
  // VirtualColumns are copied, and we should replace them by references to the
  // temporary source
  delete filter.mind_;
  filter.mind_ = new MultiIndex(p_power);
  filter.mind_->AddDimension_cross(no_obj);
  if (virt_cols.size() < attrs.size())
    virt_cols.resize(attrs.size());
  fill(virt_cols.begin(), virt_cols.end(), (vcolumn::VirtualColumn *)nullptr);
  for (uint i = 0; i < attrs.size(); i++) {
    vcolumn::VirtualColumn *new_vc =
        new vcolumn::SingleColumn(working_copy->attrs[i], filter.mind_, 0, 0, working_copy.get(), 0);
    virt_cols[i] = new_vc;
    attrs[i]->term.vc = new_vc;
    attrs[i]->dim = 0;
  }
  if (translate_order) {
    order_by.clear();  // translation needed: virt_cols should point to
                       // working_copy as a data source
    for (uint i = 0; i < working_copy->virt_cols.size(); i++) {
      vcolumn::VirtualColumn *orig_vc = working_copy->virt_cols[i];
      // if(in_subq && orig_vc->IsSingleColumn())
      //	working_copy->virt_cols[i] =
      // CreateVCCopy(working_copy->virt_cols[i]);
      for (uint j = 0; j < working_copy->order_by.size(); j++) {
        if (working_copy->order_by[j].vc == orig_vc) {
          working_copy->order_by[j].vc = working_copy->virt_cols[i];
        }
      }
      // TODO: redesign it for more universal solution
      vcolumn::VirtualColumn *vc = working_copy->virt_cols[i];
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
    for (uint i = 0; i < working_copy->order_by.size(); i++) {
      SortDescriptor sord;
      sord.vc = working_copy->order_by[i].vc;
      sord.dir = working_copy->order_by[i].dir;
      order_by.push_back(sord);
    }
  } else {  // Create() did move vc's used in order_by into working_copy, so we
            // should bring them back
    for (uint i = 0; i < order_by.size(); i++) {
      // find which working_copy->vc is used in ordering
      for (uint j = 0; j < working_copy->virt_cols.size(); j++) {
        if (/*working_copy->*/ order_by[i].vc == working_copy->virt_cols[j]) {
          // order_by[i].vc = working_copy->virt_cols[j];
          MoveVC(j, working_copy->virt_cols,
                 virt_cols);  // moving vc - now it is back in this
          break;
        }
      }
    }
  }
  no_global_virt_cols = (int)virt_cols.size();
  return working_copy;  // must be deleted by DeleteMaterializedCopy()
}

void TempTable::DeleteMaterializedCopy(std::shared_ptr<TempTable> &old_t)  // delete the external table and remove
                                                                           // VC pointers, make this fully
                                                                           // materialized
{
  MEASURE_FET("TempTable::DeleteMaterializedCopy(...)");
  for (uint i = 0; i < attrs.size(); i++) {  // Make sure VCs are deleted before the source table is deleted
    attrs[i]->term.vc = nullptr;
    delete virt_cols[i];
    virt_cols[i] = nullptr;
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
  DEBUG_ASSERT((no == 0 && virt_cols.size() == 0) || static_cast<size_t>(no) > virt_cols.size());
  no_global_virt_cols = -1;  // usable value only in TempTable copies and in subq
  virt_cols.resize(no);
  virt_cols_for_having.resize(no);
}

void TempTable::CreateDisplayableAttrP() {
  if (attrs.size() == 0)
    return;
  displayable_attr.clear();
  displayable_attr.resize(attrs.size());
  int idx = 0;
  for (size_t i = 0; i < attrs.size(); i++) {
    if (attrs[i]->alias) {
      displayable_attr[idx] = attrs[i];
      idx++;
    }
  }
  for (uint i = idx; i < attrs.size(); i++) displayable_attr[i] = nullptr;
}

uint TempTable::GetDisplayableAttrIndex(uint attr) {
  uint idx = -1;
  uint i;
  for (i = 0; i < attrs.size(); i++)
    if (attrs[i]->alias) {
      idx++;
      if (idx == attr)
        break;
    }
  DEBUG_ASSERT(i < attrs.size());
  return i;
}

void TempTable::AddConds(Condition *cond, CondType type) {
  MEASURE_FET("TempTable::AddConds(...)");
  switch (type) {
    case CondType::WHERE_COND: {
      // need to add one by one since where_conds can be non-empty
      filter.AddConditions(cond, CondType::WHERE_COND);
      break;
    }

    case CondType::HAVING_COND: {
      Descriptor &desc = cond->operator[](0);
      DEBUG_ASSERT(desc.tree);
      having_conds.AddDescriptor(desc.tree, this, desc.left_dims.Size());
      break;
    }

    default:
      DEBUG_ASSERT(0);
  }
}

void TempTable::AddInnerConds(Condition *cond, std::vector<TabID> &dim_aliases) {
  for (uint i = 0; i < cond->Size(); i++)
    for (uint j = 0; j < dim_aliases.size(); j++) (*cond)[i].left_dims[GetDimension(dim_aliases[j])] = true;
  filter.AddConditions(cond, CondType::ON_INNER_FILTER);
}

void TempTable::AddLeftConds(Condition *cond, std::vector<TabID> &left_aliases, std::vector<TabID> &right_aliases) {
  for (uint i = 0; i < cond->Size(); i++) {
    for (int d = 0; d < (*cond)[i].left_dims.Size(); d++) (*cond)[i].left_dims[d] = (*cond)[i].right_dims[d] = false;
    for (uint j = 0; j < left_aliases.size(); j++) (*cond)[i].left_dims[GetDimension(left_aliases[j])] = true;
    for (uint j = 0; j < right_aliases.size(); j++) (*cond)[i].right_dims[GetDimension(right_aliases[j])] = true;
  }
  filter.AddConditions(cond, CondType::ON_LEFT_FILTER);
}

void TempTable::SetMode(TMParameter mode, int64_t mode_param1, int64_t mode_param2) {
  switch (mode) {
    case TMParameter::TM_DISTINCT:
      this->mode.distinct = true;
      break;
    case TMParameter::TM_TOP:
      this->mode.top = true;
      this->mode.param1 = mode_param1;  // offset
      this->mode.param2 = mode_param2;  // limit
      break;
    case TMParameter::TM_EXISTS:
      this->mode.exists = true;
      break;
    default:
      DEBUG_ASSERT(false);
      break;
  }
}

int TempTable::AddColumn(CQTerm e, common::ColOperation mode, char *alias, bool distinct, SI si) {
  if (alias)
    no_cols++;
  common::ColumnType type = common::ColumnType::UNK;  // type of column
  bool notnull = false;                               // NULLs are allowed
  uint scale = 0;                                     // number of decimal places
  uint precision = 0;                                 // number of all places

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
          case common::ColumnType::BIT:
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
      } else if (mode == common::ColOperation::LISTING || mode == common::ColOperation::GROUP_BY) {
        if (mode == common::ColOperation::GROUP_BY)
          group_by = true;
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

  attrs.push_back(new Attr(e, mode, p_power, distinct, alias, -2, type, scale, precision, notnull,
                           e.vc ? e.vc->GetCollation() : DTCollation(), &si));
  return -(int)attrs.size();  // attributes are counted from -1, -2, ...
}

void TempTable::AddOrder(vcolumn::VirtualColumn *vc, int direction) {
  // don't sort by const expressions
  if (vc->IsConst())
    return;
  SortDescriptor d;
  d.vc = vc;
  d.dir = direction;
  bool already_added = false;
  for (uint j = 0; j < order_by.size(); j++) {
    if (order_by[j] == d) {
      already_added = true;
      break;
    }
  }
  if (!already_added)
    order_by.push_back(d);
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
  tianmu_control_.lock(m_conn->GetThreadID()) << "UNION: materializing components." << system::unlock;
  this->Materialize();
  t->Materialize();
  if ((!t->NumOfObj() && all) || (!this->NumOfObj() && !t->NumOfObj()))  // no objects = no union
    return;

  Filter first_f(NumOfObj(), p_power), first_mask(NumOfObj(),
                                                  p_power);  // mask of objects to be added to the final result set
  Filter sec_f(t->NumOfObj(), p_power), sec_mask(t->NumOfObj(), p_power);
  first_mask.Set();
  sec_mask.Set();
  if (!all) {
    tianmu_control_.lock(m_conn->GetThreadID()) << "UNION: excluding repetitions." << system::unlock;
    Filter first_f(NumOfObj(), p_power);
    first_f.Set();
    Filter sec_f(t->NumOfObj(), p_power);
    sec_f.Set();
    GroupDistinctTable dist_table(p_power);
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
            vc_ptr_t(new vcolumn::SingleColumn(GetDisplayableAttrP(i), &output_mind, 0, -i - 1, this, 0)));
        sec_vcs.push_back(
            vc_ptr_t(new vcolumn::SingleColumn(t->GetDisplayableAttrP(i), t->GetOutputMultiIndexP(), 1, -i - 1, t, 0)));
        encoder.push_back(ColumnBinEncoder());
        bool encoder_created;
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
      dist_table.InitializeB(size, NumOfObj() + t->NumOfObj() / 2);  // optimization assumption: a
                                                                     // half of values in the second
                                                                     // table will be repetitions
      MIIterator first_mit(&output_mind, p_power);
      MIIterator sec_mit(t->GetOutputMultiIndexP(), p_power);
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
            if (res != GDTResult::GDT_FULL)  // note: if v is omitted here, it will also be
                                             // omitted in sec!
              first_f.ResetDelayed(pos);
          }
          ++first_mit;
          if (m_conn->Killed())
            throw common::KilledException();
        }
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
          if (m_conn->Killed())
            throw common::KilledException();
        }
        first_f.Commit();
        sec_f.Commit();
        first_mask.Commit();
        sec_mask.Commit();
      } while (!first_f.IsEmpty() || !sec_f.IsEmpty());
    }
    delete[] input_buf;
  }
  int64_t first_no_obj = first_mask.NumOfOnes();
  int64_t sec_no_obj = sec_mask.NumOfOnes();
  int64_t new_no_obj = first_no_obj + sec_no_obj;
  tianmu_control_.lock(m_conn->GetThreadID())
      << "UNION: generating result (" << new_no_obj << " rows)." << system::unlock;
  uint new_page_size = CalculatePageSize(new_no_obj);
  for (uint i = 0; i < NumOfDisplaybleAttrs(); i++) {
    Attr *first_attr = GetDisplayableAttrP(i);
    Attr *sec_attr = t->GetDisplayableAttrP(i);
    ColumnType new_type = GetUnionType(first_attr->Type(), sec_attr->Type());
    if (first_attr->Type() == sec_attr->Type() && first_mask.IsFull() && first_no_obj && sec_no_obj &&
        first_attr->Type().GetPrecision() >= sec_attr->Type().GetPrecision()) {
      // fast path of execution: do not modify first buffer
      SetPageSize(new_page_size);
      FilterOnesIterator sec_fi(&sec_mask, p_power);
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
    Attr *new_attr =
        new Attr(CQTerm(), common::ColOperation::LISTING, p_power, false, first_attr->alias, 0, new_type.GetTypeName(),
                 new_type.GetScale(), new_type.GetPrecision(), new_type.NotNull(), new_type.GetCollation());
    new_attr->page_size = new_page_size;
    new_attr->CreateBuffer(new_no_obj, m_conn);
    if (first_attr->TypeName() == common::ColumnType::NUM && sec_attr->TypeName() == common::ColumnType::NUM &&
        first_attr->Type().GetScale() != sec_attr->Type().GetScale()) {
      uint max_scale = new_attr->Type().GetScale();
      // copy attr from first table to new_attr
      double multiplier = types::PowOfTen(max_scale - first_attr->Type().GetScale());
      FilterOnesIterator first_fi(&first_mask, p_power);
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
      FilterOnesIterator sec_fi(&sec_mask, p_power);
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
      FilterOnesIterator first_fi(&first_mask, p_power);
      for (int64_t j = 0; j < first_no_obj; j++) {
        first_attr->GetValueString(s, *first_fi);
        new_attr->SetValueString(j, s);
        ++first_fi;
      }
      FilterOnesIterator sec_fi(&sec_mask, p_power);
      for (int64_t j = 0; j < sec_no_obj; j++) {
        sec_attr->GetValueString(s, *sec_fi);
        new_attr->SetValueString(first_no_obj + j, s);
        ++sec_fi;
      }
    } else if (new_attr->Type().IsFloat()) {
      FilterOnesIterator first_fi(&first_mask, p_power);
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
      FilterOnesIterator sec_fi(&sec_mask, p_power);
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
      FilterOnesIterator first_fi(&first_mask, p_power);
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
      FilterOnesIterator sec_fi(&sec_mask, p_power);
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
    attrs[GetDisplayableAttrIndex(i)] = new_attr;
    displayable_attr[i] = new_attr;
    delete first_attr;
  }
  SetNumOfMaterialized(new_no_obj);
  // this->no_obj = new_no_obj;
  // this->Display();
  output_mind.Clear();
  output_mind.AddDimension_cross(no_obj);
}

void TempTable::Union(TempTable *t, [[maybe_unused]] int all, ResultSender *sender, int64_t &g_offset,
                      int64_t &g_limit) {
  MEASURE_FET("TempTable::UnionSender(...)");

  DEBUG_ASSERT(NumOfDisplaybleAttrs() == t->NumOfDisplaybleAttrs());
  if (NumOfDisplaybleAttrs() != t->NumOfDisplaybleAttrs())
    throw common::NotImplementedException("UNION of tables with different number of columns.");
  if (this->IsParametrized() || t->IsParametrized())
    throw common::NotImplementedException("Materialize: not implemented union of parameterized queries.");

  if (mode.top) {
    if (g_offset != 0 || g_limit != -1) {
      mode.param2 = std::min(g_offset + g_limit, mode.param2);
    }
  } else if (g_offset != 0 || g_limit != -1) {
    mode.top = true;
    mode.param1 = 0;
    mode.param2 = g_limit + g_offset;
  }

  if (g_offset != 0 || g_limit != -1)
    sender->SetLimits(&g_offset, &g_limit);
  this->Materialize(false, sender);
  if (this->IsMaterialized() && !this->IsSent())
    sender->Send(this);

  if (t->mode.top) {
    if (g_offset != 0 || g_limit != -1) {
      t->mode.param2 = std::min(g_offset + g_limit, t->mode.param2);
    }
  } else if (g_offset != 0 || g_limit != -1) {
    t->mode.top = true;
    t->mode.param1 = 0;
    t->mode.param2 = g_limit + g_offset;
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
  out << "No obj.:" << no_obj << ", No attrs.:" << this->NumOfDisplaybleAttrs() << system::endl
      << "-----------" << system::endl;
  for (int64_t i = 0; i < no_obj; i++) {
    for (uint j = 0; j < this->NumOfDisplaybleAttrs(); j++) {
      if (!attrs[j]->alias)
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

int TempTable::GetDimension(TabID alias) {
  for (uint i = 0; i < aliases.size(); i++) {
    if (aliases[i] == alias.n)
      return i;
  }
  return common::NULL_VALUE_32;
}

int64_t TempTable::GetTable64(int64_t obj, int attr) {
  if (no_obj == 0)
    return common::NULL_VALUE_64;
  DEBUG_ASSERT(obj < no_obj && (uint)attr < attrs.size());
  return attrs[attr]->GetValueInt64(obj);
}

void TempTable::GetTable_S(types::BString &s, int64_t obj, int _attr) {
  if (no_obj == 0) {
    s = types::BString();
    return;
  }
  uint attr = (uint)_attr;
  DEBUG_ASSERT(obj < no_obj && attr < attrs.size());
  attrs[attr]->GetValueString(s, obj);
}

bool TempTable::IsNull(int64_t obj, int attr) {
  if (no_obj == 0)
    return true;
  DEBUG_ASSERT(obj < no_obj && (uint)attr < attrs.size());
  return attrs[attr]->IsNull(obj);
}

void TempTable::SuspendDisplay() { m_conn->SuspendDisplay(); }

void TempTable::ResumeDisplay() { m_conn->ResumeDisplay(); }

uint64_t TempTable::ApproxAnswerSize([[maybe_unused]] int attr,
                                     [[maybe_unused]] Descriptor &d)  // provide the most probable
                                                                      // approximation of number of objects
                                                                      // matching the condition
{
  // TODO: can it be done better?
  return no_obj / 2;
}

void TempTable::GetTableString(types::BString &s, int64_t obj, uint attr) {
  if (no_obj == 0)
    s = types::BString();
  DEBUG_ASSERT(obj < no_obj && (uint)attr < attrs.size());
  attrs[attr]->GetValueString(s, obj);
}

types::TianmuValueObject TempTable::GetValueObject(int64_t obj, uint attr) {
  if (no_obj == 0)
    return types::TianmuValueObject();
  DEBUG_ASSERT(obj < no_obj && (uint)attr < attrs.size());
  return attrs[attr]->GetValue(obj);
}

uint TempTable::CalculatePageSize(int64_t _no_obj) {
  int64_t new_no_obj = _no_obj == -1 ? no_obj : _no_obj;
  uint size_of_one_record = 0;
  for (uint i = 0; i < attrs.size(); i++)
    if (attrs[i]->TypeName() == common::ColumnType::BIN || attrs[i]->TypeName() == common::ColumnType::BYTE ||
        attrs[i]->TypeName() == common::ColumnType::VARBYTE || attrs[i]->TypeName() == common::ColumnType::LONGTEXT ||
        attrs[i]->TypeName() == common::ColumnType::STRING || attrs[i]->TypeName() == common::ColumnType::VARCHAR)
      size_of_one_record += attrs[i]->Type().GetInternalSize() + 4;  // 4 bytes describing length
    else
      size_of_one_record += attrs[i]->Type().GetInternalSize();
  uint raw_size = (uint)new_no_obj;
  if (size_of_one_record < 1)
    size_of_one_record = 1;

  SetOneOutputRecordSize(size_of_one_record);
  uint64_t cache_size;
  if (mem_scale == -1)
    mem_scale = mm::TraceableObject::MemorySettingsScale();

  switch (mem_scale) {
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
  //	std::cerr << "cs=" << cache_size << std::endl;

  if (cache_size / size_of_one_record <= (uint64_t)new_no_obj) {
    raw_size = uint((cache_size - 1) / size_of_one_record);  // minus 1 to avoid overflow
  }
  if (raw_size < 1)
    raw_size = 1;
  for (uint i = 0; i < attrs.size(); i++) attrs[i]->page_size = raw_size;
  return raw_size;
}

void TempTable::SetPageSize(int64_t new_page_size) {
  for (uint i = 0; i < attrs.size(); i++) attrs[i]->SetNewPageSize((uint)new_page_size);
}

/*! Filter out rows from the given multiindex according to the tree of
 * descriptors
 *
 */

void TempTable::DisplayRSI() {
  for (uint i = 0; i < tables.size(); i++) {
    if (tables[i]->TableType() == TType::TABLE)
      ((TianmuTable *)tables[i])->DisplayRSI();
  }
}

// Refactoring: extracted methods

void TempTable::RemoveFromManagedList(const TianmuTable *tab) {
  tables.erase(std::remove(tables.begin(), tables.end(), tab), tables.end());
}

void TempTable::ApplyOffset(int64_t limit, int64_t offset) {
  // filter out all unwanted values from buffers
  no_obj = limit;
  for (uint i = 0; i < attrs.size(); i++) {
    if (attrs[i]->alias)
      attrs[i]->ApplyFilter(output_mind, offset, offset + limit);
    else
      attrs[i]->DeleteBuffer();
  }
}

void TempTable::ProcessParameters(const MIIterator &mit, const int alias) {
  MEASURE_FET("TempTable::ProcessParameters(...)");
  for (uint i = 0; i < virt_cols.size(); i++) virt_cols[i]->RequestEval(mit, alias);
  filter.ProcessParameters();
  // filter.Prepare();
  if (mode.top && LimitMayBeAppliedToWhere())  // easy case - apply limits
    filter.UpdateMultiIndex(false, (mode.param2 >= 0 ? mode.param2 : 0));
  else
    filter.UpdateMultiIndex(false, -1);
}

void TempTable::RoughProcessParameters(const MIIterator &mit, const int alias) {
  MEASURE_FET("TempTable::RoughProcessParameters(...)");
  for (uint i = 0; i < virt_cols.size(); i++) virt_cols[i]->RequestEval(mit, alias);
  filter.ProcessParameters();
  filter.RoughUpdateParamFilter();
}

bool TempTable::IsParametrized() {
  for (uint i = 0; i < virt_cols.size(); i++)
    if (virt_cols[i] && virt_cols[i]->IsParameterized())
      return true;
  return false;
}

int TempTable::DimInDistinctContext() {
  // return a dimension number if it is used only in contexts where row
  // repetitions may be omitted, e.g. distinct
  int d = -1;
  if (HasHavingConditions() || filter.mind_->NumOfDimensions() == 1)  // having or no joins
    return -1;
  bool group_by_exists = false;
  bool aggregation_exists = false;
  for (uint i = 0; i < attrs.size(); i++)
    if (attrs[i]) {
      if (attrs[i]->mode == common::ColOperation::GROUP_BY)
        group_by_exists = true;
      if (attrs[i]->mode != common::ColOperation::GROUP_BY && attrs[i]->mode != common::ColOperation::LISTING &&
          attrs[i]->mode != common::ColOperation::DELAYED)
        aggregation_exists = true;
      if (!attrs[i]->term.vc || attrs[i]->term.vc->IsConst())
        continue;
      int local_dim = attrs[i]->term.vc->GetDim();
      if (local_dim == -1 || (d != -1 && d != local_dim))
        return -1;
      d = local_dim;
    }
  // all columns are based on the same dimension
  if (group_by_exists && aggregation_exists)  // group by with aggregations - not distinct context
    return -1;
  if (group_by_exists || (mode.distinct && !aggregation_exists))  // select distinct a,b,c...;
                                                                  // select a,b,c group by a,b,c
    return d;
  // aggregations exist - check their type
  for (uint i = 0; i < attrs.size(); i++)
    if (attrs[i] && attrs[i]->mode != common::ColOperation::MIN && attrs[i]->mode != common::ColOperation::MAX &&
        !attrs[i]->distinct)
      return -1;
  return d;
}

bool TempTable::LimitMayBeAppliedToWhere() {
  if (order_by.size() > 0)  // ORDER BY => false
    return false;
  if (mode.distinct || HasHavingConditions())  // DISTINCT or HAVING  => false
    return false;
  for (uint i = 0; i < NumOfAttrs(); i++)  // GROUP BY or other aggregation => false
    if (attrs[i]->mode != common::ColOperation::LISTING)
      return false;
  return true;
}

int TempTable::AddVirtColumn(vcolumn::VirtualColumn *vc) {
  virt_cols.push_back(vc);
  virt_cols_for_having.push_back(vc->GetMultiIndex() == &output_mind);
  return (int)virt_cols.size() - 1;
}

int TempTable::AddVirtColumn(vcolumn::VirtualColumn *vc, int no) {
  DEBUG_ASSERT(static_cast<size_t>(no) < virt_cols.size());
  virt_cols_for_having[no] = (vc->GetMultiIndex() == &output_mind);
  virt_cols[no] = vc;
  return no;
}

void TempTable::ResetVCStatistics() {
  for (uint i = 0; i < virt_cols.size(); i++)
    if (virt_cols[i])
      virt_cols[i]->ResetLocalStatistics();
}

void TempTable::SetVCDistinctVals(int dim, int64_t val) {
  for (uint i = 0; i < virt_cols.size(); i++)
    if (virt_cols[i] && virt_cols[i]->GetDim() == dim)
      virt_cols[i]->SetLocalDistVals(val);
}

std::shared_ptr<TempTable> TempTable::Create(const TempTable &t, bool in_subq) {
  MEASURE_FET("TempTable::Create(...)");
  bool _is_vc_owner = !in_subq;
  std::shared_ptr<TempTable> tnew = std::shared_ptr<TempTable>(new TempTable(t, _is_vc_owner));
  if (in_subq) {
    // map<PhysicalColumn *, PhysicalColumn *> attr_translation;	//
    // SingleColumns should refer to columns of tnew
    for (uint i = 0; i < t.attrs.size(); i++) {
      // attr_translation[t.attrs[i]] = tnew->attrs[i];
      tnew->attr_back_translation[tnew->attrs[i]] = t.attrs[i];
    }
    // for(uint i = 0; i< tnew->virt_cols.size(); i++) {
    //	if(tnew->virt_cols[i]->IsSingleColumn())
    //		static_cast<vcolumn::SingleColumn*>(tnew->virt_cols[i])->TranslateSourceColumns(attr_translation);
    //	if(tnew->virt_cols_for_having[i]) {
    //		tnew->virt_cols[i]->SetMultiIndex(&tnew->output_mind, tnew);
    //	} else
    //		tnew->virt_cols[i]->SetMultiIndex(tnew->filter.mind_);
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
  for (uint i = 0; i < tables.size(); i++)
    if (tables[i]->TableType() == TType::TEMP_TABLE)
      return true;
  return false;
}

void TempTable::LockPackForUse([[maybe_unused]] unsigned attr, unsigned pack_no) {
  while (lazy && no_materialized < std::min(((int64_t)pack_no << p_power) + (1 << p_power), no_obj))
    Materialize(false, nullptr, true);
}

bool TempTable::CanOrderSources() {
  for (uint i = 0; i < order_by.size(); i++) {
    if (order_by[i].vc->GetMultiIndex() != filter.mind_)
      return false;
  }
  return true;
}

void TempTable::Materialize(bool in_subq, ResultSender *sender, bool lazy) {
  MEASURE_FET("TempTable::Materialize()");
  if (sender)
    sender->SetAffectRows(no_obj);
  CreateDisplayableAttrP();
  CalculatePageSize();
  int64_t offset = 0;  // controls the first object to be materialized
  int64_t limit = -1;  // if(limit>=0)  ->  for(row = offset; row < offset +
                       // limit; row++) ....
  bool limits_present = mode.top;
  bool exists_only = mode.exists;

  if (limits_present) {
    offset = (mode.param1 >= 0 ? mode.param1 : 0);
    limit = (mode.param2 >= 0 ? mode.param2 : 0);
    // applied => reset to default values
    mode.top = false;
    mode.param2 = -1;
    mode.param1 = 0;
  }
  int64_t local_offset = 0;  // controls the first object to be materialized in a given algorithm
  int64_t local_limit = -1;
  if (materialized && (order_by.size() > 0 || limits_present) && no_obj) {
    // if TempTable is already materialized but some additional constraints were
    // specified, e.g., order by or limit this is typical case for union, where
    // constraints are specified for the result of union after materialization
    if (limits_present) {
      local_offset = offset;
      local_limit = std::min(limit, (int64_t)no_obj - offset);
      local_limit = local_limit < 0 ? 0 : local_limit;
    } else
      local_limit = no_obj;
    if (exists_only) {
      if (local_limit == 0)  // else no change needed
        no_obj = 0;
      return;
    }

    if (order_by.size() != 0 && no_obj > 1) {
      std::shared_ptr<TempTable> temporary_source_table =
          CreateMaterializedCopy(true, in_subq);  // true: translate definition of ordering
      OrderByAndMaterialize(order_by, local_limit, local_offset);
      DeleteMaterializedCopy(temporary_source_table);
    } else if (limits_present)
      ApplyOffset(local_limit, local_offset);
    order_by.clear();
    return;
  }

  if ((materialized && !this->lazy) || (this->lazy && no_obj == no_materialized)) {
    return;
  }

  bool table_distinct = this->mode.distinct;
  bool distinct_on_materialized = false;
  for (uint i = 0; i < NumOfAttrs(); i++)
    if (attrs[i]->mode != common::ColOperation::LISTING)
      group_by = true;
  if (table_distinct && group_by) {
    distinct_on_materialized = true;
    table_distinct = false;
  }

  if (order_by.size() > 0 || group_by || this->mode.distinct || force_full_materialize)
    lazy = false;
  this->lazy = lazy;

  bool no_rows_too_large = filter.mind_->TooManyTuples();
  no_obj = -1;         // no_obj not calculated yet - wait for better moment
  VerifyAttrsSizes();  // resize attr[i] buffers basing on the current
                       // multiindex state

  // the case when there is no grouping of attributes, check also DISTINCT
  // modifier of TT
  if (!group_by && !table_distinct) {
    DEBUG_ASSERT(!distinct_on_materialized);  // should by false here, otherwise must be
                                              // added to conditions below

    if (limits_present) {
      if (no_rows_too_large && order_by.size() == 0)
        no_obj = offset + limit;  // offset + limit in the worst case
      else
        no_obj = filter.mind_->NumOfTuples();
      if (no_obj <= offset) {
        no_obj = 0;
        materialized = true;
        order_by.clear();
        return;
      }
      local_offset = offset;
      local_limit = std::min(limit, (int64_t)no_obj - offset);
      local_limit = local_limit < 0 ? 0 : local_limit;
    } else {
      no_obj = filter.mind_->NumOfTuples();
      local_limit = no_obj;
    }
    if (exists_only) {
      order_by.clear();
      return;
    }
    output_mind.Clear();
    output_mind.AddDimension_cross(local_limit);  // an artificial dimension for result

    CalculatePageSize();  // recalculate, as no_obj might changed
    // perform order by: in this case it can be done on source tables, not on
    // the result
    bool materialized_by_ordering = false;
    if (CanOrderSources())
      // false if no sorting used
      materialized_by_ordering = this->OrderByAndMaterialize(order_by, local_limit, local_offset, sender);
    if (!materialized_by_ordering) {  // not materialized yet?
      // materialize without aggregations. If ordering then do not send result
      if (order_by.size() == 0)
        FillMaterializedBuffers(local_limit, local_offset, sender, lazy);
      else  // in case of order by we need to materialize all rows to be next
            // ordered
        FillMaterializedBuffers(no_obj, 0, nullptr, lazy);
    }
  } else {
    // GROUP BY or DISTINCT -  compute aggregations
    if (limits_present && !distinct_on_materialized && order_by.size() == 0) {
      local_offset = offset;
      local_limit = limit;
      if (exists_only)
        local_limit = 1;
    }
    if (HasHavingConditions() && in_subq)
      having_conds[0].tree->Simplify(true);

    ResultSender *local_sender = (distinct_on_materialized || order_by.size() > 0 ? nullptr : sender);
    AggregationAlgorithm aggr(this);
    aggr.Aggregate(table_distinct, local_limit, local_offset,
                   local_sender);  // this->tree (HAVING) used inside
    if (no_obj == 0) {
      order_by.clear();
      return;
    }

    output_mind.Clear();
    output_mind.AddDimension_cross(no_obj);  // an artificial dimension for result
  }

  local_offset = 0;
  local_limit = -1;

  // DISTINCT after grouping
  if (distinct_on_materialized && no_obj > 1) {
    if (limits_present && order_by.size() == 0) {
      local_offset = std::min((int64_t)no_obj, offset);
      local_limit = std::min(limit, (int64_t)no_obj - local_offset);
      local_limit = local_limit < 0 ? 0 : local_limit;
      if (no_obj <= offset) {
        no_obj = 0;
        materialized = true;
        order_by.clear();
        return;
      }
    } else
      local_limit = no_obj;
    if (exists_only)
      local_limit = 1;
    std::shared_ptr<TempTable> temporary_source_table = CreateMaterializedCopy(false, in_subq);
    ResultSender *local_sender = (order_by.size() > 0 ? nullptr : sender);

    AggregationAlgorithm aggr(this);
    aggr.Aggregate(true, local_limit, local_offset,
                   local_sender);  // true => select-level distinct
    DeleteMaterializedCopy(temporary_source_table);
    output_mind.Clear();
    output_mind.AddDimension_cross(no_obj);  // an artificial dimension for result
  }                                          // end of distinct part
  // ORDER BY, if not sorted until now
  if (order_by.size() != 0) {
    if (limits_present) {
      local_offset = std::min((int64_t)no_obj, offset);
      local_limit = std::min(limit, (int64_t)no_obj - local_offset);
      local_limit = local_limit < 0 ? 0 : local_limit;
      if (no_obj <= offset) {
        no_obj = 0;
        materialized = true;
        order_by.clear();
        return;
      }
    } else
      local_limit = no_obj;
    if (no_obj > 1 && !exists_only) {
      std::shared_ptr<TempTable> temporary_source_table =
          CreateMaterializedCopy(true, in_subq);  // true: translate definition of ordering
      OrderByAndMaterialize(order_by, local_limit, local_offset, sender);
      DeleteMaterializedCopy(temporary_source_table);
    }
    order_by.clear();
    output_mind.Clear();
    output_mind.AddDimension_cross(no_obj);  // an artificial dimension for result
  }
  materialized = true;
}

// here we deal with both signed/unsigned, the exact values will be converted on send results phase.
void TempTable::RecordIterator::PrepareValues() {
  if (_currentRNo < uint64_t(table->NumOfObj())) {
    uint no_disp_attr = table->NumOfDisplaybleAttrs();
    for (uint att = 0; att < no_disp_attr; ++att) {
      common::ColumnType attrt_tmp = table->GetDisplayableAttrP(att)->TypeName();
      if (attrt_tmp == common::ColumnType::INT || attrt_tmp == common::ColumnType::MEDIUMINT) {
        int &v = (*(AttrBuffer<int> *)table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
        if (v == common::NULL_VALUE_32)
          dataTypes[att]->SetToNull();
        else
          ((types::TianmuNum *)dataTypes[att].get())->Assign(v, 0, false, attrt_tmp);
      } else if (attrt_tmp == common::ColumnType::SMALLINT) {
        short &v = (*(AttrBuffer<short> *)table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
        if (v == common::NULL_VALUE_SH)
          dataTypes[att]->SetToNull();
        else
          ((types::TianmuNum *)dataTypes[att].get())->Assign(v, 0, false, attrt_tmp);
      } else if (attrt_tmp == common::ColumnType::BYTEINT) {
        char &v = (*(AttrBuffer<char> *)table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
        if (v == common::NULL_VALUE_C)
          dataTypes[att]->SetToNull();
        else
          ((types::TianmuNum *)dataTypes[att].get())->Assign(v, 0, false, attrt_tmp);
      } else if (ATI::IsRealType(attrt_tmp)) {
        double &v = (*(AttrBuffer<double> *)table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
        if (v == NULL_VALUE_D)
          dataTypes[att]->SetToNull();
        else
          ((types::TianmuNum *)dataTypes[att].get())->Assign(v);
      } else if (attrt_tmp == common::ColumnType::NUM || attrt_tmp == common::ColumnType::BIGINT ||
                 attrt_tmp == common::ColumnType::BIT) {
        int64_t &v = (*(AttrBuffer<int64_t> *)table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
        if (v == common::NULL_VALUE_64)
          dataTypes[att]->SetToNull();
        else
          ((types::TianmuNum *)dataTypes[att].get())
              ->Assign(v, table->GetDisplayableAttrP(att)->Type().GetScale(), false, attrt_tmp);
      } else if (ATI::IsDateTimeType(attrt_tmp)) {
        int64_t &v = (*(AttrBuffer<int64_t> *)table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
        if (v == common::NULL_VALUE_64)
          dataTypes[att]->SetToNull();
        else
          ((types::TianmuDateTime *)dataTypes[att].get())->Assign(v, attrt_tmp);
      } else {
        ASSERT(ATI::IsStringType(attrt_tmp), "not all possible attr_types checked");
        (*(AttrBuffer<types::BString> *)table->GetDisplayableAttrP(att)->buffer)
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
        ATI::IsRealType(att_type) || att_type == common::ColumnType::NUM || att_type == common::ColumnType::BIGINT ||
        att_type == common::ColumnType::BIT)
      dataTypes.emplace_back(new types::TianmuNum());
    else if (ATI::IsDateTimeType(att_type))
      dataTypes.emplace_back(new types::TianmuDateTime());
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
  if (no_global_virt_cols != -1) {  // otherwise never evaluated
    for (uint i = no_global_virt_cols; i < virt_cols.size(); i++) delete virt_cols[i];

    virt_cols.resize(no_global_virt_cols);
    virt_cols = template_virt_cols;
  }
  delete template_filter;
  SetAttrsForExact();
  for (uint i = 0; i < attrs_for_rough.size(); i++) delete attrs_for_rough[i];
  for (uint i = 0; i < template_attrs.size(); i++) delete template_attrs[i];
}

void TempTableForSubquery::ResetToTemplate(bool rough, bool use_filter_shallow) {
  if (!template_filter)
    return;

  for (uint i = no_global_virt_cols; i < virt_cols.size(); i++) delete virt_cols[i];

  virt_cols.resize(template_virt_cols.size());

  for (uint i = 0; i < template_virt_cols.size(); i++) virt_cols[i] = template_virt_cols[i];

  for (uint i = 0; i < attrs.size(); i++) {
    void *orig_buf = (*attrs[i]).buffer;
    *attrs[i] = *template_attrs[i];
    (*attrs[i]).buffer = orig_buf;
  }

  if (use_filter_shallow) {
    filter = std::move(*template_filter);  // shallow
    filter_shallow_memory = true;
  } else {
    filter = *template_filter;
    filter_shallow_memory = false;
  }

  for (int i = 0; i < no_global_virt_cols; i++)
    if (!virt_cols_for_having[i])
      virt_cols[i]->SetMultiIndex(filter.mind_);

  having_conds = template_having_conds;
  order_by = template_order_by;
  mode = template_mode;
  no_obj = 0;

  (rough) ? rough_materialized = false : materialized = false;

  no_materialized = 0;
  rough_is_empty = common::Tribool();
}

void TempTableForSubquery::SetAttrsForRough() {
  if (!is_attr_for_rough) {
    for (uint i = attrs_for_exact.size(); i < attrs.size(); i++) delete attrs[i];
    attrs = attrs_for_rough;
    is_attr_for_rough = true;
  }
}

void TempTableForSubquery::SetAttrsForExact() {
  if (is_attr_for_rough) {
    for (uint i = (int)attrs_for_rough.size(); i < attrs.size(); i++) delete attrs[i];
    attrs = attrs_for_exact;
    is_attr_for_rough = false;
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
  if (rough_materialized)
    return;
  TempTable::RoughMaterialize(in_subq);
  materialized = false;
  rough_materialized = true;
}

void TempTableForSubquery::CreateTemplateIfNotExists() {
  if (attrs_for_rough.size() == 0) {
    attrs_for_exact = attrs;
    is_attr_for_rough = false;
    for (uint i = 0; i < attrs.size(); i++) attrs_for_rough.push_back(new Attr(*attrs[i]));
  }
  if (!template_filter && IsParametrized()) {
    template_filter = new ParameterizedFilter(filter);
    for (uint i = 0; i < attrs.size(); i++) template_attrs.push_back(new Attr(*attrs[i]));
    no_global_virt_cols = int(virt_cols.size());
    template_having_conds = having_conds;
    template_order_by = order_by;
    template_mode = mode;
    template_virt_cols = virt_cols;
  }
}

}  // namespace core
}  // namespace Tianmu
