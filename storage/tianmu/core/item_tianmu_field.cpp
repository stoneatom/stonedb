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

#include "item_tianmu_field.h"

#include "common/assert.h"
#include "core/compilation_tools.h"
#include "core/quick_math.h"
#include "core/transaction.h"

namespace Tianmu {
namespace core {
Item_tianmufield::Item_tianmufield(Item_field *ifield, VarID varID)
    : Item_field(current_txn_->Thd(), ifield), ifield(ifield), buf(nullptr), ivalue(nullptr) {
  this->varID.push_back(varID);
  if (ifield->type() == Item::SUM_FUNC_ITEM) {
    was_aggregation = true;
    aggregation_result = ifield->result_type();
  } else
    was_aggregation = false;
  std::sprintf(fullname, "tianmufield of %p", (void *)ifield);
  isBufOwner = false;
}

Item_tianmufield::~Item_tianmufield() {
  // if(ivalue != nullptr) delete ivalue;	// done by MySQL not TIANMU, for each Item
  // subclass
  ClearBuf();
}

void Item_tianmufield::ClearBuf() {
  if (isBufOwner) {
    delete buf;
    buf = nullptr;
  }
}

void Item_tianmufield::SetBuf(ValueOrNull *&b) {
  if (buf == nullptr) {
    isBufOwner = true;
    buf = new ValueOrNull;
  }
  b = buf;
}

void Item_tianmufield::SetType(DataType t) {
  if (ivalue != nullptr) {
    // delete ivalue;		// done by MySQL not TIANMU, for each Item subclass
    ivalue = nullptr;
  }

  tianmu_type = t;
  switch (tianmu_type.valtype) {
    case DataType::ValueType::VT_FIXED:
      if (tianmu_type.IsInt())
        ivalue = new Item_int(static_cast<longlong>(0));
      else
        ivalue = new Item_tianmudecimal(tianmu_type);
      ivalue->unsigned_flag = ifield->unsigned_flag;
      break;

    case DataType::ValueType::VT_FLOAT:
      ivalue = new Item_float(0.0, NOT_FIXED_DEC);
      break;

    case DataType::ValueType::VT_STRING:
      ivalue = new Item_string("", 0, ifield->collation.collation, ifield->collation.derivation);
      break;

    case DataType::ValueType::VT_DATETIME:
      switch (tianmu_type.attrtype) {
        case common::ColumnType::DATETIME:
          ivalue = new Item_tianmudatetime();
          break;
        case common::ColumnType::TIMESTAMP:
          ivalue = new Item_tianmutimestamp();
          break;
        case common::ColumnType::DATE:
          ivalue = new Item_tianmudate();
          break;
        case common::ColumnType::TIME:
          ivalue = new Item_tianmutime();
          break;
        case common::ColumnType::YEAR:
          ivalue = new Item_tianmuyear(tianmu_type.precision);
          break;
        default:
          TIANMU_ERROR(
              "Incorrect date/time data type passed to "
              "Item_tianmufield::SetType()");
          break;
      }
      break;

    default:
      TIANMU_ERROR("Incorrect data type passed to Item_tianmufield::SetType()");
      break;
  }
}

const ValueOrNull Item_tianmufield::GetCurrentValue() { return *buf; }

void Item_tianmufield::FeedValue() {
  switch (tianmu_type.valtype) {
    case DataType::ValueType::VT_FIXED:
      if (tianmu_type.IsInt())
        ((Item_int *)ivalue)->value = buf->Get64();
      else
        ((Item_tianmudecimal *)ivalue)->Set(buf->Get64());
      break;

    case DataType::ValueType::VT_FLOAT:
      ((Item_float *)ivalue)->value = buf->GetDouble();
      break;

    case DataType::ValueType::VT_STRING:
      ((Item_string *)ivalue)->str_value.copy(buf->sp, buf->len, ifield->collation.collation);
      break;

    case DataType::ValueType::VT_DATETIME:
      ((Item_tianmudatetime_base *)ivalue)->Set(buf->Get64(), tianmu_type.attrtype);
      break;

    default:
      TIANMU_ERROR("Unrecognized type in Item_tianmufield::FeedValue()");
  }
}

longlong Item_tianmufield::val_int() {
  // DBUG_ASSERT(fixed == 1);
  if ((null_value = buf->null))
    return 0;
  FeedValue();
  return ivalue->val_int();
}

my_decimal *Item_tianmufield::val_decimal(my_decimal *decimal_value) {
  if ((null_value = buf->null))
    return 0;
  FeedValue();
  return ivalue->val_decimal(decimal_value);
}

String *Item_tianmufield::val_str(String *str) {
  // DBUG_ASSERT(fixed == 1);
  if ((null_value = buf->null))
    return 0;
  // acceleration
  if (tianmu_type.valtype == DataType::ValueType::VT_STRING) {
    str->copy(buf->sp, buf->len, ifield->collation.collation);
    return str;
  }
  FeedValue();
  str->copy(*(ivalue->val_str(str)));
  return str;
}

double Item_tianmufield::val_real() {
  // DBUG_ASSERT(fixed == 1);
  if ((null_value = buf->null))
    return 0.0;
  FeedValue();
  if (unsigned_flag)
    return static_cast<ulonglong>(ivalue->val_real());
  else
    return ivalue->val_real();
}

bool Item_tianmufield::get_date(MYSQL_TIME *ltime, uint fuzzydate) {
  if ((null_value = buf->null) ||
      ((!(fuzzydate & TIME_FUZZY_DATE) &&
        (tianmu_type.attrtype == common::ColumnType::DATETIME || tianmu_type.attrtype == common::ColumnType::DATE) &&
        buf->x == 0)))
    return 1;  // like in Item_field::get_date - return 1 on null value.
  FeedValue();
  return ivalue->get_date(ltime, fuzzydate);
}

bool Item_tianmufield::get_time(MYSQL_TIME *ltime) {
  // Consider zero date is legal , not consider the sql_mode NO_ZERO_DATE/NO_ZERO_IN_DATE.
  // Because NO_ZERO_DATE/NO_ZERO_IN_DATE is deprecated; expect it to be removed in a future
  // release of MySQL as a separate mode name and its effect included in the effects of strict SQL mode.
  if ((null_value = buf->null))
    return 1;  // like in Item_field::get_time - return 1 on null value.
  FeedValue();
  return ivalue->get_time(ltime);
}

bool Item_tianmufield::get_timeval(struct timeval *tm, int *warnings) {
  MYSQL_TIME ltime;
  if (get_time(&ltime))
    return true;
  if (datetime_to_timeval(current_thd, &ltime, tm, warnings))
    return true;
  return false;
}

bool Item_tianmufield::operator==(Item_tianmufield const &o) const { return (varID == o.varID); }

type_conversion_status Item_tianmufield::save_in_field_inner(Field *to, bool no_conversions) {
  if ((null_value = buf->null)) {
    null_value = 1;
    return set_field_to_null_with_conversions(to, no_conversions);
  }

  null_value = 0;
  to->set_notnull();

  Field *from = ifield->result_field;
  assert(from && from->table && from->table->write_set);

  if (!bitmap_is_set(from->table->write_set, from->field_index)) {
    bitmap_set_bit(from->table->write_set, from->field_index);
  }

  FeedValue();
  ivalue->save_org_in_field(from);
  return field_conv(to, from);
}

Item_tianmudecimal::Item_tianmudecimal(DataType t) : Item_decimal(0, false) {
  scale = t.fixscale;
  scaleCoef = QuickMath::power10i(scale);
}

void Item_tianmudecimal::Set(int64_t val) {
  std::scoped_lock guard(mtx);
  std::fill(decimal_value.buf, decimal_value.buf + decimal_value.len, 0);
  if (val) {
    int2my_decimal((uint)-1, val, 0, &decimal_value);
    my_decimal_shift((uint)-1, &decimal_value, -scale);
  } else {
    my_decimal_set_zero(&decimal_value);
  }
  decimal_value.frac = scale;
}

String *Item_tianmudecimal::val_str(String *str) {
  std::scoped_lock guard(mtx);
  return Item_decimal::val_str(str);
}

my_decimal *Item_tianmudatetime_base::val_decimal(my_decimal *d) {
  int2my_decimal((uint)-1, val_int(), 0, d);
  return d;
}

bool Item_tianmudatetime_base::get_time(MYSQL_TIME *ltime) {
  std::memset(ltime, 0, sizeof(*ltime));
  dt.Store(ltime, MYSQL_TIMESTAMP_TIME);
  return 0;
}

longlong Item_tianmudatetime::val_int() {
  return dt.year * 10000000000LL + dt.month * 100000000 + dt.day * 1000000 + dt.hour * 10000 + dt.minute * 100 +
         dt.second;
}

String *Item_tianmudatetime::val_str(String *s) {
  MYSQL_TIME ltime;
  get_date(&ltime, 0);
  s->alloc(19);
  make_datetime((Date_time_format *)0, &ltime, s, 0);
  return s;
}

bool Item_tianmudatetime::get_date(MYSQL_TIME *ltime, [[maybe_unused]] uint fuzzydate) {
  // Maybe we should check against zero date?
  // Then 'fuzzydate' would be used.
  // See Field_timestamp::get_date(...)
  std::memset(ltime, 0, sizeof(*ltime));  // safety
  dt.Store(ltime, MYSQL_TIMESTAMP_DATETIME);
  return 0;
}

bool Item_tianmudatetime::get_time(MYSQL_TIME *ltime) {
  std::memset(ltime, 0, sizeof(*ltime));  // safety
  dt.Store(ltime, MYSQL_TIMESTAMP_DATETIME);
  return false;
}

longlong Item_tianmudate::val_int() { return dt.year * 10000 + dt.month * 100 + dt.day; }
String *Item_tianmudate::val_str(String *s) {
  MYSQL_TIME ltime;
  get_date(&ltime, 0);
  s->alloc(19);
  make_date((Date_time_format *)0, &ltime, s);
  return s;
}
bool Item_tianmudate::get_date(MYSQL_TIME *ltime, [[maybe_unused]] uint fuzzydate) {
  // Maybe we should check against zero date?
  // Then 'fuzzydate' would be used.
  // See Field_timestamp::get_date(...)

  std::memset(ltime, 0, sizeof(*ltime));
  dt.Store(ltime, MYSQL_TIMESTAMP_DATE);
  return 0;
}

bool Item_tianmudate::get_time(MYSQL_TIME *ltime) {
  std::memset(ltime, 0, sizeof(*ltime));
  dt.Store(ltime, MYSQL_TIMESTAMP_DATE);
  return false;
}

longlong Item_tianmutime::val_int() { return dt.hour * 10000 + dt.minute * 100 + dt.second; }
String *Item_tianmutime::val_str(String *s) {
  MYSQL_TIME ltime;
  get_time(&ltime);
  s->alloc(19);
  make_time((Date_time_format *)0, &ltime, s, 0);
  return s;
}

bool Item_tianmutime::get_date(MYSQL_TIME *ltime, [[maybe_unused]] uint fuzzydate) {
  // Maybe we should check against zero date?
  // Then 'fuzzydate' would be used.
  // See Field_timestamp::get_date(...)
  MYSQL_TIME tm;
  std::memset(ltime, 0, sizeof(*ltime));
  dt.Store(&tm, MYSQL_TIMESTAMP_TIME);
  time_to_datetime(current_thd, &tm, ltime);
  return 0;
}

longlong Item_tianmuyear::val_int() { return length == 4 ? dt.year : dt.year % 100; }
String *Item_tianmuyear::val_str(String *s) {
  s->alloc(length + 1);
  s->length(length);
  s->set_charset(&my_charset_bin);  // Safety
  std::sprintf((char *)s->ptr(), length == 2 ? "%02d" : "%04d", (int)val_int());
  return s;
}
bool Item_tianmuyear::get_date(MYSQL_TIME *ltime, [[maybe_unused]] uint fuzzydate) {
  std::memset(ltime, 0, sizeof(*ltime));
  return 1;
}
}  // namespace core
}  // namespace Tianmu
