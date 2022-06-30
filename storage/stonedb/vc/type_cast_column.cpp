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

#include "type_cast_column.h"
#include "core/transaction.h"
#include "types/rc_num.h"

namespace stonedb {
namespace vcolumn {

TypeCastColumn::TypeCastColumn(VirtualColumn *from, core::ColumnType const &target_type)
    : VirtualColumn(target_type.RemovedLookup(), from->GetMultiIndex()), is_full_const_(false), vc_(from) {
  dimension_ = from->GetDim();
}

TypeCastColumn::TypeCastColumn(const TypeCastColumn &c) : VirtualColumn(c) {
  DEBUG_ASSERT(CanCopy());
  vc_ = CreateVCCopy(c.vc_);
}

String2NumCastColumn::String2NumCastColumn(VirtualColumn *from, core::ColumnType const &to) : TypeCastColumn(from, to) {
  core::MIIterator mit(NULL, PACK_INVALID);
  is_full_const_ = vc_->IsFullConst();
  if (is_full_const_) {
    types::BString rs;
    types::RCNum rcn;
    vc_->GetValueString(rs, mit);
    if (rs.IsNull()) {
      rcv_ = types::RCNum();
      value_ = common::NULL_VALUE_64;
    } else {
      common::ErrorCode retc = ct.IsFixed() ? types::RCNum::ParseNum(rs, rcn, ct.GetScale())
                                            : types::RCNum::Parse(rs, rcn, to.GetTypeName());
      if (retc != common::ErrorCode::SUCCESS) {
        std::string s = "Truncated incorrect numeric value: \'";
        s += rs.ToString();
        s += "\'";
      }
    }
    rcv_ = rcn;
    value_ = rcn.GetValueInt64();
  }
}

int64_t String2NumCastColumn::GetNotNullValueInt64(const core::MIIterator &mit) {
  if (is_full_const_) return value_;
  types::BString rs;
  types::RCNum rcn;
  vc_->GetValueString(rs, mit);
  if (ct.IsFixed()) {
    if (types::RCNum::ParseNum(rs, rcn, ct.GetScale()) != common::ErrorCode::SUCCESS) {
      std::string s = "Truncated incorrect numeric value: \'";
      s += rs.ToString();
      s += "\'";
      common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
    }
  } else {
    if (types::RCNum::ParseReal(rs, rcn, ct.GetTypeName()) != common::ErrorCode::SUCCESS) {
      std::string s = "Truncated incorrect numeric value: \'";
      s += rs.ToString();
      s += "\'";
      common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
    }
  }
  return rcn.GetValueInt64();
}

int64_t String2NumCastColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  if (is_full_const_)
    return value_;
  else {
    types::BString rs;
    types::RCNum rcn;
    vc_->GetValueString(rs, mit);
    if (rs.IsNull()) {
      return common::NULL_VALUE_64;
    } else {
      common::ErrorCode rc;
      if (ct.IsInt()) {  // distinction for convert function
        rc = types::RCNum::Parse(rs, rcn, ct.GetTypeName());
        if (rc != common::ErrorCode::SUCCESS) {
          std::string s = "Truncated incorrect numeric value: \'";
          s += rs.ToString();
          s += "\'";
          common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        }
        if (rc == common::ErrorCode::OUT_OF_RANGE && rcn.GetValueInt64() > 0) return -1;
      } else if (ct.IsFixed()) {
        rc = types::RCNum::ParseNum(rs, rcn, ct.GetScale());
        if (rc != common::ErrorCode::SUCCESS) {
          std::string s = "Truncated incorrect numeric value: \'";
          s += rs.ToString();
          s += "\'";
          common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        }
      } else {
        if (types::RCNum::Parse(rs, rcn) != common::ErrorCode::SUCCESS) {
          std::string s = "Truncated incorrect numeric value: \'";
          s += rs.ToString();
          s += "\'";
          common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        }
      }
      return rcn.GetValueInt64();
    }
  }
}

double String2NumCastColumn::GetValueDoubleImpl(const core::MIIterator &mit) {
  if (is_full_const_)
    return *(double *)&value_;
  else {
    types::BString rs;
    types::RCNum rcn;
    vc_->GetValueString(rs, mit);
    if (rs.IsNull()) {
      return NULL_VALUE_D;
    } else {
      types::RCNum::Parse(rs, rcn, common::CT::FLOAT);
      // if(types::RCNum::Parse(rs, rcn, common::CT::FLOAT) !=
      // common::ErrorCode::SUCCESS) {
      //	std::string s = "Truncated incorrect numeric value: \'";
      //	s += (std::string)rs;
      //  s += "\'";
      //	PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN,
      // ER_TRUNCATED_WRONG_VALUE, s.c_str());
      //}
      return (double)rcn;
    }
  }
}

types::RCValueObject String2NumCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (is_full_const_)
    return rcv_;
  else {
    types::BString rs;
    types::RCNum rcn;
    vc_->GetValueString(rs, mit);
    if (rs.IsNull()) {
      return types::RCNum();
    } else if (types::RCNum::Parse(rs, rcn) != common::ErrorCode::SUCCESS) {
      std::string s = "Truncated incorrect numeric value: \'";
      s += rs.ToString();
      s += "\'";
      common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
    }
    return rcn;
  }
}

int64_t String2NumCastColumn::GetMinInt64Impl(const core::MIIterator &m) {
  if (is_full_const_)
    return value_;
  else if (IsConst()) {
    // const with parameters
    types::BString rs;
    types::RCNum rcn;
    vc_->GetValueString(rs, m);
    if (rs.IsNull()) {
      return common::NULL_VALUE_64;
    } else if (types::RCNum::Parse(rs, rcn) != common::ErrorCode::SUCCESS) {
      std::string s = "Truncated incorrect numeric value: \'";
      s += rs.ToString();
      s += "\'";
      common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
    }
    return rcn.GetValueInt64();
  } else
    return common::MINUS_INF_64;
}

int64_t String2NumCastColumn::GetMaxInt64Impl(const core::MIIterator &m) {
  int64_t v = GetMinInt64Impl(m);
  return v != common::MINUS_INF_64 ? v : common::PLUS_INF_64;
}

String2DateTimeCastColumn::String2DateTimeCastColumn(VirtualColumn *from, core::ColumnType const &to)
    : TypeCastColumn(from, to) {
  is_full_const_ = vc_->IsFullConst();
  if (is_full_const_) {
    types::BString rbs;
    core::MIIterator mit(NULL, PACK_INVALID);
    vc_->GetValueString(rbs, mit);
    if (rbs.IsNull()) {
      value_ = common::NULL_VALUE_64;
      rcv_ = types::RCDateTime();
    } else {
      types::RCDateTime rcdt;
      common::ErrorCode rc = types::RCDateTime::Parse(rbs, rcdt, TypeName());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += rbs.ToString();
        s += "\'";
        // mysql is pushing this warning anyway, removed duplication
        // PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN,
        // ER_TRUNCATED_WRONG_VALUE, s.c_str());
      }
      value_ = rcdt.GetInt64();
      rcv_ = rcdt;
    }
  }
}

int64_t String2DateTimeCastColumn::GetNotNullValueInt64(const core::MIIterator &mit) {
  if (is_full_const_) return value_;

  types::RCDateTime rcdt;
  types::BString rbs;
  vc_->GetValueString(rbs, mit);
  common::ErrorCode rc = types::RCDateTime::Parse(rbs, rcdt, TypeName());
  if (common::IsWarning(rc) || common::IsError(rc)) {
    std::string s = "Incorrect datetime value: \'";
    s += rbs.ToString();
    s += "\'";
    common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
    return common::NULL_VALUE_64;
  }
  return rcdt.GetInt64();
}

int64_t String2DateTimeCastColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  if (is_full_const_)
    return value_;
  else {
    types::RCDateTime rcdt;
    types::BString rbs;
    vc_->GetValueString(rbs, mit);
    if (rbs.IsNull())
      return common::NULL_VALUE_64;
    else {
      common::ErrorCode rc = types::RCDateTime::Parse(rbs, rcdt, TypeName());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += rbs.ToString();
        s += "\'";
        common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        return common::NULL_VALUE_64;
      } /* else if(common::IsWarning(res)) {
                           std::string s = "Incorrect Date/Time value: ";
                           s += (std::string)rbs;
                           PushWarning(ConnInfo()->Thd(),
           Sql_condition::WARN_LEVEL_WARN, ER_WARN_INVALID_TIMESTAMP,
           s.c_str());
                   }*/
      return rcdt.GetInt64();
    }
  }
}

void String2DateTimeCastColumn::GetValueStringImpl(types::BString &rbs, const core::MIIterator &mit) {
  if (is_full_const_)
    rbs = rcv_.ToBString();
  else {
    types::RCDateTime rcdt;
    vc_->GetValueString(rbs, mit);
    if (rbs.IsNull())
      return;
    else {
      common::ErrorCode rc = types::RCDateTime::Parse(rbs, rcdt, TypeName());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += rbs.ToString();
        s += "\'";
        common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        rbs = types::BString();
        return;
      } /* else if(common::IsWarning(res)) {
                           std::string s = "Incorrect Date/Time value: ";
                           s += (std::string)rbs;
                           PushWarning(ConnInfo()->Thd(),
           Sql_condition::WARN_LEVEL_WARN, ER_WARN_INVALID_TIMESTAMP,
           s.c_str());
                   }*/
      rbs = rcdt.ToBString();
    }
  }
}

types::RCValueObject String2DateTimeCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (is_full_const_)
    return rcv_;
  else {
    types::RCDateTime rcdt;
    types::BString rbs;
    vc_->GetValueString(rbs, mit);
    if (rbs.IsNull())
      return types::RCDateTime();
    else {
      common::ErrorCode rc = types::RCDateTime::Parse(rbs, rcdt, TypeName());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += rbs.ToString();
        s += "\'";
        common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        // return types::RCDateTime();
      }
      return rcdt;
    }
  }
}

int64_t String2DateTimeCastColumn::GetMinInt64Impl(const core::MIIterator &m) {
  if (is_full_const_)
    return value_;
  else if (IsConst()) {
    // const with parameters
    return ((types::RCDateTime *)GetValueImpl(m).Get())->GetInt64();
  } else
    return common::MINUS_INF_64;
}

int64_t String2DateTimeCastColumn::GetMaxInt64Impl(const core::MIIterator &m) {
  int64_t v = GetMinInt64Impl(m);
  return v != common::MINUS_INF_64 ? v : common::PLUS_INF_64;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Num2DateTimeCastColumn::Num2DateTimeCastColumn(VirtualColumn *from, core::ColumnType const &to)
    : String2DateTimeCastColumn(from, to) {
  core::MIIterator mit(NULL, PACK_INVALID);
  is_full_const_ = vc_->IsFullConst();
  if (is_full_const_) {
    core::MIIterator mit(NULL, PACK_INVALID);
    value_ = vc_->GetValueInt64(mit);
    // rcv_ = from->GetValue(mit);
    types::RCDateTime rcdt;
    if (value_ != common::NULL_VALUE_64) {
      if (TypeName() == common::CT::TIME) {
        MYSQL_TIME ltime;
        short timehour;
        TIME_from_longlong_time_packed(&ltime, value_);
        short second = ltime.second;
        short minute = ltime.minute;
        if (ltime.neg == 1)
          timehour = ltime.hour * -1;
        else
          timehour = ltime.hour;
        types::RCDateTime rctime(timehour, minute, second, common::CT::TIME);
        rcdt = rctime;
      } else {
        common::ErrorCode rc = types::RCDateTime::Parse(value_, rcdt, TypeName(), ct.GetPrecision());
        if (common::IsWarning(rc) || common::IsError(rc)) {
          std::string s = "Incorrect datetime value: \'";
          s += rcv_.ToBString().ToString();
          s += "\'";
          STONEDB_LOG(LogCtl_Level::WARN, "Num2DateTimeCast %s", s.c_str());
        }
        if (TypeName() == common::CT::TIMESTAMP) {
          // needs to convert value to UTC
          MYSQL_TIME myt;
          memset(&myt, 0, sizeof(MYSQL_TIME));
          myt.year = rcdt.Year();
          myt.month = rcdt.Month();
          myt.day = rcdt.Day();
          myt.hour = rcdt.Hour();
          myt.minute = rcdt.Minute();
          myt.second = rcdt.Second();
          myt.time_type = MYSQL_TIMESTAMP_DATETIME;
          if (!common::IsTimeStampZero(myt)) {
            my_bool myb;
            // convert local time to UTC seconds from beg. of EPOCHE
            my_time_t secs_utc = current_tx->Thd()->variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
            // UTC seconds converted to UTC TIME
            common::GMTSec2GMTTime(&myt, secs_utc);
          }
          rcdt = types::RCDateTime(myt, common::CT::TIMESTAMP);
        }
      }

      value_ = rcdt.GetInt64();
    }
    rcv_ = rcdt;
  }
}

types::RCValueObject Num2DateTimeCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (is_full_const_)
    return rcv_;
  else {
    types::RCDateTime rcdt;
    types::RCValueObject r(vc_->GetValue(mit));

    if (!r.IsNull()) {
      common::ErrorCode rc =
          types::RCDateTime::Parse(((types::RCNum)r).GetIntPart(), rcdt, TypeName(), ct.GetPrecision());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += r.ToBString().ToString();
        s += "\'";

        common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        return types::RCDateTime();
      }
      return rcdt;
    } else
      return r;
  }
}

int64_t Num2DateTimeCastColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  if (is_full_const_)
    return value_;
  else {
    types::RCDateTime rcdt;
    int64_t v = vc_->GetValueInt64(mit);
    types::RCNum r(v, vc_->Type().GetScale(), vc_->Type().IsFloat(), vc_->Type().GetTypeName());
    if (v != common::NULL_VALUE_64) {
      common::ErrorCode rc = types::RCDateTime::Parse(r.GetIntPart(), rcdt, TypeName(), ct.GetPrecision());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += r.ToBString().ToString();
        s += "\'";

        common::PushWarning(ConnInfo()->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        return common::NULL_VALUE_64;
      }
      return rcdt.GetInt64();
    } else
      return v;
  }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DateTime2VarcharCastColumn::DateTime2VarcharCastColumn(VirtualColumn *from, core::ColumnType const &to)
    : TypeCastColumn(from, to) {
  core::MIIterator mit(NULL, PACK_INVALID);
  is_full_const_ = vc_->IsFullConst();
  if (is_full_const_) {
    int64_t i = vc_->GetValueInt64(mit);
    if (i == common::NULL_VALUE_64) {
      rcv_ = types::BString();
    } else {
      types::RCDateTime rcdt(i, vc_->TypeName());
      rcv_ = rcdt.ToBString();
    }
  }
}

types::RCValueObject DateTime2VarcharCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (is_full_const_)
    return rcv_;
  else {
    types::BString rcb;
    vc_->GetValueString(rcb, mit);
    if (rcb.IsNull()) {
      return types::BString();
    } else {
      return rcb;
    }
  }
}

Num2VarcharCastColumn::Num2VarcharCastColumn(VirtualColumn *from, core::ColumnType const &to)
    : TypeCastColumn(from, to) {
  core::MIIterator mit(NULL, PACK_INVALID);
  is_full_const_ = vc_->IsFullConst();
  if (is_full_const_) {
    rcv_ = vc_->GetValue(mit);
    if (rcv_.IsNull()) {
      rcv_ = types::BString();
    } else {
      rcv_ = rcv_.ToBString();
    }
  }
}

types::RCValueObject Num2VarcharCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (is_full_const_)
    return rcv_;
  else {
    types::RCValueObject r(vc_->GetValue(mit));
    if (r.IsNull()) {
      return types::BString();
    } else {
      return r.ToBString();
    }
  }
}

void Num2VarcharCastColumn::GetValueStringImpl(types::BString &s, const core::MIIterator &m) {
  if (is_full_const_)
    s = rcv_.ToBString();
  else {
    int64_t v = vc_->GetValueInt64(m);
    if (v == common::NULL_VALUE_64)
      s = types::BString();
    else {
      types::RCNum rcd(v, vc_->Type().GetScale(), vc_->Type().IsFloat(), vc_->TypeName());
      s = rcd.ToBString();
    }
  }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DateTime2NumCastColumn::DateTime2NumCastColumn(VirtualColumn *from, core::ColumnType const &to)
    : TypeCastColumn(from, to) {
  core::MIIterator mit(NULL, PACK_INVALID);
  is_full_const_ = vc_->IsFullConst();
  if (is_full_const_) {
    rcv_ = vc_->GetValue(mit);
    if (rcv_.IsNull()) {
      rcv_ = types::RCNum();
      value_ = common::NULL_VALUE_64;
    } else {
      ((types::RCDateTime *)rcv_.Get())->ToInt64(value_);
      if (vc_->TypeName() == common::CT::YEAR && vc_->Type().GetPrecision() == 2) value_ %= 100;
      if (to.IsFloat()) {
        double x = (double)value_;
        value_ = *(int64_t *)&x;
      }
      rcv_ = types::RCNum(value_, ct.GetScale(), ct.IsFloat(), TypeName());
    }
  }
}

int64_t DateTime2NumCastColumn::GetNotNullValueInt64(const core::MIIterator &mit) {
  if (is_full_const_) return value_;
  int64_t v = vc_->GetNotNullValueInt64(mit);
  if (vc_->TypeName() == common::CT::YEAR && vc_->Type().GetPrecision() == 2) v %= 100;
  types::RCDateTime rdt(v, vc_->TypeName());
  int64_t r;
  rdt.ToInt64(r);
  if (Type().IsFloat()) {
    double x = (double)r;
    r = *(int64_t *)&x;
  }
  return r;
}

int64_t DateTime2NumCastColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  if (is_full_const_)
    return value_;
  else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64) return v;
    types::RCDateTime rdt(v, vc_->TypeName());
    int64_t r;
    rdt.ToInt64(r);
    if (vc_->TypeName() == common::CT::YEAR && vc_->Type().GetPrecision() == 2) r = r % 100;
    if (Type().IsFloat()) {
      double x = (double)r;
      r = *(int64_t *)&x;
    }
    return r;
  }
}

double DateTime2NumCastColumn::GetValueDoubleImpl(const core::MIIterator &mit) {
  if (is_full_const_) {
    int64_t v;
    types::RCDateTime rdt(value_, vc_->TypeName());
    rdt.ToInt64(v);
    return (double)v;
  } else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64) return NULL_VALUE_D;
    types::RCDateTime rdt(v, vc_->TypeName());
    rdt.ToInt64(v);
    return (double)v;
  }
}

types::RCValueObject DateTime2NumCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (is_full_const_)
    return rcv_;
  else {
    types::RCValueObject v(vc_->GetValue(mit));
    if (v.IsNull()) {
      return types::RCNum();
    } else {
      int64_t r;
      ((types::RCDateTime *)v.Get())->ToInt64(r);
      if (vc_->TypeName() == common::CT::YEAR && vc_->Type().GetPrecision() == 2) r %= 100;
      if (Type().IsFloat()) {
        double x = (double)r;
        r = *(int64_t *)&x;
      }
      return types::RCNum(r, ct.GetScale(), ct.IsFloat(), TypeName());
    }
  }
}

TimeZoneConversionCastColumn::TimeZoneConversionCastColumn(VirtualColumn *from)
    : TypeCastColumn(from, core::ColumnType(common::CT::DATETIME)) {
  DEBUG_ASSERT(from->TypeName() == common::CT::TIMESTAMP);
  core::MIIterator mit(NULL, PACK_INVALID);
  is_full_const_ = vc_->IsFullConst();
  if (is_full_const_) {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64) {
      rcv_ = types::RCDateTime();
      value_ = common::NULL_VALUE_64;
    } else {
      rcv_ = types::RCDateTime(v, vc_->TypeName());
      types::RCDateTime::AdjustTimezone(rcv_);
      value_ = ((types::RCDateTime *)rcv_.Get())->GetInt64();
    }
  }
}

int64_t TimeZoneConversionCastColumn::GetNotNullValueInt64(const core::MIIterator &mit) {
  if (is_full_const_) return value_;
  int64_t v = vc_->GetNotNullValueInt64(mit);
  types::RCDateTime rdt(v, vc_->TypeName());
  types::RCDateTime::AdjustTimezone(rdt);
  return rdt.GetInt64();
}

int64_t TimeZoneConversionCastColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  if (is_full_const_)
    return value_;
  else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64) return v;
    types::RCDateTime rdt(v, vc_->TypeName());
    types::RCDateTime::AdjustTimezone(rdt);
    return rdt.GetInt64();
  }
}

types::RCValueObject TimeZoneConversionCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (is_full_const_) {
    if (Type().IsString()) return rcv_.ToBString();
    return rcv_;
  } else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64) return types::RCDateTime();
    types::RCDateTime rdt(v, vc_->TypeName());
    types::RCDateTime::AdjustTimezone(rdt);
    if (Type().IsString()) return rdt.ToBString();
    return rdt;
  }
}

double TimeZoneConversionCastColumn::GetValueDoubleImpl(const core::MIIterator &mit) {
  if (is_full_const_) {
    int64_t v;
    types::RCDateTime rdt(value_, vc_->TypeName());
    rdt.ToInt64(v);
    return (double)v;
  } else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64) return NULL_VALUE_D;
    types::RCDateTime rdt(v, vc_->TypeName());
    types::RCDateTime::AdjustTimezone(rdt);
    rdt.ToInt64(v);
    return (double)v;
  }
}

void TimeZoneConversionCastColumn::GetValueStringImpl(types::BString &s, const core::MIIterator &mit) {
  if (is_full_const_) {
    s = rcv_.ToBString();
  } else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64) {
      s = types::BString();
      return;
    }
    types::RCDateTime rdt(v, vc_->TypeName());
    types::RCDateTime::AdjustTimezone(rdt);
    s = rdt.ToBString();
  }
}

types::RCValueObject StringCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool lookup_to_num) {
  types::BString s;
  vc_->GetValueString(s, mit);
  return s;
}

}  // namespace vcolumn
}  // namespace stonedb
