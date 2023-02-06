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
#include "types/tianmu_num.h"

namespace Tianmu {
namespace vcolumn {

TypeCastColumn::TypeCastColumn(VirtualColumn *from, core::ColumnType const &target_type)
    : VirtualColumn(target_type.RemovedLookup(), from->GetMultiIndex()), full_const_(false), vc_(from) {
  dim_ = from->GetDim();
}

TypeCastColumn::TypeCastColumn(const TypeCastColumn &c) : VirtualColumn(c) {
  DEBUG_ASSERT(CanCopy());
  vc_ = CreateVCCopy(c.vc_);
}

String2NumCastColumn::String2NumCastColumn(VirtualColumn *from, core::ColumnType const &to) : TypeCastColumn(from, to) {
  core::MIIterator mit(nullptr, PACK_INVALID);
  full_const_ = vc_->IsFullConst();
  if (full_const_) {
    types::BString rs;
    types::TianmuNum tianmu_n;
    vc_->GetValueString(rs, mit);
    if (rs.IsNull()) {
      rc_value_obj_ = types::TianmuNum();
      val_ = common::NULL_VALUE_64;
    } else {
      common::ErrorCode retc = ct.IsFixed() ? types::TianmuNum::ParseNum(rs, tianmu_n, ct.GetScale())
                                            : types::TianmuNum::Parse(rs, tianmu_n, to.GetTypeName());
      if (retc != common::ErrorCode::SUCCESS) {
        std::string s = "Truncated incorrect numeric value: \'";
        s += rs.ToString();
        s += "\'";
      }
    }
    rc_value_obj_ = tianmu_n;
    val_ = tianmu_n.GetValueInt64();
  }
}

int64_t String2NumCastColumn::GetNotNullValueInt64(const core::MIIterator &mit) {
  if (full_const_)
    return val_;
  types::BString rs;
  types::TianmuNum tianmu_n;
  vc_->GetValueString(rs, mit);
  if (ct.IsFixed()) {
    if (types::TianmuNum::ParseNum(rs, tianmu_n, ct.GetScale()) != common::ErrorCode::SUCCESS) {
      std::string s = "Truncated incorrect numeric value: \'";
      s += rs.ToString();
      s += "\'";
      common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
    }
  } else {
    if (types::TianmuNum::ParseReal(rs, tianmu_n, ct.GetTypeName()) != common::ErrorCode::SUCCESS) {
      std::string s = "Truncated incorrect numeric value: \'";
      s += rs.ToString();
      s += "\'";
      common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
    }
  }
  return tianmu_n.GetValueInt64();
}

int64_t String2NumCastColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  if (full_const_)
    return val_;
  else {
    types::BString rs;
    types::TianmuNum tianmu_n;
    vc_->GetValueString(rs, mit);
    if (rs.IsNull()) {
      return common::NULL_VALUE_64;
    } else {
      common::ErrorCode rc;
      if (ct.IsInt()) {  // distinction for convert function
        rc = types::TianmuNum::Parse(rs, tianmu_n, ct.GetTypeName());
        if (rc != common::ErrorCode::SUCCESS) {
          std::string s = "Truncated incorrect numeric value: \'";
          s += rs.ToString();
          s += "\'";
          common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        }
        if (rc == common::ErrorCode::OUT_OF_RANGE && tianmu_n.GetValueInt64() > 0)
          return -1;
      } else if (ct.IsFixed()) {
        rc = types::TianmuNum::ParseNum(rs, tianmu_n, ct.GetScale());
        if (rc != common::ErrorCode::SUCCESS) {
          std::string s = "Truncated incorrect numeric value: \'";
          s += rs.ToString();
          s += "\'";
          common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        }
      } else {
        if (types::TianmuNum::Parse(rs, tianmu_n) != common::ErrorCode::SUCCESS) {
          std::string s = "Truncated incorrect numeric value: \'";
          s += rs.ToString();
          s += "\'";
          common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        }
      }
      return tianmu_n.GetValueInt64();
    }
  }
}

double String2NumCastColumn::GetValueDoubleImpl(const core::MIIterator &mit) {
  if (full_const_)
    return *(double *)&val_;
  else {
    types::BString rs;
    types::TianmuNum tianmu_n;
    vc_->GetValueString(rs, mit);
    if (rs.IsNull()) {
      return NULL_VALUE_D;
    } else {
      types::TianmuNum::Parse(rs, tianmu_n, common::ColumnType::FLOAT);
      // if(types::TianmuNum::Parse(rs, tianmu_n, common::CT::FLOAT) !=
      // common::ErrorCode::SUCCESS) {
      //	std::string s = "Truncated incorrect numeric value: \'";
      //	s += (std::string)rs;
      //  s += "\'";
      //	PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING,
      // ER_TRUNCATED_WRONG_VALUE, s.c_str());
      //}
      return (double)tianmu_n;
    }
  }
}

types::TianmuValueObject String2NumCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (full_const_)
    return rc_value_obj_;
  else {
    types::BString rs;
    types::TianmuNum tianmu_n;
    vc_->GetValueString(rs, mit);
    if (rs.IsNull()) {
      return types::TianmuNum();
    } else if (types::TianmuNum::Parse(rs, tianmu_n) != common::ErrorCode::SUCCESS) {
      std::string s = "Truncated incorrect numeric value: \'";
      s += rs.ToString();
      s += "\'";
      common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
    }
    return tianmu_n;
  }
}

int64_t String2NumCastColumn::GetMinInt64Impl(const core::MIIterator &m) {
  if (full_const_)
    return val_;
  else if (IsConst()) {
    // const with parameters
    types::BString rs;
    types::TianmuNum tianmu_n;
    vc_->GetValueString(rs, m);
    if (rs.IsNull()) {
      return common::NULL_VALUE_64;
    } else if (types::TianmuNum::Parse(rs, tianmu_n) != common::ErrorCode::SUCCESS) {
      std::string s = "Truncated incorrect numeric value: \'";
      s += rs.ToString();
      s += "\'";
      common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
    }
    return tianmu_n.GetValueInt64();
  } else
    return common::MINUS_INF_64;
}

int64_t String2NumCastColumn::GetMaxInt64Impl(const core::MIIterator &m) {
  int64_t v = GetMinInt64Impl(m);
  return v != common::MINUS_INF_64 ? v : common::PLUS_INF_64;
}

String2DateTimeCastColumn::String2DateTimeCastColumn(VirtualColumn *from, core::ColumnType const &to)
    : TypeCastColumn(from, to) {
  full_const_ = vc_->IsFullConst();
  if (full_const_) {
    types::BString rbs;
    core::MIIterator mit(nullptr, PACK_INVALID);
    vc_->GetValueString(rbs, mit);
    if (rbs.IsNull()) {
      val_ = common::NULL_VALUE_64;
      rc_value_obj_ = types::TianmuDateTime();
    } else {
      types::TianmuDateTime tianmu_dt;
      common::ErrorCode rc = types::TianmuDateTime::Parse(rbs, tianmu_dt, TypeName());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += rbs.ToString();
        s += "\'";
        // mysql is pushing this warning anyway, removed duplication
        // PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING,
        // ER_TRUNCATED_WRONG_VALUE, s.c_str());
      }
      val_ = tianmu_dt.GetInt64();
      rc_value_obj_ = tianmu_dt;
    }
  }
}

int64_t String2DateTimeCastColumn::GetNotNullValueInt64(const core::MIIterator &mit) {
  if (full_const_)
    return val_;

  types::TianmuDateTime tianmu_dt;
  types::BString rbs;
  vc_->GetValueString(rbs, mit);
  common::ErrorCode rc = types::TianmuDateTime::Parse(rbs, tianmu_dt, TypeName());
  if (common::IsWarning(rc) || common::IsError(rc)) {
    std::string s = "Incorrect datetime value: \'";
    s += rbs.ToString();
    s += "\'";
    common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
    return common::NULL_VALUE_64;
  }
  return tianmu_dt.GetInt64();
}

int64_t String2DateTimeCastColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  if (full_const_)
    return val_;
  else {
    types::TianmuDateTime tianmu_dt;
    types::BString rbs;
    vc_->GetValueString(rbs, mit);
    if (rbs.IsNull())
      return common::NULL_VALUE_64;
    else {
      common::ErrorCode rc = types::TianmuDateTime::Parse(rbs, tianmu_dt, TypeName());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += rbs.ToString();
        s += "\'";
        common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        return common::NULL_VALUE_64;
      } /* else if(common::IsWarning(res)) {
                           std::string s = "Incorrect Date/Time value: ";
                           s += (std::string)rbs;
                           PushWarning(ConnInfo()->Thd(),
           Sql_condition::SL_WARNING, ER_WARN_INVALID_TIMESTAMP,
           s.c_str());
                   }*/
      return tianmu_dt.GetInt64();
    }
  }
}

void String2DateTimeCastColumn::GetValueStringImpl(types::BString &rbs, const core::MIIterator &mit) {
  if (full_const_)
    rbs = rc_value_obj_.ToBString();
  else {
    types::TianmuDateTime tianmu_dt;
    vc_->GetValueString(rbs, mit);
    if (rbs.IsNull())
      return;
    else {
      common::ErrorCode rc = types::TianmuDateTime::Parse(rbs, tianmu_dt, TypeName());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += rbs.ToString();
        s += "\'";
        common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        rbs = types::BString();
        return;
      } /* else if(common::IsWarning(res)) {
                           std::string s = "Incorrect Date/Time value: ";
                           s += (std::string)rbs;
                           PushWarning(ConnInfo()->Thd(),
           Sql_condition::SL_WARNING, ER_WARN_INVALID_TIMESTAMP,
           s.c_str());
                   }*/
      rbs = tianmu_dt.ToBString();
    }
  }
}

types::TianmuValueObject String2DateTimeCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (full_const_)
    return rc_value_obj_;
  else {
    types::TianmuDateTime tianmu_dt;
    types::BString rbs;
    vc_->GetValueString(rbs, mit);
    if (rbs.IsNull())
      return types::TianmuDateTime();
    else {
      common::ErrorCode rc = types::TianmuDateTime::Parse(rbs, tianmu_dt, TypeName());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += rbs.ToString();
        s += "\'";
        common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        // return types::TianmuDateTime();
      }
      return tianmu_dt;
    }
  }
}

int64_t String2DateTimeCastColumn::GetMinInt64Impl(const core::MIIterator &m) {
  if (full_const_)
    return val_;
  else if (IsConst()) {
    // const with parameters
    return ((types::TianmuDateTime *)GetValueImpl(m).Get())->GetInt64();
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
  core::MIIterator mit(nullptr, PACK_INVALID);
  full_const_ = vc_->IsFullConst();
  if (full_const_) {
    core::MIIterator mit(nullptr, PACK_INVALID);
    val_ = vc_->GetValueInt64(mit);
    // rc_value_obj_ = from->GetValue(mit);
    types::TianmuDateTime tianmu_dt;
    if (val_ != common::NULL_VALUE_64) {
      if (TypeName() == common::ColumnType::TIME) {
        MYSQL_TIME ltime;
        short timehour;
        TIME_from_longlong_time_packed(&ltime, val_);
        short second = ltime.second;
        short minute = ltime.minute;
        if (ltime.neg == 1)
          timehour = ltime.hour * -1;
        else
          timehour = ltime.hour;
        types::TianmuDateTime rctime(timehour, minute, second, common::ColumnType::TIME);
        tianmu_dt = rctime;
      } else {
        common::ErrorCode rc = types::TianmuDateTime::Parse(val_, tianmu_dt, TypeName(), ct.GetPrecision());
        if (common::IsWarning(rc) || common::IsError(rc)) {
          std::string s = "Incorrect datetime value: \'";
          s += rc_value_obj_.ToBString().ToString();
          s += "\'";
          TIANMU_LOG(LogCtl_Level::WARN, "Num2DateTimeCast %s", s.c_str());
        }
        if (TypeName() == common::ColumnType::TIMESTAMP) {
          // needs to convert value to UTC
          MYSQL_TIME myt;
          memset(&myt, 0, sizeof(MYSQL_TIME));
          myt.year = tianmu_dt.Year();
          myt.month = tianmu_dt.Month();
          myt.day = tianmu_dt.Day();
          myt.hour = tianmu_dt.Hour();
          myt.minute = tianmu_dt.Minute();
          myt.second = tianmu_dt.Second();
          myt.time_type = MYSQL_TIMESTAMP_DATETIME;
          if (!common::IsTimeStampZero(myt)) {
            my_bool myb;
            // convert local time to UTC seconds from beg. of EPOCHE
            my_time_t secs_utc = current_txn_->Thd()->variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
            // UTC seconds converted to UTC TIME
            common::GMTSec2GMTTime(&myt, secs_utc);
          }
          tianmu_dt = types::TianmuDateTime(myt, common::ColumnType::TIMESTAMP);
        }
      }

      val_ = tianmu_dt.GetInt64();
    }
    rc_value_obj_ = tianmu_dt;
  }
}

types::TianmuValueObject Num2DateTimeCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (full_const_)
    return rc_value_obj_;
  else {
    types::TianmuDateTime tianmu_dt;
    types::TianmuValueObject r(vc_->GetValue(mit));

    if (!r.IsNull()) {
      common::ErrorCode rc =
          types::TianmuDateTime::Parse(((types::TianmuNum)r).GetIntPart(), tianmu_dt, TypeName(), ct.GetPrecision());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += r.ToBString().ToString();
        s += "\'";

        common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        return types::TianmuDateTime();
      }
      return tianmu_dt;
    } else
      return r;
  }
}

int64_t Num2DateTimeCastColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  if (full_const_)
    return val_;
  else {
    types::TianmuDateTime tianmu_dt;
    int64_t v = vc_->GetValueInt64(mit);
    types::TianmuNum r(v, vc_->Type().GetScale(), vc_->Type().IsFloat(), vc_->Type().GetTypeName());
    if (v != common::NULL_VALUE_64) {
      common::ErrorCode rc = types::TianmuDateTime::Parse(r.GetIntPart(), tianmu_dt, TypeName(), ct.GetPrecision());
      if (common::IsWarning(rc) || common::IsError(rc)) {
        std::string s = "Incorrect datetime value: \'";
        s += r.ToBString().ToString();
        s += "\'";

        common::PushWarning(ConnInfo()->Thd(), Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE, s.c_str());
        return common::NULL_VALUE_64;
      }
      return tianmu_dt.GetInt64();
    } else
      return v;
  }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DateTime2VarcharCastColumn::DateTime2VarcharCastColumn(VirtualColumn *from, core::ColumnType const &to)
    : TypeCastColumn(from, to) {
  core::MIIterator mit(nullptr, PACK_INVALID);
  full_const_ = vc_->IsFullConst();
  if (full_const_) {
    int64_t i = vc_->GetValueInt64(mit);
    if (i == common::NULL_VALUE_64) {
      rc_value_obj_ = types::BString();
    } else {
      types::TianmuDateTime tianmu_dt(i, vc_->TypeName());
      rc_value_obj_ = tianmu_dt.ToBString();
    }
  }
}

types::TianmuValueObject DateTime2VarcharCastColumn::GetValueImpl(const core::MIIterator &mit,
                                                                  [[maybe_unused]] bool b) {
  if (full_const_)
    return rc_value_obj_;
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
  core::MIIterator mit(nullptr, PACK_INVALID);
  full_const_ = vc_->IsFullConst();
  if (full_const_) {
    rc_value_obj_ = vc_->GetValue(mit);
    if (rc_value_obj_.IsNull()) {
      rc_value_obj_ = types::BString();
    } else {
      rc_value_obj_ = rc_value_obj_.ToBString();
    }
  }
}

types::TianmuValueObject Num2VarcharCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (full_const_)
    return rc_value_obj_;
  else {
    types::TianmuValueObject r(vc_->GetValue(mit));
    if (r.IsNull()) {
      return types::BString();
    } else {
      return r.ToBString();
    }
  }
}

void Num2VarcharCastColumn::GetValueStringImpl(types::BString &s, const core::MIIterator &m) {
  if (full_const_)
    s = rc_value_obj_.ToBString();
  else {
    int64_t v = vc_->GetValueInt64(m);
    if (v == common::NULL_VALUE_64)
      s = types::BString();
    else {
      types::TianmuNum rcd(v, vc_->Type().GetScale(), vc_->Type().IsFloat(), vc_->TypeName());
      s = rcd.ToBString();
    }
  }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DateTime2NumCastColumn::DateTime2NumCastColumn(VirtualColumn *from, core::ColumnType const &to)
    : TypeCastColumn(from, to) {
  core::MIIterator mit(nullptr, PACK_INVALID);
  full_const_ = vc_->IsFullConst();
  if (full_const_) {
    rc_value_obj_ = vc_->GetValue(mit);
    if (rc_value_obj_.IsNull()) {
      rc_value_obj_ = types::TianmuNum();
      val_ = common::NULL_VALUE_64;
    } else {
      ((types::TianmuDateTime *)rc_value_obj_.Get())->ToInt64(val_);
      if (vc_->TypeName() == common::ColumnType::YEAR && vc_->Type().GetPrecision() == 2)
        val_ %= 100;
      if (to.IsFloat()) {
        double x = (double)val_;
        val_ = *(int64_t *)&x;
      }
      rc_value_obj_ = types::TianmuNum(val_, ct.GetScale(), ct.IsFloat(), TypeName());
    }
  }
}

int64_t DateTime2NumCastColumn::GetNotNullValueInt64(const core::MIIterator &mit) {
  if (full_const_)
    return val_;
  int64_t v = vc_->GetNotNullValueInt64(mit);
  if (vc_->TypeName() == common::ColumnType::YEAR && vc_->Type().GetPrecision() == 2)
    v %= 100;
  types::TianmuDateTime rdt(v, vc_->TypeName());
  int64_t r;
  rdt.ToInt64(r);
  if (Type().IsFloat()) {
    double x = (double)r;
    r = *(int64_t *)&x;
  }
  return r;
}

int64_t DateTime2NumCastColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  if (full_const_)
    return val_;
  else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64)
      return v;
    types::TianmuDateTime rdt(v, vc_->TypeName());
    int64_t r;
    rdt.ToInt64(r);
    if (vc_->TypeName() == common::ColumnType::YEAR && vc_->Type().GetPrecision() == 2)
      r = r % 100;
    if (Type().IsFloat()) {
      double x = (double)r;
      r = *(int64_t *)&x;
    }
    return r;
  }
}

double DateTime2NumCastColumn::GetValueDoubleImpl(const core::MIIterator &mit) {
  if (full_const_) {
    int64_t v;
    types::TianmuDateTime rdt(val_, vc_->TypeName());
    rdt.ToInt64(v);
    return (double)v;
  } else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64)
      return NULL_VALUE_D;
    types::TianmuDateTime rdt(v, vc_->TypeName());
    rdt.ToInt64(v);
    return (double)v;
  }
}

types::TianmuValueObject DateTime2NumCastColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool b) {
  if (full_const_)
    return rc_value_obj_;
  else {
    types::TianmuValueObject v(vc_->GetValue(mit));
    if (v.IsNull()) {
      return types::TianmuNum();
    } else {
      int64_t r;
      ((types::TianmuDateTime *)v.Get())->ToInt64(r);
      if (vc_->TypeName() == common::ColumnType::YEAR && vc_->Type().GetPrecision() == 2)
        r %= 100;
      if (Type().IsFloat()) {
        double x = (double)r;
        r = *(int64_t *)&x;
      }
      return types::TianmuNum(r, ct.GetScale(), ct.IsFloat(), TypeName());
    }
  }
}

TimeZoneConversionCastColumn::TimeZoneConversionCastColumn(VirtualColumn *from)
    : TypeCastColumn(from, core::ColumnType(common::ColumnType::DATETIME)) {
  DEBUG_ASSERT(from->TypeName() == common::ColumnType::TIMESTAMP);
  core::MIIterator mit(nullptr, PACK_INVALID);
  full_const_ = vc_->IsFullConst();
  if (full_const_) {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64) {
      rc_value_obj_ = types::TianmuDateTime();
      val_ = common::NULL_VALUE_64;
    } else {
      rc_value_obj_ = types::TianmuDateTime(v, vc_->TypeName());
      types::TianmuDateTime::AdjustTimezone(rc_value_obj_);
      val_ = ((types::TianmuDateTime *)rc_value_obj_.Get())->GetInt64();
    }
  }
}

int64_t TimeZoneConversionCastColumn::GetNotNullValueInt64(const core::MIIterator &mit) {
  if (full_const_)
    return val_;
  int64_t v = vc_->GetNotNullValueInt64(mit);
  types::TianmuDateTime rdt(v, vc_->TypeName());
  types::TianmuDateTime::AdjustTimezone(rdt);
  return rdt.GetInt64();
}

int64_t TimeZoneConversionCastColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  if (full_const_)
    return val_;
  else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64)
      return v;
    types::TianmuDateTime rdt(v, vc_->TypeName());
    types::TianmuDateTime::AdjustTimezone(rdt);
    return rdt.GetInt64();
  }
}

types::TianmuValueObject TimeZoneConversionCastColumn::GetValueImpl(const core::MIIterator &mit,
                                                                    [[maybe_unused]] bool b) {
  if (full_const_) {
    if (Type().IsString())
      return rc_value_obj_.ToBString();
    return rc_value_obj_;
  } else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64)
      return types::TianmuDateTime();
    types::TianmuDateTime rdt(v, vc_->TypeName());
    types::TianmuDateTime::AdjustTimezone(rdt);
    if (Type().IsString())
      return rdt.ToBString();
    return rdt;
  }
}

double TimeZoneConversionCastColumn::GetValueDoubleImpl(const core::MIIterator &mit) {
  if (full_const_) {
    int64_t v;
    types::TianmuDateTime rdt(val_, vc_->TypeName());
    rdt.ToInt64(v);
    return (double)v;
  } else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64)
      return NULL_VALUE_D;
    types::TianmuDateTime rdt(v, vc_->TypeName());
    types::TianmuDateTime::AdjustTimezone(rdt);
    rdt.ToInt64(v);
    return (double)v;
  }
}

void TimeZoneConversionCastColumn::GetValueStringImpl(types::BString &s, const core::MIIterator &mit) {
  if (full_const_) {
    s = rc_value_obj_.ToBString();
  } else {
    int64_t v = vc_->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64) {
      s = types::BString();
      return;
    }
    types::TianmuDateTime rdt(v, vc_->TypeName());
    types::TianmuDateTime::AdjustTimezone(rdt);
    s = rdt.ToBString();
  }
}

types::TianmuValueObject StringCastColumn::GetValueImpl(const core::MIIterator &mit,
                                                        [[maybe_unused]] bool lookup_to_num) {
  types::BString s;
  vc_->GetValueString(s, mit);
  return s;
}

}  // namespace vcolumn
}  // namespace Tianmu
