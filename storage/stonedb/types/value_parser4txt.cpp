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
#include "value_parser4txt.h"

#include <cstring>

#include "core/transaction.h"
#include "my_time.h"
#include "system/rc_system.h"

namespace stonedb {
namespace types {
const uint PARS_BUF_SIZE = 128;

static int String2DateTime(const BString &s, RCDateTime &rcdt, common::CT at) {
  MYSQL_TIME myt;
  MYSQL_TIME_STATUS not_used;
  if (str_to_datetime(s.GetDataBytesPointer(), s.len, &myt, TIME_DATETIME_ONLY, &not_used)) {
    return 1;
  }

  if ((at == common::CT::TIMESTAMP) && !validate_timestamp_range(&myt)) {
    rcdt = RCDateTime(0, common::CT::TIMESTAMP);
    return 0;
  }

  rcdt = RCDateTime(myt, at);
  return 0;
}

static inline bool EatWhiteSigns(char *&ptr, int &len) {
  bool vs = false;
  while (len > 0 && isspace((unsigned char)*ptr)) {
    len--;
    ptr++;
    vs = true;
  }
  return vs;
}

static inline common::ErrorCode EatInt64(char *&ptr, int &len, int64_t &out_value) {
  out_value = 0;
  int64_t out_v_tmp = 0;
  common::ErrorCode rc = common::ErrorCode::FAILED;
  bool sing = false;
  if (len > 0 && *ptr == '-') {
    sing = true;
    ptr++;
    len--;
  } else if (len > 0 && *ptr == '+') {
    ptr++;
    len--;
  }

  while (len > 0 && isdigit((unsigned char)*ptr)) {
    out_v_tmp = out_value * 10 + ((ushort)*ptr - '0');
    if (rc != common::ErrorCode::OUT_OF_RANGE) {
      if (out_v_tmp < out_value)
        rc = common::ErrorCode::OUT_OF_RANGE;
      else {
        rc = common::ErrorCode::SUCCESS;
        out_value = out_v_tmp;
      }
    }
    len--;
    ptr++;
  }

  if (sing) out_value *= -1;

  return rc;
}

common::ErrorCode ValueParserForText::ParseNum(const BString &rcs, RCNum &rcn, short scale) {
  // TODO: refactor
  char *val, *val_ptr;
  val = val_ptr = rcs.val;
  int len = rcs.len;
  EatWhiteSigns(val, len);
  if (rcs.Equals("NULL", 4)) {
    rcn.null = true;
    return common::ErrorCode::SUCCESS;
  }
  rcn.null = false;
  rcn.dbl = false;
  rcn.m_scale = 0;

  int ptr_len = len;
  val_ptr = val;
  ushort no_digs = 0;
  ushort no_digs_after_dot = 0;
  ptr_len = len;
  val_ptr = val;
  bool has_dot = false;
  bool has_unexpected_sign = false;
  int64_t v = 0;
  short sign = 1;
  bool last_set = false;
  short last = 0;
  common::ErrorCode sdbret = common::ErrorCode::SUCCESS;
  if (ptr_len > 0 && *val_ptr == '-') {
    val_ptr++;
    ptr_len--;
    sign = -1;
  }
  while (ptr_len > 0) {
    if (isdigit((uchar)*val_ptr)) {
      no_digs++;
      if (has_dot) no_digs_after_dot++;

      if ((no_digs <= 18 && scale < 0) || (no_digs <= 18 && no_digs_after_dot <= scale && scale >= 0)) {
        v *= 10;
        v += *val_ptr - '0';
        last = *val_ptr - '0';
      } else {
        no_digs--;
        if (has_dot) {
          no_digs_after_dot--;
          if (*val_ptr != '0') sdbret = common::ErrorCode::VALUE_TRUNCATED;
          if (!last_set) {
            last_set = true;
            if (*val_ptr > '4') {
              last += 1;
              v = (v / 10) * 10 + last;
            }
          }
        } else {
          rcn.value = (Uint64PowOfTen(18) - 1) * sign;
          rcn.m_scale = 0;
          return common::ErrorCode::OUT_OF_RANGE;
        }
      }
    } else if (*val_ptr == '.' && !has_dot) {
      has_dot = true;
      if (v == 0) no_digs = 0;
    } else if (isspace((uchar)*val_ptr)) {
      EatWhiteSigns(val_ptr, ptr_len);
      if (ptr_len > 0) has_unexpected_sign = true;
      break;
    } else if (*val_ptr == 'd' || *val_ptr == 'D' || *val_ptr == 'e' || *val_ptr == 'E') {
      sdbret = RCNum::ParseReal(rcs, rcn, common::CT::REAL);
      rcn = rcn.ToDecimal(scale);
      return sdbret;
    } else {
      has_unexpected_sign = true;
      break;
    }
    ptr_len--;
    val_ptr++;
  }
  if (scale != -1) {
    while (scale > no_digs_after_dot) {
      v *= 10;
      no_digs_after_dot++;
      no_digs++;
    }
    while (scale < no_digs_after_dot) {
      v /= 10;
      no_digs_after_dot--;
      no_digs--;
    }
  } else {
    scale = no_digs_after_dot;
  }

  if (no_digs > 18)
    rcn.value = (Uint64PowOfTen(18) - 1) * sign;
  else
    rcn.value = v * sign;
  rcn.m_scale = scale;
  if (has_unexpected_sign || no_digs > 18) return common::ErrorCode::VALUE_TRUNCATED;
  return sdbret;
}

common::ErrorCode ValueParserForText::Parse(const BString &rcs, RCNum &rcn, common::CT at) {
  if (at == common::CT::BIGINT) return ParseBigInt(rcs, rcn);

  // TODO: refactor
  char *val, *val_ptr;
  val = val_ptr = rcs.val;
  int len = rcs.len;
  EatWhiteSigns(val, len);
  int ptr_len = len;
  val_ptr = val;
  short scale = -1;

  if (at == common::CT::UNK) {
    bool has_dot = false;
    bool has_exp = false;
    bool has_unexpected_sign = false;
    bool can_be_minus = true;
    ushort no_digs = 0;
    ushort no_digs_after_dot = 0;
    int exponent = 0;
    int64_t v = 0;
    while (ptr_len > 0) {
      if (can_be_minus && *val_ptr == '-') {
        can_be_minus = false;
      } else if (*val_ptr == '.' && !has_dot && !has_exp) {
        has_dot = true;
        can_be_minus = false;
        if (v == 0) no_digs = 0;
      } else if (!has_exp && (*val_ptr == 'd' || *val_ptr == 'D' || *val_ptr == 'e' || *val_ptr == 'E')) {
        val_ptr++;
        ptr_len--;
        can_be_minus = true;
        has_exp = true;
        int tmp_len = ptr_len;
        common::ErrorCode sdbrc = system::EatInt(val_ptr, ptr_len, exponent);
        if (sdbrc == common::ErrorCode::SUCCESS) return common::ErrorCode::FAILED;
        if (tmp_len != ptr_len) {
          can_be_minus = false;
          if (exponent == 0 && tmp_len - ptr_len == 1) {
            has_unexpected_sign = true;
            break;
          } else
            no_digs += std::abs(exponent);
        }
      } else if (isspace((uchar)*val_ptr)) {
        EatWhiteSigns(val_ptr, ptr_len);
        if (ptr_len > 0) has_unexpected_sign = true;
        break;
      } else if (!isdigit((uchar)*val_ptr)) {
        has_unexpected_sign = true;
        break;
      } else {
        no_digs++;
        if (has_dot) no_digs_after_dot++;
        v *= 10;
        v += *val_ptr - '0';
      }
      val_ptr++;
      ptr_len--;
    }

    if (has_unexpected_sign) {
      // same as innodb , string convert to 0
      rcn.null = false;
      rcn.attrt = at;
      rcn.value = 0;
      return common::ErrorCode::VALUE_TRUNCATED;
    }

    if (has_exp)
      at = common::CT::REAL;
    else {
      if (has_dot) {
        if (std::abs((no_digs - no_digs_after_dot) + exponent) + std::abs(no_digs_after_dot - exponent) <= 18) {
          scale = std::abs(no_digs_after_dot - exponent);
          at = common::CT::NUM;
        } else
          at = common::CT::REAL;
      } else {
        if (no_digs <= 18 && no_digs > 9) {
          at = common::CT::NUM;
          scale = 0;
        } else if (no_digs > 18)
          at = common::CT::REAL;
        else
          at = common::CT::INT;
      }
    }
  } else {
    if (!core::ATI::IsNumericType(at)) return common::ErrorCode::FAILED;
  }

  rcn.null = false;
  rcn.attrt = at;
  if (core::ATI::IsRealType(at)) {
    rcn.dbl = true;
    rcn.m_scale = 0;
  } else
    rcn.dbl = false;

  if (core::ATI::IsRealType(at))
    return ValueParserForText::ParseReal(rcs, rcn, at);
  else if (at == common::CT::NUM)
    return ValueParserForText::ParseNum(rcs, rcn, scale);

  rcn.m_scale = 0;
  rcn.dbl = false;

  if (rcs.Equals("NULL", 4)) {
    rcn.null = true;
    return common::ErrorCode::SUCCESS;
  }

  common::ErrorCode ret = common::ErrorCode::SUCCESS;

  ptr_len = len;
  val_ptr = val;
  EatWhiteSigns(val_ptr, ptr_len);
  if (core::ATI::IsIntegerType(at)) {
    ret = ParseBigInt(rcs, rcn);
    int64_t v = rcn.value;

    if (at == common::CT::BYTEINT) {
      if (v > SDB_TINYINT_MAX) {
        v = SDB_TINYINT_MAX;
        ret = common::ErrorCode::OUT_OF_RANGE;
      } else if (v < SDB_TINYINT_MIN) {
        v = SDB_TINYINT_MIN;
        ret = common::ErrorCode::OUT_OF_RANGE;
      }
    } else if (at == common::CT::SMALLINT) {
      if (v > SDB_SMALLINT_MAX) {
        v = SDB_SMALLINT_MAX;
        ret = common::ErrorCode::OUT_OF_RANGE;
      } else if (v < SDB_SMALLINT_MIN) {
        v = SDB_SMALLINT_MIN;
        ret = common::ErrorCode::OUT_OF_RANGE;
      }
    } else if (at == common::CT::MEDIUMINT) {
      if (v > SDB_MEDIUMINT_MAX) {
        v = SDB_MEDIUMINT_MAX;
        ret = common::ErrorCode::OUT_OF_RANGE;
      } else if (v < SDB_MEDIUMINT_MIN) {
        v = SDB_MEDIUMINT_MIN;
        ret = common::ErrorCode::OUT_OF_RANGE;
      }
    } else if (at == common::CT::INT) {
      if (v > std::numeric_limits<int>::max()) {
        v = std::numeric_limits<int>::max();
        ret = common::ErrorCode::OUT_OF_RANGE;
      } else if (v < SDB_INT_MIN) {
        v = SDB_INT_MIN;
        ret = common::ErrorCode::OUT_OF_RANGE;
      }
    }
    rcn.dbl = false;
    rcn.m_scale = 0;
    rcn.value = v;
  }
  return ret;
}

common::ErrorCode ValueParserForText::ParseReal(const BString &rcbs, RCNum &rcn, common::CT at) {
  // TODO: refactor
  if (at == common::CT::UNK) at = common::CT::REAL;
  if (!core::ATI::IsRealType(at)) return common::ErrorCode::FAILED;

  if (rcbs.Equals("NULL", 4) || rcbs.IsNull()) {
    rcn.null = true;
    return common::ErrorCode::SUCCESS;
  }
  rcn.dbl = true;
  rcn.m_scale = 0;

  char *val = rcbs.val;
  int len = rcbs.len;
  EatWhiteSigns(val, len);

  char *val_ptr = val;
  int ptr_len = len;

  bool has_dot = false;
  bool has_exp = false;
  bool can_be_minus = true;

  common::ErrorCode ret = common::ErrorCode::SUCCESS;

  while (ptr_len > 0) {
    if (can_be_minus && *val_ptr == '-') {
      can_be_minus = false;
    } else if (*val_ptr == '.' && !has_dot && !has_exp) {
      has_dot = true;
      can_be_minus = false;
    } else if (!has_exp && (*val_ptr == 'd' || *val_ptr == 'D' || *val_ptr == 'e' || *val_ptr == 'E')) {
      val_ptr++;
      ptr_len--;
      can_be_minus = true;
      has_exp = true;
      int exponent = 0;
      if (system::EatInt(val_ptr, ptr_len, exponent) != common::ErrorCode::SUCCESS) {
        ret = common::ErrorCode::VALUE_TRUNCATED;
        break;
      }
    } else if (isspace((uchar)*val_ptr)) {
      EatWhiteSigns(val_ptr, ptr_len);
      if (ptr_len > 0) ret = common::ErrorCode::VALUE_TRUNCATED;
      break;
    } else if (!isdigit((uchar)*val_ptr)) {
      ret = common::ErrorCode::VALUE_TRUNCATED;
      break;
    }
    val_ptr++;
    ptr_len--;
  }
  char stempval[PARS_BUF_SIZE];
  if (rcbs.len >= PARS_BUF_SIZE) return common::ErrorCode::VALUE_TRUNCATED;
#ifndef NDEBUG
  // resetting stempval to avoid valgrind
  // false warnings
  std::memset(stempval, 0, PARS_BUF_SIZE);
#endif
  std::memcpy(stempval, rcbs.val, rcbs.len);
  stempval[rcbs.len] = 0;
  double d = 0.0;
  try {
    d = std::stod(std::string(stempval));
  } catch (...) {
    d = std::atof(stempval);
  }

  if (fabs(d) == 0.0)
    d = 0.0;  // convert -0.0 to 0.0
  else if ((std::isnan)(d)) {
    d = 0.0;
    ret = common::ErrorCode::OUT_OF_RANGE;
  }

  rcn.attrt = at;
  rcn.null = false;
  if (at == common::CT::REAL) {
    if (d > DBL_MAX) {
      d = DBL_MAX;
      ret = common::ErrorCode::OUT_OF_RANGE;
    } else if (d < -DBL_MAX) {
      d = -DBL_MAX;
      ret = common::ErrorCode::OUT_OF_RANGE;
    } else if (d > 0 && d < DBL_MIN) {
      d = /*DBL_MIN*/ 0;
      ret = common::ErrorCode::OUT_OF_RANGE;
    } else if (d < 0 && d > -DBL_MIN) {
      d = /*DBL_MIN*/ 0 * -1;
      ret = common::ErrorCode::OUT_OF_RANGE;
    }
    *(double *)&rcn.value = d;
  } else if (at == common::CT::FLOAT) {
    if (d > FLT_MAX) {
      d = FLT_MAX;
      ret = common::ErrorCode::OUT_OF_RANGE;
    } else if (d < -FLT_MAX) {
      d = -FLT_MAX;
      ret = common::ErrorCode::OUT_OF_RANGE;
    } else if (d > 0 && d < FLT_MIN) {
      d = 0;
      ret = common::ErrorCode::OUT_OF_RANGE;
    } else if (d < 0 && d > FLT_MIN * -1) {
      d = 0 * -1;
      ret = common::ErrorCode::OUT_OF_RANGE;
    }
    *(double *)&rcn.value = d;
  }
  return ret;
}

common::ErrorCode ValueParserForText::ParseBigInt(const BString &rcs, RCNum &rcn) {
  char *val_ptr = rcs.val;
  int len = rcs.len;
  int ptr_len = len;
  common::ErrorCode ret = common::ErrorCode::SUCCESS;

  rcn.null = false;
  rcn.attrt = common::CT::BIGINT;
  rcn.m_scale = 0;
  rcn.dbl = false;

  if (rcs.Equals("NULL", 4)) {
    rcn.null = true;
    return common::ErrorCode::SUCCESS;
  }
  int64_t v = 0;
  EatWhiteSigns(val_ptr, ptr_len);
  bool is_negative = false;
  if (ptr_len > 0 && *val_ptr == '-') {
    val_ptr++;
    ptr_len--;
    is_negative = true;
  }
  int64_t temp_v = 0;
  common::ErrorCode rc = EatInt64(val_ptr, ptr_len, temp_v);
  if (rc != common::ErrorCode::SUCCESS) {
    if (rc == common::ErrorCode::OUT_OF_RANGE) {
      ret = rc;
      v = common::SDB_BIGINT_MAX;
    } else {
      ret = common::ErrorCode::VALUE_TRUNCATED;
      v = 0;
    }
  } else {
    if (ptr_len) {
      if (*val_ptr == 'd' || *val_ptr == 'D' || *val_ptr == 'e' || *val_ptr == 'E') {
        ret = RCNum::ParseReal(rcs, rcn, common::CT::REAL);
        if (rcn.GetDecFractLen() != 0 || ret != common::ErrorCode::SUCCESS) ret = common::ErrorCode::VALUE_TRUNCATED;
        if (rcn.GetIntPartAsDouble() > common::SDB_BIGINT_MAX) {
          v = common::SDB_BIGINT_MAX;
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else if (rcn.GetIntPartAsDouble() < common::SDB_BIGINT_MIN) {
          v = common::SDB_BIGINT_MIN;
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else {
          v = (int64_t)rcn;
        }
        is_negative = false;
      } else {
        if (*val_ptr == '.') {
          val_ptr++;
          ptr_len--;
          v = temp_v;
          if (ptr_len) {
            int tmp_ptr_len = ptr_len;
            int fract = 0;
            if (system::EatInt(val_ptr, ptr_len, fract) != common::ErrorCode::SUCCESS)
              ret = common::ErrorCode::VALUE_TRUNCATED;
            else if (*val_ptr == 'e' || *val_ptr == 'E') {
              ret = RCNum::ParseReal(rcs, rcn, common::CT::REAL);
              if (rcn.GetDecFractLen() != 0 || ret != common::ErrorCode::SUCCESS)
                ret = common::ErrorCode::VALUE_TRUNCATED;
              if (rcn.GetIntPartAsDouble() > common::SDB_BIGINT_MAX) {
                v = common::SDB_BIGINT_MAX;
                ret = common::ErrorCode::OUT_OF_RANGE;
              } else if (rcn.GetIntPartAsDouble() < common::SDB_BIGINT_MIN) {
                v = common::SDB_BIGINT_MIN;
                ret = common::ErrorCode::OUT_OF_RANGE;
              } else {
                v = (int64_t)rcn;
              }
              is_negative = false;
            } else {
              if ((tmp_ptr_len - ptr_len >= 1) &&
                  Uint64PowOfTenMultiply5((tmp_ptr_len - ptr_len) - 1) <= static_cast<uint64_t>(fract))
                v++;
              ret = common::ErrorCode::VALUE_TRUNCATED;
            }
          }
        } else {
          EatWhiteSigns(val_ptr, ptr_len);
          v = temp_v;
          if (ptr_len) {
            ret = common::ErrorCode::VALUE_TRUNCATED;
          }
        }
      }
    } else
      v = temp_v;
  }

  if (is_negative) v *= -1;

  if (v > common::SDB_BIGINT_MAX) {
    v = common::SDB_BIGINT_MAX;
    ret = common::ErrorCode::OUT_OF_RANGE;
  } else if (v < common::SDB_BIGINT_MIN) {
    v = common::SDB_BIGINT_MIN;
    ret = common::ErrorCode::OUT_OF_RANGE;
  }
  rcn.dbl = false;
  rcn.m_scale = 0;
  rcn.value = v;
  return ret;
}

common::ErrorCode ValueParserForText::ParseNumeric(BString const &rcs, int64_t &out, common::CT at) {
  RCNum number;
  common::ErrorCode return_code = Parse(rcs, number, at);
  out = number.ValueInt();
  return return_code;
}

common::ErrorCode ValueParserForText::ParseBigIntAdapter(const BString &rcs, int64_t &out) {
  RCNum number;
  common::ErrorCode return_code = ParseBigInt(rcs, number);
  out = number.ValueInt();
  return return_code;
}

common::ErrorCode ValueParserForText::ParseDecimal(BString const &rcs, int64_t &out, short precision, short scale) {
  RCNum number;
  common::ErrorCode return_code = ParseNum(rcs, number, scale);
  out = number.ValueInt();
  if (out > 0 && out >= (int64_t)Uint64PowOfTen(precision)) {
    out = Uint64PowOfTen(precision) - 1;
    return_code = common::ErrorCode::OUT_OF_RANGE;
  } else if (out < 0 && out <= (int64_t)Uint64PowOfTen(precision) * -1) {
    out = Uint64PowOfTen(precision) * -1 + 1;
    return_code = common::ErrorCode::OUT_OF_RANGE;
  }
  return return_code;
}

common::ErrorCode ValueParserForText::ParseDateTimeOrTimestamp(const BString &rcs, RCDateTime &rcv, common::CT at) {
  if (rcs.IsNull() || rcs.Equals("NULL", 4)) {
    rcv.at = at;
    rcv.null = true;
    return common::ErrorCode::SUCCESS;
  }
  char *buf = rcs.val;
  int buflen = rcs.len;
  EatWhiteSigns(buf, buflen);

  if (buflen == 0) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (String2DateTime(rcs, rcv, at) == 0) {
    if (at == common::CT::TIMESTAMP) {
      // convert to UTC
      MYSQL_TIME myt;
      std::memset(&myt, 0, sizeof(MYSQL_TIME));
      myt.year = rcv.Year();
      myt.month = rcv.Month();
      myt.day = rcv.Day();
      myt.hour = rcv.Hour();
      myt.minute = rcv.Minute();
      myt.second = rcv.Second();
      myt.second_part = rcv.MicroSecond();
      myt.time_type = MYSQL_TIMESTAMP_DATETIME;
      if (!common::IsTimeStampZero(myt)) {
        my_bool myb;
        my_time_t secs_utc = current_tx->Thd()->variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
        common::GMTSec2GMTTime(&myt, secs_utc);
        myt.second_part = rcv.MicroSecond();
      }
      rcv = RCDateTime(myt, common::CT::TIMESTAMP);
    }
    return common::ErrorCode::SUCCESS;
  }

  uint64_t year = 0;
  uint month = 0, day = 0, hour = 0, minute = 0, second = 0, microsecond = 0;
  common::ErrorCode sdbrc = system::EatUInt64(buf, buflen, year);
  EatWhiteSigns(buf, buflen);
  if (sdbrc == common::ErrorCode::FAILED) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (sdbrc != common::ErrorCode::OUT_OF_RANGE && buflen != 0) year = RCDateTime::ToCorrectYear((uint)year, at);

  if (buflen == 0) return RCDateTime::Parse(year, rcv, at);

  if (!system::EatDTSeparators(buf, buflen)) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }
  if (system::EatUInt(buf, buflen, month) == common::ErrorCode::FAILED) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }
  if (!system::EatDTSeparators(buf, buflen)) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (system::EatUInt(buf, buflen, day) == common::ErrorCode::FAILED) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (!RCDateTime::CanBeDate(year, month, day)) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (!EatWhiteSigns(buf, buflen) && !system::EatDTSeparators(buf, buflen)) {
    if ((at == common::CT::DATETIME &&
         RCDateTime::IsCorrectSDBDatetime((short)year, month, day, RCDateTime::GetSpecialValue(at).Hour(),
                                          RCDateTime::GetSpecialValue(at).Minute(),
                                          RCDateTime::GetSpecialValue(at).Second())) ||
        (at == common::CT::TIMESTAMP &&
         RCDateTime::IsCorrectSDBTimestamp((short)year, month, day, RCDateTime::GetSpecialValue(at).Hour(),
                                           RCDateTime::GetSpecialValue(at).Minute(),
                                           RCDateTime::GetSpecialValue(at).Second())))
      rcv = RCDateTime((short)year, month, day, RCDateTime::GetSpecialValue(at).Hour(),
                       RCDateTime::GetSpecialValue(at).Minute(), RCDateTime::GetSpecialValue(at).Second(), at);
    return common::ErrorCode::OUT_OF_RANGE;
  }

  bool eat1 = true, eat2 = true;
  while (eat1 || eat2) {
    eat1 = system::EatDTSeparators(buf, buflen);
    eat2 = EatWhiteSigns(buf, buflen);
  }
  sdbrc = common::ErrorCode::SUCCESS;
  sdbrc = system::EatUInt(buf, buflen, hour);
  if (!common::IsError(sdbrc)) {
    if (!RCDateTime::CanBeHour(hour)) {
      rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
      return common::ErrorCode::OUT_OF_RANGE;
    } else if (!system::EatDTSeparators(buf, buflen)) {
      sdbrc = common::ErrorCode::FAILED;
    }
  } else
    hour = RCDateTime::GetSpecialValue(at).Hour();
  if (!common::IsError(sdbrc)) sdbrc = system::EatUInt(buf, buflen, minute);
  if (!common::IsError(sdbrc)) {
    if (!RCDateTime::CanBeMinute(minute)) {
      rcv = RCDateTime((short)year, (short)month, (short)day, (short)hour, (short)minute, (short)second, at);
      return common::ErrorCode::OUT_OF_RANGE;
    } else if (!system::EatDTSeparators(buf, buflen)) {
      rcv = RCDateTime((short)year, (short)month, (short)day, (short)hour, (short)minute, (short)second, at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
  } else
    minute = RCDateTime::GetSpecialValue(at).Minute();
  if (!common::IsError(sdbrc)) sdbrc = system::EatUInt(buf, buflen, second);
  if (!common::IsError(sdbrc)) {
    if (!RCDateTime::CanBeSecond(second)) {
      rcv = RCDateTime((short)year, (short)month, (short)day, (short)hour, (short)minute, (short)second, at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
  } else
    second = RCDateTime::GetSpecialValue(at).Second();
  if (sdbrc == common::ErrorCode::FAILED) sdbrc = common::ErrorCode::OUT_OF_RANGE;

  EatWhiteSigns(buf, buflen);

  short add_hours = 0;
  if (buflen >= 2) {
    if (strncasecmp(buf, "PM", 2) == 0) {
      add_hours = 12;
      buf += 2;
      buflen -= 2;
    } else if (strncasecmp(buf, "AM", 2) == 0) {
      buf += 2;
      buflen -= 2;
    }
  }
  hour += add_hours;
  EatWhiteSigns(buf, buflen);

  try {
    if (at == common::CT::DATETIME) {
      if (RCDateTime::IsCorrectSDBDatetime((short)year, (short)month, (short)day, (short)hour, (short)minute,
                                           (short)second)) {
        rcv = RCDateTime((short)year, (short)month, (short)day, (short)hour, (short)minute, (short)second, at);
        return sdbrc;
      } else {
        rcv = RCDateTime((short)year, (short)month, (short)day, (short)RCDateTime::GetSpecialValue(at).Hour(),
                         (short)RCDateTime::GetSpecialValue(at).Minute(),
                         (short)RCDateTime::GetSpecialValue(at).Second(), at);
        sdbrc = common::ErrorCode::OUT_OF_RANGE;
      }
    } else if (at == common::CT::TIMESTAMP) {
      if (RCDateTime::IsCorrectSDBTimestamp((short)year, (short)month, (short)day, (short)hour, (short)minute,
                                            (short)second)) {
        // convert to UTC
        MYSQL_TIME myt;
        std::memset(&myt, 0, sizeof(MYSQL_TIME));
        myt.year = year;
        myt.month = month;
        myt.day = day;
        myt.hour = hour;
        myt.minute = minute;
        myt.second = second;
        myt.second_part = microsecond;
        myt.time_type = MYSQL_TIMESTAMP_DATETIME;
        if (!common::IsTimeStampZero(myt)) {
          my_bool myb;
          my_time_t secs_utc = current_tx->Thd()->variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
          common::GMTSec2GMTTime(&myt, secs_utc);
          myt.second_part = microsecond;
        }
        rcv = RCDateTime(myt, common::CT::TIMESTAMP);
        return sdbrc;
      } else {
        rcv = RC_TIMESTAMP_SPEC;
        sdbrc = common::ErrorCode::OUT_OF_RANGE;
      }
    }

    if (buflen != 0) return common::ErrorCode::OUT_OF_RANGE;
    return sdbrc;
  } catch (common::DataTypeConversionException &) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }
  return common::ErrorCode::FAILED;
}

common::ErrorCode ValueParserForText::ParseTime(const BString &rcs, RCDateTime &rcv) {
  if (rcs.IsNull() || rcs.Equals("NULL", 4)) {
    rcv.at = common::CT::TIME;
    rcv.null = true;
    return common::ErrorCode::SUCCESS;
  }
  char *buf = rcs.val;
  int buflen = rcs.len;
  EatWhiteSigns(buf, buflen);
  if (buflen == 0) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::TIME));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  uint64_t tmp = 0;
  int hour = 0;
  int minute = 0;
  uint64_t second = 0;
  int sign = 1;
  bool colon = false;
  if (buflen > 0 && *buf == '-') {
    sign = -1;
    buf++;
    buflen--;
  }
  EatWhiteSigns(buf, buflen);
  if (buflen > 0 && *buf == '-') {
    sign = -1;
    buf++;
    buflen--;
  }
  EatWhiteSigns(buf, buflen);
  common::ErrorCode sdbrc = system::EatUInt64(buf, buflen, tmp);
  if (sdbrc == common::ErrorCode::FAILED && buflen > 0 && *buf == ':') {  // e.g. :12:34
    colon = true;
  } else if (sdbrc != common::ErrorCode::SUCCESS) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::TIME));
    return common::ErrorCode::VALUE_TRUNCATED;
  } else if (sdbrc == common::ErrorCode::SUCCESS) {
    second = tmp;
    EatWhiteSigns(buf, buflen);
    if (buflen == 0) {  // e.g. 235959
      return RCDateTime::Parse(second * sign, rcv, common::CT::TIME);
    }
  }

  if (buflen > 0 && *buf == ':') {
    buf++;
    buflen--;
    EatWhiteSigns(buf, buflen);
    uint tmp2 = 0;
    sdbrc = system::EatUInt(buf, buflen, tmp2);
    if (!common::IsError(sdbrc)) {  // e.g. 01:02
      EatWhiteSigns(buf, buflen);
      if (colon) {  // starts with ':'
        if (!RCDateTime::CanBeMinute(tmp2)) {
          rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::TIME));
          return common::ErrorCode::VALUE_TRUNCATED;
        }
        minute = tmp2;
        hour = 0;
        EatWhiteSigns(buf, buflen);
        if (buflen > 0 && *buf == ':') {
          buf++;
          buflen--;
          EatWhiteSigns(buf, buflen);
          sdbrc = system::EatUInt(buf, buflen, tmp2);
          if (sdbrc == common::ErrorCode::SUCCESS && RCDateTime::CanBeSecond(tmp2))
            second = tmp2;
          else {
            second = RCDateTime::GetSpecialValue(common::CT::TIME).Second();
            sdbrc = common::ErrorCode::VALUE_TRUNCATED;
          }
        } else {  // e.g. :01:
        }
      } else {  // e.g. 01:02:03
        hour = (int)second;
        if (!RCDateTime::CanBeMinute(tmp2)) {
          rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::TIME));
          return common::ErrorCode::VALUE_TRUNCATED;
        }
        minute = tmp2;
        second = 0;
        if (buflen > 0 && *buf == ':') {
          buf++;
          buflen--;
          sdbrc = system::EatUInt(buf, buflen, tmp2);
          if (sdbrc == common::ErrorCode::SUCCESS && RCDateTime::CanBeSecond(tmp2))
            second = tmp2;
          else {
            second = RCDateTime::GetSpecialValue(common::CT::TIME).Second();
            sdbrc = common::ErrorCode::VALUE_TRUNCATED;
          }
        }
      }
      EatWhiteSigns(buf, buflen);
      short add_hours = 0;
      if (buflen >= 2) {
        if (strncasecmp(buf, "PM", 2) == 0) {
          add_hours = 12;
          buf += 2;
          buflen -= 2;
        } else if (strncasecmp(buf, "AM", 2) == 0) {
          buf += 2;
          buflen -= 2;
        }
      }

      hour += add_hours;
      EatWhiteSigns(buf, buflen);

      if (sdbrc != common::ErrorCode::SUCCESS || buflen != 0) sdbrc = common::ErrorCode::VALUE_TRUNCATED;

      if (RCDateTime::IsCorrectSDBTime(short(hour * sign), short(minute * sign), short(second * sign))) {
        rcv = RCDateTime(short(hour * sign), short(minute * sign), short(second * sign), common::CT::TIME);
        return sdbrc;
      } else {
        if (hour * sign < -RC_TIME_MIN.Hour())
          rcv = RCDateTime(RC_TIME_MIN);
        else if (hour * sign > RC_TIME_MAX.Hour())
          rcv = RCDateTime(RC_TIME_MAX);
        else
          DEBUG_ASSERT(0);  // hmmm... ????

        return common::ErrorCode::OUT_OF_RANGE;
      }
    } else {                            // e.g. 01:a... or 01::... only second is set
      if (buflen > 0 && *buf == ':') {  // 01::
        EatWhiteSigns(buf, buflen);
        sdbrc = system::EatUInt(buf, buflen, tmp2);
        if (sdbrc == common::ErrorCode::SUCCESS) {
          hour = (int)second;
          if (!RCDateTime::CanBeSecond(tmp2)) {
            rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::TIME));
            return common::ErrorCode::VALUE_TRUNCATED;
          }
          second = tmp2;
        }
      } else
        return RCDateTime::Parse(second * sign, rcv, common::CT::TIME);
      if (RCDateTime::IsCorrectSDBTime(short(hour * sign), short(minute * sign), short(second * sign))) {
        rcv = RCDateTime(short(hour * sign), short(minute * sign), short(second * sign), common::CT::TIME);
        return common::ErrorCode::VALUE_TRUNCATED;
      } else {
        rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::TIME));
        return common::ErrorCode::VALUE_TRUNCATED;
      }
    }
  } else {
    if (RCDateTime::IsCorrectSDBTime(short(hour * sign), short(minute * sign), short(second * sign))) {
      rcv = RCDateTime(short(hour * sign), short(minute * sign), short(second * sign), common::CT::TIME);
      return common::ErrorCode::VALUE_TRUNCATED;
    } else {
      rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::TIME));
      return common::ErrorCode::VALUE_TRUNCATED;
    }
  }
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode ValueParserForText::ParseDate(const BString &rcs, RCDateTime &rcv) {
  if (rcs.IsNull() || rcs.Equals("NULL", 4)) {
    rcv.at = common::CT::DATE;
    rcv.null = true;
    return common::ErrorCode::SUCCESS;
  }
  char *buf = rcs.val;
  int buflen = rcs.len;
  EatWhiteSigns(buf, buflen);
  if (buflen == 0) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::DATE));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (buflen > 0 && *buf == '-') {
    rcv = RCDateTime::GetSpecialValue(common::CT::DATE);
    return common::ErrorCode::VALUE_TRUNCATED;
  }

  uint year = 0, month = 0, day = 0;
  common::ErrorCode sdbrc = system::EatUInt(buf, buflen, year);
  if (sdbrc == common::ErrorCode::FAILED) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  } else if (sdbrc != common::ErrorCode::OUT_OF_RANGE && buflen > 0) {
    year = RCDateTime::ToCorrectYear(year, common::CT::DATE);
  }

  if (buflen == 0) return RCDateTime::Parse((int64_t)year, rcv, common::CT::DATE);

  EatWhiteSigns(buf, buflen);
  if (!system::EatDTSeparators(buf, buflen)) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  }
  EatWhiteSigns(buf, buflen);
  if (system::EatUInt(buf, buflen, month) == common::ErrorCode::FAILED) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  }
  EatWhiteSigns(buf, buflen);
  if (!system::EatDTSeparators(buf, buflen)) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  }
  EatWhiteSigns(buf, buflen);
  if (system::EatUInt(buf, buflen, day) == common::ErrorCode::FAILED) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  }
  EatWhiteSigns(buf, buflen);
  if (!RCDateTime::CanBeDate(year, month, day)) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  } else
    rcv = RCDateTime(year, month, day, common::CT::DATE);
  if (buflen != 0) return common::ErrorCode::VALUE_TRUNCATED;
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode ValueParserForText::ParseYear(const BString &rcs, RCDateTime &rcv) {
  if (rcs.IsNull() || rcs.Equals("NULL", 4)) {
    rcv.at = common::CT::YEAR;
    rcv.null = true;
    return common::ErrorCode::SUCCESS;
  }
  char *buf = rcs.val;
  int buflen = rcs.len;
  EatWhiteSigns(buf, buflen);
  if (buflen == 0) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::YEAR));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (buflen > 0 && *buf == '-') {
    rcv = RCDateTime::GetSpecialValue(common::CT::YEAR);
    return common::ErrorCode::VALUE_TRUNCATED;
  }
  uint year = 0;
  int tmp_buf_len = buflen;
  common::ErrorCode sdbrc = system::EatUInt(buf, buflen, year);
  if (sdbrc != common::ErrorCode::SUCCESS) {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::YEAR));
    return common::ErrorCode::VALUE_TRUNCATED;
  } else if (year == 0 && (tmp_buf_len - buflen) < 4) {
    year = 2000;
  } else {
    year = RCDateTime::ToCorrectYear(year, common::CT::YEAR);
  }

  if (RCDateTime::IsCorrectSDBYear(year)) {
    EatWhiteSigns(buf, buflen);
    rcv = RCDateTime(year);
    if (buflen) return common::ErrorCode::OUT_OF_RANGE;
    return common::ErrorCode::SUCCESS;
  } else {
    rcv = RCDateTime(RCDateTime::GetSpecialValue(common::CT::YEAR));
    return common::ErrorCode::OUT_OF_RANGE;
  }
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode ValueParserForText::ParseDateTime(const BString &rcs, RCDateTime &rcv, common::CT at) {
  DEBUG_ASSERT(core::ATI::IsDateTimeType(at));
  switch (at) {
    case common::CT::TIMESTAMP:
    case common::CT::DATETIME:
      return ValueParserForText::ParseDateTimeOrTimestamp(rcs, rcv, at);
    case common::CT::TIME:
      return ValueParserForText::ParseTime(rcs, rcv);
    case common::CT::DATE:
      return ValueParserForText::ParseDate(rcs, rcv);
    default:  // case common::CT::YEAR :
      return ValueParserForText::ParseYear(rcs, rcv);
  }
}

common::ErrorCode ValueParserForText::ParseDateTimeAdapter(BString const &rcs, int64_t &out, common::CT at) {
  RCDateTime dt;
  common::ErrorCode return_code = ValueParserForText::ParseDateTime(rcs, dt, at);
  out = dt.GetInt64();
  return return_code;
}
}  // namespace types
}  // namespace stonedb
