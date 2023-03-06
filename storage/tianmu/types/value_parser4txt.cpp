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
#include "system/tianmu_system.h"

namespace Tianmu {
namespace types {
const uint PARS_BUF_SIZE = 128;

static int String2DateTime(const BString &s, TianmuDateTime &tianmu_dt, common::ColumnType at) {
  MYSQL_TIME myt;
  MYSQL_TIME_STATUS not_used;
  if (str_to_datetime(s.GetDataBytesPointer(), s.len_, &myt, TIME_DATETIME_ONLY, &not_used)) {
    return 1;
  }

  if ((at == common::ColumnType::TIMESTAMP) && !validate_timestamp_range(&myt)) {
    tianmu_dt = TianmuDateTime(0, common::ColumnType::TIMESTAMP);
    return 0;
  }

  tianmu_dt = TianmuDateTime(myt, at);
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

  if (sing)
    out_value *= -1;

  return rc;
}

common::ErrorCode ValueParserForText::ParseNum(const BString &tianmu_s, TianmuNum &tianmu_n, short scale) {
  // TODO: refactor
  char *val, *val_ptr;
  val = val_ptr = tianmu_s.val_;
  int len = tianmu_s.len_;
  EatWhiteSigns(val, len);
  if (tianmu_s.Equals("nullptr", 4)) {
    tianmu_n.null_ = true;
    return common::ErrorCode::SUCCESS;
  }
  tianmu_n.null_ = false;
  tianmu_n.is_double_ = false;
  tianmu_n.scale_ = 0;

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
  common::ErrorCode tianmuret = common::ErrorCode::SUCCESS;
  if (ptr_len > 0 && *val_ptr == '-') {
    val_ptr++;
    ptr_len--;
    sign = -1;
  }
  while (ptr_len > 0) {
    if (isdigit((uchar)*val_ptr)) {
      no_digs++;
      if (has_dot)
        no_digs_after_dot++;

      if ((no_digs <= 18 && scale < 0) || (no_digs <= 18 && no_digs_after_dot <= scale && scale >= 0)) {
        v *= 10;
        v += *val_ptr - '0';
        last = *val_ptr - '0';
      } else {
        no_digs--;
        if (has_dot) {
          no_digs_after_dot--;
          if (*val_ptr != '0')
            tianmuret = common::ErrorCode::VALUE_TRUNCATED;
          if (!last_set) {
            last_set = true;
            if (*val_ptr > '4') {
              last += 1;
              v = (v / 10) * 10 + last;
            }
          }
        } else {
          tianmu_n.value_ = (Uint64PowOfTen(18) - 1) * sign;
          tianmu_n.scale_ = 0;
          return common::ErrorCode::OUT_OF_RANGE;
        }
      }
    } else if (*val_ptr == '.' && !has_dot) {
      has_dot = true;
      if (v == 0)
        no_digs = 0;
    } else if (isspace((uchar)*val_ptr)) {
      EatWhiteSigns(val_ptr, ptr_len);
      if (ptr_len > 0)
        has_unexpected_sign = true;
      break;
    } else if (*val_ptr == 'd' || *val_ptr == 'D' || *val_ptr == 'e' || *val_ptr == 'E') {
      tianmuret = TianmuNum::ParseReal(tianmu_s, tianmu_n, common::ColumnType::REAL);
      tianmu_n = tianmu_n.ToDecimal(scale);
      return tianmuret;
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
    tianmu_n.value_ = (Uint64PowOfTen(18) - 1) * sign;
  else
    tianmu_n.value_ = v * sign;
  tianmu_n.scale_ = scale;
  if (has_unexpected_sign || no_digs > 18)
    return common::ErrorCode::VALUE_TRUNCATED;
  return tianmuret;
}

common::ErrorCode ValueParserForText::Parse(const BString &tianmu_s, TianmuNum &tianmu_n, common::ColumnType at,
                                            bool unsigned_flag) {
  if (at == common::ColumnType::BIGINT)
    return ParseBigInt(tianmu_s, tianmu_n);

  // TODO: refactor
  char *val, *val_ptr;
  val = val_ptr = tianmu_s.val_;
  int len = tianmu_s.len_;
  EatWhiteSigns(val, len);
  int ptr_len = len;
  val_ptr = val;
  short scale = -1;

  if (at == common::ColumnType::UNK) {
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
        if (v == 0)
          no_digs = 0;
      } else if (!has_exp && (*val_ptr == 'd' || *val_ptr == 'D' || *val_ptr == 'e' || *val_ptr == 'E')) {
        val_ptr++;
        ptr_len--;
        can_be_minus = true;
        has_exp = true;
        int tmp_len = ptr_len;
        common::ErrorCode tianmu_err_code = system::EatInt(val_ptr, ptr_len, exponent);
        if (tianmu_err_code == common::ErrorCode::SUCCESS)
          return common::ErrorCode::FAILED;
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
        if (ptr_len > 0)
          has_unexpected_sign = true;
        break;
      } else if (!isdigit((uchar)*val_ptr)) {
        has_unexpected_sign = true;
        break;
      } else {
        no_digs++;
        if (has_dot)
          no_digs_after_dot++;
        v *= 10;
        v += *val_ptr - '0';
      }
      val_ptr++;
      ptr_len--;
    }

    if (has_unexpected_sign) {
      // same as innodb , string convert to 0
      tianmu_n.null_ = false;
      tianmu_n.attr_type_ = at;
      tianmu_n.value_ = 0;
      return common::ErrorCode::VALUE_TRUNCATED;
    }

    if (has_exp)
      at = common::ColumnType::REAL;
    else {
      if (has_dot) {
        if (std::abs((no_digs - no_digs_after_dot) + exponent) + std::abs(no_digs_after_dot - exponent) <= 18) {
          scale = std::abs(no_digs_after_dot - exponent);
          at = common::ColumnType::NUM;
        } else
          at = common::ColumnType::REAL;
      } else {
        if (no_digs <= 18 && no_digs > 9) {
          at = common::ColumnType::NUM;
          scale = 0;
        } else if (no_digs > 18)
          at = common::ColumnType::REAL;
        else
          at = common::ColumnType::INT;
      }
    }
  } else {
    if (!core::ATI::IsNumericType(at))
      return common::ErrorCode::FAILED;
  }

  tianmu_n.null_ = false;
  tianmu_n.attr_type_ = at;
  if (core::ATI::IsRealType(at)) {
    tianmu_n.is_double_ = true;
    tianmu_n.scale_ = 0;
  } else
    tianmu_n.is_double_ = false;

  if (core::ATI::IsRealType(at))
    return ValueParserForText::ParseReal(tianmu_s, tianmu_n, at);
  else if (at == common::ColumnType::NUM)
    return ValueParserForText::ParseNum(tianmu_s, tianmu_n, scale);

  tianmu_n.scale_ = 0;
  tianmu_n.is_double_ = false;

  if (tianmu_s.Equals("nullptr", 4)) {
    tianmu_n.null_ = true;
    return common::ErrorCode::SUCCESS;
  }

  common::ErrorCode ret = common::ErrorCode::SUCCESS;

  ptr_len = len;
  val_ptr = val;
  EatWhiteSigns(val_ptr, ptr_len);
  if (core::ATI::IsIntegerType(at)) {
    ret = ParseBigInt(tianmu_s, tianmu_n);
    int64_t v = tianmu_n.value_;

    if (at == common::ColumnType::BYTEINT) {
      if (unsigned_flag) {
        if (v > UINT_MAX8) {
          v = UINT_MAX8;
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else if (v < 0) {
          v = 0;
          ret = common::ErrorCode::OUT_OF_RANGE;
        }
      } else {
        if (v > TIANMU_TINYINT_MAX) {
          v = TIANMU_TINYINT_MAX;
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else if (v < TIANMU_TINYINT_MIN) {
          v = TIANMU_TINYINT_MIN;
          ret = common::ErrorCode::OUT_OF_RANGE;
        }
      }
    } else if (at == common::ColumnType::SMALLINT) {
      if (unsigned_flag) {
        if (v > UINT_MAX16) {
          v = UINT_MAX16;
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else if (v < 0) {
          v = 0;
          ret = common::ErrorCode::OUT_OF_RANGE;
        }
      } else {
        if (v > TIANMU_SMALLINT_MAX) {
          v = TIANMU_SMALLINT_MAX;
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else if (v < TIANMU_SMALLINT_MIN) {
          v = TIANMU_SMALLINT_MIN;
          ret = common::ErrorCode::OUT_OF_RANGE;
        }
      }
    } else if (at == common::ColumnType::MEDIUMINT) {
      if (unsigned_flag) {
        if (v > UINT_MAX24) {
          v = UINT_MAX24;
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else if (v < 0) {
          v = 0;
          ret = common::ErrorCode::OUT_OF_RANGE;
        }
      } else {
        if (v > TIANMU_MEDIUMINT_MAX) {
          v = TIANMU_MEDIUMINT_MAX;
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else if (v < TIANMU_MEDIUMINT_MIN) {
          v = TIANMU_MEDIUMINT_MIN;
          ret = common::ErrorCode::OUT_OF_RANGE;
        }
      }
    } else if (at == common::ColumnType::INT) {
      if (unsigned_flag) {
        if (v > UINT_MAX32) {
          v = UINT_MAX32;
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else if (v < 0) {
          v = 0;
          ret = common::ErrorCode::OUT_OF_RANGE;
        }
      } else {
        if (v > std::numeric_limits<int>::max()) {
          v = std::numeric_limits<int>::max();
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else if (v < TIANMU_INT_MIN) {
          v = TIANMU_INT_MIN;
          ret = common::ErrorCode::OUT_OF_RANGE;
        }
      }
    }
    tianmu_n.is_double_ = false;
    tianmu_n.scale_ = 0;
    tianmu_n.value_ = v;
  }
  return ret;
}

common::ErrorCode ValueParserForText::ParseReal(const BString &tianmu_s, TianmuNum &tianmu_n, common::ColumnType at) {
  // TODO: refactor
  if (at == common::ColumnType::UNK)
    at = common::ColumnType::REAL;
  if (!core::ATI::IsRealType(at))
    return common::ErrorCode::FAILED;

  if (tianmu_s.Equals("nullptr", 4) || tianmu_s.IsNull()) {
    tianmu_n.null_ = true;
    return common::ErrorCode::SUCCESS;
  }
  tianmu_n.is_double_ = true;
  tianmu_n.scale_ = 0;

  char *val = tianmu_s.val_;
  int len = tianmu_s.len_;
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
      if (ptr_len > 0)
        ret = common::ErrorCode::VALUE_TRUNCATED;
      break;
    } else if (!isdigit((uchar)*val_ptr)) {
      ret = common::ErrorCode::VALUE_TRUNCATED;
      break;
    }
    val_ptr++;
    ptr_len--;
  }
  char stempval[PARS_BUF_SIZE];
  if (tianmu_s.len_ >= PARS_BUF_SIZE)
    return common::ErrorCode::VALUE_TRUNCATED;
#ifndef NDEBUG
  // resetting stempval to avoid valgrind
  // false warnings
  std::memset(stempval, 0, PARS_BUF_SIZE);
#endif
  std::memcpy(stempval, tianmu_s.val_, tianmu_s.len_);
  stempval[tianmu_s.len_] = 0;
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

  tianmu_n.attr_type_ = at;
  tianmu_n.null_ = false;
  if (at == common::ColumnType::REAL) {
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
    *(double *)&tianmu_n.value_ = d;
  } else if (at == common::ColumnType::FLOAT) {
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
    *(double *)&tianmu_n.value_ = d;
  }
  return ret;
}

common::ErrorCode ValueParserForText::ParseBigInt(const BString &tianmu_s, TianmuNum &tianmu_n) {
  char *val_ptr = tianmu_s.val_;
  int len = tianmu_s.len_;
  int ptr_len = len;
  common::ErrorCode ret = common::ErrorCode::SUCCESS;

  tianmu_n.null_ = false;
  tianmu_n.attr_type_ = common::ColumnType::BIGINT;
  tianmu_n.scale_ = 0;
  tianmu_n.is_double_ = false;

  if (tianmu_s.Equals("nullptr", 4)) {
    tianmu_n.null_ = true;
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
      v = common::TIANMU_BIGINT_MAX;
    } else {
      ret = common::ErrorCode::VALUE_TRUNCATED;
      v = 0;
    }
  } else {
    if (ptr_len) {
      if (*val_ptr == 'd' || *val_ptr == 'D' || *val_ptr == 'e' || *val_ptr == 'E') {
        ret = TianmuNum::ParseReal(tianmu_s, tianmu_n, common::ColumnType::REAL);
        if (tianmu_n.GetDecFractLen() != 0 || ret != common::ErrorCode::SUCCESS)
          ret = common::ErrorCode::VALUE_TRUNCATED;
        if (tianmu_n.GetIntPartAsDouble() > common::TIANMU_BIGINT_MAX) {
          v = common::TIANMU_BIGINT_MAX;
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else if (tianmu_n.GetIntPartAsDouble() < common::TIANMU_BIGINT_MIN) {
          v = common::TIANMU_BIGINT_MIN;
          ret = common::ErrorCode::OUT_OF_RANGE;
        } else {
          v = (int64_t)tianmu_n;
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
              ret = TianmuNum::ParseReal(tianmu_s, tianmu_n, common::ColumnType::REAL);
              if (tianmu_n.GetDecFractLen() != 0 || ret != common::ErrorCode::SUCCESS)
                ret = common::ErrorCode::VALUE_TRUNCATED;
              if (tianmu_n.GetIntPartAsDouble() > common::TIANMU_BIGINT_MAX) {
                v = common::TIANMU_BIGINT_MAX;
                ret = common::ErrorCode::OUT_OF_RANGE;
              } else if (tianmu_n.GetIntPartAsDouble() < common::TIANMU_BIGINT_MIN) {
                v = common::TIANMU_BIGINT_MIN;
                ret = common::ErrorCode::OUT_OF_RANGE;
              } else {
                v = (int64_t)tianmu_n;
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

  if (is_negative)
    v *= -1;

  if (v > common::TIANMU_BIGINT_MAX) {
    v = common::TIANMU_BIGINT_MAX;
    ret = common::ErrorCode::OUT_OF_RANGE;
  } else if (v < common::TIANMU_BIGINT_MIN) {
    v = common::TIANMU_BIGINT_MIN;
    ret = common::ErrorCode::OUT_OF_RANGE;
  }
  tianmu_n.is_double_ = false;
  tianmu_n.scale_ = 0;
  tianmu_n.value_ = v;
  return ret;
}

common::ErrorCode ValueParserForText::ParseNumeric(BString const &tianmu_s, int64_t &out, common::ColumnType at,
                                                   bool unsigned_flag) {
  TianmuNum number;
  common::ErrorCode return_code = Parse(tianmu_s, number, at, unsigned_flag);
  out = number.ValueInt();
  return return_code;
}

common::ErrorCode ValueParserForText::ParseBigIntAdapter(const BString &tianmu_s, int64_t &out) {
  TianmuNum number;
  common::ErrorCode return_code = ParseBigInt(tianmu_s, number);
  out = number.ValueInt();
  return return_code;
}

// Unlike the other numeric values, the exactly bit value has already stored int Field_bit in GetOneRow() function,
// and make the string buffer to arg tianmu_s, so this function just convert the string value to int64 back.
common::ErrorCode ValueParserForText::ParseBitAdapter(const BString &tianmu_s, int64_t &out) {
  char *val_ptr = tianmu_s.val_;
  int len = tianmu_s.len_;
  // No matter null value or not, the value stored in char buffer at least one byte and up to 8 bytes.
  // calculated by len = (prec+7)/8
  DEBUG_ASSERT(len >= 1 && len <= 8);

  // The parse code may never go here, but we still check null value for integrity.
  if (tianmu_s.Equals("nullptr", 4)) {
    out = common::NULL_VALUE_64;
    return common::ErrorCode::SUCCESS;
  }

  switch (len) {
    case 1: {
      out = mi_uint1korr(val_ptr);
      break;
    }
    case 2: {
      out = mi_uint2korr(val_ptr);
      break;
    }
    case 3: {
      out = mi_uint3korr(val_ptr);
      break;
    }
    case 4: {
      out = mi_uint4korr(val_ptr);
      break;
    }
    case 5: {
      out = mi_uint5korr(val_ptr);
      break;
    }
    case 6: {
      out = mi_uint6korr(val_ptr);
      break;
    }
    case 7: {
      out = mi_uint7korr(val_ptr);
      break;
    }
    default: {
      out = mi_uint8korr(val_ptr);
      break;
    }
  }

  return common::ErrorCode::SUCCESS;
}

common::ErrorCode ValueParserForText::ParseDecimal(BString const &tianmu_s, int64_t &out, short precision,
                                                   short scale) {
  TianmuNum number;
  common::ErrorCode return_code = ParseNum(tianmu_s, number, scale);
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

common::ErrorCode ValueParserForText::ParseDateTimeOrTimestamp(const BString &tianmu_s, TianmuDateTime &rcv,
                                                               common::ColumnType at) {
  if (tianmu_s.IsNull() || tianmu_s.Equals("nullptr", 4)) {
    rcv.at_ = at;
    rcv.null_ = true;
    return common::ErrorCode::SUCCESS;
  }
  char *buf = tianmu_s.val_;
  int buflen = tianmu_s.len_;
  EatWhiteSigns(buf, buflen);

  if (buflen == 0) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (String2DateTime(tianmu_s, rcv, at) == 0) {
    if (at == common::ColumnType::TIMESTAMP) {
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
        my_time_t secs_utc = current_txn_->Thd()->variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
        common::GMTSec2GMTTime(&myt, secs_utc);
        myt.second_part = rcv.MicroSecond();
      }
      rcv = TianmuDateTime(myt, common::ColumnType::TIMESTAMP);
    }
    return common::ErrorCode::SUCCESS;
  }

  uint64_t year = 0;
  uint month = 0, day = 0, hour = 0, minute = 0, second = 0, microsecond = 0;
  common::ErrorCode tianmu_err_code = system::EatUInt64(buf, buflen, year);
  EatWhiteSigns(buf, buflen);
  if (tianmu_err_code == common::ErrorCode::FAILED) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (tianmu_err_code != common::ErrorCode::OUT_OF_RANGE && buflen != 0 &&
      static_cast<short>(year) != kTianmuDateSpec.Year())
    year = TianmuDateTime::ToCorrectYear((uint)year, at);

  if (buflen == 0)
    return TianmuDateTime::Parse(year, rcv, at);

  if (!system::EatDTSeparators(buf, buflen)) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }
  if (system::EatUInt(buf, buflen, month) == common::ErrorCode::FAILED) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }
  if (!system::EatDTSeparators(buf, buflen)) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (system::EatUInt(buf, buflen, day) == common::ErrorCode::FAILED) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (!TianmuDateTime::CanBeDate(year, month, day)) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (!EatWhiteSigns(buf, buflen) && !system::EatDTSeparators(buf, buflen)) {
    if ((at == common::ColumnType::DATETIME &&
         TianmuDateTime::IsCorrectTianmuDatetime((short)year, month, day, TianmuDateTime::GetSpecialValue(at).Hour(),
                                                 TianmuDateTime::GetSpecialValue(at).Minute(),
                                                 TianmuDateTime::GetSpecialValue(at).Second())) ||
        (at == common::ColumnType::TIMESTAMP &&
         TianmuDateTime::IsCorrectTianmuTimestamp((short)year, month, day, TianmuDateTime::GetSpecialValue(at).Hour(),
                                                  TianmuDateTime::GetSpecialValue(at).Minute(),
                                                  TianmuDateTime::GetSpecialValue(at).Second())) ||
        (at == common::ColumnType::DATE && TianmuDateTime::IsCorrectTianmuDate((short)year, month, day)))
      rcv = TianmuDateTime((short)year, month, day, TianmuDateTime::GetSpecialValue(at).Hour(),
                           TianmuDateTime::GetSpecialValue(at).Minute(), TianmuDateTime::GetSpecialValue(at).Second(),
                           at);
    return common::ErrorCode::OUT_OF_RANGE;
  }

  bool eat1 = true, eat2 = true;
  while (eat1 || eat2) {
    eat1 = system::EatDTSeparators(buf, buflen);
    eat2 = EatWhiteSigns(buf, buflen);
  }
  tianmu_err_code = common::ErrorCode::SUCCESS;
  tianmu_err_code = system::EatUInt(buf, buflen, hour);
  if (!common::IsError(tianmu_err_code)) {
    if (!TianmuDateTime::CanBeHour(hour)) {
      rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(at));
      return common::ErrorCode::OUT_OF_RANGE;
    } else if (!system::EatDTSeparators(buf, buflen)) {
      tianmu_err_code = common::ErrorCode::FAILED;
    }
  } else
    hour = TianmuDateTime::GetSpecialValue(at).Hour();

  if (!common::IsError(tianmu_err_code))
    tianmu_err_code = system::EatUInt(buf, buflen, minute);
  if (!common::IsError(tianmu_err_code)) {
    if (!TianmuDateTime::CanBeMinute(minute)) {
      rcv = TianmuDateTime((short)year, (short)month, (short)day, (short)hour, (short)minute, (short)second, at);
      return common::ErrorCode::OUT_OF_RANGE;
    } else if (!system::EatDTSeparators(buf, buflen)) {
      rcv = TianmuDateTime((short)year, (short)month, (short)day, (short)hour, (short)minute, (short)second, at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
  } else
    minute = TianmuDateTime::GetSpecialValue(at).Minute();

  if (!common::IsError(tianmu_err_code))
    tianmu_err_code = system::EatUInt(buf, buflen, second);
  if (!common::IsError(tianmu_err_code)) {
    if (!TianmuDateTime::CanBeSecond(second)) {
      rcv = TianmuDateTime((short)year, (short)month, (short)day, (short)hour, (short)minute, (short)second, at);
      return common::ErrorCode::OUT_OF_RANGE;
    }
  } else
    second = TianmuDateTime::GetSpecialValue(at).Second();

  if (tianmu_err_code == common::ErrorCode::FAILED)
    tianmu_err_code = common::ErrorCode::OUT_OF_RANGE;

  if (system::EatDTSeparators(buf, buflen))  // contains micro seconds
  {
    tianmu_err_code = system::EatUInt(buf, buflen, microsecond);
    if (!common::IsError(tianmu_err_code)) {
      if (!TianmuDateTime::CanBeMicroSecond(microsecond)) {
        short s_year = static_cast<short>(year);
        short s_month = static_cast<short>(month);
        short s_day = static_cast<short>(day);
        short s_hour = static_cast<short>(hour);
        short s_minute = static_cast<short>(minute);
        short s_second = static_cast<short>(second);
        rcv = TianmuDateTime(s_year, s_month, s_day, s_hour, s_minute, s_second, microsecond, at);
        return common::ErrorCode::OUT_OF_RANGE;
      }
    } else
      microsecond = TianmuDateTime::GetSpecialValue(at).MicroSecond();
  }

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
    if (at == common::ColumnType::DATETIME) {
      short s_year = static_cast<short>(year);
      short s_month = static_cast<short>(month);
      short s_day = static_cast<short>(day);
      short s_hour = static_cast<short>(hour);
      short s_minute = static_cast<short>(minute);
      short s_second = static_cast<short>(second);
      if (TianmuDateTime::IsCorrectTianmuDatetime(s_year, s_year, s_day, s_hour, s_minute, s_second) ||
          TianmuDateTime::IsZeroTianmuDate(s_year, s_month, s_day)) {
        rcv = TianmuDateTime(s_year, s_month, s_day, s_hour, s_minute, s_second, microsecond, at);
        return tianmu_err_code;
      } else {
        rcv = TianmuDateTime(s_year, s_month, s_day, static_cast<short>(TianmuDateTime::GetSpecialValue(at).Hour()),
                             static_cast<short>(TianmuDateTime::GetSpecialValue(at).Minute()),
                             static_cast<short>(TianmuDateTime::GetSpecialValue(at).Second()), at);
        tianmu_err_code = common::ErrorCode::OUT_OF_RANGE;
      }
    } else if (at == common::ColumnType::TIMESTAMP) {
      if (TianmuDateTime::IsCorrectTianmuTimestamp((short)year, (short)month, (short)day, (short)hour, (short)minute,
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
          my_time_t secs_utc = current_txn_->Thd()->variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
          common::GMTSec2GMTTime(&myt, secs_utc);
          myt.second_part = microsecond;
        }
        rcv = TianmuDateTime(myt, common::ColumnType::TIMESTAMP);
        return tianmu_err_code;
      } else {
        rcv = kTianmuTimestampSpec;
        tianmu_err_code = common::ErrorCode::OUT_OF_RANGE;
      }
    }

    if (buflen != 0)
      return common::ErrorCode::OUT_OF_RANGE;
    return tianmu_err_code;
  } catch (common::DataTypeConversionException &) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(at));
    return common::ErrorCode::OUT_OF_RANGE;
  }
  return common::ErrorCode::FAILED;
}

common::ErrorCode ValueParserForText::ParseTime(const BString &tianmu_s, TianmuDateTime &rcv) {
  if (tianmu_s.IsNull() || tianmu_s.Equals("nullptr", 4)) {
    rcv.at_ = common::ColumnType::TIME;
    rcv.null_ = true;
    return common::ErrorCode::SUCCESS;
  }
  char *buf = tianmu_s.val_;
  int buflen = tianmu_s.len_;
  EatWhiteSigns(buf, buflen);
  if (buflen == 0) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::TIME));
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
  common::ErrorCode tianmu_err_code = system::EatUInt64(buf, buflen, tmp);
  if (tianmu_err_code == common::ErrorCode::FAILED && buflen > 0 && *buf == ':') {  // e.g. :12:34
    colon = true;
  } else if (tianmu_err_code != common::ErrorCode::SUCCESS) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::TIME));
    return common::ErrorCode::VALUE_TRUNCATED;
  } else if (tianmu_err_code == common::ErrorCode::SUCCESS) {
    second = tmp;
    EatWhiteSigns(buf, buflen);
    if (buflen == 0) {  // e.g. 235959
      return TianmuDateTime::Parse(second * sign, rcv, common::ColumnType::TIME);
    }
  }

  if (buflen > 0 && *buf == ':') {
    buf++;
    buflen--;
    EatWhiteSigns(buf, buflen);
    uint tmp2 = 0;
    tianmu_err_code = system::EatUInt(buf, buflen, tmp2);
    if (!common::IsError(tianmu_err_code)) {  // e.g. 01:02
      EatWhiteSigns(buf, buflen);
      if (colon) {  // starts with ':'
        if (!TianmuDateTime::CanBeMinute(tmp2)) {
          rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::TIME));
          return common::ErrorCode::VALUE_TRUNCATED;
        }
        minute = tmp2;
        hour = 0;
        EatWhiteSigns(buf, buflen);
        if (buflen > 0 && *buf == ':') {
          buf++;
          buflen--;
          EatWhiteSigns(buf, buflen);
          tianmu_err_code = system::EatUInt(buf, buflen, tmp2);
          if (tianmu_err_code == common::ErrorCode::SUCCESS && TianmuDateTime::CanBeSecond(tmp2))
            second = tmp2;
          else {
            second = TianmuDateTime::GetSpecialValue(common::ColumnType::TIME).Second();
            tianmu_err_code = common::ErrorCode::VALUE_TRUNCATED;
          }
        } else {  // e.g. :01:
        }
      } else {  // e.g. 01:02:03
        hour = (int)second;
        if (!TianmuDateTime::CanBeMinute(tmp2)) {
          rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::TIME));
          return common::ErrorCode::VALUE_TRUNCATED;
        }
        minute = tmp2;
        second = 0;
        if (buflen > 0 && *buf == ':') {
          buf++;
          buflen--;
          tianmu_err_code = system::EatUInt(buf, buflen, tmp2);
          if (tianmu_err_code == common::ErrorCode::SUCCESS && TianmuDateTime::CanBeSecond(tmp2))
            second = tmp2;
          else {
            second = TianmuDateTime::GetSpecialValue(common::ColumnType::TIME).Second();
            tianmu_err_code = common::ErrorCode::VALUE_TRUNCATED;
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

      if (tianmu_err_code != common::ErrorCode::SUCCESS || buflen != 0)
        tianmu_err_code = common::ErrorCode::VALUE_TRUNCATED;

      if (TianmuDateTime::IsCorrectTianmuTime(short(hour * sign), short(minute * sign), short(second * sign))) {
        rcv = TianmuDateTime(short(hour * sign), short(minute * sign), short(second * sign), common::ColumnType::TIME);
        return tianmu_err_code;
      } else {
        if (hour * sign < -kTianmuTimeMin.Hour())
          rcv = TianmuDateTime(kTianmuTimeMin);
        else if (hour * sign > kTianmuTimeMax.Hour())
          rcv = TianmuDateTime(kTianmuTimeMax);
        else
          DEBUG_ASSERT(0);  // hmmm... ????

        return common::ErrorCode::OUT_OF_RANGE;
      }
    } else {                            // e.g. 01:a... or 01::... only second is set
      if (buflen > 0 && *buf == ':') {  // 01::
        EatWhiteSigns(buf, buflen);
        tianmu_err_code = system::EatUInt(buf, buflen, tmp2);
        if (tianmu_err_code == common::ErrorCode::SUCCESS) {
          hour = (int)second;
          if (!TianmuDateTime::CanBeSecond(tmp2)) {
            rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::TIME));
            return common::ErrorCode::VALUE_TRUNCATED;
          }
          second = tmp2;
        }
      } else
        return TianmuDateTime::Parse(second * sign, rcv, common::ColumnType::TIME);
      if (TianmuDateTime::IsCorrectTianmuTime(short(hour * sign), short(minute * sign), short(second * sign))) {
        rcv = TianmuDateTime(short(hour * sign), short(minute * sign), short(second * sign), common::ColumnType::TIME);
        return common::ErrorCode::VALUE_TRUNCATED;
      } else {
        rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::TIME));
        return common::ErrorCode::VALUE_TRUNCATED;
      }
    }
  } else {
    if (TianmuDateTime::IsCorrectTianmuTime(short(hour * sign), short(minute * sign), short(second * sign))) {
      rcv = TianmuDateTime(short(hour * sign), short(minute * sign), short(second * sign), common::ColumnType::TIME);
      return common::ErrorCode::VALUE_TRUNCATED;
    } else {
      rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::TIME));
      return common::ErrorCode::VALUE_TRUNCATED;
    }
  }
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode ValueParserForText::ParseDate(const BString &tianmu_s, TianmuDateTime &rcv) {
  if (tianmu_s.IsNull() || tianmu_s.Equals("nullptr", 4)) {
    rcv.at_ = common::ColumnType::DATE;
    rcv.null_ = true;
    return common::ErrorCode::SUCCESS;
  }
  char *buf = tianmu_s.val_;
  int buflen = tianmu_s.len_;
  EatWhiteSigns(buf, buflen);
  if (buflen == 0) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::DATE));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (buflen > 0 && *buf == '-') {
    rcv = TianmuDateTime::GetSpecialValue(common::ColumnType::DATE);
    return common::ErrorCode::VALUE_TRUNCATED;
  }

  uint year = 0, month = 0, day = 0;
  common::ErrorCode tianmu_err_code = system::EatUInt(buf, buflen, year);
  if (tianmu_err_code == common::ErrorCode::FAILED) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  } else if (tianmu_err_code != common::ErrorCode::OUT_OF_RANGE && buflen > 0) {
    year = TianmuDateTime::ToCorrectYear(year, common::ColumnType::DATE);
  }

  if (buflen == 0)
    return TianmuDateTime::Parse((int64_t)year, rcv, common::ColumnType::DATE);

  EatWhiteSigns(buf, buflen);
  if (!system::EatDTSeparators(buf, buflen)) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  }
  EatWhiteSigns(buf, buflen);
  if (system::EatUInt(buf, buflen, month) == common::ErrorCode::FAILED) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  }
  EatWhiteSigns(buf, buflen);
  if (!system::EatDTSeparators(buf, buflen)) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  }
  EatWhiteSigns(buf, buflen);
  if (system::EatUInt(buf, buflen, day) == common::ErrorCode::FAILED) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  }
  EatWhiteSigns(buf, buflen);
  if (!TianmuDateTime::CanBeDate(year, month, day)) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::DATE));
    return common::ErrorCode::VALUE_TRUNCATED;
  } else
    rcv = TianmuDateTime(year, month, day, common::ColumnType::DATE);
  if (buflen != 0)
    return common::ErrorCode::VALUE_TRUNCATED;
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode ValueParserForText::ParseYear(const BString &tianmu_s, TianmuDateTime &rcv) {
  if (tianmu_s.IsNull() || tianmu_s.Equals("nullptr", 4)) {
    rcv.at_ = common::ColumnType::YEAR;
    rcv.null_ = true;
    return common::ErrorCode::SUCCESS;
  }
  char *buf = tianmu_s.val_;
  int buflen = tianmu_s.len_;
  EatWhiteSigns(buf, buflen);
  if (buflen == 0) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::YEAR));
    return common::ErrorCode::OUT_OF_RANGE;
  }

  if (buflen > 0 && *buf == '-') {
    rcv = TianmuDateTime::GetSpecialValue(common::ColumnType::YEAR);
    return common::ErrorCode::VALUE_TRUNCATED;
  }
  uint year = 0;
  int tmp_buf_len = buflen;
  common::ErrorCode tianmu_err_code = system::EatUInt(buf, buflen, year);
  if (tianmu_err_code != common::ErrorCode::SUCCESS) {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::YEAR));
    return common::ErrorCode::VALUE_TRUNCATED;
  } else if (year == 0 && (tmp_buf_len - buflen) < 4) {
    year = 2000;
  } else {
    year = TianmuDateTime::ToCorrectYear(year, common::ColumnType::YEAR);
  }

  if (TianmuDateTime::IsCorrectTianmuYear(year)) {
    EatWhiteSigns(buf, buflen);
    rcv = TianmuDateTime(year);
    if (buflen)
      return common::ErrorCode::OUT_OF_RANGE;
    return common::ErrorCode::SUCCESS;
  } else {
    rcv = TianmuDateTime(TianmuDateTime::GetSpecialValue(common::ColumnType::YEAR));
    return common::ErrorCode::OUT_OF_RANGE;
  }
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode ValueParserForText::ParseDateTime(const BString &tianmu_s, TianmuDateTime &rcv,
                                                    common::ColumnType at) {
  DEBUG_ASSERT(core::ATI::IsDateTimeType(at));
  switch (at) {
    case common::ColumnType::TIMESTAMP:
    case common::ColumnType::DATETIME:
    case common::ColumnType::DATE:
      return ValueParserForText::ParseDateTimeOrTimestamp(tianmu_s, rcv, at);
    case common::ColumnType::TIME:
      return ValueParserForText::ParseTime(tianmu_s, rcv);
    default:  // case common::CT::YEAR :
      return ValueParserForText::ParseYear(tianmu_s, rcv);
  }
}

common::ErrorCode ValueParserForText::ParseDateTimeAdapter(BString const &tianmu_s, int64_t &out,
                                                           common::ColumnType at) {
  TianmuDateTime dt;
  common::ErrorCode return_code = ValueParserForText::ParseDateTime(tianmu_s, dt, at);
  out = dt.GetInt64();
  return return_code;
}
}  // namespace types
}  // namespace Tianmu
