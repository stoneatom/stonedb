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

#include "aggregator_basic.h"

#include "core/transaction.h"
#include "system/rc_system.h"

namespace stonedb {
namespace core {
void AggregatorSum64::PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) {
  stats_updated = false;
  int64_t *p = (int64_t *)buf;
  if (*p == common::NULL_VALUE_64) {
    *p = 0;
  }
  double overflow_check = double(*p) + double(v) * factor;
  if (overflow_check > 9.223372037e+18 || overflow_check < -9.223372037e+18)
    throw common::NotImplementedException("Aggregation overflow.");
  *p += v * factor;
}

void AggregatorSum64::Merge(unsigned char *buf, unsigned char *src_buf) {
  int64_t *p = (int64_t *)buf;
  int64_t *ps = (int64_t *)src_buf;
  if (*ps == common::NULL_VALUE_64) return;
  stats_updated = false;
  if (*p == common::NULL_VALUE_64) {
    *p = 0;
  }
  double overflow_check = double(*p) + double(*ps);
  if (overflow_check > 9.223372037e+18 || overflow_check < -9.223372037e+18)
    throw common::NotImplementedException("Aggregation overflow.");
  *p += *ps;
}

void AggregatorSum64::SetAggregatePackSum(int64_t par1, int64_t factor) {
  double overflow_check = double(par1) * factor;
  if (overflow_check > 9.223372037e+18 || overflow_check < -9.223372037e+18)
    throw common::NotImplementedException("Aggregation overflow.");
  pack_sum = par1 * factor;
}

bool AggregatorSum64::AggregatePack(unsigned char *buf) {
  DEBUG_ASSERT(pack_sum != common::NULL_VALUE_64);
  PutAggregatedValue(buf, pack_sum, 1);
  return true;
}

void AggregatorSumD::PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) {
  stats_updated = false;
  common::double_int_t *p = (common::double_int_t *)buf;
  if ((*p).i == common::NULL_VALUE_64) {
    (*p).i = 0;
  }
  (*p).d += *((double *)(&v)) * factor;
}

void AggregatorSumD::Merge(unsigned char *buf, unsigned char *src_buf) {
  common::double_int_t *p = (common::double_int_t *)buf;
  common::double_int_t *ps = (common::double_int_t *)src_buf;
  if ((*ps).i == common::NULL_VALUE_64) return;
  stats_updated = false;
  if ((*p).i == common::NULL_VALUE_64) {
    (*p).i = 0;
  }
  (*p).d += (*ps).d;
}

void AggregatorSumD::PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) {
  stats_updated = false;
  types::RCNum val(common::CT::REAL);
  double d_val = 0.0;
  if (!v.IsEmpty()) {
    auto r = types::RCNum::ParseReal(v, val, common::CT::REAL);
    if ((r == common::ErrorCode::SUCCESS || r == common::ErrorCode::OUT_OF_RANGE) && !val.IsNull()) {
      d_val = double(val);
    }
  }
  PutAggregatedValue(buf, *((int64_t *)(&d_val)), factor);
}

bool AggregatorSumD::AggregatePack(unsigned char *buf) {
  PutAggregatedValue(buf, pack_sum, 1);
  return true;
}

///////////////

void AggregatorAvg64::PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) {
  stats_updated = false;
  *((double *)buf) += double(v) * factor;
  if (!warning_issued && (*((double *)buf) > 9.223372037e+18 || *((double *)buf) < -9.223372037e+18)) {
    common::PushWarning(current_tx->Thd(), Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                        "Values rounded in average()");
    warning_issued = true;
  }
  *((int64_t *)(buf + 8)) += factor;
}

void AggregatorAvg64::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((int64_t *)(src_buf + 8)) == 0) return;
  stats_updated = false;
  *((double *)buf) += *((double *)src_buf);
  if (!warning_issued && (*((double *)buf) > 9.223372037e+18 || *((double *)buf) < -9.223372037e+18)) {
    common::PushWarning(current_tx->Thd(), Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                        "Values rounded in average()");
    warning_issued = true;
  }
  *((int64_t *)(buf + 8)) += *((int64_t *)(src_buf + 8));
}

double AggregatorAvg64::GetValueD(unsigned char *buf) {
  if (*((int64_t *)(buf + 8)) == 0) return NULL_VALUE_D;
  return *((double *)buf) / *((int64_t *)(buf + 8)) / prec_factor;
}

bool AggregatorAvg64::AggregatePack(unsigned char *buf) {
  stats_updated = false;
  *((double *)buf) += pack_sum;
  *((int64_t *)(buf + 8)) += pack_not_nulls;
  return true;
}

void AggregatorAvgD::PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) {
  stats_updated = false;
  *((double *)buf) += *((double *)(&v)) * factor;
  *((int64_t *)(buf + 8)) += factor;
}

void AggregatorAvgD::PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) {
  stats_updated = false;
  types::RCNum val(common::CT::REAL);
  if (!v.IsEmpty()) {
    auto r = types::RCNum::ParseReal(v, val, common::CT::REAL);
    if ((r == common::ErrorCode::SUCCESS || r == common::ErrorCode::OUT_OF_RANGE) && !val.IsNull()) {
      double d_val = double(val);
      PutAggregatedValue(buf, *((int64_t *)(&d_val)), factor);
    }
  }
}

void AggregatorAvgD::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((int64_t *)(src_buf + 8)) == 0) return;
  stats_updated = false;
  *((double *)buf) += *((double *)src_buf);
  *((int64_t *)(buf + 8)) += *((int64_t *)(src_buf + 8));
}

double AggregatorAvgD::GetValueD(unsigned char *buf) {
  if (*((int64_t *)(buf + 8)) == 0) return NULL_VALUE_D;
  return *((double *)buf) / *((int64_t *)(buf + 8));
}

bool AggregatorAvgD::AggregatePack(unsigned char *buf) {
  stats_updated = false;
  *((double *)buf) += pack_sum;
  *((int64_t *)(buf + 8)) += pack_not_nulls;
  return true;
}

void AggregatorAvgYear::PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) {
  stats_updated = false;
  *((double *)buf) += double(types::DT::YearSortEncoding(v)) * factor;
  *((int64_t *)(buf + 8)) += factor;
}

void AggregatorAvgYear::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((int64_t *)(src_buf + 8)) == 0) return;
  stats_updated = false;
  *((double *)buf) += *((double *)src_buf);
  *((int64_t *)(buf + 8)) += *((int64_t *)(src_buf + 8));
}

void AggregatorAvgYear::PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) {
  stats_updated = false;
  types::RCNum val(common::CT::INT);
  if (!v.IsEmpty() && types::RCNum::ParseNum(v, val, 0) == common::ErrorCode::SUCCESS && !val.IsNull()) {
    *((double *)buf) += double(val.GetValueInt64()) * factor;
    *((int64_t *)(buf + 8)) += factor;
  }
}

void AggregatorMin32::PutAggregatedValue(unsigned char *buf, int64_t v, [[maybe_unused]] int64_t factor) {
  if (*((int *)buf) == common::NULL_VALUE_32 || *((int *)buf) > v) {
    stats_updated = false;
    *((int *)buf) = (int)v;
  }
}

void AggregatorMin32::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((int *)src_buf) == common::NULL_VALUE_32) return;
  if (*((int *)buf) == common::NULL_VALUE_32 || *((int *)buf) > *((int *)src_buf)) {
    stats_updated = false;
    *((int *)buf) = *((int *)src_buf);
  }
}

int64_t AggregatorMin32::GetValue64(unsigned char *buf) {
  if (*((int *)buf) == common::NULL_VALUE_32) return common::NULL_VALUE_64;
  return *((int *)buf);
}

void AggregatorMin64::PutAggregatedValue(unsigned char *buf, int64_t v, [[maybe_unused]] int64_t factor) {
  if (*((int64_t *)buf) == common::NULL_VALUE_64 || *((int64_t *)buf) > v) {
    stats_updated = false;
    *((int64_t *)buf) = v;
  }
}

void AggregatorMin64::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((int64_t *)src_buf) == common::NULL_VALUE_64) return;
  if (*((int64_t *)buf) == common::NULL_VALUE_64 || *((int64_t *)buf) > *((int64_t *)src_buf)) {
    stats_updated = false;
    *((int64_t *)buf) = *((int64_t *)src_buf);
  }
}

bool AggregatorMin64::AggregatePack(unsigned char *buf) {
  DEBUG_ASSERT(pack_min != common::NULL_VALUE_64);
  PutAggregatedValue(buf, pack_min, 1);
  return true;
}

bool AggregatorMinD::AggregatePack(unsigned char *buf) {
  DEBUG_ASSERT(pack_min != common::NULL_VALUE_64);
  int64_t pack_min_64 = *(int64_t *)&pack_min;  // pack_min is double, so it needs conversion into
                                                // int64_t-encoded-double
  PutAggregatedValue(buf, pack_min_64, 1);
  return true;
}

bool AggregatorMax64::AggregatePack(unsigned char *buf) {
  DEBUG_ASSERT(pack_max != common::NULL_VALUE_64);
  PutAggregatedValue(buf, pack_max, 1);
  return true;
}

bool AggregatorMaxD::AggregatePack(unsigned char *buf) {
  DEBUG_ASSERT(pack_max != NULL_VALUE_D);
  int64_t pack_max_64 = *(int64_t *)&pack_max;  // pack_max is double, so it needs conversion into
                                                // int64_t-encoded-double
  PutAggregatedValue(buf, pack_max_64, 1);
  return true;
}

void AggregatorMinD::PutAggregatedValue(unsigned char *buf, int64_t v, [[maybe_unused]] int64_t factor) {
  if (*((int64_t *)buf) == common::NULL_VALUE_64 || *((double *)buf) > *((double *)(&v))) {
    stats_updated = false;
    *((double *)buf) = *((double *)(&v));
  }
}

void AggregatorMinD::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((int64_t *)src_buf) == common::NULL_VALUE_64) return;
  if (*((int64_t *)buf) == common::NULL_VALUE_64 || *((double *)buf) > *((double *)src_buf)) {
    stats_updated = false;
    *((double *)buf) = *((double *)src_buf);
  }
}

void AggregatorMinT::PutAggregatedValue(unsigned char *buf, const types::BString &v, [[maybe_unused]] int64_t factor) {
  DEBUG_ASSERT((uint)val_len >= v.len);
  if (*((unsigned short *)buf) == 0 && buf[2] == 0) {  // still null
    stats_updated = false;
    std::memset(buf + 2, 0, val_len);
    *((unsigned short *)buf) = v.len;
    if (v.len > 0)
      std::memcpy(buf + 2, v.val, v.len);
    else
      buf[2] = 1;  // empty string indicator (non-null)
  } else {
    types::BString m((char *)buf + 2, *((unsigned short *)buf));
    if (m > v) {
      stats_updated = false;
      std::memset(buf + 2, 0, val_len);
      *((unsigned short *)buf) = v.len;
      if (v.len > 0)
        std::memcpy(buf + 2, v.val, v.len);
      else
        buf[2] = 1;  // empty string indicator (non-null)
    }
  }
}

void AggregatorMinT::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((unsigned short *)src_buf) == 0 && src_buf[2] == 0) return;
  if (*((unsigned short *)buf) == 0 && buf[2] == 0) {  // still null
    stats_updated = false;
    std::memcpy(buf, src_buf, val_len + 2);
  } else {
    types::BString m((char *)buf + 2, *((unsigned short *)buf));
    types::BString v((char *)src_buf + 2, *((unsigned short *)src_buf));
    if (m > v) {
      stats_updated = false;
      std::memcpy(buf, src_buf, val_len + 2);
    }
  }
}

types::BString AggregatorMinT::GetValueT(unsigned char *buf) {
  int len = *((unsigned short *)(buf));
  char *p = (char *)(buf + 2);
  if (len == 0) {
    if (*p != 0)                           // empty string indicator: len==0 and nontrivial character
      return types::BString("", 0, true);  // empty string
    return types::BString();               // null value
  }
  types::BString res(p, len);
  return res;
}

AggregatorMinT_UTF::AggregatorMinT_UTF(int max_len, DTCollation coll) : AggregatorMinT(max_len), collation(coll) {}

void AggregatorMinT_UTF::PutAggregatedValue(unsigned char *buf, const types::BString &v,
                                            [[maybe_unused]] int64_t factor) {
  DEBUG_ASSERT((uint)val_len >= v.len);
  if (*((unsigned short *)buf) == 0 && buf[2] == 0) {  // still null
    stats_updated = false;
    std::memset(buf + 2, 0, val_len);
    *((unsigned short *)buf) = v.len;
    if (v.len > 0)
      std::memcpy(buf + 2, v.val, v.len);
    else
      buf[2] = 1;  // empty string indicator (non-null)
  } else {
    types::BString m((char *)buf + 2, *((unsigned short *)buf));
    if (CollationStrCmp(collation, m, v) > 0) {
      stats_updated = false;
      std::memset(buf + 2, 0, val_len);
      *((unsigned short *)buf) = v.len;
      if (v.len > 0)
        std::memcpy(buf + 2, v.val, v.len);
      else
        buf[2] = 1;  // empty string indicator (non-null)
    }
  }
}

void AggregatorMinT_UTF::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((unsigned short *)src_buf) == 0 && src_buf[2] == 0) return;
  if (*((unsigned short *)buf) == 0 && buf[2] == 0) {  // still null
    stats_updated = false;
    std::memcpy(buf, src_buf, val_len + 2);
  } else {
    types::BString m((char *)buf + 2, *((unsigned short *)buf));
    types::BString v((char *)src_buf + 2, *((unsigned short *)src_buf));
    if (CollationStrCmp(collation, m, v) > 0) {
      stats_updated = false;
      std::memcpy(buf, src_buf, val_len + 2);
    }
  }
}

void AggregatorMax32::PutAggregatedValue(unsigned char *buf, int64_t v, [[maybe_unused]] int64_t factor) {
  if (*((int *)buf) == common::NULL_VALUE_32 || *((int *)buf) < v) {
    stats_updated = false;
    *((int *)buf) = (int)v;
  }
}

int64_t AggregatorMax32::GetValue64(unsigned char *buf) {
  if (*((int *)buf) == common::NULL_VALUE_32) return common::NULL_VALUE_64;
  return *((int *)buf);
}

void AggregatorMax32::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((int *)src_buf) == common::NULL_VALUE_32) return;
  if (*((int *)buf) == common::NULL_VALUE_32 || *((int *)buf) < *((int *)src_buf)) {
    stats_updated = false;
    *((int *)buf) = *((int *)src_buf);
  }
}

void AggregatorMax64::PutAggregatedValue(unsigned char *buf, int64_t v, [[maybe_unused]] int64_t factor) {
  if (*((int64_t *)buf) == common::NULL_VALUE_64 || *((int64_t *)buf) < v) {
    stats_updated = false;
    *((int64_t *)buf) = v;
  }
}

void AggregatorMax64::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((int64_t *)src_buf) == common::NULL_VALUE_64) return;
  if (*((int64_t *)buf) == common::NULL_VALUE_64 || *((int64_t *)buf) < *((int64_t *)src_buf)) {
    stats_updated = false;
    *((int64_t *)buf) = *((int64_t *)src_buf);
  }
}

void AggregatorMaxD::PutAggregatedValue(unsigned char *buf, int64_t v, [[maybe_unused]] int64_t factor) {
  if (*((int64_t *)buf) == common::NULL_VALUE_64 || *((double *)buf) < *((double *)(&v))) {
    stats_updated = false;
    *((double *)buf) = *((double *)(&v));
  }
}

void AggregatorMaxD::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((int64_t *)src_buf) == common::NULL_VALUE_64) return;
  if (*((int64_t *)buf) == common::NULL_VALUE_64 || *((double *)buf) < *((double *)src_buf)) {
    stats_updated = false;
    *((double *)buf) = *((double *)src_buf);
  }
}

AggregatorMaxT_UTF::AggregatorMaxT_UTF(int max_len, DTCollation coll) : AggregatorMaxT(max_len), collation(coll) {}

void AggregatorMaxT::PutAggregatedValue(unsigned char *buf, const types::BString &v, [[maybe_unused]] int64_t factor) {
  stats_updated = false;
  DEBUG_ASSERT((uint)val_len >= v.len);
  if (*((unsigned short *)buf) == 0 && buf[2] == 0) {  // still null
    std::memset(buf + 2, 0, val_len);
    *((unsigned short *)buf) = v.len;
    if (v.len > 0)
      std::memcpy(buf + 2, v.val, v.len);
    else
      buf[2] = 1;  // empty string indicator (non-null)
  } else {
    types::BString m((char *)buf + 2, *((unsigned short *)buf));
    if (m < v) {
      std::memset(buf + 2, 0, val_len);
      *((unsigned short *)buf) = v.len;
      if (v.len > 0)
        std::memcpy(buf + 2, v.val, v.len);
      else
        buf[2] = 1;  // empty string indicator (non-null)
    }
  }
}

void AggregatorMaxT::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((unsigned short *)src_buf) == 0 && src_buf[2] == 0) return;
  if (*((unsigned short *)buf) == 0 && buf[2] == 0) {  // still null
    stats_updated = false;
    std::memcpy(buf, src_buf, val_len + 2);
  } else {
    types::BString m((char *)buf + 2, *((unsigned short *)buf));
    types::BString v((char *)src_buf + 2, *((unsigned short *)src_buf));
    if (m < v) {
      stats_updated = false;
      std::memcpy(buf, src_buf, val_len + 2);
    }
  }
}

types::BString AggregatorMaxT::GetValueT(unsigned char *buf) {
  int len = *((unsigned short *)(buf));
  char *p = (char *)(buf + 2);
  if (len == 0) {
    if (*p != 0)                           // empty string indicator: len==0 and nontrivial character
      return types::BString("", 0, true);  // empty string
    return types::BString();               // null value
  }
  types::BString res(p, len);
  return res;
}

void AggregatorMaxT_UTF::PutAggregatedValue(unsigned char *buf, const types::BString &v,
                                            [[maybe_unused]] int64_t factor) {
  stats_updated = false;
  DEBUG_ASSERT((uint)val_len >= v.len);
  if (*((unsigned short *)buf) == 0 && buf[2] == 0) {  // still null
    std::memset(buf + 2, 0, val_len);
    *((unsigned short *)buf) = v.len;
    if (v.len > 0)
      std::memcpy(buf + 2, v.val, v.len);
    else
      buf[2] = 1;  // empty string indicator (non-null)
  } else {
    types::BString m((char *)buf + 2, *((unsigned short *)buf));
    if (CollationStrCmp(collation, m, v) < 0) {
      std::memset(buf + 2, 0, val_len);
      *((unsigned short *)buf) = v.len;
      if (v.len > 0)
        std::memcpy(buf + 2, v.val, v.len);
      else
        buf[2] = 1;  // empty string indicator (non-null)
    }
  }
}

void AggregatorMaxT_UTF::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((unsigned short *)src_buf) == 0 && src_buf[2] == 0) return;
  if (*((unsigned short *)buf) == 0 && buf[2] == 0) {  // still null
    stats_updated = false;
    std::memcpy(buf, src_buf, val_len + 2);
  } else {
    types::BString m((char *)buf + 2, *((unsigned short *)buf));
    types::BString v((char *)src_buf + 2, *((unsigned short *)src_buf));
    if (CollationStrCmp(collation, m, v) < 0) {
      stats_updated = false;
      std::memcpy(buf, src_buf, val_len + 2);
    }
  }
}

int64_t AggregatorList32::GetValue64(unsigned char *buf) {
  if (*((int *)buf) == common::NULL_VALUE_32) return common::NULL_VALUE_64;
  return *((int *)buf);
}

void AggregatorListT::PutAggregatedValue(unsigned char *buf, const types::BString &v, [[maybe_unused]] int64_t factor) {
  DEBUG_ASSERT((uint)val_len >= v.len);
  if (*((unsigned short *)buf) == 0 && buf[2] == 0) {  // still null
    stats_updated = false;
    *((unsigned short *)buf) = v.len;
    if (v.len > 0)
      std::memcpy(buf + 2, v.val, v.len);
    else
      buf[2] = 1;  // empty string indicator (non-null)
    value_set = true;
  }
}

void AggregatorListT::Merge(unsigned char *buf, unsigned char *src_buf) {
  if (*((unsigned short *)buf) == 0 && buf[2] == 0 && (*((unsigned short *)src_buf) != 0 || src_buf[2] != 0)) {
    stats_updated = false;
    std::memcpy(buf, src_buf, val_len + 2);
    value_set = true;
  }
}

types::BString AggregatorListT::GetValueT(unsigned char *buf) {
  int len = *((unsigned short *)(buf));
  char *p = (char *)(buf + 2);
  if (len == 0) {
    if (*p != 0)                           // empty string indicator: len==0 and nontrivial character
      return types::BString("", 0, true);  // empty string
    return types::BString();               // null value
  }
  types::BString res(p, len);
  return res;
}
}  // namespace core
}  // namespace stonedb
