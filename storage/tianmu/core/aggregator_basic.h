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
#ifndef TIANMU_CORE_AGGREGATOR_BASIC_H_
#define TIANMU_CORE_AGGREGATOR_BASIC_H_
#pragma once

#include "core/aggregator.h"

namespace Tianmu {
namespace core {
/*! \brief A generalization of aggregation algorithms (counters) to be used in
 * GROUP BY etc. See "Aggregator.h" for more details.
 *
 * \note The aim of this class hierarchy is to make aggregators more pluggable,
 * as well as to replace multiple ifs and switch/cases by virtual methods.
 * Therefore it is suggested to implement all variants as separate subclasses,
 * e.g. to distinguish 32- and 64-bit counters.
 */

/*!
 * \brief An aggregator for SUM(...) of numerical (int64_t) values.
 *
 * The counter consists of just one 64-bit value:
 *     <cur_sum_64>
 * Start value: common::NULL_VALUE_64.
 * Throws an exception on overflow.
 */
class AggregatorSum64 : public TIANMUAggregator {
  using TIANMUAggregator::PutAggregatedValue;

 public:
  AggregatorSum64() : TIANMUAggregator() {
    pack_sum = 0;
    pack_min = 0;
    pack_max = 0;
    null_group_found = false;
  }
  AggregatorSum64(AggregatorSum64 &sec)
      : TIANMUAggregator(sec),
        pack_sum(sec.pack_sum),
        pack_min(sec.pack_min),
        pack_max(sec.pack_max),
        null_group_found(sec.null_group_found) {}

  TIANMUAggregator *Copy() override { return new AggregatorSum64(*this); }
  int BufferByteSize() override { return 8; }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  int64_t GetValue64(unsigned char *buf) override { return *((int64_t *)buf); }
  void Reset(unsigned char *buf) override { *((int64_t *)buf) = common::NULL_VALUE_64; }
  // Optimization part
  bool PackAggregationNeedsSum() override { return true; }
  void SetAggregatePackSum(int64_t par1, int64_t factor) override;
  bool AggregatePack(unsigned char *buf) override;

  // if a data pack contains only 0, then no need to update
  bool PackAggregationNeedsMin() override { return true; }
  bool PackAggregationNeedsMax() override { return true; }
  void SetAggregatePackMin(int64_t par1) override { pack_min = par1; }
  void SetAggregatePackMax(int64_t par1) override { pack_max = par1; }
  void ResetStatistics() override {
    null_group_found = false;
    stats_updated = false;
  }
  bool UpdateStatistics(unsigned char *buf) override {
    if (*((int64_t *)buf) == common::NULL_VALUE_64)
      null_group_found = true;
    return null_group_found;  // if found, do not search any more
  }
  bool PackCannotChangeAggregation() override {
    DEBUG_ASSERT(stats_updated);
    return !null_group_found && pack_min == 0 && pack_max == 0;  // uniform 0 pack - no change for sum
  }

 private:
  int64_t pack_sum;
  int64_t pack_min;  // min and max are used to check whether a pack may update
                     // sum (i.e. both 0 means "no change")
  int64_t pack_max;
  bool null_group_found;  // true if SetStatistics found a null group (no
                          // optimization possible)
};

/*!
 * \brief An aggregator for SUM(...) of double values.
 *
 * The counter consists of just one double value:
 *     <cur_sum_double>
 * Start value: NULL_VALUE_D.
 */
class AggregatorSumD : public TIANMUAggregator {
  using TIANMUAggregator::PutAggregatedValue;

 public:
  AggregatorSumD() : TIANMUAggregator() { pack_sum = 0; }
  AggregatorSumD(AggregatorSumD &sec) : TIANMUAggregator(sec), pack_sum(sec.pack_sum) {}
  TIANMUAggregator *Copy() override { return new AggregatorSumD(*this); }
  int BufferByteSize() override { return 8; }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  int64_t GetValue64(unsigned char *buf) override { return *((int64_t *)buf); }  // double passed as 64-bit code
  double GetValueD(unsigned char *buf) override { return *((double *)buf); }
  void Reset(unsigned char *buf) override { *((double *)buf) = NULL_VALUE_D; }
  ///////////// Optimization part /////////////////
  bool PackAggregationNeedsSum() override { return true; }
  void SetAggregatePackSum(int64_t par1, int64_t factor) override {
    double d = *((double *)(&par1)) * factor;
    pack_sum = *((int64_t *)(&d));
  }
  bool AggregatePack(unsigned char *buf) override;

 private:
  int64_t pack_sum;
};

/*!
 * \brief An aggregator for AVG(...) of numerical (int64_t) values.
 *
 * The counter consists of a double sum and counter:
 *     <cur_sum_double><cur_count_64>
 * Start value: 0, but GetValueD will return NULL_VALUE_D.
 */
class AggregatorAvg64 : public TIANMUAggregator {
  using TIANMUAggregator::PutAggregatedValue;

 public:
  AggregatorAvg64(int precision) {
    prec_factor = types::PowOfTen(precision);
    warning_issued = false;
    pack_not_nulls = 0;
    pack_sum = 0;
  }
  AggregatorAvg64(AggregatorAvg64 &sec)
      : TIANMUAggregator(sec),
        pack_sum(sec.pack_sum),
        pack_not_nulls(sec.pack_not_nulls),
        prec_factor(sec.prec_factor),
        warning_issued(sec.warning_issued) {}
  TIANMUAggregator *Copy() override { return new AggregatorAvg64(*this); }
  int BufferByteSize() override { return 16; }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  double GetValueD(unsigned char *buf) override;
  int64_t GetValue64(unsigned char *buf) override {  // double passed as 64-bit code
    double res = GetValueD(buf);
    return *((int64_t *)(&res));
  }

  void Reset(unsigned char *buf) override {
    *((double *)buf) = 0;
    *((int64_t *)(buf + 8)) = 0;
  }

  ///////////// Optimization part /////////////////
  bool PackAggregationNeedsSum() override { return true; }
  bool PackAggregationNeedsNotNulls() override { return true; }
  void SetAggregatePackSum(int64_t par1, int64_t factor) override { pack_sum = double(par1) * factor; }
  void SetAggregatePackNotNulls(int64_t par1) override { pack_not_nulls = par1; }
  bool AggregatePack(unsigned char *buf) override;

 private:
  double pack_sum;
  int64_t pack_not_nulls;

  double prec_factor;  // precision factor: the calculated avg must be divided
                       // by it
  bool warning_issued;
};

/*!
 * \brief An aggregator for AVG(...) of double values.
 *
 * The counter consists of a double sum and counter:
 *     <cur_sum_double><cur_count_64>
 * Start value: 0, but GetValueD will return NULL_VALUE_D.
 */
class AggregatorAvgD : public TIANMUAggregator {
 public:
  using TIANMUAggregator::PutAggregatedValue;

  AggregatorAvgD() : TIANMUAggregator() {
    pack_not_nulls = 0;
    pack_sum = 0;
  }
  AggregatorAvgD(AggregatorAvgD &sec)
      : TIANMUAggregator(sec), pack_sum(sec.pack_sum), pack_not_nulls(sec.pack_not_nulls) {}
  TIANMUAggregator *Copy() override { return new AggregatorAvgD(*this); }
  int BufferByteSize() override { return 16; }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  double GetValueD(unsigned char *buf) override;
  int64_t GetValue64(unsigned char *buf) override {  // double passed as 64-bit code
    double res = GetValueD(buf);
    return *((int64_t *)(&res));
  }

  void Reset(unsigned char *buf) override {
    *((double *)buf) = 0;
    *((int64_t *)(buf + 8)) = 0;
  }

  ///////////// Optimization part /////////////////
  bool PackAggregationNeedsSum() override { return true; }
  bool PackAggregationNeedsNotNulls() override { return true; }
  void SetAggregatePackSum(int64_t par1, int64_t factor) override { pack_sum = *((double *)(&par1)) * factor; }
  void SetAggregatePackNotNulls(int64_t par1) override { pack_not_nulls = par1; }
  bool AggregatePack(unsigned char *buf) override;

 private:
  double pack_sum;
  int64_t pack_not_nulls;
};

/*!
 * \brief An aggregator for AVG(...) of year values.
 *
 * The counter consists of a double sum and counter:
 *     <cur_sum_double><cur_count_64>
 * Start value: 0, but GetValueD will return NULL_VALUE_D.
 */

class AggregatorAvgYear : public AggregatorAvgD {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorAvgYear() : AggregatorAvgD() {}
  AggregatorAvgYear(AggregatorAvgYear &sec) : AggregatorAvgD(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorAvgYear(*this); }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;

  // Optimization part
  bool PackAggregationNeedsSum() override { return false; }
  bool PackAggregationNeedsNotNulls() override { return false; }
  bool AggregatePack([[maybe_unused]] unsigned char *buf) override { return false; }  // no optimization
};

//! Abstract class for all kinds of MIN(...)
class AggregatorMin : public TIANMUAggregator {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorMin() : TIANMUAggregator() {}
  AggregatorMin(AggregatorMin &sec) : TIANMUAggregator(sec) {}
  // Optimization part
  bool PackAggregationDistinctIrrelevant() override { return true; }
  bool FactorNeeded() override { return false; }
  bool IgnoreDistinct() override { return true; }
  bool PackAggregationNeedsMin() override { return true; }
};

/*!
 * \brief An aggregator for MIN(...) of int32 values.
 *
 * The counter consists of a current min:
 *     <cur_min_32>
 * Start value: NULL_VALUE (32 bit), will return common::NULL_VALUE_64.
 */
class AggregatorMin32 : public AggregatorMin {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorMin32() : AggregatorMin() {
    stat_max = 0;
    pack_min = 0;
    null_group_found = false;
  }
  AggregatorMin32(AggregatorMin32 &sec)
      : AggregatorMin(sec), stat_max(sec.stat_max), pack_min(sec.pack_min), null_group_found(sec.null_group_found) {}
  TIANMUAggregator *Copy() override { return new AggregatorMin32(*this); }
  int BufferByteSize() override { return 4; }
  void PutAggregatedValue(unsigned char *buf, int64_t factor) override {
    stats_updated = false;
    if (*((int *)buf) == common::NULL_VALUE_32 || *((int *)buf) > (int)factor)
      *((int *)buf) = (int)factor;
  }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  void SetAggregatePackMin(int64_t par1) override { pack_min = par1; }
  int64_t GetValue64(unsigned char *buf) override;

  void Reset(unsigned char *buf) override { *((int *)buf) = common::NULL_VALUE_32; }
  void ResetStatistics() override {
    null_group_found = false;
    stat_max = INT_MIN;
    stats_updated = false;
  }
  bool UpdateStatistics(unsigned char *buf) override {
    if (*((int *)buf) == common::NULL_VALUE_32)
      null_group_found = true;
    else if (*((int *)buf) > stat_max)
      stat_max = *((int *)buf);
    return false;
  }
  bool PackCannotChangeAggregation() override {
    DEBUG_ASSERT(stats_updated);
    return !null_group_found && pack_min != common::NULL_VALUE_64 && pack_min >= (int64_t)stat_max;
  }

 private:
  int stat_max;           // maximal min found so far
  int64_t pack_min;       // min and max are used to check whether a pack may update
                          // sum (i.e. both 0 means "no change")
  bool null_group_found;  // true if SetStatistics found a null group (no
                          // optimization possible)
};

/*!
 * \brief An aggregator for MIN(...) of int64_t values.
 *
 * The counter consists of a current min:
 *     <cur_min_64>
 * Start value: common::NULL_VALUE_64.
 */
class AggregatorMin64 : public AggregatorMin {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorMin64() : AggregatorMin() {
    stat_max = 0;
    pack_min = 0;
    null_group_found = false;
  }
  AggregatorMin64(AggregatorMin64 &sec)
      : AggregatorMin(sec), stat_max(sec.stat_max), pack_min(sec.pack_min), null_group_found(sec.null_group_found) {}
  TIANMUAggregator *Copy() override { return new AggregatorMin64(*this); }
  int BufferByteSize() override { return 8; }
  void PutAggregatedValue(unsigned char *buf, int64_t factor) override {
    stats_updated = false;
    if (*((int64_t *)buf) == common::NULL_VALUE_64 || *((int64_t *)buf) > (int64_t)factor)
      *((int64_t *)buf) = (int64_t)factor;
  }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  int64_t GetValue64(unsigned char *buf) override { return *((int64_t *)buf); }
  void SetAggregatePackMin(int64_t par1) override { pack_min = par1; }
  bool AggregatePack(unsigned char *buf) override;
  void Reset(unsigned char *buf) override { *((int64_t *)buf) = common::NULL_VALUE_64; }
  void ResetStatistics() override {
    null_group_found = false;
    stat_max = common::MINUS_INF_64;
    stats_updated = false;
  }
  bool UpdateStatistics(unsigned char *buf) override {
    if (*((int64_t *)buf) == common::NULL_VALUE_64)
      null_group_found = true;
    else if (*((int64_t *)buf) > stat_max)
      stat_max = *((int64_t *)buf);
    return false;
  }
  bool PackCannotChangeAggregation() override {
    DEBUG_ASSERT(stats_updated);
    return !null_group_found && pack_min != common::NULL_VALUE_64 && pack_min >= stat_max;
  }

 private:
  int64_t stat_max;
  int64_t pack_min;
  bool null_group_found;  // true if SetStatistics found a null group (no
                          // optimization possible)
};

/*!
 * \brief An aggregator for MIN(...) of double values.
 *
 * The counter consists of a current min:
 *     <cur_min_double>
 * Start value: NULL_VALUE_D.
 */
class AggregatorMinD : public AggregatorMin {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorMinD() : AggregatorMin() {
    stat_max = 0;
    pack_min = 0;
    null_group_found = false;
  }
  AggregatorMinD(AggregatorMinD &sec)
      : AggregatorMin(sec), stat_max(sec.stat_max), pack_min(sec.pack_min), null_group_found(sec.null_group_found) {}
  TIANMUAggregator *Copy() override { return new AggregatorMinD(*this); }
  int BufferByteSize() override { return 8; }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  double GetValueD(unsigned char *buf) override { return *((double *)buf); }
  int64_t GetValue64(unsigned char *buf) override { return *((int64_t *)buf); }  // double passed as 64-bit code
  void SetAggregatePackMin(int64_t par1) override {
    if (par1 == common::MINUS_INF_64)
      pack_min = common::MINUS_INF_DBL;
    else if (par1 == common::PLUS_INF_64)
      pack_min = common::PLUS_INF_DBL;
    else
      pack_min = *((double *)&par1);
  }
  bool AggregatePack(unsigned char *buf) override;

  void Reset(unsigned char *buf) override { *((double *)buf) = NULL_VALUE_D; }
  void ResetStatistics() override {
    null_group_found = false;
    stat_max = NULL_VALUE_D;
    stats_updated = false;
  }
  bool UpdateStatistics(unsigned char *buf) override {
    if (*((int64_t *)buf) == common::NULL_VALUE_64)
      null_group_found = true;
    else if (*(int64_t *)&stat_max == common::NULL_VALUE_64 || *((double *)buf) > stat_max)
      stat_max = *((double *)buf);
    return false;
  }
  bool PackCannotChangeAggregation() override {
    DEBUG_ASSERT(stats_updated);
    return !null_group_found && !IsDoubleNull(pack_min) && !IsDoubleNull(stat_max) && pack_min >= stat_max;
  }

 private:
  double stat_max;
  double pack_min;
  bool null_group_found;  // true if SetStatistics found a null group (no
                          // optimization possible)
};

/*!
 * \brief An aggregator for MIN(...) of string values.
 *
 * The counter consists of a current min, together with its 16-bit length:
 *     <cur_min_len_16><cur_min_txt>
 * Start value: all 0.
 * Null value is indicated by \0,\0,\0 and zero-length non-null string by
 * \0,\0,\1.
 */
class AggregatorMinT : public AggregatorMin {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorMinT(int max_len) : val_len(max_len) {}
  AggregatorMinT(AggregatorMinT &sec) : AggregatorMin(sec), val_len(sec.val_len) {}
  TIANMUAggregator *Copy() override { return new AggregatorMinT(*this); }
  int BufferByteSize() override { return val_len + 2; }
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  types::BString GetValueT(unsigned char *buf) override;
  bool PackAggregationNeedsMin() override { return false; }
  void Reset(unsigned char *buf) override { std::memset(buf, 0, val_len + 2); }

 protected:
  int val_len;  // a maximal length of string stored (without \0 on the end,
                // without len
};

/*!
 * \brief An aggregator for MIN(...) of string values utilizing CHARSET.
 *
 * The counter consists of a current min, together with its 16-bit length:
 *     <cur_min_len_16><cur_min_txt>
 * Start value: all 0.
 * Null value is indicated by \0,\0,\0 and zero-length non-null string by
 * \0,\0,\1.
 */
class AggregatorMinT_UTF : public AggregatorMinT {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorMinT_UTF(int max_len, DTCollation coll);
  AggregatorMinT_UTF(AggregatorMinT_UTF &sec) : AggregatorMinT(sec), collation(sec.collation) {}
  TIANMUAggregator *Copy() override { return new AggregatorMinT_UTF(*this); }
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;

 private:
  DTCollation collation;
};

//! Abstract class for all kinds of MAX(...)
class AggregatorMax : public TIANMUAggregator {
 public:
  AggregatorMax() : TIANMUAggregator() {}
  AggregatorMax(AggregatorMax &sec) : TIANMUAggregator(sec) {}
  // Optimization part
  bool PackAggregationDistinctIrrelevant() override { return true; }
  bool FactorNeeded() override { return false; }
  bool IgnoreDistinct() override { return true; }
  bool PackAggregationNeedsMax() override { return true; }
  using TIANMUAggregator::PutAggregatedValue;
};

/*!
 * \brief An aggregator for MAX(...) of int32 values.
 *
 * The counter consists of a current max:
 *     <cur_max_32>
 * Start value: NULL_VALUE (32 bit), will return common::NULL_VALUE_64.
 */
class AggregatorMax32 : public AggregatorMax {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorMax32() : AggregatorMax() {
    stat_min = 0;
    pack_max = 0;
    null_group_found = false;
  }
  AggregatorMax32(AggregatorMax32 &sec)
      : AggregatorMax(sec), stat_min(sec.stat_min), pack_max(sec.pack_max), null_group_found(sec.null_group_found) {}
  TIANMUAggregator *Copy() override { return new AggregatorMax32(*this); }
  int BufferByteSize() override { return 4; }
  void PutAggregatedValue(unsigned char *buf, int64_t factor) override {
    stats_updated = false;
    if (*((int *)buf) == common::NULL_VALUE_32 || *((int *)buf) < (int)factor)
      *((int *)buf) = (int)factor;
  }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  void SetAggregatePackMax(int64_t par1) override { pack_max = par1; }
  int64_t GetValue64(unsigned char *buf) override;

  void Reset(unsigned char *buf) override { *((int *)buf) = common::NULL_VALUE_32; }
  void ResetStatistics() override {
    null_group_found = false;
    stat_min = INT_MAX;
    stats_updated = false;
  }
  bool UpdateStatistics(unsigned char *buf) override {
    if (*((int *)buf) == common::NULL_VALUE_32)
      null_group_found = true;
    else if (*((int *)buf) < stat_min)
      stat_min = *((int *)buf);
    return false;
  }
  bool PackCannotChangeAggregation() override {
    DEBUG_ASSERT(stats_updated);
    return !null_group_found && pack_max != common::NULL_VALUE_64 && pack_max <= (int64_t)stat_min;
  }

 private:
  int stat_min;           // maximal min found so far
  int64_t pack_max;       // min and max are used to check whether a pack may update
                          // sum (i.e. both 0 means "no change")
  bool null_group_found;  // true if SetStatistics found a null group (no
                          // optimization possible)
};

/*!
 * \brief An aggregator for MAX(...) of int64_t values.
 *
 * The counter consists of a current max:
 *     <cur_max_64>
 * Start value: common::NULL_VALUE_64.
 */
class AggregatorMax64 : public AggregatorMax {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorMax64() : AggregatorMax() {
    stat_min = 0;
    pack_max = 0;
    null_group_found = false;
  }
  AggregatorMax64(AggregatorMax64 &sec)
      : AggregatorMax(sec), stat_min(sec.stat_min), pack_max(sec.pack_max), null_group_found(sec.null_group_found) {}
  TIANMUAggregator *Copy() override { return new AggregatorMax64(*this); }
  int BufferByteSize() override { return 8; }
  void PutAggregatedValue(unsigned char *buf, int64_t factor) override {
    stats_updated = false;
    if (*((int64_t *)buf) == common::NULL_VALUE_64 || *((int64_t *)buf) < (int64_t)factor)
      *((int64_t *)buf) = (int64_t)factor;
  }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  int64_t GetValue64(unsigned char *buf) override { return *((int64_t *)buf); }
  void SetAggregatePackMax(int64_t par1) override { pack_max = par1; }
  bool AggregatePack(unsigned char *buf) override;

  void Reset(unsigned char *buf) override { *((int64_t *)buf) = common::NULL_VALUE_64; }
  void ResetStatistics() override {
    null_group_found = false;
    stat_min = common::PLUS_INF_64;
    stats_updated = false;
  }
  bool UpdateStatistics(unsigned char *buf) override {
    if (*((int64_t *)buf) == common::NULL_VALUE_64)
      null_group_found = true;
    else if (*((int64_t *)buf) < stat_min)
      stat_min = *((int64_t *)buf);
    return false;
  }
  bool PackCannotChangeAggregation() override {
    DEBUG_ASSERT(stats_updated);
    return !null_group_found && pack_max != common::NULL_VALUE_64 && pack_max <= stat_min;
  }

 private:
  int64_t stat_min;
  int64_t pack_max;
  bool null_group_found;  // true if SetStatistics found a null group (no
                          // optimization possible)
};

/*!
 * \brief An aggregator for MAX(...) of double values.
 *
 * The counter consists of a current max:
 *     <cur_max_double>
 * Start value: NULL_VALUE_D.
 */
class AggregatorMaxD : public AggregatorMax {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorMaxD() : AggregatorMax() {
    stat_min = 0;
    pack_max = 0;
    null_group_found = false;
  }
  AggregatorMaxD(AggregatorMaxD &sec)
      : AggregatorMax(sec), stat_min(sec.stat_min), pack_max(sec.pack_max), null_group_found(sec.null_group_found) {}
  TIANMUAggregator *Copy() override { return new AggregatorMaxD(*this); }
  int BufferByteSize() override { return 8; }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  double GetValueD(unsigned char *buf) override { return *((double *)buf); }
  int64_t GetValue64(unsigned char *buf) override { return *((int64_t *)buf); }  // double passed as 64-bit code
  void SetAggregatePackMax(int64_t par1) override {
    if (par1 == common::MINUS_INF_64)
      pack_max = common::MINUS_INF_DBL;
    else if (par1 == common::PLUS_INF_64)
      pack_max = common::PLUS_INF_DBL;
    else
      pack_max = *((double *)&par1);
  }
  bool AggregatePack(unsigned char *buf) override;

  void Reset(unsigned char *buf) override { *((double *)buf) = NULL_VALUE_D; }
  void ResetStatistics() override {
    null_group_found = false;
    stat_min = NULL_VALUE_D;
    stats_updated = false;
  }
  bool UpdateStatistics(unsigned char *buf) override {
    if (*((int64_t *)buf) == common::NULL_VALUE_64)
      null_group_found = true;
    else if (*(int64_t *)&stat_min == common::NULL_VALUE_64 || *((double *)buf) < stat_min)
      stat_min = *((double *)buf);
    return false;
  }
  bool PackCannotChangeAggregation() override {
    DEBUG_ASSERT(stats_updated);
    return !null_group_found && !IsDoubleNull(pack_max) && !IsDoubleNull(stat_min) && pack_max <= stat_min;
  }

 private:
  double stat_min;
  double pack_max;
  bool null_group_found;  // true if SetStatistics found a null group (no
                          // optimization possible)
};

/*!
 * \brief An aggregator for MAX(...) of string values.
 *
 * The counter consists of a current max, together with its 16-bit length:
 *     <cur_max_len_16><cur_max_txt>
 * Start value: all 0.
 * Null value is indicated by \0,\0,\0 and zero-length non-null string by
 * \0,\0,\1.
 */
class AggregatorMaxT : public AggregatorMax {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorMaxT(int max_len) : val_len(max_len) {}
  AggregatorMaxT(AggregatorMaxT &sec) : AggregatorMax(sec), val_len(sec.val_len) {}
  TIANMUAggregator *Copy() override { return new AggregatorMaxT(*this); }
  int BufferByteSize() override { return val_len + 2; }
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  types::BString GetValueT(unsigned char *buf) override;
  bool PackAggregationNeedsMax() override { return false; }
  void Reset(unsigned char *buf) override { std::memset(buf, 0, val_len + 2); }

 protected:
  int val_len;  // a maximal length of string stored (without \0 on the end,
                // without len
};

/*!
 * \brief An aggregator for MAX(...) of string values utilizing CHARSET.
 *
 * The counter consists of a current max, together with its 16-bit length:
 *     <cur_max_len_16><cur_max_txt>
 * Start value: all 0.
 * Null value is indicated by \0,\0,\0 and zero-length non-null string by
 * \0,\0,\1.
 */
class AggregatorMaxT_UTF : public AggregatorMaxT {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorMaxT_UTF(int max_len, DTCollation coll);
  AggregatorMaxT_UTF(AggregatorMaxT_UTF &sec) : AggregatorMaxT(sec), collation(sec.collation) {}
  TIANMUAggregator *Copy() override { return new AggregatorMaxT_UTF(*this); }
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;

 private:
  DTCollation collation;
};

/*!
 * \brief An aggregator just to remember the first int32 values.
 *
 * The counter consists of a current value:
 *     <cur_val_32>
 * Start value: NULL_VALUE (32 bit), will return common::NULL_VALUE_64.
 */
class AggregatorList32 : public TIANMUAggregator {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorList32() : TIANMUAggregator() { value_set = false; }
  AggregatorList32(AggregatorList32 &sec) : TIANMUAggregator(sec), value_set(sec.value_set) {}
  TIANMUAggregator *Copy() override { return new AggregatorList32(*this); }
  int BufferByteSize() override { return 4; }
  void PutAggregatedValue(unsigned char *buf, int64_t v, [[maybe_unused]] int64_t factor) override {
    if (*((int *)buf) == common::NULL_VALUE_32) {
      stats_updated = false;
      *((int *)buf) = (int)v;
      value_set = true;
    }
  }
  void Merge(unsigned char *buf, unsigned char *src_buf) override {
    if (*((int *)buf) == common::NULL_VALUE_32 && *((int *)src_buf) != common::NULL_VALUE_32) {
      stats_updated = false;
      *((int *)buf) = *((int *)src_buf);
      value_set = true;
    }
  }

  int64_t GetValue64(unsigned char *buf) override;

  void Reset(unsigned char *buf) override {
    *((int *)buf) = common::NULL_VALUE_32;
    value_set = false;
  }
  // Optimization part
  bool IgnoreDistinct() override { return true; }
  bool FactorNeeded() override { return false; }
  bool PackCannotChangeAggregation() override { return value_set; }  // all values are unchangeable
  bool UpdateStatistics(unsigned char *buf) override {
    if (*((int *)buf) == common::NULL_VALUE_32)
      value_set = false;
    return !value_set;  // if found, do not search any more
  }

 private:
  bool value_set;  // true, if all values are not null
};

/*!
 * \brief An aggregator just to remember the first int64_t or double values.
 *
 * The counter consists of a current value:
 *     <cur_val_64>
 * Start value: common::NULL_VALUE_64.
 */
class AggregatorList64 : public TIANMUAggregator {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorList64() : TIANMUAggregator() { value_set = false; }
  AggregatorList64(AggregatorList64 &sec) : TIANMUAggregator(sec), value_set(sec.value_set) {}
  TIANMUAggregator *Copy() override { return new AggregatorList64(*this); }
  int BufferByteSize() override { return 8; }
  void PutAggregatedValue(unsigned char *buf, int64_t v, [[maybe_unused]] int64_t factor) override {
    if (*((int64_t *)buf) == common::NULL_VALUE_64) {
      stats_updated = false;
      *((int64_t *)buf) = v;
      value_set = true;
    }
  }
  void Merge(unsigned char *buf, unsigned char *src_buf) override {
    if (*((int64_t *)buf) == common::NULL_VALUE_64 && *((int64_t *)src_buf) != common::NULL_VALUE_64) {
      stats_updated = false;
      *((int64_t *)buf) = *((int64_t *)src_buf);
      value_set = true;
    }
  }

  int64_t GetValue64(unsigned char *buf) override { return *((int64_t *)buf); }
  void Reset(unsigned char *buf) override {
    *((int64_t *)buf) = common::NULL_VALUE_64;
    value_set = false;
  }
  // Optimization part
  bool IgnoreDistinct() override { return true; }
  bool FactorNeeded() override { return false; }
  bool PackCannotChangeAggregation() override { return value_set; }  // all values are unchangeable
  bool UpdateStatistics(unsigned char *buf) override {
    if (*((int64_t *)buf) == common::NULL_VALUE_64)
      value_set = false;
    return !value_set;  // if found, do not search any more
  }

 private:
  bool value_set;  // true, if all values are not null
};

/*!
 * \brief An aggregator just to remember the first string values.
 *
 * The counter consists of a current value, together with its 16-bit length:
 *     <cur_len_16><cur_val_txt>
 * Start value: all 0.
 * Null value is indicated by \0,\0,\0 and zero-length non-null string by
 * \0,\0,\1.
 */
class AggregatorListT : public TIANMUAggregator {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorListT(int max_len) : val_len(max_len) { value_set = false; }
  AggregatorListT(AggregatorListT &sec) : TIANMUAggregator(sec), value_set(sec.value_set), val_len(sec.val_len) {}
  TIANMUAggregator *Copy() override { return new AggregatorListT(*this); }
  int BufferByteSize() override { return val_len + 2; }
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  types::BString GetValueT(unsigned char *buf) override;

  void Reset(unsigned char *buf) override {
    std::memset(buf, 0, val_len + 2);
    value_set = false;
  }
  ///////////// Optimization part /////////////////
  bool IgnoreDistinct() override { return true; }
  bool FactorNeeded() override { return false; }
  bool PackCannotChangeAggregation() override { return value_set; }  // all values are unchangeable
  bool UpdateStatistics(unsigned char *buf) override {
    if (*((unsigned short *)buf) == 0 && buf[2] == 0)
      value_set = false;
    return !value_set;  // if found, do not search any more
  }

 private:
  bool value_set;  // true, if all values are not null
  int val_len;     // a maximal length of string stored (without \0 on the end,
                   // without len
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_AGGREGATOR_BASIC_H_
