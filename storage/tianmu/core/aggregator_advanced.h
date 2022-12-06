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
#ifndef TIANMU_CORE_AGGREGATOR_ADVANCED_H_
#define TIANMU_CORE_AGGREGATOR_ADVANCED_H_
#pragma once

#include <map>

#include "core/aggregator.h"
#include "core/temp_table.h"
#include "util/log_ctl.h"

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

//! Abstract class for all "second central moment"-like statistical operations,
//  i.e. VAR_POP, VAR_SAMP, STD_POP, STD_SAMP
class AggregatorStat : public TIANMUAggregator {
 public:
  // Buffer contents for all functions, 8+8+8 bytes:
  //	<no_obj> <A> <Q>  - int64_t, double, double
  AggregatorStat() = default;
  AggregatorStat(const AggregatorStat &sec) : TIANMUAggregator(sec) {}
  void Merge(unsigned char *buf, unsigned char *src_buf) override;
  int BufferByteSize() override { return 24; }
  void Reset(unsigned char *buf) override {
    *((int64_t *)buf) = 0;
    *((double *)(buf + 8)) = 0;
    *((double *)(buf + 16)) = 0;
  }

 protected:
  int64_t &NumOfObj(unsigned char *buf) { return *((int64_t *)buf); }
  // efficient implementation from WIKI
  // http://en.wikipedia.org/wiki/Standard_deviation
  double &A(unsigned char *buf) { return *((double *)(buf + 8)); }
  double &Q(unsigned char *buf) { return *((double *)(buf + 16)); }
  double VarPop(unsigned char *buf) { return Q(buf) / NumOfObj(buf); }
  double VarSamp(unsigned char *buf) { return Q(buf) / (NumOfObj(buf) - 1); }
};

class AggregatorStat64 : public AggregatorStat {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorStat64(int precision) : prec_factor(types::PowOfTen(precision)) {}
  AggregatorStat64(const AggregatorStat64 &sec) : AggregatorStat(sec), prec_factor(sec.prec_factor) {}
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;

 protected:
  double prec_factor;  // precision factor: the values must be divided by it
};

class AggregatorStatD : public AggregatorStat {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorStatD() = default;
  AggregatorStatD(const AggregatorStatD &sec) : AggregatorStat(sec) {}
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override;
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;

  double GetValueD(unsigned char *buf) override {
    int64_t v = GetValue64(buf);
    return *(double *)(&v);
  }
};

class AggregatorVarPop64 : public AggregatorStat64 {
 public:
  AggregatorVarPop64(int precision) : AggregatorStat64(precision) {}
  AggregatorVarPop64(const AggregatorVarPop64 &sec) : AggregatorStat64(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorVarPop64(*this); }
  int64_t GetValue64(unsigned char *buf) override;
};

class AggregatorVarSamp64 : public AggregatorStat64 {
 public:
  AggregatorVarSamp64(int precision) : AggregatorStat64(precision) {}
  AggregatorVarSamp64(const AggregatorVarSamp64 &sec) : AggregatorStat64(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorVarSamp64(*this); }
  int64_t GetValue64(unsigned char *buf) override;
};

class AggregatorStdPop64 : public AggregatorStat64 {
 public:
  AggregatorStdPop64(int precision) : AggregatorStat64(precision) {}
  AggregatorStdPop64(const AggregatorStdPop64 &sec) : AggregatorStat64(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorStdPop64(*this); }
  int64_t GetValue64(unsigned char *buf) override;
};

class AggregatorStdSamp64 : public AggregatorStat64 {
 public:
  AggregatorStdSamp64(int precision) : AggregatorStat64(precision) {}
  AggregatorStdSamp64(const AggregatorStdSamp64 &sec) : AggregatorStat64(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorStdSamp64(*this); }
  int64_t GetValue64(unsigned char *buf) override;
};

class AggregatorVarPopD : public AggregatorStatD {
 public:
  AggregatorVarPopD() = default;
  AggregatorVarPopD(const AggregatorVarPopD &sec) : AggregatorStatD(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorVarPopD(*this); }
  int64_t GetValue64(unsigned char *buf) override;
};

class AggregatorVarSampD : public AggregatorStatD {
 public:
  AggregatorVarSampD() = default;
  AggregatorVarSampD(const AggregatorVarSampD &sec) : AggregatorStatD(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorVarSampD(*this); }
  int64_t GetValue64(unsigned char *buf) override;
};

class AggregatorStdPopD : public AggregatorStatD {
 public:
  AggregatorStdPopD() = default;
  AggregatorStdPopD(const AggregatorStdPopD &sec) : AggregatorStatD(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorStdPopD(*this); }
  int64_t GetValue64(unsigned char *buf) override;
};

class AggregatorStdSampD : public AggregatorStatD {
 public:
  AggregatorStdSampD() = default;
  AggregatorStdSampD(const AggregatorStdSampD &sec) : AggregatorStatD(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorStdSampD(*this); }
  int64_t GetValue64(unsigned char *buf) override;
};

class AggregatorBitAnd : public TIANMUAggregator {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorBitAnd() = default;
  AggregatorBitAnd(const AggregatorBitAnd &sec) : TIANMUAggregator(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorBitAnd(*this); }
  int BufferByteSize() override { return 8; }
  void Reset(unsigned char *buf) override { *((int64_t *)buf) = 0xFFFFFFFFFFFFFFFFULL; }
  void PutAggregatedValue(unsigned char *buf, int64_t v, [[maybe_unused]] int64_t factor) override {
    *((int64_t *)buf) = (*((int64_t *)buf) & v);
  }
  void Merge(unsigned char *buf, unsigned char *src_buf) override {
    *((int64_t *)buf) = (*((int64_t *)buf) & *((int64_t *)src_buf));
  }
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  int64_t GetValue64(unsigned char *buf) override { return *((int64_t *)buf); }
  bool FactorNeeded() override { return false; }
  bool IgnoreDistinct() override { return true; }
};

class AggregatorBitOr : public TIANMUAggregator {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorBitOr() = default;
  AggregatorBitOr(const AggregatorBitOr &sec) : TIANMUAggregator(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorBitOr(*this); }
  int BufferByteSize() override { return 8; }
  void Reset(unsigned char *buf) override { *((int64_t *)buf) = 0; }
  void PutAggregatedValue(unsigned char *buf, int64_t v, [[maybe_unused]] int64_t factor) override {
    *((int64_t *)buf) = (*((int64_t *)buf) | v);
  }
  void Merge(unsigned char *buf, unsigned char *src_buf) override {
    *((int64_t *)buf) = (*((int64_t *)buf) | *((int64_t *)src_buf));
  }
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  int64_t GetValue64(unsigned char *buf) override { return *((int64_t *)buf); }
  bool FactorNeeded() override { return false; }
  bool IgnoreDistinct() override { return true; }
};

class AggregatorBitXor : public TIANMUAggregator {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorBitXor() : TIANMUAggregator() {}
  AggregatorBitXor(const AggregatorBitXor &sec) : TIANMUAggregator(sec) {}
  TIANMUAggregator *Copy() override { return new AggregatorBitXor(*this); }
  int BufferByteSize() override { return 8; }
  void Reset(unsigned char *buf) override { *((int64_t *)buf) = 0; }
  void PutAggregatedValue(unsigned char *buf, int64_t v, int64_t factor) override {
    if (factor % 2 == 1)
      *((int64_t *)buf) = (*((int64_t *)buf) ^ v);
  }
  void Merge(unsigned char *buf, unsigned char *src_buf) override {
    *((int64_t *)buf) = (*((int64_t *)buf) ^ *((int64_t *)src_buf));
  }
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  int64_t GetValue64(unsigned char *buf) override { return *((int64_t *)buf); }
};

class AggregatorGroupConcat : public TIANMUAggregator {
 public:
  using TIANMUAggregator::PutAggregatedValue;
  AggregatorGroupConcat() = delete;
  AggregatorGroupConcat(SI si, common::ColumnType type) : si(si), attrtype(type) {}
  AggregatorGroupConcat(const AggregatorGroupConcat &sec)
      : TIANMUAggregator(sec), si(sec.si), gconcat_maxlen(sec.gconcat_maxlen), attrtype(sec.attrtype) {}

  TIANMUAggregator *Copy() override { return new AggregatorGroupConcat(*this); }
  int BufferByteSize() override { return gconcat_maxlen; }
  void Reset(unsigned char *buf) override { *((int64_t *)buf) = 0; }
  void PutAggregatedValue([[maybe_unused]] unsigned char *buf, [[maybe_unused]] int64_t v,
                          [[maybe_unused]] int64_t factor) override {
    TIANMU_ERROR("Internal error: invalid call for AggregatorGroupConcat");
    return;
  }
  void Merge([[maybe_unused]] unsigned char *buf, [[maybe_unused]] unsigned char *src_buf) override {
    TIANMU_ERROR("Internal error: invalid call for AggregatorGroupConcat");
    return;
  }
  void PutAggregatedValue(unsigned char *buf, const types::BString &v, int64_t factor) override;
  int64_t GetValue64([[maybe_unused]] unsigned char *buf) override {
    TIANMU_ERROR("Internal error: invalid call for AggregatorGroupConcat");
    return -1;
  }
  types::BString GetValueT(unsigned char *buf) override;

 private:
  const SI si{",", ORDER::ORDER_NOT_RELEVANT};
  const uint gconcat_maxlen = tianmu_group_concat_max_len;
  std::map<unsigned char *, unsigned int> lenmap;  // store aggregation column length
  common::ColumnType attrtype = common::ColumnType::STRING;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_AGGREGATOR_ADVANCED_H_
