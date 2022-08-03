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
#ifndef TIANMU_CORE_QUICK_MATH_H_
#define TIANMU_CORE_QUICK_MATH_H_
#pragma once

#include <cmath>

#include "common/assert.h"
#include "common/common_definitions.h"
#include "core/bin_tools.h"

namespace Tianmu {
namespace core {
// Precomputed values of some mathematical formulas, used to speed up
// computations
class QuickMath {
 public:
  static const uint MAX_NLOG2N = 65536;
  static const uint MAX_LOG2 = 65536;
  static const uint MAX_POW10 = 18;

 private:
  // array of precomputed values of n*log2(n), for n=0,...,2^16  - for fast
  // computation of entropy
  static double tab_nlog2n[MAX_NLOG2N + 1];
  static double tab_log2[MAX_LOG2 + 1];  // precomputed values of log2(n)
  static double tab_pow10f[MAX_POW10 + 1];
  static int64_t tab_pow10i[MAX_POW10 + 1];
  static void Init();

 public:
  QuickMath() {}
  QuickMath(int) {
    Init();
  }  // should be invoked exactly once during program execution, to force
     // initialization
  static const double logof2;
  static double nlog2n(uint n) {
    DEBUG_ASSERT(n <= MAX_NLOG2N);
    return tab_nlog2n[n];
  }
  // static double log(uint n)			{ DEBUG_ASSERT(n <= MAX_LOG);
  // return tab_log[n]; }
  static double log2(uint n) {
    DEBUG_ASSERT(n <= MAX_LOG2);
    return tab_log2[n];
  }
  static double log2(double x) { return std::log(x) / logof2; }
  // returns 10^n
  static double power10f(uint n) {
    DEBUG_ASSERT(n <= MAX_POW10);
    return tab_pow10f[n];
  }
  static int64_t power10i(uint n) {
    DEBUG_ASSERT(n <= MAX_POW10);
    return tab_pow10i[n];
  }

  // number of digits in decimal notation of 'n'
  static uint precision10(uint64_t n);

  // number of bits in binary notation of 'n'
  static uint precision2(uint64_t n) { return GetBitLen(n); }
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_QUICK_MATH_H_
