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

#include "quick_math.h"

namespace Tianmu {
namespace core {
const double QuickMath::logof2 = log(2.0);
double QuickMath::tab_nlog2n[QuickMath::MAX_NLOG2N + 1];
double QuickMath::tab_log2[QuickMath::MAX_LOG2 + 1];
double QuickMath::tab_pow10f[QuickMath::MAX_POW10 + 1];
int64_t QuickMath::tab_pow10i[QuickMath::MAX_POW10 + 1];

void QuickMath::Init() {
  tab_nlog2n[0] = tab_log2[0] = 0.0;
  for (uint n = 1; n <= MAX_LOG2; n++) tab_log2[n] = log2((double)n);
  for (uint n = 1; n <= MAX_NLOG2N; n++) tab_nlog2n[n] = n * log2(n);

  tab_pow10f[0] = 1.0;
  tab_pow10i[0] = 1;
  for (uint n = 1; n <= MAX_POW10; n++) {
    tab_pow10i[n] = tab_pow10i[n - 1] * 10;
    tab_pow10f[n] = (double)tab_pow10i[n];
  }
}

uint QuickMath::precision10(uint64_t n) {
  uint e = 0;
  while ((e <= MAX_POW10) && (n >= uint64_t(tab_pow10i[e]))) e++;
  return e;
}

QuickMath ___math___(1);  // force initialization of static members
}  // namespace core
}  // namespace Tianmu
