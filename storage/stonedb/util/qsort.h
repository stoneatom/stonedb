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
#ifndef STONEDB_UTIL_QSORT_H_
#define STONEDB_UTIL_QSORT_H_
#pragma once

using comp_func_ib = int (*)(const void *, const void *);

inline void __ib_swap_local(char *a, char *b, int len) {
  for (int j = 0; j < len; j++) {
    char t = a[j];
    a[j] = b[j];
    b[j] = t;
  }
}

static void __bubble_sort(char *b, int lo, int hi, int s, comp_func_ib cmpsrt) {
  bool swapped = true;
  int i;

  while (swapped) {
    // forward direction
    swapped = false;
    for (i = lo; i < hi; i++) {
      if (cmpsrt((void *)(b + i * s), (void *)(b + (i + 1) * s)) > 0) {
        __ib_swap_local(b + i * s, b + (i + 1) * s, s);
        swapped = true;
      }
    }

    // no swap - means sorted.
    if (!swapped) break;

    // backward direction
    swapped = false;
    hi--;

    for (i = hi; i > lo; i--) {
      if (cmpsrt((void *)(b + i * s), (void *)(b + (i - 1) * s)) < 0) {
        __ib_swap_local(b + i * s, b + (i - 1) * s, s);
        swapped = true;
      }
    }
    lo++;
  }
}

#define PIVOT_SIZE 32

static void __quicksort_ib(char *b, int lo, int hi, int s, comp_func_ib cmpsrt, int call_seq) {
  if (call_seq > 8192) {
    __bubble_sort(b, lo, hi, s, cmpsrt);
    return;
  }
  int i = lo;
  int j = hi;
  int pv = (lo + hi) / 2;

  char _space[PIVOT_SIZE];
  char *pivot = _space;

  // actual byte position for elements where i, j are the index position.
  int ii = i * s;
  int jj = j * s;

  if (s > PIVOT_SIZE) {
    pivot = (char *)malloc(s);
    if (!pivot) return;
  }
  std::memcpy(pivot, b + pv * s, s);

  while (i <= j) {
    while (cmpsrt((void *)(b + ii), (void *)pivot) < 0) {
      i++;
      ii += s;
    };
    while (cmpsrt((void *)(b + jj), (void *)pivot) > 0) {
      j--;
      jj -= s;
    };
    if (i <= j) {
      __ib_swap_local(b + ii, b + jj, s);
      i++;
      ii += s;
      j--;
      jj -= s;
    }
  }
  if (lo < j) __quicksort_ib(b, lo, j, s, cmpsrt, call_seq + 1);
  if (i < hi) __quicksort_ib(b, i, hi, s, cmpsrt, call_seq + 1);

  if (s > PIVOT_SIZE) free(pivot);
}

// buf, total elements, element size, compare function
static void qsort_ib(void *b, int l, int s, comp_func_ib cmpsrt) { __quicksort_ib((char *)b, 0, l - 1, s, cmpsrt, 0); }

#endif  // STONEDB_UTIL_QSORT_H_
