//<!***************************************************************************
/**
 * Copyright (C) 2021-2022 StoneAtom Group Holding Limited
 */
//<!***************************************************************************

#include "mi_rough_sorter.h"

namespace stonedb {
namespace core {
MINewContentsRSorter::MINewContentsRSorter(MultiIndex *_m, IndexTable **t_new, int min_block_shift) {
  rough_comp_bit_offset = 16;  // default - pack size
  bound_queue_size = 2 + MAX_SORT_SIZE / 10;

  worker = new MINewContentsRSorterWorker(bound_queue_size, this);

  mind = _m;
  p_power = mind->NoPower();
  no_dim = mind->NoDimensions();

  tcomp = new IndexTable *[no_dim];  // these table should be compared (more
                                     // than one pack, potentially)
  tsort = new IndexTable *[no_dim];
  tall = new IndexTable *[no_dim];  // all tables
  tcheck = new bool[no_dim];        // these dimensions should be checked for pack numbers

  no_cols_to_compare = 0;
  no_cols_to_sort = 0;
  for (int dim = 0; dim < no_dim; dim++) {
    tall[dim] = NULL;  // will remain NULL if the dimension is not covered by sorter
    AddColumn(t_new[dim], dim);
  }
  last_pack = new int[no_dim];
  block_size = (int64_t(1)) << min_block_shift;
  ResetStats();
  start_sorting = 0;
  synchronized = true;
}

MINewContentsRSorter::~MINewContentsRSorter() {
  delete worker;
  delete[] tcomp;
  delete[] tsort;
  delete[] tcheck;
  delete[] tall;
  delete[] last_pack;
}

void MINewContentsRSorter::AddColumn(IndexTable *t, int dim) {
  DEBUG_ASSERT(t == NULL || tall[dim] == NULL);  // else already added - something is wrong!
  tall[dim] = t;
  tcheck[dim] = false;
  if (t) {
    tsort[no_cols_to_sort++] = t;
    if (mind->OrigSize(dim) > (1U << p_power)) {
      tcomp[no_cols_to_compare++] = tall[dim];
      tcheck[dim] = true;
    }
  }
}

void MINewContentsRSorter::Commit(int64_t obj) {
  if (sorting_needed) {
    synchronized = false;
    for (int i = 0; i < no_cols_to_sort; i++) tsort[i]->Get64(start_sorting);  // make sure the proper block is loaded
    // Sort now
    worker->RoughSort(start_sorting, obj - 1);
  }
  ResetStats();
  start_sorting = obj;
}

void MINewContentsRSorter::ResetStats() {
  for (int dim = 0; dim < no_dim; dim++) {
    last_pack[dim] = -1;
  }
  anything_changed = false;
  sorting_needed = false;
  std::memset(minihash_table, 0, sizeof(minihash_table));
}

void MINewContentsRSorter::Barrier()  // finish all unfinished work, if any
{
  if (synchronized) return;
  worker->Barrier();
  synchronized = true;
}

void MINewContentsRSorter::RoughQSort(uint *bound_queue, uint64_t start_tuple, uint64_t stop_tuple) {
  // Sorting itself
  uint64_t s1, s2, i, j, mid;
  uint64_t *mid_val = new uint64_t[no_cols_to_compare];
  bool not_done;
  bound_queue[0] = 0;
  bound_queue[1] = uint(stop_tuple - start_tuple);
  int queue_read = 0, queue_write = 2;  // jump by two
  while (queue_read != queue_write) {   // this loop is instead of Quicksort recurrency
    s1 = bound_queue[queue_read] + start_tuple;
    s2 = bound_queue[queue_read + 1] + start_tuple;
    queue_read = (queue_read + 2) % bound_queue_size;
    if (s1 == s2) continue;
    if (s2 - s1 < 20) {  // bubble sort for small buffers
      j = s2;
      do {
        not_done = false;
        for (uint64_t i = s1; i < s2; i++)
          if (RoughCompare(i, i + 1) > 0) {  // sorting includes stop_tuple
            SwitchMaterial(i, i + 1);
            not_done = true;
          }
      } while (not_done);
    } else {
      i = s1;  // QuickSort here
      j = s2;
      mid = (i + j) / 2;
      FillMidValues(mid, mid_val);
      do {
        while (RoughCompareWithMid(i, mid_val) < 0) i++;
        while (RoughCompareWithMid(j, mid_val) > 0) j--;
        if (i <= j) {
          if (i != j) {
            SwitchMaterial(i, j);
            if (i == mid) {
              mid = j;  // move the middle point, if needed
              FillMidValues(mid, mid_val);
            } else if (j == mid) {
              mid = i;
              FillMidValues(mid, mid_val);
            }
          }
          i++;
          j--;
        }
      } while (i <= j);
      if (s1 < j) {
        bound_queue[queue_write] = uint(s1 - start_tuple);
        bound_queue[queue_write + 1] = uint(j - start_tuple);
        queue_write += 2;
        if (queue_write >= bound_queue_size)  // cyclic buffer
          queue_write = queue_write - bound_queue_size;
      }
      if (i < s2) {
        bound_queue[queue_write] = uint(i - start_tuple);
        bound_queue[queue_write + 1] = uint(s2 - start_tuple);
        queue_write += 2;
        if (queue_write >= bound_queue_size)  // cyclic buffer
          queue_write = queue_write - bound_queue_size;
      }
    }
  }
  delete[] mid_val;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

MINewContentsRSorterWorker::MINewContentsRSorterWorker(int _bound_queue_size, MINewContentsRSorter *_rough_sorter) {
  bound_queue_size = _bound_queue_size;
  bound_queue = new uint[bound_queue_size + 1];
  rough_sorter = _rough_sorter;
}

MINewContentsRSorterWorker::~MINewContentsRSorterWorker() { delete[] bound_queue; }
}  // namespace core
}  // namespace stonedb