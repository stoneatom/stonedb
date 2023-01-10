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

#include <vector>

#include "core/joiner_sort.h"
#include "core/mi_new_contents.h"
#include "core/sorter3.h"
#include "core/transaction.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
JoinerSort::~JoinerSort() { delete outer_filter; }

void JoinerSort::ExecuteJoinConditions(Condition &cond) {
  MEASURE_FET("JoinerSort::ExecuteJoinConditions(...)");
  why_failed = JoinFailure::NOT_FAILED;
  vcolumn::VirtualColumn *vc1 = cond[0].attr.vc;
  vcolumn::VirtualColumn *vc2 = cond[0].val1.vc;
  if (vc1 == nullptr || vc2 == nullptr) {
    why_failed = JoinFailure::FAIL_COMPLEX;
    return;
  } else {                                           // Normalize: let vc1 = a smaller table
    DimensionVector dims1(mind->NumOfDimensions());  // Initial dimension descriptions
    DimensionVector dims2(mind->NumOfDimensions());
    vc1->MarkUsedDims(dims1);
    vc2->MarkUsedDims(dims2);
    if (mind->NumOfTuples(dims1) > mind->NumOfTuples(dims2)) {
      cond[0].SwitchSides();
      vc1 = cond[0].attr.vc;
      vc2 = cond[0].val1.vc;
    }
  }
  bool less = (cond[0].op == common::Operator::O_LESS_EQ || cond[0].op == common::Operator::O_LESS);
  int sharp = ((cond[0].op == common::Operator::O_MORE || cond[0].op == common::Operator::O_LESS)
                   ? 0
                   : 1);  // std::memcmp(...) < 0 for sharp, <= 0 for not sharp

  DimensionVector dims1(mind->NumOfDimensions());  // Initial dimension descriptions
  DimensionVector dims2(mind->NumOfDimensions());
  vc1->MarkUsedDims(dims1);  // smaller table (traversed)
  vc2->MarkUsedDims(dims2);  // bigger table (matched)
  mind->MarkInvolvedDimGroups(dims1);
  mind->MarkInvolvedDimGroups(dims2);

  JoinerSortWrapper sort_encoder(less);
  // Note: we are always checking "traversed < matched" condition. If ">" is
  // needed, we are encoding values as for decreasing order.
  bool compatible = sort_encoder.SetKeyColumns(vc1, vc2);

  if (dims1.Intersects(dims2) ||  // both materialized - we should rather use a simple loop
      !compatible) {              // could not prepare common encoding
    why_failed = JoinFailure::FAIL_COMPLEX;
    return;
  }

  // Analyze additional conditions to be executed by JoinerSort
  DimensionVector dims_used_in_cond(mind->NumOfDimensions());
  DimensionVector dims_sum(dims1);  // only for checking if condition may be executed
  dims_sum.Plus(dims2);
  for (uint i = 1; i < cond.Size(); i++) {
    DimensionVector dims_sec(mind->NumOfDimensions());
    cond[i].DimensionUsed(dims_sec);
    if (dims_sum.Includes(dims_sec)) {  // only additional conditions on the
                                        // same dimensions are legal
      other_cond_exist = true;
      other_cond.push_back(cond[i]);
      dims_used_in_cond.Plus(dims_sec);
    } else {
      why_failed = JoinFailure::FAIL_COMPLEX;  // too complex case
      return;
    }
  }

  MIIterator mit1(mind, dims1);
  MIIterator mit2(mind, dims2);

  // Create outer join part, if needed
  DimensionVector outer_dims(cond[0].right_dims);  // outer_dims will be filled with nulls for
                                                   // non-matching tuples
  outer_nulls_only = false;
  if (!outer_dims.IsEmpty()) {
    if (dims1.Includes(outer_dims)) {
      watch_matched = true;  // watch the non-outer dim for unmatched tuples and
                             // add them with nulls on outer dim
      outer_filter = new Filter(mit2.NumOfTuples(), mit2.GetPower(), true);
      sort_encoder.WatchMatched();
    } else if (dims2.Includes(outer_dims)) {  // outer dims are among matched =>
                                              // watch traversed for unused
      watch_traversed = true;
      outer_filter = new Filter(mit1.NumOfTuples(), mit1.GetPower(), true);
      sort_encoder.WatchTraversed();
    } else {
      why_failed = JoinFailure::FAIL_COMPLEX;  // example: select ... from (t1, t2) left join
                                               // t3 on t1.a < t2.b;
      return;
    }
    outer_nulls_only = true;
    for (int j = 0; j < outer_dims.Size(); j++)
      if (outer_dims[j] && tips.null_only[j] == false)
        outer_nulls_only = false;
  }

  // Create encoder and sorters
  sort_encoder.SetDimensions(mind, dims1, dims2, dims_used_in_cond, tips.count_only);
  int key_bytes = sort_encoder.KeyBytes();
  std::unique_ptr<Sorter3> s1(
      Sorter3::CreateSorter(mit1.NumOfTuples(), key_bytes, sort_encoder.TraverseBytes(), -1,
                            -1));  // mem_modifier = -1, because there is another sorter in memory
  std::unique_ptr<Sorter3> s2(Sorter3::CreateSorter(mit2.NumOfTuples(), key_bytes, sort_encoder.MatchBytes(), -1, -1));

  int64_t actual_s1_size = 0;
  int64_t actual_s2_size = 0;
  int packrows_matched = 0;
  int packrows_omitted = 0;

  // Fill sorters
  int64_t outer_index = 0;
  while (mit1.IsValid()) {
    if (mit1.PackrowStarted()) {
      // Note: rough level based on rough v2 values should be already done in
      // RoughCheck
      vc1->LockSourcePacks(mit1);
      if (m_conn->Killed())
        throw common::KilledException();
    }
    if (!vc1->IsNull(mit1)) {
      s1->PutValue(sort_encoder.EncodeForSorter1(vc1, mit1, outer_index));
      actual_s1_size++;
    }
    ++mit1;
    outer_index++;
  }
  vc1->UnlockSourcePacks();

  outer_index = 0;
  while (mit2.IsValid()) {
    if (mit2.PackrowStarted()) {
      packrows_matched++;
      if (sort_encoder.PackPossible(vc2,
                                    mit2))  // rough comparison with statistics of the first sorter
        vc2->LockSourcePacks(mit2);
      else {
        mit2.NextPackrow();
        packrows_omitted++;
        continue;
      }
      if (m_conn->Killed())
        throw common::KilledException();
    }
    if (!vc2->IsNull(mit2)) {
      s2->PutValue(sort_encoder.EncodeForSorter2(vc2, mit2, outer_index));
      actual_s2_size++;
    }
    ++mit2;
    outer_index++;
  }
  vc2->UnlockSourcePacks();
  sort_encoder.InitCache(actual_s1_size);

  // Initialize a place for result of join (a new multiindex contents)
  MINewContents new_mind(mind, tips);
  new_mind.SetDimensions(dims_sum);
  new_mind.Init(actual_s1_size * actual_s2_size / 2);

  if (packrows_omitted > 0)
    tianmu_control_.lock(m_conn->GetThreadID())
        << "Roughly omitted " << int(packrows_omitted / double(packrows_matched) * 10000.0) / 100.0 << "% packrows."
        << system::unlock;
  tianmu_control_.lock(m_conn->GetThreadID())
      << "Joining sorters created for " << actual_s1_size << " and " << actual_s2_size << " tuples." << system::unlock;

  // the main joiner loop
  int64_t no_of_traversed = 1;
  int64_t result_size = 0;
  unsigned char *cur_traversed = s1->GetNextValue();
  unsigned char *cur_matched = nullptr;
  bool cache_full = false;
  // Note: we are always checking "traversed < matched" condition. If ">" is
  // needed, sorters are defined as descending.
  while (cur_traversed) {
    if (m_conn->Killed())
      throw common::KilledException();
    if (cur_matched == nullptr) {  // the first pass or the end of matched sorter
      if (cache_full) {
        tianmu_control_.lock(m_conn->GetThreadID())
            << "Traversed " << no_of_traversed << "/" << actual_s1_size << " tuples, produced " << result_size
            << " tuples so far." << system::unlock;
        cache_full = false;
        sort_encoder.ClearCache();
        s2->Rewind();
      }
      cur_matched = s2->GetNextValue();
      // find the first matched which is > cur_traversed
      while (cur_matched && !(std::memcmp(cur_traversed, cur_matched, key_bytes) < sharp))
        cur_matched = s2->GetNextValue();
    }
    // find all traversed which also meet the condition
    while (!cache_full && cur_matched) {
      if (cur_traversed && std::memcmp(cur_traversed, cur_matched, key_bytes) < sharp) {  // inequality still holds
        cache_full = !sort_encoder.AddToCache(cur_traversed + key_bytes);
        cur_traversed = s1->GetNextValue();
        no_of_traversed++;
      } else
        break;
    }
    // Now the cache contains all traversed tuples which are <= cur_matched
    if (cur_matched) {
      do {  // do it at least once...
        AddTuples(new_mind, sort_encoder, cur_matched + key_bytes, result_size,
                  dims_used_in_cond);  // Add all tuples from current cache
        cur_matched = s2->GetNextValue();
        if (tips.limit > -1 && result_size >= tips.limit)
          break;
      } while (cur_matched && cur_traversed == nullptr);  // this loop is for the case of cur_traversed completed
    }
    // End when there is no more matched rows and the cache does not need to be
    // rewinded
    if (cur_matched == nullptr && !cache_full)
      break;
    if (tips.limit > -1 && result_size >= tips.limit)
      break;
    if (new_mind.NoMoreTuplesPossible())
      break;  // stop the join if nothing new may be obtained in some optimized
              // cases
  }
  for (auto &j : other_cond) j.UnlockSourcePacks();

  // Outer tuples, if any
  int64_t outer_tuples = 0;
  if (watch_traversed && !outer_filter->IsEmpty())
    outer_tuples += AddOuterTuples(new_mind, sort_encoder, dims1);
  if (watch_matched && !outer_filter->IsEmpty())
    outer_tuples += AddOuterTuples(new_mind, sort_encoder, dims2);
  if (outer_tuples > 0)
    tianmu_control_.lock(m_conn->GetThreadID())
        << "Added " << outer_tuples << " null tuples by outer join." << system::unlock;
  result_size += outer_tuples;

  // Commit the result
  if (tips.count_only) {
    DimensionVector dim_out(dims1);
    dim_out.Plus(dims2);
    mind->MakeCountOnly(result_size, dim_out);
  } else
    new_mind.Commit(result_size);
}

void JoinerSort::AddTuples(MINewContents &new_mind, JoinerSortWrapper &sort_encoder, unsigned char *matched_row,
                           int64_t &result_size, DimensionVector &dims_used_in_cond) {
  MEASURE_FET("JoinerSort::AddTuples(...)");
  if (tips.count_only && !other_cond_exist && !watch_matched && !watch_traversed) {
    result_size += sort_encoder.CacheUsed();
    return;
  }
  MIDummyIterator mit(mind);
  for (int cache_pos = 0; cache_pos < sort_encoder.CacheUsed(); cache_pos++) {
    if (m_conn->Killed())
      throw common::KilledException();
    bool add_now = true;
    // check other conditions
    if (other_cond_exist) {
      for (int i = 0; i < mind->NumOfDimensions(); i++)
        if (dims_used_in_cond[i])
          mit.Set(i, sort_encoder.DimValue(i, cache_pos, matched_row));
      for (auto &j : other_cond) {
        j.LockSourcePacks(mit);
        if (types::RequiresUTFConversions(j.GetCollation())) {
          if (j.CheckCondition_UTF(mit) == false) {
            add_now = false;
            break;
          }

        } else {
          if (j.CheckCondition(mit) == false) {
            add_now = false;
            break;
          }
        }
      }
    }
    // add, if conditions met
    if (add_now) {
      if (!outer_nulls_only) {
        if (!tips.count_only) {
          for (int d = 0; d < mind->NumOfDimensions(); d++)
            if (sort_encoder.DimEncoded(d))
              new_mind.SetNewTableValue(d, sort_encoder.DimValue(d, cache_pos, matched_row));
          new_mind.CommitNewTableValues();
        }
        result_size++;
      }
      if (watch_matched || watch_traversed)
        outer_filter->Reset(sort_encoder.GetOuterIndex(cache_pos, matched_row));
      if (tips.limit > -1 && result_size >= tips.limit)
        return;
    }
  }
}

int64_t JoinerSort::AddOuterTuples(MINewContents &new_mind, JoinerSortWrapper &sort_encoder,
                                   DimensionVector &iterate_watched) {
  MEASURE_FET("JoinerSort::AddOuterTuples(...)");
  if (!tips.count_only) {
    MIIterator mit(mind, iterate_watched);
    int64_t outer_row = 0;
    while (mit.IsValid()) {
      if (outer_filter->Get(outer_row)) {
        for (int d = 0; d < mind->NumOfDimensions(); d++)
          if (sort_encoder.DimEncoded(d)) {
            if (iterate_watched[d])
              new_mind.SetNewTableValue(d, mit[d]);
            else
              new_mind.SetNewTableValue(d, common::NULL_VALUE_64);
          }
        new_mind.CommitNewTableValues();
      }
      ++mit;
      outer_row++;
    }
  }
  return outer_filter->NumOfOnes();
}

JoinerSortWrapper::JoinerSortWrapper(bool _less) {
  less = _less;
  encoder = nullptr;
  cache = nullptr;
  key_bytes = 0;       // size of key data for both sorters
  traverse_bytes = 0;  // total size of the first ("traverse") sorter
  match_bytes = 0;     // total size of the second ("match") sorter
  cache_size = 0;
  cache_bytes = 0;
  cur_cache_used = 0;
  buf = nullptr;
  buf_bytes = 0;
  no_dims = 0;
  outer_offset = 0;
  min_traversed = nullptr;
  watch_traversed = false;
  watch_matched = false;
}

JoinerSortWrapper::~JoinerSortWrapper() {
  delete encoder;
  if (cache)
    dealloc(cache);
  delete[] buf;
  delete[] min_traversed;
}

bool JoinerSortWrapper::SetKeyColumns(vcolumn::VirtualColumn *v1,
                                      vcolumn::VirtualColumn *v2)  // return false if not compatible
{
  int encoder_flags = (ColumnBinEncoder::ENCODER_IGNORE_NULLS | ColumnBinEncoder::ENCODER_MONOTONIC);
  if (!less)
    encoder_flags = (encoder_flags | ColumnBinEncoder::ENCODER_DESCENDING);
  encoder = new ColumnBinEncoder(encoder_flags);
  bool compatible = encoder->PrepareEncoder(v1, v2);
  return compatible;
}

void JoinerSortWrapper::SetDimensions(MultiIndex *mind, DimensionVector &dim_tr, DimensionVector &dim_match,
                                      [[maybe_unused]] DimensionVector &dim_other, [[maybe_unused]] bool count_only) {
  // TODO: take optimizations into account (dim_other, count_only, forget_now)
  key_bytes = encoder->GetPrimarySize();
  encoder->SetPrimaryOffset(0);  // key values always on the beginning of buffer
  traverse_bytes = key_bytes;
  match_bytes = key_bytes;
  no_dims = mind->NumOfDimensions();
  int64_t tuple_size;
  for (int i = 0; i < no_dims; i++) {
    if (dim_tr[i]) {
      tuple_size = mind->OrigSize(i) + 1;  // +1 because 0 is reserved for nulls
      dim_traversed.push_back(true);
      dim_size.push_back(CalculateByteSize(tuple_size));
      dim_offset.push_back(traverse_bytes - key_bytes);
      traverse_bytes += CalculateByteSize(tuple_size);
    } else if (dim_match[i]) {
      tuple_size = mind->OrigSize(i) + 1;  // +1 because 0 is reserved for nulls
      dim_traversed.push_back(false);
      dim_size.push_back(CalculateByteSize(tuple_size));
      dim_offset.push_back(match_bytes - key_bytes);
      match_bytes += CalculateByteSize(tuple_size);
    } else {
      dim_traversed.push_back(false);
      dim_size.push_back(0);
      dim_offset.push_back(0);
    }
  }
  if (watch_matched) {
    outer_offset = match_bytes - key_bytes;
    match_bytes += 8;
  }
  if (watch_traversed) {
    outer_offset = traverse_bytes - key_bytes;
    traverse_bytes += 8;
  }
  DEBUG_ASSERT(buf == nullptr);
  buf_bytes = std::max(traverse_bytes, match_bytes);
  buf = new unsigned char[buf_bytes];
}

unsigned char *JoinerSortWrapper::EncodeForSorter1([[maybe_unused]] vcolumn::VirtualColumn *v, MIIterator &mit,
                                                   int64_t outer_pos)  // for Traversed
{
  std::memset(buf, 0, buf_bytes);
  encoder->Encode(buf, mit, nullptr, true);
  int64_t dim_value = 0;
  for (int i = 0; i < no_dims; i++) {
    if (dim_traversed[i] == true) {
      dim_value = mit[i];
      if (dim_value == common::NULL_VALUE_64)
        dim_value = 0;
      else
        dim_value += 1;
      std::memcpy(buf + key_bytes + dim_offset[i], &dim_value, dim_size[i]);
    }
  }
  if (watch_traversed)
    std::memcpy(buf + key_bytes + outer_offset, &outer_pos, 8);
  if (min_traversed == nullptr) {
    min_traversed = new unsigned char[key_bytes];
    std::memcpy(min_traversed, buf, key_bytes);
  } else {
    if (std::memcmp(min_traversed, buf, key_bytes) > 0)
      std::memcpy(min_traversed, buf, key_bytes);
  }
  return buf;
}

unsigned char *JoinerSortWrapper::EncodeForSorter2(vcolumn::VirtualColumn *v, MIIterator &mit,
                                                   int64_t outer_pos)  // for Matched
{
  std::memset(buf, 0, buf_bytes);
  encoder->Encode(buf, mit, v, false);
  int64_t dim_value = 0;
  for (int i = 0; i < no_dims; i++)
    if (dim_traversed[i] == false && dim_size[i] > 0) {
      dim_value = mit[i];
      if (dim_value == common::NULL_VALUE_64)
        dim_value = 0;
      else
        dim_value += 1;
      std::memcpy(buf + key_bytes + dim_offset[i], &dim_value, dim_size[i]);
    }
  if (watch_matched)
    std::memcpy(buf + key_bytes + outer_offset, &outer_pos, 8);
  return buf;
}

bool JoinerSortWrapper::PackPossible(vcolumn::VirtualColumn *v, MIIterator &mit) {
  // Note: we are always checking "traversed < matched" condition. If ">" is
  // needed, we are encoding values as for decreasing order. Assuming v is
  // "matched" (the second) virtual column
  if (!watch_matched && v->GetNumOfNulls(mit) == mit.GetPackSizeLeft())
    return false;
  if (min_traversed == nullptr || v->Type().Lookup())
    return true;
  else if (v->Type().IsString()) {
    types::BString local_stat;
    if (less)
      local_stat = v->GetMaxString(mit);
    else
      local_stat = v->GetMinString(mit);
    if (!local_stat.IsNull()) {
      std::memset(buf, 0, buf_bytes);
      encoder->PutValueString(buf, local_stat, true,
                              false);                      // true: the second encoded column, false: don't update stats
      if (std::memcmp(min_traversed, buf, key_bytes) > 0)  // min(a) > max(b)  =>  a < b is always false
        return false;
    }
  } else {
    int64_t local_stat = common::NULL_VALUE_64;
    if (less)
      local_stat = v->GetMaxInt64(mit);
    else
      local_stat = v->GetMinInt64(mit);
    if (local_stat != common::NULL_VALUE_64 && local_stat != common::PLUS_INF_64 &&
        local_stat != common::MINUS_INF_64) {
      std::memset(buf, 0, buf_bytes);
      bool encoded = encoder->PutValue64(buf, local_stat, true,
                                         false);  // true: the second encoded column, false: don't update stats
      if (!encoded)
        return true;                                       // problem with encoding - play safe
      if (std::memcmp(min_traversed, buf, key_bytes) > 0)  // min(a) > max(b)  =>  a < b is always false
        return false;
    }
  }
  return true;
}

void JoinerSortWrapper::InitCache(int64_t no_of_rows) {
  cache_bytes = traverse_bytes - key_bytes;
  cache_size = no_of_rows;  // no limits - will be limited for non-virtual cache below
  if (cache_bytes > 0 && cache_size > 0) {
    if (cache_bytes * size_t(cache_size) > 64_MB) {
      int64_t max_mem_size = mm::TraceableObject::MaxBufferSize(-1);  // -1, because there are two buffers
      if (cache_bytes * cache_size > max_mem_size)
        cache_size = max_mem_size / cache_bytes;
    }
    cache = (unsigned char *)alloc(cache_bytes * cache_size, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
    if (cache == nullptr)
      throw common::OutOfMemoryException();
  }
}

bool JoinerSortWrapper::AddToCache(unsigned char *v)  // return false if there is no more space in
                                                      // cache (i.e. the next operation would fail)
{
  DEBUG_ASSERT(cur_cache_used < cache_size);
  if (cache_bytes > 0)
    std::memcpy(cache + cur_cache_used * cache_bytes, v, cache_bytes);
  cur_cache_used++;
  return (cur_cache_used < cache_size);
}

int64_t JoinerSortWrapper::DimValue(int d, int cache_position, unsigned char *matched_buffer) {
  int64_t dim_value = 0;
  DEBUG_ASSERT(dim_size[d] > 0);
  if (dim_traversed[d])
    std::memcpy(&dim_value, cache + cache_bytes * cache_position + dim_offset[d],
                dim_size[d]);  // get it from cache
  else
    std::memcpy(&dim_value, matched_buffer + dim_offset[d],
                dim_size[d]);  // get it from matched sorter
  if (dim_value == 0)
    return common::NULL_VALUE_64;
  return dim_value - 1;
}

int64_t JoinerSortWrapper::GetOuterIndex(int cache_position,
                                         unsigned char *matched_buffer)  // take it from traversed or matched,
                                                                         // depending on the context
{
  int64_t ind_value = 0;
  if (watch_traversed)
    std::memcpy(&ind_value, cache + cache_bytes * cache_position + outer_offset,
                8);  // get it from cache
  else if (watch_matched)
    std::memcpy(&ind_value, matched_buffer + outer_offset,
                8);  // get it from matched sorter
  return ind_value;
}
}  // namespace core
}  // namespace Tianmu
