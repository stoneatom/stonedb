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

#include "parameterized_filter.h"

#include "core/condition_encoder.h"
#include "core/engine.h"
#include "core/joiner.h"
#include "core/mi_iterator.h"
#include "core/mi_updating_iterator.h"
#include "core/pack_orderer.h"
#include "core/query.h"
#include "core/rough_multi_index.h"
#include "core/temp_table.h"
#include "core/transaction.h"
#include "core/value_set.h"
#include "util/thread_pool.h"
#include "vc/const_column.h"
#include "vc/const_expr_column.h"
#include "vc/expr_column.h"
#include "vc/in_set_column.h"
#include "vc/single_column.h"
#include "vc/subselect_column.h"
#include "vc/type_cast_column.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {

ParameterizedFilter::ParameterizedFilter(uint32_t power, CondType filter_type)
    : mind_shallow_memory_(false),
      mind_(new MultiIndex(power)),
      rough_mind_(nullptr),
      table_(nullptr),
      filter_type_(filter_type) {}

ParameterizedFilter &ParameterizedFilter::operator=(const ParameterizedFilter &pf) {
  if (this == &pf)
    return *this;

  if (mind_ && (!mind_shallow_memory_)) {
    delete mind_;
    mind_ = nullptr;
  }

  mind_ = (pf.mind_) ? new MultiIndex(*pf.mind_) : nullptr;

  AssignInternal(pf);
  mind_shallow_memory_ = false;

  return *this;
}

ParameterizedFilter &ParameterizedFilter::operator=(ParameterizedFilter &&pf) {
  if (this == &pf) {
    return *this;
  }

  if (mind_ && (!mind_shallow_memory_)) {
    delete mind_;
    mind_ = nullptr;
  }

  mind_ = pf.mind_;
  rough_mind_ = pf.rough_mind_;
  table_ = pf.table_;

  mind_shallow_memory_ = true;
  return *this;
}

ParameterizedFilter::ParameterizedFilter(const ParameterizedFilter &pf) {
  mind_ = (pf.mind_) ? new MultiIndex(*pf.mind_) : nullptr;

  rough_mind_ = nullptr;
  AssignInternal(pf);
  mind_shallow_memory_ = false;
}

ParameterizedFilter::~ParameterizedFilter() {
  if (mind_shallow_memory_) {
    return;
  }

  if (nullptr != mind_) {
    delete mind_;
    mind_ = nullptr;
  }

  if (nullptr != rough_mind_) {
    delete rough_mind_;
    rough_mind_ = nullptr;
  }
}

void ParameterizedFilter::AssignInternal(const ParameterizedFilter &pf) {
  if (rough_mind_) {
    delete rough_mind_;
    rough_mind_ = nullptr;
  }

  rough_mind_ = (pf.rough_mind_) ? new RoughMultiIndex(*pf.rough_mind_) : nullptr;

  for (uint i = 0; i < pf.descriptors_.Size(); i++) {
    if (!pf.descriptors_[i].done)
      descriptors_.AddDescriptor(pf.descriptors_[i]);
  }

  parametrized_desc_ = pf.parametrized_desc_;
  table_ = pf.table_;
  filter_type_ = pf.filter_type_;
}

void ParameterizedFilter::AddConditions(Condition *new_cond, CondType type) {
  Condition &cond = *new_cond;

  for (uint i = 0; i < cond.Size(); i++) {
    cond[i].SetCondType(type);
    (cond[i].IsParameterized()) ? parametrized_desc_.AddDescriptor(cond[i]) : descriptors_.AddDescriptor(cond[i]);
  }
}

void ParameterizedFilter::ProcessParameters() {
  MEASURE_FET("ParameterizedFilter::ProcessParameters(...)");

  descriptors_.Clear();
  for (uint i = 0; i < parametrized_desc_.Size(); i++) descriptors_.AddDescriptor(parametrized_desc_[i]);
  parametrized_desc_.Clear();
}

void ParameterizedFilter::PrepareRoughMultiIndex() {
  MEASURE_FET("ParameterizedFilter::PrepareRoughMultiIndex(...)");

  if (rough_mind_ || !mind_)
    return;

  std::vector<int> packs;
  for (uint i = 0; i < (uint)mind_->NumOfDimensions(); i++)
    packs.push_back((int)((mind_->OrigSize(i) + ((1 << mind_->ValueOfPower()) - 1)) >> mind_->ValueOfPower()));

  rough_mind_ = new RoughMultiIndex(packs);
  for (uint d = 0; d < (uint)mind_->NumOfDimensions(); d++) {
    Filter *f = mind_->GetFilter(d);

    for (int p = 0; p < rough_mind_->NumOfPacks(d); p++) {
      if (f == nullptr)
        rough_mind_->SetPackStatus(d, p, common::RoughSetValue::RS_UNKNOWN);
      else if (f->IsFull(p))
        rough_mind_->SetPackStatus(d, p, common::RoughSetValue::RS_ALL);
      else if (f->IsEmpty(p))
        rough_mind_->SetPackStatus(d, p, common::RoughSetValue::RS_NONE);
      else
        rough_mind_->SetPackStatus(d, p, common::RoughSetValue::RS_SOME);
    }
  }
}

double ParameterizedFilter::EvaluateConditionNonJoinWeight(Descriptor &desc, bool for_or) {
  // Interpretation of weight:
  // an approximation of logarithm of answer size
  // (in case of for_or: an approximation of (full_table - answer) size)
  // 0 -> time is very short (constant).
  // high weight -> schedule this query to be executed later
  double eval = 0.0;
  uint64_t no_distinct{0}, no_distinct2{0};
  uint64_t answer_size{0};

  if (desc.IsTrue() || desc.IsFalse())
    eval = 0;                                    // constant time
  else if (desc.IsType_AttrValOrAttrValVal()) {  // typical condition: attr=val
    if (!desc.encoded) {
      return log(1 + double(desc.attr.vc->NumOfTuples())) + 5;  // +5 as a penalty for complex expression
    }

    vcolumn::SingleColumn *col = static_cast<vcolumn::SingleColumn *>(desc.attr.vc);
    answer_size = col->ApproxAnswerSize(desc);
    if (for_or)
      answer_size = desc.attr.vc->NumOfTuples() - answer_size;

    int64_t no_in_values = 1;
    if (desc.op == common::Operator::O_IN || desc.op == common::Operator::O_NOT_IN) {
      vcolumn::MultiValColumn *iscol = static_cast<vcolumn::MultiValColumn *>(desc.val1.vc);
      MIIterator mitor(nullptr, desc.table->Getpackpower());
      no_in_values = iscol->NumOfValues(mitor);
    }

    eval = log(1 + double(answer_size));  // approximate size of the result
    if (no_in_values > 1)
      eval += log(double(no_in_values)) * 0.5;  // INs are potentially slower (many comparisons needed)

    if (col->Type().IsString() && !col->Type().Lookup())
      eval += 0.5;  // strings are slower

    if (col->Type().IsFloat())
      eval += 0.1;  // floats are slower

    if (desc.op == common::Operator::O_LIKE || desc.op == common::Operator::O_NOT_LIKE)
      eval += 0.2;  // these operators need more work

    // Processing descriptor on PK firstly
    if (desc.IsleftIndexSearch())
      eval = 0.001;

  } else if (desc.IsType_AttrAttr()) {  // attr=attr on the same table
    uint64_t no_obj =
        desc.attr.vc->NumOfTuples();  // changed to uint64_t to prevent negative alogarithm for common::NULL_VALUE_64
    if (!desc.encoded)
      return log(1 + double(2 * no_obj)) + 5;  // +5 as a penalty for complex expression
    else if (desc.op == common::Operator::O_EQ) {
      no_distinct = desc.attr.vc->GetApproxDistVals(false);
      if (no_distinct == 0)
        no_distinct = 1;

      no_distinct2 = desc.val1.vc->GetApproxDistVals(false);
      if (no_distinct2 == 0)
        no_distinct2 = 1;

      if (no_distinct2 > no_distinct)
        no_distinct = no_distinct2;  // find the attribute with smaller abstract classes

      if (for_or)
        eval = log(1 + (no_obj - double(no_obj) / no_distinct));
      else
        eval = log(1 + double(no_obj) / no_distinct);  // size of the smaller abstract class
    } else {
      eval = log(1 + double(no_obj) / 2);  // other operators filter potentially a half of objects
    }

    eval += 1;  // add to compensate opening two packs
  } else if (desc.IsType_OrTree() && !desc.IsType_Join()) {
    eval = desc.tree->root->EvaluateConditionWeight(this, for_or);
  } else {  // expressions and other types, incl. joins (to be calculated separately)
    if (desc.IsType_TIANMUExpression())
      return log(1 + double(desc.attr.vc->NumOfTuples())) + 2;  // +2 as a penalty for TIANMU complex expression

    eval = 99999;
  }

  return eval;
}

double ParameterizedFilter::EvaluateConditionJoinWeight(Descriptor &desc) {
  double eval = 0.0;
  int dim1 = desc.attr.vc->GetDim();
  int dim2 = desc.val1.vc->GetDim();
  /*
  if(dim1 == 0 && dim2 == 2) return 1.11;
  if(dim1 == 1 && dim2 == 6) return 1.12;
  if(dim1 == 1 && dim2 == 2) return 1.13;
  if(dim1 == 5 && dim2 == 7) return 1.14;
  if(dim1 == 4 && dim2 == 5) return 1.15;
  if(dim1 == 3 && dim2 == 4) return 1.16;
  */
  if (dim1 > -1 && dim2 > -1) {
    uint64_t no_obj1 =
        mind_->OrigSize(dim1);  // changed to uint64_t to prevent negative alogarithm for common::NULL_VALUE_64
    uint64_t no_obj2 = mind_->OrigSize(dim2);
    if (mind_->GetFilter(dim1))
      no_obj1 = mind_->DimSize(dim1);

    if (mind_->GetFilter(dim2))
      no_obj2 = mind_->DimSize(dim2);

    if (no_obj1 == 0 || no_obj2 == 0)
      return 1;

    uint64_t no_distinct1{0}, no_distinct2{0};
    double r_select1 = 0.5 + 0.5 * desc.attr.vc->RoughSelectivity();  // 1 if no rough exclusions,
                                                                      // close to 0.5 if good
                                                                      // rough performance
    double r_select2 = 0.5 + 0.5 * desc.val1.vc->RoughSelectivity();
    double c_select1 = double(no_obj1) / mind_->OrigSize(dim1);  // condition selectivity (assuming 1:n)
    double c_select2 = double(no_obj2) / mind_->OrigSize(dim2);
    double bigger_table_size = (double)std::max(no_obj1, no_obj2);

    if (desc.op == common::Operator::O_EQ) {
      no_distinct1 = desc.attr.vc->GetApproxDistVals(false);
      no_distinct2 = desc.val1.vc->GetApproxDistVals(false);

      if (no_distinct1 >= 0.99 * mind_->DimSize(dim1)) {  // potentially 1:n join
        eval = double(no_obj2) * r_select2 * c_select1 + double(no_obj1) / 100;
        eval = log(1 + eval);  // result will be of size of the second table_
                               // filtered by the value of the first one
        if (no_obj2 > no_obj1 &&
            mind_->DimSize(dim1) <= (1U << desc.table->Getpackpower()))  // bonus for potential small-large join //65536
          eval -= 1;

      } else if (no_distinct2 >= 0.99 * mind_->DimSize(dim2)) {  // potentially n:1 join
        eval = double(no_obj1) * r_select1 * c_select2 + double(no_obj2) / 100;
        eval = log(1 + eval);  // result will be of size of the first table_filtered by the value of the second one

        if (no_obj1 > no_obj2 &&
            mind_->DimSize(dim2) <= (1U << desc.table->Getpackpower()))  // bonus for potential small-large join //65536
          eval -= 1;

      } else {
        // approximated size: (no_of_classes) * (ave_class_size1 *
        // ave_class_size2) = no_of_classes * (no_obj1 / no_dist1) * (no_obj2 /
        // no_dist2)
        //                    where no_of_classes = min(no_dist1,no_dist2) ,
        //                    because they must have common values
        no_distinct1 = (uint64_t)(no_distinct1 * double(no_obj1) / mind_->DimSize(dim1));
        no_distinct2 = (uint64_t)(no_distinct2 * double(no_obj2) / mind_->DimSize(dim2));
        int64_t no_of_classes = std::min(no_distinct1, no_distinct2);

        eval = log(double(no_of_classes < 1 ? 1 : no_of_classes))  // instead of multiplications
               + (log(1 + double(no_obj1)) - log(double(no_distinct1))) +
               (log(1 + double(no_obj2)) - log(double(no_distinct2)));
      }
    } else
      eval = log(1 + double(no_obj1)) +
             log(1 + double(no_obj2));  // these join operators generate potentially large result

    eval += (eval - log(1 + bigger_table_size)) * 2;  // bonus for selective strength
    if (!mind_->IsUsedInOutput(dim1))                 // bonus for not used (cumulative, if both are not used)
      eval *= 0.75;

    if (!mind_->IsUsedInOutput(dim2))
      eval *= 0.75;

    eval += 20;
  } else  // other types of joins
    eval = 99999;

  return eval;
}

bool ParameterizedFilter::RoughUpdateMultiIndex() {
  MEASURE_FET("ParameterizedFilter::RoughUpdateMultiIndex(...)");

  bool is_nonempty = true;
  bool false_desc = false;

  for (uint i = 0; i < descriptors_.Size(); i++) {
    if (!descriptors_[i].done && descriptors_[i].IsInner() && (descriptors_[i].IsTrue()))
      descriptors_[i].done = true;

    if (descriptors_[i].IsFalse() && descriptors_[i].IsInner()) {
      rough_mind_->MakeDimensionEmpty();
      false_desc = true;
    }
  }

  // one-dimensional conditions
  if (!false_desc) {
    // init by previous values of mind_ (if any nontrivial)
    for (int i = 0; i < mind_->NumOfDimensions(); i++) {
      Filter *loc_f = mind_->GetFilter(i);
      rough_mind_->UpdateGlobalRoughFilter(i, loc_f);  // if the filter is nontrivial, then copy pack status
    }

    for (uint i = 0; i < descriptors_.Size(); i++) {
      if (descriptors_[i].done || descriptors_[i].IsDelayed() || !descriptors_[i].IsInner() ||
          descriptors_[i].GetJoinType() != DescriptorJoinType::DT_NON_JOIN)
        continue;

      DimensionVector dims(mind_->NumOfDimensions());
      descriptors_[i].DimensionUsed(dims);
      int dim = dims.GetOneDim();
      if (dim == -1)
        continue;
      common::RoughSetValue *rf = rough_mind_->GetLocalDescFilter(dim, i);  // rough filter for a descriptor
      descriptors_[i].ClearRoughValues();                                   // clear accumulated rough values
                                                                            // for descriptor
      MIIterator mit(mind_, dim, true);
      while (mit.IsValid()) {
        int p = mit.GetCurPackrow(dim);
        if (p >= 0 && rf[p] != common::RoughSetValue::RS_NONE)
          rf[p] = descriptors_[i].EvaluateRoughlyPack(mit);  // rough values are also accumulated inside

        mit.NextPackrow();

        if (mind_->m_conn->Killed())
          throw common::KilledException();
      }

      bool this_nonempty = rough_mind_->UpdateGlobalRoughFilter(dim, i);  // update the filter using local information
      is_nonempty = (is_nonempty && this_nonempty);
      descriptors_[i].UpdateVCStatistics();
      descriptors_[i].SimplifyAfterRoughAccumulate();  // simplify tree if there is a
                                                       // roughly trivial leaf
    }
  }

  // Recalculate all multidimensional dependencies only if there are 1-dim
  // descriptors_ which can benefit from the projection
  if (DimsWith1dimFilters())
    RoughMakeProjections();

  // Displaying statistics
  if (tianmu_control_.isOn() || mind_->m_conn->Explain()) {
    int pack_full = 0, pack_some = 0, pack_all = 0;
    tianmu_control_.lock(mind_->m_conn->GetThreadID()) << "Packs/packrows after KN evaluation:" << system::unlock;

    for (int dim = 0; dim < rough_mind_->NumOfDimensions(); dim++) {
      std::stringstream ss;
      pack_full = 0;
      pack_some = 0;
      pack_all = rough_mind_->NumOfPacks(dim);

      for (int b = 0; b < pack_all; b++) {
        if (rough_mind_->GetPackStatus(dim, b) == common::RoughSetValue::RS_ALL)
          pack_full++;
        else if (rough_mind_->GetPackStatus(dim, b) != common::RoughSetValue::RS_NONE)
          pack_some++;
      }

      ss << "(t" << dim << ") Pckrows: " << pack_all << ", susp. " << pack_some << " ("
         << pack_all - (pack_full + pack_some) << " empty " << pack_full
         << " full). Conditions: " << rough_mind_->NumOfConditions(dim);

      if (mind_->m_conn->Explain()) {
        mind_->m_conn->SetExplainMsg(ss.str());
      } else {
        tianmu_control_.lock(mind_->m_conn->GetThreadID()) << ss.str() << system::unlock;
      }
    }
  }

  return is_nonempty;
}

bool ParameterizedFilter::PropagateRoughToMind() {
  MEASURE_FET("ParameterizedFilter::PropagateRoughToMind(...)");

  bool is_nonempty = true;
  for (int i = 0; i < rough_mind_->NumOfDimensions(); i++) {  // update classical multiindex
    Filter *f = mind_->GetUpdatableFilter(i);
    if (!f)
      continue;

    for (int b = 0; b < rough_mind_->NumOfPacks(i); b++) {
      if (rough_mind_->GetPackStatus(i, b) == common::RoughSetValue::RS_NONE)
        f->ResetBlock(b);
    }

    if (f->IsEmpty())
      is_nonempty = false;
  }

  return is_nonempty;
}

void ParameterizedFilter::RoughUpdateJoins() {
  MEASURE_FET("ParameterizedFilter::RoughUpdateJoins(...)");

  bool join_or_delayed_present = false;
  std::set<int> dims_to_be_suspect;

  for (uint i = 0; i < descriptors_.Size(); i++) {
    if (!descriptors_[i].done && descriptors_[i].IsDelayed()) {
      join_or_delayed_present = true;
      break;
    }

    if (!descriptors_[i].done && descriptors_[i].IsOuter()) {
      for (int j = 0; j < descriptors_[i].right_dims.Size(); j++)
        if (descriptors_[i].right_dims[j])
          dims_to_be_suspect.insert(j);
    }

    if (!descriptors_[i].done && descriptors_[i].IsType_Join() && descriptors_[i].IsInner()) {
      descriptors_[i].DimensionUsed(descriptors_[i].left_dims);
      for (int j = 0; j < descriptors_[i].left_dims.Size(); j++)
        if (descriptors_[i].left_dims[j])
          dims_to_be_suspect.insert(j);
    }
  }

  if (join_or_delayed_present)
    rough_mind_->MakeDimensionSuspect();  // no common::RoughSetValue::RS_ALL packs
  else if (dims_to_be_suspect.size()) {
    for (auto &it : dims_to_be_suspect)
      rough_mind_->MakeDimensionSuspect(it);  // no common::RoughSetValue::RS_ALL packs
  }
}

void ParameterizedFilter::RoughMakeProjections() {
  for (int dim = 0; dim < mind_->NumOfDimensions(); dim++) RoughMakeProjections(dim, false);

  rough_mind_->UpdateReducedDimension();
}

void ParameterizedFilter::RoughMakeProjections(int to_dim, bool update_reduced) {
  MEASURE_FET("ParameterizedFilter::RoughMakeProjections(...)");

  int total_excluded = 0;
  std::vector<int> dims_reduced = rough_mind_->GetReducedDimensions();

  for (auto const &dim1 : dims_reduced) {
    if (dim1 == to_dim)
      continue;

    // find all descriptors_ which may potentially influence other dimensions
    std::vector<Descriptor> local_desc;
    for (uint i = 0; i < descriptors_.Size(); i++) {
      if (descriptors_[i].done || descriptors_[i].IsDelayed())
        continue;

      if (descriptors_[i].IsOuter())
        return;  // do not make any projections in case of outer joins present

      if (!descriptors_[i].attr.vc || descriptors_[i].attr.vc->GetDim() == -1 || !descriptors_[i].val1.vc ||
          descriptors_[i].val1.vc->GetDim() == -1)
        continue;  // only SingleColumns processed here

      DimensionVector dims(mind_->NumOfDimensions());
      descriptors_[i].DimensionUsed(dims);

      if (descriptors_[i].attr.vc && dims[dim1] && dims[to_dim] && dims.NoDimsUsed() == 2)
        local_desc.push_back(descriptors_[i]);
    }

    // make projection to another dimension
    for (uint i = 0; i < local_desc.size(); i++) {
      // find the other dimension
      Descriptor &ld = local_desc[i];
      DimensionVector dims(mind_->NumOfDimensions());
      ld.DimensionUsed(dims);

      PackOrderer po;
      vcolumn::VirtualColumn *matched_vc;
      MIDummyIterator local_mit(mind_);
      if (dim1 == ld.attr.vc->GetDim()) {
        po.Init(ld.attr.vc, PackOrderer::OrderType::kRangeSimilarity,
                rough_mind_->GetRSValueTable(ld.attr.vc->GetDim()));
        matched_vc = ld.val1.vc;
      } else {
        po.Init(ld.val1.vc, PackOrderer::OrderType::kRangeSimilarity,
                rough_mind_->GetRSValueTable(ld.val1.vc->GetDim()));
        matched_vc = ld.attr.vc;
      }

      // for each dim2 pack, check whether it may be joined with anything nonempty on dim1
      for (int p2 = 0; p2 < rough_mind_->NumOfPacks(to_dim); p2++) {
        if (rough_mind_->GetPackStatus(to_dim, p2) == common::RoughSetValue::RS_NONE)
          continue;

        bool pack_possible = false;
        local_mit.SetPack(to_dim, p2);

        po.RewindToMatch(matched_vc, local_mit);

        while (po.IsValid()) {
          if (rough_mind_->GetPackStatus(dim1, po.Current()) == common::RoughSetValue::RS_NONE) {
            ++po;
            continue;
          }

          local_mit.SetPack(dim1, po.Current());  // set a dummy position, just for transferring pack number

          if (ld.attr.vc->RoughCheck(local_mit, ld) != common::RoughSetValue::RS_NONE) {
            pack_possible = true;
            break;
          }

          ++po;
        }

        if (!pack_possible) {
          rough_mind_->SetPackStatus(to_dim, p2, common::RoughSetValue::RS_NONE);
          total_excluded++;
        }
      }
    }

    if (update_reduced)
      rough_mind_->UpdateReducedDimension(dim1);

    local_desc.clear();
  }

  if (total_excluded > 0) {
    tianmu_control_.lock(mind_->m_conn->GetThreadID())
        << "Packrows excluded by rough multidimensional projections: " << total_excluded << system::unlock;
    rough_mind_->UpdateLocalRoughFilters(to_dim);

    for (auto const &dim1 : dims_reduced) {
      Filter *f = mind_->GetUpdatableFilter(dim1);
      if (!f)
        continue;

      for (int b = 0; b < rough_mind_->NumOfPacks(dim1); b++) {
        if (rough_mind_->GetPackStatus(dim1, b) == common::RoughSetValue::RS_NONE)
          f->ResetBlock(b);
      }
    }

    mind_->UpdateNumOfTuples();
  }
}

bool ParameterizedFilter::DimsWith1dimFilters() {
  DimensionVector dv1(mind_->NumOfDimensions()), dv2(mind_->NumOfDimensions());
  bool first = true;

  for (uint i = 0; i < descriptors_.Size(); i++) {
    if (descriptors_[i].done || !descriptors_[i].IsInner() || descriptors_[i].IsType_Join() ||
        descriptors_[i].IsDelayed())
      continue;

    if (first) {
      descriptors_[i].DimensionUsed(dv1);
      first = false;
    } else {
      descriptors_[i].DimensionUsed(dv2);
      if (!dv1.Intersects(dv2))
        return true;
    }
  }

  return false;
}

void ParameterizedFilter::PrepareJoiningStep(Condition &join_desc, Condition &desc, int desc_no, MultiIndex &mind_) {
  // join parameters based on the first joining condition
  DimensionVector dims1(mind_.NumOfDimensions());
  desc[desc_no].DimensionUsed(dims1);
  mind_.MarkInvolvedDimGroups(dims1);
  DimensionVector cur_outer_dim(desc[desc_no].right_dims);

  bool outer_present = !cur_outer_dim.IsEmpty();

  // add join (two-table_) conditions first
  for (uint i = desc_no; i < desc.Size(); i++) {
    if (desc[i].done || desc[i].IsDelayed() || !desc[i].IsType_JoinSimple())
      continue;

    DimensionVector dims2(mind_.NumOfDimensions());
    desc[i].DimensionUsed(dims2);
    if (desc[i].right_dims == cur_outer_dim && (outer_present || dims1.Includes(dims2))) {
      // can be executed together if all dimensions of the other condition are
      // present in the base one or in case of outer join
      join_desc.AddDescriptor(desc[i]);
      desc[i].done = true;  // setting in advance, as we already copied the
                            // descriptor to be processed
    }
  }

  // add the rest of conditions (e.g. one-dimensional outer conditions), which
  // are not "done" yet
  for (uint i = desc_no; i < desc.Size(); i++) {
    if (desc[i].done || desc[i].IsDelayed())
      continue;

    DimensionVector dims2(mind_.NumOfDimensions());
    desc[i].DimensionUsed(dims2);
    if (desc[i].right_dims == cur_outer_dim && (outer_present || dims1.Includes(dims2))) {
      // can be executed together if all dimensions of the other condition are
      // present in the base one or in case of outer join
      join_desc.AddDescriptor(desc[i]);
      desc[i].done = true;  // setting in advance, as we already copied the descriptor to be processed
    }
  }
}

void ParameterizedFilter::DescriptorJoinOrdering() {
  // calculate join weights
  for (uint i = 0; i < descriptors_.Size(); i++) {
    if (!descriptors_[i].done && descriptors_[i].IsType_JoinSimple())
      descriptors_[i].evaluation = EvaluateConditionJoinWeight(descriptors_[i]);
  }

  // descriptor ordering by evaluation weight - again
  for (uint k = 0; k < descriptors_.Size(); k++) {
    for (uint i = 0; i < descriptors_.Size() - 1; i++) {
      uint j = i + 1;  // bubble sort

      if (descriptors_[i].evaluation > descriptors_[j].evaluation &&
          descriptors_[j].right_dims ==
              descriptors_[i].right_dims) {  // do not change outer join order, if incompatible
        descriptors_[i].swap(descriptors_[j]);
      }
    }
  }
}

void ParameterizedFilter::UpdateJoinCondition(Condition &cond, JoinTips &tips)

{
  // Calculate joins (i.e. any condition using attributes from two dimensions)
  // as well as other conditions (incl. one-dim) flagged as "outer join"

  thd_proc_info(mind_->ConnInfo().Thd(), "join");
  DimensionVector all_involved_dims(mind_->NumOfDimensions());
  RoughSimplifyCondition(cond);

  for (uint i = 0; i < cond.Size(); i++) cond[i].DimensionUsed(all_involved_dims);

  bool is_outer = cond[0].IsOuter();

  // Apply conditions
  int conditions_used = cond.Size();
  JoinAlgType join_alg = JoinAlgType::JTYPE_NONE;
  TwoDimensionalJoiner::JoinFailure join_result = TwoDimensionalJoiner::JoinFailure::NOT_FAILED;
  join_alg = TwoDimensionalJoiner::ChooseJoinAlgorithm(*mind_, cond);

  // Joining itself
  do {
    auto joiner = TwoDimensionalJoiner::CreateJoiner(join_alg, *mind_, tips, table_);
    if (join_result == TwoDimensionalJoiner::JoinFailure::FAIL_WRONG_SIDES)  // the previous result, if any
      joiner->ForceSwitchingSides();

    joiner->ExecuteJoinConditions(cond);
    join_result = joiner->WhyFailed();

    if (join_result != TwoDimensionalJoiner::JoinFailure::NOT_FAILED)
      join_alg = TwoDimensionalJoiner::ChooseJoinAlgorithm(join_result, join_alg, cond.Size());

  } while (join_result != TwoDimensionalJoiner::JoinFailure::NOT_FAILED);

  for (int i = 0; i < conditions_used; i++) cond.EraseFirst();  // erase the first condition (already used)

  mind_->UpdateNumOfTuples();
  // display results (the last alg.)
  DisplayJoinResults(all_involved_dims, join_alg, is_outer, conditions_used);
}

void ParameterizedFilter::DisplayJoinResults(DimensionVector &all_involved_dims, JoinAlgType join_performed,
                                             bool is_outer, int conditions_used) {
  if (!tianmu_control_.isOn())
    return;

  int64_t tuples_after_join = mind_->NumOfTuples(all_involved_dims);

  char buf[30] = {0};
  if (conditions_used > 1)
    std::sprintf(buf, "%d cond. ", conditions_used);
  else
    std::strcpy(buf, "");

  if (is_outer)
    std::strcat(buf, "outer ");
  else
    std::strcat(buf, "inner ");

  char buf_dims[500] = {0};
  buf_dims[0] = '\0';
  bool first = true;

  for (int i = 0; i < mind_->NumOfDimensions(); i++) {
    if (!all_involved_dims[i])
      continue;

    if (first) {
      if (all_involved_dims.NoDimsUsed() == 1)
        std::sprintf(buf_dims, "...-%d", i);
      else
        std::sprintf(buf_dims, "%d", i);

      first = false;
    } else
      std::sprintf(buf_dims + std::strlen(buf_dims), "-%d", i);
  }

  tianmu_control_.lock(mind_->m_conn->GetThreadID())
      << "Tuples after " << buf << "join " << buf_dims
      << (join_performed == JoinAlgType::JTYPE_SORT
              ? " [sort]: "
              : (join_performed == JoinAlgType::JTYPE_MAP
                     ? " [map]:  "
                     : (join_performed == JoinAlgType::JTYPE_HASH
                            ? " [hash]: "
                            : (join_performed == JoinAlgType::JTYPE_GENERAL ? " [loop]: " : " [????]: "))))
      << tuples_after_join << " \t" << mind_->Display() << system::unlock;
}

void ParameterizedFilter::RoughSimplifyCondition(Condition &cond) {
  for (uint i = 0; i < cond.Size(); i++) {
    Descriptor &desc = cond[i];
    if (desc.op == common::Operator::O_FALSE || desc.op == common::Operator::O_TRUE || !desc.IsType_OrTree())
      continue;

    DimensionVector all_dims(mind_->NumOfDimensions());  // "false" for all dimensions
    desc.DimensionUsed(all_dims);
    desc.ClearRoughValues();

    MIIterator mit(mind_, all_dims);
    while (mit.IsValid()) {
      desc.RoughAccumulate(mit);
      mit.NextPackrow();
    }

    desc.SimplifyAfterRoughAccumulate();
  }
}

bool ParameterizedFilter::TryToMerge(Descriptor &d1, Descriptor &d2)  // true, if d2 is no longer needed
{
  // Assumption: d1 is earlier than d2 (important for outer joins)
  MEASURE_FET("ParameterizedFilter::TryToMerge(...)");

  if (d1.IsType_OrTree() || d2.IsType_OrTree())
    return false;

  if (d1 == d2)
    return true;

  if (d1.attr.vc == d2.attr.vc && d1.IsInner() && d2.IsInner()) {
    // IS_NULL and anything based on the same column => FALSE
    // NOT_NULL and anything based on the same column => NOT_NULL is not needed
    // Exceptions:
    //		null NOT IN {empty set}
    //		null < ALL {empty set} etc.
    if ((d1.op == common::Operator::O_IS_NULL && d2.op != common::Operator::O_IS_NULL && !IsSetOperator(d2.op)) ||
        (d2.op == common::Operator::O_IS_NULL && d1.op != common::Operator::O_IS_NULL && !IsSetOperator(d1.op))) {
      d1.op = common::Operator::O_FALSE;
      d1.CalculateJoinType();
      return true;
    }

    if (d1.op == common::Operator::O_NOT_NULL && !IsSetOperator(d2.op)) {
      d1 = d2;
      return true;
    }

    if (d2.op == common::Operator::O_NOT_NULL && !IsSetOperator(d1.op))
      return true;
  }

  // If a condition is repeated both on outer join list and after WHERE, then
  // the first one is not needed
  //    t1 LEFT JOIN t2 ON (a=b AND c=5) WHERE a=b   =>   t1 LEFT JOIN t2 ON c=5
  //    WHERE a=b
  if (d1.EqualExceptOuter(d2)) {
    if (d1.IsInner() && d2.IsOuter())  // delete d2
      return true;

    if (d1.IsOuter() && d2.IsInner()) {  // delete d1
      d1 = d2;
      return true;
    }
  }

  // Content-based merging
  if (d1.attr.vc == d2.attr.vc) {
    if (d1.attr.vc->TryToMerge(d1, d2))
      return true;
  }

  return false;
}

void ParameterizedFilter::SyntacticalDescriptorListPreprocessing(bool for_rough_query) {
  MEASURE_FET("ParameterizedFilter::SyntacticalDescriptorListPreprocessing(...)");

  // outer joins preprocessing (delaying conditions etc.)
  bool outer_join_found = false;
  uint no_desc = uint(descriptors_.Size());
  DimensionVector all_outer(mind_->NumOfDimensions());

  for (uint i = 0; i < no_desc; i++) {
    if (descriptors_[i].done || !descriptors_[i].IsOuter())
      continue;

    outer_join_found = true;
    all_outer.Plus(descriptors_[i].right_dims);
  }

  if (outer_join_found) {
    for (uint i = 0; i < no_desc; i++) {
      if (descriptors_[i].done || !descriptors_[i].IsInner())
        continue;

      DimensionVector inner(mind_->NumOfDimensions());
      descriptors_[i].DimensionUsed(inner);
      if (all_outer.Intersects(inner)) {
        if (descriptors_[i].NullMayBeTrue()) {
          descriptors_[i].delayed = true;  // delayed, i.e. must be calculated after nulls occur
        } else {
          // e.g. t1 LEFT JOIN t2 ON a1=a2 WHERE t2.c = 5   - in such cases
          // all nulls are excluded anyway,
          //                                                  so it is an
          //                                                  equivalent of
          //                                                  inner join
          all_outer.Minus(inner);  // set all involved dimensions as no longer outer
        }
      }
    }

    for (uint j = 0; j < no_desc; j++) {
      if (descriptors_[j].done || !descriptors_[j].IsOuter() || all_outer.Intersects(descriptors_[j].right_dims))
        continue; /*[descriptors_[i].outer_dim])*/

      descriptors_[j].right_dims.Clean();
      /*outer_dim = -1;*/  // change outer joins to inner (these identified above)
    }
  }

  bool false_desc = false;

  // descriptor preparation (normalization etc.)
  for (uint i = 0; i < no_desc; i++) {            // Note that desc.size() may enlarge
                                                  // when joining with BETWEEN occur
    DEBUG_ASSERT(descriptors_[i].done == false);  // If not false, check it carefully.
    if (descriptors_[i].IsTrue()) {
      if (descriptors_[i].IsInner())
        descriptors_[i].done = true;
      continue;
    } else if (descriptors_[i].IsFalse())
      continue;

    if (descriptors_[i].IsDelayed())
      continue;

    DEBUG_ASSERT(descriptors_[i].lop == common::LogicalOperator::O_AND);
    // if(descriptors_[i].lop != common::LogicalOperator::O_AND && descriptors_[i].IsType_Join() &&
    // (descriptors_[i].op == common::Operator::O_BETWEEN || descriptors_[i].op ==
    // common::Operator::O_NOT_BETWEEN))
    //	throw common::NotImplementedException("This kind of join condition with
    // OR is not
    // implemented.");

    // normalization of descriptor of type 1 between a and b
    if (descriptors_[i].op == common::Operator::O_BETWEEN) {
      if (descriptors_[i].GetJoinType() != DescriptorJoinType::DT_COMPLEX_JOIN && !descriptors_[i].IsType_Join() &&
          descriptors_[i].attr.vc && descriptors_[i].attr.vc->IsConst()) {
        std::swap(descriptors_[i].attr.vc, descriptors_[i].val1.vc);
        descriptors_[i].op = common::Operator::O_LESS_EQ;

        // now, the second part
        Descriptor dd(table_, mind_->NumOfDimensions());
        dd.attr = descriptors_[i].val2;
        descriptors_[i].val2.vc = nullptr;
        dd.op = common::Operator::O_MORE_EQ;
        dd.val1 = descriptors_[i].val1;
        dd.done = false;
        dd.left_dims = descriptors_[i].left_dims;
        dd.right_dims = descriptors_[i].right_dims;
        dd.CalculateJoinType();

        descriptors_[i].CalculateJoinType();
        descriptors_.AddDescriptor(dd);

        no_desc++;
      } else if (descriptors_[i].IsType_JoinSimple()) {
        // normalization of descriptor of type a between 1 and b
        descriptors_[i].op = common::Operator::O_MORE_EQ;
        Descriptor dd(table_, mind_->NumOfDimensions());
        dd.attr = descriptors_[i].attr;
        dd.val1 = descriptors_[i].val2;
        descriptors_[i].val2.vc = nullptr;
        dd.op = common::Operator::O_LESS_EQ;
        dd.done = false;
        dd.left_dims = descriptors_[i].left_dims;
        dd.right_dims = descriptors_[i].right_dims;
        dd.CalculateJoinType();

        descriptors_[i].CalculateJoinType();
        descriptors_.AddDescriptor(dd);

        no_desc++;
      }
    }

    descriptors_[i].CoerceColumnTypes();
    descriptors_[i].Simplify();
    if (descriptors_[i].IsFalse()) {
      false_desc = true;
    }
  }

  // join descriptor merging (before normalization, because we may then add a condition for another column)
  std::vector<Descriptor> added_cond;
  for (uint i = 0; i < no_desc; i++) {  // t1.x == t2.y && t2.y == 5   =>   t1.x == 5
    if (descriptors_[i].done || descriptors_[i].op != common::Operator::O_EQ || !descriptors_[i].IsType_JoinSimple() ||
        !descriptors_[i].IsInner())
      continue;

    // desc[i] is a joining (eq.) condition
    for (uint j = 0; j < descriptors_.Size(); j++) {
      if (i == j || descriptors_[j].done || !descriptors_[j].IsType_AttrValOrAttrValVal() || !descriptors_[j].IsInner())
        continue;

      // desc[j] is a second condition, non-join
      if (descriptors_[j].attr.vc == descriptors_[i].attr.vc) {
        // the same table_ and column
        if (descriptors_[j].op ==
            common::Operator::O_EQ) {  // t2.y == t1.x && t2.y == 5  change to  t1.x == 5 && t2.y == 5
          descriptors_[i].attr = descriptors_[i].val1;
          descriptors_[i].val1 = descriptors_[j].val1;
          descriptors_[i].CalculateJoinType();
          descriptors_[i].CoerceColumnTypes();
          break;
        } else {  // t2.y == t1.x && t2.y > 5  change to  t2.y == t1.x && t2.y > 5 && t1.x > 5
          Descriptor dd(table_, mind_->NumOfDimensions());
          dd.attr = descriptors_[i].val1;
          dd.op = descriptors_[j].op;
          dd.val1 = descriptors_[j].val1;
          dd.val2 = descriptors_[j].val2;
          dd.CalculateJoinType();
          dd.CoerceColumnTypes();
          added_cond.push_back(dd);
        }
      }

      if (descriptors_[j].attr.vc == descriptors_[i].val1.vc) {  // the same as above for val1
        // the same table_ and column
        if (descriptors_[j].op ==
            common::Operator::O_EQ) {  // t1.x == t2.y && t2.y == 5  change to  t1.x == 5 && t2.y == 5
          descriptors_[i].val1 = descriptors_[j].val1;
          descriptors_[i].CalculateJoinType();
          descriptors_[i].CoerceColumnTypes();
          break;
        } else {  // t1.x == t2.y && t2.y > 5  change to  t1.x == t2.y && t2.y > 5 && t1.x > 5
          Descriptor dd(table_, mind_->NumOfDimensions());
          dd.attr = descriptors_[i].attr;
          dd.op = descriptors_[j].op;
          dd.val1 = descriptors_[j].val1;
          dd.val2 = descriptors_[j].val2;
          dd.CalculateJoinType();
          dd.CoerceColumnTypes();
          added_cond.push_back(dd);
        }
      }
    }
  }

  if (!added_cond.empty() && tianmu_control_.isOn())
    tianmu_control_.lock(mind_->m_conn->GetThreadID())
        << "Adding " << int(added_cond.size()) << " conditions..." << system::unlock;

  for (uint i = 0; i < added_cond.size(); i++) {
    descriptors_.AddDescriptor(added_cond[i]);
    no_desc++;
  }

  // attribute-based transformation (normalization) of descriptors_
  // "attr-operator-value" and other, if possible
  if (!false_desc) {
    for (uint i = 0; i < no_desc; i++) {
      DimensionVector all_dims(mind_->NumOfDimensions());
      descriptors_[i].DimensionUsed(all_dims);
      bool additional_nulls_possible = false;

      for (int d = 0; d < mind_->NumOfDimensions(); d++) {
        if (all_dims[d] && mind_->GetFilter(d) == nullptr)
          additional_nulls_possible = true;
      }

      if (descriptors_[i].IsOuter())
        additional_nulls_possible = true;

      ConditionEncoder::EncodeIfPossible(descriptors_[i], for_rough_query, additional_nulls_possible);

      if (descriptors_[i].IsTrue()) {  // again, because something might be simplified
        if (descriptors_[i].IsInner())
          descriptors_[i].done = true;

        continue;
      }
    }

    // descriptor merging
    for (uint i = 0; i < no_desc; i++) {
      if (descriptors_[i].done || descriptors_[i].IsDelayed())
        continue;

      for (uint jj = i + 1; jj < no_desc; jj++) {
        if (descriptors_[jj].right_dims != descriptors_[i].right_dims || descriptors_[jj].done ||
            descriptors_[jj].IsDelayed())
          continue;

        if (TryToMerge(descriptors_[i], descriptors_[jj])) {
          tianmu_control_.lock(mind_->m_conn->GetThreadID()) << "Merging conditions..." << system::unlock;
          descriptors_[jj].done = true;
        }
      }
    }
  }
}

void ParameterizedFilter::DescriptorListOrdering() {
  MEASURE_FET("ParameterizedFilter::DescriptorListOrdering(...)");
  // descriptor evaluating
  // evaluation weight: higher value means potentially larger result (i.e. to be
  // calculated as late as possible)
  if (descriptors_.Size() > 1 || descriptors_[0].IsType_OrTree()) {  // else all evaluations remain 0 (default)
    for (uint i = 0; i < descriptors_.Size(); i++) {
      // to the end of queue or joins are evaluated separately.
      descriptors_[i].evaluation = (descriptors_[i].done || descriptors_[i].IsDelayed())
                                       ? 100000
                                       : EvaluateConditionNonJoinWeight(descriptors_[i]);
    }
  }

  // descriptor ordering by evaluation weight
  for (uint k = 0; k < descriptors_.Size(); k++) {
    for (uint i = 0; i < descriptors_.Size() - 1; i++) {
      int j = i + 1;  // bubble sort

      if (descriptors_[i].evaluation > descriptors_[j].evaluation && descriptors_[j].IsInner() &&
          descriptors_[i].IsInner()) {  // do not change outer join order
        descriptors_[i].swap(descriptors_[j]);
      }
    }
  }
}

void ParameterizedFilter::UpdateMultiIndex(bool count_only, int64_t limit) {
  MEASURE_FET("ParameterizedFilter::UpdateMultiIndex(...)");

  thd_proc_info(mind_->ConnInfo().Thd(), "update multi-index");

  if (descriptors_.Size() < 1) {
    PrepareRoughMultiIndex();
    FilterDeletedForSelectAll();
    rough_mind_->ClearLocalDescFilters();

    return;
  } else { /*Judge whether there is filtering logic of the current table_. If not, filter the data of the current
              table_*/
    auto &rcTables = table_->GetTables();
    /*
      no_dims is a destination number of dimensions.
      It needs to be increased according to the number of executions.
    */

    int no_dims = 0;
    for (uint tableIndex = 0; tableIndex < rcTables.size(); tableIndex++) {
      auto rcTable = rcTables[tableIndex];
      if (rcTable->TableType() == TType::TEMP_TABLE)
        continue;

      bool isVald = false;
      for (uint i = 0; i < descriptors_.Size(); i++) {
        Descriptor &desc = descriptors_[i];
        /*The number of values in the (var_map) corresponding to the entity column is always 1,
        so only the first element in the (var_map) is judged here.*/
        if ((desc.attr.vc && desc.attr.vc->GetVarMap().size() >= 1) &&
            // Use the tab object in VarMap to compare with the corresponding table_
            (desc.attr.vc->GetVarMap()[0].GetTabPtr().get() == rcTable) &&
            (desc.GetJoinType() == DescriptorJoinType::DT_NON_JOIN)) {
          isVald = true;
          break;
        }
      }

      if (!isVald) {
        FilterDeletedByTable(rcTable, no_dims, tableIndex);
        no_dims++;
      }
    }
  }

  SyntacticalDescriptorListPreprocessing();

  bool empty_cannot_grow = true;  // if false (e.g. outer joins), then do not optimize empty multiindex as empty result

  for (uint i = 0; i < descriptors_.Size(); i++) {
    if (descriptors_[i].IsOuter())
      empty_cannot_grow = false;
  }

  // special cases
  bool nonempty = true;

  DescriptorListOrdering();

  // descriptor display
  if (tianmu_control_.isOn()) {
    tianmu_control_.lock(mind_->m_conn->GetThreadID()) << "Initial execution plan (non-join):" << system::unlock;
    for (uint i = 0; i < descriptors_.Size(); i++)
      if (!descriptors_[i].done && !descriptors_[i].IsType_Join() && descriptors_[i].IsInner()) {
        char buf[1000] = {0};
        std::strcpy(buf, " ");
        descriptors_[i].ToString(buf, 1000);

        if (descriptors_[i].IsDelayed())
          tianmu_control_.lock(mind_->m_conn->GetThreadID())
              << "Delayed: " << buf << " \t(" << int(descriptors_[i].evaluation * 100) / 100.0 << ")" << system::unlock;
        else
          tianmu_control_.lock(mind_->m_conn->GetThreadID())
              << "Cnd(" << i << "):  " << buf << " \t(" << int(descriptors_[i].evaluation * 100) / 100.0 << ")"
              << system::unlock;
      }
  }

  // end now if the multiindex is empty
  if (mind_->ZeroTuples() && empty_cannot_grow) {
    mind_->Empty();
    PrepareRoughMultiIndex();
    rough_mind_->ClearLocalDescFilters();
    return;
  }

  // Prepare execution - rough set part
  for (uint i = 0; i < descriptors_.Size(); i++) {
    if (descriptors_[i].done || !descriptors_[i].IsInner())
      continue;

    if (descriptors_[i].IsTrue()) {
      descriptors_[i].done = true;
      continue;
    } else if (descriptors_[i].IsFalse()) {
      if (!descriptors_[i].attr.vc)
        continue;

      descriptors_[i].done = true;
      if (empty_cannot_grow) {
        mind_->Empty();
        PrepareRoughMultiIndex();
        rough_mind_->ClearLocalDescFilters();

        return;
      } else {
        DimensionVector dims(mind_->NumOfDimensions());
        descriptors_[i].attr.vc->MarkUsedDims(dims);
        mind_->MakeCountOnly(0, dims);
      }
    }
  }

  if (rough_mind_)
    rough_mind_->ClearLocalDescFilters();

  PrepareRoughMultiIndex();
  nonempty = RoughUpdateMultiIndex();  // calculate all rough conditions,

  if ((!nonempty && empty_cannot_grow) || mind_->m_conn->Explain()) {
    mind_->Empty();  // nonempty==false if the whole result is empty (outer joins considered)
    rough_mind_->ClearLocalDescFilters();
    return;
  }

  PropagateRoughToMind();  // exclude common::RoughSetValue::RS_NONE from mind_

  // count other types of conditions, e.g. joins (i.e. conditions using
  // attributes from two dimensions)
  int no_of_join_conditions = 0;  // count also one-dimensional outer join conditions
  int no_of_delayed_conditions = 0;
  for (uint i = 0; i < descriptors_.Size(); i++) {
    if (descriptors_[i].done)
      continue;

    if (descriptors_[i].IsType_Join() || descriptors_[i].IsDelayed() || descriptors_[i].IsOuter() ||
        descriptors_[i].IsType_In() || descriptors_[i].IsType_Exists()) {
      (!descriptors_[i].IsDelayed()) ? no_of_join_conditions++ : no_of_delayed_conditions++;
    }
  }

  // Apply all one-dimensional filters (after where, i.e. without
  // outer joins)
  int last_desc_dim = -1;
  int cur_dim = -1;

  int no_desc = 0;
  for (uint i = 0; i < descriptors_.Size(); i++) {
    if (!descriptors_[i].done && descriptors_[i].IsInner() && !descriptors_[i].IsType_Join() &&
        !descriptors_[i].IsDelayed() && !descriptors_[i].IsType_Exists() && !descriptors_[i].IsType_In())
      ++no_desc;
  }

  int desc_no = 0;
  for (uint i = 0; i < descriptors_.Size(); i++) {
    if (descriptors_[i].done || !descriptors_[i].IsInner() || descriptors_[i].IsType_Join() ||
        descriptors_[i].IsDelayed() || descriptors_[i].IsType_In() || descriptors_[i].IsType_Exists())
      continue;

    ++desc_no;

    if (descriptors_[i].attr.vc) {
      cur_dim = descriptors_[i].attr.vc->GetDim();
    }

    if (last_desc_dim != -1 && cur_dim != -1 && last_desc_dim != cur_dim) {
      // Make all possible projections to other dimensions
      RoughMakeProjections(cur_dim, false);
    }

    // limit should be applied only for the last descriptor
    ApplyDescriptor(i, (desc_no != no_desc || no_of_delayed_conditions > 0 || no_of_join_conditions) ? -1 : limit);
    if (!descriptors_[i].attr.vc)
      continue;  // probably desc got simplified and is true or false

    if (cur_dim >= 0 && mind_->GetFilter(cur_dim) && mind_->GetFilter(cur_dim)->IsEmpty() && empty_cannot_grow) {
      mind_->Empty();
      if (tianmu_control_.isOn()) {
        tianmu_control_.lock(mind_->m_conn->GetThreadID())
            << "Empty result set after non-join condition evaluation (WHERE)" << system::unlock;
      }

      rough_mind_->ClearLocalDescFilters();
      return;
    }

    last_desc_dim = cur_dim;
  }

  rough_mind_->UpdateReducedDimension();
  mind_->UpdateNumOfTuples();

  for (int i = 0; i < mind_->NumOfDimensions(); i++) {
    if (mind_->GetFilter(i))
      table_->SetVCDistinctVals(
          i, mind_->GetFilter(i)->NumOfOnes());  // distinct values - not more than the number of rows after WHERE
  }

  rough_mind_->ClearLocalDescFilters();

  // Some displays
  if (tianmu_control_.isOn()) {
    int pack_full = 0, pack_some = 0, pack_all = 0;
    tianmu_control_.lock(mind_->m_conn->GetThreadID())
        << "Packrows after exact evaluation (execute WHERE end):" << system::unlock;

    for (uint i = 0; i < (uint)mind_->NumOfDimensions(); i++) {
      if (!mind_->GetFilter(i))
        continue;

      Filter *f = mind_->GetFilter(i);
      pack_full = 0;
      pack_some = 0;
      pack_all = (int)((mind_->OrigSize(i) + ((1 << mind_->ValueOfPower()) - 1)) >> mind_->ValueOfPower());
      for (int b = 0; b < pack_all; b++) {
        if (f->IsFull(b))
          pack_full++;
        else if (!f->IsEmpty(b))
          pack_some++;
      }

      tianmu_control_.lock(mind_->m_conn->GetThreadID())
          << "(t" << i << "): " << pack_all << " all packrows, " << pack_full + pack_some << " to open (including "
          << pack_full << " full)" << system::unlock;
    }
  }

  DescriptorJoinOrdering();

  // descriptor display for joins
  if (tianmu_control_.isOn()) {
    bool first_time = true;

    for (uint i = 0; i < descriptors_.Size(); i++) {
      if (descriptors_[i].done || !(descriptors_[i].IsType_Join() || descriptors_[i].IsOuter()))
        continue;

      if (first_time) {
        tianmu_control_.lock(mind_->m_conn->GetThreadID()) << "Join execution plan:" << system::unlock;
        first_time = false;
      }

      char buf[1000] = {0};
      std::strcpy(buf, " ");
      descriptors_[i].ToString(buf, 1000);

      if (descriptors_[i].IsDelayed())
        tianmu_control_.lock(mind_->m_conn->GetThreadID())
            << "Delayed: " << buf << " \t(" << int(descriptors_[i].evaluation * 100) / 100.0 << ")" << system::unlock;
      else
        tianmu_control_.lock(mind_->m_conn->GetThreadID())
            << "Cnd(" << i << "):  " << buf << " \t(" << int(descriptors_[i].evaluation * 100) / 100.0 << ")"
            << system::unlock;
    }
  }

  bool join_or_delayed_present = false;
  for (uint i = 0; i < descriptors_.Size(); i++) {
    if (mind_->ZeroTuples() && empty_cannot_grow) {
      // set all following descriptors_ to done
      for (uint j = i; j < descriptors_.Size(); j++) descriptors_[j].done = true;
      break;
    }

    if (descriptors_[i].done || descriptors_[i].IsDelayed())
      continue;

    // Merging join conditions
    Condition join_desc;
    PrepareJoiningStep(join_desc, descriptors_, i, *mind_);  // group together all join conditions for one step
    no_of_join_conditions -= join_desc.Size();
    JoinTips join_tips(*mind_);

    // Optimization: Check whether there exists "a is null" delayed condition for an outer join
    if (join_desc[0].IsOuter()) {
      for (uint i = 0; i < descriptors_.Size(); i++) {
        if (descriptors_[i].IsDelayed() && !descriptors_[i].done && descriptors_[i].op == common::Operator::O_IS_NULL &&
            join_desc[0].right_dims.Get(descriptors_[i].attr.vc->GetDim()) &&
            !descriptors_[i].attr.vc->IsNullsPossible() && descriptors_[i].IsTypeJoinExprOn()) {
          for (int j = 0; j < join_desc[0].right_dims.Size(); j++) {
            if (join_desc[0].right_dims[j] != true)
              continue;

            join_tips.null_only[j] = true;
          }

          descriptors_[i].done = true;  // completed inside joining algorithms
          no_of_delayed_conditions--;
        }
      }
    }

    if (no_of_join_conditions == 0 &&  // optimizations used only for the last group of conditions
        no_of_delayed_conditions == 0 && parametrized_desc_.Size() == 0) {
      // Optimization: count_only is true => do not materialize multiindex
      // (just counts tuples). WARNING: in this case cannot use multiindex for
      // any operations other than NumOfTuples().
      if (count_only)
        join_tips.count_only = true;

      join_tips.limit = limit;
      // only one dim used in distinct context?
      int distinct_dim = table_->DimInDistinctContext();
      int dims_in_output = 0;

      for (int dim = 0; dim < mind_->NumOfDimensions(); dim++) {
        if (mind_->IsUsedInOutput(dim))
          dims_in_output++;
      }

      if (distinct_dim != -1 && dims_in_output == 1)
        join_tips.distinct_only[distinct_dim] = true;
    }

    // Optimization: Check whether all dimensions are really used
    DimensionVector dims_used(mind_->NumOfDimensions());
    for (uint jj = 0; jj < descriptors_.Size(); jj++) {
      if (jj != i && !descriptors_[jj].done)
        descriptors_[jj].DimensionUsed(dims_used);
    }

    // can't utilize not_used_dims in case there are parameterized descs left
    if (parametrized_desc_.Size() == 0) {
      for (int dim = 0; dim < mind_->NumOfDimensions(); dim++)
        if (!mind_->IsUsedInOutput(dim) && dims_used[dim] == false)
          join_tips.forget_now[dim] = true;
    }

    // Joining itself
    UpdateJoinCondition(join_desc, join_tips);
  }

  // Execute all delayed conditions
  for (uint i = 0; i < descriptors_.Size(); i++) {
    if (descriptors_[i].done)
      continue;

    tianmu_control_.lock(mind_->m_conn->GetThreadID()) << "Executing delayed Cnd(" << i << ")" << system::unlock;
    descriptors_[i].CoerceColumnTypes();
    descriptors_[i].Simplify();
    ApplyDescriptor(i);
    join_or_delayed_present = true;
  }

  if (join_or_delayed_present)
    rough_mind_->MakeDimensionSuspect();  // no common::RoughSetValue::RS_ALL packs

  mind_->UpdateNumOfTuples();
}

void ParameterizedFilter::RoughUpdateParamFilter() {
  MEASURE_FET("ParameterizedFilter::RoughUpdateParamFilter(...)");

  // Prepare();
  PrepareRoughMultiIndex();
  if (descriptors_.Size() < 1)
    return;

  SyntacticalDescriptorListPreprocessing(true);
  bool non_empty = RoughUpdateMultiIndex();
  if (!non_empty)
    rough_mind_->MakeDimensionEmpty();

  RoughUpdateJoins();
}

void ParameterizedFilter::ApplyDescriptor(int desc_number, int64_t limit)
// desc_number = -1 => switch off the rough part
{
  Descriptor &desc = descriptors_[desc_number];
  if (desc.op == common::Operator::O_TRUE) {
    desc.done = true;
    return;
  }

  if (desc.op == common::Operator::O_FALSE) {
    mind_->Empty();
    desc.done = true;
    return;
  }

  DimensionVector dims(mind_->NumOfDimensions());
  desc.DimensionUsed(dims);
  mind_->MarkInvolvedDimGroups(dims);  // create iterators on whole groups (important for
                                       // multidimensional updatable iterators)
  int no_dims = dims.NoDimsUsed();
  if (no_dims == 0 && !desc.IsDeterministic())
    dims.SetAll();

  // Check the easy case (one-dim, parallelizable)
  int one_dim = -1;
  common::RoughSetValue *rf = nullptr;
  if (no_dims == 1) {
    for (int i = 0; i < mind_->NumOfDimensions(); i++) {
      if (!dims[i])
        continue;

      if (mind_->GetFilter(i))
        one_dim = i;  // exactly one filter (non-join or join with forgotten dims)

      break;
    }
  }

  if (one_dim != -1)
    rf = rough_mind_->GetLocalDescFilter(one_dim, desc_number,
                                         true);  // "true" here means that we demand an existing local rough filter

  int packs_no = (int)((mind_->OrigSize(one_dim) + ((1 << mind_->ValueOfPower()) - 1)) >> mind_->ValueOfPower());
  int pack_all = rough_mind_->NumOfPacks(one_dim);
  int pack_some = 0;

  for (int b = 0; b < pack_all; b++) {
    if (rough_mind_->GetPackStatus(one_dim, b) != common::RoughSetValue::RS_NONE)
      pack_some++;
  }

  MIUpdatingIterator mit(mind_, dims);
  desc.CopyDesCond(mit);
  if (desc.EvaluateOnIndex(mit, limit) == common::ErrorCode::SUCCESS) {
    tianmu_control_.lock(mind_->m_conn->GetThreadID())
        << "EvaluateOnIndex done, desc number " << desc_number << system::unlock;
  } else {
    int poolsize = ha_tianmu_engine_->query_thread_pool.size();
    if ((tianmu_sysvar_threadpoolsize > 0) && (packs_no / poolsize > 0) && !desc.IsType_Subquery() &&
        !desc.ExsitTmpTable()) {
      int step = 0;
      int task_num = 0;
      /*Partition task slice*/
      if (pack_some <= poolsize) {
        task_num = poolsize;
      } else {
        step = pack_some / poolsize;
        task_num = packs_no / step;
      }

      int mod = packs_no % task_num;
      int num = packs_no / task_num;

      desc.InitParallel(task_num, mit);

      std::vector<MultiIndex> mis;
      mis.reserve(task_num);

      std::vector<MIUpdatingIterator> taskIterator;
      taskIterator.reserve(task_num);

      for (int i = 0; i < task_num; ++i) {
        auto &mi = mis.emplace_back(*mind_, true);
        int pstart = ((i == 0) ? 0 : mod + i * num);
        int pend = mod + (i + 1) * num - 1;

        auto &mii = taskIterator.emplace_back(&mi, dims);
        mii.SetTaskNum(task_num);
        mii.SetTaskId(i);
        mii.SetNoPacksToGo(pend);
        mii.RewindToPack(pstart);
      }

      utils::result_set<void> res;
      for (int i = 0; i < task_num; ++i) {
        res.insert(ha_tianmu_engine_->query_thread_pool.add_task(&ParameterizedFilter::TaskProcessPacks, this,
                                                                 &taskIterator[i], current_txn_, rf, &dims, desc_number,
                                                                 limit, one_dim));
      }
      res.get_all_with_except();

      if (mind_->m_conn->Killed())
        throw common::KilledException("catch thread pool Exception: TaskProcessPacks");

      mind_->UpdateNumOfTuples();
    } else {
      common::RoughSetValue cur_roughval;
      uint64_t passed = 0;
      int pack = -1;

      while (mit.IsValid()) {
        if (limit != -1 && rf) {  // rf - not null if there is one dim only (otherwise packs make no sense)
          if (passed >= (uint64_t)limit) {
            mit.ResetCurrentPack();
            mit.NextPackrow();
            continue;
          }

          if (mit.PackrowStarted()) {
            if (pack != -1)
              passed += mit.NumOfOnesUncommited(pack);
            pack = mit.GetCurPackrow(one_dim);
          }
        }

        if (rf && mit.GetCurPackrow(one_dim) >= 0)
          cur_roughval = rf[mit.GetCurPackrow(one_dim)];
        else
          cur_roughval = common::RoughSetValue::RS_SOME;

        if (cur_roughval == common::RoughSetValue::RS_NONE) {
          mit.ResetCurrentPack();
          mit.NextPackrow();
        } else if (cur_roughval == common::RoughSetValue::RS_ALL) {
          mit.NextPackrow();
        } else {
          // common::RoughSetValue::RS_SOME or common::RoughSetValue::RS_UNKNOWN
          desc.EvaluatePack(mit);
        }

        if (mind_->m_conn->Killed())
          throw common::KilledException();
      }

      mit.Commit();
    }
  }

  desc.done = true;
  if (one_dim != -1 && mind_->GetFilter(one_dim)) {  // update global rough part
    Filter *f = mind_->GetFilter(one_dim);

    for (int p = 0; p < rough_mind_->NumOfPacks(one_dim); p++) {
      if (!f->IsEmpty(p))
        continue;

      rough_mind_->SetPackStatus(one_dim, p, common::RoughSetValue::RS_NONE);
    }
  }

  desc.UpdateVCStatistics();
  return;
}

void ParameterizedFilter::TaskProcessPacks(MIUpdatingIterator *taskIterator, Transaction *ci, common::RoughSetValue *rf,
                                           [[maybe_unused]] DimensionVector *dims, int desc_number, int64_t limit,
                                           int one_dim) {
  common::RoughSetValue cur_roughval;
  uint64_t passed = 0;
  int pack = -1;
  Descriptor &desc = descriptors_[desc_number];
  current_txn_ = ci;
  common::SetMySQLTHD(ci->Thd());

  while (taskIterator->IsValid()) {
    if (limit != -1 && rf) {
      if (passed >= (uint64_t)limit) {
        taskIterator->ResetCurrentPack();
        taskIterator->NextPackrow();
        continue;
      }

      if (taskIterator->PackrowStarted()) {
        if (pack != -1)
          passed += taskIterator->NumOfOnesUncommited(pack);
        pack = taskIterator->GetCurPackrow(one_dim);
      }
    }

    if (rf && taskIterator->GetCurPackrow(one_dim) >= 0)
      cur_roughval = rf[taskIterator->GetCurPackrow(one_dim)];  //?D??¦Ì¡À?¡ã¡ã¨¹¨º?¡¤??¨¹?DRS_SOME¡À¨ª¨º?2?¡¤??¨¹?D
    else
      cur_roughval = common::RoughSetValue::RS_SOME;

    if (cur_roughval == common::RoughSetValue::RS_NONE) {
      taskIterator->ResetCurrentPack();
      taskIterator->NextPackrow();
    } else if (cur_roughval == common::RoughSetValue::RS_ALL) {
      taskIterator->NextPackrow();
    } else {
      desc.EvaluatePack(*taskIterator);
    }
  }

  taskIterator->Commit(false);
}

void ParameterizedFilter::FilterDeletedByTable(JustATable *rcTable, int &no_dims, int tableIndex) {
  Descriptor desc(table_, no_dims);
  desc.op = common::Operator::O_EQ_ALL;
  desc.encoded = true;

  // Use column 0 to filter the table_ data
  int firstColumn = 0;
  PhysicalColumn *phc = rcTable->GetColumn(firstColumn);
  vcolumn::SingleColumn *vc = new vcolumn::SingleColumn(phc, mind_, 0, 0, rcTable, tableIndex);
  if (!vc)
    throw common::OutOfMemoryException();

  DimensionVector dims(mind_->NumOfDimensions());
  vc->MarkUsedDims(dims);
  mind_->MarkInvolvedDimGroups(
      dims);  // create iterators on whole groups (important for multidimensional updatable iterators)

  MIUpdatingIterator mit(mind_, dims);
  desc.CopyDesCond(mit);

  vc->LockSourcePacks(mit);
  while (mit.IsValid()) {
    vc->EvaluatePack(mit, desc);

    if (mind_->m_conn->Killed())
      throw common::KilledException();
  }

  vc->UnlockSourcePacks();
  mit.Commit();
  if (vc) {
    delete vc;
    vc = nullptr;
  }
}

void ParameterizedFilter::FilterDeletedForSelectAll() {
  if (!table_)
    return;

  auto &rcTables = table_->GetTables();
  int no_dims = 0;

  for (uint tableIndex = 0; tableIndex < rcTables.size(); tableIndex++) {
    auto rcTable = rcTables[tableIndex];
    if (rcTable->TableType() == TType::TEMP_TABLE)
      continue;
    FilterDeletedByTable(rcTable, no_dims, tableIndex);
    no_dims++;
  }
  mind_->UpdateNumOfTuples();
}

}  // namespace core
}  // namespace Tianmu
