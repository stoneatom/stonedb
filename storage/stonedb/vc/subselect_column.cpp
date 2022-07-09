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

#include "subselect_column.h"

#include "core/compiled_query.h"
#include "core/mysql_expression.h"
#include "core/rc_attr.h"
#include "core/value_set.h"
#include "vc/const_column.h"

namespace stonedb {
namespace vcolumn {

SubSelectColumn::SubSelectColumn(core::TempTable *tmptable_for_subquery_, core::MultiIndex *multi_idx, core::TempTable *temp_table,
                                 int temp_table_alias)
    : MultiValColumn(tmptable_for_subquery_->GetColumnType(0), multi_idx),
      tmptable_for_subquery_(std::static_pointer_cast<core::TempTableForSubquery>(tmptable_for_subquery_->shared_from_this())),
      parent_tt_alias_(temp_table_alias),
      min_max_uptodate_(false),
      num_of_cached_values_(0) {
  const std::vector<core::JustATable *> *tables = &temp_table->GetTables();
  const std::vector<int> *aliases = &temp_table->GetAliases();

  col_idx = 0;
  for (uint i = 0; i < tmptable_for_subquery_->NumOfAttrs(); i++) {
    if (tmptable_for_subquery_->IsDisplayAttr(i)) {
      col_idx = i;
      break;
    }
  }

  ct = tmptable_for_subquery_->GetColumnType(col_idx);
  core::MysqlExpression::SetOfVars all_params;

  for (uint i = 0; i < tmptable_for_subquery_->NoVirtColumns(); i++) {
    core::MysqlExpression::SetOfVars params = tmptable_for_subquery_->GetVirtualColumn(i)->GetParams();
    all_params.insert(params.begin(), params.end());
    core::MysqlExpression::sdbfields_cache_t sdbfields = tmptable_for_subquery_->GetVirtualColumn(i)->GetSDBItems();
    if (!sdbfields.empty()) {
      for (auto &sdbfield : sdbfields) {
        auto sdbitem = sdb_items_.find(sdbfield.first);
        if (sdbitem == sdb_items_.end())
          sdb_items_.insert(sdbfield);
        else {
          sdbitem->second.insert(sdbfield.second.begin(), sdbfield.second.end());
        }
      }
    }
  }

  for (auto &iter : all_params) {
    auto ndx_it = find(aliases->begin(), aliases->end(), iter.tab);
    if (ndx_it != aliases->end()) {
      int ndx = (int)distance(aliases->begin(), ndx_it);

      var_map_.push_back(VarMap(iter, (*tables)[ndx], ndx));
      var_types_[iter] = (*tables)[ndx]->GetColumnType(var_map_[var_map_.size() - 1].col_idx_);
      var_buf_for_exact_[iter] = std::vector<core::MysqlExpression::value_or_null_info_t>();  // now empty,
                                                                                             // pointers
      // inserted by SetBufs()
    } else if (iter.tab == temp_table_alias) {
      var_map_.push_back(VarMap(iter, temp_table, 0));
      var_types_[iter] = temp_table->GetColumnType(var_map_[var_map_.size() - 1].col_idx_);
      var_buf_for_exact_[iter] = std::vector<core::MysqlExpression::value_or_null_info_t>();  // now empty,
                                                                                             // pointers
      // inserted by SetBufs()
    } else {
      // parameter
      params_.insert(iter);
    }
  }
  SetBufs(&var_buf_for_exact_);
  var_buf_for_rough_ = var_buf_for_exact_;

  if (var_map_.size() == 1)
    dimension_ = var_map_[0].dimension_;
  else
    dimension_ = -1;
  first_eval_for_rough_ = true;
  out_of_date_rough_ = true;
}

SubSelectColumn::SubSelectColumn(const SubSelectColumn &c)
    : MultiValColumn(c),
      col_idx(c.col_idx),
      tmptable_for_subquery_(c.tmptable_for_subquery_),
      min_(c.min_),
      max_(c.max_),
      min_max_uptodate_(c.min_max_uptodate_),
      expected_type_(c.expected_type_),
      var_buf_for_exact_(),
      num_of_cached_values_(c.num_of_cached_values_),
      out_of_date_rough_(c.out_of_date_rough_) {
  var_types_.empty();
  if (c.cache_.get()) {
    cache_ = std::make_shared<core::ValueSet>(*c.cache_.get());
    // core::ValueSet* vs = new core::ValueSet(*c.cache_.get());
    // cache_ = std::shared_ptr<core::ValueSet>(vs);
  }
}

SubSelectColumn::~SubSelectColumn() {
}

void SubSelectColumn::SetBufs(core::MysqlExpression::var_buf_t *bufs) {
  DEBUG_ASSERT(bufs);
  for (auto &it : sdb_items_) {
    auto buf_set = bufs->find(it.first);
    if (buf_set != bufs->end()) {
      // for each sdbitem* in the set it->second put its buffer to buf_set.second
      for (auto &sdbfield : it.second) {
        core::ValueOrNull *von;
        sdbfield->SetBuf(von);
        buf_set->second.push_back(core::MysqlExpression::value_or_null_info_t(core::ValueOrNull(), von));
      }
    }
  }
}

types::BString SubSelectColumn::GetMinStringImpl([[maybe_unused]] const core::MIIterator &mit) {
  types::BString s;
  DEBUG_ASSERT(!"To be implemented.");
  return s;
}

types::BString SubSelectColumn::GetMaxStringImpl([[maybe_unused]] const core::MIIterator &mit) {
  types::BString s;
  DEBUG_ASSERT(!"To be implemented.");
  return s;
}

size_t SubSelectColumn::MaxStringSizeImpl()  // maximal byte string length in column
{
  return ct.GetPrecision();
}

core::PackOntologicalStatus SubSelectColumn::GetPackOntologicalStatusImpl(
    [[maybe_unused]] const core::MIIterator &mit) {
  return core::PackOntologicalStatus::NORMAL;
}

void SubSelectColumn::EvaluatePackImpl([[maybe_unused]] core::MIUpdatingIterator &mit,
                                       [[maybe_unused]] core::Descriptor &desc) {
  DEBUG_ASSERT(!"To be implemented.");
  DEBUG_ASSERT(0);
}

common::Tribool SubSelectColumn::ContainsImpl(core::MIIterator const &mit, types::RCDataType const &v) {
  // If the sub-select is something like 'select null from xxx' then there
  // is no need to execute the sub-select, just return common::TRIBOOL_UNKNOWN.
  VirtualColumn *vc = tmptable_for_subquery_->GetAttrP(col_idx)->term.vc;
  if (vc->IsFullConst() && vc->IsNull(core::MIIterator(NULL, multi_idx_->NoPower()))) return common::TRIBOOL_UNKNOWN;

  PrepareSubqResult(mit, false);
  common::Tribool res = false;
  if (!cache_) {
    cache_ = std::shared_ptr<core::ValueSet>(new core::ValueSet(multi_idx_->NoPower()));
    cache_->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
  }

  if (cache_->Contains(v, GetCollation()))
    return true;
  else if (cache_->ContainsNulls())
    res = common::TRIBOOL_UNKNOWN;

  bool added_new_value = false;
  if (types::RequiresUTFConversions(GetCollation()) && Type().IsString()) {
    for (int64_t i = num_of_cached_values_; i < tmptable_for_subquery_->NumOfObj(); i++) {
      num_of_cached_values_++;
      added_new_value = true;
      if (tmptable_for_subquery_->IsNull(i, col_idx)) {
        cache_->AddNull();
        res = common::TRIBOOL_UNKNOWN;
      } else {
        types::BString val;
        tmptable_for_subquery_->GetTableString(val, i, col_idx);
        val.MakePersistent();
        cache_->Add(val);
        if (CollationStrCmp(GetCollation(), val, v.ToBString()) == 0) {
          res = true;
          break;
        }
      }
    }
  } else {
    for (int64_t i = num_of_cached_values_; i < tmptable_for_subquery_->NumOfObj(); i++) {
      num_of_cached_values_++;
      added_new_value = true;
      if (tmptable_for_subquery_->IsNull(i, col_idx)) {
        cache_->AddNull();
        res = common::TRIBOOL_UNKNOWN;
      } else {
        types::RCValueObject value;
        if (Type().IsString()) {
          types::BString s;
          tmptable_for_subquery_->GetTableString(s, i, col_idx);
          value = s;
          static_cast<types::BString *>(value.Get())->MakePersistent();
        } else
          value = tmptable_for_subquery_->GetValueObject(i, col_idx);
        cache_->Add(value);
        if (value == v) {
          res = true;
          break;
        }
      }
    }
  }
  if (added_new_value && num_of_cached_values_ == tmptable_for_subquery_->NumOfObj())
    cache_->ForcePreparation();  // completed for the first time => recalculate
                                // cache_
  return res;
}

void SubSelectColumn::PrepareAndFillCache() {
  if (!cache_) {
    cache_ = std::shared_ptr<core::ValueSet>(new core::ValueSet(multi_idx_->NoPower()));
    cache_->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
  }
  for (int64_t i = num_of_cached_values_; i < tmptable_for_subquery_->NumOfObj(); i++) {
    num_of_cached_values_++;
    if (tmptable_for_subquery_->IsNull(i, col_idx)) {
      cache_->AddNull();
    } else {
      types::RCValueObject value;
      if (Type().IsString()) {
        types::BString s;
        tmptable_for_subquery_->GetTableString(s, i, col_idx);
        value = s;
        static_cast<types::BString *>(value.Get())->MakePersistent();
      } else
        value = tmptable_for_subquery_->GetValueObject(i, col_idx);
      cache_->Add(value);
    }
  }
  cache_->ForcePreparation();
  cache_->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
}

bool SubSelectColumn::IsSetEncoded(common::CT at,
                                   int scale)  // checks whether the set is constant and fixed size equal to
                                               // the given one
{
  if (!cache_ || !cache_->EasyMode() || !tmptable_for_subquery_->IsMaterialized() || num_of_cached_values_ < tmptable_for_subquery_->NumOfObj()) return false;
  return (scale == ct.GetScale() &&
          (at == expected_type_.GetTypeName() ||
           (core::ATI::IsFixedNumericType(at) && core::ATI::IsFixedNumericType(expected_type_.GetTypeName()))));
}

common::Tribool SubSelectColumn::Contains64Impl(const core::MIIterator &mit, int64_t val)  // easy case for integers
{
  if (cache_ && cache_->EasyMode()) {
    common::Tribool contains = false;
    if (val == common::NULL_VALUE_64) return common::TRIBOOL_UNKNOWN;
    if (cache_->Contains(val))
      contains = true;
    else if (cache_->ContainsNulls())
      contains = common::TRIBOOL_UNKNOWN;
    return contains;
  }
  return ContainsImpl(mit, types::RCNum(val, ct.GetScale()));
}

common::Tribool SubSelectColumn::ContainsStringImpl(const core::MIIterator &mit,
                                                    types::BString &val)  // easy case for strings
{
  if (cache_ && cache_->EasyMode()) {
    common::Tribool contains = false;
    if (val.IsNull()) return common::TRIBOOL_UNKNOWN;
    if (cache_->Contains(val))
      contains = true;
    else if (cache_->ContainsNulls())
      contains = common::TRIBOOL_UNKNOWN;
    return contains;
  }
  return ContainsImpl(mit, val);
}

int64_t SubSelectColumn::NoValuesImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  if (!tmptable_for_subquery_->IsMaterialized()) tmptable_for_subquery_->Materialize();
  return tmptable_for_subquery_->NumOfObj();
}

int64_t SubSelectColumn::AtLeastNoDistinctValuesImpl(core::MIIterator const &mit, int64_t const at_least) {
  DEBUG_ASSERT(at_least > 0);
  PrepareSubqResult(mit, false);
  core::ValueSet vals(multi_idx_->NoPower());
  vals.Prepare(expected_type_.GetTypeName(), expected_type_.GetScale(), expected_type_.GetCollation());

  if (types::RequiresUTFConversions(GetCollation()) && Type().IsString()) {
    types::BString buf(NULL, types::CollationBufLen(GetCollation(), tmptable_for_subquery_->MaxStringSize(col_idx)), true);
    for (int64_t i = 0; vals.NoVals() < at_least && i < tmptable_for_subquery_->NumOfObj(); i++) {
      if (!tmptable_for_subquery_->IsNull(i, col_idx)) {
        types::BString s;
        tmptable_for_subquery_->GetTable_S(s, i, col_idx);
        ConvertToBinaryForm(s, buf, GetCollation());
        vals.Add(buf);
      }
    }
  } else {
    for (int64_t i = 0; vals.NoVals() < at_least && i < tmptable_for_subquery_->NumOfObj(); i++) {
      if (!tmptable_for_subquery_->IsNull(i, col_idx)) {
        types::RCValueObject val = tmptable_for_subquery_->GetValueObject(i, col_idx);
        vals.Add(val);
      }
    }
  }

  return vals.NoVals();
}

bool SubSelectColumn::ContainsNullImpl(const core::MIIterator &mit) {
  PrepareSubqResult(mit, false);
  for (int64_t i = 0; i < tmptable_for_subquery_->NumOfObj(); ++i)
    if (tmptable_for_subquery_->IsNull(i, col_idx)) return true;
  return false;
}

std::unique_ptr<MultiValColumn::IteratorInterface> SubSelectColumn::BeginImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  return std::unique_ptr<MultiValColumn::IteratorInterface>(new IteratorImpl(tmptable_for_subquery_->begin(), expected_type_));
}

std::unique_ptr<MultiValColumn::IteratorInterface> SubSelectColumn::EndImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  return std::unique_ptr<MultiValColumn::IteratorInterface>(new IteratorImpl(tmptable_for_subquery_->end(), expected_type_));
}

void SubSelectColumn::RequestEval(const core::MIIterator &mit, const int tta) {
  is_first_eval_ = true;
  first_eval_for_rough_ = true;
  for (uint i = 0; i < tmptable_for_subquery_->NoVirtColumns(); i++) tmptable_for_subquery_->GetVirtualColumn(i)->RequestEval(mit, tta);
}

void SubSelectColumn::PrepareSubqResult(const core::MIIterator &mit, bool exists_only) {
  MEASURE_FET("SubSelectColumn::PrepareSubqCopy(...)");
  bool cor = IsCorrelated();
  if (!cor) {
    if (tmptable_for_subquery_->IsFullyMaterialized()) return;
  }

  tmptable_for_subquery_->CreateTemplateIfNotExists();

  if (cor && (FeedArguments(mit, false))) {
    cache_.reset();
    num_of_cached_values_ = 0;
    tmptable_for_subquery_->ResetToTemplate(false);
    tmptable_for_subquery_->ResetVCStatistics();
    tmptable_for_subquery_->SuspendDisplay();
    try {
      tmptable_for_subquery_->ProcessParameters(mit,
                              parent_tt_alias_);  // exists_only is not needed
                                                 // here (limit already set)
    } catch (...) {
      tmptable_for_subquery_->ResumeDisplay();
      throw;
    }
    tmptable_for_subquery_->ResumeDisplay();
  }
  tmptable_for_subquery_->SuspendDisplay();
  try {
    if (exists_only) tmptable_for_subquery_->SetMode(core::TMParameter::TM_EXISTS);
    tmptable_for_subquery_->Materialize(cor);
  } catch (...) {
    tmptable_for_subquery_->ResumeDisplay();
    // tmptable_for_subquery_->ResetForSubq();
    throw;
  }
  tmptable_for_subquery_->ResumeDisplay();
}

void SubSelectColumn::RoughPrepareSubqCopy(const core::MIIterator &mit,
                                           [[maybe_unused]] core::SubSelectOptimizationType sot) {
  MEASURE_FET("SubSelectColumn::RoughPrepareSubqCopy(...)");
  tmptable_for_subquery_->CreateTemplateIfNotExists();
  if ((!IsCorrelated() && first_eval_for_rough_) || (IsCorrelated() && (FeedArguments(mit, true)))) {
    out_of_date_rough_ = false;
    //		cache_.reset();
    //		num_of_cached_values_ = 0;
    tmptable_for_subquery_->ResetToTemplate(true);
    tmptable_for_subquery_->ResetVCStatistics();
    tmptable_for_subquery_->SuspendDisplay();
    try {
      tmptable_for_subquery_->RoughProcessParameters(mit, parent_tt_alias_);
    } catch (...) {
      tmptable_for_subquery_->ResumeDisplay();
    }
    tmptable_for_subquery_->ResumeDisplay();
  }
  tmptable_for_subquery_->SuspendDisplay();
  try {
    tmptable_for_subquery_->RoughMaterialize(true);
  } catch (...) {
  }
  tmptable_for_subquery_->ResumeDisplay();
}

bool SubSelectColumn::IsCorrelated() const {
  if (var_map_.size() || params_.size()) return true;
  return false;
}

bool SubSelectColumn::IsNullImpl(const core::MIIterator &mit) {
  PrepareSubqResult(mit, false);
  return tmptable_for_subquery_->IsNull(0, col_idx);
}

types::RCValueObject SubSelectColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool lookup_to_num) {
  PrepareSubqResult(mit, false);
  types::RCValueObject val = tmptable_for_subquery_->GetValueObject(0, col_idx);
  if (expected_type_.IsString()) return val.ToBString();
  if (expected_type_.IsNumeric() && core::ATI::IsStringType(val.Type())) {
    types::RCNum rc;
    types::RCNum::Parse(*static_cast<types::BString *>(val.Get()), rc, expected_type_.GetTypeName());
    val = rc;
  }
  return val;
}

int64_t SubSelectColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  PrepareSubqResult(mit, false);
  return tmptable_for_subquery_->GetTable64(0, col_idx);
}

core::RoughValue SubSelectColumn::RoughGetValue(const core::MIIterator &mit, core::SubSelectOptimizationType sot) {
  RoughPrepareSubqCopy(mit, sot);
  return core::RoughValue(tmptable_for_subquery_->GetTable64(0, col_idx), tmptable_for_subquery_->GetTable64(1, col_idx));
}

double SubSelectColumn::GetValueDoubleImpl(const core::MIIterator &mit) {
  PrepareSubqResult(mit, false);
  int64_t v = tmptable_for_subquery_->GetTable64(0, col_idx);
  return *((double *)(&v));
}

void SubSelectColumn::GetValueStringImpl(types::BString &s, core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  tmptable_for_subquery_->GetTable_S(s, 0, col_idx);
}

types::RCValueObject SubSelectColumn::GetSetMinImpl(core::MIIterator const &mit) {
  // assert: this->params are all set
  PrepareSubqResult(mit, false);
  if (!min_max_uptodate_) CalculateMinMax();
  return min_;
}

types::RCValueObject SubSelectColumn::GetSetMaxImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  if (!min_max_uptodate_) CalculateMinMax();
  return max_;
}
bool SubSelectColumn::CheckExists(core::MIIterator const &mit) {
  PrepareSubqResult(mit, true);  // true: exists_only
  return tmptable_for_subquery_->NumOfObj() > 0;
}

bool SubSelectColumn::IsEmptyImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  return tmptable_for_subquery_->NumOfObj() == 0;
}

common::Tribool SubSelectColumn::RoughIsEmpty(core::MIIterator const &mit, core::SubSelectOptimizationType sot) {
  RoughPrepareSubqCopy(mit, sot);
  return tmptable_for_subquery_->RoughIsEmpty();
}

void SubSelectColumn::CalculateMinMax() {
  if (!tmptable_for_subquery_->IsMaterialized()) tmptable_for_subquery_->Materialize();
  if (tmptable_for_subquery_->NumOfObj() == 0) {
    min_ = max_ = types::RCValueObject();
    min_max_uptodate_ = true;
    return;
  }

  min_ = max_ = types::RCValueObject();
  bool found_not_null = false;
  if (types::RequiresUTFConversions(GetCollation()) && Type().IsString() && expected_type_.IsString()) {
    types::BString val_s, min_s, max_s;
    for (int64_t i = 0; i < tmptable_for_subquery_->NumOfObj(); i++) {
      tmptable_for_subquery_->GetTable_S(val_s, i, col_idx);
      if (val_s.IsNull()) continue;
      if (!found_not_null && !val_s.IsNull()) {
        found_not_null = true;
        min_s.PersistentCopy(val_s);
        max_s.PersistentCopy(val_s);
        continue;
      }
      if (CollationStrCmp(GetCollation(), val_s, max_s) > 0) {
        max_s.PersistentCopy(val_s);
      } else if (CollationStrCmp(GetCollation(), val_s, min_s) < 0) {
        min_s.PersistentCopy(val_s);
      }
    }
    min_ = min_s;
    max_ = max_s;
  } else {
    types::RCValueObject val;
    for (int64_t i = 0; i < tmptable_for_subquery_->NumOfObj(); i++) {
      val = tmptable_for_subquery_->GetValueObject(i, col_idx);
      if (expected_type_.IsString()) {
        val = val.ToBString();
        static_cast<types::BString *>(val.Get())->MakePersistent();
      } else if (expected_type_.IsNumeric() && core::ATI::IsStringType(val.Type())) {
        types::RCNum rc;
        types::RCNum::Parse(*static_cast<types::BString *>(val.Get()), rc, expected_type_.GetTypeName());
        val = rc;
      }

      if (!found_not_null && !val.IsNull()) {
        found_not_null = true;
        min_ = max_ = val;
        continue;
      }
      if (val > max_) {
        max_ = val;
      } else if (val < min_) {
        min_ = val;
      }
    }
  }
  min_max_uptodate_ = true;
}

bool SubSelectColumn::FeedArguments(const core::MIIterator &mit, bool for_rough) {
  MEASURE_FET("SubSelectColumn::FeedArguments(...)");

  bool diff;
  if (for_rough) {
    diff = first_eval_for_rough_;
    for (auto &iter : var_map_) {
      core::ValueOrNull v = iter.GetTabPtr()->GetComplexValue(mit[iter.dimension_], iter.col_idx_);
      auto cache_ = var_buf_for_rough_.find(iter.var_);
      v.MakeStringOwner();
      if (cache_->second.empty()) {  // empty if STONEDBexpression - feeding unnecessary
        bool ldiff = v != param_cache_for_rough_[iter.var_];
        diff = diff || ldiff;
        if (ldiff) param_cache_for_rough_[iter.var_] = v;
      } else {
        DEBUG_ASSERT(cache_ != var_buf_for_rough_.end());
        diff = diff || (v != cache_->second.begin()->first);
        if (diff)
          for (auto &val_it : cache_->second) *(val_it.second) = val_it.first = v;
      }
    }
    first_eval_for_rough_ = false;
  } else {
    diff = is_first_eval_;
    for (auto &iter : var_map_) {
      core::ValueOrNull v = iter.GetTabPtr()->GetComplexValue(mit[iter.dimension_], iter.col_idx_);
      auto cache_ = var_buf_for_exact_.find(iter.var_);
      v.MakeStringOwner();
      if (cache_->second.empty()) {  // empty if STONEDBexpression - feeding unnecessary
        bool ldiff = v != param_cache_for_exact_[iter.var_];
        diff = diff || ldiff;
        if (ldiff) param_cache_for_exact_[iter.var_] = v;
      } else {
        DEBUG_ASSERT(cache_ != var_buf_for_exact_.end());
        core::ValueOrNull v1 = *(cache_->second.begin()->second);
        core::ValueOrNull v2 = (cache_->second.begin()->first);
        diff = diff || (v != cache_->second.begin()->first);
        if (diff)
          for (auto &val_it : cache_->second) *(val_it.second) = val_it.first = v;
      }
    }
    is_first_eval_ = false;
  }

  if (diff) {
    min_max_uptodate_ = false;
    out_of_date_rough_ = true;
  }
  return diff;
}

void SubSelectColumn::SetExpectedTypeImpl(core::ColumnType const &ct) { expected_type_ = ct; }

bool SubSelectColumn::MakeParallelReady() {
  core::MIDummyIterator mit(multi_idx_);
  PrepareSubqResult(mit, false);
  if (!tmptable_for_subquery_->IsMaterialized()) tmptable_for_subquery_->Materialize();
  if (tmptable_for_subquery_->NumOfObj() > tmptable_for_subquery_->GetPageSize()) return false;  // multipage Attrs - not thread safe
  // below assert doesn't take into account lazy field
  // NoMaterialized() tells how many rows in lazy mode are materialized
  // DEBUG_ASSERT(tmptable_for_subquery_->NumOfObj() == tmptable_for_subquery_->NoMaterialized());
  PrepareAndFillCache();
  return true;
}

}  // namespace vcolumn
}  // namespace stonedb
