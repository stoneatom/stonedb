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
#include "core/tianmu_attr.h"
#include "core/value_set.h"
#include "vc/const_column.h"

namespace Tianmu {
namespace vcolumn {

SubSelectColumn::SubSelectColumn(core::TempTable *tmp_tab_subq_ptr_, core::MultiIndex *multi_index,
                                 core::TempTable *temp_table, int temp_table_alias)
    : MultiValColumn(tmp_tab_subq_ptr_->GetColumnType(0), multi_index),
      tmp_tab_subq_ptr_(std::static_pointer_cast<core::TempTableForSubquery>(tmp_tab_subq_ptr_->shared_from_this())),
      parent_tt_alias_(temp_table_alias),
      check_min_max_uptodate_(false),
      no_cached_values_(0) {
  const std::vector<core::JustATable *> *tables = &temp_table->GetTables();
  const std::vector<int> *aliases = &temp_table->GetAliases();

  col_idx_ = 0;
  for (uint i = 0; i < tmp_tab_subq_ptr_->NumOfAttrs(); i++)
    if (tmp_tab_subq_ptr_->IsDisplayAttr(i)) {
      col_idx_ = i;
      break;
    }
  ct = tmp_tab_subq_ptr_->GetColumnType(col_idx_);
  core::MysqlExpression::SetOfVars all_params;

  for (uint i = 0; i < tmp_tab_subq_ptr_->NumOfVirtColumns(); i++) {
    core::MysqlExpression::SetOfVars params_ = tmp_tab_subq_ptr_->GetVirtualColumn(i)->GetParams();
    all_params.insert(params_.begin(), params_.end());
    core::MysqlExpression::tianmu_fields_cache_t tianmufields =
        tmp_tab_subq_ptr_->GetVirtualColumn(i)->GetTIANMUItems();
    if (!tianmufields.empty()) {
      for (auto &tianmufield : tianmufields) {
        auto tianmuitem = tianmu_items_.find(tianmufield.first);
        if (tianmuitem == tianmu_items_.end())
          tianmu_items_.insert(tianmufield);
        else {
          tianmuitem->second.insert(tianmufield.second.begin(), tianmufield.second.end());
        }
      }
    }
  }

  for (auto &iter : all_params) {
    auto ndx_it = find(aliases->begin(), aliases->end(), iter.tab);
    if (ndx_it != aliases->end()) {
      int ndx = (int)distance(aliases->begin(), ndx_it);

      var_map_.push_back(VarMap(iter, (*tables)[ndx], ndx));
      var_types_[iter] = (*tables)[ndx]->GetColumnType(var_map_[var_map_.size() - 1].col_ndx);
      var_buf_for_exact_[iter] = std::vector<core::MysqlExpression::value_or_null_info_t>();  // now empty,
                                                                                              // pointers
      // inserted by SetBufs()
    } else if (iter.tab == temp_table_alias) {
      var_map_.push_back(VarMap(iter, temp_table, 0));
      var_types_[iter] = temp_table->GetColumnType(var_map_[var_map_.size() - 1].col_ndx);
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
    dim_ = var_map_[0].dim;
  else
    dim_ = -1;
  if_first_eval_for_rough_ = true;
  if_out_of_date_rough_ = true;
}

SubSelectColumn::SubSelectColumn(const SubSelectColumn &c)
    : MultiValColumn(c),
      col_idx_(c.col_idx_),
      tmp_tab_subq_ptr_(c.tmp_tab_subq_ptr_),
      min_(c.min_),
      max_(c.max_),
      check_min_max_uptodate_(c.check_min_max_uptodate_),
      expected_type_(c.expected_type_),
      var_buf_for_exact_(),
      no_cached_values_(c.no_cached_values_),
      if_out_of_date_rough_(c.if_out_of_date_rough_) {
  if (c.cache_.get()) {
    cache_ = std::make_shared<core::ValueSet>(*c.cache_.get());
    // core::ValueSet* vs = new core::ValueSet(*c.cache_.get());
    // cache = std::shared_ptr<core::ValueSet>(vs);
  }
}

SubSelectColumn::~SubSelectColumn() {
  //	if(!!table)
  //			table->TranslateBackVCs();
  //
  //	if(!!table_for_rough)
  //		table_for_rough->TranslateBackVCs();
  //	if(table != tmp_tab_subq_ptr_) {
  //		table.reset();
  //		table_for_rough.reset();
  //	}
}

void SubSelectColumn::SetBufs(core::MysqlExpression::var_buf_t *bufs) {
  DEBUG_ASSERT(bufs);
  for (auto &it : tianmu_items_) {
    auto buf_set = bufs->find(it.first);
    if (buf_set != bufs->end()) {
      // for each tianmuitem* in the set it->second put its buffer to buf_set.second
      for (auto &tianmufield : it.second) {
        core::ValueOrNull *von;
        tianmufield->SetBuf(von);
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

core::PackOntologicalStatus SubSelectColumn::GetPackOntologicalStatusImpl([
    [maybe_unused]] const core::MIIterator &mit) {
  return core::PackOntologicalStatus::NORMAL;
}

void SubSelectColumn::EvaluatePackImpl([[maybe_unused]] core::MIUpdatingIterator &mit,
                                       [[maybe_unused]] core::Descriptor &desc) {
  DEBUG_ASSERT(!"To be implemented.");
  DEBUG_ASSERT(0);
}

common::Tribool SubSelectColumn::ContainsImpl(core::MIIterator const &mit, types::TianmuDataType const &v) {
  // If the sub-select is something like 'select null from xxx' then there
  // is no need to execute the sub-select, just return common::TRIBOOL_UNKNOWN.
  Tianmu::core::TempTable::Attr *attr = tmp_tab_subq_ptr_->GetAttrP(col_idx_);
  if (attr && attr->IsListField()) {
    VirtualColumn *vc = attr->term.vc;
    if (vc->IsFullConst() && vc->IsNull(core::MIIterator(nullptr, multi_index_->ValueOfPower())))
      return common::TRIBOOL_UNKNOWN;
  }

  PrepareSubqResult(mit, false);
  common::Tribool res = false;
  if (!cache_) {
    cache_ = std::shared_ptr<core::ValueSet>(new core::ValueSet(multi_index_->ValueOfPower()));
    cache_->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
  }

  if (cache_->Contains(v, GetCollation()))
    return true;
  else if (cache_->ContainsNulls())
    res = common::TRIBOOL_UNKNOWN;

  bool added_new_value = false;
  if (types::RequiresUTFConversions(GetCollation()) && Type().IsString()) {
    for (int64_t i = no_cached_values_; i < tmp_tab_subq_ptr_->NumOfObj(); i++) {
      no_cached_values_++;
      added_new_value = true;
      if (tmp_tab_subq_ptr_->IsNull(i, col_idx_)) {
        cache_->AddNull();
        res = common::TRIBOOL_UNKNOWN;
      } else {
        types::BString val;
        tmp_tab_subq_ptr_->GetTableString(val, i, col_idx_);
        val.MakePersistent();
        cache_->Add(val);
        if (CollationStrCmp(GetCollation(), val, v.ToBString()) == 0) {
          res = true;
          break;
        }
      }
    }
  } else {
    for (int64_t i = no_cached_values_; i < tmp_tab_subq_ptr_->NumOfObj(); i++) {
      no_cached_values_++;
      added_new_value = true;
      if (tmp_tab_subq_ptr_->IsNull(i, col_idx_)) {
        cache_->AddNull();
        res = common::TRIBOOL_UNKNOWN;
      } else {
        types::TianmuValueObject value;
        if (Type().IsString()) {
          types::BString s;
          tmp_tab_subq_ptr_->GetTableString(s, i, col_idx_);
          value = s;
          static_cast<types::BString *>(value.Get())->MakePersistent();
        } else
          value = tmp_tab_subq_ptr_->GetValueObject(i, col_idx_);
        cache_->Add(value);
        if (value == v) {
          res = true;
          break;
        }
      }
    }
  }
  if (added_new_value && no_cached_values_ == tmp_tab_subq_ptr_->NumOfObj())
    cache_->ForcePreparation();  // completed for the first time => recalculate
                                 // cache_
  return res;
}

void SubSelectColumn::PrepareAndFillCache() {
  if (!cache_) {
    cache_ = std::shared_ptr<core::ValueSet>(new core::ValueSet(multi_index_->ValueOfPower()));
    cache_->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
  }
  for (int64_t i = no_cached_values_; i < tmp_tab_subq_ptr_->NumOfObj(); i++) {
    no_cached_values_++;
    if (tmp_tab_subq_ptr_->IsNull(i, col_idx_)) {
      cache_->AddNull();
    } else {
      types::TianmuValueObject value;
      if (Type().IsString()) {
        types::BString s;
        tmp_tab_subq_ptr_->GetTableString(s, i, col_idx_);
        value = s;
        static_cast<types::BString *>(value.Get())->MakePersistent();
      } else
        value = tmp_tab_subq_ptr_->GetValueObject(i, col_idx_);
      cache_->Add(value);
    }
  }
  cache_->ForcePreparation();
  cache_->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
}

bool SubSelectColumn::IsSetEncoded(common::ColumnType at,
                                   int scale)  // checks whether the set is constant and fixed size equal to
                                               // the given one
{
  if (!cache_ || !cache_->EasyMode() || !tmp_tab_subq_ptr_->IsMaterialized() ||
      no_cached_values_ < tmp_tab_subq_ptr_->NumOfObj())
    return false;
  return (scale == ct.GetScale() &&
          (at == expected_type_.GetTypeName() ||
           (core::ATI::IsFixedNumericType(at) && core::ATI::IsFixedNumericType(expected_type_.GetTypeName()))));
}

common::Tribool SubSelectColumn::Contains64Impl(const core::MIIterator &mit, int64_t val)  // easy case for integers
{
  if (cache_ && cache_->EasyMode()) {
    common::Tribool contains = false;
    if (val == common::NULL_VALUE_64)
      return common::TRIBOOL_UNKNOWN;
    if (cache_->Contains(val))
      contains = true;
    else if (cache_->ContainsNulls())
      contains = common::TRIBOOL_UNKNOWN;
    return contains;
  }
  return ContainsImpl(mit, types::TianmuNum(val, ct.GetScale()));
}

common::Tribool SubSelectColumn::ContainsStringImpl(const core::MIIterator &mit,
                                                    types::BString &val)  // easy case for strings
{
  if (cache_ && cache_->EasyMode()) {
    common::Tribool contains = false;
    if (val.IsNull())
      return common::TRIBOOL_UNKNOWN;
    if (cache_->Contains(val))
      contains = true;
    else if (cache_->ContainsNulls())
      contains = common::TRIBOOL_UNKNOWN;
    return contains;
  }
  return ContainsImpl(mit, val);
}

int64_t SubSelectColumn::NumOfValuesImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  if (!tmp_tab_subq_ptr_->IsMaterialized())
    tmp_tab_subq_ptr_->Materialize();
  return tmp_tab_subq_ptr_->NumOfObj();
}

int64_t SubSelectColumn::AtLeastNoDistinctValuesImpl(core::MIIterator const &mit, int64_t const at_least) {
  DEBUG_ASSERT(at_least > 0);
  PrepareSubqResult(mit, false);
  core::ValueSet vals(multi_index_->ValueOfPower());
  vals.Prepare(expected_type_.GetTypeName(), expected_type_.GetScale(), expected_type_.GetCollation());

  if (types::RequiresUTFConversions(GetCollation()) && Type().IsString()) {
    types::BString buf(nullptr, types::CollationBufLen(GetCollation(), tmp_tab_subq_ptr_->MaxStringSize(col_idx_)),
                       true);
    for (int64_t i = 0; vals.NoVals() < at_least && i < tmp_tab_subq_ptr_->NumOfObj(); i++) {
      if (!tmp_tab_subq_ptr_->IsNull(i, col_idx_)) {
        types::BString s;
        tmp_tab_subq_ptr_->GetTable_S(s, i, col_idx_);
        ConvertToBinaryForm(s, buf, GetCollation());
        vals.Add(buf);
      }
    }
  } else {
    for (int64_t i = 0; vals.NoVals() < at_least && i < tmp_tab_subq_ptr_->NumOfObj(); i++) {
      if (!tmp_tab_subq_ptr_->IsNull(i, col_idx_)) {
        types::TianmuValueObject val = tmp_tab_subq_ptr_->GetValueObject(i, col_idx_);
        vals.Add(val);
      }
    }
  }

  return vals.NoVals();
}

bool SubSelectColumn::ContainsNullImpl(const core::MIIterator &mit) {
  PrepareSubqResult(mit, false);
  for (int64_t i = 0; i < tmp_tab_subq_ptr_->NumOfObj(); ++i)
    if (tmp_tab_subq_ptr_->IsNull(i, col_idx_))
      return true;
  return false;
}

std::unique_ptr<MultiValColumn::IteratorInterface> SubSelectColumn::BeginImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  return std::unique_ptr<MultiValColumn::IteratorInterface>(
      new IteratorImpl(tmp_tab_subq_ptr_->begin(), expected_type_));
}

std::unique_ptr<MultiValColumn::IteratorInterface> SubSelectColumn::EndImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  return std::unique_ptr<MultiValColumn::IteratorInterface>(new IteratorImpl(tmp_tab_subq_ptr_->end(), expected_type_));
}

void SubSelectColumn::RequestEval(const core::MIIterator &mit, const int tta) {
  first_eval_ = true;
  if_first_eval_for_rough_ = true;
  for (uint i = 0; i < tmp_tab_subq_ptr_->NumOfVirtColumns(); i++)
    tmp_tab_subq_ptr_->GetVirtualColumn(i)->RequestEval(mit, tta);
}

void SubSelectColumn::PrepareSubqResult(const core::MIIterator &mit, bool exists_only) {
  MEASURE_FET("SubSelectColumn::PrepareSubqCopy(...)");
  bool cor = IsCorrelated();
  if (!cor) {
    if (tmp_tab_subq_ptr_->IsFullyMaterialized())
      return;
  }

  tmp_tab_subq_ptr_->CreateTemplateIfNotExists();

  if (cor && (FeedArguments(mit, false))) {
    cache_.reset();
    no_cached_values_ = 0;
    tmp_tab_subq_ptr_->ResetToTemplate(false);
    tmp_tab_subq_ptr_->ResetVCStatistics();
    tmp_tab_subq_ptr_->SuspendDisplay();
    try {
      tmp_tab_subq_ptr_->ProcessParameters(mit,
                                           parent_tt_alias_);  // exists_only is not needed
                                                               // here (limit already set)
    } catch (...) {
      tmp_tab_subq_ptr_->ResumeDisplay();
      throw;
    }
    tmp_tab_subq_ptr_->ResumeDisplay();
  }
  tmp_tab_subq_ptr_->SuspendDisplay();
  try {
    if (exists_only)
      tmp_tab_subq_ptr_->SetMode(core::TMParameter::TM_EXISTS);
    tmp_tab_subq_ptr_->Materialize(cor);
  } catch (...) {
    tmp_tab_subq_ptr_->ResumeDisplay();
    // tmp_tab_subq_ptr_->ResetForSubq();
    throw;
  }
  tmp_tab_subq_ptr_->ResumeDisplay();
}

void SubSelectColumn::RoughPrepareSubqCopy(const core::MIIterator &mit,
                                           [[maybe_unused]] core::SubSelectOptimizationType sot) {
  MEASURE_FET("SubSelectColumn::RoughPrepareSubqCopy(...)");
  tmp_tab_subq_ptr_->CreateTemplateIfNotExists();
  if ((!IsCorrelated() && if_first_eval_for_rough_) || (IsCorrelated() && (FeedArguments(mit, true)))) {
    if_out_of_date_rough_ = false;
    //		cache_.reset();
    //		no_cached_values_ = 0;
    tmp_tab_subq_ptr_->ResetToTemplate(true);
    tmp_tab_subq_ptr_->ResetVCStatistics();
    tmp_tab_subq_ptr_->SuspendDisplay();
    try {
      tmp_tab_subq_ptr_->RoughProcessParameters(mit, parent_tt_alias_);
    } catch (...) {
      tmp_tab_subq_ptr_->ResumeDisplay();
    }
    tmp_tab_subq_ptr_->ResumeDisplay();
  }
  tmp_tab_subq_ptr_->SuspendDisplay();
  try {
    tmp_tab_subq_ptr_->RoughMaterialize(true);
  } catch (...) {
  }
  tmp_tab_subq_ptr_->ResumeDisplay();
}

bool SubSelectColumn::IsCorrelated() const {
  if (var_map_.size() || params_.size())
    return true;
  return false;
}

bool SubSelectColumn::IsNullImpl(const core::MIIterator &mit) {
  PrepareSubqResult(mit, false);
  return tmp_tab_subq_ptr_->IsNull(0, col_idx_);
}

types::TianmuValueObject SubSelectColumn::GetValueImpl(const core::MIIterator &mit,
                                                       [[maybe_unused]] bool lookup_to_num) {
  PrepareSubqResult(mit, false);
  types::TianmuValueObject val = tmp_tab_subq_ptr_->GetValueObject(0, col_idx_);
  if (expected_type_.IsString())
    return val.ToBString();
  if (expected_type_.IsNumeric() && core::ATI::IsStringType(val.Type())) {
    types::TianmuNum tn;
    types::TianmuNum::Parse(*static_cast<types::BString *>(val.Get()), tn, expected_type_.GetTypeName());
    val = tn;
  }
  return val;
}

int64_t SubSelectColumn::GetValueInt64Impl(const core::MIIterator &mit) {
  PrepareSubqResult(mit, false);
  return tmp_tab_subq_ptr_->GetTable64(0, col_idx_);
}

core::RoughValue SubSelectColumn::RoughGetValue(const core::MIIterator &mit, core::SubSelectOptimizationType sot) {
  RoughPrepareSubqCopy(mit, sot);
  return core::RoughValue(tmp_tab_subq_ptr_->GetTable64(0, col_idx_), tmp_tab_subq_ptr_->GetTable64(1, col_idx_));
}

double SubSelectColumn::GetValueDoubleImpl(const core::MIIterator &mit) {
  PrepareSubqResult(mit, false);
  int64_t v = tmp_tab_subq_ptr_->GetTable64(0, col_idx_);
  return *((double *)(&v));
}

void SubSelectColumn::GetValueStringImpl(types::BString &s, core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  tmp_tab_subq_ptr_->GetTable_S(s, 0, col_idx_);
}

types::TianmuValueObject SubSelectColumn::GetSetMinImpl(core::MIIterator const &mit) {
  // assert: this->params_ are all set
  PrepareSubqResult(mit, false);
  if (!check_min_max_uptodate_)
    CalculateMinMax();
  return min_;
}

types::TianmuValueObject SubSelectColumn::GetSetMaxImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  if (!check_min_max_uptodate_)
    CalculateMinMax();
  return max_;
}
bool SubSelectColumn::CheckExists(core::MIIterator const &mit) {
  PrepareSubqResult(mit, true);  // true: exists_only
  return tmp_tab_subq_ptr_->NumOfObj() > 0;
}

bool SubSelectColumn::IsEmptyImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  return tmp_tab_subq_ptr_->NumOfObj() == 0;
}

common::Tribool SubSelectColumn::RoughIsEmpty(core::MIIterator const &mit, core::SubSelectOptimizationType sot) {
  RoughPrepareSubqCopy(mit, sot);
  return tmp_tab_subq_ptr_->RoughIsEmpty();
}

void SubSelectColumn::CalculateMinMax() {
  if (!tmp_tab_subq_ptr_->IsMaterialized())
    tmp_tab_subq_ptr_->Materialize();
  if (tmp_tab_subq_ptr_->NumOfObj() == 0) {
    min_ = max_ = types::TianmuValueObject();
    check_min_max_uptodate_ = true;
    return;
  }

  min_ = max_ = types::TianmuValueObject();
  bool found_not_null = false;
  if (types::RequiresUTFConversions(GetCollation()) && Type().IsString() && expected_type_.IsString()) {
    types::BString val_s, min_s, max_s;
    for (int64_t i = 0; i < tmp_tab_subq_ptr_->NumOfObj(); i++) {
      tmp_tab_subq_ptr_->GetTable_S(val_s, i, col_idx_);
      if (val_s.IsNull())
        continue;
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
    types::TianmuValueObject val;
    for (int64_t i = 0; i < tmp_tab_subq_ptr_->NumOfObj(); i++) {
      val = tmp_tab_subq_ptr_->GetValueObject(i, col_idx_);
      if (expected_type_.IsString()) {
        val = val.ToBString();
        static_cast<types::BString *>(val.Get())->MakePersistent();
      } else if (expected_type_.IsNumeric() && core::ATI::IsStringType(val.Type())) {
        types::TianmuNum tn;
        types::TianmuNum::Parse(*static_cast<types::BString *>(val.Get()), tn, expected_type_.GetTypeName());
        val = tn;
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
  check_min_max_uptodate_ = true;
}

bool SubSelectColumn::FeedArguments(const core::MIIterator &mit, bool for_rough) {
  MEASURE_FET("SubSelectColumn::FeedArguments(...)");

  bool diff;
  if (for_rough) {
    diff = if_first_eval_for_rough_;
    for (auto &iter : var_map_) {
      core::ValueOrNull v = iter.GetTabPtr()->GetComplexValue(mit[iter.dim], iter.col_ndx);
      auto cache = var_buf_for_rough_.find(iter.var_id);
      v.MakeStringOwner();
      if (cache->second.empty()) {  // empty if TIANMUexpression - feeding unnecessary
        bool ldiff = v != param_cache_for_rough_[iter.var_id];
        diff = diff || ldiff;
        if (ldiff)
          param_cache_for_rough_[iter.var_id] = v;
      } else {
        DEBUG_ASSERT(cache != var_buf_for_rough_.end());
        diff = diff || (v != cache->second.begin()->first);
        if (diff)
          for (auto &val_it : cache->second) *(val_it.second) = val_it.first = v;
      }
    }
    if_first_eval_for_rough_ = false;
  } else {
    diff = first_eval_;
    for (auto &iter : var_map_) {
      core::ValueOrNull v = iter.GetTabPtr()->GetComplexValue(mit[iter.dim], iter.col_ndx);
      auto cache = var_buf_for_exact_.find(iter.var_id);
      v.MakeStringOwner();
      if (cache->second.empty()) {  // empty if TIANMUexpression - feeding unnecessary
        bool ldiff = v != param_cache_for_exact_[iter.var_id];
        diff = diff || ldiff;
        if (ldiff)
          param_cache_for_exact_[iter.var_id] = v;
      } else {
        DEBUG_ASSERT(cache != var_buf_for_exact_.end());
        core::ValueOrNull v1 = *(cache->second.begin()->second);
        core::ValueOrNull v2 = (cache->second.begin()->first);
        diff = diff || (v != cache->second.begin()->first);
        if (diff)
          for (auto &val_it : cache->second) *(val_it.second) = val_it.first = v;
      }
    }
    first_eval_ = false;
  }

  if (diff) {
    check_min_max_uptodate_ = false;
    if_out_of_date_rough_ = true;
  }
  return diff;
}

void SubSelectColumn::SetExpectedTypeImpl(core::ColumnType const &ct) { expected_type_ = ct; }

bool SubSelectColumn::MakeParallelReady() {
  core::MIDummyIterator mit(multi_index_);
  PrepareSubqResult(mit, false);
  if (!tmp_tab_subq_ptr_->IsMaterialized())
    tmp_tab_subq_ptr_->Materialize();
  if (tmp_tab_subq_ptr_->NumOfObj() > tmp_tab_subq_ptr_->GetPageSize())
    return false;  // multipage Attrs - not thread safe
  // below assert doesn't take into account lazy field
  // NumOfMaterialized() tells how many rows in lazy mode are materialized
  DEBUG_ASSERT(tmp_tab_subq_ptr_->NumOfObj() == tmp_tab_subq_ptr_->NumOfMaterialized());
  PrepareAndFillCache();
  return true;
}

}  // namespace vcolumn
}  // namespace Tianmu
