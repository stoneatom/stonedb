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

SubSelectColumn::SubSelectColumn(core::TempTable *subq, core::MultiIndex *mind, core::TempTable *temp_table,
                                 int temp_table_alias)
    : MultiValColumn(subq->GetColumnType(0), mind),
      subq(std::static_pointer_cast<core::TempTableForSubquery>(subq->shared_from_this())),
      parent_tt_alias(temp_table_alias),
      min_max_uptodate(false),
      no_cached_values(0) {
  const std::vector<core::JustATable *> *tables = &temp_table->GetTables();
  const std::vector<int> *aliases = &temp_table->GetAliases();

  col_idx = 0;
  for (uint i = 0; i < subq->NumOfAttrs(); i++)
    if (subq->IsDisplayAttr(i)) {
      col_idx = i;
      break;
    }
  ct = subq->GetColumnType(col_idx);
  core::MysqlExpression::SetOfVars all_params;

  for (uint i = 0; i < subq->NumOfVirtColumns(); i++) {
    core::MysqlExpression::SetOfVars params = subq->GetVirtualColumn(i)->GetParams();
    all_params.insert(params.begin(), params.end());
    core::MysqlExpression::sdbfields_cache_t sdbfields = subq->GetVirtualColumn(i)->GetSDBItems();
    if (!sdbfields.empty()) {
      for (auto &sdbfield : sdbfields) {
        auto sdbitem = sdbitems.find(sdbfield.first);
        if (sdbitem == sdbitems.end())
          sdbitems.insert(sdbfield);
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

      var_map.push_back(VarMap(iter, (*tables)[ndx], ndx));
      var_types[iter] = (*tables)[ndx]->GetColumnType(var_map[var_map.size() - 1].col_ndx);
      var_buf_for_exact[iter] = std::vector<core::MysqlExpression::value_or_null_info_t>();  // now empty,
                                                                                             // pointers
      // inserted by SetBufs()
    } else if (iter.tab == temp_table_alias) {
      var_map.push_back(VarMap(iter, temp_table, 0));
      var_types[iter] = temp_table->GetColumnType(var_map[var_map.size() - 1].col_ndx);
      var_buf_for_exact[iter] = std::vector<core::MysqlExpression::value_or_null_info_t>();  // now empty,
                                                                                             // pointers
      // inserted by SetBufs()
    } else {
      // parameter
      params.insert(iter);
    }
  }
  SetBufs(&var_buf_for_exact);
  var_buf_for_rough = var_buf_for_exact;

  if (var_map.size() == 1)
    dim = var_map[0].dim;
  else
    dim = -1;
  first_eval_for_rough = true;
  out_of_date_rough = true;
}

SubSelectColumn::SubSelectColumn(const SubSelectColumn &c)
    : MultiValColumn(c),
      col_idx(c.col_idx),
      subq(c.subq),
      min(c.min),
      max(c.max),
      min_max_uptodate(c.min_max_uptodate),
      expected_type_(c.expected_type_),
      var_buf_for_exact(),
      no_cached_values(c.no_cached_values),
      out_of_date_rough(c.out_of_date_rough) {
  var_types.empty();
  if (c.cache.get()) {
    cache = std::make_shared<core::ValueSet>(*c.cache.get());
    // core::ValueSet* vs = new core::ValueSet(*c.cache.get());
    // cache = std::shared_ptr<core::ValueSet>(vs);
  }
}

SubSelectColumn::~SubSelectColumn() {
  //	if(!!table)
  //			table->TranslateBackVCs();
  //
  //	if(!!table_for_rough)
  //		table_for_rough->TranslateBackVCs();
  //	if(table != subq) {
  //		table.reset();
  //		table_for_rough.reset();
  //	}
}

void SubSelectColumn::SetBufs(core::MysqlExpression::var_buf_t *bufs) {
  DEBUG_ASSERT(bufs);
  for (auto &it : sdbitems) {
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
  VirtualColumn *vc = subq->GetAttrP(col_idx)->term.vc;
  if (vc->IsFullConst() && vc->IsNull(core::MIIterator(NULL, mind->ValueOfPower()))) return common::TRIBOOL_UNKNOWN;

  PrepareSubqResult(mit, false);
  common::Tribool res = false;
  if (!cache) {
    cache = std::shared_ptr<core::ValueSet>(new core::ValueSet(mind->ValueOfPower()));
    cache->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
  }

  if (cache->Contains(v, GetCollation()))
    return true;
  else if (cache->ContainsNulls())
    res = common::TRIBOOL_UNKNOWN;

  bool added_new_value = false;
  if (types::RequiresUTFConversions(GetCollation()) && Type().IsString()) {
    for (int64_t i = no_cached_values; i < subq->NumOfObj(); i++) {
      no_cached_values++;
      added_new_value = true;
      if (subq->IsNull(i, col_idx)) {
        cache->AddNull();
        res = common::TRIBOOL_UNKNOWN;
      } else {
        types::BString val;
        subq->GetTableString(val, i, col_idx);
        val.MakePersistent();
        cache->Add(val);
        if (CollationStrCmp(GetCollation(), val, v.ToBString()) == 0) {
          res = true;
          break;
        }
      }
    }
  } else {
    for (int64_t i = no_cached_values; i < subq->NumOfObj(); i++) {
      no_cached_values++;
      added_new_value = true;
      if (subq->IsNull(i, col_idx)) {
        cache->AddNull();
        res = common::TRIBOOL_UNKNOWN;
      } else {
        types::RCValueObject value;
        if (Type().IsString()) {
          types::BString s;
          subq->GetTableString(s, i, col_idx);
          value = s;
          static_cast<types::BString *>(value.Get())->MakePersistent();
        } else
          value = subq->GetValueObject(i, col_idx);
        cache->Add(value);
        if (value == v) {
          res = true;
          break;
        }
      }
    }
  }
  if (added_new_value && no_cached_values == subq->NumOfObj())
    cache->ForcePreparation();  // completed for the first time => recalculate
                                // cache
  return res;
}

void SubSelectColumn::PrepareAndFillCache() {
  if (!cache) {
    cache = std::shared_ptr<core::ValueSet>(new core::ValueSet(mind->ValueOfPower()));
    cache->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
  }
  for (int64_t i = no_cached_values; i < subq->NumOfObj(); i++) {
    no_cached_values++;
    if (subq->IsNull(i, col_idx)) {
      cache->AddNull();
    } else {
      types::RCValueObject value;
      if (Type().IsString()) {
        types::BString s;
        subq->GetTableString(s, i, col_idx);
        value = s;
        static_cast<types::BString *>(value.Get())->MakePersistent();
      } else
        value = subq->GetValueObject(i, col_idx);
      cache->Add(value);
    }
  }
  cache->ForcePreparation();
  cache->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
}

bool SubSelectColumn::IsSetEncoded(common::CT at,
                                   int scale)  // checks whether the set is constant and fixed size equal to
                                               // the given one
{
  if (!cache || !cache->EasyMode() || !subq->IsMaterialized() || no_cached_values < subq->NumOfObj()) return false;
  return (scale == ct.GetScale() &&
          (at == expected_type_.GetTypeName() ||
           (core::ATI::IsFixedNumericType(at) && core::ATI::IsFixedNumericType(expected_type_.GetTypeName()))));
}

common::Tribool SubSelectColumn::Contains64Impl(const core::MIIterator &mit, int64_t val)  // easy case for integers
{
  if (cache && cache->EasyMode()) {
    common::Tribool contains = false;
    if (val == common::NULL_VALUE_64) return common::TRIBOOL_UNKNOWN;
    if (cache->Contains(val))
      contains = true;
    else if (cache->ContainsNulls())
      contains = common::TRIBOOL_UNKNOWN;
    return contains;
  }
  return ContainsImpl(mit, types::RCNum(val, ct.GetScale()));
}

common::Tribool SubSelectColumn::ContainsStringImpl(const core::MIIterator &mit,
                                                    types::BString &val)  // easy case for strings
{
  if (cache && cache->EasyMode()) {
    common::Tribool contains = false;
    if (val.IsNull()) return common::TRIBOOL_UNKNOWN;
    if (cache->Contains(val))
      contains = true;
    else if (cache->ContainsNulls())
      contains = common::TRIBOOL_UNKNOWN;
    return contains;
  }
  return ContainsImpl(mit, val);
}

int64_t SubSelectColumn::NumOfValuesImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  if (!subq->IsMaterialized()) subq->Materialize();
  return subq->NumOfObj();
}

int64_t SubSelectColumn::AtLeastNoDistinctValuesImpl(core::MIIterator const &mit, int64_t const at_least) {
  DEBUG_ASSERT(at_least > 0);
  PrepareSubqResult(mit, false);
  core::ValueSet vals(mind->ValueOfPower());
  vals.Prepare(expected_type_.GetTypeName(), expected_type_.GetScale(), expected_type_.GetCollation());

  if (types::RequiresUTFConversions(GetCollation()) && Type().IsString()) {
    types::BString buf(NULL, types::CollationBufLen(GetCollation(), subq->MaxStringSize(col_idx)), true);
    for (int64_t i = 0; vals.NoVals() < at_least && i < subq->NumOfObj(); i++) {
      if (!subq->IsNull(i, col_idx)) {
        types::BString s;
        subq->GetTable_S(s, i, col_idx);
        ConvertToBinaryForm(s, buf, GetCollation());
        vals.Add(buf);
      }
    }
  } else {
    for (int64_t i = 0; vals.NoVals() < at_least && i < subq->NumOfObj(); i++) {
      if (!subq->IsNull(i, col_idx)) {
        types::RCValueObject val = subq->GetValueObject(i, col_idx);
        vals.Add(val);
      }
    }
  }

  return vals.NoVals();
}

bool SubSelectColumn::ContainsNullImpl(const core::MIIterator &mit) {
  PrepareSubqResult(mit, false);
  for (int64_t i = 0; i < subq->NumOfObj(); ++i)
    if (subq->IsNull(i, col_idx)) return true;
  return false;
}

std::unique_ptr<MultiValColumn::IteratorInterface> SubSelectColumn::BeginImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  return std::unique_ptr<MultiValColumn::IteratorInterface>(new IteratorImpl(subq->begin(), expected_type_));
}

std::unique_ptr<MultiValColumn::IteratorInterface> SubSelectColumn::EndImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  return std::unique_ptr<MultiValColumn::IteratorInterface>(new IteratorImpl(subq->end(), expected_type_));
}

void SubSelectColumn::RequestEval(const core::MIIterator &mit, const int tta) {
  first_eval = true;
  first_eval_for_rough = true;
  for (uint i = 0; i < subq->NumOfVirtColumns(); i++) subq->GetVirtualColumn(i)->RequestEval(mit, tta);
}

void SubSelectColumn::PrepareSubqResult(const core::MIIterator &mit, bool exists_only) {
  MEASURE_FET("SubSelectColumn::PrepareSubqCopy(...)");
  bool cor = IsCorrelated();
  if (!cor) {
    if (subq->IsFullyMaterialized()) return;
  }

  subq->CreateTemplateIfNotExists();

  if (cor && (FeedArguments(mit, false))) {
    cache.reset();
    no_cached_values = 0;
    subq->ResetToTemplate(false);
    subq->ResetVCStatistics();
    subq->SuspendDisplay();
    try {
      subq->ProcessParameters(mit,
                              parent_tt_alias);  // exists_only is not needed
                                                 // here (limit already set)
    } catch (...) {
      subq->ResumeDisplay();
      throw;
    }
    subq->ResumeDisplay();
  }
  subq->SuspendDisplay();
  try {
    if (exists_only) subq->SetMode(core::TMParameter::TM_EXISTS);
    subq->Materialize(cor);
  } catch (...) {
    subq->ResumeDisplay();
    // subq->ResetForSubq();
    throw;
  }
  subq->ResumeDisplay();
}

void SubSelectColumn::RoughPrepareSubqCopy(const core::MIIterator &mit,
                                           [[maybe_unused]] core::SubSelectOptimizationType sot) {
  MEASURE_FET("SubSelectColumn::RoughPrepareSubqCopy(...)");
  subq->CreateTemplateIfNotExists();
  if ((!IsCorrelated() && first_eval_for_rough) || (IsCorrelated() && (FeedArguments(mit, true)))) {
    out_of_date_rough = false;
    //		cache.reset();
    //		no_cached_values = 0;
    subq->ResetToTemplate(true);
    subq->ResetVCStatistics();
    subq->SuspendDisplay();
    try {
      subq->RoughProcessParameters(mit, parent_tt_alias);
    } catch (...) {
      subq->ResumeDisplay();
    }
    subq->ResumeDisplay();
  }
  subq->SuspendDisplay();
  try {
    subq->RoughMaterialize(true);
  } catch (...) {
  }
  subq->ResumeDisplay();
}

bool SubSelectColumn::IsCorrelated() const {
  if (var_map.size() || params.size()) return true;
  return false;
}

bool SubSelectColumn::IsNullImpl(const core::MIIterator &mit) {
  PrepareSubqResult(mit, false);
  return subq->IsNull(0, col_idx);
}

types::RCValueObject SubSelectColumn::GetValueImpl(const core::MIIterator &mit, [[maybe_unused]] bool lookup_to_num) {
  PrepareSubqResult(mit, false);
  types::RCValueObject val = subq->GetValueObject(0, col_idx);
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
  return subq->GetTable64(0, col_idx);
}

core::RoughValue SubSelectColumn::RoughGetValue(const core::MIIterator &mit, core::SubSelectOptimizationType sot) {
  RoughPrepareSubqCopy(mit, sot);
  return core::RoughValue(subq->GetTable64(0, col_idx), subq->GetTable64(1, col_idx));
}

double SubSelectColumn::GetValueDoubleImpl(const core::MIIterator &mit) {
  PrepareSubqResult(mit, false);
  int64_t v = subq->GetTable64(0, col_idx);
  return *((double *)(&v));
}

void SubSelectColumn::GetValueStringImpl(types::BString &s, core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  subq->GetTable_S(s, 0, col_idx);
}

types::RCValueObject SubSelectColumn::GetSetMinImpl(core::MIIterator const &mit) {
  // assert: this->params are all set
  PrepareSubqResult(mit, false);
  if (!min_max_uptodate) CalculateMinMax();
  return min;
}

types::RCValueObject SubSelectColumn::GetSetMaxImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  if (!min_max_uptodate) CalculateMinMax();
  return max;
}
bool SubSelectColumn::CheckExists(core::MIIterator const &mit) {
  PrepareSubqResult(mit, true);  // true: exists_only
  return subq->NumOfObj() > 0;
}

bool SubSelectColumn::IsEmptyImpl(core::MIIterator const &mit) {
  PrepareSubqResult(mit, false);
  return subq->NumOfObj() == 0;
}

common::Tribool SubSelectColumn::RoughIsEmpty(core::MIIterator const &mit, core::SubSelectOptimizationType sot) {
  RoughPrepareSubqCopy(mit, sot);
  return subq->RoughIsEmpty();
}

void SubSelectColumn::CalculateMinMax() {
  if (!subq->IsMaterialized()) subq->Materialize();
  if (subq->NumOfObj() == 0) {
    min = max = types::RCValueObject();
    min_max_uptodate = true;
    return;
  }

  min = max = types::RCValueObject();
  bool found_not_null = false;
  if (types::RequiresUTFConversions(GetCollation()) && Type().IsString() && expected_type_.IsString()) {
    types::BString val_s, min_s, max_s;
    for (int64_t i = 0; i < subq->NumOfObj(); i++) {
      subq->GetTable_S(val_s, i, col_idx);
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
    min = min_s;
    max = max_s;
  } else {
    types::RCValueObject val;
    for (int64_t i = 0; i < subq->NumOfObj(); i++) {
      val = subq->GetValueObject(i, col_idx);
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
        min = max = val;
        continue;
      }
      if (val > max) {
        max = val;
      } else if (val < min) {
        min = val;
      }
    }
  }
  min_max_uptodate = true;
}

bool SubSelectColumn::FeedArguments(const core::MIIterator &mit, bool for_rough) {
  MEASURE_FET("SubSelectColumn::FeedArguments(...)");

  bool diff;
  if (for_rough) {
    diff = first_eval_for_rough;
    for (auto &iter : var_map) {
      core::ValueOrNull v = iter.GetTabPtr()->GetComplexValue(mit[iter.dim], iter.col_ndx);
      auto cache = var_buf_for_rough.find(iter.var);
      v.MakeStringOwner();
      if (cache->second.empty()) {  // empty if STONEDBexpression - feeding unnecessary
        bool ldiff = v != param_cache_for_rough[iter.var];
        diff = diff || ldiff;
        if (ldiff) param_cache_for_rough[iter.var] = v;
      } else {
        DEBUG_ASSERT(cache != var_buf_for_rough.end());
        diff = diff || (v != cache->second.begin()->first);
        if (diff)
          for (auto &val_it : cache->second) *(val_it.second) = val_it.first = v;
      }
    }
    first_eval_for_rough = false;
  } else {
    diff = first_eval;
    for (auto &iter : var_map) {
      core::ValueOrNull v = iter.GetTabPtr()->GetComplexValue(mit[iter.dim], iter.col_ndx);
      auto cache = var_buf_for_exact.find(iter.var);
      v.MakeStringOwner();
      if (cache->second.empty()) {  // empty if STONEDBexpression - feeding unnecessary
        bool ldiff = v != param_cache_for_exact[iter.var];
        diff = diff || ldiff;
        if (ldiff) param_cache_for_exact[iter.var] = v;
      } else {
        DEBUG_ASSERT(cache != var_buf_for_exact.end());
        core::ValueOrNull v1 = *(cache->second.begin()->second);
        core::ValueOrNull v2 = (cache->second.begin()->first);
        diff = diff || (v != cache->second.begin()->first);
        if (diff)
          for (auto &val_it : cache->second) *(val_it.second) = val_it.first = v;
      }
    }
    first_eval = false;
  }

  if (diff) {
    min_max_uptodate = false;
    out_of_date_rough = true;
  }
  return diff;
}

void SubSelectColumn::SetExpectedTypeImpl(core::ColumnType const &ct) { expected_type_ = ct; }

bool SubSelectColumn::MakeParallelReady() {
  core::MIDummyIterator mit(mind);
  PrepareSubqResult(mit, false);
  if (!subq->IsMaterialized()) subq->Materialize();
  if (subq->NumOfObj() > subq->GetPageSize()) return false;  // multipage Attrs - not thread safe
  // below assert doesn't take into account lazy field
  // NumOfMaterialized() tells how many rows in lazy mode are materialized
  DEBUG_ASSERT(subq->NumOfObj() == subq->NumOfMaterialized());
  PrepareAndFillCache();
  return true;
}

}  // namespace vcolumn
}  // namespace stonedb
