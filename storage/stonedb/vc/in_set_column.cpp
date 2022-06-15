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

#include "in_set_column.h"

#include "const_column.h"
#include "core/compiled_query.h"
#include "core/mysql_expression.h"
#include "core/rc_attr.h"
#include "expr_column.h"

namespace stonedb {
namespace vcolumn {
InSetColumn::InSetColumn(core::ColumnType const &ct, core::MultiIndex *mind,
                         std::vector<VirtualColumn *> const &columns_)
    : MultiValColumn(ct, mind),
      columns(columns_),
      full_cache(false),
      cache(mind->NoPower()),
      expected_type(ct),
      last_mit(NULL),
      last_mit_size(0) {
  std::set<VarMap> uvms;
  for (auto &it : columns) {
    auto const &vm = it->GetVarMap();
    uvms.insert(vm.begin(), vm.end());
    core::MysqlExpression::sdbfields_cache_t const &sdbfields_cache = it->GetSDBItems();
    sdbitems.insert(sdbfields_cache.begin(), sdbfields_cache.end());
  }
  std::vector<VarMap>(uvms.begin(), uvms.end()).swap(var_map);
  std::set<int> dims;
  for (auto &it : var_map) dims.insert(it.dim);
  if (dims.size() == 1)
    dim = *(dims.begin());
  else
    dim = -1;

  is_const =
      std::find_if(columns.begin(), columns.end(), [](VirtualColumn *col) { return !col->IsConst(); }) == columns.end();
}

InSetColumn::InSetColumn(core::ColumnType const &ct, core::MultiIndex *mind, core::ValueSet &external_valset)
    : MultiValColumn(ct, mind), cache(external_valset), expected_type(ct), last_mit(NULL), last_mit_size(0) {
  dim = -1;
  is_const = true;
  full_cache = true;
  cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
}

InSetColumn::InSetColumn(const InSetColumn &c)
    : MultiValColumn(c),
      columns(c.columns),
      full_cache(c.full_cache),
      cache(c.cache),
      expected_type(c.expected_type),
      is_const(c.is_const),
      last_mit(c.last_mit),
      last_mit_size(c.last_mit_size) {}

InSetColumn::~InSetColumn() {}

bool InSetColumn::IsConst() const { return is_const; }

void InSetColumn::RequestEval(const core::MIIterator &mit, const int tta) {
  full_cache = false;
  cache.Clear();
  for (unsigned int i = 0; i < columns.size(); i++) columns[i]->RequestEval(mit, tta);
}

types::BString InSetColumn::DoGetMinString([[maybe_unused]] const core::MIIterator &mit) {
  types::BString s;
  DEBUG_ASSERT(!"To be implemented.");
  return s;
}

types::BString InSetColumn::DoGetMaxString([[maybe_unused]] const core::MIIterator &mit) {
  types::BString s;
  DEBUG_ASSERT(!"To be implemented.");
  return s;
}

size_t InSetColumn::DoMaxStringSize()  // maximal byte string length in column
{
  return ct.GetPrecision();
}

core::PackOntologicalStatus InSetColumn::DoGetPackOntologicalStatus([[maybe_unused]] const core::MIIterator &mit) {
  DEBUG_ASSERT(!"To be implemented.");
  return core::PackOntologicalStatus::NORMAL;
}

void InSetColumn::DoEvaluatePack([[maybe_unused]] core::MIUpdatingIterator &mit,
                                 [[maybe_unused]] core::Descriptor &desc) {
  DEBUG_ASSERT(!"To be implemented.");
  DEBUG_ASSERT(0);  // comparison of a const with a const should be simplified earlier
}

bool InSetColumn::IsSetEncoded(common::CT at, int scale) {
  return (cache.EasyMode() && scale == ct.GetScale() &&
          (at == expected_type.GetTypeName() ||
           (core::ATI::IsFixedNumericType(at) && core::ATI::IsFixedNumericType(expected_type.GetTypeName()))));
}

common::Tribool InSetColumn::DoContains64(const core::MIIterator &mit, int64_t val)  // easy case for numerics
{
  if (cache.EasyMode()) {
    common::Tribool contains = false;
    if (val == common::NULL_VALUE_64) return common::TRIBOOL_UNKNOWN;
    if (cache.Contains(val))
      contains = true;
    else if (cache.ContainsNulls())
      contains = common::TRIBOOL_UNKNOWN;
    return contains;
  }
  return DoContains(mit, types::RCNum(val, ct.GetScale()));
}

common::Tribool InSetColumn::DoContainsString(const core::MIIterator &mit,
                                              types::BString &val)  // easy case for numerics
{
  if (cache.EasyMode()) {
    common::Tribool contains = false;
    if (val.IsNull()) return common::TRIBOOL_UNKNOWN;
    if (cache.Contains(val))
      contains = true;
    else if (cache.ContainsNulls())
      contains = common::TRIBOOL_UNKNOWN;
    return contains;
  }
  return DoContains(mit, val);
}

common::Tribool InSetColumn::DoContains(const core::MIIterator &mit, const types::RCDataType &val) {
  common::Tribool contains = false;
  if (val.IsNull()) return common::TRIBOOL_UNKNOWN;
  if (IsConst()) {
    if (!full_cache) PrepareCache(mit);
    cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());

    if (cache.Contains(val, GetCollation()))
      contains = true;
    else if (cache.ContainsNulls())
      contains = common::TRIBOOL_UNKNOWN;
  } else {
    // TODO: remember mit value for the last cache creation and reuse cache if
    // possible.
    if (types::RequiresUTFConversions(GetCollation()) && Type().IsString()) {
      for (auto &it : columns) {
        types::BString s;
        it->GetValueString(s, mit);
        if (s.IsNull())
          contains = common::TRIBOOL_UNKNOWN;
        else {
          // ConvertToBinaryForm(s, buf, 0, GetCollation().collation, true);
          if (CollationStrCmp(GetCollation(), val.ToBString(), s) == 0) {
            contains = true;
            break;
          }
        }
      }
    } else {
      for (auto &it : columns) {
        if (it->IsNull(mit))
          contains = common::TRIBOOL_UNKNOWN;
        else if (val == *(it->GetValue(mit, false).Get())) {
          contains = true;
          break;
        }
      }
    }
  }
  return contains;
}

int64_t InSetColumn::DoNoValues([[maybe_unused]] core::MIIterator const &mit) {
  if (full_cache && is_const) return cache.NoVals();
  return columns.size();
}

bool InSetColumn::DoIsEmpty(core::MIIterator const &mit) { return DoAtLeastNoDistinctValues(mit, 1) == 0; }

void InSetColumn::PrepareCache(const core::MIIterator &mit, const int64_t &at_least) {
  // MEASURE_FET("InSetColumn::PrepareCache(...)");
  full_cache = false;
  cache.Clear();
  auto it(columns.begin());
  auto end(columns.end());
  if (types::RequiresUTFConversions(GetCollation()) && Type().IsString()) {
    core::ValueSet bin_cache(mit.GetPower());
    bin_cache.Prepare(expected_type.GetTypeName(), expected_type.GetScale(), expected_type.GetCollation());
    int bin_size = 0;
    int max_str_size = 0;
    for (; (it != end) && (bin_cache.NoVals() <= at_least); ++it) max_str_size += (*it)->MaxStringSize();
    it = columns.begin();
    types::BString buf(NULL, types::CollationBufLen(GetCollation(), max_str_size), true);
    for (; (it != end) && (bin_cache.NoVals() <= at_least); ++it) {
      types::BString s;
      (*it)->GetValueString(s, mit);
      ConvertToBinaryForm(s, buf, GetCollation());
      bin_size = bin_cache.NoVals();
      bin_cache.Add(buf);
      if (bin_size < bin_cache.NoVals()) cache.Add(s);
    }
  } else {
    for (; (it != end) && (cache.NoVals() <= at_least); ++it) cache.Add((*it)->GetValue(mit));
  }
  if ((at_least == common::PLUS_INF_64) || (it == end)) full_cache = true;
}

int64_t InSetColumn::DoAtLeastNoDistinctValues(const core::MIIterator &mit, int64_t const at_least) {
  DEBUG_ASSERT(at_least > 0);
  if (!full_cache || !is_const) PrepareCache(mit, at_least);
  return cache.NoVals();
}

bool InSetColumn::DoContainsNull(const core::MIIterator &mit) {
  if (!full_cache || !is_const) PrepareCache(mit);
  cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
  return cache.ContainsNulls();
}

std::unique_ptr<MultiValColumn::IteratorInterface> InSetColumn::DoBegin(core::MIIterator const &mit) {
  if (!full_cache || !is_const) PrepareCache(mit);
  cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
  return std::unique_ptr<MultiValColumn::IteratorInterface>(new IteratorImpl(cache.begin()));
}

std::unique_ptr<MultiValColumn::IteratorInterface> InSetColumn::DoEnd(core::MIIterator const &mit) {
  if (!full_cache || !is_const) PrepareCache(mit);
  cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
  return std::unique_ptr<MultiValColumn::IteratorInterface>(new IteratorImpl(cache.end()));
}

types::RCValueObject InSetColumn::DoGetSetMin(core::MIIterator const &mit) {
  if (!full_cache || !is_const) PrepareCache(mit);
  cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
  return types::RCValueObject(*cache.Min());
}

types::RCValueObject InSetColumn::DoGetSetMax(core::MIIterator const &mit) {
  if (!full_cache || !is_const) PrepareCache(mit);
  cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
  return types::RCValueObject(*cache.Max());
}

void InSetColumn::DoSetExpectedType(core::ColumnType const &ct) { expected_type = ct; }

char *InSetColumn::ToString(char p_buf[], size_t buf_ct) const {
  if (full_cache && is_const) {
    int64_t no_vals = cache.NoVals();
    std::snprintf(p_buf, buf_ct, "%ld vals", no_vals);
  }
  return p_buf;
}

void InSetColumn::LockSourcePacks(const core::MIIterator &mit) {
  for (unsigned int i = 0; i < columns.size(); i++) {
    columns[i]->LockSourcePacks(mit);
  }
}

bool InSetColumn::CanCopy() const {
  for (unsigned int i = 0; i < columns.size(); i++) {
    if (!columns[i]->CanCopy()) return false;
  }
  return true;
}
bool InSetColumn::DoCopyCond(const core::MIIterator &mit, types::CondArray &condition,
                             [[maybe_unused]] DTCollation coll) {
  bool success = true;
  cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
  if (types::RequiresUTFConversions(GetCollation()) && Type().IsString()) {
    for (auto &it : columns) {
      types::BString s;
      it->GetValueString(s, mit);
      if (s.IsNull()) {
        STONEDB_LOG(LogCtl_Level::WARN, "DoCopyCond condition is NULL");
        condition.clear();
        success = false;
        break;
      } else {
        condition.push_back(s);
      }
    }
  } else {
    success = false;
  }
  return success;
}

bool InSetColumn::DoCopyCond([[maybe_unused]] const core::MIIterator &mit, std::shared_ptr<utils::Hash64> &condition,
                             DTCollation coll) {
  cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
  return cache.CopyCondition(expected_type.GetTypeName(), condition, coll);
}

bool InSetColumn::PrepareValueSet(const core::MIIterator &mit) {
  if (!full_cache || !is_const) PrepareCache(mit);
  cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
  return true;
}
}  // namespace vcolumn
}  // namespace stonedb
