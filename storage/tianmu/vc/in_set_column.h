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
#ifndef TIANMU_VC_IN_SET_COLUMN_H_
#define TIANMU_VC_IN_SET_COLUMN_H_
#pragma once

#include "core/mi_updating_iterator.h"
#include "core/pack_guardian.h"
#include "core/temp_table.h"
#include "core/value_set.h"
#include "vc/multi_value_column.h"

namespace Tianmu {
namespace core {
class MysqlExpression;
}  // namespace core

namespace vcolumn {

/*! \brief A column defined by a set of expressions like in IN operator.
 * InSetColumn is associated with an core::MultiIndex object and cannot exist
 * without it. Values contained in InSetColumn object may not exist physically,
 * they can be computed on demand.
 */
class InSetColumn : public MultiValColumn {
 public:
  class IteratorImpl : public MultiValColumn::IteratorInterface {
   protected:
    explicit IteratorImpl(core::ValueSet::const_iterator const &it_) : it(it_) {}
    virtual ~IteratorImpl() {}
    void DoNext() override { ++it; }
    std::unique_ptr<LazyValueInterface> DoGetValue() override {
      return std::unique_ptr<LazyValueInterface>(new LazyValueImpl(*it));
    }
    bool DoNotEq(IteratorInterface const *it_) const override {
      return it != static_cast<IteratorImpl const *>(it_)->it;
    }
    std::unique_ptr<IteratorInterface> DoClone() const override {
      return std::unique_ptr<IteratorInterface>(new IteratorImpl(*this));
    }
    friend class InSetColumn;
    core::ValueSet::const_iterator it;
  };

  class LazyValueImpl : public LazyValueInterface {
    explicit LazyValueImpl(types::TianmuDataType const *v) : value(v) {}

   public:
    std::unique_ptr<LazyValueInterface> DoClone() const override {
      return std::unique_ptr<LazyValueInterface>(new LazyValueImpl(value));
    }
    types::BString DoGetString() const override {
      DEBUG_ASSERT(dynamic_cast<types::BString const *>(value));
      return *static_cast<types::BString const *>(value);
    }
    types::TianmuNum DoGetRCNum() const override {
      DEBUG_ASSERT(dynamic_cast<types::TianmuNum const *>(value));
      return *static_cast<types::TianmuNum const *>(value);
    }
    types::TianmuValueObject DoGetValue() const override { return *value; }
    int64_t DoGetInt64() const override {
      DEBUG_ASSERT(dynamic_cast<types::TianmuNum const *>(value));
      return static_cast<types::TianmuNum const *>(value)->ValueInt();
    }
    bool DoIsNull() const override { return value->IsNull(); }
    virtual ~LazyValueImpl() {}

   private:
    types::TianmuDataType const *value;
    friend class IteratorImpl;
  };

  InSetColumn(const core::ColumnType &ct, core::MultiIndex *multi_index, const std::vector<VirtualColumn *> &columns);
  InSetColumn(core::ColumnType const &ct, core::MultiIndex *multi_index,
              core::ValueSet &external_valset);  // a special version for sets of constants
  InSetColumn(const InSetColumn &c);

  virtual ~InSetColumn();
  bool IsSetEncoded(common::ColumnType at,
                    int scale) override;  // checks whether the set is constant and fixed size
                                          // equal to the given one
  bool IsConst() const override;
  bool IsInSet() const override { return true; }
  char *ToString(char p_buf[], size_t buf_ct) const override;
  void RequestEval(const core::MIIterator &mit, const int tta) override;
  core::ColumnType &GetExpectedType() { return expected_type_; }
  void LockSourcePacks(const core::MIIterator &mit) override;
  void LockSourcePacks(const core::MIIterator &mit, int);
  bool CanCopy() const override;
  std::vector<VirtualColumn *> GetChildren() const override { return columns; }
  std::vector<VirtualColumn *> columns;
  virtual bool PrepareValueSet(const core::MIIterator &mit);

 protected:
  int64_t NumOfValuesImpl(core::MIIterator const &mit) override;
  int64_t AtLeastNoDistinctValuesImpl(const core::MIIterator &mit, int64_t const at_least) override;
  bool ContainsNullImpl(const core::MIIterator &mit) override;
  types::TianmuValueObject GetSetMinImpl(const core::MIIterator &mit) override;
  types::TianmuValueObject GetSetMaxImpl(const core::MIIterator &mit) override;
  std::unique_ptr<IteratorInterface> BeginImpl(const core::MIIterator &) override;
  std::unique_ptr<IteratorInterface> EndImpl(const core::MIIterator &) override;
  common::Tribool ContainsImpl(const core::MIIterator &, const types::TianmuDataType &) override;
  common::Tribool Contains64Impl(const core::MIIterator &,
                                 int64_t) override;  // easy case for integers
  common::Tribool ContainsStringImpl(const core::MIIterator &,
                                     types::BString &) override;  // easy case for encoded strings
  void SetExpectedTypeImpl(const core::ColumnType &) override;
  bool IsEmptyImpl(const core::MIIterator &) override;

  bool IsNullsPossibleImpl([[maybe_unused]] bool val_nulls_possible) override {
    DEBUG_ASSERT(!"To be implemented.");
    return true;
  }

  int64_t GetMinInt64Impl([[maybe_unused]] const core::MIIterator &mit) override {
    // DEBUG_ASSERT( !"To be implemented." );
    return common::MINUS_INF_64;
  }

  int64_t GetMaxInt64Impl([[maybe_unused]] const core::MIIterator &mit) override {
    // DEBUG_ASSERT( !"To be implemented." );
    return common::PLUS_INF_64;
  }

  int64_t RoughMinImpl() override {
    // DEBUG_ASSERT( !"To be implemented." );
    return common::MINUS_INF_64;
  }

  int64_t RoughMaxImpl() override {
    // DEBUG_ASSERT( !"To be implemented." );
    return common::PLUS_INF_64;
  }

  types::BString GetMaxStringImpl(const core::MIIterator &mit) override;
  types::BString GetMinStringImpl(const core::MIIterator &mit) override;

  bool IsDistinctImpl() override {
    DEBUG_ASSERT(!"To be implemented.");
    return (false);
  }

  size_t MaxStringSizeImpl() override;
  core::PackOntologicalStatus GetPackOntologicalStatusImpl(const core::MIIterator &mit) override;
  void EvaluatePackImpl(core::MIUpdatingIterator &mit, core::Descriptor &desc) override;
  virtual common::ErrorCode EvaluateOnIndexImpl([[maybe_unused]] core::MIUpdatingIterator &mit,
                                                [[maybe_unused]] core::Descriptor &desc,
                                                [[maybe_unused]] int64_t limit) override {
    TIANMU_ERROR("To be implemented.");
    return common::ErrorCode::FAILED;
  }

  const core::MysqlExpression::tianmu_fields_cache_t &GetTIANMUItems() const override { return tianmu_items_; }
  bool CopyCondImpl(const core::MIIterator &mit, types::CondArray &condition, DTCollation coll) override;
  bool CopyCondImpl(const core::MIIterator &mit, std::shared_ptr<utils::Hash64> &condition, DTCollation coll) override;

 private:
  mutable bool full_cache_;
  mutable core::ValueSet cache_;
  core::ColumnType expected_type_;
  bool is_const_;
  char *last_mit_;     // to be used for tracing cache_ creation for mit values
  int last_mit_size_;  // to be used for tracing cache_ creation for mit values
  void PrepareCache(const core::MIIterator &mit, const int64_t &at_least = common::PLUS_INF_64);
};

}  // namespace vcolumn
}  // namespace Tianmu

#endif  // TIANMU_VC_IN_SET_COLUMN_H_
