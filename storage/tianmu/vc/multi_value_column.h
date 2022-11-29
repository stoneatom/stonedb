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
#ifndef TIANMU_VC_MULTI_VALUE_COLUMN_H_
#define TIANMU_VC_MULTI_VALUE_COLUMN_H_
#pragma once

#include "core/mi_updating_iterator.h"
#include "core/pack_guardian.h"
#include "types/tianmu_num.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
class MysqlExpression;
}

namespace vcolumn {
/*! \brief A column defined by a set of expressions like in IN operator.
 * MultiValColumn is associated with an core::MultiIndex object and cannot exist
 * without it. Values contained in MultiValColumn object may not exist
 * physically, they can be computed on demand.
 */
class MultiValColumn : public VirtualColumn {
 public:
  class LazyValueInterface {
   public:
    virtual ~LazyValueInterface() {}
    inline std::unique_ptr<LazyValueInterface> Clone() const { return DoClone(); }
    inline types::BString GetString() const { return DoGetString(); }
    inline types::TianmuNum GetRCNum() const { return DoGetRCNum(); }
    inline types::TianmuValueObject GetValue() const { return DoGetValue(); }
    inline int64_t GetInt64() const { return DoGetInt64(); }
    inline bool IsNull() const { return DoIsNull(); }

   private:
    virtual std::unique_ptr<LazyValueInterface> DoClone() const = 0;
    virtual types::BString DoGetString() const = 0;
    virtual types::TianmuNum DoGetRCNum() const = 0;
    virtual types::TianmuValueObject DoGetValue() const = 0;
    virtual int64_t DoGetInt64() const = 0;
    virtual bool DoIsNull() const = 0;
  };

  class Iterator;
  class LazyValue {
   public:
    LazyValue(LazyValue const &lv) : impl(lv.impl.get() ? lv.impl->Clone() : std::unique_ptr<LazyValueInterface>()) {}
    LazyValue &operator=(LazyValue const &lv) {
      if (&lv != this) {
        if (lv.impl.get())
          impl = lv.impl->Clone();
        else
          impl.reset();
      }
      return *this;
    }

    LazyValue *operator->() { return this; }
    types::BString GetString() { return impl->GetString(); }
    types::TianmuNum GetRCNum() { return impl->GetRCNum(); }
    types::TianmuValueObject GetValue() { return impl->GetValue(); }
    int64_t GetInt64() { return impl->GetInt64(); }
    bool IsNull() { return impl->IsNull(); }

   private:
    std::unique_ptr<LazyValueInterface> impl;
    LazyValue(std::unique_ptr<LazyValueInterface> impl_) : impl(std::move(impl_)) {}
    friend class Iterator;
  };

  class IteratorInterface {
   public:
    virtual ~IteratorInterface() {}
    inline void Next() { DoNext(); }
    inline std::unique_ptr<LazyValueInterface> GetValue() { return DoGetValue(); }
    inline bool NotEq(IteratorInterface const *it) const { return DoNotEq(it); }
    std::unique_ptr<IteratorInterface> clone() const { return DoClone(); }

   private:
    virtual std::unique_ptr<IteratorInterface> DoClone() const = 0;
    virtual void DoNext() = 0;
    virtual std::unique_ptr<LazyValueInterface> DoGetValue() = 0;
    virtual bool DoNotEq(IteratorInterface const *) const = 0;
  };

  class Iterator {
   public:
    Iterator(Iterator const &it)
        : owner(it.owner), impl(it.impl.get() ? it.impl->clone() : std::unique_ptr<IteratorInterface>()) {}
    Iterator &operator=(Iterator const &i) {
      if (&i != this) {
        if (i.impl.get())
          impl = i.impl->clone();
        else
          impl.reset();
      }
      return *this;
    }
    Iterator &operator++() {
      impl->Next();
      return *this;
    }
    LazyValue operator*() { return LazyValue(impl->GetValue()); }
    LazyValue operator->() { return LazyValue(impl->GetValue()); }
    bool operator!=(Iterator const &it) const {
      DEBUG_ASSERT(owner == it.owner);
      return (impl->NotEq(it.impl.get()));
    }

   private:
    MultiValColumn const *owner;
    std::unique_ptr<IteratorInterface> impl;
    Iterator(MultiValColumn const *owner_, std::unique_ptr<IteratorInterface> impl_)
        : owner(owner_), impl(std::move(impl_)) {}
    friend class MultiValColumn;
  };

  /*! \brief Create a column that represent const value.
   *
   * \param ct - type of column.
   * \param multi_index - core::MultiIndex.
   * \param expressions - a STL container of core::MysqlExpression*.
   */
  MultiValColumn(core::ColumnType const &ct, core::MultiIndex *multi_index) : VirtualColumn(ct, multi_index) {}
  MultiValColumn(const MultiValColumn &c) : VirtualColumn(c) { tianmu_items_ = c.tianmu_items_; }
  virtual ~MultiValColumn() {}

  bool IsMultival() const override { return true; }
  inline common::Tribool Contains64(core::MIIterator const &mit, int64_t val) { return Contains64Impl(mit, val); }
  inline common::Tribool ContainsString(core::MIIterator const &mit, types::BString &val) {
    return ContainsStringImpl(mit, val);
  }
  inline common::Tribool Contains(core::MIIterator const &mit, types::TianmuDataType const &val) {
    return (ContainsImpl(mit, val));
  }
  virtual bool IsSetEncoded([[maybe_unused]] common::ColumnType at, [[maybe_unused]] int scale) {
    return false;
  }  // checks whether the set is constant and fixed size equal to the given one
  inline bool IsEmpty(core::MIIterator const &mit) { return (IsEmptyImpl(mit)); }
  inline int64_t NumOfValues(core::MIIterator const &mit) { return NumOfValuesImpl(mit); }
  inline int64_t AtLeastNoDistinctValues(core::MIIterator const &mit, int64_t const at_least) {
    return AtLeastNoDistinctValuesImpl(mit, at_least);
  }
  inline bool ContainsNull(const core::MIIterator &mit) { return ContainsNullImpl(mit); }
  virtual bool CheckExists(core::MIIterator const &mit) { return (NumOfValuesImpl(mit) > 0); }
  inline Iterator begin(core::MIIterator const &mit) { return Iterator(this, BeginImpl(mit)); }
  inline Iterator end(core::MIIterator const &mit) { return Iterator(this, EndImpl(mit)); }
  inline types::TianmuValueObject GetSetMin(core::MIIterator const &mit) { return GetSetMinImpl(mit); }
  inline types::TianmuValueObject GetSetMax(core::MIIterator const &mit) { return GetSetMaxImpl(mit); }
  inline void SetExpectedType(core::ColumnType const &ct) { SetExpectedTypeImpl(ct); }
  int64_t GetNotNullValueInt64([[maybe_unused]] const core::MIIterator &mit) override {
    DEBUG_ASSERT(0);
    return 0;
  }
  void GetNotNullValueString([[maybe_unused]] types::BString &s,
                             [[maybe_unused]] const core::MIIterator &mit) override {
    DEBUG_ASSERT(0);
    ;
  }
  inline bool CopyCond(core::MIIterator const &mit, types::CondArray &condition, DTCollation coll) {
    return CopyCondImpl(mit, condition, coll);
  }
  inline bool CopyCond(core::MIIterator const &mit, std::shared_ptr<utils::Hash64> &condition, DTCollation coll) {
    return CopyCondImpl(mit, condition, coll);
  }

 protected:
  virtual types::TianmuValueObject GetSetMinImpl(core::MIIterator const &mit) = 0;
  virtual types::TianmuValueObject GetSetMaxImpl(core::MIIterator const &mit) = 0;

  virtual int64_t NumOfValuesImpl(core::MIIterator const &mit) = 0;
  virtual int64_t AtLeastNoDistinctValuesImpl(core::MIIterator const &, int64_t const at_least) = 0;
  virtual bool ContainsNullImpl(const core::MIIterator &) = 0;

  virtual std::unique_ptr<IteratorInterface> BeginImpl(core::MIIterator const &) = 0;
  virtual std::unique_ptr<IteratorInterface> EndImpl(core::MIIterator const &) = 0;
  virtual void SetExpectedTypeImpl(core::ColumnType const &) = 0;

  int64_t GetValueInt64Impl([[maybe_unused]] const core::MIIterator &mit) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
    return (0);
  }
  double GetValueDoubleImpl([[maybe_unused]] const core::MIIterator &mit) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
    return (0);
  }
  bool IsNullImpl([[maybe_unused]] const core::MIIterator &mit) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
    return (false);
  }
  void GetValueStringImpl([[maybe_unused]] types::BString &s, [[maybe_unused]] const core::MIIterator &mit) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
  }
  types::TianmuValueObject GetValueImpl([[maybe_unused]] const core::MIIterator &mit,
                                        [[maybe_unused]] bool lookup_to_num) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
    return (types::TianmuValueObject());
  }
  int64_t GetNumOfNullsImpl([[maybe_unused]] const core::MIIterator &mit,
                            [[maybe_unused]] bool val_nulls_possible) override {
    return common::NULL_VALUE_64;
  }
  bool IsRoughNullsOnlyImpl() const override {
    return false;
  }  // implement properly when DoRoughMin/Max are implemented non-trivially

  int64_t GetSumImpl([[maybe_unused]] const core::MIIterator &mit, [[maybe_unused]] bool &nonnegative) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
    nonnegative = false;
    return common::NULL_VALUE_64;
  }
  int64_t GetApproxDistValsImpl([[maybe_unused]] bool incl_nulls,
                                [[maybe_unused]] core::RoughMultiIndex *rough_mind) override {
    if (multi_index_->TooManyTuples())
      return common::PLUS_INF_64;
    return multi_index_->NumOfTuples();  // default
  }

  virtual common::Tribool Contains64Impl(core::MIIterator const &mit, int64_t val) = 0;
  virtual common::Tribool ContainsStringImpl(core::MIIterator const &mit, types::BString &val) = 0;
  virtual common::Tribool ContainsImpl(core::MIIterator const &, types::TianmuDataType const &) = 0;
  virtual bool IsEmptyImpl(core::MIIterator const &) = 0;
  virtual bool CopyCondImpl(core::MIIterator const &mit, types::CondArray &condition, DTCollation coll) = 0;
  virtual bool CopyCondImpl(core::MIIterator const &mit, std::shared_ptr<utils::Hash64> &condition,
                            DTCollation coll) = 0;
  bool CanCopy() const override {
    return false;
  }  // even copies refer to the same core::TempTable and core::TempTable cannot
     // be used in parallel due to Attr paging
  bool IsThreadSafe() override { return false; }
  core::MysqlExpression::tianmu_fields_cache_t tianmu_items_;  // items used in mysqlExpressions
};
}  // namespace vcolumn
}  // namespace Tianmu

#endif  // TIANMU_VC_MULTI_VALUE_COLUMN_H_
