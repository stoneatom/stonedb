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
#ifndef STONEDB_VC_MULTI_VALUE_COLUMN_H_
#define STONEDB_VC_MULTI_VALUE_COLUMN_H_
#pragma once

#include "core/mi_updating_iterator.h"
#include "core/pack_guardian.h"
#include "types/rc_num.h"
#include "vc/virtual_column.h"

namespace stonedb {
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
    inline types::RCNum GetRCNum() const { return DoGetRCNum(); }
    inline types::RCValueObject GetValue() const { return DoGetValue(); }
    inline int64_t GetInt64() const { return DoGetInt64(); }
    inline bool IsNull() const { return IsNullImpl(); }

   private:
    virtual std::unique_ptr<LazyValueInterface> DoClone() const = 0;
    virtual types::BString DoGetString() const = 0;
    virtual types::RCNum DoGetRCNum() const = 0;
    virtual types::RCValueObject DoGetValue() const = 0;
    virtual int64_t DoGetInt64() const = 0;
    virtual bool IsNullImpl() const = 0;
  };

  class Iterator;
  class LazyValue {
   public:
    LazyValue(LazyValue const &lv) : impl_(lv.impl_.get() ? lv.impl_->Clone() : std::unique_ptr<LazyValueInterface>()) {}
    LazyValue &operator=(LazyValue const &lv) {
      if (&lv != this) {
        if (lv.impl_.get())
          impl_ = lv.impl_->Clone();
        else
          impl_.reset();
      }
      return *this;
    }

    LazyValue *operator->() { return this; }
    types::BString GetString() { return impl_->GetString(); }
    types::RCNum GetRCNum() { return impl_->GetRCNum(); }
    types::RCValueObject GetValue() { return impl_->GetValue(); }
    int64_t GetInt64() { return impl_->GetInt64(); }
    bool IsNull() { return impl_->IsNull(); }

   private:
    std::unique_ptr<LazyValueInterface> impl_;
    LazyValue(std::unique_ptr<LazyValueInterface> impl_) : impl_(std::move(impl_)) {}
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
        : owner_(it.owner_), impl_(it.impl_.get() ? it.impl_->clone() : std::unique_ptr<IteratorInterface>()) {}
    Iterator &operator=(Iterator const &i) {
      if (&i != this) {
        if (i.impl_.get())
          impl_ = i.impl_->clone();
        else
          impl_.reset();
      }
      return *this;
    }
    Iterator &operator++() {
      impl_->Next();
      return *this;
    }

    LazyValue operator*() { return LazyValue(impl_->GetValue()); }
    LazyValue operator->() { return LazyValue(impl_->GetValue()); }
    bool operator!=(Iterator const &it) const {
      DEBUG_ASSERT(owner_ == it.owner_);
      return (impl_->NotEq(it.impl_.get()));
    }

   private:
    MultiValColumn const *owner_;
    std::unique_ptr<IteratorInterface> impl_;

    Iterator(MultiValColumn const *owner, std::unique_ptr<IteratorInterface> impl)
        : owner_(owner), impl_(std::move(impl)) {}

    friend class MultiValColumn;
  };

  /*! \brief Create a column that represent const value.
   *
   * \param ct - type of column.
   * \param mind - core::MultiIndex.
   * \param expressions - a STL container of core::MysqlExpression*.
   */
  MultiValColumn(core::ColumnType const &ct, core::MultiIndex *mind) : VirtualColumn(ct, mind) {}
  MultiValColumn(const MultiValColumn &c) : VirtualColumn(c) { sdb_items_ = c.sdb_items_; }
  virtual ~MultiValColumn() {}

  bool IsMultival() const override { return true; }
  inline common::Tribool Contains64(core::MIIterator const &mit, int64_t val) { return Contains64Impl(mit, val); }
  inline common::Tribool ContainsString(core::MIIterator const &mit, types::BString &val) {
    return ContainsStringImpl(mit, val);
  }

  inline common::Tribool Contains(core::MIIterator const &mit, types::RCDataType const &val) {
    return (ContainsImpl(mit, val));
  }

  virtual bool IsSetEncoded([[maybe_unused]] common::CT at, [[maybe_unused]] int scale) {
    return false;
  }  // checks whether the set is constant and fixed size equal to the given one

  inline bool IsEmpty(core::MIIterator const &mit) { return (IsEmptyImpl(mit)); }
  inline int64_t NoValues(core::MIIterator const &mit) { return NoValuesImpl(mit); }
  inline int64_t AtLeastNoDistinctValues(core::MIIterator const &mit, int64_t const at_least) {
    return AtLeastNoDistinctValuesImpl(mit, at_least);
  }

  inline bool ContainsNull(const core::MIIterator &mit) { return ContainsNullImpl(mit); }
  virtual bool CheckExists(core::MIIterator const &mit) { return (NoValuesImpl(mit) > 0); }
  inline Iterator begin(core::MIIterator const &mit) { return Iterator(this, BeginImpl(mit)); }
  inline Iterator end(core::MIIterator const &mit) { return Iterator(this, EndImpl(mit)); }
  inline types::RCValueObject GetSetMin(core::MIIterator const &mit) { return GetSetMinImpl(mit); }
  inline types::RCValueObject GetSetMax(core::MIIterator const &mit) { return GetSetMaxImpl(mit); }
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
  virtual types::RCValueObject GetSetMinImpl(core::MIIterator const &mit) = 0;
  virtual types::RCValueObject GetSetMaxImpl(core::MIIterator const &mit) = 0;

  virtual int64_t NoValuesImpl(core::MIIterator const &mit) = 0;
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

  types::RCValueObject GetValueImpl([[maybe_unused]] const core::MIIterator &mit,
                                    [[maybe_unused]] bool lookup_to_num) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
    return (types::RCValueObject());
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
    if (multi_idx_->TooManyTuples()) return common::PLUS_INF_64;
    return multi_idx_->NoTuples();  // default
  }

  virtual common::Tribool Contains64Impl(core::MIIterator const &mit, int64_t val) = 0;
  virtual common::Tribool ContainsStringImpl(core::MIIterator const &mit, types::BString &val) = 0;
  virtual common::Tribool ContainsImpl(core::MIIterator const &, types::RCDataType const &) = 0;
  virtual bool IsEmptyImpl(core::MIIterator const &) = 0;
  virtual bool CopyCondImpl(core::MIIterator const &mit, types::CondArray &condition, DTCollation coll) = 0;
  virtual bool CopyCondImpl(core::MIIterator const &mit, std::shared_ptr<utils::Hash64> &condition,
                            DTCollation coll) = 0;
  bool CanCopy() const override {
    return false;
  }  // even copies refer to the same core::TempTable and core::TempTable cannot
     // be used in parallel due to Attr paging
  bool IsThreadSafe() override { return false; }

  core::MysqlExpression::sdbfields_cache_t sdb_items_;  // items used in mysqlExpressions
};
}  // namespace vcolumn
}  // namespace stonedb

#endif  // STONEDB_VC_MULTI_VALUE_COLUMN_H_
