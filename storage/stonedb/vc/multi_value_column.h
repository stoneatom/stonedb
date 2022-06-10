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
    inline bool IsNull() const { return DoIsNull(); }

   private:
    virtual std::unique_ptr<LazyValueInterface> DoClone() const = 0;
    virtual types::BString DoGetString() const = 0;
    virtual types::RCNum DoGetRCNum() const = 0;
    virtual types::RCValueObject DoGetValue() const = 0;
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
    types::RCNum GetRCNum() { return impl->GetRCNum(); }
    types::RCValueObject GetValue() { return impl->GetValue(); }
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
   * \param mind - core::MultiIndex.
   * \param expressions - a STL container of core::MysqlExpression*.
   */
  MultiValColumn(core::ColumnType const &ct, core::MultiIndex *mind) : VirtualColumn(ct, mind) {}
  MultiValColumn(const MultiValColumn &c) : VirtualColumn(c) { sdbitems = c.sdbitems; }
  virtual ~MultiValColumn() {}
  bool IsMultival() const override { return true; }
  inline common::Tribool Contains64(core::MIIterator const &mit, int64_t val) { return DoContains64(mit, val); }
  inline common::Tribool ContainsString(core::MIIterator const &mit, types::BString &val) {
    return DoContainsString(mit, val);
  }
  inline common::Tribool Contains(core::MIIterator const &mit, types::RCDataType const &val) {
    return (DoContains(mit, val));
  }
  virtual bool IsSetEncoded([[maybe_unused]] common::CT at, [[maybe_unused]] int scale) {
    return false;
  }  // checks whether the set is constant and fixed size equal to the given one
  inline bool IsEmpty(core::MIIterator const &mit) { return (DoIsEmpty(mit)); }
  inline int64_t NoValues(core::MIIterator const &mit) { return DoNoValues(mit); }
  inline int64_t AtLeastNoDistinctValues(core::MIIterator const &mit, int64_t const at_least) {
    return DoAtLeastNoDistinctValues(mit, at_least);
  }
  inline bool ContainsNull(const core::MIIterator &mit) { return DoContainsNull(mit); }
  virtual bool CheckExists(core::MIIterator const &mit) { return (DoNoValues(mit) > 0); }
  inline Iterator begin(core::MIIterator const &mit) { return Iterator(this, DoBegin(mit)); }
  inline Iterator end(core::MIIterator const &mit) { return Iterator(this, DoEnd(mit)); }
  inline types::RCValueObject GetSetMin(core::MIIterator const &mit) { return DoGetSetMin(mit); }
  inline types::RCValueObject GetSetMax(core::MIIterator const &mit) { return DoGetSetMax(mit); }
  inline void SetExpectedType(core::ColumnType const &ct) { DoSetExpectedType(ct); }
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
    return DoCopyCond(mit, condition, coll);
  }
  inline bool CopyCond(core::MIIterator const &mit, std::shared_ptr<utils::Hash64> &condition, DTCollation coll) {
    return DoCopyCond(mit, condition, coll);
  }

 protected:
  virtual types::RCValueObject DoGetSetMin(core::MIIterator const &mit) = 0;
  virtual types::RCValueObject DoGetSetMax(core::MIIterator const &mit) = 0;
  virtual int64_t DoNoValues(core::MIIterator const &mit) = 0;
  virtual int64_t DoAtLeastNoDistinctValues(core::MIIterator const &, int64_t const at_least) = 0;
  virtual bool DoContainsNull(const core::MIIterator &) = 0;
  virtual std::unique_ptr<IteratorInterface> DoBegin(core::MIIterator const &) = 0;
  virtual std::unique_ptr<IteratorInterface> DoEnd(core::MIIterator const &) = 0;
  virtual void DoSetExpectedType(core::ColumnType const &) = 0;
  int64_t DoGetValueInt64([[maybe_unused]] const core::MIIterator &mit) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
    return (0);
  }
  double DoGetValueDouble([[maybe_unused]] const core::MIIterator &mit) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
    return (0);
  }
  bool DoIsNull([[maybe_unused]] const core::MIIterator &mit) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
    return (false);
  }
  void DoGetValueString([[maybe_unused]] types::BString &s, [[maybe_unused]] const core::MIIterator &mit) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
  }
  types::RCValueObject DoGetValue([[maybe_unused]] const core::MIIterator &mit,
                                  [[maybe_unused]] bool lookup_to_num) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
    return (types::RCValueObject());
  }
  int64_t DoGetNoNulls([[maybe_unused]] const core::MIIterator &mit,
                       [[maybe_unused]] bool val_nulls_possible) override {
    return common::NULL_VALUE_64;
  }
  bool DoRoughNullsOnly() const override {
    return false;
  }  // implement properly when DoRoughMin/Max are implemented non-trivially

  int64_t DoGetSum([[maybe_unused]] const core::MIIterator &mit, [[maybe_unused]] bool &nonnegative) override {
    DEBUG_ASSERT(!"Invalid call for this type of column.");
    nonnegative = false;
    return common::NULL_VALUE_64;
  }
  int64_t DoGetApproxDistVals([[maybe_unused]] bool incl_nulls,
                              [[maybe_unused]] core::RoughMultiIndex *rough_mind) override {
    if (mind->TooManyTuples()) return common::PLUS_INF_64;
    return mind->NoTuples();  // default
  }
  virtual common::Tribool DoContains64(core::MIIterator const &mit, int64_t val) = 0;
  virtual common::Tribool DoContainsString(core::MIIterator const &mit, types::BString &val) = 0;
  virtual common::Tribool DoContains(core::MIIterator const &, types::RCDataType const &) = 0;
  virtual bool DoIsEmpty(core::MIIterator const &) = 0;
  virtual bool DoCopyCond(core::MIIterator const &mit, types::CondArray &condition, DTCollation coll) = 0;
  virtual bool DoCopyCond(core::MIIterator const &mit, std::shared_ptr<utils::Hash64> &condition, DTCollation coll) = 0;
  bool CanCopy() const override {
    return false;
  }  // even copies refer to the same core::TempTable and core::TempTable cannot
     // be used in parallel due to Attr paging
  bool IsThreadSafe() override { return false; }
  core::MysqlExpression::sdbfields_cache_t sdbitems;  // items used in mysqlExpressions
};
}  // namespace vcolumn
}  // namespace stonedb

#endif  // STONEDB_VC_MULTI_VALUE_COLUMN_H_
