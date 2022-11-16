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
#ifndef TIANMU_CORE_CONDITION_H_
#define TIANMU_CORE_CONDITION_H_
#pragma once

#include <set>

#include "core/cq_term.h"
#include "core/descriptor.h"

namespace Tianmu {
namespace core {
class Condition {
 protected:
  std::vector<Descriptor> descriptors;

 public:
  virtual ~Condition() {}
  virtual void AddDescriptor(CQTerm e1, common::Operator op, CQTerm e2, CQTerm e3, TempTable *t, int no_dims,
                             char like_esc);
  virtual void AddDescriptor(DescTree *tree, TempTable *t, int no_dims);
  virtual void AddDescriptor(const Descriptor &desc);
  uint Size() const { return (uint)descriptors.size(); }
  void Clear() { descriptors.clear(); }
  void EraseFirst() { descriptors.erase(descriptors.begin()); }
  Descriptor &operator[](int i) { return descriptors[i]; }
  const Descriptor &operator[](int i) const { return descriptors[i]; }
  virtual bool IsType_Tree() { return false; }
  virtual void Simplify();
  void MakeSingleColsPrivate(std::vector<vcolumn::VirtualColumn *> &);
};

class SingleTreeCondition : public Condition {
  DescTree *tree;

 public:
  SingleTreeCondition() { tree = nullptr; }
  SingleTreeCondition(CQTerm e1, common::Operator op, CQTerm e2, CQTerm e3, TempTable *t, int no_dims, char like_esc);
  virtual ~SingleTreeCondition();
  using Condition::AddDescriptor;
  virtual void AddDescriptor(common::LogicalOperator lop, CQTerm e1, common::Operator op, CQTerm e2, CQTerm e3,
                             TempTable *t, int no_dims, char like_esc);
  void AddTree(common::LogicalOperator lop, DescTree *tree, int no_dims);
  DescTree *GetTree() { return tree; }
  bool IsType_Tree() override { return true; }
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_CONDITION_H_
