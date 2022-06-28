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

#include "condition.h"

#include "core/temp_table.h"

namespace stonedb {
namespace core {
void Condition::AddDescriptor(CQTerm e1, common::Operator op, CQTerm e2, CQTerm e3, TempTable *t, int no_dims,
                              char like_esc) {
  descriptors.push_back(Descriptor(e1, op, e2, e3, t, no_dims, like_esc));
}

void Condition::AddDescriptor(DescTree *tree, TempTable *t, int no_dims) {
  descriptors.push_back(Descriptor(tree, t, no_dims));
}

void Condition::AddDescriptor(const Descriptor &desc) { descriptors.push_back(desc); }

void Condition::Simplify() {
  int size = int(descriptors.size());
  for (int i = 0; i < size; i++) {
    if (descriptors[i].op == common::Operator::O_OR_TREE) {
      DEBUG_ASSERT(descriptors[i].tree);
      Descriptor desc;
      do {
        if ((descriptors[i].op != common::Operator::O_OR_TREE)) break;
        desc = descriptors[i].tree->ExtractDescriptor();
        if (!desc.IsEmpty()) {
          descriptors[i].Simplify(true);  // true required not to simplify parameters
          desc.CalculateJoinType();
          desc.CoerceColumnTypes();
          descriptors.push_back(desc);
        }
      } while (!desc.IsEmpty());
    }
  }
}

void Condition::MakeSingleColsPrivate(std::vector<vcolumn::VirtualColumn *> &virt_cols) {
  DEBUG_ASSERT(descriptors.size() == 1);
  descriptors[0].tree->MakeSingleColsPrivate(virt_cols);
}

SingleTreeCondition::SingleTreeCondition(CQTerm e1, common::Operator op, CQTerm e2, CQTerm e3, TempTable *t,
                                         int no_dims, char like_esc) {
  tree = new DescTree(e1, op, e2, e3, t, no_dims, like_esc);
}

SingleTreeCondition::~SingleTreeCondition() { delete tree; }

void SingleTreeCondition::AddDescriptor(common::LogicalOperator lop, CQTerm e1, common::Operator op, CQTerm e2,
                                        CQTerm e3, TempTable *t, int no_dims, char like_esc) {
  if (tree)
    tree->AddDescriptor(lop, e1, op, e2, e3, t, no_dims, like_esc);
  else
    tree = new DescTree(e1, op, e2, e3, t, no_dims, like_esc);
}

void SingleTreeCondition::AddTree(common::LogicalOperator lop, DescTree *sec_tree, int no_dims) {
  if (tree)
    tree->AddTree(lop, sec_tree, no_dims);
  else
    tree = new DescTree(*sec_tree);
}
}  // namespace core
}  // namespace stonedb
