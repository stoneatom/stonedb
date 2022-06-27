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
#ifndef STONEDB_CORE_DESCRIPTOR_H_
#define STONEDB_CORE_DESCRIPTOR_H_
#pragma once

#include <mutex>
#include <vector>

#include "common/common_definitions.h"
#include "core/cq_term.h"
#include "core/dimension_group.h"

namespace stonedb {
namespace core {
class ParameterizedFilter;
class MIIterator;
class TempTable;
class DescTree;
class MIUpdatingIterator;
class QueryOperator;

/*! DescriptorJoinType::DT_NON_JOIN - 1 dimension condition
 *  DescriptorJoinType::DT_SIMPLE_JOIN - suitable for e.g. hash join
 *  DescriptorJoinType::DT_COMPLEX_JOIN - possibly requires nested loop join
 */
enum class DescriptorJoinType { DT_NON_JOIN, DT_SIMPLE_JOIN, DT_COMPLEX_JOIN, DT_NOT_KNOWN_YET };
enum class SubSelectOptimizationType { ROW_BASED, PACK_BASED };

class Descriptor {
 public:
  common::Operator op;
  CQTerm attr;  // descriptor is usually "attr op val1 [val2]", e.g. "a = 7", "a
                // BETWEEN 8 AND 12"
  CQTerm val1;
  CQTerm val2;

  common::LogicalOperator lop;  // OR is not implemented on some lower levels!
                                // Only valid in HAVING tree.
  bool sharp;                   // used to indicate that BETWEEN contains sharp inequalities
  bool encoded;
  bool done;
  double evaluation;  // approximate weight ("hardness") of the condition;
                      // execution order is based on it
  bool delayed;       // descriptor is delayed (usually because of outer joins) tbd.
                      // after joins
  TempTable *table;
  DescTree *tree;
  DimensionVector left_dims;
  DimensionVector right_dims;
  int parallsize = 0;
  std::vector<common::RSValue> rvs;  // index corresponding threadid
  common::RSValue rv;                // rough evaluation of descriptor (accumulated or used locally)
  char like_esc;                     // nonstandard LIKE escape character
  std::mutex mtx;

  Descriptor();
  Descriptor(TempTable *t,
             int no_dims);  // no_dims is a destination number of dimensions
                            // (multiindex may be smaller at this point)
  ~Descriptor();
  Descriptor(const Descriptor &desc);
  Descriptor(CQTerm e1, common::Operator pr, CQTerm e2, CQTerm e3, TempTable *t, int no_dims, char like_escape = '\\');
  Descriptor(DescTree *tree, TempTable *t, int no_dims);
  Descriptor(TempTable *t, vcolumn::VirtualColumn *v1, common::Operator pr, vcolumn::VirtualColumn *v2 = NULL,
             vcolumn::VirtualColumn *v3 = NULL);

  void swap(Descriptor &d);

  Descriptor &operator=(const Descriptor &desc);
  int operator==(const Descriptor &sec) const;
  bool operator<(const Descriptor &sec) const;
  bool operator<=(const Descriptor &sec) const;
  bool EqualExceptOuter(const Descriptor &sec);  // descriptors are equal, but one may be inner
                                                 // and other may be outer
  void Simplify(bool in_having = false);
  void SwitchSides();
  bool IsEmpty() const { return op == common::Operator::O_UNKNOWN_FUNC; }
  bool IsTrue() const { return op == common::Operator::O_TRUE; }
  bool IsFalse() const { return op == common::Operator::O_FALSE; }
  bool IsSDBItemsEmpty();
  bool WithoutAttrs();
  bool WithoutTypeCast();

  void UpdateVCStatistics();  // Apply all the information from constants etc.
                              // to involved VC

  bool CheckCondition_UTF(const MIIterator &mit);
  bool CheckCondition(const MIIterator &mit);  // Assumption: LockSourcePacks done externally.
  bool IsNull(const MIIterator &mit);
  void LockSourcePacks(const MIIterator &mit);
  void LockSourcePacks(const MIIterator &mit, int th_no);
  void EvaluatePack(MIUpdatingIterator &mit, int th_no);
  void EvaluatePack(MIUpdatingIterator &mit);  // Assumption: no locking needed, done inside
  common::RSValue EvaluateRoughlyPack(const MIIterator &mit);

  void UnlockSourcePacks();
  void DimensionUsed(DimensionVector &dims);
  bool IsParameterized() const;
  bool IsDeterministic() const;
  bool IsType_OrTree() const { return op == common::Operator::O_OR_TREE; }
  bool IsType_JoinSimple() const;
  bool IsType_AttrAttr() const;
  bool IsType_AttrValOrAttrValVal() const;
  bool IsType_AttrMultiVal() const;
  // Note: CalculateJoinType() must be executed before checking a join type
  void CalculateJoinType();
  DescriptorJoinType GetJoinType() const { return desc_t; }
  bool IsType_Subquery();

  bool IsType_STONEDBExpression() const;  // only columns, constants and STONEDBExpressions
  bool IsType_JoinComplex() const;
  bool IsType_Join() const { return (IsType_JoinSimple() || IsType_JoinComplex()); }
  bool IsDelayed() const { return delayed; }
  bool IsInner() { return right_dims.IsEmpty(); }   // join_type == JoinType::JO_INNER; }
  bool IsOuter() { return !right_dims.IsEmpty(); }  // (IsLeftJoin() || IsRightJoin() || IsFullOuterJoin()); }
  char *ToString(char buf[], size_t buf_ct);
  void CoerceColumnTypes();
  bool NullMayBeTrue();  // true, if the descriptor may give nontrivial answer
                         // if any of involved dimension is null
  const QueryOperator *CreateQueryOperator(common::Operator type) const;
  void CoerceColumnType(vcolumn::VirtualColumn *&to_be_casted);
  DTCollation GetCollation() const { return collation; }
  void CopyStatefulVCs(std::vector<vcolumn::VirtualColumn *> &local_cols);
  bool IsParallelReady();
  void EvaluatePackImpl(MIUpdatingIterator &mit);
  void SimplifyAfterRoughAccumulate();
  void RoughAccumulate(MIIterator &mit);
  void ClearRoughValues();
  bool CopyDesCond(MIUpdatingIterator &mit);
  void PrepareValueSet(MIIterator &mit);
  // for muti-thread
  void InitRvs(int value);
  void InitParallel(int value, MIIterator &mit);
  int GetParallelSize() { return parallsize; }
  void MClearRoughValues(int taskid);
  common::RSValue MEvaluateRoughlyPack(const MIIterator &mit, int taskid);
  void MLockSourcePacks(const MIIterator &mit, int taskid);
  bool ExsitTmpTable() const;
  bool IsleftIndexSearch() const;
  common::ErrorCode EvaluateOnIndex(MIUpdatingIterator &mit, int64_t limit);

 private:
  /*! \brief Checks condition for set operator, e.g., <ALL
   * \pre LockSourcePacks done externally.
   * \param mit - iterator on MultiIndex
   * \param op - operator to check
   * \return bool
   */
  bool CheckSetCondition(const MIIterator &mit, common::Operator op);
  bool IsNull_Set(const MIIterator &mit, common::Operator op);
  common::Tribool RoughCheckSetSubSelectCondition(const MIIterator &mit, common::Operator op,
                                                  SubSelectOptimizationType sot);
  bool CheckSetCondition_UTF(const MIIterator &mit, common::Operator op);

  void AppendString(char *buffer, size_t bufferSize, const char *string, size_t stringLength, size_t offset) const;
  void AppendConstantToString(char buffer[], size_t size, const QueryOperator *operatorObject) const;
  void AppendUnaryOperatorToString(char buffer[], size_t size, const QueryOperator *operatorObject) const;
  void AppendBinaryOperatorToString(char buffer[], size_t size, const QueryOperator *operatorObject) const;
  void AppendTernaryOperatorToString(char buffer[], size_t size, const QueryOperator *operatorObject) const;

  void CoerceCollation();
  common::Tribool RoughCheckSubselectCondition(MIIterator &mit, SubSelectOptimizationType);
  bool CheckTmpInTerm(const CQTerm &t) const;
  //! make the type of to_be_casted to be comparable to attr.vc by wrapping
  //! to_be_casted in a vcolumn::TypeCastColumn
  DescriptorJoinType desc_t;
  DTCollation collation;

 public:
  bool null_after_simplify;  // true if Simplify set common::Operator::O_FALSE because of
                             // NULL
};

class SortDescriptor {
 public:
  vcolumn::VirtualColumn *vc;
  int dir;  // ordering direction: 0 - ascending, 1 - descending
  SortDescriptor() : vc(NULL), dir(0){};
  int operator==(const SortDescriptor &sec) { return (dir == sec.dir) && (vc == sec.vc); }
};

bool IsSetOperator(common::Operator op);
bool IsSetAllOperator(common::Operator op);
bool IsSetAnyOperator(common::Operator op);
bool ISTypeOfEqualOperator(common::Operator op);
bool ISTypeOfNotEqualOperator(common::Operator op);
bool ISTypeOfLessOperator(common::Operator op);
bool ISTypeOfLessEqualOperator(common::Operator op);
bool ISTypeOfMoreOperator(common::Operator op);
bool ISTypeOfMoreEqualOperator(common::Operator op);
bool IsSimpleEqualityOperator(common::Operator op);

struct DescTreeNode {
  DescTreeNode(common::LogicalOperator _lop, TempTable *t, int no_dims)
      : desc(t, no_dims), locked(0), left(NULL), right(NULL), parent(NULL) {
    desc.lop = _lop;
  }

  DescTreeNode(CQTerm e1, common::Operator op, CQTerm e2, CQTerm e3, TempTable *t, int no_dims, char like_esc)
      : desc(t, no_dims), locked(0), left(NULL), right(NULL), parent(NULL) {
    desc.attr = e1;
    desc.op = op;
    desc.val1 = e2;
    desc.val2 = e3;
    desc.like_esc = like_esc;
    desc.CoerceColumnTypes();
    desc.CalculateJoinType();
    desc.Simplify(true);
  }

  DescTreeNode(DescTreeNode &n, [[maybe_unused]] bool in_subq = false)
      : desc(n.desc), locked(0), left(NULL), right(NULL), parent(NULL) {}
  ~DescTreeNode();
  bool CheckCondition(MIIterator &mit);
  bool IsNull(MIIterator &mit);
  void EvaluatePack(MIUpdatingIterator &mit);
  void PrepareToLock(int locked_by);  // mark all descriptors as "must be locked before use"
  void UnlockSourcePacks();           // must be run after all CheckCondition(), before
                                      // query destructor
  common::Tribool Simplify(DescTreeNode *&root, bool in_having = false);
  bool IsParameterized();
  void DimensionUsed(DimensionVector &dims);
  bool NullMayBeTrue();
  void EncodeIfPossible(bool for_rough_query, bool additional_nulls);
  double EvaluateConditionWeight(ParameterizedFilter *p, bool for_or);
  common::RSValue EvaluateRoughlyPack(const MIIterator &mit);
  void CollectDescriptor(std::vector<std::pair<int, Descriptor>> &desc_counts);

  void IncreaseDescriptorCount(std::vector<std::pair<int, Descriptor>> &desc_counts);

  /*! \brief Check if descriptor is common to all subtrees
   * \param desc - descriptor to be evaluated
   * \return bool -
   */
  bool CanBeExtracted(Descriptor &desc);

  void ExtractDescriptor(Descriptor &desc, DescTreeNode *&root);
  void LockSourcePacks(const MIIterator &mit, const int th_no);
  void CopyStatefulVCs(std::vector<vcolumn::VirtualColumn *> &local_cols);
  bool IsParallelReady();
  common::Tribool ReplaceNode(DescTreeNode *src, DescTreeNode *dst, DescTreeNode *&root);
  bool UseRoughAccumulated();  // true if any leaf becomes trivial
  void RoughAccumulate(MIIterator &mit);
  void ClearRoughValues();
  void MakeSingleColsPrivate(std::vector<vcolumn::VirtualColumn *> &virt_cols);
  bool IsSDBItemsEmpty();
  bool WithoutAttrs();
  bool WithoutTypeCast();
  // for muti-thread
  common::RSValue MEvaluateRoughlyPack(const MIIterator &mit, int taskid);
  void MClearRoughValues(int taskid);
  void MPrepareToLock(int locked_by, int taskid);
  void MEvaluatePack(MIUpdatingIterator &mit, int taskid);
  void PrepareValueSet(MIIterator &mit);
  void InitRvs(int value);
  void InitLocks(int value);
  int GetLockSize() { return locks_size; }
  int locks_size = 0;
  std::vector<int> locks;  // for a leaf: >= 0 => source pack must be locked
                           // before use, -1 => already locked

  Descriptor desc;
  int locked;  // for a leaf: >= 0 => source pack must be locked before use, -1
               // => already locked
  DescTreeNode *left, *right, *parent;
};

// Tree of descriptors: internal nodes - predicates, leaves - terms
// Used to store conditions filtering output columns of TempTable
// Used, particularly, to store conditions of 'having' clause

class DescTree {
 public:
  DescTreeNode *root, *curr;
  DescTreeNode *Copy(DescTreeNode *node);
  //  void Release(DescTreeNode * &node); // releases whole subtree starting
  //  from node

  DescTree(CQTerm e1, common::Operator op, CQTerm e2, CQTerm e3, TempTable *q, int no_dims, char like_esc);
  DescTree(DescTree &);
  ~DescTree() { delete root; }
  void Display();
  void Display(DescTreeNode *node);

  bool Left() {
    if (!curr || !curr->left) return false;
    curr = curr->left;
    return true;
  }
  bool Right() {
    if (!curr || !curr->right) return false;
    curr = curr->right;
    return true;
  }
  bool Up() {
    if (!curr || !curr->parent) return false;
    curr = curr->parent;
    return true;
  }
  void Root() { curr = root; }
  // make lop the root, make current tree the left child, make descriptor the
  // right child
  void AddDescriptor(common::LogicalOperator lop, CQTerm e1, common::Operator op, CQTerm e2, CQTerm e3, TempTable *t,
                     int no_dims, char like_esc);
  // make lop the root, make current tree the left child, make tree the right
  // child
  void AddTree(common::LogicalOperator lop, DescTree *tree, int no_dims);
  bool IsParameterized();
  void DimensionUsed(DimensionVector &dims) { root->DimensionUsed(dims); }
  bool NullMayBeTrue() { return root->NullMayBeTrue(); }
  common::Tribool Simplify(bool in_having) { return root->Simplify(root, in_having); }
  Descriptor ExtractDescriptor();
  void CopyStatefulVCs(std::vector<vcolumn::VirtualColumn *> &local_cols) { root->CopyStatefulVCs(local_cols); }
  bool IsParallelReady() { return root->IsParallelReady(); }
  void RoughAccumulate(MIIterator &mit) { root->RoughAccumulate(mit); }
  bool UseRoughAccumulated() { return root->UseRoughAccumulated(); }  // true if anything to simplify
  void MakeSingleColsPrivate(std::vector<vcolumn::VirtualColumn *> &virt_cols);
  bool IsSDBItemsEmpty() { return root->IsSDBItemsEmpty(); }
  bool WithoutAttrs() { return root->WithoutAttrs(); }
  bool WithoutTypeCast() { return root->WithoutTypeCast(); }
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_DESCRIPTOR_H_
