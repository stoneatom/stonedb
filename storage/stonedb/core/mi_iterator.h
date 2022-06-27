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
#ifndef STONEDB_CORE_MI_ITERATOR_H_
#define STONEDB_CORE_MI_ITERATOR_H_
#pragma once

#include "core/multi_index.h"

namespace stonedb {
namespace core {
class PackOrderer;

/*! \brief Used for iteration on chosen dimensions in a MultiIndex.
 * Usage example:
 * MIIterator it(mind,...);
 * it.Rewind();
 * while(it.IsValid()) {
 *   ...
 *   ++it;
 * }
 *
 */
class MIIterator {
 public:
  // The splitting capability supported by MIIterator.
  struct SliceCapability {
    enum class Type {
      kDisable = 0,  // Disable splitting into block.
      kLinear,       // Splitting into block with pack or line, the internal iterator
                     // is FilterOnesIterator.
      kFixed         // Splitting into block with a fixed value,
                     // the internal iterator is
                     // DimensionGroupMultiMaterialized::DGIterator.
    };
    Type type = Type::kDisable;
    std::vector<int64_t> slices;  // Every element is slice size.
  };

  /*! create an iterator for a MultiIndex and set to the \e mind begin
   *
   * \param mind the MultiIndex for which the iterator is created
   * Example:
   * 		DimensionVector dimensions(mind);
   *		for(...)
   *			dimensions[...we are interested in...] = true;
   *		MIIterator mit(mind, dimensions);
   *
   */
  MIIterator(MultiIndex *mind,
             uint32_t power);  // note: mind=NULL will create a local multiindex
                               // - one dim., one row
  MIIterator(MultiIndex *mind, int one_dimension,
             bool lock);  // -1 is an equivalent of all dimensions// = true
  MIIterator(MultiIndex *mind, DimensionVector &dimensions);
  MIIterator(MultiIndex *mind, DimensionVector &dimensions,
             std::vector<PackOrderer> &po);  // Ordered iterator

  MIIterator(const MIIterator &, bool lock = true);
  MIIterator();  // empty constructor: only for special cases of MIDummyIterator
  virtual ~MIIterator();

  enum class MIIteratorType { MII_NORMAL, MII_DUMMY, MII_LOOKUP };

  /*! Get the array of row numbers for each dimension in the MultiIndex for the
   * current position of the iterator.
   *
   * \return Returns a pointer to a member array of row numbers with its length
   * equal to the number of dimensions in the associated MultiIndex. Only the
   * elements corresponding to dimensions used for MIIterator creation are set
   * by the iterator, the rest is left as common::NULL_VALUE_64. Cannot be used
   * at the end of MultiIndex (check IsValid() first).
   */
  const int64_t *operator*() const {
    DEBUG_ASSERT(valid);
    return cur_pos;
  }

  /*! Get the current value of iterator for the given dimension.
   *
   * \return Returns a row number in the associated MultiIndex.
   * Only the elements corresponding to dimensions used for MIIterator
   * creation are set by the iterator, the rest is left as
   * common::NULL_VALUE_64. Cannot be used at the end of MultiIndex (check
   * IsValid() first).
   */
  int64_t operator[](int n) const {
    DEBUG_ASSERT(valid && n >= 0 && n < no_dims);
    return cur_pos[n];
  }

  MIIterator &operator=(const MIIterator &m) {
    MIIterator tmp(m);
    swap(tmp);
    return *this;
  }

  //! virtual version of operator++, as we need it sometimes to be flexible, and
  //! sometimes to be fast (inline)
  virtual MIIterator &Increment() { return this->operator++(); }
  //! move to the next row position within an associated MultiIndex
  inline MIIterator &operator++() {
    DEBUG_ASSERT(valid);
    next_pack_started = false;
    if (one_filter_dim > -1) {  // Optimized (one-dimensional filter) part:
      ++(*one_filter_it);
      valid = one_filter_it->IsValid();
      --pack_size_left;
      if (valid) {
        cur_pos[one_filter_dim] = one_filter_it->GetCurPos(one_filter_dim);
        if (pack_size_left == 0) {
          cur_pack[one_filter_dim] = int(cur_pos[one_filter_dim] >> p_power);
          InitNextPackrow();
        }
      }
    } else {  // General part:
      GeneralIncrement();
      if (pack_size_left != common::NULL_VALUE_64) {  // else the packrow is too large to be
                                                      // iterated through
        --pack_size_left;
        if (pack_size_left == 0) InitNextPackrow();
      }
    }
    DEBUG_ASSERT(one_filter_dim == -1 || cur_pos[one_filter_dim] >> p_power == cur_pack[one_filter_dim]);
    return *this;
  }

  //! Skip a number of rows (equivalent of a number of ++it)
  void Skip(int64_t offset);

  //! set the iterator to the begin of the associated MultiIndex
  void Rewind();
  /*! True if the next call to operator* will return a valid result. False e.g.
   * if end has been reached.
   */
  bool IsValid() const { return valid; }
  MultiIndex *GetMultiIndex() { return mind; }
  MIIteratorType Type() const { return mii_type; }
  /*! Get the number of rows omitted in unused dimensions on each step of
   * iterator. Used e.g. as a factor in sum(...), count(...).
   *
   * \return Returns either a number of omitted rows,
   * or common::NULL_VALUE_64 if the number is too big (> 2^63).
   * \pre The result is correct for an initial state of multiindex (no further
   * changes apply).
   */
  int64_t Factor() const { return omitted_factor; }
  /*! Get the number of rows the iterator will iterate through.
   *
   * \return Returns either a number of rows to iterate,
   * or common::NULL_VALUE_64 if the number is too big (> 2^63).
   * \pre The result is correct for an initial state of multiindex (no further
   * changes apply).
   */
  virtual int64_t NoTuples() const { return no_obj; }
  //! Total number of tuples in the source multiindex
  int64_t NoOrigTuples() const { return mind->NoTuples(); }
  //! How many rows (including the current one) are ahead of the current
  //! position in the current packrow
  virtual int64_t GetPackSizeLeft() const { return pack_size_left; }
  /*! True, if there were any null object numbers (NOT null values!) possibly
   * found in the current packrow for a given dimension \warning True may mean
   * that the null value is possible, but does not guarantee it!
   */
  virtual bool NullsPossibleInPack(int dim) const { return it[it_for_dim[dim]]->NullsExist(dim); }
  virtual bool NullsPossibleInPack() const {
    for (int i = 0; i < no_dims; i++) {
      if (dimensions.Get(i) && it[it_for_dim[i]]->NullsExist(i)) return true;
    }
    return false;
  }

  /*! True, if the current packrow contain exactly the whole pack for a
   * dimension (no repetitions, no null objects)
   */
  virtual bool WholePack(int dim) const;

  /*!  Go to the beginning of the next packrow
   * If the end is reached, IsValid() becomes false.
   */
  virtual void NextPackrow();

  /*! Get pack numbers for each dimension in the associated MultiIndex for the
   * current position. \return Returns a pointer to a member array of pack
   * numbers with its length equal to the number of dimensions in the associated
   * MultiIndex. Only the elements corresponding to dimensions used for
   * MIIterator
   * creation are set by the iterator.
   */
  const int *GetCurPackrow() const { return cur_pack; }
  int GetCurPackrow(int dim) const { return cur_pack[dim]; }
  uint32_t GetPower() const { return p_power; }
  void SetPower(uint32_t power) { p_power = power; }
  virtual int GetNextPackrow(int dim, int ahead = 1) const;

  int GetCurInpack(int dim) const {
    DEBUG_ASSERT(valid && dim >= 0 && dim < no_dims);
    return int(cur_pos[dim] & ((1 << p_power) - 1));
  }

  //! True, if the current row is the first one in the packrow
  bool PackrowStarted() const { return next_pack_started; }
  void swap(MIIterator &);

  virtual bool DimUsed(int dim) const { return dimensions.Get(dim); }  // is the dimension used in iterator?
  DimensionVector DimsUsed() { return dimensions; }
  int NoDimensions() const { return no_dims; }
  int64_t OrigSize(int dim) const { return mind->OrigSize(dim); }  // upper limit of a number of rows
  bool IsThreadSafe();
  bool BarrierAfterPackrow();  // true, if we must synchronize threads before
                               // NextPackrow() (including multiindex end)
  void SetTaskId(int id) { TaskId = id; }
  int GetTaskId() const { return TaskId; }
  void SetTaskNum(int num) { TasksNum = num; }
  int GetTaskNum() const { return TasksNum; }
  //! after traversing up to n non-empty DPs the iterator becomes invalid
  //! works only if IsSingleFilter()
  void SetNoPacksToGo(int n);
  virtual bool RewindToPack(const int pack);

  int GetOneFilterDim() const { return one_filter_dim; }
  SliceCapability GetSliceCapability() const;

  void RewindToRow(int64_t row);

 protected:
  void GeneralIncrement();  // a general-case part of operator++

  std::vector<DimensionGroup::Iterator *> it;  // iterators created for the groups
  std::vector<DimensionGroup *> dg;            // external pointer: dimension groups to be iterated
  int *it_for_dim;                             // iterator no. for given dimensions

  MultiIndex *mind;           // a multiindex the iterator is created for
  bool mind_created_locally;  // true, if the multiindex should be deleted in
                              // descructor
  int no_dims;                // number of dimensions in the associated multiindex
  DimensionVector dimensions;

  int64_t *cur_pos;  // a buffer storing the current position
  int *cur_pack;     // a buffer storing the current pack number
  bool valid;        // false if we are already out of multiindex

  int64_t omitted_factor;  // how many tuples in omitted dimensions are skipped
                           // at each iterator step
  // common::NULL_VALUE_64 means more than 2^63
  int64_t no_obj;    // how many steps (tuples) will the iterator perform
  uint32_t p_power;  // 2^p_power per pack max number
  // common::NULL_VALUE_64 means more than 2^63
  int64_t pack_size_left;  // how many steps (tuples) will the iterator
  // perform inside the current packrow, incl. the current one
  bool next_pack_started;  // true if the current row is the first one inside
                           // the packrow
  MIIteratorType mii_type;
  std::vector<PackOrderer> &po;  // for ordered iterator

  // Optimized part

  int one_filter_dim;                       // if there is only one filter to be iterated, this is
                                            // its number, otherwise -1
  DimensionGroup::Iterator *one_filter_it;  // external pointer: the iterator
                                            // connected to the one dim, if any
  int TaskId;                               // for multithread
  int TasksNum;                             // for multithread
 private:
  void Init(bool lock = true);
  inline void InitNextPackrow() {  // find boundaries of the current packrow,
    // assumption: pack_size_left is properly updated only if the method is used
    // at the first row of a packrow.
    if (!valid) return;
    pack_size_left = 1;
    for (unsigned int i = 0; i < it.size(); i++) {
      pack_size_left = SafeMultiplication(pack_size_left, it[i]->GetPackSizeLeft());
    }
    next_pack_started = true;
  }
};

/*! \brief Used for storing position of another iterator and for combining
 * positions from several iterators.
 *
 */
class MIDummyIterator : public MIIterator {
 public:
  MIDummyIterator(int dims);
  MIDummyIterator(MultiIndex *mind);
  MIDummyIterator(const MIIterator &sec) : MIIterator(sec) { valid = true; }
  void Combine(const MIIterator &sec);  // copy position from the second
                                        // iterator (only the positions used)
  void Set(int dim, int64_t val);       // set a position manually
  void SetPack(int dim, int p);         // set a position manually
  int GetNextPackrow(int dim, [[maybe_unused]] int ahead = 1) const override { return MIIterator::GetCurPackrow(dim); }
  bool NullsPossibleInPack(int dim) const override { return (cur_pos[dim] == common::NULL_VALUE_64); }
  bool NullsPossibleInPack() const override;

  // These functions cannot be easily implemented for DummyIterator, so they
  // return safe defaults
  bool WholePack([[maybe_unused]] int dim) const override { return false; }
  // These functions are usually unused, but if (for test reasons) we need them,
  // simulate one-row nonull multiindex
  int64_t NoTuples() const override { return 1; }
  int64_t GetPackSizeLeft() const override { return 1; }
  // These functions are illegal for MIDummyIterator
  MIIterator &Increment() override {
    DEBUG_ASSERT(0);
    return *this;
  }
  MIIterator &operator++() {
    DEBUG_ASSERT(0);
    return *this;
  }
  int64_t Factor() const {
    DEBUG_ASSERT(0);
    return omitted_factor;
  }
  void NextPackrow() override { DEBUG_ASSERT(0); }
  bool DimUsed([[maybe_unused]] int dim) const override {
    DEBUG_ASSERT(0);
    return true;
  }
};

/*! \brief Used for storing position in lookup dictionary.
 *
 */

class MILookupIterator : public MIDummyIterator {
 public:
  MILookupIterator() : MIDummyIterator(1) { mii_type = MIIteratorType::MII_LOOKUP; }
  MILookupIterator(const MILookupIterator &sec) : MIDummyIterator(sec) {}
  void Set(int64_t val) { MIDummyIterator::Set(0, val); }
  void Invalidate() { valid = false; }
};

class MIInpackIterator : public MIIterator {
 public:
  MIInpackIterator(const MIIterator &sec) : MIIterator(sec) {}
  MIInpackIterator() : MIIterator() {}
  void swap(MIInpackIterator &i);
  MIInpackIterator &operator=(const MIInpackIterator &m) {
    MIInpackIterator tmp(m);
    swap(tmp);
    return *this;
  }
  MIInpackIterator &operator=(const MIIterator &m) {
    MIInpackIterator tmp(m);
    swap(tmp);
    return *this;
  }

  // These functions have special meaning for MIInpackIterator:
  void NextPackrow() override { valid = false; }
  int GetNextPackrow(int dim, int ahead = 1) const override;
  void Rewind() { DEBUG_ASSERT(0); }  // do not use
  MIIterator &Increment() override { return this->operator++(); }
  // Special version of iterator (use Increment() if the iterator should be
  // virtual)
  inline MIIterator &operator++() {
    DEBUG_ASSERT(valid);
    next_pack_started = false;
    if (one_filter_dim > -1) {
      valid = one_filter_it->NextInsidePack();
      valid = valid && one_filter_it->IsValid();
      if (valid) cur_pos[one_filter_dim] = one_filter_it->GetCurPos(one_filter_dim);
    } else {
      // General part:
      GeneralIncrement();
    }
    --pack_size_left;
    if (pack_size_left == 0) valid = false;
    return *this;
  }

  void GeneralIncrement();
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_MI_ITERATOR_H_
