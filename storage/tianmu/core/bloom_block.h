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

/*
   A filter block is stored near the end of a Table file.  It contains
   filters (e.g., bloom filters) for all data blocks in the table combined
   into a single filter block.
*/
#ifndef TIANMU_CORE_BLOOM_BLOCK_H_
#define TIANMU_CORE_BLOOM_BLOCK_H_
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "common/assert.h"

namespace Tianmu {
namespace core {
class Slice {
 public:
  // Create an empty slice.
  Slice() : data_(""), size_(0) {}
  // Create a slice that refers to d[0,n-1].
  Slice(const char *d, size_t n) : data_(d), size_(n) {}
  // Create a slice that refers to the contents of "s"
  Slice(const std::string &s) : data_(s.data()), size_(s.size()) {}
  // Create a slice that refers to s[0,std::strlen(s)-1]
  Slice(const char *s) : data_(s), size_(std::strlen(s)) {}
  // Return a pointer to the beginning of the referenced data
  const char *data() const { return data_; }
  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; }
  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; }
  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  char operator[](size_t n) const {
    DEBUG_ASSERT(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  void clear() {
    data_ = "";
    size_ = 0;
  }

  // Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) {
    DEBUG_ASSERT(n <= size());
    data_ += n;
    size_ -= n;
  }

  // Return a string that contains the copy of the referenced data.
  std::string ToString() const { return std::string(data_, size_); }
  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice &b) const;

  // Return true iff "x" is a prefix of "*this"
  bool starts_with(const Slice &x) const { return ((size_ >= x.size_) && (std::memcmp(data_, x.data_, x.size_) == 0)); }

 private:
  const char *data_;
  size_t size_;

  // Intentionally copyable
};

inline bool operator==(const Slice &x, const Slice &y) {
  return ((x.size() == y.size()) && (std::memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice &x, const Slice &y) { return !(x == y); }

inline int Slice::compare(const Slice &b) const {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = std::memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_)
      r = -1;
    else if (size_ > b.size_)
      r = +1;
  }
  return r;
}

class FilterPolicy {
 public:
  virtual ~FilterPolicy(){};

  // Return the name of this policy.  Note that if the filter encoding
  // changes in an incompatible way, the name returned by this method
  // must be changed.  Otherwise, old incompatible filters may be
  // passed to methods of this type.
  virtual const char *Name() const = 0;

  // keys[0,n-1] contains a list of keys (potentially with duplicates)
  // that are ordered according to the user supplied comparator.
  // Append a filter that summarizes keys[0,n-1] to *dst.
  //
  // Warning: do not change the initial contents of *dst.  Instead,
  // append the newly constructed filter to *dst.
  virtual void CreateFilter(const Slice *keys, int n, std::string *dst) const = 0;

  // "filter" contains the data appended by a preceding call to
  // CreateFilter() on this class.  This method must return true if
  // the key was in the list of keys passed to CreateFilter().
  // This method may return true or false if the key was not on the
  // list, but it should aim to return false with a high probability.
  virtual bool KeyMayMatch(const Slice &key, const Slice &filter) const = 0;
};

// Return a new filter policy that uses a bloom filter with approximately
// the specified number of bits per key.  A good value for bits_per_key
// is 10, which yields a filter with ~ 1% false positive rate.
//
// Callers must delete the result after any database that is using the
// result has been closed.
//
// Note: if you are using a custom comparator that ignores some parts
// of the keys being compared, you must not use NewBloomFilterPolicy()
// and must provide your own FilterPolicy that also ignores the
// corresponding parts of the keys.  For example, if the comparator
// ignores trailing spaces, it would be incorrect to use a
// FilterPolicy (like NewBloomFilterPolicy) that does not ignore
// trailing spaces in keys.
extern const FilterPolicy *NewBloomFilterPolicy(int bits_per_key);

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy *);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice &key);
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy *policy_;
  std::string keys_;             // Flattened key contents
  std::vector<size_t> start_;    // Starting index in keys_ of each key
  std::string result_;           // Filter data computed so far
  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument
  std::vector<uint32_t> filter_offsets_;

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder &);
  void operator=(const FilterBlockBuilder &);
};

class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy *policy, const Slice &contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice &key);

 private:
  const FilterPolicy *policy_;
  const char *data_;    // Pointer to filter data (at block-start)
  const char *offset_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_BLOOM_BLOCK_H_
