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
#include "types/tianmu_data_types.h"

#include "common/assert.h"
#include "core/tools.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace types {

BString::BString()  // null_ string
{
  null_ = true;
  len_ = 0;
  val_ = 0;
  pos_ = 0;
  persistent_ = false;
}

BString::BString(const char *v, size_t length, bool persistent) : persistent_(persistent) {
  // NOTE: we allow val_ to be nullptr. In this case, no value will be copied (just
  // reserve a place for future use). Only persistent!
  pos_ = 0;
  null_ = false;
  if (length == 0) {
    if (v)
      len_ = std::strlen(v);
    else
      len_ = 0;
  } else
    len_ = (uint)length;
  if (persistent == false)
    val_ = const_cast<char *>(v);
  else {
    val_ = new char[len_];
    if (v)
      std::memcpy(val_, v, len_);
  }
}

BString::BString(const BString &tianmu_s)
    : ValueBasic<BString>(tianmu_s), pos_(tianmu_s.pos_), persistent_(tianmu_s.persistent_) {
  null_ = tianmu_s.null_;
  if (!null_) {
    len_ = tianmu_s.len_;
    if (persistent_) {
      val_ = new char[len_ + pos_];
      std::memcpy(val_, tianmu_s.val_, len_ + pos_);
    } else
      val_ = tianmu_s.val_;
  } else {
    len_ = 0;
    val_ = 0;
    pos_ = 0;
    persistent_ = false;
  }
}

BString::~BString() {
  if (persistent_)
    delete[] val_;
}

BString &BString::operator=(const TianmuDataType &tianmu_dt) {
  if (this == &tianmu_dt)
    return *this;

  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    *this = reinterpret_cast<BString &>(const_cast<TianmuDataType &>(tianmu_dt));
  else
    TIANMU_ERROR("bad cast");

  return *this;
}

bool BString::Parse(BString &in, BString &out) {
  out = in;
  return true;
}

common::ColumnType BString::Type() const { return common::ColumnType::STRING; }

void BString::PutString(char *&dest, ushort len, bool move_ptr) const {
  if (this->len_ > len) {
    ASSERT(this->len_ <= len, "should be 'this->len_ <= len'");
  }
  ASSERT(this->len_ <= len, "should be 'this->len_ <= len'");
  if (this->len_ == 0)
    std::memset(dest, ' ', len);
  else {
    std::memcpy(dest, val_, this->len_);
    std::memset(dest + this->len_, ' ', len - this->len_);
  }
  if (move_ptr)
    dest += len;
}

void BString::PutVarchar(char *&dest, uchar prefixlen, bool move_ptr) const {
  if (prefixlen == 0)
    PutString(dest, len_);
  if (len_ == 0) {
    std::memset(dest, 0, prefixlen);
    if (move_ptr)
      dest += prefixlen;
  } else {
    switch (prefixlen) {
      case 1:
        *(uchar *)dest = (uchar)len_;
        break;
      case 2:
        *(ushort *)dest = (ushort)len_;
        break;
      case 4:
        *(uint *)dest = (uint)len_;
        break;
      default:
        TIANMU_ERROR("not implemented");
    }
    std::memcpy(dest + prefixlen, val_, len_);
    if (move_ptr)
      dest += prefixlen + len_;
  }
}

BString &BString::operator=(const BString &tianmu_s) {
  if (this == &tianmu_s)
    return *this;

  null_ = tianmu_s.null_;
  if (null_) {
    if (persistent_)
      delete[] val_;
    val_ = 0;
    len_ = 0;
    pos_ = 0;
  } else {
    if (tianmu_s.persistent_) {
      uint tmp_len = tianmu_s.len_ + tianmu_s.pos_;
      if (!persistent_ || tmp_len > len_ + pos_) {
        if (persistent_)
          delete[] val_;
        val_ = new char[tmp_len];
      }
      len_ = tianmu_s.len_;
      pos_ = tianmu_s.pos_;
      std::memcpy(val_, tianmu_s.val_, len_ + pos_);
    } else {
      if (persistent_)
        delete[] val_;
      len_ = tianmu_s.len_;
      pos_ = tianmu_s.pos_;
      val_ = tianmu_s.val_;
    }
  }
  persistent_ = tianmu_s.persistent_;
  return *this;
}

void BString::PersistentCopy(const BString &tianmu_s) {
  if (this == &tianmu_s) {
    MakePersistent();
    return;
  }

  null_ = tianmu_s.null_;
  if (null_) {
    delete[] val_;
    val_ = 0;
    len_ = 0;
    pos_ = 0;
  } else {
    uint tmp_len = tianmu_s.len_ + tianmu_s.pos_;
    if (!persistent_ || tmp_len > len_ + pos_) {
      if (persistent_)
        delete[] val_;
      val_ = new char[tmp_len];
    }
    len_ = tianmu_s.len_;
    pos_ = tianmu_s.pos_;
    std::memcpy(val_, tianmu_s.val_, len_ + pos_);
  }
  persistent_ = true;
}

std::string BString::ToString() const {
  if (len_)
    return std::string(val_ + pos_, len_);
  return std::string();
}

char &BString::operator[](size_t pos) const {
  DEBUG_ASSERT(pos < len_);  // Out of BString. Note: val_ is not ended by '\0'.
  return val_[this->pos_ + pos];
}

BString &BString::operator+=(ushort pos) {
  DEBUG_ASSERT((int)len_ - pos >= 0);
  this->pos_ = this->pos_ + (ushort)pos;
  this->len_ -= pos;
  return *this;
}

BString &BString::operator-=(ushort pos) {
  DEBUG_ASSERT(pos <= this->pos_);
  this->pos_ = this->pos_ - (ushort)pos;
  this->len_ += pos;
  return *this;
}

bool BString::Like(const BString &pattern, char escape_character) {
  if (pattern.IsEmpty())
    return this->IsEmpty();
  BString processed_pattern;  // to be used as an alternative source in case of
                              // processed pattern (escape chars)
  BString processed_wildcards;
  char *p = pattern.val_;  // a short for pattern (or processed pattern)
  char *w = pattern.val_;  // a short for wildcard map (or na original pattern,
                           // if no escape chars)
  char *v = val_ + pos_;   // a short for the data itself
  uint pattern_len = pattern.len_;

  // Escape characters processing
  bool escaped = false;
  for (uint i = 0; i < pattern_len - 1; i++) {
    if (p[i] == escape_character && (p[i + 1] == escape_character || p[i + 1] == '_' || p[i + 1] == '%')) {
      escaped = true;
      break;
    }
  }
  if (escaped) {  // redefine the pattern by processing escape characters
    processed_pattern = BString(nullptr, pattern_len, true);
    processed_wildcards = BString(nullptr, pattern_len, true);
    uint i = 0;  // position of the processed pattern
    uint j = 0;  // position of the original pattern
    while (j < pattern_len) {
      if (j < pattern_len - 1 && p[j] == escape_character &&
          (p[j + 1] == escape_character || p[j + 1] == '_' || p[j + 1] == '%')) {
        j++;
        processed_pattern[i] = p[j];
        processed_wildcards[i] = ' ';
        j++;
        i++;
      } else {
        processed_pattern[i] = p[j];
        if (p[j] == '_' || p[j] == '%')
          processed_wildcards[i] = p[j];  // copy only wildcards
        else
          processed_wildcards[i] = ' ';
        j++;
        i++;
      }
    }
    pattern_len = i;  // the rest of pattern buffers are just ignored
    p = processed_pattern.val_;
    w = processed_wildcards.val_;
  }

  // Pattern processing
  bool was_wild = false;          // are we in "after %" mode?
  uint cur_p = 0, cur_p_beg = 0;  // pattern positions
  uint cur_s = 0, cur_s_beg = 0;  // source positions

  do {
    while (cur_p < pattern_len && w[cur_p] == '%') {  // first omit all %
      was_wild = true;
      cur_p++;
    }
    cur_s_beg = cur_s;
    cur_p_beg = cur_p;
    do {                                             // internal loop: try to match a part between %...%
      while (cur_p < pattern_len && cur_s < len_ &&  // find the first match...
             (v[cur_s] == p[cur_p] || w[cur_p] == '_') && w[cur_p] != '%') {
        cur_s++;
        cur_p++;
      }
      if (cur_s < len_ &&
          ((cur_p < pattern_len && w[cur_p] != '%') || cur_p >= pattern_len)) {  // not matching (loop finished
        // prematurely) - try the next source
        // position
        if (!was_wild)
          return false;  // no % up to now => the first non-matching is critical
        cur_p = cur_p_beg;
        cur_s = ++cur_s_beg;  // step forward in the source, rewind the matching
                              // pointers
      }
      if (cur_s == len_) {  // end of the source
        while (cur_p < pattern_len) {
          if (w[cur_p] != '%')  // Pattern nontrivial yet? No more chances for matching.
            return false;
          cur_p++;
        }
        return true;
      }
    } while (cur_p < pattern_len && w[cur_p] != '%');  // try the next match position
  } while (cur_p < pattern_len && cur_s < len_);
  return true;
}

void BString::MakePersistent() {
  if (persistent_)
    return;
  char *n_val = new char[len_ + pos_];
  std::memcpy(n_val, val_, len_ + pos_);
  val_ = n_val;
  persistent_ = true;
}

bool BString::GreaterEqThanMin(const void *txt_min) {
  const unsigned char *s = reinterpret_cast<const unsigned char *>(txt_min);

  if (null_ == true)
    return false;
  uint min_len = 8;
  while (min_len > 0 && s[min_len - 1] == '\0') min_len--;
  for (uint i = 0; i < min_len && i < len_; i++)
    if (((unsigned char *)val_)[i + pos_] < s[i])
      return false;
    else if (((unsigned char *)val_)[i + pos_] > s[i])
      return true;
  if (len_ < min_len)
    return false;
  return true;
}

bool BString::GreaterEqThanMinUTF(const void *txt_min, DTCollation col, bool use_full_len) {
  if (null_ == true)
    return false;
  if (RequiresUTFConversions(col)) {
    uint useful_len = 0;
    const char *s = reinterpret_cast<const char *>(txt_min);
    uint min_byte_len = std::memchr(s, 0, 8) ? std::strlen(s) : 8;
    if (!use_full_len) {
      uint min_charlen = col.collation->cset->numchars(col.collation, s, s + min_byte_len);

      uint next_char_len, chars_included = 0;
      while (true) {
        if (useful_len >= len_ || chars_included == min_charlen)
          break;
        next_char_len = col.collation->cset->mbcharlen(col.collation, (uchar)val_[useful_len + pos_]);
        DEBUG_ASSERT("wide character unrecognized" && next_char_len > 0);
        useful_len += next_char_len;
        chars_included++;
      }
    } else
      useful_len = len_;
    return col.collation->coll->strnncoll(col.collation, (uchar *)val_, useful_len,
                                          reinterpret_cast<const unsigned char *>(txt_min), min_byte_len, 0) >= 0;
  } else
    return GreaterEqThanMin(txt_min);
}

bool BString::LessEqThanMax(const void *txt_max) {
  if (null_ == true)
    return false;
  const unsigned char *s = reinterpret_cast<const unsigned char *>(txt_max);
  for (uint i = 0; i < 8 && i < len_; i++)
    if (((unsigned char *)val_)[i + pos_] > s[i])
      return false;
    else if (((unsigned char *)val_)[i + pos_] < s[i])
      return true;
  return true;
}

bool BString::LessEqThanMaxUTF(const void *txt_max, DTCollation col, bool use_full_len) {
  if (null_ == true)
    return false;
  if (RequiresUTFConversions(col)) {
    uint useful_len = 0;
    const char *s = reinterpret_cast<const char *>(txt_max);
    uint max_byte_len = std::memchr(s, 0, 8) ? std::strlen(s) : (uint)8;
    if (!use_full_len) {
      uint max_charlen = col.collation->cset->numchars(col.collation, s, s + max_byte_len);

      uint next_char_len, chars_included = 0;
      while (true) {
        if (useful_len >= len_ || chars_included == max_charlen)
          break;
        next_char_len = col.collation->cset->mbcharlen(col.collation, (uchar)val_[useful_len + pos_]);
        DEBUG_ASSERT("wide character unrecognized" && next_char_len > 0);
        useful_len += next_char_len;
        chars_included++;
      }
    } else
      useful_len = len_;
    return col.collation->coll->strnncoll(col.collation, (uchar *)val_, useful_len,
                                          reinterpret_cast<const unsigned char *>(txt_max), max_byte_len, 0) <= 0;
  } else
    return LessEqThanMax(txt_max);
}

bool BString::IsEmpty() const {
  if (null_ == true)
    return false;
  return len_ == 0 ? true : false;
}

bool BString::IsNullOrEmpty() const { return ((len_ == 0 || null_) ? true : false); }

bool BString::operator==(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return CompareWith(reinterpret_cast<BString &>(const_cast<TianmuDataType &>(tianmu_dt))) == 0;
  return CompareWith(tianmu_dt.ToBString()) == 0;
}

bool BString::operator==(const BString &tianmu_s) const {
  if (null_ || tianmu_s.IsNull())
    return false;
  return CompareWith(tianmu_s) == 0;
}

bool BString::operator<(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return CompareWith(reinterpret_cast<BString &>(const_cast<TianmuDataType &>(tianmu_dt))) < 0;
  return CompareWith(tianmu_dt.ToBString()) < 0;
}

bool BString::operator>(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return CompareWith(reinterpret_cast<BString &>(const_cast<TianmuDataType &>(tianmu_dt))) > 0;
  return CompareWith(tianmu_dt.ToBString()) > 0;
}

bool BString::operator>=(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return CompareWith(reinterpret_cast<BString &>(const_cast<TianmuDataType &>(tianmu_dt))) >= 0;
  return CompareWith(tianmu_dt.ToBString()) >= 0;
}

bool BString::operator<=(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return CompareWith(reinterpret_cast<BString &>(const_cast<TianmuDataType &>(tianmu_dt))) <= 0;
  return CompareWith(tianmu_dt.ToBString()) <= 0;
}

bool BString::operator!=(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return true;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return CompareWith(reinterpret_cast<BString &>(const_cast<TianmuDataType &>(tianmu_dt))) != 0;
  return CompareWith(tianmu_dt.ToBString()) != 0;
}

uint BString::GetHashCode() const {
  if (null_)
    return 0;
  uint hc = 0;
  int a = 1040021;
  for (uint i = 0; i < len_; i++) hc = (hc * a + val_[i]) & 1048575;
  return hc;
}

std::ostream &operator<<(std::ostream &out, const BString &tianmu_s) {
  out.write(tianmu_s.val_ + tianmu_s.pos_, tianmu_s.len_);
  return out;
}

void BString::CopyTo(void *dest, size_t count) const {
  uint l = (len_ - pos_) < count ? (len_ - pos_) : count;
  std::memcpy(dest, val_ + pos_, l);
  if (l <= count)
    std::memset((char *)dest + l, 0, count - l);
}

bool operator!=(const BString &tianmu_s1, const BString &tianmu_s2) {
  if (tianmu_s1.IsNull() || tianmu_s2.IsNull())
    return true;
  return tianmu_s1.CompareWith(tianmu_s2) != 0;
}

size_t BString::RoundUpTo8Bytes(const DTCollation &dt) const {
  size_t useful_len = 0;
  if (dt.collation->mbmaxlen > 1) {
    int next_char_len;
    while (true) {
      if (useful_len >= len_)
        break;
      next_char_len = dt.collation->cset->mbcharlen(dt.collation, (uchar)val_[useful_len + pos_]);

      if (next_char_len == 0) {
        TIANMU_LOG(LogCtl_Level::WARN, "RoundUpTo8Bytes() detect non-UTF8 character");
        next_char_len = 1;
      }

      DEBUG_ASSERT("wide character unrecognized" && next_char_len > 0);
      if (useful_len + next_char_len > 8)
        break;
      useful_len += next_char_len;
    }
  } else
    useful_len = len_ > 8 ? 8 : len_;

  return useful_len;
}

}  // namespace types
}  // namespace Tianmu
