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
#include "types/rc_data_types.h"

#include "common/assert.h"
#include "core/tools.h"
#include "vc/virtual_column.h"

namespace stonedb {
namespace types {
BString::BString()  // null string
{
  null = true;
  len = 0;
  val = 0;
  pos = 0;
  persistent = false;
}

BString::BString(const char *v, size_t length, bool persistent) : persistent(persistent) {
  // NOTE: we allow val to be NULL. In this case, no value will be copied (just
  // reserve a place for future use). Only persistent!
  pos = 0;
  null = false;
  if (length == 0) {
    if (v)
      len = std::strlen(v);
    else
      len = 0;
  } else
    len = (uint)length;
  if (persistent == false)
    val = const_cast<char *>(v);
  else {
    val = new char[len];
    if (v) std::memcpy(val, v, len);
  }
}

BString::BString(const BString &rcbs) : ValueBasic<BString>(rcbs), pos(rcbs.pos), persistent(rcbs.persistent) {
  null = rcbs.null;
  if (!null) {
    len = rcbs.len;
    if (persistent) {
      val = new char[len + pos];
      std::memcpy(val, rcbs.val, len + pos);
    } else
      val = rcbs.val;
  } else {
    len = 0;
    val = 0;
    pos = 0;
    persistent = false;
  }
}

BString::~BString() {
  if (persistent) delete[] val;
}

BString &BString::operator=(const RCDataType &rcdt) {
  if (this == &rcdt) return *this;

  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    *this = (BString &)rcdt;
  else
    STONEDB_ERROR("bad cast");

  return *this;
}

bool BString::Parse(BString &in, BString &out) {
  out = in;
  return true;
}

common::CT BString::Type() const { return common::CT::STRING; }

void BString::PutString(char *&dest, ushort len, bool move_ptr) const {
  ASSERT(this->len <= len, "should be 'this->len <= len'");
  if (this->len == 0)
    std::memset(dest, ' ', len);
  else {
    std::memcpy(dest, val, this->len);
    std::memset(dest + this->len, ' ', len - this->len);
  }
  if (move_ptr) dest += len;
}

void BString::PutVarchar(char *&dest, uchar prefixlen, bool move_ptr) const {
  if (prefixlen == 0) PutString(dest, len);
  if (len == 0) {
    std::memset(dest, 0, prefixlen);
    if (move_ptr) dest += prefixlen;
  } else {
    switch (prefixlen) {
      case 1:
        *(uchar *)dest = (uchar)len;
        break;
      case 2:
        *(ushort *)dest = (ushort)len;
        break;
      case 4:
        *(uint *)dest = (uint)len;
        break;
      default:
        STONEDB_ERROR("not implemented");
    }
    std::memcpy(dest + prefixlen, val, len);
    if (move_ptr) dest += prefixlen + len;
  }
}

BString &BString::operator=(const BString &rcbs) {
  if (this == &rcbs) return *this;

  null = rcbs.null;
  if (null) {
    if (persistent) delete[] val;
    val = 0;
    len = 0;
    pos = 0;
  } else {
    if (rcbs.persistent) {
      uint tmp_len = rcbs.len + rcbs.pos;
      if (!persistent || tmp_len > len + pos) {
        if (persistent) delete[] val;
        val = new char[tmp_len];
      }
      len = rcbs.len;
      pos = rcbs.pos;
      std::memcpy(val, rcbs.val, len + pos);
    } else {
      if (persistent) delete[] val;
      len = rcbs.len;
      pos = rcbs.pos;
      val = rcbs.val;
    }
  }
  persistent = rcbs.persistent;
  return *this;
}

void BString::PersistentCopy(const BString &rcbs) {
  if (this == &rcbs) {
    MakePersistent();
    return;
  }

  null = rcbs.null;
  if (null) {
    delete[] val;
    val = 0;
    len = 0;
    pos = 0;
  } else {
    uint tmp_len = rcbs.len + rcbs.pos;
    if (!persistent || tmp_len > len + pos) {
      if (persistent) delete[] val;
      val = new char[tmp_len];
    }
    len = rcbs.len;
    pos = rcbs.pos;
    std::memcpy(val, rcbs.val, len + pos);
  }
  persistent = true;
}

std::string BString::ToString() const {
  if (len) return std::string(val + pos, len);
  return std::string();
}

char &BString::operator[](size_t pos) const {
  DEBUG_ASSERT(pos < len);  // Out of BString. Note: val is not ended by '\0'.
  return val[this->pos + pos];
}

BString &BString::operator+=(ushort pos) {
  DEBUG_ASSERT((int)len - pos >= 0);
  this->pos = this->pos + (ushort)pos;
  this->len -= pos;
  return *this;
}

BString &BString::operator-=(ushort pos) {
  DEBUG_ASSERT(pos <= this->pos);
  this->pos = this->pos - (ushort)pos;
  this->len += pos;
  return *this;
}

bool BString::Like(const BString &pattern, char escape_character) {
  if (pattern.IsEmpty()) return this->IsEmpty();
  BString processed_pattern;  // to be used as an alternative source in case of
                              // processed pattern (escape chars)
  BString processed_wildcards;
  char *p = pattern.val;  // a short for pattern (or processed pattern)
  char *w = pattern.val;  // a short for wildcard map (or na original pattern,
                          // if no escape chars)
  char *v = val + pos;    // a short for the data itself
  uint pattern_len = pattern.len;

  // Escape characters processing
  bool escaped = false;
  for (uint i = 0; i < pattern_len - 1; i++) {
    if (p[i] == escape_character && (p[i + 1] == escape_character || p[i + 1] == '_' || p[i + 1] == '%')) {
      escaped = true;
      break;
    }
  }
  if (escaped) {  // redefine the pattern by processing escape characters
    processed_pattern = BString(NULL, pattern_len, true);
    processed_wildcards = BString(NULL, pattern_len, true);
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
    p = processed_pattern.val;
    w = processed_wildcards.val;
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
    do {                                            // internal loop: try to match a part between %...%
      while (cur_p < pattern_len && cur_s < len &&  // find the first match...
             (v[cur_s] == p[cur_p] || w[cur_p] == '_') && w[cur_p] != '%') {
        cur_s++;
        cur_p++;
      }
      if (cur_s < len &&
          ((cur_p < pattern_len && w[cur_p] != '%') || cur_p >= pattern_len)) {  // not matching (loop finished
        // prematurely) - try the next source
        // position
        if (!was_wild) return false;  // no % up to now => the first non-matching is critical
        cur_p = cur_p_beg;
        cur_s = ++cur_s_beg;  // step forward in the source, rewind the matching
                              // pointers
      }
      if (cur_s == len) {  // end of the source
        while (cur_p < pattern_len) {
          if (w[cur_p] != '%')  // Pattern nontrivial yet? No more chances for matching.
            return false;
          cur_p++;
        }
        return true;
      }
    } while (cur_p < pattern_len && w[cur_p] != '%');  // try the next match position
  } while (cur_p < pattern_len && cur_s < len);
  return true;
}

void BString::MakePersistent() {
  if (persistent) return;
  char *n_val = new char[len + pos];
  std::memcpy(n_val, val, len + pos);
  val = n_val;
  persistent = true;
}

bool BString::GreaterEqThanMin(const void *txt_min) {
  const unsigned char *s = reinterpret_cast<const unsigned char *>(txt_min);

  if (null == true) return false;
  uint min_len = 8;
  while (min_len > 0 && s[min_len - 1] == '\0') min_len--;
  for (uint i = 0; i < min_len && i < len; i++)
    if (((unsigned char *)val)[i + pos] < s[i])
      return false;
    else if (((unsigned char *)val)[i + pos] > s[i])
      return true;
  if (len < min_len) return false;
  return true;
}

bool BString::GreaterEqThanMinUTF(const void *txt_min, DTCollation col, bool use_full_len) {
  if (null == true) return false;
  if (RequiresUTFConversions(col)) {
    uint useful_len = 0;
    const char *s = reinterpret_cast<const char *>(txt_min);
    uint min_byte_len = std::memchr(s, 0, 8) ? std::strlen(s) : 8;
    if (!use_full_len) {
      uint min_charlen = col.collation->cset->numchars(col.collation, s, s + min_byte_len);

      uint next_char_len, chars_included = 0;
      while (true) {
        if (useful_len >= len || chars_included == min_charlen) break;
        next_char_len = col.collation->cset->mbcharlen(col.collation, (uchar)val[useful_len + pos]);
        DEBUG_ASSERT("wide character unrecognized" && next_char_len > 0);
        useful_len += next_char_len;
        chars_included++;
      }
    } else
      useful_len = len;
    return col.collation->coll->strnncoll(col.collation, (uchar *)val, useful_len,
                                          reinterpret_cast<const unsigned char *>(txt_min), min_byte_len, 0) >= 0;
  } else
    return GreaterEqThanMin(txt_min);
}

bool BString::LessEqThanMax(const void *txt_max) {
  if (null == true) return false;
  const unsigned char *s = reinterpret_cast<const unsigned char *>(txt_max);
  for (uint i = 0; i < 8 && i < len; i++)
    if (((unsigned char *)val)[i + pos] > s[i])
      return false;
    else if (((unsigned char *)val)[i + pos] < s[i])
      return true;
  return true;
}

bool BString::LessEqThanMaxUTF(const void *txt_max, DTCollation col, bool use_full_len) {
  if (null == true) return false;
  if (RequiresUTFConversions(col)) {
    uint useful_len = 0;
    const char *s = reinterpret_cast<const char *>(txt_max);
    uint max_byte_len = std::memchr(s, 0, 8) ? std::strlen(s) : (uint)8;
    if (!use_full_len) {
      uint max_charlen = col.collation->cset->numchars(col.collation, s, s + max_byte_len);

      uint next_char_len, chars_included = 0;
      while (true) {
        if (useful_len >= len || chars_included == max_charlen) break;
        next_char_len = col.collation->cset->mbcharlen(col.collation, (uchar)val[useful_len + pos]);
        DEBUG_ASSERT("wide character unrecognized" && next_char_len > 0);
        useful_len += next_char_len;
        chars_included++;
      }
    } else
      useful_len = len;
    return col.collation->coll->strnncoll(col.collation, (uchar *)val, useful_len,
                                          reinterpret_cast<const unsigned char *>(txt_max), max_byte_len, 0) <= 0;
  } else
    return LessEqThanMax(txt_max);
}

bool BString::IsEmpty() const {
  if (null == true) return false;
  return len == 0 ? true : false;
}

bool BString::IsNullOrEmpty() const { return ((len == 0 || null) ? true : false); }

bool BString::operator==(const RCDataType &rcdt) const {
  if (null || rcdt.IsNull()) return false;
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return CompareWith((BString &)rcdt) == 0;
  return CompareWith(rcdt.ToBString()) == 0;
}

bool BString::operator==(const BString &rcs) const {
  if (null || rcs.IsNull()) return false;
  return CompareWith(rcs) == 0;
}

bool BString::operator<(const RCDataType &rcdt) const {
  if (null || rcdt.IsNull()) return false;
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return CompareWith((BString &)rcdt) < 0;
  return CompareWith(rcdt.ToBString()) < 0;
}

bool BString::operator>(const RCDataType &rcdt) const {
  if (null || rcdt.IsNull()) return false;
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return CompareWith((BString &)rcdt) > 0;
  return CompareWith(rcdt.ToBString()) > 0;
}

bool BString::operator>=(const RCDataType &rcdt) const {
  if (null || rcdt.IsNull()) return false;
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return CompareWith((BString &)rcdt) >= 0;
  return CompareWith(rcdt.ToBString()) >= 0;
}

bool BString::operator<=(const RCDataType &rcdt) const {
  if (null || rcdt.IsNull()) return false;
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return CompareWith((BString &)rcdt) <= 0;
  return CompareWith(rcdt.ToBString()) <= 0;
}

bool BString::operator!=(const RCDataType &rcdt) const {
  if (null || rcdt.IsNull()) return true;
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return CompareWith((BString &)rcdt) != 0;
  return CompareWith(rcdt.ToBString()) != 0;
}

uint BString::GetHashCode() const {
  if (null) return 0;
  uint hc = 0;
  int a = 1040021;
  for (uint i = 0; i < len; i++) hc = (hc * a + val[i]) & 1048575;
  return hc;
}

std::ostream &operator<<(std::ostream &out, const BString &rcbs) {
  out.write(rcbs.val + rcbs.pos, rcbs.len);
  return out;
}

void BString::CopyTo(void *dest, size_t count) const {
  uint l = (len - pos) < count ? (len - pos) : count;
  std::memcpy(dest, val + pos, l);
  if (l <= count) std::memset((char *)dest + l, 0, count - l);
}

bool operator!=(const BString &rcbs1, const BString &rcbs2) {
  if (rcbs1.IsNull() || rcbs2.IsNull()) return true;
  return rcbs1.CompareWith(rcbs2) != 0;
}

size_t BString::RoundUpTo8Bytes(const DTCollation &dt) const {
  size_t useful_len = 0;
  if (dt.collation->mbmaxlen > 1) {
    int next_char_len;
    while (true) {
      if (useful_len >= len) break;
      next_char_len = dt.collation->cset->mbcharlen(dt.collation, (uchar)val[useful_len + pos]);

      if (next_char_len == 0) {
        STONEDB_LOG(LogCtl_Level::WARN, "RoundUpTo8Bytes() detect non-UTF8 character");
        next_char_len = 1;
      }

      DEBUG_ASSERT("wide character unrecognized" && next_char_len > 0);
      if (useful_len + next_char_len > 8) break;
      useful_len += next_char_len;
    }
  } else
    useful_len = len > 8 ? 8 : len;
  return useful_len;
}
}  // namespace types
}  // namespace stonedb
