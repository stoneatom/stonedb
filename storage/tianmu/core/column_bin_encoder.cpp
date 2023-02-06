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

#include "column_bin_encoder.h"
#include "core/bin_tools.h"
#include "core/mi_iterator.h"
#include "core/transaction.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
ColumnBinEncoder::ColumnBinEncoder(int flags) {
  ignore_nulls = ((flags & ENCODER_IGNORE_NULLS) != 0);
  monotonic_encoding = ((flags & ENCODER_MONOTONIC) != 0);
  noncomparable = ((flags & ENCODER_NONCOMPARABLE) != 0);
  descending = ((flags & ENCODER_DESCENDING) != 0);
  decodable = ((flags & ENCODER_DECODABLE) != 0);

  implicit = false;
  disabled = false;

  vc = nullptr;
  val_offset = 0;
  val_sec_offset = 0;
  val_size = 0;
  val_sec_size = 0;
}

ColumnBinEncoder::ColumnBinEncoder(const ColumnBinEncoder &sec) {
  implicit = sec.implicit;
  ignore_nulls = sec.ignore_nulls;
  monotonic_encoding = sec.monotonic_encoding;
  noncomparable = sec.noncomparable;
  descending = sec.descending;
  decodable = sec.decodable;
  disabled = sec.disabled;
  vc = sec.vc;
  val_offset = sec.val_offset;
  val_sec_offset = sec.val_sec_offset;
  val_size = sec.val_size;
  val_sec_size = sec.val_sec_size;
  dup_col = sec.dup_col;
  if (sec.my_encoder.get())
    my_encoder.reset(sec.my_encoder->Copy());
  else
    my_encoder.reset();
}

ColumnBinEncoder &ColumnBinEncoder::operator=(const ColumnBinEncoder &sec) {
  if (&sec != this) {
    ignore_nulls = sec.ignore_nulls;
    monotonic_encoding = sec.monotonic_encoding;
    noncomparable = sec.noncomparable;
    descending = sec.descending;
    decodable = sec.decodable;
    disabled = sec.disabled;
    implicit = sec.implicit;
    vc = sec.vc;
    val_offset = sec.val_offset;
    val_sec_offset = sec.val_sec_offset;
    val_size = sec.val_size;
    val_sec_size = sec.val_sec_size;
    dup_col = sec.dup_col;
    my_encoder = std::move(sec.my_encoder);
  }
  return *this;
}

ColumnBinEncoder::~ColumnBinEncoder() {
  if (vc && !implicit && dup_col == -1)
    vc->UnlockSourcePacks();
}

bool ColumnBinEncoder::PrepareEncoder(vcolumn::VirtualColumn *_vc, vcolumn::VirtualColumn *_vc2) {
  if (_vc == nullptr)
    return false;
  bool nulls_possible = false;
  if (!ignore_nulls)
    nulls_possible = _vc->IsNullsPossible() || (_vc2 != nullptr && _vc2->IsNullsPossible());
  vc = _vc;
  ColumnType vct = vc->Type();
  ColumnType vct2 = _vc2 ? _vc2->Type() : ColumnType();
  bool lookup_encoder = false;
  bool text_stat_encoder = false;
  if (vct.IsFixed() && (!_vc2 || (vct2.IsFixed() && vct.GetScale() == vct2.GetScale()))) {  // int/dec of the same scale
    my_encoder.reset(new ColumnBinEncoder::EncoderInt(vc, decodable, nulls_possible, descending));
  } else if (vct.IsFloat() || (_vc2 && vct2.IsFloat())) {
    my_encoder.reset(new ColumnBinEncoder::EncoderDouble(vc, decodable, nulls_possible, descending));
  } else if (vct.IsFixed() && !vct2.IsString()) {  // Decimals for different scale (the same
                                                   // scale is done by EncoderInt)
    my_encoder.reset(new ColumnBinEncoder::EncoderDecimal(vc, decodable, nulls_possible, descending));
  } else if (vct.GetTypeName() == common::ColumnType::DATE) {
    my_encoder.reset(new ColumnBinEncoder::EncoderDate(vc, decodable, nulls_possible, descending));
  } else if (vct.GetTypeName() == common::ColumnType::YEAR) {
    my_encoder.reset(new ColumnBinEncoder::EncoderYear(vc, decodable, nulls_possible, descending));
  } else if (!monotonic_encoding && vct.Lookup() && _vc2 == nullptr &&
             !types::RequiresUTFConversions(vc->GetCollation())) {  // Lookup encoding: only non-UTF
    my_encoder.reset(new ColumnBinEncoder::EncoderLookup(vc, decodable, nulls_possible, descending));
    lookup_encoder = true;
  } else if (!monotonic_encoding && vct.Lookup() && _vc2 != nullptr &&
             vct2.Lookup()) {  // Lookup in joining - may be UTF
    my_encoder.reset(new ColumnBinEncoder::EncoderLookup(vc, decodable, nulls_possible, descending));
    lookup_encoder = true;
  } else if (vct.IsString() || vct2.IsString()) {
    DTCollation col_v1 = vc->GetCollation();
    DTCollation coll = col_v1;
    if (_vc2) {
      DTCollation col_v2 = _vc2->GetCollation();
      coll = types::ResolveCollation(col_v1, col_v2);
    }
    if (!noncomparable)  // noncomparable => non-sorted cols in sorter, don't
                         // UTF-encode.
      my_encoder.reset(new ColumnBinEncoder::EncoderText_UTF(vc, decodable, nulls_possible, descending));
    else {
      my_encoder.reset(new ColumnBinEncoder::EncoderTextStat(vc, decodable, nulls_possible, descending));
      if (!my_encoder->Valid()) {
        if (!monotonic_encoding && !decodable && vc->MaxStringSize() > HASH_FUNCTION_BYTE_SIZE)
          my_encoder.reset(new ColumnBinEncoder::EncoderTextMD5(vc, decodable, nulls_possible, descending));
        else
          my_encoder.reset(new ColumnBinEncoder::EncoderText(vc, decodable, nulls_possible, descending));
      } else
        text_stat_encoder = true;
    }
  } else if (vct.IsDateTime() && (!_vc2 || vct2.IsDateTime())) {  // Date/time types except special cases
                                                                  // (above)
    my_encoder.reset(new ColumnBinEncoder::EncoderInt(vc, decodable, nulls_possible, descending));
  } else {
    DEBUG_ASSERT(!"wrong combination of encoded columns");  // Other types not
                                                            // implemented yet
    my_encoder.reset(new ColumnBinEncoder::EncoderText(vc, decodable, nulls_possible, descending));
  }
  if (_vc2 != nullptr) {  // multiple column encoding?
    bool encoding_possible = my_encoder->SecondColumn(_vc2);
    if (!encoding_possible) {
      bool second_try = false;
      if (lookup_encoder) {  // try to use text encoder instead of lookup  not
                             // need use EncoderText_UTF
        my_encoder.reset(new ColumnBinEncoder::EncoderText(vc, decodable, nulls_possible, descending));
        second_try = my_encoder->SecondColumn(_vc2);
      }
      if (text_stat_encoder) {
        if (!monotonic_encoding && !decodable && vc->MaxStringSize() > HASH_FUNCTION_BYTE_SIZE)
          my_encoder.reset(new ColumnBinEncoder::EncoderTextMD5(vc, decodable, nulls_possible, descending));
        else
          my_encoder.reset(new ColumnBinEncoder::EncoderText(vc, decodable, nulls_possible, descending));
        second_try = my_encoder->SecondColumn(_vc2);
      }
      if (!second_try)
        return false;
    }
  }
  val_size = my_encoder->ValueSize();
  val_sec_size = my_encoder->ValueSizeSec();
  return true;
}

void ColumnBinEncoder::Encode(uchar *buf, MIIterator &mit, vcolumn::VirtualColumn *alternative_vc, bool update_stats) {
  if (implicit)
    return;
  my_encoder->Encode(buf + val_offset, buf + val_sec_offset, (alternative_vc ? alternative_vc : vc), mit, update_stats);
}

bool ColumnBinEncoder::PutValue64(uchar *buf, int64_t v, bool sec_column, bool update_stats) {
  if (implicit)
    return false;
  return my_encoder->EncodeInt64(buf + val_offset, buf + val_sec_offset, v, sec_column, update_stats);
}

bool ColumnBinEncoder::PutValueString(uchar *buf, types::BString &v, bool sec_column, bool update_stats) {
  if (implicit)
    return false;
  return my_encoder->EncodeString(buf + val_offset, buf + val_sec_offset, v, sec_column, update_stats);
}

int64_t ColumnBinEncoder::ValEncode(MIIterator &mit, bool update_stats) {
  if (implicit)
    return common::NULL_VALUE_64;
  return my_encoder->ValEncode(vc, mit, update_stats);
}

int64_t ColumnBinEncoder::ValPutValue64(int64_t v, bool update_stats) {
  if (implicit)
    return common::NULL_VALUE_64;
  return my_encoder->ValEncodeInt64(v, update_stats);
}

int64_t ColumnBinEncoder::ValPutValueString(types::BString &v, bool update_stats) {
  if (implicit)
    return common::NULL_VALUE_64;
  return my_encoder->ValEncodeString(v, update_stats);
}

bool ColumnBinEncoder::IsString() { return my_encoder->IsString(); }

int64_t ColumnBinEncoder::GetValue64(uchar *buf, const MIDummyIterator &mit, bool &is_null) {
  is_null = false;
  if (implicit) {
    vc->LockSourcePacks(mit);
    int64_t v = vc->GetValueInt64(mit);
    if (v == common::NULL_VALUE_64)
      is_null = true;
    return v;
  }
  if (my_encoder->IsNull(buf + val_offset, buf + val_sec_offset)) {
    is_null = true;
    return common::NULL_VALUE_64;
  }
  return my_encoder->GetValue64(buf + val_offset, buf + val_sec_offset);
}

types::BString ColumnBinEncoder::GetValueT(uchar *buf, const MIDummyIterator &mit) {
  if (implicit) {
    vc->LockSourcePacks(mit);
    types::BString s;
    if (vc->IsNull(mit))
      return s;
    vc->GetNotNullValueString(s, mit);
    return s;
  }
  if (my_encoder->IsNull(buf + val_offset, buf + val_sec_offset))
    return types::BString();
  return my_encoder->GetValueT(buf + val_offset, buf + val_sec_offset);
}

void ColumnBinEncoder::UpdateStatistics(unsigned char *buf)  // get value from the buffer and update internal statistics
{
  DEBUG_ASSERT(!implicit);
  my_encoder->UpdateStatistics(buf + val_offset);
}

bool ColumnBinEncoder::ImpossibleValues(int64_t pack_min,
                                        int64_t pack_max)  // return true if the current contents of the encoder is
                                                           // out of scope
{
  if (implicit)
    return false;
  return my_encoder->ImpossibleInt64Values(pack_min, pack_max);
}

bool ColumnBinEncoder::ImpossibleValues(types::BString &pack_min, types::BString &pack_max) {
  return my_encoder->ImpossibleStringValues(pack_min, pack_max);
}

void ColumnBinEncoder::ClearStatistics() { my_encoder->ClearStatistics(); }

int64_t ColumnBinEncoder::MaxCode()  // Maximal integer code, if it makes any
                                     // sense (or common::NULL_VALUE_64)
{
  return my_encoder->MaxCode();
}

void ColumnBinEncoder::LoadPacks(MIIterator *mit) {
  if (IsEnabled() && IsNontrivial() && dup_col == -1)  // constant num. columns are trivial
    LockSourcePacks(*mit);
}

ColumnBinEncoder::EncoderInt::EncoderInt(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible,
                                         bool _descending, bool calc_int_statistics)
    : ColumnValueEncoder(vc, decodable, nulls_possible, _descending) {
  min_val = 0;
  if (calc_int_statistics) {  // false if the column is actually not an integer
                              // (e.g. dec, date, textstat) and stats are
                              // calculated elsewhere
    min_val = vc->RoughMin();
    max_code = uint64_t(vc->RoughMax() - min_val);
    if (nulls_possible) {
      if (max_code < UINT64_MAX) {
        null_status = 1;  // 0 is null
        max_code++;
      } else
        null_status = 2;  // separate byte
    } else
      null_status = 0;
    size = CalculateByteSize(max_code) + (null_status == 2 ? 1 : 0);
    size_sec = 0;
  }
  min_found = common::PLUS_INF_64;
  max_found = common::MINUS_INF_64;
}

bool ColumnBinEncoder::EncoderInt::SecondColumn(vcolumn::VirtualColumn *vc) {
  if (!vc->Type().IsFixed() && !(this->vc_type.IsDateTime() && vc->Type().IsDateTime())) {
    tianmu_control_.lock(vc->ConnInfo()->GetThreadID())
        << "Nontrivial comparison: date/time with non-date/time" << system::unlock;
    return false;
  }
  if ((this->vc_type.IsDateTime()) ^ (vc->Type().IsDateTime()))  // support datetime/timestamp
    return false;                                                // union timestamp/datetime
  // Easy case: integers/decimals with the same precision
  int64_t new_min_val = vc->RoughMin();
  int64_t max_val = max_code + min_val - (null_status == 1 ? 1 : 0);
  int64_t new_max_val = vc->RoughMax();
  if (min_val > new_min_val)
    min_val = new_min_val;
  if (max_val < new_max_val)
    max_val = new_max_val;
  max_code = uint64_t(max_val - min_val);
  if (null_status == 1 && max_code == (UINT64_MAX))
    null_status = 2;  // separate byte
  size = CalculateByteSize(max_code) + (null_status == 2 ? 1 : 0);
  return true;
}

void ColumnBinEncoder::EncoderInt::Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
                                          bool update_stats) {
  if (null_status > 0 && vc->IsNull(mit))
    SetNull(buf, buf_sec);
  else
    EncodeInt64(buf, buf_sec, vc->GetNotNullValueInt64(mit), false, update_stats);
}

bool ColumnBinEncoder::EncoderInt::EncodeInt64(uchar *buf, uchar *buf_sec, int64_t v, [[maybe_unused]] bool sec_column,
                                               bool update_stats) {
  if (null_status > 0 && v == common::NULL_VALUE_64) {
    SetNull(buf, buf_sec);
    return true;
  }
  uint64_t coded_val;
  int loc_size = size;
  if (update_stats) {
    if (v > max_found)
      max_found = v;
    if (v < min_found)
      min_found = v;
  }
  coded_val = uint64_t(v - min_val) + (null_status == 1 ? 1 : 0);
  if (descending)
    coded_val = max_code - coded_val;
  if (null_status == 2) {
    *buf = (descending ? '\0' : '\1');  // not null
    buf++;
    loc_size--;
  }
  uchar *val_ptr = (uchar *)(&coded_val) + loc_size;  // the end of meaningful bytes
  for (int i = 0; i < loc_size; i++)
    *(buf++) = *(--val_ptr);  // change endianess - to make numbers comparable
                              // by std::memcmp()
  return true;
}

int64_t ColumnBinEncoder::EncoderInt::ValEncode(vcolumn::VirtualColumn *vc, MIIterator &mit, bool update_stats) {
  if (null_status > 0 && vc->IsNull(mit))
    return ValEncodeInt64(common::NULL_VALUE_64, update_stats);
  return ValEncodeInt64(vc->GetNotNullValueInt64(mit), update_stats);
}

int64_t ColumnBinEncoder::EncoderInt::ValEncodeInt64(int64_t v, bool update_stats) {
  DEBUG_ASSERT(null_status < 2);  // should be used only for small values, when
                                  // an additional byte is not needed
  if (v == common::NULL_VALUE_64) {
    if (descending)
      return max_code;
    return 0;
  }
  if (update_stats) {
    if (v > max_found)
      max_found = v;
    if (v < min_found)
      min_found = v;
  }
  int64_t coded_val = (v - min_val) + (null_status == 1 ? 1 : 0);
  if (descending)
    return max_code - coded_val;
  return coded_val;
}

void ColumnBinEncoder::EncoderInt::SetNull(uchar *buf, [[maybe_unused]] uchar *buf_sec) {
  // Assume three bytes of int
  // mode              |   null    |   not null  |
  // ---------------------------------------------
  // status=1, ascend. |    000    |    xxx      |
  // status=2, ascend. |   0000    |   1xxx      |
  // status=1, descend.|    max    |    xxx      |
  // status=2, descend.|   1000    |   0xxx      |
  // ---------------------------------------------

  DEBUG_ASSERT(null_status > 0);
  std::memset(buf, 0,
              size);  // zero on the first and all other bytes (to be changed below)
  if (descending) {
    if (null_status == 1) {
      uchar *val_ptr = (uchar *)(&max_code) + size;  // the end of meaningful bytes
      for (uint i = 0; i < size; i++)
        *(buf++) = *(--val_ptr);  // change endianess - to make numbers
                                  // comparable by std::memcmp()
    } else {
      *buf = '\1';
    }
  }
}

bool ColumnBinEncoder::EncoderInt::IsNull(uchar *buf, [[maybe_unused]] uchar *buf_sec) {
  if (null_status == 0)
    return false;
  uint64_t zero = 0;
  if (descending) {
    if (null_status == 1) {
      uint64_t coded_val = 0;
      uchar *val_ptr = (uchar *)(&coded_val) + size;            // the end of meaningful bytes
      for (uint i = 0; i < size; i++) *(--val_ptr) = *(buf++);  // change endianess
      return (coded_val == max_code);
    } else
      return (*buf == '\1');
  } else {
    if (null_status == 1)
      return (std::memcmp(buf, &zero, size) == 0);
    else
      return (*buf == '\0');
  }
}

int64_t ColumnBinEncoder::EncoderInt::GetValue64(uchar *buf, [[maybe_unused]] uchar *buf_sec) {
  int loc_size = size;
  if (null_status == 2) {
    if ((!descending && *buf == '\0') || (descending && *buf == '\1'))
      return common::NULL_VALUE_64;
    buf++;
    loc_size--;
  }
  uint64_t coded_val = 0;
  uchar *val_ptr = (uchar *)(&coded_val) + loc_size;           // the end of meaningful bytes
  for (int i = 0; i < loc_size; i++) *(--val_ptr) = *(buf++);  // change endianess

  if (descending)
    coded_val = max_code - coded_val;
  return coded_val - (null_status == 1 ? 1 : 0) + min_val;
}

void ColumnBinEncoder::EncoderInt::UpdateStatistics(unsigned char *buf) {
  int64_t v = GetValue64(buf, nullptr);
  if (null_status > 0 && v == common::NULL_VALUE_64)
    return;
  if (v > max_found)
    max_found = v;
  if (v < min_found)
    min_found = v;
}

bool ColumnBinEncoder::EncoderInt::ImpossibleInt64Values(int64_t pack_min, int64_t pack_max) {
  if (pack_min == common::NULL_VALUE_64 || pack_max == common::NULL_VALUE_64 || min_found == common::PLUS_INF_64)
    return false;
  if (pack_min > max_found || pack_max < min_found)
    return true;
  return false;
}

void ColumnBinEncoder::EncoderInt::ClearStatistics() {
  min_found = common::PLUS_INF_64;
  max_found = common::MINUS_INF_64;
}

ColumnBinEncoder::EncoderDecimal::EncoderDecimal(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible,
                                                 bool _descending)
    : EncoderInt(vc, decodable, nulls_possible, _descending, false) {
  multiplier = 1.0;
  sec_multiplier = 1.0;
  scale = vc->Type().GetScale();
  int64_t pmin = vc->RoughMin();
  int64_t pmax = vc->RoughMax();
  min_val = pmin;
  max_code = uint64_t(pmax - pmin);
  if (nulls_possible) {
    if (max_code < UINT64_MAX) {
      null_status = 1;  // 0 is null
      max_code++;
    } else
      null_status = 2;  // separate byte
  } else
    null_status = 0;
  size = CalculateByteSize(max_code);
  size_sec = 0;
}

bool ColumnBinEncoder::EncoderDecimal::SecondColumn(vcolumn::VirtualColumn *vc) {
  int64_t max_val = max_code + min_val - (null_status == 1 ? 1 : 0);
  int64_t new_min_val = vc->RoughMin();
  int64_t new_max_val = vc->RoughMax();
  int new_scale = vc->Type().GetScale();
  if (new_scale > scale) {
    multiplier = types::PowOfTen(new_scale - scale);
    min_val *= int64_t(multiplier);
    max_val *= int64_t(multiplier);
  } else if (new_scale < scale) {
    sec_multiplier = types::PowOfTen(scale - new_scale);
    new_min_val *= int64_t(sec_multiplier);
    new_max_val *= int64_t(sec_multiplier);
  }
  scale = std::max(new_scale, scale);
  if (min_val > new_min_val)
    min_val = new_min_val;
  if (max_val < new_max_val)
    max_val = new_max_val;
  max_code = uint64_t(max_val - min_val);
  if (null_status == 1 && max_code == (UINT64_MAX))
    null_status = 2;  // separate byte
  size = CalculateByteSize(max_code) + (null_status == 2 ? 1 : 0);
  return true;
}

void ColumnBinEncoder::EncoderDecimal::Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
                                              bool update_stats /*= false*/) {
  if (null_status > 0 && vc->IsNull(mit))
    SetNull(buf, buf_sec);
  else {
    int64_t v = vc->GetNotNullValueInt64(mit);
    if (vc->Type().GetScale() < scale)
      v *= int64_t(types::PowOfTen(scale - vc->Type().GetScale()));
    EncoderInt::EncodeInt64(buf, buf_sec, v, false, update_stats);
  }
}

bool ColumnBinEncoder::EncoderDecimal::EncodeInt64(uchar *buf, uchar *buf_sec, int64_t v, bool sec_column,
                                                   bool update_stats) {
  if (null_status > 0 && v == common::NULL_VALUE_64)
    SetNull(buf, buf_sec);
  else {
    if (v != common::PLUS_INF_64 && v != common::MINUS_INF_64)
      v *= int64_t(sec_column ? sec_multiplier : multiplier);
    return EncoderInt::EncodeInt64(buf, buf_sec, v, false, update_stats);
  }
  return true;
}

bool ColumnBinEncoder::EncoderDecimal::ImpossibleInt64Values(int64_t pack_min, int64_t pack_max) {
  if (pack_min == common::NULL_VALUE_64 || pack_max == common::NULL_VALUE_64 || min_found == common::PLUS_INF_64)
    return false;
  if (pack_min != common::MINUS_INF_64)
    pack_min *= int64_t(sec_multiplier);  // assuming that pack_min, pack_max
                                          // always belong to the second column!
  if (pack_max != common::PLUS_INF_64)
    pack_max *= int64_t(sec_multiplier);
  if (pack_min > max_found || pack_max < min_found)
    return true;
  return false;
}

ColumnBinEncoder::EncoderDate::EncoderDate(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible,
                                           bool _descending)
    : EncoderInt(vc, decodable, nulls_possible, _descending, false) {
  int64_t pmin = types::DT::DateSortEncoding(vc->RoughMin());
  int64_t pmax = types::DT::DateSortEncoding(vc->RoughMax());
  min_val = pmin;
  max_code = uint64_t(pmax - pmin);
  if (nulls_possible) {
    null_status = 1;  // 0 is null - because dates never reach max. int.
    if (max_code != uint64_t(UINT64_MAX))
      max_code++;
  } else
    null_status = 0;
  size = CalculateByteSize(max_code);
  size_sec = 0;
}

bool ColumnBinEncoder::EncoderDate::SecondColumn(vcolumn::VirtualColumn *vc) {
  // Possible conversions: support datetime/timestamp/date.
  if (!vc->Type().IsDateTime()) {
    tianmu_control_.lock(vc->ConnInfo()->GetThreadID())
        << "Nontrivial comparison: date with non-datetime." << system::unlock;
    return false;
  }
  int64_t new_min_val = types::DT::DateSortEncoding(vc->RoughMin());
  int64_t max_val = max_code + min_val - (null_status == 1 ? 1 : 0);
  int64_t new_max_val = types::DT::DateSortEncoding(vc->RoughMax());
  if (min_val > new_min_val)
    min_val = new_min_val;
  if (max_val < new_max_val)
    max_val = new_max_val;
  max_code = uint64_t(max_val - min_val) + (null_status == 1 && max_code != UINT64_MAX ? 1 : 0);
  ;
  size = CalculateByteSize(max_code);
  return true;
}

void ColumnBinEncoder::EncoderDate::Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
                                           bool update_stats) {
  if (null_status > 0 && vc->IsNull(mit))
    SetNull(buf, buf_sec);
  else
    EncoderInt::EncodeInt64(buf, buf_sec, types::DT::DateSortEncoding(vc->GetNotNullValueInt64(mit)), false,
                            update_stats);
}

bool ColumnBinEncoder::EncoderDate::EncodeInt64(uchar *buf, uchar *buf_sec, int64_t v, [[maybe_unused]] bool sec_column,
                                                bool update_stats) {
  if (null_status > 0 && v == common::NULL_VALUE_64)
    SetNull(buf, buf_sec);
  else
    return EncoderInt::EncodeInt64(buf, buf_sec, types::DT::DateSortEncoding(v), false, update_stats);
  return true;
}

int64_t ColumnBinEncoder::EncoderDate::ValEncode(vcolumn::VirtualColumn *vc, MIIterator &mit, bool update_stats) {
  if (null_status > 0 && vc->IsNull(mit))
    return EncoderInt::ValEncodeInt64(common::NULL_VALUE_64, update_stats);
  return EncoderInt::ValEncodeInt64(types::DT::DateSortEncoding(vc->GetNotNullValueInt64(mit)), update_stats);
}

int64_t ColumnBinEncoder::EncoderDate::ValEncodeInt64(int64_t v, bool update_stats) {
  if (null_status > 0 && v == common::NULL_VALUE_64)
    return EncoderInt::ValEncodeInt64(common::NULL_VALUE_64, update_stats);
  return EncoderInt::ValEncodeInt64(types::DT::DateSortEncoding(v), update_stats);
}

int64_t ColumnBinEncoder::EncoderDate::GetValue64(uchar *buf, uchar *buf_sec) {
  if (IsNull(buf, buf_sec))
    return common::NULL_VALUE_64;
  return types::DT::DateSortDecoding(EncoderInt::GetValue64(buf, buf_sec));
}

void ColumnBinEncoder::EncoderDate::UpdateStatistics(unsigned char *buf) {
  int64_t v = EncoderInt::GetValue64(buf, nullptr);
  if (null_status > 0 && v == common::NULL_VALUE_64)
    return;
  if (v > max_found)  // min/max_found as types::DT::DateSortEncoding
                      // values
    max_found = v;
  if (v < min_found)
    min_found = v;
}

bool ColumnBinEncoder::EncoderDate::ImpossibleInt64Values(int64_t pack_min, int64_t pack_max) {
  if (pack_min == common::NULL_VALUE_64 || pack_max == common::NULL_VALUE_64 || min_found == common::PLUS_INF_64)
    return false;
  if (types::DT::DateSortEncoding(pack_min) > max_found || types::DT::DateSortEncoding(pack_max) < min_found)
    return true;
  return false;
}

ColumnBinEncoder::EncoderYear::EncoderYear(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible,
                                           bool _descending)
    : EncoderInt(vc, decodable, nulls_possible, _descending, false) {
  int64_t pmin = types::DT::YearSortEncoding(vc->RoughMin());
  int64_t pmax = types::DT::YearSortEncoding(vc->RoughMax());
  min_val = pmin;
  max_code = uint64_t(pmax - pmin);
  if (nulls_possible) {
    null_status = 1;  // 0 is null - because years never reach max. int.
    if (max_code != UINT64_MAX)
      max_code++;
  } else
    null_status = 0;
  size = CalculateByteSize(max_code);
  size_sec = 0;
}

bool ColumnBinEncoder::EncoderYear::SecondColumn(vcolumn::VirtualColumn *vc) {
  // Possible conversions: only years.
  if (vc->Type().GetTypeName() != common::ColumnType::YEAR) {
    tianmu_control_.lock(vc->ConnInfo()->GetThreadID())
        << "Nontrivial comparison: year with non-year" << system::unlock;
    return false;
  }

  int64_t new_min_val = types::DT::YearSortEncoding(vc->RoughMin());
  int64_t max_val = max_code + min_val - (null_status == 1 ? 1 : 0);
  int64_t new_max_val = types::DT::YearSortEncoding(vc->RoughMax());
  if (min_val > new_min_val)
    min_val = new_min_val;
  if (max_val < new_max_val)
    max_val = new_max_val;
  max_code = uint64_t(max_val - min_val);
  if (max_code != UINT64_MAX && null_status == 1)
    max_code++;
  size = CalculateByteSize(max_code);
  return true;
}

void ColumnBinEncoder::EncoderYear::Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
                                           bool update_stats) {
  if (null_status > 0 && vc->IsNull(mit))
    SetNull(buf, buf_sec);
  else
    EncoderInt::EncodeInt64(buf, buf_sec, types::DT::YearSortEncoding(vc->GetNotNullValueInt64(mit)), false,
                            update_stats);
}

bool ColumnBinEncoder::EncoderYear::EncodeInt64(uchar *buf, uchar *buf_sec, int64_t v, [[maybe_unused]] bool sec_column,
                                                bool update_stats) {
  if (null_status > 0 && v == common::NULL_VALUE_64)
    SetNull(buf, buf_sec);
  else
    return EncoderInt::EncodeInt64(buf, buf_sec, types::DT::YearSortEncoding(v), false, update_stats);
  return true;
}

int64_t ColumnBinEncoder::EncoderYear::ValEncode(vcolumn::VirtualColumn *vc, MIIterator &mit, bool update_stats) {
  if (null_status > 0 && vc->IsNull(mit))
    return EncoderInt::ValEncodeInt64(common::NULL_VALUE_64, update_stats);
  return EncoderInt::ValEncodeInt64(types::DT::YearSortEncoding(vc->GetNotNullValueInt64(mit)), update_stats);
}

int64_t ColumnBinEncoder::EncoderYear::ValEncodeInt64(int64_t v, bool update_stats) {
  if (null_status > 0 && v == common::NULL_VALUE_64)
    return EncoderInt::ValEncodeInt64(common::NULL_VALUE_64, update_stats);
  return EncoderInt::ValEncodeInt64(types::DT::YearSortEncoding(v), update_stats);
}

int64_t ColumnBinEncoder::EncoderYear::GetValue64(uchar *buf, uchar *buf_sec) {
  if (IsNull(buf, buf_sec))
    return common::NULL_VALUE_64;
  return types::DT::YearSortDecoding(EncoderInt::GetValue64(buf, buf_sec));
}

void ColumnBinEncoder::EncoderYear::UpdateStatistics(unsigned char *buf) {
  int64_t v = EncoderInt::GetValue64(buf, nullptr);
  if (null_status > 0 && v == common::NULL_VALUE_64)
    return;
  if (v > max_found)  // min/max_found as types::DT::YearSortEncoding
                      // values
    max_found = v;
  if (v < min_found)
    min_found = v;
}

bool ColumnBinEncoder::EncoderYear::ImpossibleInt64Values(int64_t pack_min, int64_t pack_max) {
  if (pack_min == common::NULL_VALUE_64 || pack_max == common::NULL_VALUE_64 || min_found == common::PLUS_INF_64)
    return false;
  if (types::DT::YearSortEncoding(pack_min) > max_found || types::DT::YearSortEncoding(pack_max) < min_found)
    return true;
  return false;
}

ColumnBinEncoder::EncoderDouble::EncoderDouble(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible,
                                               bool _descending)
    : ColumnValueEncoder(vc, decodable, nulls_possible, _descending) {
  multiplier_vc1 = 0;  // 0 if the first vc is double, otherwise it is regarded
                       // as int (decimal) which must be multiplied
  multiplier_vc2 = 0;  // 0 if the second vc is double, otherwise it is regarded
                       // as int (decimal) which must be multiplied
  if (vc->Type().IsFixed())
    multiplier_vc1 = int64_t(types::PowOfTen(vc->Type().GetScale()));  // encode int/dec as double
  if (nulls_possible) {
    null_status = 2;
    size = 9;
  } else {
    null_status = 0;
    size = 8;
  }
}

bool ColumnBinEncoder::EncoderDouble::SecondColumn(vcolumn::VirtualColumn *vc) {
  // Possible conversions: all numericals.
  if (!vc->Type().IsFixed() && !vc->Type().IsFloat()) {
    tianmu_control_.lock(vc->ConnInfo()->GetThreadID())
        << "Nontrivial comparison: floating-point with non-numeric" << system::unlock;
    return false;
  }
  if (vc->Type().IsFixed())
    multiplier_vc2 = int64_t(types::PowOfTen(vc->Type().GetScale()));  // encode int/dec as double
  return true;
}

void ColumnBinEncoder::EncoderDouble::Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
                                             [[maybe_unused]] bool update_stats) {
  if (null_status == 2) {
    if (vc->IsNull(mit)) {
      SetNull(buf, buf_sec);
      return;
    }
    *buf = (descending ? '\0' : '\1');  // not null
    buf++;
  }
  double val = vc->GetValueDouble(mit);  // note that non-float columns will be properly converted here
  int64_t coded_val = MonotonicDouble2Int64(*(int64_t *)&val);
  if (descending)
    Negate((uchar *)&coded_val, 8);
  uchar *val_ptr = (uchar *)(&coded_val) + 8;  // the end of meaningful bytes
  for (int i = 0; i < 8; i++)
    *(buf++) = *(--val_ptr);  // change endianess - to make numbers comparable
                              // by std::memcmp()
}

bool ColumnBinEncoder::EncoderDouble::EncodeInt64(uchar *buf, uchar *buf_sec, int64_t v, bool sec_column,
                                                  [[maybe_unused]] bool update_stats) {
  if (null_status == 2) {
    if (v == common::NULL_VALUE_64) {
      SetNull(buf, buf_sec);
      return true;
    }
    *buf = (descending ? '\0' : '\1');  // not null
    buf++;
  }
  int64_t local_mult = (sec_column ? multiplier_vc2 : multiplier_vc1);
  int64_t coded_val;
  if (local_mult == 0)
    coded_val = MonotonicDouble2Int64(v);
  else {
    double d = double(v) / local_mult;  // decimal encoded as double
    coded_val = MonotonicDouble2Int64(*((int64_t *)(&d)));
  }
  if (descending)
    Negate((uchar *)&coded_val, 8);
  uchar *val_ptr = (uchar *)(&coded_val) + 8;  // the end of meaningful bytes
  for (int i = 0; i < 8; i++)
    *(buf++) = *(--val_ptr);  // change endianess - to make numbers comparable
                              // by std::memcmp()
  return true;
}

void ColumnBinEncoder::EncoderDouble::SetNull(uchar *buf, [[maybe_unused]] uchar *buf_sec) {
  // mode              |   null    |   not null  |
  // ---------------------------------------------
  // status=2, ascend. |   00..00  |   1<64bit>  |
  // status=2, descend.|   10..00  |   0<64bit>  |
  // ---------------------------------------------
  DEBUG_ASSERT(null_status == 2);
  std::memset(buf, 0, 9);
  if (descending)
    *buf = '\1';
}

bool ColumnBinEncoder::EncoderDouble::IsNull(uchar *buf, [[maybe_unused]] uchar *buf_sec) {
  if (null_status != 2)
    return false;
  if (descending)
    return (*buf == '\1');
  return (*buf == '\0');
}

int64_t ColumnBinEncoder::EncoderDouble::GetValue64(uchar *buf, [[maybe_unused]] uchar *buf_sec) {
  if (null_status == 2) {
    if ((!descending && *buf == '\0') || (descending && *buf == '\1'))
      return common::NULL_VALUE_64;
    buf++;
  }
  uint64_t coded_val = 0;
  uchar *val_ptr = (uchar *)(&coded_val) + 8;           // the end of meaningful bytes
  for (int i = 0; i < 8; i++) *(--val_ptr) = *(buf++);  // change endianess
  if (descending)
    Negate((uchar *)&coded_val, 8);
  return MonotonicInt642Double(coded_val);
}

/*
               Type               length  MySQL
             ----------------------------------------
                 TINYTEXT        256 bytes
                 TEXT               65,535 bytes	~64kb
                 MEDIUMTEXT   16,777,215 bytes  ~16MB
                 LONGTEXT       4,294,967,295 bytes	~4GB
            -----------------------------------------
*/

ColumnBinEncoder::EncoderText::EncoderText(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible,
                                           bool _descending)
    : ColumnValueEncoder(vc, decodable, nulls_possible, _descending),
      min_max_set(false)

{
  size = vc->MaxStringSize() + sizeof(uint32_t);  // 4 bytes for len( 4G)
  size_sec = 0;
  null_status = (nulls_possible ? 1 : 0);
}

ColumnBinEncoder::EncoderText::~EncoderText() {}

bool ColumnBinEncoder::EncoderText::SecondColumn(vcolumn::VirtualColumn *vc) {
  size = std::max(size,
                  vc->MaxStringSize() + sizeof(uint32_t));  // 4 bytes for len
  return true;
}

void ColumnBinEncoder::EncoderText::Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
                                           bool update_stats) {
  if (null_status > 0 && vc->IsNull(mit)) {
    SetNull(buf, buf_sec);
    return;
  }
  std::memset(buf, 0, size);
  types::BString s;
  vc->GetNotNullValueString(s, mit);
  if (update_stats) {
    if (!min_max_set) {
      maxs.PersistentCopy(s);
      mins.PersistentCopy(s);
      min_max_set = true;
    } else {
      if (s > maxs)
        maxs.PersistentCopy(s);
      if (s < mins)
        mins.PersistentCopy(s);
    }
  }
  ASSERT(s.len_ <= (uint)size, "Size of buffer too small");
  if (s.len_ > 0)
    std::memcpy(buf, s.GetDataBytesPointer(), s.len_);
  uint32_t length = s.len_ + 1;
  std::memcpy(buf + size - sizeof(uint32_t), &length, sizeof(uint32_t));
  if (descending)
    Negate(buf, size);
}

bool ColumnBinEncoder::EncoderText::EncodeString(uchar *buf, uchar *buf_sec, types::BString &s,
                                                 [[maybe_unused]] bool sec_column, bool update_stats) {
  if (null_status > 0 && s.IsNull()) {
    SetNull(buf, buf_sec);
    return true;
  }
  std::memset(buf, 0, size);
  if (update_stats) {
    if (!min_max_set) {
      maxs.PersistentCopy(s);
      mins.PersistentCopy(s);
      min_max_set = true;
    } else {
      if (s > maxs)
        maxs.PersistentCopy(s);
      if (s < mins)
        mins.PersistentCopy(s);
    }
  }
  ASSERT(s.len_ <= (uint)size, "Size of buffer too small");
  if (s.len_ > 0)
    std::memcpy(buf, s.GetDataBytesPointer(), s.len_);
  uint32_t length = s.len_ + 1;
  std::memcpy(buf + size - sizeof(uint32_t), &length, sizeof(uint32_t));
  if (descending)
    Negate(buf, size);
  return true;
}

void ColumnBinEncoder::EncoderText::SetNull(uchar *buf, [[maybe_unused]] uchar *buf_sec) {
  if (descending)
    std::memset(buf, 255, size);
  else
    std::memset(buf, 0, size);
}

bool ColumnBinEncoder::EncoderText::IsNull(uchar *buf, [[maybe_unused]] uchar *buf_sec) {
  if (descending)
    return (*(reinterpret_cast<uint32_t *>(buf + size - sizeof(uint32_t))) == 0xffffffff);
  return (*(reinterpret_cast<uint32_t *>(buf + size - sizeof(uint32_t))) == 0);
}

types::BString ColumnBinEncoder::EncoderText::GetValueT(uchar *buf, uchar *buf_sec) {
  if (IsNull(buf, buf_sec))
    return types::BString();
  if (descending)
    Negate(buf, size);
  uint32_t len = *(reinterpret_cast<uint32_t *>(buf + size - sizeof(uint32_t))) - 1;
  if (len == 0)
    return types::BString("");
  return types::BString((char *)buf,
                        len);  // the types::BString is generated as temporary
}

bool ColumnBinEncoder::EncoderText::ImpossibleStringValues(types::BString &pack_min, types::BString &pack_max) {
  int lenmin = std::min(pack_min.len_, maxs.len_);
  int lenmax = std::min(pack_max.len_, mins.len_);

  if (std::strncmp(pack_min.val_, maxs.val_, lenmin) > 0 || std::strncmp(pack_max.val_, mins.val_, lenmax) < 0)
    return true;
  return false;
}

ColumnBinEncoder::EncoderText_UTF::EncoderText_UTF(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible,
                                                   bool _descending)
    : ColumnValueEncoder(vc, decodable, nulls_possible, _descending), min_max_set(false) {
  collation = vc->GetCollation();
  uint coded_len = types::CollationBufLen(collation, vc->MaxStringSize());
  size = coded_len + sizeof(uint32_t);  // 4 bytes for len
  size_sec = 0;
  if (decodable)
    size_sec = vc->MaxStringSize() + sizeof(uint32_t);  // just a raw data plus len
  null_status = (nulls_possible ? 1 : 0);
}

ColumnBinEncoder::EncoderText_UTF::~EncoderText_UTF() {}

bool ColumnBinEncoder::EncoderText_UTF::SecondColumn(vcolumn::VirtualColumn *vc2) {
  if (vc_type.IsString() && vc2->Type().IsString() && collation.collation != vc2->GetCollation().collation) {
    tianmu_control_.lock(vc2->ConnInfo()->GetThreadID())
        << "Nontrivial comparison: " << collation.collation->name << " with " << vc2->GetCollation().collation->name
        << system::unlock;
    return false;
  }
  size_t coded_len = types::CollationBufLen(collation, vc2->MaxStringSize());
  size = std::max(size, coded_len + sizeof(uint32_t));  // 4 bytes for len
  if (size_sec > 0)
    size_sec = std::max(size_sec, vc2->MaxStringSize() + sizeof(uint32_t));  // 4 bytes for len
  return true;
}

void ColumnBinEncoder::EncoderText_UTF::Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
                                               bool update_stats) {
  if (null_status > 0 && vc->IsNull(mit)) {
    SetNull(buf, buf_sec);
    return;
  }
  std::memset(buf, 0, size);
  types::BString s;
  vc->GetNotNullValueString(s, mit);
  if (update_stats) {
    if (!min_max_set) {
      maxs.PersistentCopy(s);
      mins.PersistentCopy(s);
      min_max_set = true;
    } else {
      if (CollationStrCmp(collation, s, maxs) > 0)
        maxs.PersistentCopy(s);
      if (CollationStrCmp(collation, s, mins) < 0)
        mins.PersistentCopy(s);
    }
  }
  common::strnxfrm(collation, buf, size - sizeof(uint32_t), (uchar *)s.GetDataBytesPointer(), s.len_);
  // int coded_len = types::CollationBufLen(collation, s.len);
  uint32_t length = s.len_ + 1;
  std::memcpy(buf + size - sizeof(uint32_t), &length, sizeof(uint32_t));
  if (descending)
    Negate(buf, size);
  if (size_sec > 0) {
    std::memset(buf_sec, 0, size_sec);
    std::memcpy(buf_sec + size_sec - sizeof(uint32_t), &length, sizeof(uint32_t));
    if (s.len_ > 0)
      std::memcpy(buf_sec, s.GetDataBytesPointer(), s.len_);
  }
}

bool ColumnBinEncoder::EncoderText_UTF::EncodeString(uchar *buf, uchar *buf_sec, types::BString &s,
                                                     [[maybe_unused]] bool sec_column, bool update_stats) {
  if (null_status > 0 && s.IsNull()) {
    SetNull(buf, buf_sec);
    return true;
  }
  std::memset(buf, 0, size);
  if (update_stats) {
    if (!min_max_set) {
      maxs = s;
      mins = s;
      min_max_set = true;
    } else {
      if (CollationStrCmp(collation, s, maxs) > 0)
        maxs = s;
      if (CollationStrCmp(collation, s, mins) < 0)
        mins = s;
    }
  }
  common::strnxfrm(collation, buf, size - sizeof(uint32_t), (uchar *)s.GetDataBytesPointer(), s.len_);
  uint32_t length = s.len_ + 1;
  std::memcpy(buf + size - sizeof(uint32_t), &length, sizeof(uint32_t));
  if (descending)
    Negate(buf, size);
  if (size_sec > 0) {
    std::memset(buf_sec, 0, size_sec);
    std::memcpy(buf_sec + size_sec - sizeof(uint32_t), &length, sizeof(uint32_t));
    if (s.len_ > 0)
      std::memcpy(buf_sec, s.GetDataBytesPointer(), s.len_);
  }
  return true;
}

void ColumnBinEncoder::EncoderText_UTF::SetNull(uchar *buf, uchar *buf_sec) {
  if (descending)
    std::memset(buf, 255, size);
  else
    std::memset(buf, 0, size);
  if (size_sec > 0)
    std::memset(buf_sec, 0, size_sec);
}

bool ColumnBinEncoder::EncoderText_UTF::IsNull([[maybe_unused]] uchar *buf, uchar *buf_sec) {
  DEBUG_ASSERT(size_sec > 0);
  return (*(reinterpret_cast<uint32_t *>(buf_sec + size_sec - sizeof(uint32_t))) == 0);
}

types::BString ColumnBinEncoder::EncoderText_UTF::GetValueT(uchar *buf, uchar *buf_sec) {
  DEBUG_ASSERT(size_sec > 0);
  if (null_status > 0 && IsNull(buf, buf_sec))
    return types::BString();
  uint32_t len = *(reinterpret_cast<uint32_t *>(buf_sec + size_sec - sizeof(uint32_t))) - 1;
  if (len == 0)
    return types::BString("");
  else
    return types::BString((char *)buf_sec, len,
                          false);  // the types::BString is generated as temporary
}

bool ColumnBinEncoder::EncoderText_UTF::ImpossibleStringValues(types::BString &pack_min, types::BString &pack_max) {
  unsigned char min[8] = {};
  unsigned char max[8] = {};
  std::memcpy(min, pack_min.val_, pack_min.len_);
  std::memcpy(max, pack_max.val_, pack_max.len_);
  if (!maxs.GreaterEqThanMinUTF(min, collation) || !mins.LessEqThanMaxUTF(max, collation))
    return true;
  return false;
}

ColumnBinEncoder::EncoderLookup::EncoderLookup(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible,
                                               bool _descending)
    : EncoderInt(vc, decodable, nulls_possible, _descending, false) {
  int64_t pmin = vc->RoughMin();
  int64_t pmax = vc->RoughMax();
  min_val = pmin;
  max_code = uint64_t(pmax - pmin);
  // 0 is always nullptr for lookup (because we must encode non-existing strings)
  null_status = 1;  // 0 is null
  max_code++;
  size = CalculateByteSize(max_code);
  size_sec = 0;
  first_vc = vc;
  sec_vc = nullptr;
  translate2 = nullptr;
  no_sec_values = -1;
}

ColumnBinEncoder::EncoderLookup::EncoderLookup(const EncoderLookup &sec) : EncoderInt(sec) {
  no_sec_values = sec.no_sec_values;
  first_vc = sec.first_vc;
  sec_vc = sec.sec_vc;
  if (sec.translate2) {
    translate2 = new int[no_sec_values];
    std::memcpy(translate2, sec.translate2, no_sec_values * sizeof(int));
  } else
    translate2 = nullptr;
}

ColumnBinEncoder::EncoderLookup::~EncoderLookup() { delete[] translate2; }

bool ColumnBinEncoder::EncoderLookup::SecondColumn(vcolumn::VirtualColumn *vc) {
  sec_vc = vc;
  int64_t max_val = max_code + min_val - 1;
  int64_t new_min_val = vc->RoughMin();
  int64_t new_max_val = vc->RoughMax();
  no_sec_values = (int)new_max_val + 1;
  delete[] translate2;
  translate2 = new int[no_sec_values];
  DTCollation collation = first_vc->GetCollation();
  if (collation.collation != sec_vc->GetCollation().collation)
    return false;
  if (types::RequiresUTFConversions(collation)) {
    for (int i = 0; i < no_sec_values; i++) {
      int code = -1;
      types::BString val2 = sec_vc->DecodeValue_S(i);
      for (int j = (int)min_val; j < max_val + 1; j++) {
        types::BString val1 = first_vc->DecodeValue_S(j);
        if (CollationStrCmp(collation, val1, val2) == 0) {
          if (code != -1)
            return false;  // ambiguous translation - cannot encode properly
          code = j;
        }
      }
      translate2[i] = code;
    }
  } else {  // binary encoding: internal dictionary search may be used
    for (int i = 0; i < no_sec_values; i++) {
      types::BString val2 = sec_vc->DecodeValue_S(i);
      int code = first_vc->EncodeValue_S(val2);  // common::NULL_VALUE_32 if not found
      translate2[i] = (code < 0 ? -1 : code);
    }
  }
  if (min_val > new_min_val)
    min_val = new_min_val;
  if (max_val < new_max_val)
    max_val = new_max_val;
  max_code = uint64_t(max_val - min_val);
  size = CalculateByteSize(max_code);
  return true;
}

types::BString ColumnBinEncoder::EncoderLookup::GetValueT(unsigned char *buf, unsigned char *buf_sec) {
  if (IsNull(buf, buf_sec))
    return types::BString();
  int64_t v = EncoderInt::GetValue64(buf, buf_sec);
  return first_vc->DecodeValue_S(v);
}

void ColumnBinEncoder::EncoderLookup::Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
                                             bool update_stats /*= false*/) {
  if (null_status > 0 && vc->IsNull(mit))
    SetNull(buf, buf_sec);
  else {
    int64_t v = vc->GetNotNullValueInt64(mit);
    if (vc == sec_vc) {
      v = translate2[v];
      if (v == -1) {
        SetNull(buf, buf_sec);
        return;
      }
    }
    EncoderInt::EncodeInt64(buf, buf_sec, v, false, update_stats);
  }
}

bool ColumnBinEncoder::EncoderLookup::EncodeInt64(uchar *buf, uchar *buf_sec, int64_t v, bool sec_column,
                                                  bool update_stats) {
  if (null_status > 0 && v == common::NULL_VALUE_64)
    SetNull(buf, buf_sec);
  else {
    if (sec_column) {
      v = translate2[v];
      if (v == -1)
        v = common::NULL_VALUE_64;  // value not found
    }
    return EncoderInt::EncodeInt64(buf, buf_sec, v, false, update_stats);
  }
  return true;
}

bool ColumnBinEncoder::EncoderLookup::ImpossibleInt64Values(int64_t pack_min, int64_t pack_max) {
  // assuming that pack_min, pack_max always belong to the second column!
  // Calculate min. and max. codes in the v1 encoding
  int64_t pack_v1_min = common::PLUS_INF_64;
  int64_t pack_v1_max = common::MINUS_INF_64;
  if (pack_min < 0)
    pack_min = 0;
  if (pack_max > no_sec_values - 1)
    pack_max = no_sec_values - 1;
  for (int i = (int)pack_min; i <= pack_max; i++) {
    if (pack_v1_min > translate2[i])
      pack_v1_min = translate2[i];
    if (pack_v1_max < translate2[i])
      pack_v1_max = translate2[i];
  }
  if (pack_v1_min != common::PLUS_INF_64 && (pack_v1_min > max_found || pack_v1_max < min_found))
    return true;
  return false;
}

ColumnBinEncoder::EncoderTextStat::EncoderTextStat(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible,
                                                   bool _descending)
    : EncoderInt(vc, decodable, nulls_possible, _descending, false) {
  vc->GetTextStat(coder);
  coder.CreateEncoding();
  valid = coder.IsValid();
  min_val = 0;
  max_code = coder.MaxCode() + 2;  // +1 for a PLUS_INF value, +1 for nullptr or MINUS_INF value
  null_status = 1;                 // 0 is null
  size = CalculateByteSize(max_code);
  size_sec = 0;
}

bool ColumnBinEncoder::EncoderTextStat::SecondColumn(vcolumn::VirtualColumn *vc) {
  vc->GetTextStat(coder);
  coder.CreateEncoding();  // may be created again only if there was no decoding
                           // in the meantime
  valid = coder.IsValid();
  min_val = 0;
  max_code = coder.MaxCode() + 2;  // +1 for a PLUS_INF value, +1 for nullptr or MINUS_INF value
  size = CalculateByteSize(max_code);
  return valid;
}

types::BString ColumnBinEncoder::EncoderTextStat::GetValueT(unsigned char *buf, unsigned char *buf_sec) {
  if (IsNull(buf, buf_sec))
    return types::BString();
  int64_t v = EncoderInt::GetValue64(buf, buf_sec);
  return coder.Decode(v);
}

void ColumnBinEncoder::EncoderTextStat::Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
                                               bool update_stats /*= false*/) {
  if (null_status > 0 && vc->IsNull(mit))
    SetNull(buf, buf_sec);
  else {
    types::BString vs;
    vc->GetValueString(vs, mit);
    int64_t v = coder.Encode(vs);
    EncoderInt::EncodeInt64(buf, buf_sec, v, false, update_stats);
  }
}

bool ColumnBinEncoder::EncoderTextStat::EncodeString(uchar *buf, uchar *buf_sec, types::BString &s,
                                                     [[maybe_unused]] bool sec_column, bool update_stats) {
  if (null_status > 0 && s.IsNull())
    SetNull(buf, buf_sec);
  else {
    int64_t v = coder.Encode(s);
    return EncoderInt::EncodeInt64(buf, buf_sec, v, false, update_stats);
  }
  return true;
}

int64_t ColumnBinEncoder::EncoderTextStat::ValEncode(vcolumn::VirtualColumn *vc, MIIterator &mit, bool update_stats) {
  if (null_status > 0 && vc->IsNull(mit))
    return EncoderInt::ValEncodeInt64(common::NULL_VALUE_64, update_stats);
  types::BString vs;
  vc->GetValueString(vs, mit);
  int64_t v = coder.Encode(vs);
  return EncoderInt::ValEncodeInt64(v, update_stats);
}

bool ColumnBinEncoder::EncoderTextStat::ImpossibleStringValues(types::BString &pack_min, types::BString &pack_max) {
  int64_t v_min = coder.Encode(pack_min);
  int64_t v_max = coder.Encode(pack_max, true);
  return EncoderInt::ImpossibleInt64Values(v_min, v_max);
}

ColumnBinEncoder::EncoderTextMD5::EncoderTextMD5(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible,
                                                 bool _descending)
    : ColumnValueEncoder(vc, decodable, nulls_possible, _descending),
      min_max_set(false)

{
  size = HASH_FUNCTION_BYTE_SIZE;
  size_sec = 0;
  null_status = 1;  // ignored
  DEBUG_ASSERT(!decodable);
  std::memset(null_buf, 0, HASH_FUNCTION_BYTE_SIZE);
  std::memset(empty_buf, 0, HASH_FUNCTION_BYTE_SIZE);
  std::memcpy(null_buf, "-- null --",
              10);  // constant values for special purposes
  std::memcpy(empty_buf, "-- empty --", 11);
}

ColumnBinEncoder::EncoderTextMD5::~EncoderTextMD5() {}

ColumnBinEncoder::EncoderTextMD5::EncoderTextMD5(const EncoderTextMD5 &sec)
    : ColumnValueEncoder(sec), mins(sec.mins), maxs(sec.maxs), min_max_set(sec.min_max_set) {
  size = HASH_FUNCTION_BYTE_SIZE;
  std::memset(null_buf, 0, HASH_FUNCTION_BYTE_SIZE);
  std::memset(empty_buf, 0, HASH_FUNCTION_BYTE_SIZE);
  std::memcpy(null_buf, "-- null --",
              10);  // constant values for special purposes
  std::memcpy(empty_buf, "-- empty --", 11);
}

bool ColumnBinEncoder::EncoderTextMD5::SecondColumn([[maybe_unused]] vcolumn::VirtualColumn *vc) { return true; }

void ColumnBinEncoder::EncoderTextMD5::Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
                                              bool update_stats) {
  if (vc->IsNull(mit)) {
    SetNull(buf, buf_sec);
    return;
  }
  types::BString s;
  vc->GetNotNullValueString(s, mit);
  if (update_stats) {
    if (!min_max_set) {
      maxs.PersistentCopy(s);
      mins.PersistentCopy(s);
      min_max_set = true;
    } else {
      if (s > maxs)
        maxs.PersistentCopy(s);
      if (s < mins)
        mins.PersistentCopy(s);
    }
  }
  if (s.len_ > 0) {
    HashMD5((unsigned char *)s.GetDataBytesPointer(), s.len_, buf);
    *((uint *)buf) ^= s.len_;
  } else
    std::memcpy(buf, empty_buf, size);
}

bool ColumnBinEncoder::EncoderTextMD5::EncodeString(uchar *buf, uchar *buf_sec, types::BString &s,
                                                    [[maybe_unused]] bool sec_column, bool update_stats) {
  if (s.IsNull()) {
    SetNull(buf, buf_sec);
    return true;
  }
  if (update_stats) {
    if (!min_max_set) {
      maxs.PersistentCopy(s);
      mins.PersistentCopy(s);
      min_max_set = true;
    } else {
      if (s > maxs)
        maxs.PersistentCopy(s);
      if (s < mins)
        mins.PersistentCopy(s);
    }
  }
  if (s.len_ > 0) {
    HashMD5((unsigned char *)s.GetDataBytesPointer(), s.len_, buf);
    *((uint *)buf) ^= s.len_;
  } else
    std::memcpy(buf, empty_buf, size);
  return true;
}

void ColumnBinEncoder::EncoderTextMD5::SetNull(uchar *buf, [[maybe_unused]] uchar *buf_sec) {
  std::memcpy(buf, null_buf, size);
}

bool ColumnBinEncoder::EncoderTextMD5::IsNull(uchar *buf, [[maybe_unused]] uchar *buf_sec) {
  return (std::memcmp(buf, null_buf, size) == 0);
}

bool ColumnBinEncoder::EncoderTextMD5::ImpossibleStringValues(types::BString &pack_min, types::BString &pack_max) {
  int lenmin = std::min(pack_min.len_, maxs.len_);
  int lenmax = std::min(pack_max.len_, mins.len_);

  if (std::strncmp(pack_min.val_, maxs.val_, lenmin) > 0 || std::strncmp(pack_max.val_, mins.val_, lenmax) < 0)
    return true;
  return false;
}

MultiindexPositionEncoder::MultiindexPositionEncoder(MultiIndex *mind, DimensionVector &dims) {
  val_size = 0;
  val_offset = 0;
  for (int i = 0; i < mind->NumOfDimensions(); i++)
    if (dims[i]) {
      dim_no.push_back(i);
      auto loc_size = DimByteSize(mind, i);
      dim_size.push_back(loc_size);
      dim_offset.push_back(0);  // must be set by SetPrimaryOffset()
      val_size += loc_size;
    }
}

void MultiindexPositionEncoder::SetPrimaryOffset(int _offset)  // offset of the primary storage in buffer
{
  val_offset = _offset;
  if (dim_no.size() > 0) {
    dim_offset[0] = val_offset;
    for (unsigned int i = 1; i < dim_offset.size(); i++) dim_offset[i] = dim_offset[i - 1] + dim_size[i - 1];
  }
}

void MultiindexPositionEncoder::Encode(unsigned char *buf,
                                       MIIterator &mit)  // set the buffer basing on iterator position
{
  for (unsigned int i = 0; i < dim_no.size(); i++) {
    int64_t val = mit[dim_no[i]];
    if (val == common::NULL_VALUE_64)
      val = 0;
    else
      val++;
    std::memcpy(buf + dim_offset[i], &val, dim_size[i]);
  }
}

void MultiindexPositionEncoder::GetValue(unsigned char *buf,
                                         MIDummyIterator &mit)  // set the iterator position basing on buffer
{
  for (unsigned int i = 0; i < dim_no.size(); i++) {
    int64_t val = 0;
    std::memcpy(&val, buf + dim_offset[i], dim_size[i]);
    if (val == 0)
      val = common::NULL_VALUE_64;
    else
      val--;
    mit.Set(dim_no[i], val);
  }
}

uint MultiindexPositionEncoder::DimByteSize(MultiIndex *mind, int dim)  // how many bytes is needed to encode dimensions
{
  uint64_t no_rows = mind->OrigSize(dim) + 1;
  return CalculateByteSize(no_rows);
}
}  // namespace core
}  // namespace Tianmu
