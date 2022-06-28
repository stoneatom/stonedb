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
#ifndef STONEDB_CORE_COLUMN_BIN_ENCODER_H_
#define STONEDB_CORE_COLUMN_BIN_ENCODER_H_
#pragma once

#include "core/rsi_cmap.h"
#include "types/text_stat.h"
#include "vc/virtual_column.h"

namespace stonedb {
namespace core {
class MIIterator;
class MIDummyIterator;

class ColumnBinEncoder final {
 public:
  static const int ENCODER_IGNORE_NULLS = 0x01;   // don't encode nulls - not present, or just ignore them
  static const int ENCODER_MONOTONIC = 0x02;      // code order follow value order; if false, only equality
                                                  // comparisons work
  static const int ENCODER_NONCOMPARABLE = 0x04;  // store values, do not allow comparisons (e.g. UTF8
                                                  // transformations not applied)
  static const int ENCODER_DESCENDING = 0x08;     // ordering direction
  static const int ENCODER_DECODABLE = 0x10;      // GetValue works

  class ColumnValueEncoder;
  // Special cases:
  class EncoderInt;
  class EncoderDecimal;
  class EncoderDate;
  class EncoderYear;
  class EncoderDouble;
  class EncoderText;
  class EncoderText_UTF;
  class EncoderLookup;
  class EncoderTextStat;
  class EncoderTextMD5;

  ColumnBinEncoder(int flags = 0);
  ColumnBinEncoder(const ColumnBinEncoder &sec);
  ColumnBinEncoder &operator=(const ColumnBinEncoder &);
  ~ColumnBinEncoder();

  //	void Clear();								// clear all
  // encoding data, keep
  // flags
  bool PrepareEncoder(
      vcolumn::VirtualColumn *vc1,
      vcolumn::VirtualColumn *vc2 = NULL);  // encoder for one column, or a common encoder for two of them
  // return false if encoding of a second column is not possible (incompatible)

  void Disable() { disabled = true; }
  bool IsEnabled() { return !disabled; }  // disabled if set manually or or implicit or trivial value for sorting
  bool IsNontrivial() { return (val_size > 0); }
  void SetImplicit() {
    implicit = true;
    disabled = true;
  }  // implicit: the column will be read as vc->GetValue(iterator) instead of
     // orig. values

  // Buffer descriptions
  uint GetPrimarySize()  // no. of bytes in the primary buffer
  {
    return val_size;
  }
  int GetSecondarySize()  // no. of bytes in the secondary buffer (or 0 if not
                          // needed)
  {
    return val_sec_size;
  }

  void SetPrimaryOffset(int _offset)  // offset of the primary storage in buffer
  {
    val_offset = _offset;
  }
  void SetSecondaryOffset(int _offset)  // offset of the secondary storage in buffer
  {
    val_sec_offset = _offset;
  }

  // Set / retrieve values
  void LockSourcePacks(MIIterator &mit) {
    if (vc && !implicit && dup_col == -1) vc->LockSourcePacks(mit);
  }
  void LoadPacks(MIIterator *mit);

  void Encode(unsigned char *buf, MIIterator &mit, vcolumn::VirtualColumn *alternative_vc = NULL,
              bool update_stats = false);
  bool PutValue64(unsigned char *buf, int64_t v, bool sec_column,
                  bool update_stats = false);  // used in special cases only (e.g. rough),
                                               // return false if cannot encode
  bool PutValueString(unsigned char *buf, types::BString &v, bool sec_column,
                      bool update_stats = false);  // as above

  // Versions of encoders which returns integer code (monotonic for normal
  // inequalities)
  int64_t ValEncode(MIIterator &mit, bool update_stats = false);
  int64_t ValPutValue64(int64_t v, bool update_stats = false);
  int64_t ValPutValueString(types::BString &v, bool update_stats = false);

  int64_t GetValue64(unsigned char *buf, const MIDummyIterator &mit, bool &is_null);
  types::BString GetValueT(unsigned char *buf, const MIDummyIterator &mit);
  void UpdateStatistics(unsigned char *buf);  // get value from the buffer and
                                              // update internal statistics

  bool ImpossibleValues(int64_t pack_min,
                        int64_t pack_max);  // return true if the current contents of the encoder
                                            // is out of scope
  bool ImpossibleValues(types::BString &pack_min,
                        types::BString &pack_max);  // return true if the current contents
                                                    // of the encoder is out of scope
  // Note: pack_min/pack_max may be wider than the original string size! They
  // usually has 8 bytes.
  bool IsString();
  int64_t MaxCode();  // Maximal integer code, if it makes any sense (or
                      // common::NULL_VALUE_64)
  void ClearStatistics();

  bool DefineAsEquivalent(ColumnBinEncoder const &sec) { return (vc == sec.vc); }
  void SetDupCol(int col) { dup_col = col; }

 private:
  vcolumn::VirtualColumn *vc;

  bool ignore_nulls;
  bool monotonic_encoding;
  bool descending;
  bool decodable;
  bool noncomparable;

  bool implicit;  // if true, then the column will be read as
                  // vc->GetValue(iterator) instead of orig. values
  bool disabled;  // if true, then the column will not be encoded

  // Address part:
  int val_offset;      // buffer offset of the value - externally set
  int val_sec_offset;  // secondary buffer, if needed - externally set
  uint val_size;       // number of bytes in the main part
  int val_sec_size;    // number of bytes in the secondary part, if needed
  mutable std::unique_ptr<ColumnValueEncoder> my_encoder;
  int dup_col = -1;  // duplicate sort col in sorted wrapper
};

class ColumnBinEncoder::ColumnValueEncoder {  // base class for detailed
                                              // encoders
 public:
  ColumnValueEncoder(vcolumn::VirtualColumn *vc, [[maybe_unused]] bool decodable, [[maybe_unused]] bool nulls_possible,
                     bool _descending)
      : descending(_descending), null_status(0), vc_type(vc->Type()) {}
  ColumnValueEncoder(const ColumnValueEncoder &sec)
      : descending(sec.descending),
        null_status(sec.null_status),
        vc_type(sec.vc_type),
        size(sec.size),
        size_sec(sec.size_sec) {}
  virtual ColumnValueEncoder *Copy() = 0;

  virtual ~ColumnValueEncoder() {}
  // Encode the second column to make common encoding, false if cannot
  virtual bool SecondColumn([[maybe_unused]] vcolumn::VirtualColumn *vc) { return false; }
  // Valid() is false if we failed to construct a proper encoder (rare case)
  virtual bool Valid() { return true; }
  virtual void SetNull(unsigned char *buf, unsigned char *buf_sec) = 0;
  virtual void Encode(unsigned char *buf, unsigned char *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
                      bool update_stats = false) = 0;
  virtual bool EncodeInt64([[maybe_unused]] unsigned char *buf, [[maybe_unused]] unsigned char *buf_sec,
                           [[maybe_unused]] int64_t v, [[maybe_unused]] bool sec_column,
                           [[maybe_unused]] bool update_stats) {
    return false;
  }  // used in special cases only, return false if cannot encode
  virtual bool EncodeString([[maybe_unused]] unsigned char *buf, [[maybe_unused]] unsigned char *buf_sec,
                            [[maybe_unused]] types::BString &v, [[maybe_unused]] bool sec_column,
                            [[maybe_unused]] bool update_stats) {
    return false;
  }

  virtual int64_t ValEncode([[maybe_unused]] vcolumn::VirtualColumn *vc, [[maybe_unused]] MIIterator &mit,
                            [[maybe_unused]] bool update_stats = false) {
    DEBUG_ASSERT(0);
    return 0;
  }
  virtual int64_t ValEncodeInt64([[maybe_unused]] int64_t v, [[maybe_unused]] bool update_stats) {
    DEBUG_ASSERT(0);
    return 0;
  }  // used in special cases only
  virtual int64_t ValEncodeString([[maybe_unused]] types::BString &v, [[maybe_unused]] bool update_stats) {
    DEBUG_ASSERT(0);
    return 0;
  }  // used in special cases only

  virtual bool IsNull(unsigned char *buf, unsigned char *buf_sec) = 0;
  virtual int64_t GetValue64([[maybe_unused]] unsigned char *buf, [[maybe_unused]] unsigned char *buf_sec) {
    return common::NULL_VALUE_64;
  }  // null when executed e.g. for string columns
  virtual types::BString GetValueT([[maybe_unused]] unsigned char *buf, [[maybe_unused]] unsigned char *buf_sec) {
    return types::BString();
  }  // null when executed e.g. for numerical columns
  virtual void UpdateStatistics([[maybe_unused]] unsigned char *buf) {
    return;
  }  // get value from the buffer and update internal statistics
  virtual bool ImpossibleInt64Values([[maybe_unused]] int64_t pack_min, [[maybe_unused]] int64_t pack_max) {
    return false;
  }  // default: all values are possible
  virtual bool ImpossibleStringValues([[maybe_unused]] types::BString &pack_min,
                                      [[maybe_unused]] types::BString &pack_max) {
    return false;
  }  // default: all values are possible
  virtual void ClearStatistics() {}
  virtual bool IsString() { return false; }
  virtual int64_t MaxCode() { return common::NULL_VALUE_64; }
  size_t ValueSize() { return size; }
  size_t ValueSizeSec() { return size_sec; }

 protected:
  bool descending;      // true if descending
  int null_status;      // 0 - no nulls, 1 - integer shifted (encoded as 0), 2 -
                        // separate byte
  ColumnType vc_type;   // a type of the first encoded virtual column
  size_t size = 0;      // total size in bytes
  size_t size_sec = 0;  // total size in bytes for a secondary location, or 0

  void Negate(unsigned char *buf, int loc_size) {
    for (int i = 0; i < loc_size; i++) buf[i] = ~(buf[i]);
  }
};

class ColumnBinEncoder::EncoderInt : public ColumnBinEncoder::ColumnValueEncoder {
 public:
  EncoderInt(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending,
             bool calc_int_stats = true);
  EncoderInt(const EncoderInt &sec)
      : ColumnValueEncoder(sec),
        min_val(sec.min_val),
        max_code(sec.max_code),
        min_found(sec.min_found),
        max_found(sec.max_found) {}
  ColumnValueEncoder *Copy() override { return new EncoderInt(*this); }
  bool SecondColumn(vcolumn::VirtualColumn *vc) override;

  void Encode(unsigned char *buf, unsigned char *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
              bool update_stats = false) override;
  bool EncodeInt64(unsigned char *buf, unsigned char *buf_sec, int64_t v, bool sec_column, bool update_stats) override;
  void SetNull(unsigned char *buf, unsigned char *buf_sec) override;
  bool IsNull(unsigned char *buf, unsigned char *buf_sec) override;
  int64_t GetValue64(unsigned char *buf, unsigned char *buf_sec) override;
  int64_t MaxCode() override { return max_code; }
  void UpdateStatistics(unsigned char *buf) override;

  int64_t ValEncode(vcolumn::VirtualColumn *vc, MIIterator &mit, bool update_stats = false) override;
  int64_t ValEncodeInt64(int64_t v, bool update_stats) override;

  bool ImpossibleInt64Values(int64_t pack_min, int64_t pack_max) override;
  void ClearStatistics() override;

 protected:
  int64_t min_val;
  uint64_t max_code;

  int64_t min_found;  // local statistics, for rough evaluations
  int64_t max_found;
};

class ColumnBinEncoder::EncoderDecimal : public ColumnBinEncoder::EncoderInt {
 public:
  EncoderDecimal(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
  EncoderDecimal(const EncoderDecimal &sec)
      : EncoderInt(sec), scale(sec.scale), multiplier(sec.multiplier), sec_multiplier(sec.sec_multiplier) {}
  ColumnValueEncoder *Copy() override { return new EncoderDecimal(*this); }
  bool SecondColumn(vcolumn::VirtualColumn *vc) override;
  void Encode(unsigned char *buf, unsigned char *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
              bool update_stats = false) override;
  bool EncodeInt64(unsigned char *buf, unsigned char *buf_sec, int64_t v, bool sec_column, bool update_stats) override;

  bool ImpossibleInt64Values(int64_t pack_min, int64_t pack_max) override;

 protected:
  int scale;
  double multiplier;
  double sec_multiplier;
};

class ColumnBinEncoder::EncoderDate : public ColumnBinEncoder::EncoderInt {
 public:
  EncoderDate(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
  EncoderDate(const EncoderDate &sec) : EncoderInt(sec) {}
  ColumnValueEncoder *Copy() override { return new EncoderDate(*this); }
  bool SecondColumn(vcolumn::VirtualColumn *vc) override;

  void Encode(unsigned char *buf, unsigned char *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
              bool update_stats = false) override;
  bool EncodeInt64(unsigned char *buf, unsigned char *buf_sec, int64_t v, bool sec_column, bool update_stats) override;
  int64_t ValEncode(vcolumn::VirtualColumn *vc, MIIterator &mit, bool update_stats = false) override;
  int64_t ValEncodeInt64(int64_t v, bool update_stats) override;
  int64_t GetValue64(unsigned char *buf, unsigned char *buf_sec) override;
  void UpdateStatistics(unsigned char *buf) override;
  bool ImpossibleInt64Values(int64_t pack_min, int64_t pack_max) override;
};

class ColumnBinEncoder::EncoderYear : public ColumnBinEncoder::EncoderInt {
 public:
  EncoderYear(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
  EncoderYear(const EncoderYear &sec) : EncoderInt(sec) {}
  ColumnValueEncoder *Copy() override { return new EncoderYear(*this); }
  bool SecondColumn(vcolumn::VirtualColumn *vc) override;

  void Encode(unsigned char *buf, unsigned char *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
              bool update_stats = false) override;
  bool EncodeInt64(unsigned char *buf, unsigned char *buf_sec, int64_t v, bool sec_column, bool update_stats) override;
  int64_t ValEncode(vcolumn::VirtualColumn *vc, MIIterator &mit, bool update_stats = false) override;
  int64_t ValEncodeInt64(int64_t v, bool update_stats) override;
  int64_t GetValue64(unsigned char *buf, unsigned char *buf_sec) override;
  void UpdateStatistics(unsigned char *buf) override;
  bool ImpossibleInt64Values(int64_t pack_min, int64_t pack_max) override;
};

class ColumnBinEncoder::EncoderDouble : public ColumnBinEncoder::ColumnValueEncoder {
 public:
  EncoderDouble(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
  EncoderDouble(const EncoderDouble &sec)
      : ColumnValueEncoder(sec), multiplier_vc1(sec.multiplier_vc1), multiplier_vc2(sec.multiplier_vc2) {}
  ColumnValueEncoder *Copy() override { return new EncoderDouble(*this); }
  bool SecondColumn(vcolumn::VirtualColumn *vc) override;

  bool EncodeInt64(unsigned char *buf, unsigned char *buf_sec, int64_t v, bool sec_column, bool update_stats) override;
  void Encode(unsigned char *buf, unsigned char *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
              bool update_stats = false) override;
  void SetNull(unsigned char *buf, unsigned char *buf_sec) override;
  bool IsNull(unsigned char *buf, unsigned char *buf_sec) override;
  int64_t GetValue64(unsigned char *buf, unsigned char *buf_sec) override;

 protected:
  int64_t multiplier_vc1;  // 0 if the first vc is double, otherwise it is
                           // regarded as int (decimal) which must be multiplied
  int64_t multiplier_vc2;  // 0 if the second vc is double, otherwise it is
                           // regarded as int (decimal) which must be multiplied
};

class ColumnBinEncoder::EncoderText : public ColumnBinEncoder::ColumnValueEncoder {
 public:
  EncoderText(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
  ~EncoderText();
  EncoderText(const EncoderText &sec)
      : ColumnValueEncoder(sec), mins(sec.mins), maxs(sec.maxs), min_max_set(sec.min_max_set) {}
  ColumnValueEncoder *Copy() override { return new EncoderText(*this); }
  bool SecondColumn(vcolumn::VirtualColumn *vc) override;

  // Encoding:
  // <comparable text><len+1>,   text is padded with zeros
  // null value: all 0, len = 0;    empty string: all 0, len = 1
  void Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
              bool update_stats = false) override;
  bool EncodeString(uchar *buf, uchar *buf_sec, types::BString &s, bool sec_column, bool update_stats) override;
  void SetNull(unsigned char *buf, unsigned char *buf_sec) override;
  bool IsNull(unsigned char *buf, unsigned char *buf_sec) override;
  types::BString GetValueT(unsigned char *buf, unsigned char *buf_sec) override;
  bool ImpossibleStringValues(types::BString &pack_min, types::BString &pack_max) override;
  bool IsString() override { return true; }
  types::BString mins, maxs;
  bool min_max_set;
};

class ColumnBinEncoder::EncoderText_UTF : public ColumnBinEncoder::ColumnValueEncoder {
 public:
  EncoderText_UTF(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
  ~EncoderText_UTF();
  EncoderText_UTF(const EncoderText_UTF &sec)
      : ColumnValueEncoder(sec),
        collation(sec.collation),
        mins(sec.mins),
        maxs(sec.maxs),
        min_max_set(sec.min_max_set) {}
  ColumnValueEncoder *Copy() override { return new EncoderText_UTF(*this); }
  bool SecondColumn(vcolumn::VirtualColumn *vc) override;

  // Encoding:
  // <comparable text><len+1>,   text is padded with zeros
  // null value: all 0, len = 0;    empty string: all 0, len = 1
  void Encode(unsigned char *buf, unsigned char *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
              bool update_stats = false) override;
  bool EncodeString(uchar *buf, uchar *buf_sec, types::BString &s, bool sec_column, bool update_stats) override;
  void SetNull(unsigned char *buf, unsigned char *buf_sec) override;
  bool IsNull(unsigned char *buf, unsigned char *buf_sec) override;
  types::BString GetValueT(unsigned char *buf, unsigned char *buf_sec) override;
  bool ImpossibleStringValues(types::BString &pack_min, types::BString &pack_max) override;
  bool IsString() override { return true; }

 private:
  DTCollation collation;
  types::BString mins, maxs;
  bool min_max_set;
};

class ColumnBinEncoder::EncoderLookup : public ColumnBinEncoder::EncoderInt {
 public:
  EncoderLookup(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
  EncoderLookup(const EncoderLookup &sec);
  virtual ~EncoderLookup();
  ColumnValueEncoder *Copy() override { return new EncoderLookup(*this); }
  bool SecondColumn(vcolumn::VirtualColumn *vc) override;
  void Encode(unsigned char *buf, unsigned char *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
              bool update_stats = false) override;
  bool EncodeInt64(unsigned char *buf, unsigned char *buf_sec, int64_t v, bool sec_column, bool update_stats) override;
  types::BString GetValueT(unsigned char *buf, unsigned char *buf_sec) override;
  bool ImpossibleInt64Values(int64_t pack_min, int64_t pack_max) override;

 protected:
  int *translate2;                   // code_v1 = translate2[code_v2]. Value -1 means not
                                     // matching string.
  int no_sec_values;                 // number of possible values of the second column
  vcolumn::VirtualColumn *first_vc;  // we need both vc to construct translation table
  vcolumn::VirtualColumn *sec_vc;
};

class ColumnBinEncoder::EncoderTextStat : public ColumnBinEncoder::EncoderInt {
 public:
  EncoderTextStat(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
  EncoderTextStat(const EncoderTextStat &sec)
      : EncoderInt(sec),
        mins(sec.mins),
        maxs(sec.maxs),
        min_max_set(sec.min_max_set),
        valid(sec.valid),
        coder(sec.coder) {}
  ColumnValueEncoder *Copy() override { return new EncoderTextStat(*this); }
  bool Valid() override { return valid; }
  bool SecondColumn(vcolumn::VirtualColumn *vc) override;
  void Encode(unsigned char *buf, unsigned char *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
              bool update_stats = false) override;
  bool EncodeString(uchar *buf, uchar *buf_sec, types::BString &s, bool sec_column, bool update_stats) override;
  int64_t ValEncode(vcolumn::VirtualColumn *vc, MIIterator &mit, bool update_stats = false) override;
  bool ImpossibleStringValues(types::BString &pack_min, types::BString &pack_max) override;
  bool IsString() override { return true; }
  types::BString GetValueT(unsigned char *buf, unsigned char *buf_sec) override;

 protected:
  types::BString mins, maxs;
  bool min_max_set;
  bool valid;
  types::TextStat coder;
};

class ColumnBinEncoder::EncoderTextMD5 : public ColumnBinEncoder::ColumnValueEncoder {
 public:
  EncoderTextMD5(vcolumn::VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
  EncoderTextMD5(const EncoderTextMD5 &sec);
  ~EncoderTextMD5();
  ColumnValueEncoder *Copy() override { return new EncoderTextMD5(*this); }
  bool SecondColumn(vcolumn::VirtualColumn *vc) override;

  void Encode(uchar *buf, uchar *buf_sec, vcolumn::VirtualColumn *vc, MIIterator &mit,
              bool update_stats = false) override;
  bool EncodeString(uchar *buf, uchar *buf_sec, types::BString &s, bool sec_column, bool update_stats) override;
  void SetNull(unsigned char *buf, unsigned char *buf_sec) override;
  bool IsNull(unsigned char *buf, unsigned char *buf_sec) override;
  bool ImpossibleStringValues(types::BString &pack_min, types::BString &pack_max) override;
  bool IsString() override { return true; }
  types::BString mins, maxs;
  bool min_max_set;

 private:
  char null_buf[HASH_FUNCTION_BYTE_SIZE];
  char empty_buf[HASH_FUNCTION_BYTE_SIZE];
};

class MultiindexPositionEncoder {
 public:
  MultiindexPositionEncoder(MultiIndex *mind, DimensionVector &dims);

  uint GetPrimarySize() { return val_size; }  // no. of bytes in the primary buffer
  void SetPrimaryOffset(int _offset);         // offset of the primary storage in buffer

  void Encode(unsigned char *buf,
              MIIterator &mit);  // set the buffer basing on iterator position
  void GetValue(unsigned char *buf,
                MIDummyIterator &mit);  // set the iterator position basing on buffer

  // tools:
  static uint DimByteSize(MultiIndex *mind,
                          int dim);  // how many bytes is needed to encode the dimension

 private:
  // Encoding: 0 = NULL, or k+1. Stored on a minimal number of bytes.
  int val_offset;  // buffer offset of the value - externally set
  uint val_size;   // number of bytes for all the stored dimensions

  std::vector<int> dim_no;      // actual dimension number stored on i-th position
  std::vector<int> dim_offset;  // offset of the i-th stored dimension
                                // (dim_offset[0] = val_offset)
  std::vector<uint> dim_size;   // byte size of the i-th stored dimension
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_COLUMN_BIN_ENCODER_H_