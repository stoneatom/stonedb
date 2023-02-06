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

#include "data_exporter_txt.h"

#include "types/tianmu_num.h"

namespace Tianmu {
namespace exporter {

DEforTxt::DEforTxt(const system::IOParameters &iop)
    : opt_enclosed_(iop.OptionallyEnclosed()),
      str_qualifier_(iop.StringQualifier()),
      escape_character_(iop.EscapeCharacter()),
      delimiter_(iop.Delimiter()),
      line_terminator_(iop.LineTerminator()),
      nulls_str_(iop.NullsStr()),
      destination_charset_(iop.CharsetInfoNumber() ? get_charset(iop.CharsetInfoNumber(), 0) : 0) {}

void DEforTxt::PutText(const types::BString &str) {
  WriteStringQualifier();
  size_t char_len = attr_infos_[cur_attr_].GetCollation().collation->cset->numchars(
      attr_infos_[cur_attr_].GetCollation().collation, str.val_,
      str.val_ + str.len_);          // len in chars
  WriteString(str, str.len_, true);  // len in bytes
  if ((attr_infos_[cur_attr_].Type() == common::ColumnType::STRING) && (char_len < attr_infos_[cur_attr_].CharLen()))
// it can be necessary to change the WritePad implementation to something like:
// collation->cset->fill(cs, copy->to_ptr+copy->from_length,
// copy->to_length-copy->from_length, '
// ');
// if ' ' (space) can have different codes.
// for export data not fill space
#if 0
        if (!destination_cs_) { //export as binary
            WritePad(attr_infos_[cur_attr_].Precision() - bytes_written);
        } else {
            WritePad(attr_infos_[cur_attr_].CharLen() - char_len);
        }
#endif
    WriteStringQualifier();
  WriteValueEnd();
}

void DEforTxt::PutBin(const types::BString &str) {
  int len = str.size();
  if (len > 0) {
    char *hex = new char[len * 2];
    system::Convert2Hex((const unsigned char *)str.val_, len, hex, len * 2, false);
    WriteString(types::BString(hex, len * 2), true);
    delete[] hex;
  }
  WriteValueEnd();
}

void DEforTxt::PutNumeric(int64_t num) {
  types::TianmuNum tianmu_n(num, source_attr_infos_[cur_attr_].Scale(),
                            core::ATI::IsRealType(source_attr_infos_[cur_attr_].Type()),
                            source_attr_infos_[cur_attr_].Type());
  types::BString tianmu_s = tianmu_n.ToBString();
  WriteString(tianmu_s, false);
  WriteValueEnd();
}

void DEforTxt::PutDateTime(int64_t dt) {
  types::TianmuDateTime tianmu_dt(dt, attr_infos_[cur_attr_].Type());
  types::BString tianmu_s = tianmu_dt.ToBString();
  WriteString(tianmu_s, false);
  WriteValueEnd();
}

void DEforTxt::PutRowEnd() {
  if (line_terminator_.empty())
    WriteChars("\r\n");
  else
    WriteChars(line_terminator_);
}

size_t DEforTxt::WriteString(const types::BString &str, int len, bool text_or_bin, bool is_null) {
  int res_len = 0;
  bool enclose_output{false};

  // If you omit the word OPTIONALLY, all fields are enclosed by the ENCLOSED BY character.
  // If you specify OPTIONALLY, the ENCLOSED BY character is used only to enclose values from columns that
  // have a string data type (such as CHAR, BINARY, TEXT, or ENUM):
  if (str_qualifier_ && !is_null && (text_or_bin || !opt_enclosed_)) {
    enclose_output = true;
    WriteChar(str_qualifier_, 1);
  }

  if (escape_character_) {
    escaped_.erase();
    for (size_t i = 0; i < str.size(); i++) {
      if (str[i] == str_qualifier_ || (!str_qualifier_ && !delimiter_.empty() && str[i] == delimiter_[0]))
        escaped_.append(1, escape_character_);
      escaped_.append(1, str[i]);
    }
    if (destination_charset_) {
      int max_res_len = std::max(destination_charset_->mbmaxlen * len + len,
                                 attr_infos_[cur_attr_].GetCollation().collation->mbmaxlen * len + len);
      uint errors = 0;
      res_len = copy_and_convert(data_exporter_buf_->BufAppend(max_res_len), max_res_len, destination_charset_,
                                 escaped_.c_str(), uint32_t(escaped_.length()),
                                 attr_infos_[cur_attr_].GetCollation().collation, &errors);
      data_exporter_buf_->SeekBack(max_res_len - res_len);
    } else {
      std::strncpy(data_exporter_buf_->BufAppend(uint(escaped_.length())), escaped_.c_str(), escaped_.length());
      res_len = len;
    }

  } else {
    if (destination_charset_) {
      int max_res_len = std::max(destination_charset_->mbmaxlen * len,
                                 attr_infos_[cur_attr_].GetCollation().collation->mbmaxlen * len);
      uint errors = 0;
      res_len =
          copy_and_convert(data_exporter_buf_->BufAppend(max_res_len), max_res_len, destination_charset_,
                           str.GetDataBytesPointer(), len, attr_infos_[cur_attr_].GetCollation().collation, &errors);
      data_exporter_buf_->SeekBack(max_res_len - res_len);
    } else {
      str.CopyTo(data_exporter_buf_->BufAppend((uint)len), len);
      res_len = len;
    }
  }

  // check whether to output the enclose char
  if (enclose_output) {
    WriteChar(str_qualifier_, 1);
    res_len += sizeof(char) + sizeof(char);  // enclose char + result len + enclose char
  }

  return res_len;
}

}  // namespace exporter
}  // namespace Tianmu
