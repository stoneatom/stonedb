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

#include "types/rc_num.h"

namespace stonedb {
namespace exporter {

DEforTxt::DEforTxt(const system::IOParameters &iop)
    : delim(iop.Delimiter()[0]),
      str_q(iop.StringQualifier()),
      esc(iop.EscapeCharacter()),
      line_terminator(iop.LineTerminator()[0]),
      nulls_str(iop.NullsStr()),
      destination_cs(iop.CharsetInfoNumber() ? get_charset(iop.CharsetInfoNumber(), 0) : 0) {}

void DEforTxt::PutText(const types::BString &str) {
  WriteStringQualifier();
  size_t char_len =
      deas[cur_attr].GetCollation().collation->cset->numchars(deas[cur_attr].GetCollation().collation, str.val,
                                                              str.val + str.len);  // len in chars
  WriteString(str, str.len);                                                       // len in bytes
  if ((deas[cur_attr].Type() == common::CT::STRING) && (char_len < deas[cur_attr].CharLen()))
// it can be necessary to change the WritePad implementation to something like:
// collation->cset->fill(cs, copy->to_ptr+copy->from_length,
// copy->to_length-copy->from_length, '
// ');
// if ' ' (space) can have different codes.
// for export data not fill space
#if 0
        if (!destination_cs) { //export as binary
            WritePad(deas[cur_attr].Precision() - bytes_written);
        } else {
            WritePad(deas[cur_attr].CharLen() - char_len);
        }
#endif
    WriteStringQualifier();
  WriteValueEnd();
}

void DEforTxt::PutBin(const types::BString &str) {
  int len = str.size();
  // if((rcdea[cur_attr].attrt == common::CT::BYTE) && (len <
  // rcdea[cur_attr].size))
  //	len = rcdea[cur_attr].size;
  if (len > 0) {
    char *hex = new char[len * 2];
    system::Convert2Hex((const unsigned char *)str.val, len, hex, len * 2, false);
    WriteString(types::BString(hex, len * 2));
    delete[] hex;
  }
  WriteValueEnd();
}

void DEforTxt::PutNumeric(int64_t num) {
  types::RCNum rcn(num, source_deas[cur_attr].Scale(), core::ATI::IsRealType(source_deas[cur_attr].Type()),
                   source_deas[cur_attr].Type());
  types::BString rcs = rcn.ToBString();
  WriteString(rcs);
  WriteValueEnd();
}

void DEforTxt::PutDateTime(int64_t dt) {
  types::RCDateTime rcdt(dt, deas[cur_attr].Type());
  types::BString rcs = rcdt.ToBString();
  WriteString(rcs);
  WriteValueEnd();
}

void DEforTxt::PutRowEnd() {
  if (line_terminator == 0)
    WriteString("\r\n", 2);
  else
    buf->WriteIfNonzero(line_terminator);
}

size_t DEforTxt::WriteString(const types::BString &str, int len) {
  int res_len = 0;
  if (esc) {
    escaped.erase();
    for (size_t i = 0; i < str.size(); i++) {
      if (str[i] == str_q || (!str_q && str[i] == delim)) escaped.append(1, esc);
      escaped.append(1, str[i]);
    }
    if (destination_cs) {
      int max_res_len =
          std::max(destination_cs->mbmaxlen * len + len, deas[cur_attr].GetCollation().collation->mbmaxlen * len + len);
      uint errors = 0;
      res_len = copy_and_convert(buf->BufAppend(max_res_len), max_res_len, destination_cs, escaped.c_str(),
                                 uint32_t(escaped.length()), deas[cur_attr].GetCollation().collation, &errors);
      buf->SeekBack(max_res_len - res_len);
    } else {
      std::strncpy(buf->BufAppend(uint(escaped.length())), escaped.c_str(), escaped.length());
      res_len = len;
    }

  } else {
    if (destination_cs) {
      int max_res_len =
          std::max(destination_cs->mbmaxlen * len, deas[cur_attr].GetCollation().collation->mbmaxlen * len);
      uint errors = 0;
      res_len = copy_and_convert(buf->BufAppend(max_res_len), max_res_len, destination_cs, str.GetDataBytesPointer(),
                                 len, deas[cur_attr].GetCollation().collation, &errors);
      buf->SeekBack(max_res_len - res_len);
    } else {
      str.CopyTo(buf->BufAppend((uint)len), len);
      res_len = len;
    }
  }
  return res_len;
}

}  // namespace exporter
}  // namespace stonedb
