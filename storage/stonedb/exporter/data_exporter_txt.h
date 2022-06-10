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
#ifndef STONEDB_EXPORTER_DATA_EXPORTER_TXT_H_
#define STONEDB_EXPORTER_DATA_EXPORTER_TXT_H_
#pragma once

#include "exporter/data_exporter.h"
#include "system/io_parameters.h"

namespace stonedb {
namespace exporter {

class DEforTxt : public DataExporter {
 public:
  DEforTxt(const system::IOParameters &iop);
  void PutNull() override {
    WriteNull();
    WriteValueEnd();
  }

  void PutText(const types::BString &str) override;

  void PutBin(const types::BString &str) override;

  void PutNumeric(int64_t num) override;

  void PutDateTime(int64_t dt) override;

  void PutRowEnd() override;

 protected:
  void WriteStringQualifier() { /*buf->WriteIfNonzero(str_q);*/
  }
  void WriteDelimiter() { buf->WriteIfNonzero(delim); }
  void WriteNull() { WriteString(nulls_str.c_str(), (int)nulls_str.length()); }
  void WriteString(const types::BString &str) { WriteString(str, str.size()); }
  size_t WriteString(const types::BString &str, int len);
  void WriteChar(char c, uint repeat = 1) { std::memset(buf->BufAppend(repeat), c, repeat); }
  void WritePad(uint repeat) { WriteChar(' ', repeat); }
  void WriteValueEnd() {
    if (cur_attr == no_attrs - 1)
      cur_attr = 0;
    else {
      WriteDelimiter();
      cur_attr++;
    }
  }

  uchar delim, str_q, esc;
  uchar line_terminator;
  std::string nulls_str, escaped;
  CHARSET_INFO *destination_cs;
};

}  // namespace exporter
}  // namespace stonedb

#endif  // STONEDB_EXPORTER_DATA_EXPORTER_TXT_H_
