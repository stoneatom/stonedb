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

#include "rejecter.h"

#include "system/rc_system.h"

namespace stonedb {
namespace loader {

Rejecter::Rejecter(int packrowSize, std::string const &path, int64_t abortOnCount, double abortOnThreshold)
    : reject_file(path),
      abort_on_count(abortOnCount),
      abort_on_threshold(abortOnThreshold),
      writer(),
      packrow_size(packrowSize),
      rejected(0) {}

void Rejecter::ConsumeBadRow(char const *ptr, int64_t size, int64_t row_no, int error_code) {
  bool do_throw(false);
  ++rejected;
  if (!reject_file.empty()) {
    if (!writer.get()) {
      writer.reset(new system::StoneDBFile());
      writer->OpenCreateNotExists(reject_file);
    }
    writer->WriteExact(ptr, (uint)size);
  }
  STONEDB_LOG(WARN, "Loading file error, row %ld, around:", row_no);
  STONEDB_LOG(WARN, "---------------------------------------------\n");
  STONEDB_LOG(WARN, "%s", ptr);
  STONEDB_LOG(WARN, "---------------------------------------------");

  if (abort_on_threshold > 0) {
    if (row_no > packrow_size)
      do_throw = (static_cast<double>(rejected) / static_cast<double>(row_no)) >= abort_on_threshold;
  } else if (abort_on_count > 0) {
    do_throw = (rejected >= abort_on_count);
  } else if (abort_on_count == 0)
    do_throw = true;
  if (do_throw) throw common::FormatException(row_no, error_code);
}

}  // namespace loader
}  // namespace stonedb
