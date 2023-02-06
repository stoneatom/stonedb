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

#include "system/tianmu_system.h"

namespace Tianmu {
namespace loader {

Rejecter::Rejecter(int packrowSize, std::string const &path, int64_t abortOnCount, double abortOnThreshold)
    : reject_file_(path),
      abort_on_count_(abortOnCount),
      abort_on_threshold_(abortOnThreshold),
      writer_(),
      packrow_size_(packrowSize),
      rejected_(0) {}

void Rejecter::ConsumeBadRow(char const *ptr, int64_t size, int64_t row_no, int error_code) {
  bool do_throw(false);
  ++rejected_;

  if (!reject_file_.empty()) {
    if (!writer_.get()) {
      writer_.reset(new system::TianmuFile());
      writer_->OpenCreateNotExists(reject_file_);
    }

    writer_->WriteExact(ptr, static_cast<uint>(size));
  }

  TIANMU_LOG(LogCtl_Level::WARN, "Loading file error, row %ld, around:", row_no);
  TIANMU_LOG(LogCtl_Level::WARN, "---------------------------------------------\n");
  TIANMU_LOG(LogCtl_Level::WARN, "%s", ptr);
  TIANMU_LOG(LogCtl_Level::WARN, "---------------------------------------------");

  if (abort_on_threshold_ > 0) {
    if (row_no > packrow_size_)
      do_throw = (static_cast<double>(rejected_) / static_cast<double>(row_no)) >= abort_on_threshold_;
  } else if (abort_on_count_ > 0) {
    do_throw = (rejected_ >= abort_on_count_);
  } else if (abort_on_count_ == 0)
    do_throw = true;
  if (do_throw)
    throw common::FormatException(row_no, error_code);
}

}  // namespace loader
}  // namespace Tianmu
