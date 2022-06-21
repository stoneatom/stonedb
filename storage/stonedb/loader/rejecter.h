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
#ifndef STONEDB_LOADER_REJECTER_H_
#define STONEDB_LOADER_REJECTER_H_
#pragma once

#include <string>

#include "system/stonedb_file.h"

namespace stonedb {
namespace loader {

class Rejecter {
 private:
  std::string reject_file_;
  int64_t abort_on_count_;
  double abort_on_threshold_;
  std::unique_ptr<system::StoneDBFile> writer_;
  int64_t packrow_size_;
  int64_t rejected_;

 public:
  Rejecter(int, std::string const &, int64_t, double);
  void ConsumeBadRow(char const *, int64_t, int64_t, int);

  int64_t GetNoRejectedRows() const { return rejected_; }
  bool ThresholdExceeded(int64_t no_rows) const {
    return abort_on_threshold_ != 0 &&
           (static_cast<double>(rejected_) / static_cast<double>(no_rows)) >= abort_on_threshold_;
  }
};

}  // namespace loader
}  // namespace stonedb

#endif  // STONEDB_LOADER_REJECTER_H_
