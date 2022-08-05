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
#ifndef TIANMU_CORE_RSI_INDEX_H_
#define TIANMU_CORE_RSI_INDEX_H_
#pragma once

#include "common/exception.h"
#include "core/bin_tools.h"
#include "core/dpn.h"
#include "mm/traceable_object.h"
#include "util/fs.h"

namespace Tianmu {
namespace core {
enum class FilterType : int {
  HIST,   // histogram
  CMAP,   // character maps
  BLOOM,  // bloom filter
};

class RSIndex : public mm::TraceableObject {
 public:
  RSIndex() = default;
  virtual ~RSIndex() = default;
  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_RSINDEX; }
  virtual void SaveToFile(common::TX_ID ver) = 0;

 protected:
  fs::path m_path;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_RSI_INDEX_H_
