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
#ifndef STONEDB_MM_INITIALIZER_H_
#define STONEDB_MM_INITIALIZER_H_
#pragma once

#include "common/assert.h"
#include "common/exception.h"
#include "mm/traceable_object.h"

namespace stonedb {
namespace mm {

class MemoryManagerInitializer : public TraceableObject {
 public:
  static MemoryManagerInitializer *Instance(size_t comp_size, size_t uncomp_size, std::string hugedir = "",
                                            int hugesize = 0) {
    if (instance == NULL) {
      try {
        instance = new MemoryManagerInitializer(comp_size, uncomp_size, hugedir, hugesize);
      } catch (common::OutOfMemoryException &) {
        throw;
      }
    }
    return instance;
  }

  static void deinit(bool report_leaks = false) {
    m_report_leaks = report_leaks;
    delete instance;
  }

  static void EnsureNoLeakedTraceableObject() { m_MemHandling->EnsureNoLeakedTraceableObject(); }
  TO_TYPE TraceableType() const override { return TO_TYPE::TO_INITIALIZER; }

 private:
  MemoryManagerInitializer(size_t comp_size, size_t uncomp_size, std::string hugedir = "", int hugesize = 0)
      : TraceableObject(comp_size, uncomp_size, hugedir, NULL, hugesize) {}
  virtual ~MemoryManagerInitializer() { deinitialize(m_report_leaks); }
  static MemoryManagerInitializer *instance;
  static bool m_report_leaks;
};

}  // namespace mm
}  // namespace stonedb

#endif  // STONEDB_MM_INITIALIZER_H_
