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
#ifndef STONEDB_SYSTEM_MAG_MEMORY_POLICY_H_
#define STONEDB_SYSTEM_MAG_MEMORY_POLICY_H_
#pragma once

#include "system/res_manager_policy.h"

namespace stonedb {
namespace system {
// Estimate memory policy from magnitude of configured main heap size

class MagMemoryPolicy : public ResourceManagerPolicy {
  int m_scale;

 public:
  MagMemoryPolicy(int size) {
    if (size < 200)
      m_scale = 0;  // very small
    else if (size < 500)
      m_scale = 1;  // up to 0.5 GB
    else if (size < 1200)
      m_scale = 2;  // up to 1.2 GB
    else if (size < 2500)
      m_scale = 3;  // up to 2.5 GB
    else if (size < 5000)
      m_scale = 4;  // up to 5 GB
    else if (size < 10000)
      m_scale = 5;  // up to 10 GB
    else
      m_scale = size / 10000 + 5;  // linear scale
  }

  int estimate() override { return m_scale; }
};
}  // namespace system
}  // namespace stonedb

#endif  // STONEDB_SYSTEM_MAG_MEMORY_POLICY_H_
