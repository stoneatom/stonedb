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

#ifndef TIANMU_MM_REFERENCE_OBJECT_H_
#define TIANMU_MM_REFERENCE_OBJECT_H_
#pragma once

#include "core/tools.h"
#include "mm/traceable_object.h"

namespace Tianmu {
namespace mm {

class ReferenceObject : public TraceableObject {
 public:
  ReferenceObject(core::PackCoordinate &r) : TraceableObject() {
    m_coord.ID = core::COORD_TYPE::PACK;
    m_coord.co.pack = r;
  }
  ReferenceObject(core::TianmuAttrCoordinate &r) : TraceableObject() {
    m_coord.ID = core::COORD_TYPE::kTianmuAttr;
    m_coord.co.rcattr = r;
  }
  ReferenceObject(core::TOCoordinate &r) : TraceableObject() { m_coord = r; }
  TO_TYPE TraceableType() const override { return TO_TYPE::TO_REFERENCE; }
};

}  // namespace mm
}  // namespace Tianmu

#endif  // TIANMU_MM_REFERENCE_OBJECT_H_
