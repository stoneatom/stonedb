//<!***************************************************************************
/**
 * Copyright (C) 2021-2022 StoneAtom Group Holding Limited
 */
//<!***************************************************************************
#ifndef STONEDB_MM_REFERENCE_OBJECT_H_
#define STONEDB_MM_REFERENCE_OBJECT_H_
#pragma once

#include "core/tools.h"
#include "mm/traceable_object.h"

namespace stonedb {
namespace mm {

class ReferenceObject : public TraceableObject {
 public:
  ReferenceObject(core::PackCoordinate &r) : TraceableObject() {
    m_coord.ID = core::COORD_TYPE::PACK;
    m_coord.co.pack = r;
  }
  ReferenceObject(core::RCAttrCoordinate &r) : TraceableObject() {
    m_coord.ID = core::COORD_TYPE::RCATTR;
    m_coord.co.rcattr = r;
  }
  ReferenceObject(core::TOCoordinate &r) : TraceableObject() { m_coord = r; }
  TO_TYPE TraceableType() const override { return TO_TYPE::TO_REFERENCE; }
};

}  // namespace mm
}  // namespace stonedb

#endif  // STONEDB_MM_REFERENCE_OBJECT_H_
