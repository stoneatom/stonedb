//<!***************************************************************************
/**
 * Copyright (C) 2021-2022 StoneAtom Group Holding Limited
 */
//<!***************************************************************************

#include "initializer.h"

namespace stonedb {
namespace mm {

MemoryManagerInitializer *MemoryManagerInitializer::instance = NULL;

bool MemoryManagerInitializer::m_report_leaks = false;

}  // namespace mm
}  // namespace stonedb
