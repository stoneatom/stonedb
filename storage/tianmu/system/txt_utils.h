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
#ifndef TIANMU_SYSTEM_TXT_UTILS_H_
#define TIANMU_SYSTEM_TXT_UTILS_H_
#pragma once

#include "common/exception.h"
#include "compress/defs.h"

namespace Tianmu {
namespace system {

// bool TcharToAscii(const TCHAR* src, char* dest, unsigned long destSize);  //
// Converts TCHAR string to char string const char* DecodeError(int
// common::ErrorCode, char* msg_buffer, unsigned long size);

void Convert2Hex(const unsigned char *src, int src_size, char *dest, int dest_size, bool zero_term = true);

bool EatDTSeparators(char *&ptr, int &len);
common::ErrorCode EatUInt(char *&ptr, int &len, uint &out_value);
common::ErrorCode EatInt(char *&ptr, int &len, int &out_value);
common::ErrorCode EatUInt64(char *&ptr, int &len, uint64_t &out_value);
bool CanBeDTSeparator(char c);

}  // namespace system
}  // namespace Tianmu

#endif  // TIANMU_SYSTEM_TXT_UTILS_H_
