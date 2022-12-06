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
#ifndef TIANMU_SYSTEM_RES_MANAGER_H_
#define TIANMU_SYSTEM_RES_MANAGER_H_
#pragma once

#include "system/res_manager_base.h"
#include "system/res_manager_policy.h"

namespace Tianmu {
namespace system {

class ResourceManager : public ResourceManagerBase {
 public:
  ResourceManager();
  ~ResourceManager();

  int GetMemoryScale() override { return res_manage_policy_->estimate(); }

 private:
  ResourceManagerPolicy *res_manage_policy_;
};

}  // namespace system
}  // namespace Tianmu

#endif  // TIANMU_SYSTEM_RES_MANAGER_H_
