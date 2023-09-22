/*
  Copyright (c) 2018, 2022, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/

#ifndef MYSQLROUTER_TIANMU_SECONDARY_SERVER_COMPONENT_INCLUDED
#define MYSQLROUTER_TIANMU_SECONDARY_SERVER_COMPONENT_INCLUDED  

#include <memory>
#include <vector>

#include "mysql/harness/stdx/monitor.h"
#include "mysqlrouter/tianmu_secondary_server_export.h"
#include "mysqlrouter/tianmu_secondary_server_global_scope.h"

namespace server_tianmu_secondary {
class MySQLServerTianmuSecondary;
}

class TIANMU_SECONDARY_SERVER_EXPORT TianmuSecondaryServerComponent {
 public:
  // disable copy, as we are a single-instance
  TianmuSecondaryServerComponent(TianmuSecondaryServerComponent const &) = delete;
  void operator=(TianmuSecondaryServerComponent const &) = delete;

  static TianmuSecondaryServerComponent &get_instance();

  void register_server(const std::string &name,
                       std::shared_ptr<server_tianmu_secondary::MySQLServerTianmuSecondary> srv);

  std::shared_ptr<TianmuSecondaryServerGlobalScope> get_global_scope();
  void close_all_connections();

 private:
  Monitor<std::map<std::string, std::weak_ptr<server_tianmu_secondary::MySQLServerTianmuSecondary>>>
      srvs_{{}};

  TianmuSecondaryServerComponent() = default;
};

#endif //MYSQLROUTER_TIANMU_SECONDARY_SERVER_COMPONENT_INCLUDED
