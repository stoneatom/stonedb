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

#ifndef MYSQLROUTER_TIANMU_SECONDARY_SERVER_REST_CLIENT_INCLUDED
#define MYSQLROUTER_TIANMU_SECONDARY_SERVER_REST_CLIENT_INCLUDED

#include <chrono>
#include <string>

/** @brief URI for the tianmu secondary server globals */
static constexpr const char kTianmuSecondaryServerGlobalsRestUri[] =
    "/api/v1/tianmu_secondary_server/globals/";

/** @class TianmuSecondaryServerHttpClient
 *
 * Allows communicating with mysql server tianmu secondary via
 * HTTP port
 *
 **/
class TianmuSecondaryServerRestClient {
 public:
  /** @brief Constructor
   *
   * @param http_port       port on which http server handles the http requests
   * @param http_hostname   hostname of the http server that handles the http
   * requests
   */
  TianmuSecondaryServerRestClient(const uint16_t http_port,
                       const std::string &http_hostname = "127.0.0.1");

  /** @brief Sets values of the all globals in the server tianmu secondary via
   *         http inteface.
   *         Example:
   *             set_globals("{\"secondary_removed\": true}");
   *
   * @param globals_json    json string with the globals names and values to set
   */
  void set_globals(const std::string &globals_json);

  /** @brief Gets all the tianmu secondary server globals as a json string
   */
  std::string get_globals_as_json_string();

  /** @brief Gets a selected tianmu secondary server int global value
   *
   * @param global_name    name of the global
   */
  int get_int_global(const std::string &global_name);

  /** @brief Gets a selected tianmu secondary server bool global value
   *
   * @param global_name    name of the global
   */
  bool get_bool_global(const std::string &global_name);

  /** @brief Sends Delete request to the tianmu secondary server
   *         on the selected URI
   *
   * @param uri uri for the Delete request
   */
  void send_delete(const std::string &uri);

  /**
   * @brief Wait until a REST endpoint returns !404.
   *
   * at tianmu secondary startup the socket starts to listen before the REST endpoint gets
   * registered. As long as it returns 404 Not Found we should wait and retry.
   *
   * @param max_wait_time max time to wait for endpoint being ready
   * @returns true once endpoint doesn't return 404 anymore, fails otherwise
   */
  bool wait_for_rest_endpoint_ready(
      std::chrono::milliseconds max_wait_time =
          kTianmuSecondaryServerDefaultRestEndpointTimeout) const noexcept;

 private:
  static constexpr std::chrono::milliseconds kTianmuSecondaryServerMaxRestEndpointStepTime{
      100};
  static constexpr std::chrono::milliseconds
      kTianmuSecondaryServerDefaultRestEndpointTimeout{1000};
  const std::string http_hostname_;
  const uint16_t http_port_;
};

#endif  // MYSQLROUTER_MOCK_SERVER_REST_CLIENT_INCLUDED
