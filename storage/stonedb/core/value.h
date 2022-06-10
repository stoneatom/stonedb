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
#ifndef STONEDB_CORE_VALUE_H_
#define STONEDB_CORE_VALUE_H_
#pragma once

#include <string>
#include <variant>

namespace stonedb {
namespace core {
class Value final {
 public:
  Value() = default;
  explicit Value(int64_t i) { var = i; }
  ~Value() = default;

  bool HasValue() const { return !std::holds_alternative<std::monostate>(var); }
  bool IsInt() const { return std::holds_alternative<int64_t>(var); }
  bool IsDouble() const { return std::holds_alternative<double>(var); }
  bool IsString() const { return std::holds_alternative<std::string>(var); }
  bool IsStringView() const { return std::holds_alternative<std::string_view>(var); }

  void SetInt(int64_t v) { var = v; }
  int64_t &GetInt() { return std::get<int64_t>(var); }
  const int64_t &GetInt() const { return std::get<int64_t>(var); }

  void SetDouble(double v) { var = v; }
  double &GetDouble() { return std::get<double>(var); }
  const double &GetDouble() const { return std::get<double>(var); }

  void SetString(char *s, uint n) { var.emplace<std::string>(s, n); }
  std::string &GetString() { return std::get<std::string>(var); }
  const std::string &GetString() const { return std::get<std::string>(var); }

  void SetStringView(char *s, uint n) { var.emplace<std::string_view>(s, n); }
  std::string_view &GetStringView() { return std::get<std::string_view>(var); }
  const std::string_view &GetStringView() const { return std::get<std::string_view>(var); }

 private:
  std::variant<std::monostate, int64_t, double, std::string, std::string_view> var;
};
}  // namespace core
}  // namespace stonedb

#endif  // STONEDB_CORE_VALUE_H_
