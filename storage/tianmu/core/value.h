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
#ifndef TIANMU_CORE_VALUE_H_
#define TIANMU_CORE_VALUE_H_
#pragma once

#include <boost/variant.hpp>
#include <string>
#include <variant>

namespace Tianmu {
namespace core {
// std::variant has some bug in gcc8
class Value final {
 public:
  Value() = default;
  explicit Value(int64_t i) { var_ = i; }
  ~Value() = default;

#if defined(__GNUC__) && (__GNUC__ == 8)
  bool HasValue() const { return var_.which() != static_cast<int>(ValueType::kValueNull); }
  bool IsInt() const { return var_.which() == static_cast<int>(ValueType::kValueInt); }
  bool IsDouble() const { return var_.which() == static_cast<int>(ValueType::kValueDouble); }
  bool IsString() const { return var_.which() == static_cast<int>(ValueType::kVauleString); }
  bool IsStringView() const { return var_.which() == static_cast<int>(ValueType::kValueStringView); }
#else
  bool HasValue() const { return !std::holds_alternative<std::monostate>(var_); }
  bool IsInt() const { return std::holds_alternative<int64_t>(var_); }
  bool IsDouble() const { return std::holds_alternative<double>(var_); }
  bool IsString() const { return std::holds_alternative<std::string>(var_); }
  bool IsStringView() const { return std::holds_alternative<std::string_view>(var_); }
#endif

  void SetInt(int64_t v) { var_ = v; }
#if defined(__GNUC__) && (__GNUC__ == 8)
  int64_t &GetInt() { return boost::get<int64_t>(var_); }
  const int64_t &GetInt() const { return boost::get<int64_t>(var_); }
#else
  int64_t &GetInt() { return std::get<int64_t>(var_); }
  const int64_t &GetInt() const { return std::get<int64_t>(var_); }
#endif

  void SetDouble(double v) { var_ = v; }
#if defined(__GNUC__) && (__GNUC__ == 8)
  double &GetDouble() { return boost::get<double>(var_); }
  const double &GetDouble() const { return boost::get<double>(var_); }
#else
  double &GetDouble() { return std::get<double>(var_); }
  const double &GetDouble() const { return std::get<double>(var_); }
#endif

  void SetString(char *s, uint n) { var_.emplace<std::string>(s, n); }
#if defined(__GNUC__) && (__GNUC__ == 8)
  std::string &GetString() { return boost::get<std::string>(var_); }
  const std::string &GetString() const { return boost::get<std::string>(var_); }
#else
  std::string &GetString() { return std::get<std::string>(var_); }
  const std::string &GetString() const { return std::get<std::string>(var_); }
#endif

#if defined(__GNUC__) && (__GNUC__ == 8)
  void SetStringView(char *s, uint n) { var_ = std::string_view(s, n); }
  std::string_view &GetStringView() { return boost::get<std::string_view>(var_); }
  const std::string_view &GetStringView() const { return boost::get<std::string_view>(var_); }
#else
  void SetStringView(char *s, uint n) { var_.emplace<std::string_view>(s, n); }
  std::string_view &GetStringView() { return std::get<std::string_view>(var_); }
  const std::string_view &GetStringView() const { return std::get<std::string_view>(var_); }
#endif

 private:
#if defined(__GNUC__) && (__GNUC__ == 8)
  enum class ValueType {
    kValueNull = 0,
    kValueInt = 1,
    kValueDouble = 2,
    kVauleString = 3,
    kValueStringView = 4,
  };
  boost::variant<boost::blank, int64_t, double, std::string, std::string_view> var_;
#else
  std::variant<std::monostate, int64_t, double, std::string, std::string_view> var_;
#endif
};

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_VALUE_H_
