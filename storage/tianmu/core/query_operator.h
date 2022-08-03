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
#ifndef TIANMU_CORE_QUERY_OPERATOR_H_
#define TIANMU_CORE_QUERY_OPERATOR_H_
#pragma once

#include <string>

namespace Tianmu {
namespace core {
/**
        The base object representation of a Query common::Operator.

        Further use will require the further definition of this base
        class' functionality and pure virtual member interfaces but as
        a basic example of this stub's intended use:

                class NotEqualQueryOperator : public QueryOperator
                {
                        public:
                                NotEqualQueryOperator():QueryOperator(common::Operator::O_NOT_EQ,
   "<>") {}
                }

 */

class QueryOperator {
 public:
  // Instantiates the query operator object representation.
  // \param Fetch the common::Operator enumeration type this object represents.
  // \param Fetch the string representation of this query operator.
  QueryOperator(common::Operator t, const std::string &sr) : type(t), string_rep(sr) {}
  // Fetch the common::Operator enumeration type this object represents.
  common::Operator GetType() const { return type; }
  // Fetch the string representation of this query operator.
  std::string AsString() const { return string_rep; }

 protected:
  // The common::Operator enumeration type this object represents.
  common::Operator type;

  // A string representation of the query operator.  This string
  // reflects what would have been enter in the originating SQL
  // statement.
  std::string string_rep;
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_QUERY_OPERATOR_H_
