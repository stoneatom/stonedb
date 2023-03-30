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

/**
 which is used as a compiler factory to gen a compiler and to compile the query into
 quer plan.
*/
#ifndef __OPTIMIZER_COMPILER_H__
#define __OPTIMIZER_COMPILER_H__

#include "core/item_tianmu_field.h"
#include "core/mysql_expression.h"
#include "optimizer/joiner.h"
#include "optimizer/plan/query_plan.h"
#include "sql/ha_my_tianmu.h"
#include "vc/column_type.h"

namespace Tianmu {
namespace Optimizer {

#include <mutex>

template <typename CompilerT>
class CompilerF {
 public:
  static CompilerT &instance() {
    static CompilerT instance_;
    return instance_;
  }

 private:
  CompilerF() = default;
  CompilerF(const CompilerF &) = delete;
  CompilerF &operator=(const CompilerF &) = delete;
  ~CompilerF() = default;

  static std::mutex mutex_;
};

class Compiler {};

class Tianmu_compiler : public Compiler {
 public:
  Tianmu_compiler() {}
  ~Tianmu_compiler() {}

  QueryPlan *Optimize(THD *thd, LEX *lex, Query_result *&result);

 private:
  // Do some basic checks and logical trnasformation on Lex tree, which is permanent.
  void Prepare(THD *thd, LEX *lex);

  // IN 8.0, will use `Query_block`.
  SELECT_LEX *query_stmt_;
};

}  // namespace Optimizer
}  // namespace Tianmu
#endif
