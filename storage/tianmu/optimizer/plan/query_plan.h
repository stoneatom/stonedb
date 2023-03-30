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

// this is base class of query plan used for tianmu engine.
// why we do this? due to it's hard to do optimization and pipeline execution on
// this current codes and framework. Therefore, we should split the optimization
// into stages, such as, prepare, setup, prepare, transformation,optimization,
// query plan gen, execution. just like the other sql engines do.

#ifndef __OPTIMIZER_QUERY_PLAN__
#define __OPTIMIZER_QUERY_PLAN__

#include "optimizer/compile/cq_term.h"

namespace Tianmu {
namespace Optimizer {

// in future we will change ns `core` to `optimizer`.
using namespace core;

enum class QueryPlanType { NONE = 0, TABLE_SCAN, JOIN, AGG };

// the basic plan
class QueryPlan {
 public:
  QueryPlan(QueryPlanType type, QueryPlan *parent) {
    type_ = type;
    parent_ = parent;
  }
  virtual ~QueryPlan() = default;

 private:
  // type of query plan.
  QueryPlanType type_{QueryPlanType::NONE};

  // parent of this plan.
  QueryPlan *parent_;
};

class TableScan : public QueryPlan {
 public:
  TableScan() : QueryPlan(QueryPlanType::TABLE_SCAN, nullptr) {}
  TableScan(QueryPlan *parent) : QueryPlan(QueryPlanType::TABLE_SCAN, parent) {}

  ~TableScan() {}

 public:
  enum class ScanType { SEQ_SCAN = 0, INDX_SCAN };

 private:
  QueryPlan *parent_;
  ScanType scan_type_;
  TabID tid_;
};

class Join : public QueryPlan {};

class Aggregation : public QueryPlan {};

}  // namespace Optimizer
}  // namespace Tianmu

#endif
