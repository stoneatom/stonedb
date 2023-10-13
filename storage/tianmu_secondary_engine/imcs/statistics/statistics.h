/* Copyright (c) 2018, 2022, Oracle and/or its affiliates.

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
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

  /**
   The basic statistics info.
   Created 6/2/2023 Hom.LEE
  */

#ifndef __STATISTICS_H__
#define __STATISTICS_H__

#include "../imcs_base.h"

namespace Tianmu {
namespace IMCS {

namespace Statistics {

namespace COST {
  //nil cost unit
  constexpr double COST_UNIT_NIL = 0.0;
  
  //cpu cost unit
  constexpr double COST_UNIT_CPU = 0.5;

  //memory cost unit
  constexpr double COST_UNIT_MEM = 0.5;

  //io cost unit
  constexpr double COST_UNIT_IO = 1.0;

  //network cost unit
  constexpr double COST_UNIT_NET = 1.5;

  //OBS or OSS (object storage service) cost unit
  constexpr double COST_IO_OBS = 2.0; 

} //ns COST

class Mcv {

};

//the base class for icms statistics. (abs class)
class Statistics {
public:
  Statistics() : type_(STATISTICS_TYPE::BASE) {}
  Statistics(STATISTICS_TYPE type) : type_(type) {}

  virtual ~Statistics(){}

  //gets which type of statistics used. Histogram, or CAMP, etc.
  virtual STATISTICS_TYPE type() { return type_; }

  //refresh the statistics.
  virtual IMCS_STATUS analyze() = 0;
  
  //the type.
  STATISTICS_TYPE type_;

  //MCV
  Mcv *mvcs_;
};

}  // namespace statistics
}  // namespace IMCS
}  // namespace Tianmu

#endif //__STATISTICS_H__
