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
   Histogram algorithm.

   Created 6/2/2023 Hom.LEE
  */

#ifndef __HISTOGRAM_H__
#define __HISTOGRAM_H_

#include "../imcs_base.h"
#include "statistics.h"

namespace Tianmu{
namespace IMCS {

namespace Statistics{

class Histogram : public Statistics {
public:
  Histogram() : Statistics(STATISTICS_TYPE::HIST) {}
  virtual ~Histogram() {}

  //do anaylzing the data statistics.
  virtual IMCS_STATUS analyze() final;
};

}  //namespace statistics

}  //namespace IMCU
}  //namespace Tianmu

#endif //__HISTOGRAM_H__
