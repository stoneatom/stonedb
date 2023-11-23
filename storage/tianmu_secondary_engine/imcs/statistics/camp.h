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
   statistics using CAMP for string type.

   Created 6/2/2023 Hom.LEE
  */

#ifndef __CAMP_H__
#define __CAMP_H__

#include "../imcs_base.h"
#include "statistics.h"

namespace Tianmu {
namespace IMCS {

namespace Statistics {

class Camp : public Statistics {
public:
  Camp () : Statistics(STATISTICS_TYPE::CAMP) {}
  virtual ~Camp() {}

  //do analyzing the data statistics.
  virtual IMCS_STATUS analyze () final;
};

}  //namespace statistics

}  //namespace IMCS
}  //namespace Tianmu

#endif //__CAMP_H__
