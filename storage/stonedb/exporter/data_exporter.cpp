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

#include "exporter/data_exporter.h"

#include "common/assert.h"
#include "core/engine.h"
#include "core/rc_attr.h"
#include "system/large_buffer.h"

namespace stonedb {
namespace exporter {

void DataExporter::Init(std::shared_ptr<system::LargeBuffer> buffer, std::vector<core::AttributeTypeInfo> source_deas,
                        fields_t const &fields, std::vector<core::AttributeTypeInfo> &result_deas) {
  _fields = fields;
  buf = buffer;

  this->source_deas = source_deas;
  this->deas = result_deas;
  this->no_attrs = int(deas.size());

  for (size_t i = 0; i < deas.size(); ++i) {
    common::CT f_at = rceng->GetCorrespondingType(fields[i]);
    if (core::ATI::IsStringType(deas[i].Type()) && !core::ATI::IsStringType(f_at))
      this->deas[i] = core::AttributeTypeInfo(f_at, deas[i].NotNull());
  }

  cur_attr = 0;
  row = NULL;
  row_ptr = NULL;
  nulls_indicator = 0;
}

DataExporter::~DataExporter() {}

}  // namespace exporter
}  // namespace stonedb
