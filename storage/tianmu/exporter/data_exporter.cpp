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
#include "core/tianmu_attr.h"
#include "system/large_buffer.h"

namespace Tianmu {
namespace exporter {

void DataExporter::Init(std::shared_ptr<system::LargeBuffer> buffer, std::vector<core::AttributeTypeInfo> source_deas,
                        fields_t const &fields, std::vector<core::AttributeTypeInfo> &result_deas) {
  fields_ = fields;
  data_exporter_buf_ = buffer;

  this->source_attr_infos_ = source_deas;
  this->attr_infos_ = result_deas;
  this->no_attrs_ = int(attr_infos_.size());

  for (size_t i = 0; i < attr_infos_.size(); ++i) {
    common::ColumnType f_at = ha_tianmu_engine_->GetCorrespondingType(fields[i]);
    if (core::ATI::IsStringType(attr_infos_[i].Type()) && !core::ATI::IsStringType(f_at))
      this->attr_infos_[i] = core::AttributeTypeInfo(f_at, attr_infos_[i].NotNull());
  }

  cur_attr_ = 0;
  row_ = nullptr;
  row_ptr_ = nullptr;
  nulls_indicator_ = 0;
}

DataExporter::~DataExporter() {}

}  // namespace exporter
}  // namespace Tianmu
