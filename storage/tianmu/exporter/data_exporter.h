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
#ifndef TIANMU_EXPORTER_DATA_EXPORTER_H_
#define TIANMU_EXPORTER_DATA_EXPORTER_H_
#pragma once

#include "common/common_definitions.h"
#include "core/bin_tools.h"
#include "system/channel_out.h"
#include "system/large_buffer.h"
#include "system/txt_utils.h"

namespace Tianmu {
namespace exporter {

class DataExporter {
 public:
  DataExporter() : progress_out_(nullptr), row_(nullptr), row_ptr_(nullptr), nulls_indicator_(nullptr) {}
  virtual void Init(std::shared_ptr<system::LargeBuffer> buffer, std::vector<core::AttributeTypeInfo> source_deas,
                    fields_t const &fields, std::vector<core::AttributeTypeInfo> &result_deas);
  virtual ~DataExporter();
  void FlushBuffer();

  void SetProgressOut(system::ChannelOut *po) { progress_out_ = po; }
  void ShowProgress(int no_eq);

  virtual void PutNull() = 0;
  virtual void PutText(const types::BString &str) = 0;
  virtual void PutBin(const types::BString &str) = 0;
  virtual void PutNumeric(int64_t num) = 0;
  virtual void PutDateTime(int64_t dt) = 0;
  virtual void PutRowEnd() = 0;

 protected:
  int cur_attr_ = 0;
  std::vector<core::AttributeTypeInfo> attr_infos_;
  std::vector<core::AttributeTypeInfo> source_attr_infos_;
  fields_t fields_;

  std::shared_ptr<system::LargeBuffer> data_exporter_buf_;
  system::ChannelOut *progress_out_;

  int no_attrs_ = 0;

  // the fields below should be moved to RCDEforBinIndicator
  char *row_;
  char *row_ptr_;
  int max_row_size_ = 0;
  int nulls_indicator_len_ = 0;
  char *nulls_indicator_;
};

}  // namespace exporter
}  // namespace Tianmu

#endif  // TIANMU_EXPORTER_DATA_EXPORTER_H_
