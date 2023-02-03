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

#include "load_parser.h"

#include "binlog.h"
#include "core/engine.h"
#include "core/tianmu_attr.h"
#include "loader/value_cache.h"
#include "log_event.h"
#include "system/io_parameters.h"
#include "util/timer.h"

namespace Tianmu {
namespace loader {
LoadParser::LoadParser(TianmuAttrPtrVect_t &attrs, const system::IOParameters &iop, uint packsize,
                       std::unique_ptr<system::Stream> &f)
    : attrs_(attrs),
      start_time_(types::TianmuDateTime::GetCurrent().GetInt64()),
      io_param_(iop),
      pack_size_(packsize),
      rejecter_(packsize, iop.GetRejectFile(), iop.GetAbortOnCount(), iop.GetAbortOnThreshold()),
      num_of_obj_(attrs[0]->NumOfObj()) {
  std::vector<uchar> columns_collations;
  for (auto &it : attrs) columns_collations.push_back(it->GetCollation().collation->number);

  strategy_ = std::make_shared<ParsingStrategy>(iop, columns_collations);

  utils::Timer timer;
  if (!read_buffer_.BufOpen(f))
    throw common::NetStreamException("Unable to open file " + std::string(iop.Path()));

  if (read_buffer_.BufSize() == 0)
    throw common::NetStreamException("File looks to be empty: " + std::string(iop.Path()));

  cur_ptr_ = read_buffer_.Buf();
  buf_end_ = cur_ptr_ + read_buffer_.BufSize();

  timer.Print(__PRETTY_FUNCTION__);
  tab_index_ = ha_tianmu_engine_->GetTableIndex("./" + io_param_.TableName());
}

uint LoadParser::GetPackrow(uint no_of_rows, std::vector<ValueCache> &value_buffers) {
  value_buffers.reserve(attrs_.size());
  for (uint att = 0; att < attrs_.size(); att++) {
    int64_t init_capacity;
    if (last_pack_size_.size() > att)
      init_capacity = static_cast<int64_t>(last_pack_size_[att] * 1.1) + 128;
    else {
      auto max_value_size = sizeof(int64_t);
      if (core::ATI::IsStringType(attrs_[att]->TypeName()) && attrs_[att]->Type().GetPrecision() < max_value_size)
        max_value_size = attrs_[att]->Type().GetPrecision();
      init_capacity = pack_size_ * max_value_size + 512;
    }
    value_buffers.emplace_back(pack_size_, init_capacity);
  }

  uint no_of_rows_returned;
  for (no_of_rows_returned = 0; no_of_rows_returned < no_of_rows; no_of_rows_returned++) {
    if (!MakeRow(value_buffers))
      break;
  }

  last_pack_size_.clear();
  for (auto &it : value_buffers) {
    last_pack_size_.push_back(it.SumarizedSize());
  }

  return no_of_rows_returned;
}

bool LoadParser::MakeRow(std::vector<ValueCache> &value_buffers) {
  uint rowsize = 0;
  int errorinfo;

  bool cont = true;
  bool eof = false;

  while (cont) {
    switch (strategy_->GetOneRow(cur_ptr_, buf_end_ - cur_ptr_, value_buffers, rowsize, errorinfo, eof)) {
      case ParsingStrategy::ParseResult::EOB:
        if (mysql_bin_log.is_open())
          binlog_loaded_block(read_buffer_.Buf(), cur_ptr_);
        if (read_buffer_.BufFetch(int(buf_end_ - cur_ptr_))) {
          cur_ptr_ = read_buffer_.Buf();
          buf_end_ = cur_ptr_ + read_buffer_.BufSize();
        } else {
          // reaching the end of the buffer
          if (cur_ptr_ != buf_end_) {
            // rejecter_.ConsumeBadRow(cur_ptr_, buf_end_ - cur_ptr_, cur_row_ + 1, errorinfo == -1 ? -1 : errorinfo +
            // 1);
            // do not cousume the row, take this as the normal line
            eof = true;
          } else {
            cur_row_++;
            cont = false;
          }
        }
        break;

      case ParsingStrategy::ParseResult::ERROR:
        rejecter_.ConsumeBadRow(cur_ptr_, rowsize, cur_row_ + 1, errorinfo + 1);
        cur_ptr_ += rowsize;
        cur_row_++;
        cont = false;
        break;

      case ParsingStrategy::ParseResult::OK: {
        bool make_value_ok{true};
        for (uint att = 0; make_value_ok && att < attrs_.size(); ++att)
          if (!MakeValue(att, value_buffers[att])) {
            rejecter_.ConsumeBadRow(cur_ptr_, rowsize, cur_row_ + 1, att + 1);
            make_value_ok = false;
          }

        cur_ptr_ += rowsize;
        cur_row_++;

        if (!make_value_ok)
          break;

        for (uint att = 0; att < attrs_.size(); ++att) {
          value_buffers[att].Commit();
        }

        num_of_row_++;
        io_param_.GetTHD()->get_stmt_da()->inc_current_row_for_condition();
        if (num_of_skip_ < io_param_.GetSkipLines()) /*check skip lines */
        {
          // does not load this line,continue to get next line
          num_of_skip_++;
          num_of_row_--;
          for (uint att = 0; att < attrs_.size(); ++att) {
            value_buffers[att].Rollback();

            auto &attr(attrs_[att]);
            attr->RollBackIfAutoInc();
          }
          break;
        } else if (tab_index_ != nullptr) { /* check duplicate */
          if (HA_ERR_FOUND_DUPP_KEY == ProcessInsertIndex(tab_index_, value_buffers, num_of_row_ - 1)) {
            // dose not load this line, continue to get next line
            num_of_row_--;
            num_of_dup_++;
            for (uint att = 0; att < attrs_.size(); ++att) {
              value_buffers[att].Rollback();

              auto &attr(attrs_[att]);
              attr->RollBackIfAutoInc();
            }
            break;
          }
        }

        return true;
      }
    }
  }

  return false;
}

bool LoadParser::MakeValue(uint att, ValueCache &buffer) {
  if (attrs_[att]->TypeName() == common::ColumnType::TIMESTAMP) {
    if (buffer.ExpectedNull() && attrs_[att]->Type().NotNull()) {
      *reinterpret_cast<int64_t *>(buffer.Prepare(sizeof(int64_t))) = start_time_;
      buffer.ExpectedSize(sizeof(int64_t));
      buffer.ExpectedNull(false);
    }
  }

  // deal with auto increment
  auto &attr(attrs_[att]);
  if (core::ATI::IsIntegerType(attrs_[att]->TypeName()) && attr->GetIfAutoInc()) {
    int64_t *buf = reinterpret_cast<int64_t *>(buffer.Prepare(sizeof(uint64_t)));

    if (*buf == 0)  // Value of auto inc column was not assigned by user
      *buf = attr->AutoIncNext();

    if (static_cast<uint64_t>(*buf) > attr->GetAutoInc()) {
      if (*buf > 0 || ((attr->TypeName() == common::ColumnType::BIGINT) && attr->GetIfUnsigned()))
        attr->SetAutoInc(*buf);
    }
    buffer.ExpectedSize(sizeof(uint64_t));
  }

  // validate the value length
  if (core::ATI::IsStringType(attrs_[att]->TypeName()) && !buffer.ExpectedNull() &&
      (size_t)buffer.ExpectedSize() > attrs_[att]->Type().GetPrecision())
    return false;

  if (attrs_[att]->Type().Lookup() && !buffer.ExpectedNull()) {
    types::BString s(ZERO_LENGTH_STRING, 0);
    buffer.Prepare(sizeof(int64_t));
    s.val_ = static_cast<char *>(buffer.PreparedBuffer());
    s.len_ = buffer.ExpectedSize();
    *reinterpret_cast<int64_t *>(buffer.PreparedBuffer()) = attrs_[att]->EncodeValue_T(s, true);
    buffer.ExpectedSize(sizeof(int64_t));
  }

  return true;
}

int LoadParser::ProcessInsertIndex(std::shared_ptr<index::TianmuTableIndex> tab, std::vector<ValueCache> &vcs,
                                   uint no_rows) {
  std::vector<std::string> fields;
  size_t lastrow = vcs[0].NumOfValues();
  ASSERT(lastrow >= 1, "should be 'lastrow >= 1'");
  std::vector<uint> cols = tab->KeyCols();
  for (auto &col : cols) {
    fields.emplace_back(vcs[col].GetDataBytesPointer(lastrow - 1), vcs[col].Size(lastrow - 1));
  }

  if (tab->InsertIndex(current_txn_, fields, num_of_obj_ + no_rows) == common::ErrorCode::DUPP_KEY) {
    TIANMU_LOG(LogCtl_Level::INFO, "Load discard this row for duplicate key");
    return HA_ERR_FOUND_DUPP_KEY;
  }

  return 0;
}

int LoadParser::binlog_loaded_block(const char *buf_start, const char *buf_end) {
  LOAD_FILE_INFO *lf_info = nullptr;
  uint block_len = 0;

  lf_info = static_cast<LOAD_FILE_INFO *>(io_param_.GetLogInfo());
  uchar *buffer = reinterpret_cast<uchar *>(const_cast<char *>(buf_start));
  uint max_event_size = lf_info->thd->variables.max_allowed_packet;

  if (lf_info == nullptr)
    return -1;
  if (lf_info->thd->is_current_stmt_binlog_format_row())
    return 0;

  for (block_len = (uint)(buf_end - buf_start); block_len > 0;
       buffer += std::min(block_len, max_event_size), block_len -= std::min(block_len, max_event_size)) {
    if (lf_info->wrote_create_file) {
      Append_block_log_event a(lf_info->thd, lf_info->thd->db().str, buffer, std::min(block_len, max_event_size),
                               lf_info->log_delayed);
      if (mysql_bin_log.write_event(&a))
        return -1;
    } else {
      Begin_load_query_log_event b(lf_info->thd, lf_info->thd->db().str, buffer, std::min(block_len, max_event_size),
                                   lf_info->log_delayed);
      if (mysql_bin_log.write_event(&b))
        return -1;

      lf_info->wrote_create_file = 1;
    }
  }

  return 0;
}

}  // namespace loader
}  // namespace Tianmu
