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

#include "core/engine.h"
#include "core/rc_attr.h"
#include "loader/value_cache.h"
#include "system/io_parameters.h"
#include "util/timer.h"

namespace stonedb {
namespace loader {
LoadParser::LoadParser(RCAttrPtrVect_t &attrs, const system::IOParameters &iop, uint packsize,
                       std::unique_ptr<system::Stream> &f)
    : attrs(attrs),
      start_time(types::RCDateTime::GetCurrent().GetInt64()),
      ioparam(iop),
      pack_size(packsize),
      rejecter(packsize, iop.GetRejectFile(), iop.GetAbortOnCount(), iop.GetAbortOnThreshold()),
      num_of_obj(attrs[0]->NumOfObj()) {
  std::vector<uchar> columns_collations;
  for (auto &it : attrs) columns_collations.push_back(it->GetCollation().collation->number);

  strategy = std::make_shared<ParsingStrategy>(iop, columns_collations);

  utils::Timer timer;
  if (!read_buffer.BufOpen(f)) throw common::NetStreamException("Unable to open file " + std::string(iop.Path()));

  if (read_buffer.BufSize() == 0)
    throw common::NetStreamException("File looks to be empty: " + std::string(iop.Path()));

  cur_ptr = read_buffer.Buf();
  buf_end = cur_ptr + read_buffer.BufSize();

  timer.Print(__PRETTY_FUNCTION__);
  tab_index = rceng->GetTableIndex("./" + ioparam.TableName());
}

uint LoadParser::GetPackrow(uint no_of_rows, std::vector<ValueCache> &value_buffers) {
  value_buffers.reserve(attrs.size());
  for (uint att = 0; att < attrs.size(); att++) {
    int64_t init_capacity;
    if (last_pack_size.size() > att)
      init_capacity = static_cast<int64_t>(last_pack_size[att] * 1.1) + 128;
    else {
      auto max_value_size = sizeof(int64_t);
      if (core::ATI::IsStringType(attrs[att]->TypeName()) && attrs[att]->Type().GetPrecision() < max_value_size)
        max_value_size = attrs[att]->Type().GetPrecision();
      init_capacity = pack_size * max_value_size + 512;
    }
    value_buffers.emplace_back(pack_size, init_capacity);
  }

  uint no_of_rows_returned;
  for (no_of_rows_returned = 0; no_of_rows_returned < no_of_rows; no_of_rows_returned++) {
    if (!MakeRow(value_buffers)) break;
  }

  last_pack_size.clear();
  for (auto &it : value_buffers) {
    last_pack_size.push_back(it.SumarizedSize());
  }

  return no_of_rows_returned;
}

bool LoadParser::MakeRow(std::vector<ValueCache> &value_buffers) {
  uint rowsize = 0;
  int errorinfo;

  bool cont = true;
  while (cont) {
    bool make_value_ok;
    switch (strategy->GetOneRow(cur_ptr, buf_end - cur_ptr, value_buffers, rowsize, errorinfo)) {
      case ParsingStrategy::ParseResult::EOB:
        if (mysql_bin_log.is_open()) binlog_loaded_block(read_buffer.Buf(), cur_ptr);
        if (read_buffer.BufFetch(int(buf_end - cur_ptr))) {
          cur_ptr = read_buffer.Buf();
          buf_end = cur_ptr + read_buffer.BufSize();
        } else {
          // reaching the end of the buffer
          if (cur_ptr != buf_end)
            rejecter.ConsumeBadRow(cur_ptr, buf_end - cur_ptr, cur_row + 1, errorinfo == -1 ? -1 : errorinfo + 1);
          cur_row++;
          cont = false;
        }
        break;

      case ParsingStrategy::ParseResult::ERROR:
        rejecter.ConsumeBadRow(cur_ptr, rowsize, cur_row + 1, errorinfo + 1);
        cur_ptr += rowsize;
        cur_row++;
        break;

      case ParsingStrategy::ParseResult::OK:
        make_value_ok = true;
        for (uint att = 0; make_value_ok && att < attrs.size(); ++att)
          if (!MakeValue(att, value_buffers[att])) {
            rejecter.ConsumeBadRow(cur_ptr, rowsize, cur_row + 1, att + 1);
            make_value_ok = false;
          }
        cur_ptr += rowsize;
        cur_row++;
        if (make_value_ok) {
          for (uint att = 0; att < attrs.size(); ++att) value_buffers[att].Commit();
          // check key
          row_no++;
          if (tab_index != nullptr) {
            if (HA_ERR_FOUND_DUPP_KEY == ProcessInsertIndex(tab_index, value_buffers, row_no - 1)) {
              row_no--;
              dup_no++;
              for (uint att = 0; att < attrs.size(); ++att) value_buffers[att].Rollback();
            }
          }
          return true;
        }
        break;
    }
  }

  return false;
}

bool LoadParser::MakeValue(uint att, ValueCache &buffer) {
  if (attrs[att]->TypeName() == common::CT::TIMESTAMP) {
    if (buffer.ExpectedNull() && attrs[att]->Type().NotNull()) {
      *reinterpret_cast<int64_t *>(buffer.Prepare(sizeof(int64_t))) = start_time;
      buffer.ExpectedSize(sizeof(int64_t));
      buffer.ExpectedNull(false);
    }
  }

  // validate the value length
  if (core::ATI::IsStringType(attrs[att]->TypeName()) && !buffer.ExpectedNull() &&
      (size_t)buffer.ExpectedSize() > attrs[att]->Type().GetPrecision())
    return false;

  if (attrs[att]->Type().IsLookup() && !buffer.ExpectedNull()) {
    types::BString s(ZERO_LENGTH_STRING, 0);
    buffer.Prepare(sizeof(int64_t));
    s.val = static_cast<char *>(buffer.PreparedBuffer());
    s.len = buffer.ExpectedSize();
    *reinterpret_cast<int64_t *>(buffer.PreparedBuffer()) = attrs[att]->EncodeValue_T(s, true);
    buffer.ExpectedSize(sizeof(int64_t));
  }

  return true;
}

int LoadParser::ProcessInsertIndex(std::shared_ptr<index::RCTableIndex> tab, std::vector<ValueCache> &vcs,
                                   uint no_rows) {
  std::vector<std::string_view> fields;
  size_t lastrow = vcs[0].NumOfValues();
  ASSERT(lastrow >= 1, "should be 'lastrow >= 1'");
  std::vector<uint> cols = tab->KeyCols();
  for (auto &col : cols) {
    fields.emplace_back(vcs[col].GetDataBytesPointer(lastrow - 1), vcs[col].Size(lastrow - 1));
  }

  if (tab->InsertIndex(current_tx, fields, num_of_obj + no_rows) == common::ErrorCode::DUPP_KEY) {
    STONEDB_LOG(LogCtl_Level::INFO, "Load discard this row for duplicate key");
    return HA_ERR_FOUND_DUPP_KEY;
  }

  return 0;
}

int LoadParser::binlog_loaded_block(const char *buf_start, const char *buf_end) {
  LOAD_FILE_INFO *lf_info = nullptr;
  uint block_len = 0;

  lf_info = static_cast<LOAD_FILE_INFO *>(ioparam.GetLogInfo());
  uchar *buffer = reinterpret_cast<uchar *>(const_cast<char *>(buf_start));
  uint max_event_size = lf_info->thd->variables.max_allowed_packet;

  if (lf_info == nullptr) return -1;
  if (lf_info->thd->is_current_stmt_binlog_format_row()) return 0;

  for (block_len = (uint)(buf_end - buf_start); block_len > 0;
       buffer += std::min(block_len, max_event_size), block_len -= std::min(block_len, max_event_size)) {
    if (lf_info->wrote_create_file) {
      Append_block_log_event a(lf_info->thd, lf_info->thd->db, buffer, std::min(block_len, max_event_size),
                               lf_info->log_delayed);
      if (mysql_bin_log.write_event(&a)) return -1;
    } else {
      Begin_load_query_log_event b(lf_info->thd, lf_info->thd->db, buffer, std::min(block_len, max_event_size),
                                   lf_info->log_delayed);
      if (mysql_bin_log.write_event(&b)) return -1;

      lf_info->wrote_create_file = 1;
    }
  }

  return 0;
}

}  // namespace loader
}  // namespace stonedb
