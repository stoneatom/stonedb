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

#include <cinttypes>

#include "common/data_format.h"
#include "core/engine.h"
#include "core/transaction.h"
#include "types/rc_item_types.h"
#include "types/value_parser4txt.h"

namespace Tianmu {
namespace core {
// stonedb8 List -> mem_root_deque
void scan_fields(mem_root_deque<Item *> &fields, uint *&buf_lens, std::map<int, Item *> &items_backup) {
  Item *item;
  Field *f;
  Item_field *ifield;
  Item_ref *iref;
  Item_sum *is;
  Item::Type item_type;
  Item_sum::Sumfunctype sum_type;

  buf_lens = new uint[CountVisibleFields(fields)];  // stonedb8
  uint item_id = 0;
  uint field_length = 0;
  uint total_length = 0;

  // we don't need to check each type f->pack>length() returns correct value

  // check fileds
  std::unordered_set<size_t> used_fields;

  types::Item_sum_int_rcbase *isum_int_rcbase;
  types::Item_sum_sum_rcbase *isum_sum_rcbase;
  types::Item_sum_hybrid_rcbase *isum_hybrid_rcbase;

  for (auto it = fields.begin(); it != fields.end(); ++it) {  // stonedb8
    item = *it;
    item_type = item->type();
    buf_lens[item_id] = 0;
    switch (item_type) {
      case Item::FIELD_ITEM: {  // regular select
        ifield = (Item_field *)item;
        f = ifield->get_result_field();
        auto iter_uf = used_fields.find((size_t)f);
        if (iter_uf == used_fields.end()) {
          used_fields.insert((size_t)f);
          field_length = f->pack_length();
          buf_lens[item_id] = field_length;
          total_length += field_length;
        } else
          buf_lens[item_id] = 0;
        break;
      }
      case Item::COND_ITEM:
      case Item::SUBSELECT_ITEM:
      case Item::FUNC_ITEM:
      case Item::REF_ITEM: {  // select from view
        types::Item_sum_hybrid_rcbase *tmp = new types::Item_sum_hybrid_rcbase();
        items_backup[item_id] = item;
        tmp->decimals = item->decimals;
        tmp->hybrid_type_ = item->result_type();
        tmp->unsigned_flag = item->unsigned_flag;
        tmp->hybrid_field_type_ = item->data_type();
        tmp->collation.set(item->collation);
        tmp->value_.set_charset(item->collation.collation);
        fields[item_id] = tmp;  // stonedb8
        break;
      }
      case Item::SUM_FUNC_ITEM: {
        is = (Item_sum *)item;
        sum_type = is->sum_func();

        if (sum_type == Item_sum::GROUP_CONCAT_FUNC || sum_type == Item_sum::MIN_FUNC ||
            sum_type == Item_sum::MAX_FUNC) {
          isum_hybrid_rcbase = new types::Item_sum_hybrid_rcbase();
          items_backup[item_id] = item;

          // We have to add some knowledge to our item from original item
          isum_hybrid_rcbase->decimals = is->decimals;
          isum_hybrid_rcbase->hybrid_type_ = is->result_type();
          isum_hybrid_rcbase->unsigned_flag = is->unsigned_flag;
          isum_hybrid_rcbase->hybrid_field_type_ = is->data_type();
          isum_hybrid_rcbase->collation.set(is->collation);
          isum_hybrid_rcbase->value_.set_charset(is->collation.collation);
          fields[item_id] = isum_hybrid_rcbase;  // stonedb8
          buf_lens[item_id] = 0;
          break;
        } else if (sum_type == Item_sum::COUNT_FUNC || sum_type == Item_sum::COUNT_DISTINCT_FUNC ||
                   sum_type == Item_sum::SUM_BIT_FUNC) {
          isum_int_rcbase = new types::Item_sum_int_rcbase();
          isum_int_rcbase->unsigned_flag = is->unsigned_flag;
          items_backup[item_id] = item;
          fields[item_id] = isum_int_rcbase;  // stonedb8
          buf_lens[item_id] = 0;
          break;
          // we can use the same type for SUM,AVG and SUM DIST, AVG DIST
        } else if (sum_type == Item_sum::SUM_FUNC || sum_type == Item_sum::AVG_FUNC ||
                   sum_type == Item_sum::SUM_DISTINCT_FUNC || sum_type == Item_sum::AVG_DISTINCT_FUNC ||
                   sum_type == Item_sum::VARIANCE_FUNC || sum_type == Item_sum::STD_FUNC) {
          isum_sum_rcbase = new types::Item_sum_sum_rcbase();
          items_backup[item_id] = item;

          // We have to add some knowledge to our item from original item
          isum_sum_rcbase->decimals = is->decimals;
          isum_sum_rcbase->hybrid_type_ = is->result_type();
          isum_sum_rcbase->unsigned_flag = is->unsigned_flag;
          fields[item_id] = isum_sum_rcbase;  // stonedb8
          buf_lens[item_id] = 0;
          break;
        } else {
          buf_lens[item_id] = 0;
          break;
        }
      }  // end case
      default:
        break;
    }  // end switch
    item_id++;
  }  // end while

  item_id = 0;

  for (auto it = fields.begin(); it != fields.end(); ++it) {  // stonedb8
    item = *it;
    item_type = item->type();
    switch (item_type) {
      case Item::FIELD_ITEM:  // regular select
        ifield = (Item_field *)item;
        f = ifield->get_result_field();
        break;
      case Item::REF_ITEM:  // select from view
        iref = (Item_ref *)item;
        ifield = (Item_field *)(*iref->ref);
        f = ifield->get_result_field();
        break;
      default:
        break;
    }
    item_id++;
  }
}

void restore_fields(mem_root_deque<Item *> &fields, std::map<int, Item *> &items_backup) {
  // nothing to restore
  if (items_backup.size() == 0) {
    return;
  }

  int count = 0;
  for (auto it = fields.begin(); it != fields.end(); ++it) {
    if (items_backup.find(count) != items_backup.end()) fields[count] = items_backup[count];
    count++;
  }
}

inline static void SetFieldState(Field *field, bool is_null) {
  if (is_null) {
    if (field->is_nullable())
      field->set_null();
    else  // not nullable, but null needed because of outer join
      field->table->null_row = 1;
  } else {
    if (field->is_nullable())
      field->set_notnull();
    else  // not nullable, but null needed because of outer join
      field->table->null_row = 0;
  }
}

// stonedb8 List -> mem_root_deque
ResultSender::ResultSender(THD *thd, Query_result *res, mem_root_deque<Item *> &fields)
    : thd(thd),
      res(res),
      buf_lens(NULL),
      fields(fields),
      is_initialized(false),
      offset(NULL),
      limit(NULL),
      rows_sent(0) {}

void ResultSender::Init([[maybe_unused]] TempTable *t) {
  thd->set_proc_info("Sending data");
  DBUG_PRINT("info", ("%s", thd->proc_info()));
  res->send_result_set_metadata(thd, fields, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF);
  thd->lex->unit->offset_limit_cnt = 0;
  thd->current_found_rows = 0;
  scan_fields(fields, buf_lens, items_backup);
}

void ResultSender::Send(TempTable::RecordIterator &iter) {
  if ((iter.currentRowNumber() & 0x7fff) == 0)
    if (current_txn_->Killed()) throw common::KilledException();

  TempTable *owner(iter.Owner());
  if (!is_initialized) {
    owner->CreateDisplayableAttrP();
    Init(owner);
    is_initialized = true;
  }
  if (owner && !owner->IsSent()) owner->SetIsSent();

  // func found_rows() need limit_found_rows
  thd->current_found_rows++;
  thd->update_previous_found_rows();
  if (offset && *offset > 0) {
    --(*offset);
    return;
  }

  if (limit) {
    if (*limit == 0) return;
    --(*limit);
  }

  TempTable::Record record(*iter);
  SendRecord(record.m_it.dataTypes);
  rows_sent++;
}

void ResultSender::SendRow(const std::vector<std::unique_ptr<types::RCDataType>> &record, TempTable *owner) {
  if (current_txn_->Killed()) throw common::KilledException();

  if (!is_initialized) {
    owner->CreateDisplayableAttrP();
    Init(owner);
    is_initialized = true;
  }
  if (owner && !owner->IsSent()) owner->SetIsSent();

  // func found_rows() need limit_found_rows
  thd->current_found_rows++;
  thd->update_previous_found_rows();
  if (offset && *offset > 0) {
    --(*offset);
    return;
  }

  if (limit) {
    if (*limit == 0) return;
    --(*limit);
  }

  SendRecord(record);
  rows_sent++;
}

void ResultSender::SendRecord(const std::vector<std::unique_ptr<types::RCDataType>> &r) {
  Item *item;
  Field *f;
  Item_field *ifield;
  Item_ref *iref;
  Item_sum *is;
  types::Item_sum_int_rcbase *isum_int_rcbase;
  types::Item_sum_sum_rcbase *isum_sum_rcbase;
  Item_sum::Sumfunctype sum_type;

  uint col_id = 0;
  int64_t value;

  for (auto it = fields.begin(); it != fields.end(); ++it) {  // stonedb8
    item = *it;
    int is_null = 0;
    types::RCDataType &rcdt(*r[col_id]);
    switch (item->type()) {
      case Item::DEFAULT_VALUE_ITEM:
      case Item::FIELD_ITEM:  // regular select
        ifield = (Item_field *)item;
        f = ifield->get_result_field();
        // if buf_lens[col_id] is 0 means that f->ptr was not assigned
        // because it was assigned for this instance of object
        if (buf_lens[col_id] != 0) {
          bitmap_set_bit(f->table->write_set, f->field_index());
          auto is_null = Engine::ConvertToField(f, rcdt, NULL);
          SetFieldState(f, is_null);
        }
        break;
      case Item::REF_ITEM:  // select from view
        iref = (Item_ref *)item;
        ifield = (Item_field *)(*iref->ref);
        f = ifield->get_result_field();
        if (buf_lens[col_id] != 0) {
          bitmap_set_bit(f->table->write_set, f->field_index());
          auto is_null = Engine::ConvertToField(f, rcdt, NULL);
          SetFieldState(f, is_null);
        }
        break;
      case Item::SUM_FUNC_ITEM:
        is = (Item_sum *)item;
        sum_type = is->sum_func();

        // used only MIN_FUNC
        if (sum_type == Item_sum::MIN_FUNC || sum_type == Item_sum::MAX_FUNC ||
            sum_type == Item_sum::GROUP_CONCAT_FUNC) {
          types::Item_sum_hybrid_rcbase *isum_hybrid_rcbase = (types::Item_sum_hybrid_rcbase *)is;
          if (isum_hybrid_rcbase->result_type() == DECIMAL_RESULT) {
            Engine::Convert(is_null, isum_hybrid_rcbase->dec_value(), rcdt, item->decimals);
            isum_hybrid_rcbase->null_value = is_null;
          } else if (isum_hybrid_rcbase->result_type() == INT_RESULT) {
            Engine::Convert(is_null, isum_hybrid_rcbase->int64_value(), rcdt, isum_hybrid_rcbase->hybrid_field_type_);
            isum_hybrid_rcbase->null_value = is_null;
          } else if (isum_hybrid_rcbase->result_type() == REAL_RESULT) {
            Engine::Convert(is_null, isum_hybrid_rcbase->real_value(), rcdt);
            isum_hybrid_rcbase->null_value = is_null;
          } else if (isum_hybrid_rcbase->result_type() == STRING_RESULT) {
            Engine::Convert(is_null, isum_hybrid_rcbase->string_value(), rcdt, isum_hybrid_rcbase->hybrid_field_type_);
            isum_hybrid_rcbase->null_value = is_null;
          }
          break;
        }
        // do not check COUNT_DISTINCT_FUNC, we use only this for both types
        if (sum_type == Item_sum::COUNT_FUNC || sum_type == Item_sum::SUM_BIT_FUNC) {
          isum_int_rcbase = (types::Item_sum_int_rcbase *)is;
          Engine::Convert(is_null, value, rcdt, is->data_type());
          if (is_null) value = 0;
          isum_int_rcbase->int64_value(value);
          break;
        }
        if (sum_type == Item_sum::SUM_FUNC || sum_type == Item_sum::STD_FUNC || sum_type == Item_sum::VARIANCE_FUNC) {
          isum_sum_rcbase = (types::Item_sum_sum_rcbase *)is;
          if (isum_sum_rcbase->result_type() == DECIMAL_RESULT) {
            Engine::Convert(is_null, isum_sum_rcbase->dec_value(), rcdt);
            isum_sum_rcbase->null_value = is_null;
          } else if (isum_sum_rcbase->result_type() == REAL_RESULT) {
            Engine::Convert(is_null, isum_sum_rcbase->real_value(), rcdt);
            isum_sum_rcbase->null_value = is_null;
          }
          break;
        }
        break;
      default:
        // Const items like item_int etc. do not need any conversion
        break;
    }  // end switch
    col_id++;
  }  // end while
  res->send_data(thd, fields);
}

void ResultSender::Send(TempTable *t) {
  DEBUG_ASSERT(t->IsMaterialized());
  t->CreateDisplayableAttrP();
  TempTable::RecordIterator iter = t->begin();
  TempTable::RecordIterator iter_end = t->end();
  for (; iter != iter_end; ++iter) {
    Send(iter);
  }
}

void ResultSender::Finalize(TempTable *result_table) {
  if (!is_initialized) {
    is_initialized = true;
    Init(result_table);
  }
  if (result_table && !result_table->IsSent()) Send(result_table);
  CleanUp();
  SendEof();
  ulonglong cost_time = (my_micro_time() - thd->start_utime) / 1000;
  auto &sctx = thd->m_main_security_ctx;
  if (rc_querylog_.isOn())
    rc_querylog_ << system::lock << "\tClientIp:" << (sctx.ip().length ? sctx.ip().str : "unkownn")
                 << "\tClientHostName:" << (sctx.host().length ? sctx.host().str : "unknown")
                 << "\tClientPort:" << thd->peer_port << "\tUser:" << sctx.user().str << global_serverinfo_
                 << "\tAffectRows:" << affect_rows << "\tResultRows:" << rows_sent << "\tDBName:" << thd->db().str
                 << "\tCosttime(ms):" << cost_time << "\tSQL:" << thd->query().str << system::unlock;
  TIANMU_LOG(LogCtl_Level::DEBUG, "Result: %ld Costtime(ms): %llu", rows_sent, cost_time);
}

void ResultSender::CleanUp() { restore_fields(fields, items_backup); }

void ResultSender::SendEof() { res->send_eof(thd); }

ResultSender::~ResultSender() { delete[] buf_lens; }

// stonedb8 List -> mem_root_deque
ResultExportSender::ResultExportSender(THD *thd, Query_result *result, mem_root_deque<Item *> &fields)
    : ResultSender(thd, result, fields) {
  export_res = dynamic_cast<exporter::select_tianmu_export *>(result);
  DEBUG_ASSERT(export_res);
}

void ResultExportSender::SendEof() {
  rcbuffer->FlushAndClose();
  export_res->SetRowCount((ha_rows)rows_sent);
  export_res->SendOk(thd);
}

void init_field_scan_helpers(THD *&thd, TABLE &tmp_table, TABLE_SHARE &share) {
  tmp_table.alias = 0;
  tmp_table.s = &share;
  init_tmp_table_share(thd, &share, "", 0, "", "", nullptr);

  tmp_table.s->db_create_options = 0;
  tmp_table.s->db_low_byte_first = false; /* or true */
  tmp_table.null_row = 0;                 // stonedb8 TABLE::maybe_null is deleted, and not use it
}

fields_t::value_type guest_field_type(THD *&thd, TABLE &tmp_table, Item *&item) {
  Field *field = NULL;
  Field *tmp_field = NULL;
  Field *def_field = NULL;
  if (item->type() == Item::FUNC_ITEM) {
    if (item->result_type() != STRING_RESULT)
      field = item->tmp_table_field(&tmp_table);
    else
      field = item->tmp_table_field_from_field_type(&tmp_table, 0);
  } else
    field =
        create_tmp_field(thd, &tmp_table, item, item->type(), (Func_ptr_array *)0, &tmp_field, &def_field, 0, 0, 0, 0);
  fields_t::value_type f(MYSQL_TYPE_STRING);
  if (field) {
    f = field->type();
    delete field;
  } else
    throw common::DatabaseException("failed to guess item type");
  return (f);
}

AttributeTypeInfo create_ati(THD *&thd, TABLE &tmp_table, Item *&item) {
  Field *field = NULL;
  Field *tmp_field = NULL;
  Field *def_field = NULL;
  if (item->type() == Item::FUNC_ITEM) {
    if (item->result_type() != STRING_RESULT)
      field = item->tmp_table_field(&tmp_table);
    else
      field = item->tmp_table_field_from_field_type(&tmp_table, 0);
  } else
    field =
        create_tmp_field(thd, &tmp_table, item, item->type(), (Func_ptr_array *)0, &tmp_field, &def_field, 0, 0, 0, 0);
  if (!field) throw common::DatabaseException("failed to guess item type");
  AttributeTypeInfo ati = Engine::GetCorrespondingATI(*field);
  delete field;
  return ati;
}

void ResultExportSender::Init(TempTable *t) {
  thd->set_proc_info("Exporting data");
  DBUG_PRINT("info", ("%s", thd->proc_info()));
  DEBUG_ASSERT(t);

  thd->lex->unit->offset_limit_cnt = 0;

  std::unique_ptr<system::IOParameters> iop;

  common::TIANMUError tianmu_error;

  export_res->send_result_set_metadata(thd, fields, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF);

  if ((tianmu_error = Engine::GetIOParams(iop, *thd, *export_res->SqlExchange(), 0, NULL, true)) !=
      common::ErrorCode::SUCCESS)
    throw common::Exception("Unable to get IOP");

  fields_t f;
  Item *item;
  TABLE tmp_table;  // Used during 'Create_field()'
  TABLE_SHARE share;
  init_field_scan_helpers(thd, tmp_table, share);

  std::vector<AttributeTypeInfo> deas = t->GetATIs(iop->GetEDF() != common::EDF::TRI_UNKNOWN);
  int i = 0;

  for (auto it = fields.begin(); it != fields.end(); ++it) {  // stonedb8
    item = *it;
    fields_t::value_type ft = guest_field_type(thd, tmp_table, item);
    f.push_back(ft);
    deas[i] = create_ati(thd, tmp_table, item);
    i++;
  }

  rcbuffer = std::make_shared<system::LargeBuffer>();
  if (!rcbuffer->BufOpen(*iop)) throw common::FileException("Unable to open file or named pipe.");

  rcde = common::DataFormat::GetDataFormat(iop->GetEDF())->CreateDataExporter(*iop);
  rcde->Init(rcbuffer, t->GetATIs(iop->GetEDF() != common::EDF::TRI_UNKNOWN), f, deas);
}

// send to Exporter
void ResultExportSender::SendRecord(const std::vector<std::unique_ptr<types::RCDataType>> &r) {
  Item *l_item;
  uint o = 0;
  for (auto it = fields.begin(); it != fields.end(); ++it) {  // stonedb8
    l_item = *it;
    types::RCDataType &rcdt(*r[o]);
    if (rcdt.IsNull())
      rcde->PutNull();
    else if (ATI::IsTxtType(rcdt.Type())) {
      types::BString val(rcdt.ToBString());
      if (l_item->data_type() == MYSQL_TYPE_DATE) {
        types::RCDateTime dt;
        types::ValueParserForText::ParseDateTime(val, dt, common::CT::DATE);
        rcde->PutDateTime(dt.GetInt64());
      } else if ((l_item->data_type() == MYSQL_TYPE_DATETIME) || (l_item->data_type() == MYSQL_TYPE_TIMESTAMP)) {
        types::RCDateTime dt;
        types::ValueParserForText::ParseDateTime(val, dt, common::CT::DATETIME);
        if (l_item->data_type() == MYSQL_TYPE_TIMESTAMP) {
          types::RCDateTime::AdjustTimezone(dt);
        }
        rcde->PutDateTime(dt.GetInt64());
      } else {
        // values from binary columns from TempTable are retrieved as
        // types::BString -> they get common::CT::STRING type, so an
        // additional check is necessary
        if (dynamic_cast<Item_field *>(l_item) && static_cast<Item_field *>(l_item)->field->binary())
          rcde->PutBin(val);
        else
          rcde->PutText(val);
      }
    } else if (ATI::IsBinType(rcdt.Type()))
      rcde->PutBin(rcdt.ToBString());
    else if (ATI::IsNumericType(rcdt.Type())) {
      if (rcdt.Type() == common::CT::BYTEINT)
        rcde->PutNumeric((char)dynamic_cast<types::RCNum &>(rcdt).ValueInt());
      else if (rcdt.Type() == common::CT::SMALLINT)
        rcde->PutNumeric((short)dynamic_cast<types::RCNum &>(rcdt).ValueInt());
      else if (rcdt.Type() == common::CT::INT || rcdt.Type() == common::CT::MEDIUMINT)
        rcde->PutNumeric((int)dynamic_cast<types::RCNum &>(rcdt).ValueInt());
      else
        rcde->PutNumeric(dynamic_cast<types::RCNum &>(rcdt).ValueInt());
    } else if (ATI::IsDateTimeType(rcdt.Type())) {
      if (rcdt.Type() == common::CT::TIMESTAMP) {
        // timezone conversion
        types::RCDateTime &dt(dynamic_cast<types::RCDateTime &>(rcdt));
        types::RCDateTime::AdjustTimezone(dt);
        rcde->PutDateTime(dt.GetInt64());
      } else
        rcde->PutDateTime(dynamic_cast<types::RCDateTime &>(rcdt).GetInt64());
    }
    o++;
  }
  rcde->PutRowEnd();
}
}  // namespace core
}  // namespace Tianmu
