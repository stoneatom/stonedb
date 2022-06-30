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

#include "mysql_expression.h"

#include "common/assert.h"
#include "core/compilation_tools.h"
#include "core/engine.h"
#include "core/transaction.h"
#include "types/value_parser4txt.h"

namespace stonedb {
namespace core {
MysqlExpression::MysqlExpression(Item *item, Item2VarID &item2varid) {
  PrintItemTree("MysqlExpression constructed from item tree:", item);

  deterministic = dynamic_cast<Item_func_set_user_var *>(item) ? false : true;

  mysql_type = item->result_type();
  if (mysql_type == DECIMAL_RESULT) {
    uint orig_scale = item->decimals;
    Query::GetPrecisionScale(item, (int &)decimal_precision, (int &)decimal_scale, false);
    if (orig_scale > decimal_scale) {
      std::string s =
          "Precision of an expression result was reduced due to DECIMAL type "
          "limitations";
      push_warning(current_tx->Thd(), Sql_condition::WARN_LEVEL_WARN, ER_WARN_DATA_OUT_OF_RANGE, s.c_str());
    }
  } else {
    decimal_precision = 0;
    decimal_scale = 0;
  }

  this->item2varid = &item2varid;
  this->item = TransformTree(item, TransformDirection::FORWARD);
  this->item2varid = NULL;
}

MysqlExpression::~MysqlExpression() { TransformTree(item, TransformDirection::BACKWARD); }

bool MysqlExpression::HandledResultType(Item *item) {
  // Warning: if result type is DECIMAL and precision and/or scale greater than
  // 18, precision/scale of result values will be truncated.
  Item_result type = item->result_type();
  if ((dynamic_cast<Item_func_get_user_var *>(item)) && type == STRING_RESULT) {
    String s;
    if (item->val_str(&s) == NULL)
      item->max_length = 0;
    else
      item->max_length = (dynamic_cast<Item_func_get_user_var *>(item))->val_str(&s)->length();
  }

  return (type == INT_RESULT) || (type == REAL_RESULT) ||
         ((type == DECIMAL_RESULT) && (item->decimals != NOT_FIXED_DEC)) ||
         ((type == STRING_RESULT) && (item->max_length <= (uint32_t)4294967295U));
}

bool MysqlExpression::HandledFieldType(Item_result type) {
  // Here, we don't have to worry about precision/scale of decimals,
  // since all values that come to us were previously stored in SDB on int64_t,
  // so we'll cope with them
  return (type == INT_RESULT) || (type == REAL_RESULT) || (type == DECIMAL_RESULT) || (type == STRING_RESULT);
}

bool MysqlExpression::SanityAggregationCheck(Item *item, std::set<Item *> &fields, bool toplevel /*= true*/,
                                             bool *has_aggregation /*= NULL*/) {
  // printItemTree(item);
  if (!item) return false;
  if (toplevel && !HandledResultType(item)) return false;
  if (toplevel && has_aggregation) *has_aggregation = false;

  /*
   * *FIXME*
   * Item::SUBSELECT_ITEM is not handled here.
   */
  switch (static_cast<int>(item->type())) {
    case Item::INT_ITEM:
    case Item::REAL_ITEM:
    case Item::STRING_ITEM:
    case Item::DECIMAL_ITEM:
    case Item::NULL_ITEM:
    case Item::VARBIN_ITEM:
      return true;

    case static_cast<int>(Item_sdbfield::enumSDBFiledItem::SDBFIELD_ITEM):
      if (has_aggregation) {
        if (Query::IsAggregationItem(((Item_sdbfield *)item)->OriginalItem())) *has_aggregation = true;
      }
      fields.insert(((Item_sdbfield *)item)->OriginalItem());
      return true;
    case Item::FIELD_ITEM:
      if (((Item_field *)item)->field && !HandledFieldType(item->result_type())) return false;
      fields.insert(item);
      return true;
    case Item::FUNC_ITEM: {
      if (dynamic_cast<Item_func_trig_cond *>(item) != NULL) return false;

      // currently stored procedures not supported
      if (dynamic_cast<Item_func_sp *>(item) != NULL) {
        Item_func_sp *ifunc = dynamic_cast<Item_func_sp *>(item);
        if (ifunc->argument_count() != 0) return false;
        return true;
      }

      // Otherwise, it's a regular function/operator (hopefully)
      Item_func *ifunc = dynamic_cast<Item_func *>(item);
      bool correct = true;
      for (uint i = 0; i < ifunc->argument_count(); i++) {
        Item **args = ifunc->arguments();
        correct = (correct && SanityAggregationCheck(args[i], fields, false, has_aggregation));
        if (!correct) break;
      }
      return correct;
    }
    case Item::COND_ITEM: {
      Item_cond *cond = dynamic_cast<Item_cond *>(item);
      List_iterator<Item> li(*cond->argument_list());
      Item *arg;
      bool correct = true;
      while ((arg = li++)) {
        correct = (correct && SanityAggregationCheck(arg, fields, false, has_aggregation));
        if (!correct) break;
      }
      return correct;
    }
    case Item::SUM_FUNC_ITEM: {
      if (!HandledFieldType(item->result_type())) return false;
      if (has_aggregation) *has_aggregation = true;
      fields.insert(item);
      return true;
    }
    case Item::REF_ITEM: {
      Item_ref *iref = dynamic_cast<Item_ref *>(item);
      if (!iref->ref) return false;
      Item *arg = *(iref->ref);
      return SanityAggregationCheck(arg, fields, toplevel, has_aggregation);
    }

    default:
      return false;
  }
}

Item_sdbfield *MysqlExpression::GetSdbfieldItem(Item_field *ifield) {
  auto key = item2varid->find(ifield);
  DEBUG_ASSERT(key != item2varid->end());
  auto it = sdbfields_cache.find(key->second);
  Item_sdbfield *sdbfield = NULL;
  if (it != sdbfields_cache.end()) {
    sdbfield = *it->second.begin();
    sdbfield->varID.push_back(key->second);
  } else {
    sdbfield = new Item_sdbfield(ifield, key->second);
    std::set<Item_sdbfield *> s_tmp;
    s_tmp.insert(sdbfield);
    sdbfields_cache.insert(make_pair(key->second, s_tmp));
  }
  return (sdbfield);
}

void MysqlExpression::RemoveUnusedVarID(Item *root) {
  switch (static_cast<int>(root->type())) {
    case Item::FIELD_ITEM:
    case Item::SUM_FUNC_ITEM:
      break;
    case (Item::Type)Item_sdbfield::enumSDBFiledItem::SDBFIELD_ITEM: {
      Item_sdbfield *sdbfield = static_cast<Item_sdbfield *>(root);
      sdbfield->varID.pop_back();
    } break;

    case Item::FUNC_ITEM: {
      Item_func *ifunc = static_cast<Item_func *>(root);
      for (uint i = 0; i < ifunc->argument_count(); i++) {
        Item **args = ifunc->arguments();
        RemoveUnusedVarID(args[i]);
      }
      break;
    }
    case Item::COND_ITEM: {
      Item_cond *cond = static_cast<Item_cond *>(root);
      List_iterator<Item> li(*cond->argument_list());
      Item *arg;
      while ((arg = li++)) RemoveUnusedVarID(arg);
      break;
    }

    case Item::REF_ITEM: {
      Item_ref *iref = dynamic_cast<Item_ref *>(root);
      Item *arg = *(iref->ref);
      RemoveUnusedVarID(arg);
      break;
    }

    case Item::INT_ITEM:
    case Item::REAL_ITEM:
    case Item::STRING_ITEM:
    case Item::DECIMAL_ITEM:
    case Item::NULL_ITEM:
    case Item::VARBIN_ITEM:
      break;

    default:
      STONEDB_ERROR(
          "Unexpected type of item passed to "
          "MysqlExpression::IncrementUsageCounter()");
  }
}

Item *MysqlExpression::TransformTree(Item *root, TransformDirection dir) {
  // Warning: the Item "tree" given by MySQL not necessarily has to be a tree,
  // but instead it can be a DAG (Directed Acyclic Graph).
  // In this case, TransformTree may visit the same nodes several times,
  // so it must be prepared for seeing nodes that are already transformed.
  PrintItemTree("transform tree", root);
  switch (static_cast<int>(root->type())) {
    case Item::FIELD_ITEM: {
      if (dir == TransformDirection::BACKWARD)  // already transformed (DAG case)
        return root;
      // dir == FORWARD
      Item_sdbfield *sdbfield = GetSdbfieldItem(static_cast<Item_field *>(root));
      vars.insert(sdbfield->varID[0]);

      return sdbfield;
    }
    case Item::SUM_FUNC_ITEM: {
      if (dir == TransformDirection::BACKWARD)  // already transformed (DAG case)
        return root;
      // dir == FORWARD
      Item_sdbfield *sdbsum = GetSdbfieldItem(static_cast<Item_field *>(root));
      vars.insert(sdbsum->varID[0]);
      // Item_sdbsum* sdbsum = new Item_sdbsum(aggregation,
      // (*item2varid)[aggregation]);
      return sdbsum;
    }
    case (Item::Type)Item_sdbfield::enumSDBFiledItem::SDBFIELD_ITEM: {
      Item_sdbfield *sdbfield = static_cast<Item_sdbfield *>(root);
      if (dir == TransformDirection::FORWARD) {  // already transformed (DAG case)
        Item_field *ifield = sdbfield->OriginalItem();
        // if(!(sdbfield->varID == (*item2varid)[ifield])) {
        //  char err[256];
        //  VarID v1 = sdbfield->varID;
        //  VarID v2 = (*item2varid)[ifield];
        //  std::sprintf(err, "Internal error. The same table field occurs twice
        //  in two expressions "
        //          "but in each one has different identifiers assigned: (%d,%d)
        //          vs
        //          (%d,%d).",
        //          v1.tab, v1.col, v2.tab, v2.col);
        //  //std::fprintf(stderr, "Error in MysqlExpression::TransformTree():
        //  %s\n", err); throw common::Exception(err);
        //}
        std::set<Item_sdbfield *> s_tmp;
        s_tmp.insert(sdbfield);
        sdbfields_cache.insert(std::make_pair((*item2varid)[ifield], s_tmp));

        vars.insert((*item2varid)[ifield]);
        sdbfield->varID.push_back((*item2varid)[ifield]);
        // sdbfields_cache.insert(std::make_pair(sdbfield->varID, sdbfield));
        return root;
      }
      // dir == BACKWARD
      // delete sdbfield;   // done by MySQL not STONEDB, for each Item subclass
      sdbfield->ClearBuf();
      return sdbfield->OriginalItem();
    }

    case Item::FUNC_ITEM: {
      Item_func *ifunc = static_cast<Item_func *>(root);
      if (dir == TransformDirection::FORWARD &&
                (dynamic_cast<Item_func_rand *>(ifunc) || dynamic_cast<Item_func_last_insert_id *>(ifunc) ||
                 dynamic_cast<Item_func_get_system_var *>(ifunc) || dynamic_cast<Item_func_is_free_lock *>(ifunc) ||
                 dynamic_cast<Item_func_is_used_lock *>(ifunc) || dynamic_cast<Item_func_row_count *>(ifunc) ||
                 dynamic_cast<Item_func_sp *>(ifunc)
                 /*                   // disputable functions start here - should they be nondeterministic?
                                           || dynamic_cast<Item_func_weekday*>(ifunc)
                                           || dynamic_cast<Item_func_unix_timestamp*>(ifunc)
                                           || dynamic_cast<Item_func_time_to_sec*>(ifunc) ||dynamic_cast<Item_date*>(ifunc)
                                           || dynamic_cast<Item_func_curtime*>(ifunc) ||dynamic_cast<Item_func_now*>(ifunc)
                                           || dynamic_cast<Item_func_microsecond*>(ifunc) ||dynamic_cast<Item_func_last_day*>(ifunc)
                                           */  // end
                                               // of
                 // disputable
                 // functions
                 ))
        deterministic = false;

      //          if((dynamic_cast<Item_func_between*>(ifunc))) {
      //              Item_func_between* ifb =
      //              dynamic_cast<Item_func_between*>(ifunc);
      //              TransformTree(*ifb->ge_cmp.a,dir);
      //              TransformTree(*ifb->ge_cmp.b,dir);
      //              TransformTree(*ifb->le_cmp.a,dir);
      //              TransformTree(*ifb->le_cmp.b,dir);
      //          }
      for (uint i = 0; i < ifunc->argument_count(); i++) {
        Item **args = ifunc->arguments();
        args[i] = TransformTree(args[i], dir);
      }
      return root;
    }

    case Item::COND_ITEM: {
      Item_cond *cond = static_cast<Item_cond *>(root);
      List_iterator<Item> li(*cond->argument_list());
      Item *arg;
      while ((arg = li++)) *(li.ref()) = TransformTree(arg, dir);
      return root;
    }

    case Item::REF_ITEM: {
      Item_ref *iref = dynamic_cast<Item_ref *>(root);
      Item *arg = *(iref->ref);
      arg = TransformTree(arg, dir);
      *(iref->ref) = arg;
      return root;
    }

    case Item::INT_ITEM:
    case Item::REAL_ITEM:
    case Item::STRING_ITEM:
    case Item::DECIMAL_ITEM:
    case Item::NULL_ITEM:
    case Item::VARBIN_ITEM:
      return root;

    default:
      STONEDB_ERROR("Unexpected type of item passed to MysqlExpression::TransformTree()");
      return root;
  }
}

MysqlExpression::SetOfVars &MysqlExpression::GetVars() { return vars; }

void MysqlExpression::SetBufsOrParams(var_buf_t *bufs) {
  DEBUG_ASSERT(bufs);
  for (auto &it : sdbfields_cache) {
    auto buf_set = bufs->find(it.first);
    if (buf_set != bufs->end()) {
      // for each sdbitem* in the vector it->second put its buffer to
      // buf_set.second
      for (auto &sdbfield : it.second) {
        ValueOrNull *von;
        sdbfield->SetBuf(von);
        buf_set->second.push_back(value_or_null_info_t(ValueOrNull(), von));
      }
    }
  }
}

DataType MysqlExpression::EvalType(TypeOfVars *tv) {
  // set types of variables (_sdbfieldsCache)
  if (tv) {
    DataType fieldtype;
    auto sdbfield_set = sdbfields_cache.begin();
    while (sdbfield_set != sdbfields_cache.end()) {
      auto it = tv->find(sdbfield_set->first);
      if (it != tv->end()) {
        for (auto &sdbfield : sdbfield_set->second) {
          fieldtype = it->second;
          sdbfield->SetType(fieldtype);
        }
      }
      sdbfield_set++;
    }
  }

  // calculate result type
  //  type = DataType();
  switch (mysql_type) {
    case INT_RESULT:
      type = DataType(common::CT::BIGINT);
      break;
    case REAL_RESULT:
      type = DataType(common::CT::FLOAT);
      break;
    case DECIMAL_RESULT:
      type = DataType(common::CT::NUM, decimal_precision, decimal_scale);
      break;
    case STRING_RESULT:  // GA: in case of time item->max_length can contain
                         // invalid value
      if ((item->type() != Item_sdbfield::get_sdbitem_type() && item->field_type() == MYSQL_TYPE_TIME) ||
          (item->type() == Item_sdbfield::get_sdbitem_type() &&
           static_cast<Item_sdbfield *>(item)->IsAggregation() == false && item->field_type() == MYSQL_TYPE_TIME))
        type = DataType(common::CT::TIME, 17, 0, item->collation);
      else if ((item->type() != Item_sdbfield::get_sdbitem_type() && item->field_type() == MYSQL_TYPE_TIMESTAMP) ||
               (item->type() == Item_sdbfield::get_sdbitem_type() &&
                static_cast<Item_sdbfield *>(item)->IsAggregation() == false &&
                item->field_type() == MYSQL_TYPE_TIMESTAMP))
        type = DataType(common::CT::TIMESTAMP, 17, 0, item->collation);
      else if ((item->type() != Item_sdbfield::get_sdbitem_type() && item->field_type() == MYSQL_TYPE_DATETIME) ||
               (item->type() == Item_sdbfield::get_sdbitem_type() &&
                static_cast<Item_sdbfield *>(item)->IsAggregation() == false &&
                item->field_type() == MYSQL_TYPE_DATETIME))
        type = DataType(common::CT::DATETIME, 17, 0, item->collation);
      else if ((item->type() != Item_sdbfield::get_sdbitem_type() && item->field_type() == MYSQL_TYPE_DATE) ||
               (item->type() == Item_sdbfield::get_sdbitem_type() &&
                static_cast<Item_sdbfield *>(item)->IsAggregation() == false && item->field_type() == MYSQL_TYPE_DATE))
        type = DataType(common::CT::DATE, 17, 0, item->collation);
      else
        // type = DataType(common::CT::STRING, item->max_length, 0,
        // item->collation);
        type =
            DataType(common::CT::STRING,
                     (item->str_value.length() == 0) ? item->max_length
                                                     : (item->str_value.length() * item->collation.collation->mbmaxlen),
                     0, item->collation);
      // here, it seems that item->max_length
      // stores total length in bytes, not in characters
      // (which are different in case of multi-byte characters)
      // TempFix2017-1-13:
      // Multiply mbmaxlen when max_length equals str length to work around
      // the bug inside common::TxtDataFormat::StaticExtrnalSize.
      break;
    case ROW_RESULT:
      DEBUG_ASSERT(0 && "unexpected type: ROW_RESULT");
      break;
  }
  return type;
}

MysqlExpression::StringType MysqlExpression::GetStringType() {
  if (mysql_type == STRING_RESULT) {
    if ((item->type() != Item_sdbfield::get_sdbitem_type() && item->field_type() == MYSQL_TYPE_TIME) ||
        (item->type() == Item_sdbfield::get_sdbitem_type() &&
         static_cast<Item_sdbfield *>(item)->IsAggregation() == false && item->field_type() == MYSQL_TYPE_TIME))
      return StringType::STRING_TIME;
    else
      return StringType::STRING_NORMAL;
  }
  return StringType::STRING_NOTSTRING;
}

std::shared_ptr<ValueOrNull> MysqlExpression::Evaluate() {
  switch (mysql_type) {
    case INT_RESULT:
      return ItemInt2ValueOrNull(item);
    case REAL_RESULT:
      return ItemReal2ValueOrNull(item);
    case DECIMAL_RESULT:
      return ItemDecimal2ValueOrNull(item, decimal_scale);
    case STRING_RESULT:
      return ItemString2ValueOrNull(item, type.precision, type.attrtype);
    default:
      DEBUG_ASSERT(0 && "unexpected value");
  }
  return std::make_shared<ValueOrNull>();
}

void MysqlExpression::CheckDecimalError(int err) {
  // See mysql_source_dir/include/decimal.h for definition of error values
  // (E_DEC_...)
  if (err > E_DEC_TRUNCATED)
    throw common::Exception(
        "Numeric result of an expression is too large and cannot be handled by "
        "stonedb. "
        "Please use an explicit cast to a data type handled by stonedb, "
        "e.g. CAST(<expr> AS DECIMAL(18,6)).");
}

std::shared_ptr<ValueOrNull> MysqlExpression::ItemReal2ValueOrNull(Item *item) {
  auto val = std::make_shared<ValueOrNull>();
  double v = item->val_real();
  if (v == -0.0) v = 0.0;
  int64_t vint = *(int64_t *)&v;
  if (vint == common::NULL_VALUE_64) vint++;
  v = *(double *)&vint;
  val->SetDouble(v);
  if (item->null_value) {
    return std::make_shared<ValueOrNull>();
  }
  return val;
}

std::shared_ptr<ValueOrNull> MysqlExpression::ItemDecimal2ValueOrNull(Item *item, int dec_scale) {
  auto val = std::make_shared<ValueOrNull>();
  my_decimal dec;
  my_decimal *retdec = item->val_decimal(&dec);
  if (retdec != NULL) {
    if (retdec != &dec) my_decimal2decimal(retdec, &dec);
    int64_t v;
    int err;
    // err = my_decimal_shift((uint)-1, &dec, item->decimals <= 18 ?
    // item->decimals : 18);
    if (dec_scale == -1)
      err = my_decimal_shift((uint)-1, &dec, item->decimals <= 18 ? item->decimals : 18);
    else
      err = my_decimal_shift((uint)-1, &dec, dec_scale <= 18 ? dec_scale : 18);
    CheckDecimalError(err);
    err = my_decimal2int((uint)-1, &dec, false, (longlong *)&v);
    CheckDecimalError(err);
    val->SetFixed(v);
  }
  if (item->null_value) return std::make_shared<ValueOrNull>();
  return val;
}

std::shared_ptr<ValueOrNull> MysqlExpression::ItemString2ValueOrNull(Item *item, int maxstrlen, common::CT a_type) {
  auto val = std::make_shared<ValueOrNull>();
  String string_result;
  String *ret = item->val_str(&string_result);
  if (ret != NULL) {
    char *p = ret->c_ptr_safe();
    if (ATI::IsDateTimeType(a_type)) {
      types::RCDateTime rcdt;
      types::BString rbs(p);
      if (rbs.IsNull())
        return val = std::make_shared<ValueOrNull>(common::NULL_VALUE_64);
      else {
        common::ErrorCode rc = types::ValueParserForText::ParseDateTime(rbs, rcdt, a_type);
        if (common::IsError(rc)) {
          return val = std::make_shared<ValueOrNull>(common::NULL_VALUE_64);
        }
        val->SetFixed(rcdt.GetInt64());
      }
    } else {
      uint len = ret->length();
      DEBUG_ASSERT(p[len] == 0);
      if (maxstrlen >= 0 && len > uint(maxstrlen)) {
        len = maxstrlen;
        p[len] = 0;
      }
      val->SetString(p, len);
      val->MakeStringOwner();
    }
  }
  if (item->null_value) return std::make_shared<ValueOrNull>();
  return val;
}

std::shared_ptr<ValueOrNull> MysqlExpression::ItemInt2ValueOrNull(Item *item) {
  auto val = std::make_shared<ValueOrNull>();
  int64_t v = item->val_int();
  if (v == common::NULL_VALUE_64) v++;
  if (v < 0 && item->unsigned_flag)
    throw common::NotImplementedException("Out of range: unsigned data type is not supported.");
  val->SetFixed(v);
  if (item->null_value) return std::make_shared<ValueOrNull>();
  return val;
}

bool SameSDBField(Item_sdbfield *const &l, Item_sdbfield *const &r) {
  return (!(l || r)) || (l && r && ((*l) == (*r)));
}

bool SameSDBFieldSet(MysqlExpression::sdbfields_cache_t::value_type const &l,
                     MysqlExpression::sdbfields_cache_t::value_type const &r) {
  return l.second.size() == r.second.size() && equal(l.second.begin(), l.second.end(), r.second.begin(), SameSDBField);
}

bool operator==(Item const &, Item const &);

namespace {
bool generic_item_same(Item const &l_, Item const &r_) {
  return ((&l_ == &r_) || ((l_.type() == r_.type()) && (l_.result_type() == r_.result_type()) &&
                           (l_.cast_to_int_type() == r_.cast_to_int_type()) &&
                           (l_.string_field_type() == r_.string_field_type()) && (l_.field_type() == r_.field_type()) &&
                           (l_.const_during_execution() == r_.const_during_execution()) && (l_ == r_)));
}
}  // namespace

bool operator==(Item const &l_, Item const &r_) {
  Item::Type t = l_.type();
  bool same = t == r_.type();
  if (same) {
    switch (static_cast<int>(t)) {
      case (Item::FIELD_ITEM): {
        same = false;  // not implemented
                       //              Item_field const* l = static_cast<Item_field
                       //              const*>(&l_); Item_field const* r =
                       //              static_cast<Item_field const*>(&r_);
                       //                  same = l->field->
      } break;
      case (Item::COND_ITEM):
      case (Item::FUNC_ITEM): {
        Item_func const *l = static_cast<Item_func const *>(&l_);
        Item_func const *r = static_cast<Item_func const *>(&r_);
        same = !std::strcmp(l->func_name(), r->func_name());
        same = same && (l->arg_count == r->arg_count);
        same = same && l->functype() == r->functype();
        if (l->functype() == Item_func::GUSERVAR_FUNC) {
          if (same) {
            Item_func_get_user_var const *ll = static_cast<Item_func_get_user_var const *>(&l_);
            Item_func_get_user_var const *rr = static_cast<Item_func_get_user_var const *>(&r_);
            same = !std::strcmp(ll->name.ptr(), rr->name.ptr());
          }
        } else {
          same = same && l->arg_count == r->arg_count;
          for (uint i = 0; same && (i < l->arg_count); ++i) same = same && (*l->arguments()[i] == *r->arguments()[i]);

          // Item_func* lll = (Item_func*)&l;
          // Item_func* mmm = (Item_func*)&r;

          // bool x = l->const_item();
          // bool y = r->const_item();
          // longlong zzz = lll->val_int_result();
          // longlong vvv = mmm->val_int_result();
          same = same && (l->const_item() == r->const_item());
          if (same && l->const_item()) same = ((Item_func *)&l_)->val_int() == ((Item_func *)&r_)->val_int();
          if (dynamic_cast<const Item_date_add_interval *>(&l_)) {
            const Item_date_add_interval *l = static_cast<const Item_date_add_interval *>(&l_);
            const Item_date_add_interval *r = static_cast<const Item_date_add_interval *>(&r_);
            same = same && dynamic_cast<const Item_date_add_interval *>(&r_);
            same = same && ((l->int_type == r->int_type) && (l->date_sub_interval == r->date_sub_interval));
          }
          if (l->functype() == Item_func::IN_FUNC) {
            const Item_func_in *l = static_cast<const Item_func_in *>(&l_);
            const Item_func_in *r = static_cast<const Item_func_in *>(&r_);
            same = same && l->negated == r->negated;
          }
          if (same && (l->functype() == Item_func::COND_AND_FUNC || l->functype() == Item_func::COND_OR_FUNC)) {
            Item_cond *l = const_cast<Item_cond *>(static_cast<Item_cond const *>(&l_));
            Item_cond *r = const_cast<Item_cond *>(static_cast<Item_cond const *>(&r_));
            List_iterator<Item> li(*l->argument_list());
            List_iterator<Item> ri(*r->argument_list());
            Item *il, *ir;
            while ((il = li++) && (ir = ri++)) {
              same = same && *il == *ir;
            }
            same = same && (!ir && !il);
          }
          if (same && l->functype() == Item_func::XOR_FUNC) {
            same = false;  // not implemented.
          }
        }
      } break;
      case static_cast<int>(Item_sdbfield::enumSDBFiledItem::SDBFIELD_ITEM): {
        Item_sdbfield const *l = static_cast<Item_sdbfield const *>(&l_);
        Item_sdbfield const *r = static_cast<Item_sdbfield const *>(&r_);
        same = (*l == *r);
      } break;
      case (Item::REF_ITEM): {
        Item_ref const *l = static_cast<Item_ref const *>(&l_);
        Item_ref const *r = static_cast<Item_ref const *>(&r_);
        same = (!(l->ref || r->ref)) ||
               (l->ref && r->ref &&
                ((!(*(l->ref) || *(r->ref))) || (*(l->ref) && *(r->ref) && (*(*(l->ref)) == *(*(r->ref))))));
      } break;
      case (Item::NULL_ITEM):
      case (Item::STRING_ITEM):
      case (Item::DECIMAL_ITEM):
      case (Item::REAL_ITEM):
      case (Item::VARBIN_ITEM):
      case (Item::INT_ITEM): {
        same = l_.eq(&r_, true);
      } break;
      default: {
        same = generic_item_same(l_, r_);
      } break;
    }
  }
  return (same);
}

bool MysqlExpression::operator==(MysqlExpression const &other) const {
  return ((mysql_type == other.mysql_type) && (decimal_precision == other.decimal_precision) &&
          (decimal_scale == other.decimal_scale) && (deterministic == other.deterministic) &&
          (*item == *(other.item)) && (sdbfields_cache.size() == other.sdbfields_cache.size()) && vars == other.vars &&
          equal(sdbfields_cache.begin(), sdbfields_cache.end(), other.sdbfields_cache.begin(), SameSDBFieldSet));
}
}  // namespace core
}  // namespace stonedb
