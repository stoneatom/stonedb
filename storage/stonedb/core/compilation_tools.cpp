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

#include "compilation_tools.h"
#include "core/compiled_query.h"
#include "core/engine.h"
#include "core/mysql_expression.h"
#include "core/value_set.h"

namespace stonedb {
namespace core {
Item *UnRef(Item *item) {
  if (!item) return 0;
  bool changed;
  do {
    changed = false;
    if (item->type() == Item::REF_ITEM) {
      item = *(((Item_ref *)item)->ref);
      changed = true;
    }
    // Item_int_with_ref* int_ref = dynamic_cast<Item_int_with_ref*>(item);
    // if(int_ref) {//item_int_with_ref
    //	item = int_ref->real_item();
    //	changed = true;
    //}
  } while (changed);
  return item;
}

const char *TablePath(TABLE_LIST *tab) {
  ASSERT_MYSQL_STRING(tab->table->s->path);
  return tab->table->s->path.str;
}

int OperationUnmysterify(Item *item, common::ColOperation &oper, bool &distinct,
                         [[maybe_unused]] const int group_by_clause) {
  distinct = false;

  item = UnRef(item);

  switch (static_cast<int>(item->type())) {
    case Item::NULL_ITEM:
    case Item::INT_ITEM:
    case Item::VARBIN_ITEM:
    case Item::STRING_ITEM:
    case Item::DECIMAL_ITEM:
    case Item::REAL_ITEM:
    case Item::FUNC_ITEM:
    case Item::COND_ITEM:
    case Item::SUBSELECT_ITEM:
    case static_cast<int>(Item_sdbfield::enumSDBFiledItem::SDBFIELD_ITEM):
    case Item::FIELD_ITEM:                  // regular select
      oper = common::ColOperation::LISTING; /*GROUP_BY : LISTING;*/
      break;

    case Item::SUM_FUNC_ITEM:  // min(k), max(k), count(), avg(k), sum
      switch (((Item_sum *)item)->sum_func()) {
        case Item_sum::MIN_FUNC:
          oper = common::ColOperation::MIN; /*common::ColOperation::MIN;*/
          break;
        case Item_sum::MAX_FUNC:
          oper = common::ColOperation::MAX; /*common::ColOperation::MAX;*/
          break;
        case Item_sum::SUM_DISTINCT_FUNC:
          distinct = true;
          [[fallthrough]];
        case Item_sum::SUM_FUNC:
          oper = common::ColOperation::SUM; /*common::ColOperation::SUM;*/
          break;
        case Item_sum::AVG_DISTINCT_FUNC:
          distinct = true;
          [[fallthrough]];
        case Item_sum::AVG_FUNC:
          oper = common::ColOperation::AVG; /*common::ColOperation::AVG;*/
          break;
        case Item_sum::COUNT_DISTINCT_FUNC:
          distinct = true;
          [[fallthrough]];
        case Item_sum::COUNT_FUNC:
          oper = common::ColOperation::COUNT; /*common::ColOperation::COUNT;*/
          break;
        case Item_sum::STD_FUNC:
          if (((Item_sum_variance *)item)->sample == 1)
            oper = common::ColOperation::STD_SAMP;
          else
            oper = common::ColOperation::STD_POP;
          break;
        case Item_sum::VARIANCE_FUNC:
          if (((Item_sum_variance *)item)->sample == 1)
            oper = common::ColOperation::VAR_SAMP;
          else
            oper = common::ColOperation::VAR_POP;
          break;
        case Item_sum::SUM_BIT_FUNC:
          if (dynamic_cast<Item_sum_xor *>(item) != NULL)
            oper = common::ColOperation::BIT_XOR;
          else if (dynamic_cast<Item_sum_and *>(item) != NULL)
            oper = common::ColOperation::BIT_AND;
          else
            oper = common::ColOperation::BIT_OR;
          break;
        case Item_sum::GROUP_CONCAT_FUNC:
          distinct = ((Item_func_group_concat *)item)->get_distinct();
          STONEDB_LOG(LogCtl_Level::DEBUG, "group_concat distinct %d, sepertator %s, direction %d", distinct,
                      ((Item_func_group_concat *)item)->get_separator()->c_ptr(),
                      ((Item_func_group_concat *)item)->direction());
          oper = common::ColOperation::GROUP_CONCAT;
          break;
        default:
          return RETURN_QUERY_TO_MYSQL_ROUTE;
      }
      break;
    default:
      return RETURN_QUERY_TO_MYSQL_ROUTE;
  }
  return RCBASE_QUERY_ROUTE;
}

const char *FieldType(enum_field_types ft) {
  // For declaration of enum_field_types see mysql.../include/mysql_com.h
  static const char *low_names[] = {
      "MYSQL_TYPE_DECIMAL", "MYSQL_TYPE_TINY", "MYSQL_TYPE_SHORT",     "MYSQL_TYPE_LONG",     "MYSQL_TYPE_FLOAT",
      "MYSQL_TYPE_DOUBLE",  "MYSQL_TYPE_NULL", "MYSQL_TYPE_TIMESTAMP", "MYSQL_TYPE_LONGLONG", "MYSQL_TYPE_INT24",
      "MYSQL_TYPE_DATE",    "MYSQL_TYPE_TIME", "MYSQL_TYPE_DATETIME",  "MYSQL_TYPE_YEAR",     "MYSQL_TYPE_NEWDATE",
      "MYSQL_TYPE_VARCHAR", "MYSQL_TYPE_BIT"};
  static const char *high_names[] = {"MYSQL_TYPE_NEWDECIMAL", "MYSQL_TYPE_ENUM",        "MYSQL_TYPE_SET",
                                     "MYSQL_TYPE_TINY_BLOB",  "MYSQL_TYPE_MEDIUM_BLOB", "MYSQL_TYPE_LONG_BLOB",
                                     "MYSQL_TYPE_BLOB",       "MYSQL_TYPE_VAR_STRING",  "MYSQL_TYPE_STRING",
                                     "MYSQL_TYPE_GEOMETRY"};

  size_t i = ft;
  if (i < sizeof(low_names) / sizeof(*low_names)) return low_names[i];
  if ((i >= 246) && (i <= 255)) return high_names[i - 246];
  return "<unknown field type>";
}

void PrintItemTree(Item *item, int indent) {
  static const char *name_of[] = {"FIELD_ITEM",        "FUNC_ITEM",         "SUM_FUNC_ITEM",      "STRING_ITEM",
                                  "INT_ITEM",          "REAL_ITEM",         "NULL_ITEM",          "VARBIN_ITEM",
                                  "COPY_STR_ITEM",     "FIELD_AVG_ITEM",    "DEFAULT_VALUE_ITEM", "PROC_ITEM",
                                  "COND_ITEM",         "REF_ITEM",          "FIELD_STD_ITEM",     "FIELD_VARIANCE_ITEM",
                                  "INSERT_VALUE_ITEM", "SUBSELECT_ITEM",    "ROW_ITEM",           "CACHE_ITEM",
                                  "TYPE_HOLDER",       "PARAM_ITEM",        "TRIGGER_FIELD_ITEM", "DECIMAL_ITEM",
                                  "XPATH_NODESET",     "XPATH_NODESET_CMP", "VIEW_FIXER_ITEM"};
  static const char *sum_name_of[] = {"COUNT_FUNC",       "COUNT_DISTINCT_FUNC", "SUM_FUNC",     "SUM_DISTINCT_FUNC",
                                      "AVG_FUNC",         "AVG_DISTINCT_FUNC",   "MIN_FUNC",     "MAX_FUNC",
                                      "STD_FUNC",         "VARIANCE_FUNC",       "SUM_BIT_FUNC", "UDF_SUM_FUNC",
                                      "GROUP_CONCAT_FUNC"};

  for (int i = 0; i <= indent; i++) std::fprintf(stderr, "*");
  std::fprintf(stderr, " ");
  indent += 1;

  if (!item) {
    std::fprintf(stderr, "NULL item\n");
    return;
  }

  const char *name;
  Item::Type type = item->type();

  if ((int)type < sizeof(name_of) / sizeof(*name_of))
    name = name_of[type];
  else
    name = "enumSDBFiledItem::SDBFIELD_ITEM";

  const char *result = "<unknown result type>";
  switch (item->result_type()) {
    case STRING_RESULT:
      result = "STRING_RESULT";
      break;
    case INT_RESULT:
      result = "INT_RESULT";
      break;
    case REAL_RESULT:
      result = "REAL_RESULT";
      break;
    case DECIMAL_RESULT:
      result = "DECIMAL_RESULT";
      break;
    default:
      break;
  }
  std::fprintf(stderr, "%s %s @%p %s %s max_length=%d", name, item->full_name(), (void *)item,
               FieldType(item->field_type()), result, item->max_length);

  if (item->result_type() == DECIMAL_RESULT)
    std::fprintf(stderr, " [prec=%d,dec=%d,int=%d]", item->decimal_precision(), (int)item->decimals,
                 item->decimal_int_part());

  switch (type) {
    case Item::FUNC_ITEM: {
      Item_func *func = static_cast<Item_func *>(item);

      std::fprintf(stderr, " func_name=%s\n", func->func_name());
      String str;
      // GA, print function takes extra argument but do not use it in the base
      // class.
      func->print(&str, QT_ORDINARY);
      std::fprintf(stderr, " f contents: %s\n", str.c_ptr_safe());

      Item **args = func->arguments();
      for (uint i = 0; i < func->argument_count(); i++) PrintItemTree(args[i], indent);
      return;
    }
    case Item::COND_ITEM: {
      Item_cond *cond = static_cast<Item_cond *>(item);

      std::fprintf(stderr, " func_name=%s\n", cond->func_name());

      List_iterator<Item> li(*cond->argument_list());
      Item *arg;
      while ((arg = li++)) PrintItemTree(arg, indent);
      return;
    }
    case Item::SUM_FUNC_ITEM: {
      Item_sum *sum_func = static_cast<Item_sum *>(item);

      uint index = sum_func->sum_func();
      const char *sumname;
      if (index >= sizeof(sum_name_of) / sizeof(*sum_name_of))
        sumname = "<UNKNOWN>";
      else
        sumname = sum_name_of[index];

      std::fprintf(stderr, "  %s\n", sumname);

      uint args_count = sum_func->get_arg_count();
      for (uint i = 0; i < args_count; i++) PrintItemTree(sum_func->get_arg(i), indent);
      return;
    }
    case Item::REF_ITEM: {
      Item_ref *ref = static_cast<Item_ref *>(item);
      Item *real = ref->real_item();
      std::fprintf(stderr, "\n");
      if (ref != real) PrintItemTree(real, indent);
      return;
    }
    case Item::INT_ITEM: {
      Item_int_with_ref *int_ref = dynamic_cast<Item_int_with_ref *>(item);
      if (!int_ref) break;
      // else item is an instance of Item_int_with_ref, not Item_int
      std::fprintf(stderr, " [Item_int_with_ref]\n");
      PrintItemTree(int_ref->real_item(), indent);
      return;
    }
    case Item::SUBSELECT_ITEM: {
      Item_subselect *ss = dynamic_cast<Item_subselect *>(item);
      std::fprintf(stderr, " subselect type %d\n", ss->substype());
      String str;
      // GA, print function takes extra argument but do not use it.
      // ss->get_unit()->print(&str, QT_ORDINARY);  //  gone in mysql5.6
      std::fprintf(stderr, "%s\n", str.c_ptr_safe());
      break;
    }
    default:
      //
      break;
  }

  std::fprintf(stderr, "\n");
}

void PrintItemTree(char const *info, Item *item) {
  // Comment out this line to switch printing on
  return;
  // Comment out this line to switch printing on

  std::fprintf(stderr, "%s\n", info);
  PrintItemTree(item);
}
}  // namespace core
}  // namespace stonedb
