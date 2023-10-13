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

#include "table_filter.h"

#include <algorithm>
#include <vector>

#include "conjunction_filter.h"
#include "constant_filter.h"
#include "item_cmpfunc.h"
#include "null_filter.h"

namespace Tianmu {
namespace IMCS {

std::unique_ptr<Table_filter_set> translate(Item *item,
                                            TianmuSecondaryShare *share);

std::unique_ptr<Table_filter_set> create_filter_set(TianmuSecondaryShare *share,
                                                    Item *item) {
  return translate(item, share);
}

std::unique_ptr<Table_filter_set> translate(Item *item,
                                            TianmuSecondaryShare *share) {
  switch (item->type()) {
    case Item::FUNC_ITEM: {
      Item_func *func_item = dynamic_cast<Item_func *>(item);
      Item **child_items = func_item->arguments();
      switch (func_item->functype()) {
        case Item_func::ISNULL_FUNC:
        case Item_func::ISNOTNULL_FUNC: {
          Item *item = child_items[0];
          if (item->type() != Item::FIELD_ITEM) {
            return nullptr;
          }
          Item_field *field_item = dynamic_cast<Item_field *>(item);
          Field *field = field_item->field;
          auto it = share->field_to_col_idx_.find(field->field_name);
          if (it == share->field_to_col_idx_.end()) {
            return nullptr;
          }
          std::unique_ptr<Table_filter_set> filter_set(new Table_filter_set());
          if (func_item->functype() == Item_func::ISNULL_FUNC) {
            filter_set->filters.emplace(it->second, new Null_filter());
          } else {
            filter_set->filters.emplace(it->second, new Not_null_filter());
          }
          return filter_set;
        }
        case Item_func::EQ_FUNC:
        case Item_func::NE_FUNC:
        case Item_func::LT_FUNC:
        case Item_func::LE_FUNC:
        case Item_func::GE_FUNC:
        case Item_func::GT_FUNC: {
          Item_func_comparison *cmp_func_item =
              dynamic_cast<Item_func_comparison *>(func_item);
          Item *le_item = child_items[0];
          Item *ri_item = child_items[1];
          bool reverse = false;
          // force to make left item be field
          if (ri_item->type() == Item::FIELD_ITEM) {
            std::swap(le_item, ri_item);
            reverse = true;
          }
          if (le_item->type() != Item::FIELD_ITEM) {
            return nullptr;
          }
          Item_num *num_item = nullptr;
          switch (ri_item->type()) {
            case Item::INT_ITEM:
            case Item::REAL_ITEM:
            case Item::DECIMAL_ITEM: {
              num_item = dynamic_cast<Item_num *>(ri_item);
              break;
            }
            case Item::STRING_ITEM:
              // TODO: handle string item
            case Item::NULL_ITEM:
              // TODO: handle null item
            default:
              return nullptr;
          }
          Comparison_type type;
          Item_func::Functype func_type = reverse
                                              ? cmp_func_item->rev_functype()
                                              : cmp_func_item->functype();
          switch (func_type) {
            case Item_func::EQ_FUNC:
              type = Comparison_type::EQUAL;
              break;
            case Item_func::NE_FUNC:
              type = Comparison_type::NOT_EQUAL;
              break;
            case Item_func::LT_FUNC:
              type = Comparison_type::LESS_THAN;
              break;
            case Item_func::LE_FUNC:
              type = Comparison_type::LESS_THAN_OR_EQUAL_TO;
              break;
            case Item_func::GE_FUNC:
              type = Comparison_type::GREATER_THAN;
              break;
            case Item_func::GT_FUNC:
              type = Comparison_type::GREATER_THAN_OR_EQUAL_TO;
              break;
            default:
              return nullptr;
          }
          Item_field *field_item = reinterpret_cast<Item_field *>(le_item);
          Field *field = field_item->field;
          auto it = share->field_to_col_idx_.find(field->field_name);
          if (it == share->field_to_col_idx_.end()) {
            return nullptr;
          }

          std::unique_ptr<Table_filter_set> filter_set(new Table_filter_set());
          filter_set->filters.emplace(
              it->second, new Constant_filter(type, num_item,
                                              share->fields_[it->second].type));
          return filter_set;
        }
        case Item_func::BETWEEN: {
          // TODO:
          return nullptr;
        }
        default:
          return nullptr;
      }
    }

    case Item::COND_ITEM: {
      Item_cond *cond_item = reinterpret_cast<Item_cond *>(item);
      if (cond_item->functype() != Item_func::COND_AND_FUNC &&
          cond_item->functype() != Item_func::COND_OR_FUNC) {
        return nullptr;
      }
      List<Item> *list = cond_item->argument_list();
      std::vector<std::unique_ptr<Table_filter_set>> filter_set_list;
      for (auto it = list->begin(); it != list->end(); it++) {
        std::unique_ptr<Table_filter_set> child_filter_set =
            translate(&(*it), share);
        if (child_filter_set.get() == nullptr) {
          continue;
        }
        filter_set_list.push_back(std::move(child_filter_set));
      };
      std::unique_ptr<Table_filter_set> filter_set(new Table_filter_set());
      for (auto &child_filter_set : filter_set_list) {
        for (auto &[cid, child_filter] : child_filter_set->filters) {
          auto it = filter_set->filters.find(cid);
          // local pointer
          Conjunction_filter *filter = nullptr;
          if (it == filter_set->filters.end()) {
            if (cond_item->functype() == Item_func::COND_AND_FUNC) {
              filter = new Conjunction_and_filter();
              filter_set->filters.emplace(cid, filter);
            } else {
              filter = new Conjunction_or_filter();
              filter_set->filters.emplace(cid, filter);
            }
          } else {
            filter = dynamic_cast<Conjunction_filter *>(it->second.get());
          }
          filter->add_child(child_filter);
        }
      }
      return filter_set;
    }
    default:
      return nullptr;
  }
}

}  // namespace IMCS

}  // namespace Tianmu