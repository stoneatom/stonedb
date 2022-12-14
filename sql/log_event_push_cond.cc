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

#include "log_event.h"

#include "base64.h"            // base64_encode
#include "binary_log_funcs.h"  // my_timestamp_binary_length

#include <base64.h>
#include <my_bitmap.h>
#include <map>
#include "rpl_utility.h"
/* This is necessary for the List manipuation */
#include "sql_list.h"                           /* I_List */
#include "hash.h"
#include "sql_digest.h"
#include "rpl_gtid.h"
#include "xa_aux.h"

#include "sql_base.h" 
#include "sql_parse.h"
#include "sql_optimizer.h"
#include "log.h"


/*
  In the binlog row format Rows_log_event:: do_table_scan_and_update() logic.
  This file provides conditional push down function for slave library synchronization logic.
  It is used to avoid full table scanning and improve the performance of master slave synchronization.
  Currently, only Tianmu Engine uses this function.
*/


/**
  Prints a quoted string to io cache.
  Control characters are displayed as hex sequence, e.g. \x00
  
  @param[in] file              IO cache
  @param[in] prt               Pointer to string
  @param[in] length            String length
*/

static void
my_b_write_quoted(IO_CACHE *file, const uchar *ptr, uint length)
{
  const uchar *s;
  my_b_printf(file, "'");
  for (s= ptr; length > 0 ; s++, length--)
  {
    if (*s > 0x1F && *s != '\'' && *s != '\\')
      my_b_write(file, s, 1);
    else
    {
      uchar hex[10];
      size_t len= my_snprintf((char*) hex, sizeof(hex), "%s%02x", "\\x", *s);
      my_b_write(file, hex, len);
    }
  }
  my_b_printf(file, "'");
}

/**
  Prints a bit string to io cache in format  b'1010'.
  
  @param[in] file              IO cache
  @param[in] ptr               Pointer to string
  @param[in] nbits             Number of bits
*/
static void
my_b_write_bit(char *file, const uchar *ptr, uint nbits)
{
  uint bitnum, nbits8= ((nbits + 7) / 8) * 8, skip_bits= nbits8 - nbits;
  strncpy(file , "b'" , 2);
  file += 2;
  for (bitnum= skip_bits ; bitnum < nbits8; bitnum++)
  {
    int is_set= (ptr[(bitnum) / 8] >> (7 - bitnum % 8))  & 0x01;
    char *c = (char*) (is_set ? "1" : "0");
    strncpy(file , c ,1);
    file ++;
  }
  strncpy(file , "'" ,1);
}


/**
  Prints a packed string to io cache.
  The string consists of length packed to 1 or 2 bytes,
  followed by string data itself.
  
  @param[in] file              IO cache
  @param[in] ptr               Pointer to string
  @param[in] length            String size
  
  @retval   - number of bytes scanned.
*/
static size_t
my_b_write_quoted_with_length(IO_CACHE *file, const uchar *ptr, uint length)
{
  if (length < 256)
  {
    length= *ptr;
    my_b_write_quoted(file, ptr + 1, length);
    return length + 1;
  }
  else
  {
    length= uint2korr(ptr);
    my_b_write_quoted(file, ptr + 2, length);
    return length + 2;
  }
}


/**
  Prints a 32-bit number in both signed and unsigned representation
  
  @param[in] file              IO cache
  @param[in] sl                Signed number
  @param[in] ul                Unsigned number
*/
static void
my_b_write_sint32_and_uint32(IO_CACHE *file, int32 si, uint32 ui)
{
  my_b_printf(file, "%d", si);
  if (si < 0)
    my_b_printf(file, " (%u)", ui);
}

static double PowOfTen(int exponent) { return std::pow((double)10, exponent); }

/**
 Conversion conditions of column data
  
  @param[std::string] str_condition       condition cache
  @param[Field *] f                       Column Properties
  @param[in] meta                         Column meta information
  @param[bool] unwanted_str               Whether string condition is required
  
  @retval   - number of bytes scanned from ptr.
*/
static size_t
filed_str_to_sql_conditions(std::string &str_condition, Field *f , uint meta, bool unwanted_str)
{
  const uchar *ptr = f->is_null() ? nullptr:f->ptr;
  if(!ptr){
    return 0;
  }
  uint32 length= 0;
  uint32 ASC_max = 255;
  if (f->type() == MYSQL_TYPE_STRING)
  {
    if (meta > ASC_max)
    {
      uint byte0= meta >> 8;
      uint byte1= meta & 0xFF;
      
      if ((byte0 & 0x30) != 0x30)
      {
        /* a long CHAR() field: see #37426 */
        length= byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
      }
      else
        length = meta & 0xFF;
    }
    else
      length= meta;
  }

  switch (f->type()) {
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_TINY:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_LONGLONG:
    {
      int64_t v = f->val_int();
      str_condition = std::to_string(v);
      return 4;
    }
  case MYSQL_TYPE_DECIMAL:
  //case MYSQL_TYPE_FLOAT:
  case MYSQL_TYPE_DOUBLE:
    {
      double v = f->val_real();
      str_condition = std::to_string(v);
      return 4;
    }
  case MYSQL_TYPE_BIT:
    {
      /* Meta-data: bit_len, bytes_in_rec, 2 bytes */
      uint nbits= ((meta >> 8) * 8) + (meta & 0xFF);
      length= (nbits + 7) / 8;
      char tmp[120] = {0};
      my_b_write_bit(tmp, ptr, nbits);
      str_condition = tmp;
      return length;
    }
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
  {
      MYSQL_TIME my_time;
      std::memset(&my_time, 0, sizeof(my_time));
      f->get_time(&my_time);
      char tmp[64] = {0};
      sprintf(tmp, "'%04d-%02d-%02d %02d:%02d:%02d'",
                   my_time.year , my_time.month ,my_time.day,
                   my_time.hour , my_time.minute ,my_time.second);
      str_condition = tmp;
      return 8;
    }
  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_TIME2:
    {
      MYSQL_TIME my_time;
      std::memset(&my_time, 0, sizeof(my_time));
      f->get_time(&my_time);
      char tmp[32] = {0};
      sprintf(tmp, "'%02d:%02d:%02d'",
                  my_time.hour , my_time.minute ,my_time.second);
      str_condition = tmp;            
      return 3;
    }
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_NEWDATE:
    {
      MYSQL_TIME my_time;
      std::memset(&my_time, 0, sizeof(my_time));
      f->get_time(&my_time);
      char tmp[32] = {0};
      std::sprintf(tmp, "'%04d-%02d-%02d'", my_time.year , my_time.month ,my_time.day);
      str_condition = tmp;
      return 4;
    }

  case MYSQL_TYPE_YEAR:
    {
      MYSQL_TIME my_time;
      std::memset(&my_time, 0, sizeof(my_time));
      f->get_time(&my_time);
      char tmp[32] = {0};
      sprintf(tmp, "'%04d'",  my_time.year);
      str_condition = tmp;
      return 1;
    }
  
  case MYSQL_TYPE_ENUM:
    switch (meta & 0xFF) {
    case 1:
    {
      int64_t v = f->val_int();
      str_condition = std::to_string(v);
      return 1;
    }
    case 2:
    {
      int32 i32= uint2korr(ptr);
      char tmp[64] = {0};
      sprintf(tmp, "%d", i32);
      str_condition = tmp;
      return 2;
    }
    default:
      return 0;
    }
    break;
    
  case MYSQL_TYPE_SET:
  {
    char tmp[64] = {0};
    my_b_write_bit(tmp, ptr , (meta & 0xFF) * 8);
    str_condition = tmp;
    return meta & 0xFF;
  }
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_JSON:
  {
    if(unwanted_str){
        return 0;
      }
    String str;
    f->val_str(&str);
    std::string str1(str.ptr(), str.length());
    str_condition = "'" + str1 + "'";
    return 4;
  }
  default:
    break;
  }
  return 0;
}

//Check whether there are numbers or time types in the table
static bool checklist_have_number_or_time(Field **field, uint field_num)
{
  if(!field) return false;
  for (uint col_id = 0; col_id < field_num; col_id++) {
    Field *f = field[col_id];
    switch (f->type()) {
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_DOUBLE:
      case MYSQL_TYPE_TIMESTAMP:
      case MYSQL_TYPE_TIMESTAMP2:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_DATETIME2:
      case MYSQL_TYPE_YEAR:
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_NEWDATE:
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_TIME2:
      {
        return true;
      }
      default:
        break;
    }
  }
  return false;
}


bool Rows_log_event::column_information_to_conditions(std::string &sql_statemens,
                                                      std::string &prefix)
{
  sql_statemens += prefix;
  Field **field = m_table->field;
  if(!field) return false;
  /*
    If there is a unique constraint,
    use the field of the unique constraint as the push down condition
  */
  std::string key_field_name;
  if(m_table->s->key_info &&
    m_table->s->key_info->key_part &&
    m_table->s->key_info->key_part->field)
  {
    key_field_name = m_table->s->key_info->key_part->field->field_name;
  }
  int cond_num = 0;
  bool unwanted_str = false;
  /*
    If there is a number or time type in the table,
    the string type value is not used as the push down condition.
  */
  if(checklist_have_number_or_time(field, m_table->s->fields)) unwanted_str = true;

  for (uint col_id = 0; col_id < m_table->s->fields; col_id++) {
    Field *f = field[col_id];
    uint16 meta;
    f->save_field_metadata((uchar*)&meta);
    std::string str_cond;

    if(!key_field_name.empty()) {

      if(key_field_name.compare(f->field_name) != 0) {
        continue;
      }

      filed_str_to_sql_conditions(str_cond, f, meta, false);
      if(str_cond.empty()) {
        col_id = 0;
        key_field_name = "";
        continue;
      }

      sql_statemens += "`" + std::string(f->field_name) + "`=" + str_cond;
      sql_statemens.push_back('\0');
      return true;
    }

    filed_str_to_sql_conditions(str_cond, f, meta, unwanted_str);
    if(str_cond.empty()){
      continue;
    }
    if(cond_num > 0 )
      sql_statemens += " and ";
    sql_statemens += "`" + std::string(f->field_name) + "`=" + str_cond;
    cond_num ++;
  }
  sql_statemens.push_back('\0');
  return true;
}


bool Rows_log_event::row_event_to_statement(LEX_CSTRING &lex_str, std::string &sql_statement)
{
  Log_event_type general_type_code= get_general_type_code();
  if((general_type_code != binary_log::DELETE_ROWS_EVENT) && 
     (general_type_code != binary_log::UPDATE_ROWS_EVENT))
    return false;
  /*
    Here, only need to use the predicate condition after (where) to filter data,
    regardless of the SQL type,
    The syntax of select is simple, so select statements are used here to compile conditions
  */
  std::string sql_command = "SELECT * FROM `", sql_clause = " WHERE ";

  sql_statement = sql_command + m_table->s->db.str + "`.`" + m_table->s->table_name.str +"`";
  if (!column_information_to_conditions(sql_statement, sql_clause))
    return false;
  lex_str.str = sql_statement.c_str();
  lex_str.length = sql_statement.length();
  return true;
}

bool Rows_log_event::can_push_down()
{
  //Judge whether it is a tianmu engine
  if(!(m_table && m_table->s && 
    (m_table->s->db_type() ? m_table->s->db_type()->db_type == DB_TYPE_TIANMU: false)))
    return false;
  THD *thd = m_table->in_use;
  if(!thd || !thd->lex || !thd->lex->select_lex)
    return false;
  /*
    Clean up parse- and item trees in case this function was called before for
    the same THD.
  */
  lex_start(thd);
  LEX_CSTRING lex_str;
  //IO_CACHE io_chache_statement;
  std::string sql_statement;
  Parser_state parser_state;
  SELECT_LEX *const select_lex= thd->lex->select_lex;
  Item *conds = nullptr;
  ORDER *order= nullptr;

  //Get SQL statement
  if(!row_event_to_statement(lex_str, sql_statement))
    goto error;
  DBUG_PRINT("info",(" fuxiang row_event_to_statement: %s", sql_statement.c_str()));
  thd->set_query(lex_str);
  thd->set_query_id(next_query_id());
  DBUG_EXECUTE_IF("tianmu_can_push_down_falied",
    {
      goto error;
    });

  //Parse statement
  if (parser_state.init(thd, thd->query().str, thd->query().length))
    goto error;
  parser_state.m_input.m_has_digest= true;
  parser_state.m_input.m_compute_digest= true;
  if(parse_sql(thd, &parser_state, NULL))
    goto error;

  //Set the association between syntax tree and table and column
  select_lex->table_list.first->table = m_table;
  //Set access permissions for columns
  thd->want_privilege = 0;
  /* Perform name resolution only in the first table - 'table_list'. */
  select_lex->context.resolve_in_table_list_only(select_lex->get_table_list());
  select_lex->make_active_options(0, 0);
  if (select_lex->setup_conds(thd))
    goto error;
  
  //Get the conditions for pushing down
  if (select_lex->get_optimizable_conditions(thd, &conds, NULL))
    goto error;

  //Resolve references to table column for a function and its argument
  //conds->fix_fields(thd, &conds);
  order = select_lex->order_list.first;
  if (conds || order)
    static_cast<void>(substitute_gc(thd, select_lex, conds, NULL, order));
  /*
    Non delete tables are pruned in SELECT_LEX::prepare,
  */
  if (prune_partitions(thd, m_table, conds))
    goto error;

  //Conditional optimization
  if (conds)
  {
    COND_EQUAL *cond_equal= NULL;
    Item::cond_result result;
    if (optimize_cond(thd, &conds, &cond_equal, select_lex->join_list,
                      &result))
      goto error;
    if (conds)
    {
      conds= substitute_for_best_equal_field(conds, cond_equal, 0);
      if (conds == NULL)
      {
        goto error;
      }
      conds->update_used_tables();
    }
  }else
    goto error;

  //Conditional push down
  m_table->file->inited = handler::RND;
  if(!m_table->file->cond_push(conds))
    goto end;
  else
    goto error;
end:
  return true; 
error:
  sql_print_information("can_push_down : falied");
  sql_print_information(lex_str.str);
  return false;
}


