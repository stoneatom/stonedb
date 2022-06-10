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

#include <ctime>

#include "common/assert.h"
#include "core/engine.h"
#include "core/transaction.h"

namespace stonedb {
namespace core {
bool Engine::ConvertToField(Field *field, types::RCDataType &rcitem, std::vector<uchar> *blob_buf) {
  if (rcitem.IsNull()) {
    std::memset(field->ptr, 0, 2);
    field->set_null();
    return true;
  }

  field->set_notnull();

  switch (field->type()) {
    case MYSQL_TYPE_VARCHAR: {
      DEBUG_ASSERT(dynamic_cast<types::BString *>(&rcitem));
      types::BString &str_val = (types::BString &)rcitem;
      if (str_val.size() > field->field_length)
        throw common::DatabaseException("Incorrect field size: " + std::to_string(str_val.size()));
      if (field->field_length <= 255)
        str_val.PutVarchar((char *&)field->ptr, 1, false);
      else if (field->field_length <= SHORT_MAX)
        str_val.PutVarchar((char *&)field->ptr, 2, false);
      break;
    }
    case MYSQL_TYPE_STRING:
      if (dynamic_cast<types::BString *>(&rcitem)) {
        ((types::BString &)rcitem).PutString((char *&)field->ptr, (ushort)field->field_length, false);
      } else {
        rcitem.ToBString().PutString((char *&)field->ptr, (ushort)field->field_length, false);
      }
      break;
    case MYSQL_TYPE_BLOB: {
      DEBUG_ASSERT(dynamic_cast<types::BString *>(&rcitem));
      Field_blob *blob = (Field_blob *)field;
      if (blob_buf == NULL) {
        blob->set_ptr(((types::BString &)rcitem).len, (uchar *)((types::BString &)rcitem).val);
        blob->copy();
      } else {
        blob->store(((types::BString &)rcitem).val, ((types::BString &)rcitem).len, &my_charset_bin);
        uchar *src, *tgt;

        uint packlength = blob->pack_length_no_ptr();
        uint length = blob->get_length(blob->ptr);
        std::memcpy(&src, blob->ptr + packlength, sizeof(char *));
        if (src) {
          blob_buf->resize(length);
          tgt = &((*blob_buf)[0]);
          bmove(tgt, src, length);
          std::memcpy(blob->ptr + packlength, &tgt, sizeof(char *));
        }
      }
      break;
    }
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL: {
      my_decimal md;
      if (rcitem.Type() == common::CT::REAL) {
        double2decimal((double)((types::RCNum &)(rcitem)), &md);
      } else {
        int is_null;
        Engine::Convert(is_null, &md, rcitem);
      }
      decimal_round(&md, &md, ((Field_new_decimal *)field)->decimals(), HALF_UP);
      decimal2bin(&md, (uchar *)field->ptr, ((Field_new_decimal *)field)->precision,
                  ((Field_new_decimal *)field)->decimals());
      break;
    }
    default:
      switch (rcitem.Type()) {
        case common::CT::BYTEINT:
        case common::CT::SMALLINT:
        case common::CT::MEDIUMINT:
        case common::CT::INT:
        case common::CT::BIGINT:
        case common::CT::REAL:
        case common::CT::FLOAT:
        case common::CT::NUM:
          switch (field->type()) {
            case MYSQL_TYPE_TINY:
              *(char *)field->ptr = (char)(int64_t)((types::RCNum &)(rcitem));
              break;
            case MYSQL_TYPE_SHORT:
              *(short *)field->ptr = (short)(int64_t)((types::RCNum &)(rcitem));
              break;
            case MYSQL_TYPE_INT24:
              int3store((char *)field->ptr, (int)(int64_t)((types::RCNum &)(rcitem)));
              break;
            case MYSQL_TYPE_LONG:
              *(int *)field->ptr = (int)(int64_t)((types::RCNum &)(rcitem));
              break;
            case MYSQL_TYPE_LONGLONG:
              *(int64_t *)field->ptr = (int64_t)((types::RCNum &)(rcitem));
              break;
            case MYSQL_TYPE_FLOAT:
              *(float *)field->ptr = (float)((types::RCNum &)(rcitem));
              break;
            case MYSQL_TYPE_DOUBLE:
              *(double *)field->ptr = (double)((types::RCNum &)(rcitem));
              break;
            default:
              DEBUG_ASSERT(!"No data types conversion available!");
              break;
          }
          break;
        case common::CT::STRING:
          switch (field->type()) {
            case MYSQL_TYPE_VARCHAR: {
              types::BString &str_val = (types::BString &)rcitem;
              if (str_val.size() > field->field_length)
                throw common::DatabaseException("Incorrect field size " + std::to_string(str_val.size()));
              if (field->field_length <= 255) {
                str_val.PutVarchar((char *&)field->ptr, 1, false);
              } else if (field->field_length <= SHORT_MAX) {  // PACK_SIZE - 1
                str_val.PutVarchar((char *&)field->ptr, 2, false);
              }
              break;
            }
            case MYSQL_TYPE_STRING:
              ((types::BString &)rcitem).PutString((char *&)field->ptr, (ushort)field->field_length, false);
              break;
            case MYSQL_TYPE_BLOB: {
              Field_blob *blob = (Field_blob *)field;
              if (blob_buf == NULL) {
                blob->set_ptr(((types::BString &)rcitem).len, (uchar *)((types::BString &)rcitem).val);
                blob->copy();
              } else {
                blob->store(((types::BString &)rcitem).val, ((types::BString &)rcitem).len, &my_charset_bin);
                uchar *src, *tgt;

                uint packlength = blob->pack_length_no_ptr();
                uint length = blob->get_length(blob->ptr);
                std::memcpy(&src, blob->ptr + packlength, sizeof(char *));
                if (src) {
                  blob_buf->resize(length);
                  tgt = &((*blob_buf)[0]);
                  bmove(tgt, src, length);
                  std::memcpy(blob->ptr + packlength, &tgt, sizeof(char *));
                }
              }
              break;
            }
            case MYSQL_TYPE_DATE: {
              char tmp[10];
              char *tmpptr = tmp;
              ((types::BString &)rcitem).PutString(tmpptr, ushort(sizeof(tmp)), false);
              ((Field_newdate *)field)->store(tmp, sizeof(tmp), NULL);
              break;
            }
            case MYSQL_TYPE_TIME: {
              char tmp[10];
              char *tmpptr = tmp;
              ((types::BString &)rcitem).PutString(tmpptr, ushort(sizeof(tmp)), false);
              ((Field_time *)field)->store(tmp, sizeof(tmp), NULL);
              break;
            }
            case MYSQL_TYPE_DATETIME: {
              char tmp[19];
              char *tmpptr = tmp;
              ((types::BString &)rcitem).PutString(tmpptr, ushort(sizeof(tmp)), false);
              ((Field_datetime *)field)->store(tmp, sizeof(tmp), NULL);
              break;
            }
            default:
              ((types::BString &)rcitem).PutString((char *&)field->ptr, (ushort)field->field_length, false);
              break;
          }

          break;
        case common::CT::YEAR: {
          ASSERT(field->type() == MYSQL_TYPE_YEAR);
          auto rcdt = dynamic_cast<types::RCDateTime *>(&rcitem);
          MYSQL_TIME my_time = {};
          rcdt->Store(&my_time, MYSQL_TIMESTAMP_DATE);
          field->store_time(&my_time);
          break;
        }
        case common::CT::DATE: {
          if (field->type() == MYSQL_TYPE_DATE || field->type() == MYSQL_TYPE_NEWDATE) {
            auto rcdt = dynamic_cast<types::RCDateTime *>(&rcitem);
            MYSQL_TIME my_time = {};
            rcdt->Store(&my_time, MYSQL_TIMESTAMP_DATE);
            field->store_time(&my_time);
          }
          break;
        }

          /*
          Datetime representations:
           - Packed:  In-memory representation
           - Binary:  On-disk representation
           - Structure inside MySQL: MYSQL_TIME

               Type        before MySQL 5.6.4        Storage as of MySQL 5.6.4
               ---------------------------------------------------------------
               YEAR        1 byte,little endian      Unchanged
               DATE        3 bytes,little endian     Unchanged
               TIME        3 bytes,little endian     3 bytes +
          fractional-seconds storage, big endian TIMESTAMP   4 bytes,little
          endian     4 bytes + fractional-seconds storage, big endian DATETIME
          8 bytes,little endian     5 bytes + fractional-seconds storage, big
          endian

          DATETIME encoding for nonfractional part:
          -----------------------------------------

              1 bit  sign           (1= non-negative, 0= negative)
             17 bits year*13+month  (year 0-9999, month 0-12)
              5 bits day            (0-31)
              5 bits hour           (0-23)
              6 bits minute         (0-59)
              6 bits second         (0-59)
             ---------------------------
             40 bits = 5 bytes
             The sign bit is always 1

          */

        case common::CT::TIME: {
          ASSERT(field->type() == MYSQL_TYPE_TIME);
          auto rcdt = dynamic_cast<types::RCDateTime *>(&rcitem);
          MYSQL_TIME my_time = {};
          rcdt->Store(&my_time, MYSQL_TIMESTAMP_TIME);
          field->store_time(&my_time);
          break;
        }
        case common::CT::DATETIME: {
          ASSERT(field->type() == MYSQL_TYPE_DATETIME);
          auto rcdt = dynamic_cast<types::RCDateTime *>(&rcitem);
          MYSQL_TIME my_time = {};
          rcdt->Store(&my_time, MYSQL_TIMESTAMP_DATETIME);
          field->store_time(&my_time);
          break;
        }
        case common::CT::TIMESTAMP: {
          auto rcdt = dynamic_cast<types::RCDateTime *>(&rcitem);
          MYSQL_TIME my_time = {};
          types::RCDateTime::AdjustTimezone(*rcdt);
          rcdt->Store(&my_time, MYSQL_TIMESTAMP_DATETIME);
          field->store_time(&my_time);
          break;
        }
        default:
          break;
      }
      break;
  }
  return false;
}

#define DIG_PER_DEC1 9
#define DIG_BASE 1000000000
#define ROUND_UP(X) (((X) + DIG_PER_DEC1 - 1) / DIG_PER_DEC1)

int Engine::Convert(int &is_null, my_decimal *value, types::RCDataType &rcitem, int output_scale) {
  if (rcitem.IsNull())
    is_null = 1;
  else {
    if (!Engine::AreConvertible(rcitem, MYSQL_TYPE_NEWDECIMAL)) return false;
    is_null = 0;
    if (rcitem.Type() == common::CT::NUM) {
      types::RCNum *rcn = (types::RCNum *)(&rcitem);
      int intg = rcn->GetDecIntLen();
      int frac = rcn->GetDecFractLen();
      int intg1 = ROUND_UP(intg);
      int frac1 = ROUND_UP(frac);
      value->intg = intg;
      value->frac = frac;
      int64_t ip = rcn->GetIntPart();
      int64_t fp = (rcn->ValueInt() % (int64_t)types::Uint64PowOfTen(rcn->Scale()));
      bool special_value_minbigint = false;
      if (uint64_t(ip) == 0x8000000000000000ULL) {
        // a special case, cannot be converted like that
        special_value_minbigint = true;
        ip += 1;  // just for now...
      }
      if (ip < 0) {
        ip *= -1;
        value->sign(true);
        if (fp < 0) fp *= -1;
      } else if (ip == 0 && fp < 0) {
        fp *= -1;
        value->sign(true);
      } else
        value->sign(false);

      decimal_digit_t *buf = value->buf + intg1;
      for (int i = intg1; i > 0; i--) {
        *--buf = decimal_digit_t(ip % DIG_BASE);
        if (special_value_minbigint && i == intg1) {
          *buf += 1;  // revert the special case (plus, because now it is
                      // unsigned part)
        }
        ip /= DIG_BASE;
      }
      buf = value->buf + intg1 + (frac1 - 1);
      int64_t tmp(fp);
      int no_digs = 0;
      while (tmp > 0) {
        tmp /= 10;
        no_digs++;
      }
      int tmp_prec = rcn->Scale();

      for (; frac1; frac1--) {
        int digs_to_take = tmp_prec - (frac1 - 1) * DIG_PER_DEC1;
        if (digs_to_take < 0) digs_to_take = 0;
        tmp_prec -= digs_to_take;
        int cur_pow = DIG_PER_DEC1 - digs_to_take;
        *buf-- = decimal_digit_t((fp % (int64_t)types::Uint64PowOfTen(digs_to_take)) *
                                 (int64_t)types::Uint64PowOfTen(cur_pow));
        fp /= (int64_t)types::Uint64PowOfTen(digs_to_take);
      }
      int output_scale_1 = (output_scale > 18) ? 18 : output_scale;
      my_decimal_round(0, value, (output_scale_1 == -1) ? frac : output_scale_1, false, value);
      return 1;
    } else if (rcitem.Type() == common::CT::REAL || rcitem.Type() == common::CT::FLOAT) {
      double2decimal((double)((types::RCNum &)(rcitem)), (decimal_t *)value);
      return 1;
    } else if (ATI::IsIntegerType(rcitem.Type())) {
      longlong2decimal((longlong)((types::RCNum &)(rcitem)).ValueInt(), (decimal_t *)value);
      return 1;
    }
    return false;
  }
  return 1;
}

int Engine::Convert(int &is_null, int64_t &value, types::RCDataType &rcitem, enum_field_types f_type) {
  if (rcitem.IsNull())
    is_null = 1;
  else {
    is_null = 0;
    if (rcitem.Type() == common::CT::NUM || rcitem.Type() == common::CT::BIGINT) {
      value = (int64_t)(types::RCNum &)rcitem;
      switch (f_type) {
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_INT24:
          if (value == common::NULL_VALUE_64)
            value = common::NULL_VALUE_32;
          else if (value == common::PLUS_INF_64)
            value = std::numeric_limits<int>::max();
          else if (value == common::MINUS_INF_64)
            value = SDB_INT_MIN;
          break;
        case MYSQL_TYPE_TINY:
          if (value == common::NULL_VALUE_64)
            value = common::NULL_VALUE_C;
          else if (value == common::PLUS_INF_64)
            value = SDB_TINYINT_MAX;
          else if (value == common::MINUS_INF_64)
            value = SDB_TINYINT_MIN;
          break;
        case MYSQL_TYPE_SHORT:
          if (value == common::NULL_VALUE_64)
            value = common::NULL_VALUE_SH;
          else if (value == common::PLUS_INF_64)
            value = SDB_SMALLINT_MAX;
          else if (value == common::MINUS_INF_64)
            value = SDB_SMALLINT_MIN;
          break;
        default:
          break;
      }
      return 1;
    } else if (rcitem.Type() == common::CT::INT || rcitem.Type() == common::CT::MEDIUMINT) {
      value = (int)(int64_t) dynamic_cast<types::RCNum &>(rcitem);
      return 1;
    } else if (rcitem.Type() == common::CT::BYTEINT) {
      value = (char)(int64_t) dynamic_cast<types::RCNum &>(rcitem);
      return 1;
    } else if (rcitem.Type() == common::CT::SMALLINT) {
      value = (short)(int64_t) dynamic_cast<types::RCNum &>(rcitem);
      return 1;
    } else if (rcitem.Type() == common::CT::YEAR) {
      value = dynamic_cast<types::RCDateTime &>(rcitem).Year();
      return 1;
    } else if (rcitem.Type() == common::CT::REAL) {
      value = (int64_t)(double)dynamic_cast<types::RCNum &>(rcitem);
      return 1;
    }
  }
  return 0;
}

int Engine::Convert(int &is_null, double &value, types::RCDataType &rcitem) {
  if (rcitem.IsNull())
    is_null = 1;
  else {
    if (!Engine::AreConvertible(rcitem, MYSQL_TYPE_DOUBLE)) return 0;
    is_null = 0;
    if (rcitem.Type() == common::CT::REAL) {
      value = (double)dynamic_cast<types::RCNum &>(rcitem);
      return 1;
    } else if (rcitem.Type() == common::CT::FLOAT) {
      value = (float)dynamic_cast<types::RCNum &>(rcitem);
      return 1;
    }
  }
  return 0;
}

int Engine::Convert(int &is_null, String *value, types::RCDataType &rcitem, enum_field_types f_type) {
  if (rcitem.IsNull())
    is_null = 1;
  else {
    if (!Engine::AreConvertible(rcitem, MYSQL_TYPE_STRING)) return 0;
    is_null = 0;
    if (f_type == MYSQL_TYPE_VARCHAR || f_type == MYSQL_TYPE_VAR_STRING) {
      types::BString str = rcitem.ToBString();
      value->set_ascii(str.val, str.len);
      value->copy();
    } else if (f_type == MYSQL_TYPE_STRING) {
      types::BString str = rcitem.ToBString();
      value->set_ascii(str.val, str.len);
      value->copy();
    } else if (f_type == MYSQL_TYPE_NEWDATE || f_type == MYSQL_TYPE_DATE) {
      types::BString str = rcitem.ToBString();
      value->set_ascii(str.val, str.len);
      value->copy();
    } else if (f_type == MYSQL_TYPE_TIME) {
      types::BString str = rcitem.ToBString();
      value->set_ascii(str.val, str.len);
      value->copy();
    } else if (f_type == MYSQL_TYPE_DATETIME) {
      types::BString str = rcitem.ToBString();
      value->set_ascii(str.val, str.len);
      value->copy();
    } else if (f_type == MYSQL_TYPE_TIMESTAMP) {
      if (types::RCDateTime *rcdt = dynamic_cast<types::RCDateTime *>(&rcitem)) {
        if (*rcdt != types::RC_TIMESTAMP_SPEC) {
          MYSQL_TIME local_time;
          my_time_t secs = stonedb_sec_since_epoch(rcdt->Year(), rcdt->Month(), rcdt->Day(), rcdt->Hour(),
                                                   rcdt->Minute(), rcdt->Second());
          current_tx->Thd()->variables.time_zone->gmt_sec_to_TIME(&local_time, secs);
          char buf[32];
          local_time.second_part = rcdt->MicroSecond();
          my_datetime_to_str(&local_time, buf, 0);
          value->set_ascii(buf, 19);
        } else {
          value->set_ascii("0000-00-00 00:00:00", 19);
        }
      } else {
        types::BString str = rcitem.ToBString();
        value->set_ascii(str.val, str.len);
      }
      value->copy();
    } else if (f_type == MYSQL_TYPE_BLOB || f_type == MYSQL_TYPE_MEDIUM_BLOB) {
      types::BString str = rcitem.ToBString();
      value->set_ascii(str.val, str.len);
      value->copy();
    }
    return 1;
  }
  return 0;
}

bool Engine::AreConvertible(types::RCDataType &rcitem, enum_field_types my_type, [[maybe_unused]] uint length) {
  /*if(rcitem->Type() == Engine::GetCorrespondingType(my_type, length) ||
   rcitem->IsNull()) return true;*/
  common::CT sdbtype = rcitem.Type();
  switch (my_type) {
    case MYSQL_TYPE_LONGLONG:
      if (sdbtype == common::CT::INT || sdbtype == common::CT::MEDIUMINT || sdbtype == common::CT::BIGINT ||
          (sdbtype == common::CT::NUM && dynamic_cast<types::RCNum &>(rcitem).Scale() == 0))
        return true;
      break;
    case MYSQL_TYPE_NEWDECIMAL:
      if (sdbtype == common::CT::FLOAT || sdbtype == common::CT::REAL || ATI::IsIntegerType(sdbtype) ||
          sdbtype == common::CT::NUM)
        return true;
      break;
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
      return (sdbtype == common::CT::STRING || sdbtype == common::CT::VARCHAR || sdbtype == common::CT::BYTE ||
              sdbtype == common::CT::VARBYTE || sdbtype == common::CT::LONGTEXT || sdbtype == common::CT::BIN);
    case MYSQL_TYPE_YEAR:
      return sdbtype == common::CT::YEAR;
    case MYSQL_TYPE_SHORT:
      return sdbtype == common::CT::SMALLINT;
    case MYSQL_TYPE_TINY:
      return sdbtype == common::CT::BYTEINT;
    case MYSQL_TYPE_INT24:
      return sdbtype == common::CT::MEDIUMINT;
    case MYSQL_TYPE_LONG:
      return sdbtype == common::CT::INT;
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
      return sdbtype == common::CT::FLOAT || sdbtype == common::CT::REAL;
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
      return (sdbtype == common::CT::DATETIME || sdbtype == common::CT::TIMESTAMP);
    case MYSQL_TYPE_TIME:
      return sdbtype == common::CT::TIME;
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_DATE:
      return sdbtype == common::CT::DATE;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
      return true;
    default:
      break;
  }
  return false;
}

common::CT Engine::GetCorrespondingType(const enum_field_types &eft) {
  switch (eft) {
    case MYSQL_TYPE_YEAR:
      return common::CT::YEAR;
    case MYSQL_TYPE_SHORT:
      return common::CT::SMALLINT;
    case MYSQL_TYPE_TINY:
      return common::CT::BYTEINT;
    case MYSQL_TYPE_INT24:
      return common::CT::MEDIUMINT;
    case MYSQL_TYPE_LONG:
      return common::CT::INT;
    case MYSQL_TYPE_LONGLONG:
      return common::CT::BIGINT;
    case MYSQL_TYPE_FLOAT:
      return common::CT::FLOAT;
    case MYSQL_TYPE_DOUBLE:
      return common::CT::REAL;
    case MYSQL_TYPE_TIMESTAMP:
      return common::CT::TIMESTAMP;
    case MYSQL_TYPE_DATETIME:
      return common::CT::DATETIME;
    case MYSQL_TYPE_TIME:
      return common::CT::TIME;
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_DATE:
      return common::CT::DATE;
    case MYSQL_TYPE_NEWDECIMAL:
      return common::CT::NUM;
    case MYSQL_TYPE_STRING:
      return common::CT::STRING;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_BLOB:
      return common::CT::VARCHAR;
    default:
      return common::CT::UNK;
  }
}

common::CT Engine::GetCorrespondingType(const Field &field) {
  common::CT res = GetCorrespondingType(field.type());
  if (!ATI::IsStringType(res))
    return res;
  else {
    switch (field.type()) {
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING: {
        if (const Field_str *fstr = dynamic_cast<const Field_string *>(&field)) {
          if (fstr->charset() != &my_charset_bin) return common::CT::STRING;
          return common::CT::BYTE;
        } else if (const Field_str *fvstr = dynamic_cast<const Field_varstring *>(&field)) {
          if (fvstr->charset() != &my_charset_bin) return common::CT::VARCHAR;
          return common::CT::VARBYTE;
        }
      } break;
      case MYSQL_TYPE_BLOB:
        if (const Field_str *fstr = dynamic_cast<const Field_str *>(&field)) {
          if (const Field_blob *fblo = dynamic_cast<const Field_blob *>(fstr)) {
            if (fblo->charset() != &my_charset_bin) {
              // TINYTEXT, MEDIUMTEXT, TEXT, LONGTEXT
              if (fblo->field_length > 65535) return common::CT::LONGTEXT;
              return common::CT::VARCHAR;
            } else {
              switch (field.field_length) {
                case 255:
                case 65535:
                  // TINYBLOB, BLOB
                  return common::CT::VARBYTE;
                case 16777215:
                case (size_t)4294967295UL:
                  // MEDIUMBLOB, LONGBLOB
                  return common::CT::BIN;
              }
            }
          }
        }
        break;
      default:
        return common::CT::UNK;
    }
  }
  return common::CT::UNK;
}

AttributeTypeInfo Engine::GetCorrespondingATI(Field &field) {
  common::CT at = GetCorrespondingType(field);

  if (ATI::IsNumericType(at)) {
    DEBUG_ASSERT(dynamic_cast<Field_num *>(&field));
    if (at == common::CT::NUM) {
      DEBUG_ASSERT(dynamic_cast<Field_new_decimal *>(&field));
      return AttributeTypeInfo(at, !field.maybe_null(), static_cast<Field_new_decimal &>(field).precision,
                               static_cast<Field_num &>(field).decimals());
    }
    return AttributeTypeInfo(at, !field.maybe_null(), field.field_length, static_cast<Field_num &>(field).decimals());
  }
  return AttributeTypeInfo(at, !field.maybe_null(), field.field_length);
}
}  // namespace core
}  // namespace stonedb
