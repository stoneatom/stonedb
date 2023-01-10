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
#include "common/common_definitions.h"
#include "core/filter.h"
#include "core/tianmu_attr_typeinfo.h"
#include "types/text_stat.h"
#include "types/tianmu_num.h"
#include "util/hash64.h"

#include "value_set.h"

namespace Tianmu {
namespace core {
ValueSet::ValueSet(uint32_t power) {
  prepared = false;
  contains_nulls = false;
  prep_type = common::ColumnType::UNK;
  prep_scale = 0;
  prep_collation = DTCollation();
  min = max = 0;
  no_obj = 0;
  easy_min = 0;
  easy_max = 0;
  use_easy_table = false;
  pack_power = power;
}

ValueSet::ValueSet(ValueSet &sec)
    : prepared(sec.prepared), prep_type(sec.prep_type), prep_scale(sec.prep_scale), prep_collation(sec.prep_collation) {
  pack_power = sec.pack_power;
  no_obj = sec.NoVals();
  contains_nulls = sec.contains_nulls;
  min = max = 0;
  for (const auto &it : sec.values) values.insert(it->Clone().release());

  if (sec.min)
    min = sec.min->Clone().release();

  if (sec.max)
    max = sec.max->Clone().release();

  easy_min = sec.easy_min;
  easy_max = sec.easy_max;
  use_easy_table = sec.use_easy_table;
  if (use_easy_table)
    for (int i = 0; i < no_obj; i++) easy_table[i] = sec.easy_table[i];

  if (sec.easy_vals)
    easy_vals.reset(new Filter(*sec.easy_vals));

  if (sec.easy_hash)
    easy_hash.reset(new utils::Hash64(*sec.easy_hash));

  if (sec.easy_text)
    easy_text.reset(new types::TextStat(*sec.easy_text));
}

ValueSet::~ValueSet() {
  MEASURE_FET("ValueSet::~ValueSet(...)");
  Clear();
}

void ValueSet::Clear() {
  for (auto const &it : values) delete it;
  values.clear();

  delete min;
  min = 0;
  delete max;
  max = 0;
  prepared = false;
  prep_type = common::ColumnType::UNK;
  prep_scale = 0;
  prep_collation = DTCollation();
  easy_vals.reset();
  easy_hash.reset();
  easy_text.reset();
  use_easy_table = false;
}

void ValueSet::Add64(int64_t v)  // only for integers
{
  if (v == common::NULL_VALUE_64) {
    contains_nulls = true;
    return;
  }
  types::TianmuDataType *rcv = new types::TianmuNum(v);
  if (prep_type != common::ColumnType::NUM || prep_scale != 0)
    prepared = false;

  if (!values.insert(rcv).second)
    delete rcv;
  else if (prepared) {
    if (types::RequiresUTFConversions(prep_collation)) {
      if (min->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), min->ToBString()) < 0)
        *min = *rcv;
      if (max->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), max->ToBString()) > 0)
        *max = *rcv;
    } else {
      if (min->IsNull() || *rcv < *min)
        *min = *rcv;
      if (max->IsNull() || *rcv > *max)
        *max = *rcv;
    }
  }

  no_obj = (int)values.size();
}

void ValueSet::Add(std::unique_ptr<types::TianmuDataType> tianmu_dt) {
  if (tianmu_dt->IsNull()) {
    contains_nulls = true;
    return;
  }
  types::TianmuDataType *rcv = tianmu_dt.release();
  if (prep_type != rcv->Type() ||
      (rcv->Type() == common::ColumnType::NUM && static_cast<types::TianmuNum &>(*rcv).Scale() != prep_scale))
    prepared = false;

  if (!values.insert(rcv).second)
    delete rcv;
  else if (prepared) {
    if (types::RequiresUTFConversions(prep_collation)) {
      if (min->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), min->ToBString()) < 0)
        *min = *rcv;
      if (max->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), max->ToBString()) > 0)
        *max = *rcv;
    } else {
      if (min->IsNull() || *rcv < *min)
        *min = *rcv;
      if (max->IsNull() || *rcv > *max)
        *max = *rcv;
    }
  }

  no_obj = (int)values.size();
}

void ValueSet::Add(const types::TianmuValueObject &rcv) {
  if (rcv.IsNull())
    contains_nulls = true;
  else {
    std::unique_ptr<types::TianmuDataType> tianmu_dt = rcv.Get()->Clone();
    if (tianmu_dt->IsNull()) {
      contains_nulls = true;
      return;
    }
    types::TianmuDataType *rcv = tianmu_dt.release();
    if (prep_type != rcv->Type() ||
        (rcv->Type() == common::ColumnType::NUM && static_cast<types::TianmuNum &>(*rcv).Scale() != prep_scale))
      prepared = false;

    if (!values.insert(rcv).second)
      delete rcv;
    else if (prepared) {
      if (types::RequiresUTFConversions(prep_collation)) {
        if (min->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), min->ToBString()) < 0)
          *min = *rcv;
        if (max->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), max->ToBString()) > 0)
          *max = *rcv;
      } else {
        if (min->IsNull() || *rcv < *min)
          *min = *rcv;
        if (max->IsNull() || *rcv > *max)
          *max = *rcv;
      }
    }

    no_obj = (int)values.size();
  }
}
bool ValueSet::isContains(const types::TianmuDataType &v, DTCollation coll) {
  if (v.IsNull())
    return false;
  if (types::RequiresUTFConversions(coll)) {
    for (auto const &it : values)
      if (CollationStrCmp(coll, it->ToBString(), v.ToBString()) == 0)
        return true;
    return false;
  } else
    return (values.size() > 0 && values.find(const_cast<types::TianmuDataType *>(&v)) != values.end());
}

bool ValueSet::Contains(int64_t v) {
  DEBUG_ASSERT(prepared);
  DEBUG_ASSERT(v != common::NULL_VALUE_64);

  if (use_easy_table) {
    for (int i = 0; i < no_obj; i++)
      if (v == easy_table[i])
        return true;
    return false;
  }
  if (easy_vals) {
    if (v < easy_min || v > easy_max)
      return false;
    return easy_vals->Get(v - easy_min);
  }
  if (easy_hash)
    return easy_hash->Find(v);
  return isContains(types::TianmuNum(v, prep_scale), prep_collation);
}

bool ValueSet::Contains(types::BString &v) {
  DEBUG_ASSERT(prepared);  // it implies a trivial collation

  if (v.IsNull())
    return false;
  if (easy_hash && easy_text) {
    int64_t vcode = easy_text->Encode(v);
    if (vcode == common::NULL_VALUE_64)
      return false;
    return easy_hash->Find(vcode);
  }
  return (values.size() > 0 && values.find(&v /*const_cast<TianmuDataType*>(&v)*/) != values.end());
}

bool ValueSet::Contains(const types::TianmuDataType &v, DTCollation coll) {
  if (v.IsNull())
    return false;
  if (types::RequiresUTFConversions(coll)) {
    for (auto const &it : values)
      if (CollationStrCmp(coll, it->ToBString(), v.ToBString()) == 0)
        return true;
    return false;
  } else
    return (values.size() > 0 && values.find(const_cast<types::TianmuDataType *>(&v)) != values.end());
}

bool ValueSet::Contains(const types::TianmuValueObject &v, DTCollation coll) {
  if (v.IsNull())
    return false;
  return isContains(*v.Get(), coll);
}

inline bool ValueSet::IsPrepared(common::ColumnType at, int scale, DTCollation coll) {
  return (prepared && (prep_type == at || (ATI::IsStringType(prep_type) && ATI::IsStringType(at)))  // CHAR = VARCHAR
          && prep_scale == scale &&
          (!ATI::IsStringType(at) || prep_collation.collation == coll.collation ||
           (!types::RequiresUTFConversions(prep_collation) && !types::RequiresUTFConversions(coll))));
}

void ValueSet::Prepare(common::ColumnType at, int scale, DTCollation coll) {
  if (!IsPrepared(at, scale, coll)) {
    decltype(values) new_values;
    if (ATI::IsStringType(at)) {
      delete min;
      min = new types::BString();
      delete max;
      max = new types::BString();
      bool min_set = false, max_set = false;
      if (types::RequiresUTFConversions(coll)) {
        for (const auto &it : values) {
          types::BString *v = new types::BString(it->ToBString());
          if (!min_set || CollationStrCmp(coll, v->ToBString(), min->ToBString()) < 0) {
            *min = *v;
            min_set = true;
          }
          if (!max_set || CollationStrCmp(coll, v->ToBString(), max->ToBString()) > 0) {
            *max = *v;
            max_set = true;
          }
          new_values.insert(v);
        }
      } else {
        for (const auto &it : values) {
          types::BString *v = new types::BString(it->ToBString());
          if (!min_set || *v < *min) {
            *min = *v;
            min_set = true;
          }
          if (!max_set || *v > *max) {
            *max = *v;
            max_set = true;
          }
          new_values.insert(v);
        }
      }
      std::swap(values, new_values);
      for (const auto &it : new_values) delete it;
    } else if (ATI::IsIntegerType(at) || ATI::IsBitType(at)) {
      delete min;
      min = new types::TianmuNum();
      delete max;
      max = new types::TianmuNum();
      for (const auto &it : values) {
        if (types::TianmuNum *tianmu_n = dynamic_cast<types::TianmuNum *>(it)) {
          if (tianmu_n->IsInt() || tianmu_n->IsReal()) {
            types::TianmuNum *tianmu_num_new = new types::TianmuNum();
            if (tianmu_n->IsInt())
              types::TianmuDataType::ToInt(*tianmu_n, *tianmu_num_new);
            else
              types::TianmuDataType::ToReal(*tianmu_n, *tianmu_num_new);

            if (!(*tianmu_num_new > *min))
              *min = *tianmu_num_new;
            if (!(*tianmu_num_new < *max))
              *max = *tianmu_num_new;
            if (!new_values.insert(tianmu_num_new).second)
              delete tianmu_num_new;
          }
        } else if (it->Type() == common::ColumnType::STRING) {
          types::TianmuNum *tianmu_n = new types::TianmuNum();
          if (types::TianmuNum::Parse(it->ToBString(), *tianmu_n, at) == common::ErrorCode::SUCCESS) {
            if (!(*tianmu_n > *min))
              *min = *tianmu_n;
            if (!(*tianmu_n < *max))
              *max = *tianmu_n;
            if (!new_values.insert(tianmu_n).second)
              delete tianmu_n;
          } else {
            delete tianmu_n;
            tianmu_n = new types::TianmuNum(0, scale, false, at);
            if (!new_values.insert(tianmu_n).second)
              delete tianmu_n;
          }
        }
      }
      std::swap(values, new_values);
      for (const auto &it : new_values) delete it;
    } else if (at == common::ColumnType::NUM) {
      delete min;
      min = new types::TianmuNum();
      delete max;
      max = new types::TianmuNum();
      for (const auto &it : values) {
        if (types::TianmuNum *tianmu_n = dynamic_cast<types::TianmuNum *>(it)) {
          if (tianmu_n->IsDecimal(scale)) {
            types::TianmuNum *tianmu_num_new = new types::TianmuNum();
            types::TianmuDataType::ToDecimal(*tianmu_n, scale, *tianmu_num_new);
            if (!(*tianmu_num_new > *min))
              *min = *tianmu_num_new;
            if (!(*tianmu_num_new < *max))
              *max = *tianmu_num_new;
            if (!new_values.insert(tianmu_num_new).second)
              delete tianmu_num_new;
          }
        } else if (it->Type() == common::ColumnType::STRING) {
          types::TianmuNum *tianmu_n = new types::TianmuNum();
          if (types::TianmuNum::Parse(it->ToBString(), *tianmu_n, common::ColumnType::NUM) ==
                  common::ErrorCode::SUCCESS &&
              tianmu_n->IsDecimal(scale)) {
            if (!(*tianmu_n > *min))
              *min = *tianmu_n;
            if (!(*tianmu_n < *max))
              *max = *tianmu_n;
            if (!new_values.insert(tianmu_n).second)
              delete tianmu_n;
          } else {
            delete tianmu_n;
            tianmu_n = new types::TianmuNum(0, scale, false, at);
            if (!new_values.insert(tianmu_n).second)
              delete tianmu_n;
          }
        }
      }
      std::swap(values, new_values);
      for (const auto &it : new_values) delete it;
    } else if (ATI::IsRealType(at)) {
      delete min;
      min = new types::TianmuNum(at);
      delete max;
      max = new types::TianmuNum(at);

      for (const auto &it : values) {
        if (types::TianmuNum *tianmu_n = dynamic_cast<types::TianmuNum *>(it)) {
          if (ATI::IsRealType(tianmu_n->Type())) {
            if (!(*tianmu_n > *min))
              *min = (*tianmu_n);
            if (!(*tianmu_n < *max))
              *max = (*tianmu_n);
            if (!new_values.insert(tianmu_n).second)
              delete it;
          } else {
            types::TianmuNum *tianmu_num_new = new types::TianmuNum(tianmu_n->ToReal());
            if (!(*tianmu_num_new > *min))
              *min = (*tianmu_num_new);
            if (!(*tianmu_n < *max))
              *max = (*tianmu_num_new);
            if (!new_values.insert(tianmu_num_new).second)
              delete tianmu_num_new;
            delete it;
          }
        } else if (it->Type() == common::ColumnType::STRING) {
          types::TianmuNum *tianmu_n = new types::TianmuNum();
          if (types::TianmuNum::ParseReal(*(types::BString *)it, *tianmu_n, at) == common::ErrorCode::SUCCESS) {
            if (!(*tianmu_n > *min))
              *min = *tianmu_n;
            if (!(*tianmu_n < *max))
              *max = *tianmu_n;
            if (!new_values.insert(tianmu_n).second)
              delete tianmu_n;
          } else {
            delete tianmu_n;
            tianmu_n = new types::TianmuNum(0, scale, true, at);
            if (!new_values.insert(tianmu_n).second)
              delete tianmu_n;
          }
          delete it;
        } else {
          delete it;
        }
      }
      std::swap(values, new_values);
    } else if (ATI::IsDateTimeType(at)) {
      delete min;
      min = new types::TianmuDateTime();
      delete max;
      max = new types::TianmuDateTime();

      for (const auto &it : values) {
        if (types::TianmuDateTime *tianmu_dt = dynamic_cast<types::TianmuDateTime *>(it)) {
          if (!(*tianmu_dt > *min))
            *min = *tianmu_dt;
          if (!(*tianmu_dt < *max))
            *max = *tianmu_dt;
          if (!new_values.insert(it).second)
            delete it;
        } else if (it->Type() == common::ColumnType::STRING) {
          types::TianmuDateTime *tianmu_dt = new types::TianmuDateTime();
          if (!common::IsError(types::TianmuDateTime::Parse(it->ToBString(), *tianmu_dt, at))) {
            if (!(*tianmu_dt > *min))
              *min = *tianmu_dt;
            if (!(*tianmu_dt < *max))
              *max = *tianmu_dt;
            if (!new_values.insert(tianmu_dt).second)
              delete tianmu_dt;
          } else {
            delete tianmu_dt;
            tianmu_dt =
                static_cast<types::TianmuDateTime *>(types::TianmuDateTime::GetSpecialValue(at).Clone().release());
            if (!new_values.insert(tianmu_dt).second)
              delete tianmu_dt;
          }
          delete it;
        } else if (types::TianmuNum *tianmu_n = dynamic_cast<types::TianmuNum *>(it)) {
          try {
            types::TianmuDateTime *tianmu_dt = new types::TianmuDateTime(*tianmu_n, at);
            if (!(*tianmu_dt > *min))
              *min = *tianmu_dt;
            if (!(*tianmu_dt < *max))
              *max = *tianmu_dt;
            if (!new_values.insert(tianmu_dt).second)
              delete tianmu_dt;
          } catch (common::DataTypeConversionException &) {
            delete tianmu_dt;
            tianmu_dt =
                static_cast<types::TianmuDateTime *>(types::TianmuDateTime::GetSpecialValue(at).Clone().release());
            if (!new_values.insert(tianmu_n).second)
              delete tianmu_dt;
          }
          delete it;
        } else {
          delete it;
        }
      }
      std::swap(values, new_values);
    }

    no_obj = (int)values.size();
    prepared = true;
    prep_type = at;
    prep_scale = scale;
    prep_collation = coll;
    if (no_obj > 0) {
      if (no_obj <= VS_EASY_TABLE_SIZE && !ATI::IsStringType(prep_type)) {
        int i = 0;
        for (const auto &it : values) {
          types::TianmuNum *tianmu_n = static_cast<types::TianmuNum *>(it);
          easy_table[i] = tianmu_n->ValueInt();
          i++;
        }
        use_easy_table = true;
      }
      if (!use_easy_table && ATI::IsFixedNumericType(prep_type)) {
        easy_min = ((types::TianmuNum *)min)->ValueInt();
        easy_max = ((types::TianmuNum *)max)->ValueInt();
        if (easy_max < common::PLUS_INF_64 / 2 && easy_min > common::MINUS_INF_64 / 2 &&
            size_t(easy_max - easy_min) < 8 * 64_MB        // upper limit: 64 MB for a filter
            && (easy_max - easy_min) / 8 < no_obj * 16) {  // otherwise easy_hash will be smaller
          easy_vals.reset(new Filter(easy_max - easy_min + 1, pack_power));
          for (const auto &it : values) {
            types::TianmuNum *tianmu_n = static_cast<types::TianmuNum *>(it);
            easy_vals->Set(tianmu_n->ValueInt() - easy_min);
          }
        }
      }
      if (!use_easy_table && !easy_vals && !ATI::IsStringType(prep_type)) {
        easy_hash.reset(new utils::Hash64(no_obj));
        for (const auto &it : values) {
          types::TianmuNum *tianmu_n = static_cast<types::TianmuNum *>(it);
          easy_hash->Insert(tianmu_n->ValueInt());
        }
      }
      if (ATI::IsStringType(prep_type)) {  //&& !RequiresUTFConversions(coll)
        easy_text.reset(new types::TextStat);
        bool still_valid = true;
        auto it = values.begin();
        auto it_end = values.end();
        while (it != it_end && still_valid) {
          types::BString *v = static_cast<types::BString *>(*it);
          still_valid = easy_text->AddString(*v);
          it++;
        }
        if (still_valid)
          still_valid = easy_text->CreateEncoding();

        if (still_valid) {
          easy_hash.reset(new utils::Hash64(no_obj));
          for (const auto &it : values) {
            types::BString *v = static_cast<types::BString *>(it);
            int64_t vcode = easy_text->Encode(*v);
            DEBUG_ASSERT(vcode != common::NULL_VALUE_64);  // should not occur, as we put this
                                                           // value in the previous loop
            easy_hash->Insert(vcode);
          }
        } else {
          easy_text.reset();
        }
      }
    }
  }
}

bool ValueSet::CopyCondition(types::CondArray &condition, [[maybe_unused]] DTCollation coll) {
  bool ret = false;
  for (const auto &it : values) {
    types::BString *v = static_cast<types::BString *>(it);
    condition.push_back(*v);  // RCDataTypePtr(new types::BString(*v))
    ret = true;
  }
  return ret;
}

bool ValueSet::CopyCondition([[maybe_unused]] common::ColumnType at, std::shared_ptr<utils::Hash64> &condition,
                             [[maybe_unused]] DTCollation coll) {
  bool ret = false;
  if (no_obj > 0) {
    std::shared_ptr<utils::Hash64> hash_ptr(new utils::Hash64(values.size()));
    for (const auto &it : values) {
      types::TianmuNum *tianmu_n = static_cast<types::TianmuNum *>(it);
      // condition.push_back(tianmu_n->ValueInt());
      // condition.insert
      // (std::make_pair<int64_t,int64_t>(tianmu_n->ValueInt(),tianmu_n->Value()));
      hash_ptr->Insert(tianmu_n->ValueInt());
      ret = true;
    }
    condition = hash_ptr;
    /*if ((no_obj <= VS_EASY_TABLE_SIZE && !ATI::IsStringType(at)) ||
                    (!use_easy_table && !easy_vals &&
    !ATI::IsStringType(prep_type))||
                    (!use_easy_table && ATI::IsFixedNumericType(prep_type))) {

    }*/
  }

  return ret;
}
}  // namespace core
}  // namespace Tianmu
