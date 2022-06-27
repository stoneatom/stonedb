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
#include "core/rc_attr_typeinfo.h"
#include "types/rc_num.h"
#include "types/text_stat.h"
#include "util/hash64.h"

#include "value_set.h"

namespace stonedb {
namespace core {
ValueSet::ValueSet(uint32_t power) {
  prepared = false;
  contains_nulls = false;
  prep_type = common::CT::UNK;
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

  if (sec.min) min = sec.min->Clone().release();

  if (sec.max) max = sec.max->Clone().release();

  easy_min = sec.easy_min;
  easy_max = sec.easy_max;
  use_easy_table = sec.use_easy_table;
  if (use_easy_table)
    for (int i = 0; i < no_obj; i++) easy_table[i] = sec.easy_table[i];

  if (sec.easy_vals) easy_vals.reset(new Filter(*sec.easy_vals));

  if (sec.easy_hash) easy_hash.reset(new utils::Hash64(*sec.easy_hash));

  if (sec.easy_text) easy_text.reset(new types::TextStat(*sec.easy_text));
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
  prep_type = common::CT::UNK;
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
  types::RCDataType *rcv = new types::RCNum(v);
  if (prep_type != common::CT::NUM || prep_scale != 0) prepared = false;

  if (!values.insert(rcv).second)
    delete rcv;
  else if (prepared) {
    if (types::RequiresUTFConversions(prep_collation)) {
      if (min->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), min->ToBString()) < 0) *min = *rcv;
      if (max->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), max->ToBString()) > 0) *max = *rcv;
    } else {
      if (min->IsNull() || *rcv < *min) *min = *rcv;
      if (max->IsNull() || *rcv > *max) *max = *rcv;
    }
  }

  no_obj = (int)values.size();
}

void ValueSet::Add(std::unique_ptr<types::RCDataType> rcdt) {
  if (rcdt->IsNull()) {
    contains_nulls = true;
    return;
  }
  types::RCDataType *rcv = rcdt.release();
  if (prep_type != rcv->Type() ||
      (rcv->Type() == common::CT::NUM && static_cast<types::RCNum &>(*rcv).Scale() != prep_scale))
    prepared = false;

  if (!values.insert(rcv).second)
    delete rcv;
  else if (prepared) {
    if (types::RequiresUTFConversions(prep_collation)) {
      if (min->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), min->ToBString()) < 0) *min = *rcv;
      if (max->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), max->ToBString()) > 0) *max = *rcv;
    } else {
      if (min->IsNull() || *rcv < *min) *min = *rcv;
      if (max->IsNull() || *rcv > *max) *max = *rcv;
    }
  }

  no_obj = (int)values.size();
}

void ValueSet::Add(const types::RCValueObject &rcv) {
  if (rcv.IsNull())
    contains_nulls = true;
  else {
    // Add(rcv.Get()->Clone());
    std::unique_ptr<types::RCDataType> rcdt = rcv.Get()->Clone();
    if (rcdt->IsNull()) {
      contains_nulls = true;
      return;
    }
    types::RCDataType *rcv = rcdt.release();
    if (prep_type != rcv->Type() ||
        (rcv->Type() == common::CT::NUM && static_cast<types::RCNum &>(*rcv).Scale() != prep_scale))
      prepared = false;

    if (!values.insert(rcv).second)
      delete rcv;
    else if (prepared) {
      if (types::RequiresUTFConversions(prep_collation)) {
        if (min->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), min->ToBString()) < 0) *min = *rcv;
        if (max->IsNull() || CollationStrCmp(prep_collation, rcv->ToBString(), max->ToBString()) > 0) *max = *rcv;
      } else {
        if (min->IsNull() || *rcv < *min) *min = *rcv;
        if (max->IsNull() || *rcv > *max) *max = *rcv;
      }
    }

    no_obj = (int)values.size();
  }
}
bool ValueSet::isContains(const types::RCDataType &v, DTCollation coll) {
  if (v.IsNull()) return false;
  if (types::RequiresUTFConversions(coll)) {
    for (auto const &it : values)
      if (CollationStrCmp(coll, it->ToBString(), v.ToBString()) == 0) return true;
    return false;
  } else
    return (values.size() > 0 && values.find(const_cast<types::RCDataType *>(&v)) != values.end());
}

bool ValueSet::Contains(int64_t v) {
  DEBUG_ASSERT(prepared);
  DEBUG_ASSERT(v != common::NULL_VALUE_64);

  if (use_easy_table) {
    for (int i = 0; i < no_obj; i++)
      if (v == easy_table[i]) return true;
    return false;
  }
  if (easy_vals) {
    if (v < easy_min || v > easy_max) return false;
    return easy_vals->Get(v - easy_min);
  }
  if (easy_hash) return easy_hash->Find(v);
  return isContains(types::RCNum(v, prep_scale), prep_collation);
}

bool ValueSet::Contains(types::BString &v) {
  DEBUG_ASSERT(prepared);  // it implies a trivial collation

  if (v.IsNull()) return false;
  if (easy_hash && easy_text) {
    int64_t vcode = easy_text->Encode(v);
    if (vcode == common::NULL_VALUE_64) return false;
    return easy_hash->Find(vcode);
  }
  return (values.size() > 0 && values.find(&v /*const_cast<RCDataType*>(&v)*/) != values.end());
}

bool ValueSet::Contains(const types::RCDataType &v, DTCollation coll) {
  if (v.IsNull()) return false;
  if (types::RequiresUTFConversions(coll)) {
    for (auto const &it : values)
      if (CollationStrCmp(coll, it->ToBString(), v.ToBString()) == 0) return true;
    return false;
  } else
    return (values.size() > 0 && values.find(const_cast<types::RCDataType *>(&v)) != values.end());
}

bool ValueSet::Contains(const types::RCValueObject &v, DTCollation coll) {
  if (v.IsNull()) return false;
  return isContains(*v.Get(), coll);
}

inline bool ValueSet::IsPrepared(common::CT at, int scale, DTCollation coll) {
  return (prepared && (prep_type == at || (ATI::IsStringType(prep_type) && ATI::IsStringType(at)))  // CHAR = VARCHAR
          && prep_scale == scale &&
          (!ATI::IsStringType(at) || prep_collation.collation == coll.collation ||
           (!types::RequiresUTFConversions(prep_collation) && !types::RequiresUTFConversions(coll))));
}

void ValueSet::Prepare(common::CT at, int scale, DTCollation coll) {
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
    } else if (ATI::IsIntegerType(at)) {
      delete min;
      min = new types::RCNum();
      delete max;
      max = new types::RCNum();
      for (const auto &it : values) {
        if (types::RCNum *rcn = dynamic_cast<types::RCNum *>(it)) {
          if (rcn->IsInt() || rcn->IsReal()) {
            types::RCNum *rcn_new = new types::RCNum();
            if (rcn->IsInt())
              types::RCDataType::ToInt(*rcn, *rcn_new);
            else
              types::RCDataType::ToReal(*rcn, *rcn_new);

            if (!(*rcn_new > *min)) *min = *rcn_new;
            if (!(*rcn_new < *max)) *max = *rcn_new;
            if (!new_values.insert(rcn_new).second) delete rcn_new;
          }
        } else if (it->Type() == common::CT::STRING) {
          types::RCNum *rcn = new types::RCNum();
          if (types::RCNum::Parse(it->ToBString(), *rcn, at) == common::ErrorCode::SUCCESS) {
            if (!(*rcn > *min)) *min = *rcn;
            if (!(*rcn < *max)) *max = *rcn;
            if (!new_values.insert(rcn).second) delete rcn;
          } else {
            delete rcn;
            rcn = new types::RCNum(0, scale, false, at);
            if (!new_values.insert(rcn).second) delete rcn;
          }
        }
      }
      std::swap(values, new_values);
      for (const auto &it : new_values) delete it;
    } else if (at == common::CT::NUM) {
      delete min;
      min = new types::RCNum();
      delete max;
      max = new types::RCNum();
      for (const auto &it : values) {
        if (types::RCNum *rcn = dynamic_cast<types::RCNum *>(it)) {
          if (rcn->IsDecimal(scale)) {
            types::RCNum *rcn_new = new types::RCNum();
            types::RCDataType::ToDecimal(*rcn, scale, *rcn_new);
            if (!(*rcn_new > *min)) *min = *rcn_new;
            if (!(*rcn_new < *max)) *max = *rcn_new;
            if (!new_values.insert(rcn_new).second) delete rcn_new;
          }
        } else if (it->Type() == common::CT::STRING) {
          types::RCNum *rcn = new types::RCNum();
          if (types::RCNum::Parse(it->ToBString(), *rcn, common::CT::NUM) == common::ErrorCode::SUCCESS &&
              rcn->IsDecimal(scale)) {
            if (!(*rcn > *min)) *min = *rcn;
            if (!(*rcn < *max)) *max = *rcn;
            if (!new_values.insert(rcn).second) delete rcn;
          } else {
            delete rcn;
            rcn = new types::RCNum(0, scale, false, at);
            if (!new_values.insert(rcn).second) delete rcn;
          }
        }
      }
      std::swap(values, new_values);
      for (const auto &it : new_values) delete it;
    } else if (ATI::IsRealType(at)) {
      delete min;
      min = new types::RCNum(at);
      delete max;
      max = new types::RCNum(at);

      for (const auto &it : values) {
        if (types::RCNum *rcn = dynamic_cast<types::RCNum *>(it)) {
          if (ATI::IsRealType(rcn->Type())) {
            if (!(*rcn > *min)) *min = (*rcn);
            if (!(*rcn < *max)) *max = (*rcn);
            if (!new_values.insert(rcn).second) delete it;
          } else {
            types::RCNum *rcn_new = new types::RCNum(rcn->ToReal());
            if (!(*rcn_new > *min)) *min = (*rcn_new);
            if (!(*rcn < *max)) *max = (*rcn_new);
            if (!new_values.insert(rcn_new).second) delete rcn_new;
            delete it;
          }
        } else if (it->Type() == common::CT::STRING) {
          types::RCNum *rcn = new types::RCNum();
          if (types::RCNum::ParseReal(*(types::BString *)it, *rcn, at) == common::ErrorCode::SUCCESS) {
            if (!(*rcn > *min)) *min = *rcn;
            if (!(*rcn < *max)) *max = *rcn;
            if (!new_values.insert(rcn).second) delete rcn;
          } else {
            delete rcn;
            rcn = new types::RCNum(0, scale, true, at);
            if (!new_values.insert(rcn).second) delete rcn;
          }
          delete it;
        } else {
          delete it;
        }
      }
      std::swap(values, new_values);
    } else if (ATI::IsDateTimeType(at)) {
      delete min;
      min = new types::RCDateTime();
      delete max;
      max = new types::RCDateTime();

      for (const auto &it : values) {
        if (types::RCDateTime *rcdt = dynamic_cast<types::RCDateTime *>(it)) {
          if (!(*rcdt > *min)) *min = *rcdt;
          if (!(*rcdt < *max)) *max = *rcdt;
          if (!new_values.insert(it).second) delete it;
        } else if (it->Type() == common::CT::STRING) {
          types::RCDateTime *rcdt = new types::RCDateTime();
          if (!common::IsError(types::RCDateTime::Parse(it->ToBString(), *rcdt, at))) {
            if (!(*rcdt > *min)) *min = *rcdt;
            if (!(*rcdt < *max)) *max = *rcdt;
            if (!new_values.insert(rcdt).second) delete rcdt;
          } else {
            delete rcdt;
            rcdt = static_cast<types::RCDateTime *>(types::RCDateTime::GetSpecialValue(at).Clone().release());
            if (!new_values.insert(rcdt).second) delete rcdt;
          }
          delete it;
        } else if (types::RCNum *rcn = dynamic_cast<types::RCNum *>(it)) {
          try {
            types::RCDateTime *rcdt = new types::RCDateTime(*rcn, at);
            if (!(*rcdt > *min)) *min = *rcdt;
            if (!(*rcdt < *max)) *max = *rcdt;
            if (!new_values.insert(rcdt).second) delete rcdt;
          } catch (common::DataTypeConversionException &) {
            delete rcdt;
            rcdt = static_cast<types::RCDateTime *>(types::RCDateTime::GetSpecialValue(at).Clone().release());
            if (!new_values.insert(rcn).second) delete rcdt;
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
          types::RCNum *rcn = static_cast<types::RCNum *>(it);
          easy_table[i] = rcn->ValueInt();
          i++;
        }
        use_easy_table = true;
      }
      if (!use_easy_table && ATI::IsFixedNumericType(prep_type)) {
        easy_min = ((types::RCNum *)min)->ValueInt();
        easy_max = ((types::RCNum *)max)->ValueInt();
        if (easy_max < common::PLUS_INF_64 / 2 && easy_min > common::MINUS_INF_64 / 2 &&
            size_t(easy_max - easy_min) < 8 * 64_MB        // upper limit: 64 MB for a filter
            && (easy_max - easy_min) / 8 < no_obj * 16) {  // otherwise easy_hash will be smaller
          easy_vals.reset(new Filter(easy_max - easy_min + 1, pack_power));
          for (const auto &it : values) {
            types::RCNum *rcn = static_cast<types::RCNum *>(it);
            easy_vals->Set(rcn->ValueInt() - easy_min);
          }
        }
      }
      if (!use_easy_table && !easy_vals && !ATI::IsStringType(prep_type)) {
        easy_hash.reset(new utils::Hash64(no_obj));
        for (const auto &it : values) {
          types::RCNum *rcn = static_cast<types::RCNum *>(it);
          easy_hash->Insert(rcn->ValueInt());
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
        if (still_valid) still_valid = easy_text->CreateEncoding();

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

bool ValueSet::CopyCondition([[maybe_unused]] common::CT at, std::shared_ptr<utils::Hash64> &condition,
                             [[maybe_unused]] DTCollation coll) {
  bool ret = false;
  if (no_obj > 0) {
    std::shared_ptr<utils::Hash64> hash_ptr(new utils::Hash64(values.size()));
    for (const auto &it : values) {
      types::RCNum *rcn = static_cast<types::RCNum *>(it);
      // condition.push_back(rcn->ValueInt());
      // condition.insert
      // (std::make_pair<int64_t,int64_t>(rcn->ValueInt(),rcn->Value()));
      hash_ptr->Insert(rcn->ValueInt());
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
}  // namespace stonedb
