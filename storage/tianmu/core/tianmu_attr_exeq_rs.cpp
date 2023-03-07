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

// This is a part of TianmuAttr implementation concerned with the KNs and its usage
#include "core/cq_term.h"
#include "core/engine.h"
#include "core/pack.h"
#include "core/pack_guardian.h"
#include "core/tianmu_attr.h"
#include "core/tianmu_attr_typeinfo.h"
#include "core/value_set.h"
#include "types/text_stat.h"
#include "vc/multi_value_column.h"
#include "vc/single_column.h"

namespace Tianmu {
namespace core {

// Rough queries and indexes

uint Int64StrLen(int64_t x) {
  uint min_len = 8;
  uchar *s = (uchar *)(&x);
  while (min_len > 0 && s[min_len - 1] == '\0') min_len--;
  return min_len;
}

// NOTE: similar code is in vcolumn::VirtualColumnBase::DoRoughCheck
common::RoughSetValue TianmuAttr::RoughCheck(int pack, Descriptor &d, bool additional_nulls_possible) {
  if (d.op == common::Operator::O_FALSE)
    return common::RoughSetValue::RS_NONE;
  else if (d.op == common::Operator::O_TRUE)
    return common::RoughSetValue::RS_ALL;
  // TODO: implement RoughCheck CMAP for utf8
  if (GetPackType() == common::PackType::STR && types::RequiresUTFConversions(d.GetCollation()) &&
      d.GetCollation().collation != Type().GetCollation().collation) {
    if (d.encoded) {
      LoadPackInfo();  // just in case, although the condition was encoded and
                       // this function should be executed earlier
      auto const &dpn(get_dpn(pack));
      if (dpn.NullOnly()) {  // all objects are null
        if (d.op == common::Operator::O_IS_NULL)
          return common::RoughSetValue::RS_ALL;
        else
          return common::RoughSetValue::RS_NONE;
      }
    }
    return common::RoughSetValue::RS_SOME;
  }
  if (d.IsType_AttrValOrAttrValVal()) {
    if (!d.encoded)
      return common::RoughSetValue::RS_SOME;
    vcolumn::VirtualColumn *vc1 = d.val1.vc;
    static MIIterator const mit(nullptr, pss);
    LoadPackInfo();  // just in case, although the condition was encoded and
                     // this function should be executed earlier
    auto const &dpn(get_dpn(pack));
    if (dpn.NullOnly()) {  // all objects are null
      if (d.op == common::Operator::O_IS_NULL)
        return common::RoughSetValue::RS_ALL;
      else
        return common::RoughSetValue::RS_NONE;
    }
    if (d.op == common::Operator::O_IS_NULL || d.op == common::Operator::O_NOT_NULL) {
      if (dpn.numOfNulls == 0 && !additional_nulls_possible) {
        if (d.op == common::Operator::O_IS_NULL)
          return common::RoughSetValue::RS_NONE;
        else
          return common::RoughSetValue::RS_ALL;
      }
      return common::RoughSetValue::RS_SOME;
    } else if ((d.op == common::Operator::O_LIKE || d.op == common::Operator::O_NOT_LIKE) &&
               GetPackType() == common::PackType::STR) {
      DEBUG_ASSERT(vc1->IsConst());
      types::BString pat;
      vc1->GetValueString(pat, mit);
      common::RoughSetValue res = common::RoughSetValue::RS_SOME;
      // here: check min, max
      uint pattern_prefix = 0;        // e.g. "ab_cd_e%f"  -> 7
      uint pattern_fixed_prefix = 0;  // e.g. "ab_cd_e%f"  -> 2
      uint pack_prefix;
      if (types::RequiresUTFConversions(d.GetCollation())) {
        my_match_t mm;
        if (d.GetCollation().collation->coll->instr(d.GetCollation().collation, pat.val_, pat.len_, "%", 1, &mm, 1) ==
            2)
          pattern_prefix = pattern_fixed_prefix = mm.end;

        if (d.GetCollation().collation->coll->instr(d.GetCollation().collation, pat.val_, pat.len_, "_", 1, &mm, 1) ==
            2)
          if (mm.end < pattern_fixed_prefix)
            pattern_fixed_prefix = mm.end;

        if ((pattern_fixed_prefix > 0) &&
            types::BString(pat.val_, pattern_fixed_prefix).LessEqThanMaxUTF(dpn.max_s, Type().GetCollation()) == false)
          res = common::RoughSetValue::RS_NONE;

        if (pattern_fixed_prefix > GetActualSize(pack))
          res = common::RoughSetValue::RS_NONE;
        pack_prefix = GetPrefixLength(pack);
        if (res == common::RoughSetValue::RS_SOME && pack_prefix > 0 &&
            pattern_fixed_prefix <= pack_prefix  // special case: "xyz%" and the
                                                 // pack prefix is at least 3
            && pattern_fixed_prefix + 1 == pat.len_ && pat[pattern_fixed_prefix] == '%') {
          if (d.GetCollation().collation->coll->strnncoll(d.GetCollation().collation, (const uchar *)pat.val_,
                                                          pattern_fixed_prefix, (const uchar *)dpn.min_s,
                                                          pattern_fixed_prefix, 0) == 0)
            res = common::RoughSetValue::RS_ALL;
          else
            res = common::RoughSetValue::RS_NONE;  // prefix and pattern are different
        }

      } else {
        while (pattern_prefix < pat.len_ && pat[pattern_prefix] != '%') pattern_prefix++;
        while (pattern_fixed_prefix < pat.len_ && pat[pattern_fixed_prefix] != '%' && pat[pattern_fixed_prefix] != '_')
          pattern_fixed_prefix++;

        if ((pattern_fixed_prefix > 0) && types::BString(pat.val_, pattern_fixed_prefix).LessEqThanMax(dpn.max_s) ==
                                              false)  // val_t==nullptr means +/-infty
          res = common::RoughSetValue::RS_NONE;
        if (pattern_fixed_prefix > GetActualSize(pack))
          res = common::RoughSetValue::RS_NONE;
        pack_prefix = GetPrefixLength(pack);
        if (res == common::RoughSetValue::RS_SOME && pack_prefix > 0 &&
            pattern_fixed_prefix <= pack_prefix  // special case: "xyz%" and the
                                                 // pack prefix is at least 3
            && pattern_fixed_prefix + 1 == pat.len_ && pat[pattern_fixed_prefix] == '%') {
          if (std::memcmp(pat.val_, dpn.min_s, pattern_fixed_prefix) == 0)  // pattern is equal to the prefix
            res = common::RoughSetValue::RS_ALL;
          else
            res = common::RoughSetValue::RS_NONE;  // prefix and pattern are different
        }
      }

      if (res == common::RoughSetValue::RS_SOME && std::min(pattern_prefix, pack_prefix) < pat.len_ &&
          !types::RequiresUTFConversions(d.GetCollation())) {
        types::BString pattern_for_cmap;  // note that cmap is shifted by a common prefix!
        if (pattern_prefix > pack_prefix)
          pattern_for_cmap = types::BString(pat.val_ + pack_prefix,
                                            pat.len_ - pack_prefix);  // "xyz%abc" -> "z%abc"
        else
          pattern_for_cmap = types::BString(pat.val_ + pattern_prefix,
                                            pat.len_ - pattern_prefix);  // "xyz%abc" -> "%abc"

        if (!(pattern_for_cmap.len_ == 1 && pattern_for_cmap[0] == '%')) {  // i.e. "%" => all is matching
          if (auto sp = GetFilter_CMap())
            res = sp->IsLike(pattern_for_cmap, pack, d.like_esc);
        } else
          res = common::RoughSetValue::RS_ALL;
      }
      if (d.op == common::Operator::O_NOT_LIKE) {
        if (res == common::RoughSetValue::RS_ALL)
          res = common::RoughSetValue::RS_NONE;
        else if (res == common::RoughSetValue::RS_NONE)
          res = common::RoughSetValue::RS_ALL;
      }
      if ((dpn.numOfNulls != 0 || additional_nulls_possible) && res == common::RoughSetValue::RS_ALL)
        res = common::RoughSetValue::RS_SOME;
      return res;
    } else if ((d.op == common::Operator::O_IN || d.op == common::Operator::O_NOT_IN) &&
               GetPackType() == common::PackType::STR) {
      DEBUG_ASSERT(dynamic_cast<vcolumn::MultiValColumn *>(vc1));
      vcolumn::MultiValColumn *mvc(static_cast<vcolumn::MultiValColumn *>(vc1));
      uint pack_prefix = GetPrefixLength(pack);
      common::RoughSetValue res = common::RoughSetValue::RS_SOME;
      if ((mvc->IsConst()) && (mvc->NumOfValues(mit) > 0 && mvc->NumOfValues(mit) < 64) &&
          !types::RequiresUTFConversions(d.GetCollation())) {
        if (auto sp = GetFilter_CMap()) {
          res = common::RoughSetValue::RS_NONE;  // TODO: get rid with the iterator below

          for (vcolumn::MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit);
               (it != end) && (res == common::RoughSetValue::RS_NONE); ++it) {
            types::BString v1 = it->GetString();
            if (pack_prefix <= v1.len_) {
              if (pack_prefix == 0 || std::memcmp(v1.val_, dpn.min_s, pack_prefix) == 0) {
                size_t len = v1.len_ - pack_prefix;
                types::BString v(len <= 0 ? "" : v1.val_ + pack_prefix, (int)len);
                if (v1.len_ == pack_prefix || sp->IsValue(v, v, pack) != common::RoughSetValue::RS_NONE)
                  // suspected, if any value is possible (due to the prefix or
                  // CMAP)
                  res = common::RoughSetValue::RS_SOME;
              }
            }
          }
        }
      }

      // add bloom filter for in/not in
      if (res == common::RoughSetValue::RS_SOME && (mvc->IsConst()) &&
          (mvc->NumOfValues(mit) > 0 && mvc->NumOfValues(mit) < 64)) {
        if (auto sp = GetFilter_Bloom()) {
          res = common::RoughSetValue::RS_NONE;
          for (vcolumn::MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit);
               (it != end) && (res == common::RoughSetValue::RS_NONE); ++it) {
            types::BString v = it->GetString();
            if (sp->IsValue(v, v, pack) != common::RoughSetValue::RS_NONE)
              res = common::RoughSetValue::RS_SOME;
          }
        }
      }

      if (d.op == common::Operator::O_NOT_IN) {
        if (res == common::RoughSetValue::RS_ALL)
          res = common::RoughSetValue::RS_NONE;
        else if (res == common::RoughSetValue::RS_NONE)
          res = common::RoughSetValue::RS_ALL;
      }
      if (res == common::RoughSetValue::RS_ALL && (dpn.numOfNulls > 0 || additional_nulls_possible))
        res = common::RoughSetValue::RS_SOME;
      return res;
    } else if ((d.op == common::Operator::O_IN || d.op == common::Operator::O_NOT_IN) &&
               (GetPackType() == common::PackType::INT)) {
      if (vc1->IsConst()) {
        DEBUG_ASSERT(dynamic_cast<vcolumn::MultiValColumn *>(vc1));
        vcolumn::MultiValColumn *mvc(static_cast<vcolumn::MultiValColumn *>(vc1));
        int64_t v1, v2;
        types::TianmuNum rcn_min = mvc->GetSetMin(mit);
        types::TianmuNum rcn_max = mvc->GetSetMax(mit);
        if (rcn_min.IsNull() || rcn_max.IsNull())  // cannot determine min/max
          return common::RoughSetValue::RS_SOME;
        if (Type().Lookup()) {
          v1 = rcn_min.GetValueInt64();
          v2 = rcn_max.GetValueInt64();
        } else {
          bool v1_rounded, v2_rounded;
          v1 = EncodeValue64(&rcn_min, v1_rounded);
          v2 = EncodeValue64(&rcn_max, v2_rounded);
        }

        common::RoughSetValue res = common::RoughSetValue::RS_SOME;
        if (ATI::IsRealType(TypeName()))
          return res;  // real values not implemented yet
        if (v1 > dpn.max_i || v2 < dpn.min_i) {
          res = common::RoughSetValue::RS_NONE;  // calculate as for common::Operator::O_IN and then take
                                                 // common::Operator::O_NOT_IN into account
        } else if (dpn.min_i == dpn.max_i) {
          types::TianmuValueObject tianmu_value_obj(
              ATI::IsDateTimeType(TypeName())
                  ? types::TianmuValueObject(types::TianmuDateTime(dpn.min_i, TypeName()))
                  : types::TianmuValueObject(types::TianmuNum(dpn.min_i, Type().GetScale())));
          res = (mvc->Contains(mit, *tianmu_value_obj) != false) ? common::RoughSetValue::RS_ALL
                                                                 : common::RoughSetValue::RS_NONE;
        } else {
          if (auto sp = GetFilter_Hist())
            res = sp->IsValue(v1, v2, pack, dpn.min_i, dpn.max_i);
          if (res == common::RoughSetValue::RS_ALL)  // v1, v2 are just a boundary, not
                                                     // continuous interval
            res = common::RoughSetValue::RS_SOME;
        }
        if (res == common::RoughSetValue::RS_SOME && (mvc->NumOfValues(mit) > 0 && mvc->NumOfValues(mit) < 64)) {
          bool v_rounded = false;
          res = common::RoughSetValue::RS_NONE;
          int64_t v;  // TODO: get rid with the iterator below
          for (vcolumn::MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit);
               (it != end) && (res == common::RoughSetValue::RS_NONE); ++it) {
            if (!Type().Lookup()) {  // otherwise it will be decoded to text
              v = EncodeValue64(it->GetValue().Get(), v_rounded);
            } else
              v = it->GetInt64();
            if (!v_rounded) {
              auto sp = GetFilter_Hist();
              if ((!sp && v <= dpn.max_i && v >= dpn.min_i) ||
                  (sp && sp->IsValue(v, v, pack, dpn.min_i, dpn.max_i) != common::RoughSetValue::RS_NONE)) {
                // suspected, if any value is possible
                res = common::RoughSetValue::RS_SOME;  // note: v_rounded means that this real
                                                       // value could not match this pack
                break;
              }
            }
          }
        }
        if (d.op == common::Operator::O_NOT_IN) {
          if (res == common::RoughSetValue::RS_ALL)
            res = common::RoughSetValue::RS_NONE;
          else if (res == common::RoughSetValue::RS_NONE)
            res = common::RoughSetValue::RS_ALL;
        }
        if (res == common::RoughSetValue::RS_ALL && (dpn.numOfNulls > 0 || additional_nulls_possible))
          res = common::RoughSetValue::RS_SOME;
        return res;
      }
    } else if (GetPackType() == common::PackType::STR) {  // Note: text operations as
                                                          // common::PackType::INT calculated as
                                                          // IN or below

      DEBUG_ASSERT(d.op == common::Operator::O_BETWEEN || d.op == common::Operator::O_NOT_BETWEEN);
      vcolumn::VirtualColumn *vc2 = d.val2.vc;
      common::RoughSetValue res = common::RoughSetValue::RS_SOME;
      uint pack_prefix = GetPrefixLength(pack);
      uint val_prefix = 0;
      types::BString vmin;
      types::BString vmax;
      vc1->GetValueString(vmin, mit);
      vc2->GetValueString(vmax, mit);
      if (vmin.IsNull() && vmax.IsNull())  // comparing with null - always false
        return common::RoughSetValue::RS_NONE;
      while (vmin.val_ && vmax.val_ && val_prefix < vmin.len_ && val_prefix < vmax.len_ &&
             vmin[val_prefix] == vmax[val_prefix])
        val_prefix++;  // Common prefix for values. It is a value length in case
                       // of equality.
      // check min, max
      // TODO UTF8: check PREFIX handling
      if (val_prefix > GetActualSize(pack)) {  // value to be found is longer than texts in the pack
        res = common::RoughSetValue::RS_NONE;
      } else if ((vmax.val_ && vmax.GreaterEqThanMinUTF(dpn.min_s, Type().GetCollation()) == false) ||
                 (vmin.val_ &&
                  vmin.LessEqThanMaxUTF(dpn.max_s, Type().GetCollation()) == false))  // val_t==nullptr means +/-infty
        res = common::RoughSetValue::RS_NONE;
      else if ((vmin.val_ == nullptr || vmin.GreaterEqThanMinUTF(dpn.min_s, Type().GetCollation()) == false) &&
               (vmax.val_ == nullptr ||
                vmax.LessEqThanMaxUTF(dpn.max_s, Type().GetCollation()) == false))  // val_t==nullptr means +/-infty
        res = common::RoughSetValue::RS_ALL;
      else if (pack_prefix == GetActualSize(pack) && vmin == vmax) {  // exact case for short texts
        if (vmin.GreaterEqThanMinUTF(dpn.min_s, Type().GetCollation()) &&
            vmin.LessEqThanMaxUTF(dpn.min_s, Type().GetCollation()))
          res = common::RoughSetValue::RS_ALL;
        else
          res = common::RoughSetValue::RS_NONE;
      }

      if (res == common::RoughSetValue::RS_SOME && vmin.len_ >= pack_prefix && vmax.len_ >= pack_prefix &&
          !types::RequiresUTFConversions(d.GetCollation())) {
        vmin += pack_prefix;  // redefine - shift by a common prefix
        vmax += pack_prefix;
        if (auto sp = GetFilter_CMap()) {
          res = sp->IsValue(vmin, vmax, pack);
          if (d.sharp && res == common::RoughSetValue::RS_ALL)
            res = common::RoughSetValue::RS_SOME;  // simplified version
        }

        vmin -= pack_prefix;
        vmax -= pack_prefix;
      }
      if (res == common::RoughSetValue::RS_SOME) {
        if (auto sp = GetFilter_Bloom()) {
          res = sp->IsValue(vmin, vmax, pack);
        }
      }

      if (d.op == common::Operator::O_NOT_BETWEEN) {
        if (res == common::RoughSetValue::RS_ALL)
          res = common::RoughSetValue::RS_NONE;
        else if (res == common::RoughSetValue::RS_NONE)
          res = common::RoughSetValue::RS_ALL;
      }
      if ((dpn.numOfNulls != 0 || additional_nulls_possible) && res == common::RoughSetValue::RS_ALL) {
        res = common::RoughSetValue::RS_SOME;
      }
      return res;
    } else if (GetPackType() == common::PackType::INT) {
      DEBUG_ASSERT(d.op == common::Operator::O_BETWEEN || d.op == common::Operator::O_NOT_BETWEEN);
      // common::Operator::O_BETWEEN or common::Operator::O_NOT_BETWEEN
      int64_t v1 = d.val1.vc->GetValueInt64(mit);  // 1-level values; note that these
                                                   // values were already transformed in
                                                   // EncodeCondition
      int64_t v2 = d.val2.vc->GetValueInt64(mit);
      if (!ATI::IsRealType(TypeName())) {
        if (v1 == common::MINUS_INF_64)
          v1 = dpn.min_i;
        if (v2 == common::PLUS_INF_64)
          v2 = dpn.max_i;
      } else {
        if (v1 == *(int64_t *)&common::MINUS_INF_DBL)
          v1 = dpn.min_i;
        if (v2 == *(int64_t *)&common::PLUS_INF_DBL)
          v2 = dpn.max_i;
      }
      common::RoughSetValue res =
          RoughCheckBetween(pack, v1,
                            v2);  // calculate as for common::Operator::O_BETWEEN and then consider negation
      if (d.op == common::Operator::O_NOT_BETWEEN) {
        if (res == common::RoughSetValue::RS_ALL)
          res = common::RoughSetValue::RS_NONE;
        else if (res == common::RoughSetValue::RS_NONE) {
          res = common::RoughSetValue::RS_ALL;
        }
      }
      if (res == common::RoughSetValue::RS_ALL && (dpn.numOfNulls != 0 || additional_nulls_possible)) {
        res = common::RoughSetValue::RS_SOME;
      }
      return res;
    }
  } else {
    if (!d.encoded)
      return common::RoughSetValue::RS_SOME;
    vcolumn::SingleColumn *sc =
        (static_cast<int>(d.val1.vc->IsSingleColumn()) ? static_cast<vcolumn::SingleColumn *>(d.val1.vc) : nullptr);
    TianmuAttr *sec = nullptr;
    if (sc)
      sec = dynamic_cast<TianmuAttr *>(sc->GetPhysical());
    if (d.IsType_AttrAttr() && d.op != common::Operator::O_BETWEEN && d.op != common::Operator::O_NOT_BETWEEN && sec) {
      common::RoughSetValue res = common::RoughSetValue::RS_SOME;
      // special cases, not implemented yet:
      if ((TypeName() != sec->TypeName() &&  // Exceptions:
           !(ATI::IsDateTimeType(TypeName()) && ATI::IsDateTimeType(sec->TypeName())) &&
           !(ATI::IsIntegerType(TypeName()) && ATI::IsIntegerType(sec->TypeName()))) ||
          Type().GetScale() != sec->Type().GetScale() || Type().Lookup() || sec->Type().Lookup())
        return common::RoughSetValue::RS_SOME;
      LoadPackInfo();
      sec->LoadPackInfo();

      auto const &dpn(get_dpn(pack));
      auto const &secDpn(sec->get_dpn(pack));
      if (d.op != common::Operator::O_IN && d.op != common::Operator::O_NOT_IN) {
        if (GetPackType() == common::PackType::INT && !ATI::IsRealType(TypeName())) {
          int64_t v1min = dpn.min_i;
          int64_t v1max = dpn.max_i;
          int64_t v2min = secDpn.min_i;
          int64_t v2max = secDpn.max_i;
          if (v1min >= v2max) {  // NOTE: only common::Operator::O_MORE, common::Operator::O_LESS
                                 // and common::Operator::O_EQ are analyzed here, the rest
                                 // of operators will be taken into account
                                 // later (treated as negations)
            if (d.op == common::Operator::O_LESS ||
                d.op == common::Operator::O_MORE_EQ)  // the second case will be negated soon
              res = common::RoughSetValue::RS_NONE;
            if (v1min > v2max) {
              if (d.op == common::Operator::O_EQ || d.op == common::Operator::O_NOT_EQ)
                res = common::RoughSetValue::RS_NONE;
              if (d.op == common::Operator::O_MORE || d.op == common::Operator::O_LESS_EQ)
                res = common::RoughSetValue::RS_ALL;
            }
          }
          if (v1max <= v2min) {
            if (d.op == common::Operator::O_MORE ||
                d.op == common::Operator::O_LESS_EQ)  // the second case will be negated soon
              res = common::RoughSetValue::RS_NONE;
            if (v1max < v2min) {
              if (d.op == common::Operator::O_EQ || d.op == common::Operator::O_NOT_EQ)
                res = common::RoughSetValue::RS_NONE;
              if (d.op == common::Operator::O_LESS || d.op == common::Operator::O_MORE_EQ)
                res = common::RoughSetValue::RS_ALL;
            }
          }
          if (res == common::RoughSetValue::RS_SOME &&
              (d.op == common::Operator::O_EQ ||
               d.op == common::Operator::O_NOT_EQ)) {  // the second case will be negated soon
            if (v1min == v1max && v2min == v2max) {
              if (v1min == v2min)
                res = common::RoughSetValue::RS_ALL;
              else
                res = common::RoughSetValue::RS_NONE;
            } else if (v1min == v1max) {
              auto sp2 = sec->GetFilter_Hist();
              if (sp2) {
                res = sp2->IsValue(v1min, v1max, pack, v2min, v2max);
              }
            } else if (v2min == v2max) {
              auto sp = GetFilter_Hist();
              if (sp) {
                res = sp->IsValue(v2min, v2max, pack, v1min, v1max);
              }
            } else {
              auto sp = GetFilter_Hist();
              auto sp2 = sec->GetFilter_Hist();

              // check intersection possibility on histograms
              if (sp && sp2 && (sp->Intersection(pack, v1min, v1max, sp2.get(), pack, v2min, v2max) == false))
                res = common::RoughSetValue::RS_NONE;
            }
          }
          // Now take into account all negations
          if (d.op == common::Operator::O_NOT_EQ || d.op == common::Operator::O_LESS_EQ ||
              d.op == common::Operator::O_MORE_EQ) {
            if (res == common::RoughSetValue::RS_ALL)
              res = common::RoughSetValue::RS_NONE;
            else if (res == common::RoughSetValue::RS_NONE)
              res = common::RoughSetValue::RS_ALL;
          }
        } else if (GetPackType() == common::PackType::STR) {
          types::BString v1min(dpn.min_s, Int64StrLen(dpn.min_i), false);
          types::BString v1max(dpn.max_s, Int64StrLen(dpn.max_i), false);
          types::BString v2min(dpn.min_s, Int64StrLen(dpn.min_i), false);
          types::BString v2max(dpn.max_s, Int64StrLen(dpn.max_i), false);
          if (CollationStrCmp(d.GetCollation(), v1min, v2max) >=
              0) {  // NOTE: only common::Operator::O_MORE, common::Operator::O_LESS and
                    // common::Operator::O_EQ are analyzed here, the rest of operators
                    // will be taken into account later (treated as negations)
            if (d.op == common::Operator::O_LESS ||
                d.op == common::Operator::O_MORE_EQ)  // the second case will be negated soon
              res = common::RoughSetValue::RS_NONE;
            if (CollationStrCmp(d.GetCollation(), v1min, v2max) > 0) {
              if (d.op == common::Operator::O_EQ || d.op == common::Operator::O_NOT_EQ)
                res = common::RoughSetValue::RS_NONE;
              if (d.op == common::Operator::O_MORE || d.op == common::Operator::O_LESS_EQ)
                res = common::RoughSetValue::RS_ALL;
            }
          }
          if (CollationStrCmp(d.GetCollation(), v1max, v2min) <= 0) {
            if (d.op == common::Operator::O_MORE ||
                d.op == common::Operator::O_LESS_EQ)  // the second case will be negated soon
              res = common::RoughSetValue::RS_NONE;
            if (CollationStrCmp(d.GetCollation(), v1max, v2min) < 0) {
              if (d.op == common::Operator::O_EQ || d.op == common::Operator::O_NOT_EQ)
                res = common::RoughSetValue::RS_NONE;
              if (d.op == common::Operator::O_LESS || d.op == common::Operator::O_MORE_EQ)
                res = common::RoughSetValue::RS_ALL;
            }
          }
          if (d.op == common::Operator::O_NOT_EQ || d.op == common::Operator::O_LESS_EQ ||
              d.op == common::Operator::O_MORE_EQ) {
            if (res == common::RoughSetValue::RS_ALL)
              res = common::RoughSetValue::RS_NONE;
            else if (res == common::RoughSetValue::RS_NONE)
              res = common::RoughSetValue::RS_ALL;
          }
        }
      }
      // take nulls into account
      if ((dpn.numOfNulls != 0 || secDpn.numOfNulls != 0 || additional_nulls_possible) &&
          res == common::RoughSetValue::RS_ALL)
        res = common::RoughSetValue::RS_SOME;
      return res;
    }
  }
  return common::RoughSetValue::RS_SOME;
}

common::RoughSetValue TianmuAttr::RoughCheck(int pack1, int pack2, Descriptor &d) {
  vcolumn::VirtualColumn *vc1 = d.val1.vc;
  vcolumn::VirtualColumn *vc2 = d.val2.vc;

  // Limitations for now: only the easiest numerical cases
  if (vc1 == nullptr || vc2 != nullptr || d.op != common::Operator::O_EQ || pack1 == -1 || pack2 == -1)
    return common::RoughSetValue::RS_SOME;
  vcolumn::SingleColumn *sc = nullptr;
  if (static_cast<int>(vc1->IsSingleColumn()))
    sc = static_cast<vcolumn::SingleColumn *>(vc1);
  if (sc == nullptr)
    return common::RoughSetValue::RS_SOME;
  TianmuAttr *sec = dynamic_cast<TianmuAttr *>(sc->GetPhysical());
  if (sec == nullptr || !Type().IsNumComparable(sec->Type()))
    return common::RoughSetValue::RS_SOME;

  LoadPackInfo();
  sec->LoadPackInfo();
  auto const &secDpn(sec->get_dpn(pack2));
  //	if(sec->Type().IsFloat())
  //		return RoughCheckBetween(pack1, *(double*)(&secDpn.min),
  //*(double*)(&secDpn.max));
  common::RoughSetValue r = RoughCheckBetween(pack1, secDpn.min_i, secDpn.max_i);
  return r == common::RoughSetValue::RS_ALL ? common::RoughSetValue::RS_SOME : r;
}

// check whether any value from the pack may meet the condition "... BETWEEN min
// AND max"
common::RoughSetValue TianmuAttr::RoughCheckBetween(int pack, int64_t v1, int64_t v2) {
  common::RoughSetValue res = common::RoughSetValue::RS_SOME;  // calculate as for common::Operator::O_BETWEEN
                                                               // and then consider negation
  bool is_float = Type().IsFloat();
  auto const &dpn(get_dpn(pack));

  // before:
  // if(v1 == common::PLUS_INF_64 || v2 == common::MINUS_INF_64) --> RS_NONE
  // if(is_float && (v1 == *(int64_t *)&common::PLUS_INF_DBL || v2 == *(int64_t *)&common::MINUS_INF_DBL)) --> RS_NONE
  // actually the v1 or v2 equal to boundary is illegal and it will cause empty result when condition
  // like `where col = PLUS_INF_64`, `where col = PLUS_INF_DBL`, `where col = MINUS_INF_DBL`
  if (!is_float && (v1 > dpn.max_i || v2 < dpn.min_i)) {
    res = common::RoughSetValue::RS_NONE;
  } else if (is_float && (*(double *)&v1 > dpn.max_d || *(double *)&v2 < dpn.min_d)) {
    res = common::RoughSetValue::RS_NONE;
  } else if (!is_float && (v1 <= dpn.min_i && v2 >= dpn.max_i)) {
    res = common::RoughSetValue::RS_ALL;
  } else if (is_float && (*(double *)&v1 <= dpn.min_d && *(double *)&v2 >= dpn.max_d)) {
    res = common::RoughSetValue::RS_ALL;
  } else if ((!is_float && v1 > v2) || (is_float && (*(double *)&v1 > *(double *)&v2))) {
    res = common::RoughSetValue::RS_NONE;
  } else {
    if (auto sp = GetFilter_Hist())
      res = sp->IsValue(v1, v2, pack, dpn.min_i, dpn.max_i);

    if (res == common::RoughSetValue::RS_SOME) {
      if (auto sp = GetFilter_Bloom()) {
        auto vmin = std::to_string(v1);
        auto vmax = std::to_string(v2);
        types::BString keymin(vmin.c_str(), vmin.length());
        types::BString keymax(vmax.c_str(), vmax.length());
        res = sp->IsValue(keymin, keymax, pack);
      }
    }
  }
  if (dpn.numOfNulls != 0 && res == common::RoughSetValue::RS_ALL) {
    res = common::RoughSetValue::RS_SOME;
  }
  return res;
}

int64_t TianmuAttr::RoughMin(Filter *f, common::RoughSetValue *rf)  // f == nullptr is treated as full filter
{
  LoadPackInfo();
  if (GetPackType() == common::PackType::STR)
    return common::MINUS_INF_64;
  if (f && f->IsEmpty() && rf == nullptr)
    return 0;
  int64_t res;
  if (ATI::IsRealType(TypeName())) {
    union {
      double d;
      int64_t i;
    } di;
    di.d = DBL_MAX;
    for (uint p = 0; p < SizeOfPack(); p++) {  // minimum of nonempty packs
      auto const &dpn(get_dpn(p));
      if ((f == nullptr || !f->IsEmpty(p)) && (rf == nullptr || rf[p] != common::RoughSetValue::RS_NONE)) {
        if (di.d > dpn.min_d)
          di.i = dpn.min_i;
      }
    }
    if (di.d == DBL_MAX)
      di.d = -(DBL_MAX);
    res = di.i;
  } else {
    res = common::PLUS_INF_64;
    for (uint p = 0; p < SizeOfPack(); p++) {  // minimum of nonempty packs
      auto const &dpn(get_dpn(p));
      if ((f == nullptr || !f->IsEmpty(p)) && (rf == nullptr || rf[p] != common::RoughSetValue::RS_NONE) &&
          res > dpn.min_i)
        res = dpn.min_i;
    }
    if (res == common::PLUS_INF_64)
      res = common::MINUS_INF_64;  // NULLS only
  }
  return res;
}

int64_t TianmuAttr::RoughMax(Filter *f, common::RoughSetValue *rf)  // f == nullptr is treated as full filter
{
  LoadPackInfo();
  if (GetPackType() == common::PackType::STR)
    return common::PLUS_INF_64;
  if (f && f->IsEmpty() && rf == nullptr)
    return 0;
  int64_t res;
  if (ATI::IsRealType(TypeName())) {
    union {
      double d;
      int64_t i;
    } di;
    di.d = -(DBL_MAX);
    for (uint p = 0; p < SizeOfPack(); p++) {  // minimum of nonempty packs
      auto const &dpn(get_dpn(p));
      if ((f == nullptr || !f->IsEmpty(p)) && (rf == nullptr || rf[p] != common::RoughSetValue::RS_NONE)) {
        if (di.d < dpn.max_d)
          di.i = dpn.max_i;
      }
    }
    if (di.d == -(DBL_MAX))
      di.d = DBL_MAX;
    res = di.i;
  } else {
    res = common::MINUS_INF_64;
    for (uint p = 0; p < SizeOfPack(); p++) {  // maximum of nonempty packs
      auto const &dpn(get_dpn(p));
      if ((f == nullptr || !f->IsEmpty(p)) && (rf == nullptr || rf[p] != common::RoughSetValue::RS_NONE) &&
          res < dpn.max_i)
        res = dpn.max_i;
    }
    if (res == common::MINUS_INF_64)
      res = common::PLUS_INF_64;  // NULLS only
  }
  return res;
}

std::vector<int64_t> TianmuAttr::GetListOfDistinctValuesInPack(int pack) {
  std::vector<int64_t> list_vals;
  if (GetPackType() != common::PackType::INT || pack == -1 ||
      (Type().Lookup() && types::RequiresUTFConversions(GetCollation())))
    return list_vals;
  auto const &dpn(get_dpn(pack));
  if (dpn.min_i == dpn.max_i) {
    list_vals.push_back(dpn.min_i);
    if (NumOfNulls() > 0)
      list_vals.push_back(common::NULL_VALUE_64);
    return list_vals;
  } else if (GetPackOntologicalStatus(pack) == PackOntologicalStatus::NULLS_ONLY) {
    list_vals.push_back(common::NULL_VALUE_64);
    return list_vals;
  } else if (TypeName() == common::ColumnType::REAL || TypeName() == common::ColumnType::FLOAT) {
    return list_vals;
  } else if (dpn.max_i - dpn.min_i > 0 && dpn.max_i - dpn.min_i < 1024) {
    auto sp = GetFilter_Hist();
    if (!sp || !sp->ExactMode(dpn.min_i, dpn.max_i)) {
      return list_vals;
    }
    list_vals.push_back(dpn.min_i);
    list_vals.push_back(dpn.max_i);
    for (int64_t v = dpn.min_i + 1; v < dpn.max_i; v++) {
      if (sp->IsValue(v, v, pack, dpn.min_i, dpn.max_i) != common::RoughSetValue::RS_NONE)
        list_vals.push_back(v);
    }
    if (NumOfNulls() > 0)
      list_vals.push_back(common::NULL_VALUE_64);
    return list_vals;
  }
  return list_vals;
}

uint64_t TianmuAttr::ApproxDistinctVals(bool incl_nulls, Filter *f, common::RoughSetValue *rf,
                                        bool outer_nulls_possible) {
  LoadPackInfo();
  uint64_t no_dist = 0;
  int64_t max_obj = NumOfObj();  // no more values than objects
  if (NumOfNulls() > 0 || outer_nulls_possible) {
    if (incl_nulls)
      no_dist++;  // one value for null
    max_obj = max_obj - NumOfNulls() + (incl_nulls ? 1 : 0);
    if (f && max_obj > f->NumOfOnes())
      max_obj = f->NumOfOnes();
  } else if (f)
    max_obj = f->NumOfOnes();
  if (TypeName() == common::ColumnType::DATE) {
    try {
      types::TianmuDateTime date_min(RoughMin(f, rf), common::ColumnType::DATE);
      types::TianmuDateTime date_max(RoughMax(f, rf), common::ColumnType::DATE);
      no_dist += (date_max - date_min) + 1;  // overloaded minus - a number of days between dates
    } catch (...) {                          // in case of any problems with conversion of dates - just
                                             // numerical approximation
      no_dist += RoughMax(f, rf) - RoughMin(f, rf) + 1;
    }
  } else if (TypeName() == common::ColumnType::YEAR) {
    try {
      types::TianmuDateTime date_min(RoughMin(f, rf), common::ColumnType::YEAR);
      types::TianmuDateTime date_max(RoughMax(f, rf), common::ColumnType::YEAR);
      no_dist += ((int)(date_max.Year()) - date_min.Year()) + 1;
    } catch (...) {  // in case of any problems with conversion of dates - just
                     // numerical approximation
      no_dist += RoughMax(f, rf) - RoughMin(f, rf) + 1;
    }
  } else if (GetPackType() == common::PackType::INT && TypeName() != common::ColumnType::REAL &&
             TypeName() != common::ColumnType::FLOAT) {
    int64_t cur_min = RoughMin(f, rf);  // extrema of nonempty packs
    int64_t cur_max = RoughMax(f, rf);
    uint64_t span = cur_max - cur_min + 1;
    if (span < 100000) {  // use Histograms to calculate exact number of
                          // distinct values
      Filter values_present(span, pss);
      values_present.Reset();
      for (uint p = 0; p < SizeOfPack(); p++) {
        auto const &dpn(get_dpn(p));
        if ((f == nullptr || !f->IsEmpty(p)) && (rf == nullptr || rf[p] != common::RoughSetValue::RS_NONE) &&
            dpn.min_i <= dpn.max_i) {
          // dpn.min <= dpn.max is not true e.g. when the pack contains nulls
          // only
          if (auto sp = GetFilter_Hist(); sp && sp->ExactMode(dpn.min_i, dpn.max_i)) {
            values_present.Set(dpn.min_i - cur_min);
            values_present.Set(dpn.max_i - cur_min);
            for (int64_t v = dpn.min_i + 1; v < dpn.max_i; v++)
              if (sp->IsValue(v, v, p, dpn.min_i, dpn.max_i) != common::RoughSetValue::RS_NONE) {
                values_present.Set(v - cur_min);
              }
          } else {  // no Histogram or not exact: mark the whole interval
            values_present.SetBetween(dpn.min_i - cur_min, dpn.max_i - cur_min);
          }
        }
        if (values_present.IsFull())
          break;
      }
      no_dist += values_present.NumOfOnes();
    } else
      no_dist += span;  // span between min and max
  } else if (TypeName() == common::ColumnType::REAL || TypeName() == common::ColumnType::FLOAT) {
    int64_t cur_min = RoughMin(f, rf);  // extrema of nonempty packs
    int64_t cur_max = RoughMax(f, rf);
    if (cur_min == cur_max && cur_min != common::NULL_VALUE_64)  // the only case we can do anything
      no_dist += 1;
    else
      no_dist = max_obj;
  } else if (TypeName() == common::ColumnType::STRING || TypeName() == common::ColumnType::VARCHAR ||
             TypeName() == common::ColumnType::LONGTEXT) {
    size_t max_len = 0;
    for (uint p = 0; p < SizeOfPack(); p++) {  // max len of nonempty packs
      if (f == nullptr || !f->IsEmpty(p))
        max_len = std::max(max_len, GetActualSize(p));
    }
    if (max_len > 0 && max_len < 6)
      no_dist += int64_t(256) << ((max_len - 1) * 8);
    else if (max_len > 0)
      no_dist = max_obj;  // default
    else if (max_len == 0 && max_obj)
      no_dist++;
  } else
    no_dist = max_obj;  // default

  if (no_dist > (uint64_t)max_obj)
    return max_obj;
  return no_dist;
}

uint64_t TianmuAttr::ExactDistinctVals(Filter *f)  // provide the exact number of diff. non-null values, if
                                                   // possible, or common::NULL_VALUE_64
{
  if (f == nullptr)  // no exact information about tuples => nothing can be
                     // determined for sure
    return common::NULL_VALUE_64;
  LoadPackInfo();
  if (Type().Lookup() && !types::RequiresUTFConversions(GetCollation()) && f->IsFull())
    return RoughMax(nullptr) + 1;
  bool nulls_only = true;
  for (uint p = 0; p < SizeOfPack(); p++)
    if (!f->IsEmpty(p) && GetPackOntologicalStatus(p) != PackOntologicalStatus::NULLS_ONLY) {
      nulls_only = false;
      break;
    }
  if (nulls_only)
    return 0;
  if (GetPackType() == common::PackType::INT && !types::RequiresUTFConversions(GetCollation()) &&
      TypeName() != common::ColumnType::REAL && TypeName() != common::ColumnType::FLOAT) {
    int64_t cur_min = RoughMin(f);  // extrema of nonempty packs
    int64_t cur_max = RoughMax(f);
    uint64_t span = cur_max - cur_min + 1;
    if (span < 100000) {  // use Histograms to calculate exact number of distinct values
      Filter values_present(span, ValueOfPackPower());
      values_present.Reset();

      // Phase 1: mark all values, which are present for sure
      for (uint p = 0; p < SizeOfPack(); p++) {
        if (f->IsFull(p)) {
          auto const &dpn(get_dpn(p));
          if (dpn.min_i < dpn.max_i) {
            // dpn.min <= dpn.max is not true when the pack contains nulls
            // only
            if (auto sp = GetFilter_Hist(); sp && sp->ExactMode(dpn.min_i, dpn.max_i)) {
              values_present.Set(dpn.min_i - cur_min);
              values_present.Set(dpn.max_i - cur_min);
              for (int64_t v = dpn.min_i + 1; v < dpn.max_i; v++) {
                if (sp->IsValue(v, v, p, dpn.min_i, dpn.max_i) != common::RoughSetValue::RS_NONE) {
                  values_present.Set(v - cur_min);
                }
              }
            } else {  // no Histogram or not exact: cannot calculate exact number
              return common::NULL_VALUE_64;
            }
          } else if (dpn.min_i == dpn.max_i) {  // only one value
            values_present.Set(dpn.min_i - cur_min);
          }
          if (values_present.IsFull())
            break;
        }
      }
      // Phase 2: check whether there are any other values possible in suspected packs
      for (uint p = 0; p < SizeOfPack(); p++) {
        if (!f->IsEmpty(p) && !f->IsFull(p)) {  // suspected pack
          auto const &dpn(get_dpn(p));
          if (!values_present.IsFullBetween(dpn.min_i - cur_min, dpn.max_i - cur_min)) {
            return common::NULL_VALUE_64;
          }
        }
      }
      return values_present.NumOfOnes();
    }
  }
  return common::NULL_VALUE_64;
}

double TianmuAttr::RoughSelectivity() {
  if (rough_selectivity == -1) {
    LoadPackInfo();
    if (GetPackType() == common::PackType::INT && TypeName() != common::ColumnType::REAL &&
        TypeName() != common::ColumnType::FLOAT && SizeOfPack() > 0) {
      int64_t global_min = common::PLUS_INF_64;
      int64_t global_max = common::MINUS_INF_64;
      double width_sum = 0;
      for (uint p = 0; p < SizeOfPack(); p++) {  // minimum of nonempty packs
        auto const &dpn(get_dpn(p));
        if (dpn.numOfNulls == uint(dpn.numOfRecords) + 1)
          continue;
        if (dpn.min_i < global_min)
          global_min = dpn.min_i;
        if (dpn.max_i > global_max)
          global_max = dpn.max_i;
        width_sum += double(dpn.max_i) - double(dpn.min_i) + 1;
      }
      rough_selectivity = (width_sum / SizeOfPack()) / (double(global_max) - double(global_min) + 1);
    } else
      rough_selectivity = 1;
  }
  return rough_selectivity;
}

void TianmuAttr::GetTextStat(types::TextStat &s, Filter *f) {
  bool success = false;
  LoadPackInfo();
  if (GetPackType() == common::PackType::STR && !types::RequiresUTFConversions(GetCollation())) {
    if (auto sp = GetFilter_CMap()) {
      success = true;
      for (uint p = 0; p < SizeOfPack(); p++)
        if (f == nullptr || !f->IsEmpty(p)) {
          auto const &dpn(get_dpn(p));
          if (dpn.NullOnly())
            continue;
          size_t len = GetActualSize(p);
          if (len > 48) {
            success = false;
            break;
          }
          size_t pack_prefix = GetPrefixLength(p);
          size_t i = 0;
          uchar *prefix = (uchar *)dpn.min_s;
          for (i = 0; i < pack_prefix; i++) s.AddChar(prefix[i], i);
          for (i = pack_prefix; i < len; i++) {
            s.AddLen(i);  // end of value is always possible (except a prefix)
            for (int c = 0; c < 256; c++)
              if (sp->IsSet(p, c, i - pack_prefix))
                s.AddChar(c, i);
          }
          s.AddLen(len);
          if (p % 16 == 15)  // check it from time to time
            s.CheckIfCreatePossible();
          if (!s.IsValid())
            break;
        }
    }
  }
  if (success == false)
    s.Invalidate();
}

// calculate the number of 1's in histograms and other KN stats
void TianmuAttr::RoughStats(double &hist_density, int &trivial_packs, double &span) {
  uint npack = SizeOfPack();
  hist_density = -1;
  trivial_packs = 0;
  span = -1;
  LoadPackInfo();
  for (uint pack = 0; pack < npack; pack++) {
    auto const &dpn(get_dpn(pack));
    if (dpn.NullOnly() || (GetPackType() == common::PackType::INT && dpn.numOfNulls == 0 && dpn.min_i == dpn.max_i))
      trivial_packs++;
    else {
      if (GetPackType() == common::PackType::INT && !ATI::IsRealType(TypeName())) {
        if (span == -1)
          span = 0;
        span += dpn.max_i - dpn.min_i;
      }
      if (GetPackType() == common::PackType::INT && ATI::IsRealType(TypeName())) {
        if (span == -1)
          span = 0;
        span += dpn.max_d - dpn.min_d;
      }
    }
  }
  if (span != -1) {
    int64_t tmp_min = GetMinInt64();  // always a value - i_min from TianmuAttr
    int64_t tmp_max = GetMaxInt64();
    if (ATI::IsRealType(TypeName()))
      span = (span / (npack - trivial_packs)) / (*(double *)(&tmp_max) - *(double *)(&tmp_min));
    else
      span = (span / (npack - trivial_packs)) / double(GetMaxInt64() - GetMinInt64());
  }
  if (auto sp = GetFilter_Hist(); sp && GetPackType() == common::PackType::INT) {
    int ones_found = 0, ones_needed = 0;
    for (uint pack = 0; pack < npack; pack++) {
      auto const &dpn(get_dpn(pack));
      if (uint(dpn.numOfRecords + 1) != dpn.numOfNulls && dpn.min_i + 1 < dpn.max_i) {
        int loc_no_ones;
        if (dpn.max_i - dpn.min_i > 1024)
          loc_no_ones = 1024;
        else
          loc_no_ones = int(dpn.max_i - dpn.min_i - 1);
        ones_needed += loc_no_ones;
        ones_found += sp->Count(pack, loc_no_ones);
      }
    }
    if (ones_needed > 0)
      hist_density = ones_found / double(ones_needed);
  }
}

void TianmuAttr::DisplayAttrStats(Filter *f)  // filter is for # of objects
{
  int npack = SizeOfPack();
  LoadPackInfo();
  std::stringstream ss;
  ss << "Column " << m_cid << ", table " << m_tid << (IsUnique() ? ", unique" : " ") << std::endl;
  if (GetPackType() == common::PackType::INT)
    ss << "Pack    Rows  Nulls              Min              Max             "
          "Sum  Hist. "
       << std::endl;
  else
    ss << "Pack    Rows  Nulls       Min       Max  Size CMap " << std::endl;
  // This line is 79 char. width:
  ss << "----------------------------------------------------------------------"
        "---------"
     << std::endl;
  char line_buf[150];
  for (int pack = 0; pack < npack; pack++) {
    int64_t cur_obj = 0;
    auto const &dpn(get_dpn(pack));
    if (f)
      cur_obj = f->NumOfOnes(pack);
    else if (pack == npack - 1)
      cur_obj = dpn.numOfRecords + 1;

    std::sprintf(line_buf, "%-7d %5ld %5d", pack, cur_obj, dpn.numOfNulls);

    if (uint(dpn.numOfRecords + 1) != dpn.numOfNulls) {
      if (GetPackType() == common::PackType::INT && !ATI::IsRealType(TypeName())) {
        int rsi_span = -1;
        int rsi_ones = 0;
        if (auto sp = GetFilter_Hist()) {
          rsi_span = 0;
          rsi_ones = 0;
          if (dpn.max_i > dpn.min_i + 1) {
            if (sp->ExactMode(dpn.min_i, dpn.max_i))
              rsi_span = int(dpn.max_i - dpn.min_i - 1);
            else
              rsi_span = 1024;
            rsi_ones = sp->Count(pack, rsi_span);
          }
        }
        std::sprintf(line_buf + std::strlen(line_buf), " %16ld %16ld", GetMinInt64(pack), GetMaxInt64(pack));
        bool nonnegative = false;  // not used anyway
        int64_t loc_sum = GetSum(pack, nonnegative);
        if (loc_sum != common::NULL_VALUE_64)
          std::sprintf(line_buf + std::strlen(line_buf), " %15ld", loc_sum);
        else
          std::sprintf(line_buf + std::strlen(line_buf), "             n/a");
        if (rsi_span >= 0)
          std::sprintf(line_buf + std::strlen(line_buf), "  %d/%d", rsi_ones, rsi_span);
        if (!GetFilter_Hist())
          std::sprintf(line_buf + std::strlen(line_buf), "   n/a");
      } else if (GetPackType() == common::PackType::INT && ATI::IsRealType(TypeName())) {
        bool dummy;
        int64_t sum_d = GetSum(pack, dummy);
        std::sprintf(line_buf + std::strlen(line_buf), " %16g %16g %15g  -", dpn.min_d, dpn.max_d,
                     *((double *)(&sum_d)));
      } else {  // common::PackType = common::PackType::STR
        char smin[10];
        char smax[10];
        std::memcpy(smin, dpn.min_s, 8);
        std::memcpy(smax, dpn.max_s, 8);
        smin[8] = '\0';
        smax[8] = '\0';
        std::sprintf(line_buf + std::strlen(line_buf), "  %8s  %8s %5zu ", smin, smax, GetActualSize(pack));
        if (auto sp = GetFilter_CMap(); sp && !types::RequiresUTFConversions(Type().GetCollation())) {
          uint len = sp->NumOfPositions();
          uint i;
          for (i = 0; i < len && i < 8; i++) {  // display at most 8 positions (limited by line width)
            if ((i == len - 1) || (i == 7))
              std::sprintf(line_buf + std::strlen(line_buf), "%d", sp->Count(pack, i));
            else
              std::sprintf(line_buf + std::strlen(line_buf), "%d-", sp->Count(pack, i));
          }
          if (i < len)
            std::sprintf(line_buf + std::strlen(line_buf), "...");
        } else
          std::sprintf(line_buf + std::strlen(line_buf), "n/a");
      }
    }
    ss << line_buf << std::endl;
  }
  ss << "----------------------------------------------------------------------"
        "---------"
     << std::endl;
  TIANMU_LOG(LogCtl_Level::DEBUG, ss.str().c_str());
}
}  // namespace core
}  // namespace Tianmu
