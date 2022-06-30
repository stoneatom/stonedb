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

// This is a part of RCAttr implementation concerned with the KNs and its usage
#include "core/cq_term.h"
#include "core/engine.h"
#include "core/pack.h"
#include "core/pack_guardian.h"
#include "core/rc_attr.h"
#include "core/rc_attr_typeinfo.h"
#include "core/value_set.h"
#include "types/text_stat.h"
#include "vc/multi_value_column.h"
#include "vc/single_column.h"

namespace stonedb {
namespace core {

// Rough queries and indexes

uint Int64StrLen(int64_t x) {
  uint min_len = 8;
  uchar *s = (uchar *)(&x);
  while (min_len > 0 && s[min_len - 1] == '\0') min_len--;
  return min_len;
}

// NOTE: similar code is in vcolumn::VirtualColumnBase::DoRoughCheck
common::RSValue RCAttr::RoughCheck(int pack, Descriptor &d, bool additional_nulls_possible) {
  if (d.op == common::Operator::O_FALSE)
    return common::RSValue::RS_NONE;
  else if (d.op == common::Operator::O_TRUE)
    return common::RSValue::RS_ALL;
  // TODO: implement RoughCheck CMAP for utf8
  if (GetPackType() == common::PackType::STR && types::RequiresUTFConversions(d.GetCollation()) &&
      d.GetCollation().collation != Type().GetCollation().collation) {
    if (d.encoded) {
      LoadPackInfo();  // just in case, although the condition was encoded and
                       // this function should be executed earlier
      auto const &dpn(get_dpn(pack));
      if (dpn.NullOnly()) {  // all objects are null
        if (d.op == common::Operator::O_IS_NULL)
          return common::RSValue::RS_ALL;
        else
          return common::RSValue::RS_NONE;
      }
    }
    return common::RSValue::RS_SOME;
  }
  if (d.IsType_AttrValOrAttrValVal()) {
    if (!d.encoded) return common::RSValue::RS_SOME;
    vcolumn::VirtualColumn *vc1 = d.val1.vc;
    static MIIterator const mit(NULL, pss);
    LoadPackInfo();  // just in case, although the condition was encoded and
                     // this function should be executed earlier
    auto const &dpn(get_dpn(pack));
    if (dpn.NullOnly()) {  // all objects are null
      if (d.op == common::Operator::O_IS_NULL)
        return common::RSValue::RS_ALL;
      else
        return common::RSValue::RS_NONE;
    }
    if (d.op == common::Operator::O_IS_NULL || d.op == common::Operator::O_NOT_NULL) {
      if (dpn.nn == 0 && !additional_nulls_possible) {
        if (d.op == common::Operator::O_IS_NULL)
          return common::RSValue::RS_NONE;
        else
          return common::RSValue::RS_ALL;
      }
      return common::RSValue::RS_SOME;
    } else if ((d.op == common::Operator::O_LIKE || d.op == common::Operator::O_NOT_LIKE) &&
               GetPackType() == common::PackType::STR) {
      DEBUG_ASSERT(vc1->IsConst());
      types::BString pat;
      vc1->GetValueString(pat, mit);
      common::RSValue res = common::RSValue::RS_SOME;
      // here: check min, max
      uint pattern_prefix = 0;        // e.g. "ab_cd_e%f"  -> 7
      uint pattern_fixed_prefix = 0;  // e.g. "ab_cd_e%f"  -> 2
      uint pack_prefix;
      if (types::RequiresUTFConversions(d.GetCollation())) {
        my_match_t mm;
        if (d.GetCollation().collation->coll->instr(d.GetCollation().collation, pat.val, pat.len, "%", 1, &mm, 1) == 2)
          pattern_prefix = pattern_fixed_prefix = mm.end;

        if (d.GetCollation().collation->coll->instr(d.GetCollation().collation, pat.val, pat.len, "_", 1, &mm, 1) == 2)
          if (mm.end < pattern_fixed_prefix) pattern_fixed_prefix = mm.end;

        if ((pattern_fixed_prefix > 0) &&
            types::BString(pat.val, pattern_fixed_prefix).LessEqThanMaxUTF(dpn.max_s, Type().GetCollation()) == false)
          res = common::RSValue::RS_NONE;

        if (pattern_fixed_prefix > GetActualSize(pack)) res = common::RSValue::RS_NONE;
        pack_prefix = GetPrefixLength(pack);
        if (res == common::RSValue::RS_SOME && pack_prefix > 0 &&
            pattern_fixed_prefix <= pack_prefix  // special case: "xyz%" and the
                                                 // pack prefix is at least 3
            && pattern_fixed_prefix + 1 == pat.len && pat[pattern_fixed_prefix] == '%') {
          if (d.GetCollation().collation->coll->strnncoll(d.GetCollation().collation, (const uchar *)pat.val,
                                                          pattern_fixed_prefix, (const uchar *)dpn.min_s,
                                                          pattern_fixed_prefix, 0) == 0)
            res = common::RSValue::RS_ALL;
          else
            res = common::RSValue::RS_NONE;  // prefix and pattern are different
        }

      } else {
        while (pattern_prefix < pat.len && pat[pattern_prefix] != '%') pattern_prefix++;
        while (pattern_fixed_prefix < pat.len && pat[pattern_fixed_prefix] != '%' && pat[pattern_fixed_prefix] != '_')
          pattern_fixed_prefix++;

        if ((pattern_fixed_prefix > 0) && types::BString(pat.val, pattern_fixed_prefix).LessEqThanMax(dpn.max_s) ==
                                              false)  // val_t==NULL means +/-infty
          res = common::RSValue::RS_NONE;
        if (pattern_fixed_prefix > GetActualSize(pack)) res = common::RSValue::RS_NONE;
        pack_prefix = GetPrefixLength(pack);
        if (res == common::RSValue::RS_SOME && pack_prefix > 0 &&
            pattern_fixed_prefix <= pack_prefix  // special case: "xyz%" and the
                                                 // pack prefix is at least 3
            && pattern_fixed_prefix + 1 == pat.len && pat[pattern_fixed_prefix] == '%') {
          if (std::memcmp(pat.val, dpn.min_s, pattern_fixed_prefix) == 0)  // pattern is equal to the prefix
            res = common::RSValue::RS_ALL;
          else
            res = common::RSValue::RS_NONE;  // prefix and pattern are different
        }
      }

      if (res == common::RSValue::RS_SOME && std::min(pattern_prefix, pack_prefix) < pat.len &&
          !types::RequiresUTFConversions(d.GetCollation())) {
        types::BString pattern_for_cmap;  // note that cmap is shifted by a common prefix!
        if (pattern_prefix > pack_prefix)
          pattern_for_cmap = types::BString(pat.val + pack_prefix,
                                            pat.len - pack_prefix);  // "xyz%abc" -> "z%abc"
        else
          pattern_for_cmap = types::BString(pat.val + pattern_prefix,
                                            pat.len - pattern_prefix);  // "xyz%abc" -> "%abc"

        if (!(pattern_for_cmap.len == 1 && pattern_for_cmap[0] == '%')) {  // i.e. "%" => all is matching
          if (auto sp = GetFilter_CMap()) res = sp->IsLike(pattern_for_cmap, pack, d.like_esc);
        } else
          res = common::RSValue::RS_ALL;
      }
      if (d.op == common::Operator::O_NOT_LIKE) {
        if (res == common::RSValue::RS_ALL)
          res = common::RSValue::RS_NONE;
        else if (res == common::RSValue::RS_NONE)
          res = common::RSValue::RS_ALL;
      }
      if ((dpn.nn != 0 || additional_nulls_possible) && res == common::RSValue::RS_ALL) res = common::RSValue::RS_SOME;
      return res;
    } else if ((d.op == common::Operator::O_IN || d.op == common::Operator::O_NOT_IN) &&
               GetPackType() == common::PackType::STR) {
      DEBUG_ASSERT(dynamic_cast<vcolumn::MultiValColumn *>(vc1));
      vcolumn::MultiValColumn *mvc(static_cast<vcolumn::MultiValColumn *>(vc1));
      uint pack_prefix = GetPrefixLength(pack);
      common::RSValue res = common::RSValue::RS_SOME;
      if ((mvc->IsConst()) && (mvc->NoValues(mit) > 0 && mvc->NoValues(mit) < 64) &&
          !types::RequiresUTFConversions(d.GetCollation())) {
        if (auto sp = GetFilter_CMap()) {
          res = common::RSValue::RS_NONE;  // TODO: get rid with the iterator below

          for (vcolumn::MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit);
               (it != end) && (res == common::RSValue::RS_NONE); ++it) {
            types::BString v1 = it->GetString();
            if (pack_prefix <= v1.len) {
              if (pack_prefix == 0 || std::memcmp(v1.val, dpn.min_s, pack_prefix) == 0) {
                size_t len = v1.len - pack_prefix;
                types::BString v(len <= 0 ? "" : v1.val + pack_prefix, (int)len);
                if (v1.len == pack_prefix || sp->IsValue(v, v, pack) != common::RSValue::RS_NONE)
                  // suspected, if any value is possible (due to the prefix or
                  // CMAP)
                  res = common::RSValue::RS_SOME;
              }
            }
          }
        }
      }

      // add bloom filter for in/not in
      if (res == common::RSValue::RS_SOME && (mvc->IsConst()) && (mvc->NoValues(mit) > 0 && mvc->NoValues(mit) < 64)) {
        if (auto sp = GetFilter_Bloom()) {
          res = common::RSValue::RS_NONE;
          for (vcolumn::MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit);
               (it != end) && (res == common::RSValue::RS_NONE); ++it) {
            types::BString v = it->GetString();
            if (sp->IsValue(v, v, pack) != common::RSValue::RS_NONE) res = common::RSValue::RS_SOME;
          }
        }
      }

      if (d.op == common::Operator::O_NOT_IN) {
        if (res == common::RSValue::RS_ALL)
          res = common::RSValue::RS_NONE;
        else if (res == common::RSValue::RS_NONE)
          res = common::RSValue::RS_ALL;
      }
      if (res == common::RSValue::RS_ALL && (dpn.nn > 0 || additional_nulls_possible)) res = common::RSValue::RS_SOME;
      return res;
    } else if ((d.op == common::Operator::O_IN || d.op == common::Operator::O_NOT_IN) &&
               (GetPackType() == common::PackType::INT)) {
      if (vc1->IsConst()) {
        DEBUG_ASSERT(dynamic_cast<vcolumn::MultiValColumn *>(vc1));
        vcolumn::MultiValColumn *mvc(static_cast<vcolumn::MultiValColumn *>(vc1));
        int64_t v1, v2;
        types::RCNum rcn_min = mvc->GetSetMin(mit);
        types::RCNum rcn_max = mvc->GetSetMax(mit);
        if (rcn_min.IsNull() || rcn_max.IsNull())  // cannot determine min/max
          return common::RSValue::RS_SOME;
        if (Type().IsLookup()) {
          v1 = rcn_min.GetValueInt64();
          v2 = rcn_max.GetValueInt64();
        } else {
          bool v1_rounded, v2_rounded;
          v1 = EncodeValue64(&rcn_min, v1_rounded);
          v2 = EncodeValue64(&rcn_max, v2_rounded);
        }

        common::RSValue res = common::RSValue::RS_SOME;
        if (ATI::IsRealType(TypeName())) return res;  // real values not implemented yet
        if (v1 > dpn.max_i || v2 < dpn.min_i) {
          res = common::RSValue::RS_NONE;  // calculate as for common::Operator::O_IN and then take
                                           // common::Operator::O_NOT_IN into account
        } else if (dpn.min_i == dpn.max_i) {
          types::RCValueObject rcvo(ATI::IsDateTimeType(TypeName())
                                        ? types::RCValueObject(types::RCDateTime(dpn.min_i, TypeName()))
                                        : types::RCValueObject(types::RCNum(dpn.min_i, Type().GetScale())));
          res = (mvc->Contains(mit, *rcvo) != false) ? common::RSValue::RS_ALL : common::RSValue::RS_NONE;
        } else {
          if (auto sp = GetFilter_Hist()) res = sp->IsValue(v1, v2, pack, dpn.min_i, dpn.max_i);
          if (res == common::RSValue::RS_ALL)  // v1, v2 are just a boundary, not
                                               // continuous interval
            res = common::RSValue::RS_SOME;
        }
        if (res == common::RSValue::RS_SOME && (mvc->NoValues(mit) > 0 && mvc->NoValues(mit) < 64)) {
          bool v_rounded = false;
          res = common::RSValue::RS_NONE;
          int64_t v;  // TODO: get rid with the iterator below
          for (vcolumn::MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit);
               (it != end) && (res == common::RSValue::RS_NONE); ++it) {
            if (!Type().IsLookup()) {  // otherwise it will be decoded to text
              v = EncodeValue64(it->GetValue().Get(), v_rounded);
            } else
              v = it->GetInt64();
            if (!v_rounded) {
              auto sp = GetFilter_Hist();
              if ((!sp && v <= dpn.max_i && v >= dpn.min_i) ||
                  (sp && sp->IsValue(v, v, pack, dpn.min_i, dpn.max_i) != common::RSValue::RS_NONE)) {
                // suspected, if any value is possible
                res = common::RSValue::RS_SOME;  // note: v_rounded means that this real
                                                 // value could not match this pack
                break;
              }
            }
          }
        }
        if (d.op == common::Operator::O_NOT_IN) {
          if (res == common::RSValue::RS_ALL)
            res = common::RSValue::RS_NONE;
          else if (res == common::RSValue::RS_NONE)
            res = common::RSValue::RS_ALL;
        }
        if (res == common::RSValue::RS_ALL && (dpn.nn > 0 || additional_nulls_possible)) res = common::RSValue::RS_SOME;
        return res;
      }
    } else if (GetPackType() == common::PackType::STR) {  // Note: text operations as
                                                          // common::PackType::INT calculated as
                                                          // IN or below

      DEBUG_ASSERT(d.op == common::Operator::O_BETWEEN || d.op == common::Operator::O_NOT_BETWEEN);
      vcolumn::VirtualColumn *vc2 = d.val2.vc;
      common::RSValue res = common::RSValue::RS_SOME;
      uint pack_prefix = GetPrefixLength(pack);
      uint val_prefix = 0;
      types::BString vmin;
      types::BString vmax;
      vc1->GetValueString(vmin, mit);
      vc2->GetValueString(vmax, mit);
      if (vmin.IsNull() && vmax.IsNull())  // comparing with null - always false
        return common::RSValue::RS_NONE;
      while (vmin.val && vmax.val && val_prefix < vmin.len && val_prefix < vmax.len &&
             vmin[val_prefix] == vmax[val_prefix])
        val_prefix++;  // Common prefix for values. It is a value length in case
                       // of equality.
      // check min, max
      // TODO UTF8: check PREFIX handling
      if (val_prefix > GetActualSize(pack)) {  // value to be found is longer than texts in the pack
        res = common::RSValue::RS_NONE;
      } else if ((vmax.val && vmax.GreaterEqThanMinUTF(dpn.min_s, Type().GetCollation()) == false) ||
                 (vmin.val &&
                  vmin.LessEqThanMaxUTF(dpn.max_s, Type().GetCollation()) == false))  // val_t==NULL means +/-infty
        res = common::RSValue::RS_NONE;
      else if ((vmin.val == NULL || vmin.GreaterEqThanMinUTF(dpn.min_s, Type().GetCollation()) == false) &&
               (vmax.val == NULL ||
                vmax.LessEqThanMaxUTF(dpn.max_s, Type().GetCollation()) == false))  // val_t==NULL means +/-infty
        res = common::RSValue::RS_ALL;
      else if (pack_prefix == GetActualSize(pack) && vmin == vmax) {  // exact case for short texts
        if (vmin.GreaterEqThanMinUTF(dpn.min_s, Type().GetCollation()) &&
            vmin.LessEqThanMaxUTF(dpn.min_s, Type().GetCollation()))
          res = common::RSValue::RS_ALL;
        else
          res = common::RSValue::RS_NONE;
      }

      if (res == common::RSValue::RS_SOME && vmin.len >= pack_prefix && vmax.len >= pack_prefix &&
          !types::RequiresUTFConversions(d.GetCollation())) {
        vmin += pack_prefix;  // redefine - shift by a common prefix
        vmax += pack_prefix;
        if (auto sp = GetFilter_CMap()) {
          res = sp->IsValue(vmin, vmax, pack);
          if (d.sharp && res == common::RSValue::RS_ALL) res = common::RSValue::RS_SOME;  // simplified version
        }

        vmin -= pack_prefix;
        vmax -= pack_prefix;
      }
      if (res == common::RSValue::RS_SOME) {
        if (auto sp = GetFilter_Bloom()) {
          res = sp->IsValue(vmin, vmax, pack);
        }
      }

      if (d.op == common::Operator::O_NOT_BETWEEN) {
        if (res == common::RSValue::RS_ALL)
          res = common::RSValue::RS_NONE;
        else if (res == common::RSValue::RS_NONE)
          res = common::RSValue::RS_ALL;
      }
      if ((dpn.nn != 0 || additional_nulls_possible) && res == common::RSValue::RS_ALL) {
        res = common::RSValue::RS_SOME;
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
        if (v1 == common::MINUS_INF_64) v1 = dpn.min_i;
        if (v2 == common::PLUS_INF_64) v2 = dpn.max_i;
      } else {
        if (v1 == *(int64_t *)&common::MINUS_INF_DBL) v1 = dpn.min_i;
        if (v2 == *(int64_t *)&common::PLUS_INF_DBL) v2 = dpn.max_i;
      }
      common::RSValue res =
          RoughCheckBetween(pack, v1,
                            v2);  // calculate as for common::Operator::O_BETWEEN and then consider negation
      if (d.op == common::Operator::O_NOT_BETWEEN) {
        if (res == common::RSValue::RS_ALL)
          res = common::RSValue::RS_NONE;
        else if (res == common::RSValue::RS_NONE) {
          res = common::RSValue::RS_ALL;
        }
      }
      if (res == common::RSValue::RS_ALL && (dpn.nn != 0 || additional_nulls_possible)) {
        res = common::RSValue::RS_SOME;
      }
      return res;
    }
  } else {
    if (!d.encoded) return common::RSValue::RS_SOME;
    vcolumn::SingleColumn *sc =
        (static_cast<int>(d.val1.vc->IsSingleColumn()) ? static_cast<vcolumn::SingleColumn *>(d.val1.vc) : NULL);
    RCAttr *sec = NULL;
    if (sc) sec = dynamic_cast<RCAttr *>(sc->GetPhysical());
    if (d.IsType_AttrAttr() && d.op != common::Operator::O_BETWEEN && d.op != common::Operator::O_NOT_BETWEEN && sec) {
      common::RSValue res = common::RSValue::RS_SOME;
      // special cases, not implemented yet:
      if ((TypeName() != sec->TypeName() &&  // Exceptions:
           !(ATI::IsDateTimeType(TypeName()) && ATI::IsDateTimeType(sec->TypeName())) &&
           !(ATI::IsIntegerType(TypeName()) && ATI::IsIntegerType(sec->TypeName()))) ||
          Type().GetScale() != sec->Type().GetScale() || Type().IsLookup() || sec->Type().IsLookup())
        return common::RSValue::RS_SOME;
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
              res = common::RSValue::RS_NONE;
            if (v1min > v2max) {
              if (d.op == common::Operator::O_EQ || d.op == common::Operator::O_NOT_EQ) res = common::RSValue::RS_NONE;
              if (d.op == common::Operator::O_MORE || d.op == common::Operator::O_LESS_EQ)
                res = common::RSValue::RS_ALL;
            }
          }
          if (v1max <= v2min) {
            if (d.op == common::Operator::O_MORE ||
                d.op == common::Operator::O_LESS_EQ)  // the second case will be negated soon
              res = common::RSValue::RS_NONE;
            if (v1max < v2min) {
              if (d.op == common::Operator::O_EQ || d.op == common::Operator::O_NOT_EQ) res = common::RSValue::RS_NONE;
              if (d.op == common::Operator::O_LESS || d.op == common::Operator::O_MORE_EQ)
                res = common::RSValue::RS_ALL;
            }
          }
          if (res == common::RSValue::RS_SOME &&
              (d.op == common::Operator::O_EQ ||
               d.op == common::Operator::O_NOT_EQ)) {  // the second case will be negated soon
            if (v1min == v1max && v2min == v2max) {
              if (v1min == v2min)
                res = common::RSValue::RS_ALL;
              else
                res = common::RSValue::RS_NONE;
            } else if (v1min == v1max) {
              auto sp2 = sec->GetFilter_Hist();
              res = sp2->IsValue(v1min, v1max, pack, v2min, v2max);
            } else if (v2min == v2max) {
              auto sp = GetFilter_Hist();
              res = sp->IsValue(v2min, v2max, pack, v1min, v1max);
            } else {
              auto sp = GetFilter_Hist();
              auto sp2 = sec->GetFilter_Hist();

              // check intersection possibility on histograms
              if (sp && sp2 && (sp->Intersection(pack, v1min, v1max, sp2.get(), pack, v2min, v2max) == false))
                res = common::RSValue::RS_NONE;
            }
          }
          // Now take into account all negations
          if (d.op == common::Operator::O_NOT_EQ || d.op == common::Operator::O_LESS_EQ ||
              d.op == common::Operator::O_MORE_EQ) {
            if (res == common::RSValue::RS_ALL)
              res = common::RSValue::RS_NONE;
            else if (res == common::RSValue::RS_NONE)
              res = common::RSValue::RS_ALL;
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
              res = common::RSValue::RS_NONE;
            if (CollationStrCmp(d.GetCollation(), v1min, v2max) > 0) {
              if (d.op == common::Operator::O_EQ || d.op == common::Operator::O_NOT_EQ) res = common::RSValue::RS_NONE;
              if (d.op == common::Operator::O_MORE || d.op == common::Operator::O_LESS_EQ)
                res = common::RSValue::RS_ALL;
            }
          }
          if (CollationStrCmp(d.GetCollation(), v1max, v2min) <= 0) {
            if (d.op == common::Operator::O_MORE ||
                d.op == common::Operator::O_LESS_EQ)  // the second case will be negated soon
              res = common::RSValue::RS_NONE;
            if (CollationStrCmp(d.GetCollation(), v1max, v2min) < 0) {
              if (d.op == common::Operator::O_EQ || d.op == common::Operator::O_NOT_EQ) res = common::RSValue::RS_NONE;
              if (d.op == common::Operator::O_LESS || d.op == common::Operator::O_MORE_EQ)
                res = common::RSValue::RS_ALL;
            }
          }
          if (d.op == common::Operator::O_NOT_EQ || d.op == common::Operator::O_LESS_EQ ||
              d.op == common::Operator::O_MORE_EQ) {
            if (res == common::RSValue::RS_ALL)
              res = common::RSValue::RS_NONE;
            else if (res == common::RSValue::RS_NONE)
              res = common::RSValue::RS_ALL;
          }
        }
      }
      // take nulls into account
      if ((dpn.nn != 0 || secDpn.nn != 0 || additional_nulls_possible) && res == common::RSValue::RS_ALL)
        res = common::RSValue::RS_SOME;
      return res;
    }
  }
  return common::RSValue::RS_SOME;
}

common::RSValue RCAttr::RoughCheck(int pack1, int pack2, Descriptor &d) {
  vcolumn::VirtualColumn *vc1 = d.val1.vc;
  vcolumn::VirtualColumn *vc2 = d.val2.vc;

  // Limitations for now: only the easiest numerical cases
  if (vc1 == NULL || vc2 != NULL || d.op != common::Operator::O_EQ || pack1 == -1 || pack2 == -1)
    return common::RSValue::RS_SOME;
  vcolumn::SingleColumn *sc = NULL;
  if (static_cast<int>(vc1->IsSingleColumn())) sc = static_cast<vcolumn::SingleColumn *>(vc1);
  if (sc == NULL) return common::RSValue::RS_SOME;
  RCAttr *sec = dynamic_cast<RCAttr *>(sc->GetPhysical());
  if (sec == NULL || !Type().IsNumComparable(sec->Type())) return common::RSValue::RS_SOME;

  LoadPackInfo();
  sec->LoadPackInfo();
  auto const &secDpn(sec->get_dpn(pack2));
  //	if(sec->Type().IsFloat())
  //		return RoughCheckBetween(pack1, *(double*)(&secDpn.min),
  //*(double*)(&secDpn.max));
  common::RSValue r = RoughCheckBetween(pack1, secDpn.min_i, secDpn.max_i);
  return r == common::RSValue::RS_ALL ? common::RSValue::RS_SOME : r;
}

// check whether any value from the pack may meet the condition "... BETWEEN min
// AND max"
common::RSValue RCAttr::RoughCheckBetween(int pack, int64_t v1, int64_t v2) {
  common::RSValue res = common::RSValue::RS_SOME;  // calculate as for common::Operator::O_BETWEEN
                                                   // and then consider negation
  bool is_float = Type().IsFloat();
  auto const &dpn(get_dpn(pack));
  if (!is_float && (v1 == common::PLUS_INF_64 || v2 == common::MINUS_INF_64)) {
    res = common::RSValue::RS_NONE;
  } else if (is_float && (v1 == *(int64_t *)&common::PLUS_INF_DBL || v2 == *(int64_t *)&common::MINUS_INF_DBL)) {
    res = common::RSValue::RS_NONE;
  } else if (!is_float && (v1 > dpn.max_i || v2 < dpn.min_i)) {
    res = common::RSValue::RS_NONE;
  } else if (is_float && (*(double *)&v1 > dpn.max_d || *(double *)&v2 < dpn.min_d)) {
    res = common::RSValue::RS_NONE;
  } else if (!is_float && (v1 <= dpn.min_i && v2 >= dpn.max_i)) {
    res = common::RSValue::RS_ALL;
  } else if (is_float && (*(double *)&v1 <= dpn.min_d && *(double *)&v2 >= dpn.max_d)) {
    res = common::RSValue::RS_ALL;
  } else if ((!is_float && v1 > v2) || (is_float && (*(double *)&v1 > *(double *)&v2))) {
    res = common::RSValue::RS_NONE;
  } else {
    if (auto sp = GetFilter_Hist()) res = sp->IsValue(v1, v2, pack, dpn.min_i, dpn.max_i);

    if (res == common::RSValue::RS_SOME) {
      if (auto sp = GetFilter_Bloom()) {
        auto vmin = std::to_string(v1);
        auto vmax = std::to_string(v2);
        types::BString keymin(vmin.c_str(), vmin.length());
        types::BString keymax(vmax.c_str(), vmax.length());
        res = sp->IsValue(keymin, keymax, pack);
      }
    }
  }
  if (dpn.nn != 0 && res == common::RSValue::RS_ALL) {
    res = common::RSValue::RS_SOME;
  }
  return res;
}

int64_t RCAttr::RoughMin(Filter *f, common::RSValue *rf)  // f == NULL is treated as full filter
{
  LoadPackInfo();
  if (GetPackType() == common::PackType::STR) return common::MINUS_INF_64;
  if (f && f->IsEmpty() && rf == NULL) return 0;
  int64_t res;
  if (ATI::IsRealType(TypeName())) {
    union {
      double d;
      int64_t i;
    } di;
    di.d = DBL_MAX;
    for (uint p = 0; p < NoPack(); p++) {  // minimum of nonempty packs
      auto const &dpn(get_dpn(p));
      if ((f == NULL || !f->IsEmpty(p)) && (rf == NULL || rf[p] != common::RSValue::RS_NONE)) {
        if (di.d > dpn.min_d) di.i = dpn.min_i;
      }
    }
    if (di.d == DBL_MAX) di.d = -(DBL_MAX);
    res = di.i;
  } else {
    res = common::PLUS_INF_64;
    for (uint p = 0; p < NoPack(); p++) {  // minimum of nonempty packs
      auto const &dpn(get_dpn(p));
      if ((f == NULL || !f->IsEmpty(p)) && (rf == NULL || rf[p] != common::RSValue::RS_NONE) && res > dpn.min_i)
        res = dpn.min_i;
    }
    if (res == common::PLUS_INF_64) res = common::MINUS_INF_64;  // NULLS only
  }
  return res;
}

int64_t RCAttr::RoughMax(Filter *f, common::RSValue *rf)  // f == NULL is treated as full filter
{
  LoadPackInfo();
  if (GetPackType() == common::PackType::STR) return common::PLUS_INF_64;
  if (f && f->IsEmpty() && rf == NULL) return 0;
  int64_t res;
  if (ATI::IsRealType(TypeName())) {
    union {
      double d;
      int64_t i;
    } di;
    di.d = -(DBL_MAX);
    for (uint p = 0; p < NoPack(); p++) {  // minimum of nonempty packs
      auto const &dpn(get_dpn(p));
      if ((f == NULL || !f->IsEmpty(p)) && (rf == NULL || rf[p] != common::RSValue::RS_NONE)) {
        if (di.d < dpn.max_d) di.i = dpn.max_i;
      }
    }
    if (di.d == -(DBL_MAX)) di.d = DBL_MAX;
    res = di.i;
  } else {
    res = common::MINUS_INF_64;
    for (uint p = 0; p < NoPack(); p++) {  // maximum of nonempty packs
      auto const &dpn(get_dpn(p));
      if ((f == NULL || !f->IsEmpty(p)) && (rf == NULL || rf[p] != common::RSValue::RS_NONE) && res < dpn.max_i)
        res = dpn.max_i;
    }
    if (res == common::MINUS_INF_64) res = common::PLUS_INF_64;  // NULLS only
  }
  return res;
}

std::vector<int64_t> RCAttr::GetListOfDistinctValuesInPack(int pack) {
  std::vector<int64_t> list_vals;
  if (GetPackType() != common::PackType::INT || pack == -1 ||
      (Type().IsLookup() && types::RequiresUTFConversions(GetCollation())))
    return list_vals;
  auto const &dpn(get_dpn(pack));
  if (dpn.min_i == dpn.max_i) {
    list_vals.push_back(dpn.min_i);
    if (NumOfNulls() > 0) list_vals.push_back(common::NULL_VALUE_64);
    return list_vals;
  } else if (GetPackOntologicalStatus(pack) == PackOntologicalStatus::NULLS_ONLY) {
    list_vals.push_back(common::NULL_VALUE_64);
    return list_vals;
  } else if (TypeName() == common::CT::REAL || TypeName() == common::CT::FLOAT) {
    return list_vals;
  } else if (dpn.max_i - dpn.min_i > 0 && dpn.max_i - dpn.min_i < 1024) {
    auto sp = GetFilter_Hist();
    if (!sp || !sp->ExactMode(dpn.min_i, dpn.max_i)) {
      return list_vals;
    }
    list_vals.push_back(dpn.min_i);
    list_vals.push_back(dpn.max_i);
    for (int64_t v = dpn.min_i + 1; v < dpn.max_i; v++) {
      if (sp->IsValue(v, v, pack, dpn.min_i, dpn.max_i) != common::RSValue::RS_NONE) list_vals.push_back(v);
    }
    if (NumOfNulls() > 0) list_vals.push_back(common::NULL_VALUE_64);
    return list_vals;
  }
  return list_vals;
}

uint64_t RCAttr::ApproxDistinctVals(bool incl_nulls, Filter *f, common::RSValue *rf, bool outer_nulls_possible) {
  LoadPackInfo();
  uint64_t no_dist = 0;
  int64_t max_obj = NumOfObj();  // no more values than objects
  if (NumOfNulls() > 0 || outer_nulls_possible) {
    if (incl_nulls) no_dist++;  // one value for null
    max_obj = max_obj - NumOfNulls() + (incl_nulls ? 1 : 0);
    if (f && max_obj > f->NoOnes()) max_obj = f->NoOnes();
  } else if (f)
    max_obj = f->NoOnes();
  if (TypeName() == common::CT::DATE) {
    try {
      types::RCDateTime date_min(RoughMin(f, rf), common::CT::DATE);
      types::RCDateTime date_max(RoughMax(f, rf), common::CT::DATE);
      no_dist += (date_max - date_min) + 1;  // overloaded minus - a number of days between dates
    } catch (...) {                          // in case of any problems with conversion of dates - just
                                             // numerical approximation
      no_dist += RoughMax(f, rf) - RoughMin(f, rf) + 1;
    }
  } else if (TypeName() == common::CT::YEAR) {
    try {
      types::RCDateTime date_min(RoughMin(f, rf), common::CT::YEAR);
      types::RCDateTime date_max(RoughMax(f, rf), common::CT::YEAR);
      no_dist += ((int)(date_max.Year()) - date_min.Year()) + 1;
    } catch (...) {  // in case of any problems with conversion of dates - just
                     // numerical approximation
      no_dist += RoughMax(f, rf) - RoughMin(f, rf) + 1;
    }
  } else if (GetPackType() == common::PackType::INT && TypeName() != common::CT::REAL &&
             TypeName() != common::CT::FLOAT) {
    int64_t cur_min = RoughMin(f, rf);  // extrema of nonempty packs
    int64_t cur_max = RoughMax(f, rf);
    uint64_t span = cur_max - cur_min + 1;
    if (span < 100000) {  // use Histograms to calculate exact number of
                          // distinct values
      Filter values_present(span, pss);
      values_present.Reset();
      for (uint p = 0; p < NoPack(); p++) {
        auto const &dpn(get_dpn(p));
        if ((f == NULL || !f->IsEmpty(p)) && (rf == NULL || rf[p] != common::RSValue::RS_NONE) &&
            dpn.min_i <= dpn.max_i) {
          // dpn.min <= dpn.max is not true e.g. when the pack contains nulls
          // only
          if (auto sp = GetFilter_Hist(); sp && sp->ExactMode(dpn.min_i, dpn.max_i)) {
            values_present.Set(dpn.min_i - cur_min);
            values_present.Set(dpn.max_i - cur_min);
            for (int64_t v = dpn.min_i + 1; v < dpn.max_i; v++)
              if (sp->IsValue(v, v, p, dpn.min_i, dpn.max_i) != common::RSValue::RS_NONE) {
                values_present.Set(v - cur_min);
              }
          } else {  // no Histogram or not exact: mark the whole interval
            values_present.SetBetween(dpn.min_i - cur_min, dpn.max_i - cur_min);
          }
        }
        if (values_present.IsFull()) break;
      }
      no_dist += values_present.NoOnes();
    } else
      no_dist += span;  // span between min and max
  } else if (TypeName() == common::CT::REAL || TypeName() == common::CT::FLOAT) {
    int64_t cur_min = RoughMin(f, rf);  // extrema of nonempty packs
    int64_t cur_max = RoughMax(f, rf);
    if (cur_min == cur_max && cur_min != common::NULL_VALUE_64)  // the only case we can do anything
      no_dist += 1;
    else
      no_dist = max_obj;
  } else if (TypeName() == common::CT::STRING || TypeName() == common::CT::VARCHAR ||
             TypeName() == common::CT::LONGTEXT) {
    size_t max_len = 0;
    for (uint p = 0; p < NoPack(); p++) {  // max len of nonempty packs
      if (f == NULL || !f->IsEmpty(p)) max_len = std::max(max_len, GetActualSize(p));
    }
    if (max_len > 0 && max_len < 6)
      no_dist += int64_t(256) << ((max_len - 1) * 8);
    else if (max_len > 0)
      no_dist = max_obj;  // default
    else if (max_len == 0 && max_obj)
      no_dist++;
  } else
    no_dist = max_obj;  // default

  if (no_dist > (uint64_t)max_obj) return max_obj;
  return no_dist;
}

uint64_t RCAttr::ExactDistinctVals(Filter *f)  // provide the exact number of diff. non-null values, if
                                               // possible, or common::NULL_VALUE_64
{
  if (f == NULL)  // no exact information about tuples => nothing can be
                  // determined for sure
    return common::NULL_VALUE_64;
  LoadPackInfo();
  if (Type().IsLookup() && !types::RequiresUTFConversions(GetCollation()) && f->IsFull()) return RoughMax(NULL) + 1;
  bool nulls_only = true;
  for (uint p = 0; p < NoPack(); p++)
    if (!f->IsEmpty(p) && GetPackOntologicalStatus(p) != PackOntologicalStatus::NULLS_ONLY) {
      nulls_only = false;
      break;
    }
  if (nulls_only) return 0;
  if (GetPackType() == common::PackType::INT && !types::RequiresUTFConversions(GetCollation()) &&
      TypeName() != common::CT::REAL && TypeName() != common::CT::FLOAT) {
    int64_t cur_min = RoughMin(f);  // extrema of nonempty packs
    int64_t cur_max = RoughMax(f);
    uint64_t span = cur_max - cur_min + 1;
    if (span < 100000) {  // use Histograms to calculate exact number of
                          // distinct values
      Filter values_present(span, NumOfPackpower());
      values_present.Reset();
      // Phase 1: mark all values, which are present for sure
      for (uint p = 0; p < NoPack(); p++)
        if (f->IsFull(p)) {
          auto const &dpn(get_dpn(p));
          if (dpn.min_i < dpn.max_i) {
            // dpn.min <= dpn.max is not true when the pack contains nulls
            // only
            if (auto sp = GetFilter_Hist(); sp && sp->ExactMode(dpn.min_i, dpn.max_i)) {
              values_present.Set(dpn.min_i - cur_min);
              values_present.Set(dpn.max_i - cur_min);
              for (int64_t v = dpn.min_i + 1; v < dpn.max_i; v++)
                if (sp->IsValue(v, v, p, dpn.min_i, dpn.max_i) != common::RSValue::RS_NONE) {
                  values_present.Set(v - cur_min);
                }
            } else {  // no Histogram or not exact: cannot calculate exact
                      // number
              return common::NULL_VALUE_64;
            }
          } else if (dpn.min_i == dpn.max_i) {  // only one value
            values_present.Set(dpn.min_i - cur_min);
          }
          if (values_present.IsFull()) break;
        }
      // Phase 2: check whether there are any other values possible in suspected
      // packs
      for (uint p = 0; p < NoPack(); p++) {
        if (!f->IsEmpty(p) && !f->IsFull(p)) {  // suspected pack
          auto const &dpn(get_dpn(p));
          if (!values_present.IsFullBetween(dpn.min_i - cur_min, dpn.max_i - cur_min)) {
            return common::NULL_VALUE_64;
          }
        }
      }
      return values_present.NoOnes();
    }
  }
  return common::NULL_VALUE_64;
}

double RCAttr::RoughSelectivity() {
  if (rough_selectivity == -1) {
    LoadPackInfo();
    if (GetPackType() == common::PackType::INT && TypeName() != common::CT::REAL && TypeName() != common::CT::FLOAT &&
        NoPack() > 0) {
      int64_t global_min = common::PLUS_INF_64;
      int64_t global_max = common::MINUS_INF_64;
      double width_sum = 0;
      for (uint p = 0; p < NoPack(); p++) {  // minimum of nonempty packs
        auto const &dpn(get_dpn(p));
        if (dpn.nn == uint(dpn.nr) + 1) continue;
        if (dpn.min_i < global_min) global_min = dpn.min_i;
        if (dpn.max_i > global_max) global_max = dpn.max_i;
        width_sum += double(dpn.max_i) - double(dpn.min_i) + 1;
      }
      rough_selectivity = (width_sum / NoPack()) / (double(global_max) - double(global_min) + 1);
    } else
      rough_selectivity = 1;
  }
  return rough_selectivity;
}

void RCAttr::GetTextStat(types::TextStat &s, Filter *f) {
  bool success = false;
  LoadPackInfo();
  if (GetPackType() == common::PackType::STR && !types::RequiresUTFConversions(GetCollation())) {
    if (auto sp = GetFilter_CMap()) {
      success = true;
      for (uint p = 0; p < NoPack(); p++)
        if (f == NULL || !f->IsEmpty(p)) {
          auto const &dpn(get_dpn(p));
          if (dpn.NullOnly()) continue;
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
              if (sp->IsSet(p, c, i - pack_prefix)) s.AddChar(c, i);
          }
          s.AddLen(len);
          if (p % 16 == 15)  // check it from time to time
            s.CheckIfCreatePossible();
          if (!s.IsValid()) break;
        }
    }
  }
  if (success == false) s.Invalidate();
}

// calculate the number of 1's in histograms and other KN stats
void RCAttr::RoughStats(double &hist_density, int &trivial_packs, double &span) {
  uint npack = NoPack();
  hist_density = -1;
  trivial_packs = 0;
  span = -1;
  LoadPackInfo();
  for (uint pack = 0; pack < npack; pack++) {
    auto const &dpn(get_dpn(pack));
    if (dpn.NullOnly() || (GetPackType() == common::PackType::INT && dpn.nn == 0 && dpn.min_i == dpn.max_i))
      trivial_packs++;
    else {
      if (GetPackType() == common::PackType::INT && !ATI::IsRealType(TypeName())) {
        if (span == -1) span = 0;
        span += dpn.max_i - dpn.min_i;
      }
      if (GetPackType() == common::PackType::INT && ATI::IsRealType(TypeName())) {
        if (span == -1) span = 0;
        span += dpn.max_d - dpn.min_d;
      }
    }
  }
  if (span != -1) {
    int64_t tmp_min = GetMinInt64();  // always a value - i_min from RCAttr
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
      if (uint(dpn.nr + 1) != dpn.nn && dpn.min_i + 1 < dpn.max_i) {
        int loc_no_ones;
        if (dpn.max_i - dpn.min_i > 1024)
          loc_no_ones = 1024;
        else
          loc_no_ones = int(dpn.max_i - dpn.min_i - 1);
        ones_needed += loc_no_ones;
        ones_found += sp->Count(pack, loc_no_ones);
      }
    }
    if (ones_needed > 0) hist_density = ones_found / double(ones_needed);
  }
}

void RCAttr::DisplayAttrStats(Filter *f)  // filter is for # of objects
{
  int npack = NoPack();
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
      cur_obj = f->NoOnes(pack);
    else if (pack == npack - 1)
      cur_obj = dpn.nr + 1;

    std::sprintf(line_buf, "%-7d %5ld %5d", pack, cur_obj, dpn.nn);

    if (uint(dpn.nr + 1) != dpn.nn) {
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
        if (rsi_span >= 0) std::sprintf(line_buf + std::strlen(line_buf), "  %d/%d", rsi_ones, rsi_span);
        if (!GetFilter_Hist()) std::sprintf(line_buf + std::strlen(line_buf), "   n/a");
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
          uint len = sp->NoPositions();
          uint i;
          for (i = 0; i < len && i < 8; i++) {  // display at most 8 positions (limited by line width)
            if ((i == len - 1) || (i == 7))
              std::sprintf(line_buf + std::strlen(line_buf), "%d", sp->Count(pack, i));
            else
              std::sprintf(line_buf + std::strlen(line_buf), "%d-", sp->Count(pack, i));
          }
          if (i < len) std::sprintf(line_buf + std::strlen(line_buf), "...");
        } else
          std::sprintf(line_buf + std::strlen(line_buf), "n/a");
      }
    }
    ss << line_buf << std::endl;
  }
  ss << "----------------------------------------------------------------------"
        "---------"
     << std::endl;
  STONEDB_LOG(LogCtl_Level::DEBUG, ss.str().c_str());
}
}  // namespace core
}  // namespace stonedb
