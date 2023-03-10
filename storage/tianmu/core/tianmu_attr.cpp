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
#include <numeric>
#include <optional>

#include "common/common_definitions.h"
#include "common/data_format.h"
#include "core/dpn.h"
#include "core/pack_int.h"
#include "core/pack_str.h"
#include "core/tianmu_attr.h"
#include "core/tianmu_attr_typeinfo.h"
#include "core/tools.h"
#include "core/transaction.h"
#include "core/value.h"
#include "loader/load_parser.h"
#include "loader/value_cache.h"
#include "system/fet.h"
#include "system/tianmu_file.h"
#include "types/tianmu_data_types.h"
#include "util/fs.h"
#include "util/log_ctl.h"

namespace Tianmu {
namespace core {
TianmuAttr::TianmuAttr(Transaction *tx, common::TX_ID xid, int a_num, int t_num, ColumnShare *share)
    : m_version(xid), m_tx(tx), m_tid(t_num), m_cid(a_num), m_share(share) {
  m_coord.ID = COORD_TYPE::kTianmuAttr;
  m_coord.co.rcattr[0] = m_tid;
  m_coord.co.rcattr[1] = m_cid;

  pss = m_share->pss;
  ct = m_share->ColType();
  pack_type = m_share->pt;
  LoadVersion(m_version);

  filter_creator = [this](const FilterCoordinate &co) -> std::shared_ptr<RSIndex> {
    auto t = static_cast<FilterType>(co[2]);
    common::TX_ID v(co[3], co[4]);

    switch (t) {
      case FilterType::CMAP:
        return std::make_shared<RSIndex_CMap>(Path() / common::COL_FILTER_DIR, v);
      case FilterType::HIST:
        return std::make_shared<RSIndex_Hist>(Path() / common::COL_FILTER_DIR, v);
      case FilterType::BLOOM:
        return std::make_shared<RSIndex_Bloom>(Path() / common::COL_FILTER_DIR, v);
      default:
        TIANMU_ERROR("bad type");
    }
  };
}

void TianmuAttr::Create(const fs::path &dir, const AttributeTypeInfo &ati, uint8_t pss, size_t no_rows,
                        uint64_t auto_inc_value) {
  uint32_t no_pack = common::rows2packs(no_rows, pss);

  // write meta data(immutable)
  COL_META meta{
      common::COL_FILE_MAGIC,    // file magic
      common::COL_FILE_VERSION,  // version
      pss,                       // pack size shift
      ati.Type(),                // attribute type
      ati.Fmt(),                 // format
      ati.Flag(),                // flag
      ati.Precision(),           // precision
      ati.Scale(),               // scale
  };

  system::TianmuFile fmeta;
  fmeta.OpenCreateEmpty(dir / common::COL_META_FILE);
  fmeta.WriteExact(&meta, sizeof(meta));
  fmeta.Flush();

  COL_VER_HDR hdr{
      no_rows,  // no_obj
      no_rows,  // no_nulls
      no_pack,  // no of packs
      0,        // no of deleted
      0,        // auto_inc next
      0,        // min
      0,        // max
      0,        // dict file version name. 0 means n/a
      0,        // is unique?
      0,        // is unique_updated?
      0,        // natural size
      0,        // compressed size
  };

  if (ati.Lookup()) {
    hdr.dict_ver = 1;  // starting with 1 because 0 means n/a

    fs::create_directory(dir / common::COL_DICT_DIR);
    auto dict = std::make_unique<FTree>();
    // TODO: if there is default value, we should add it into dictionary
    dict->Init(ati.Precision());
    dict->SaveData(dir / common::COL_DICT_DIR / std::to_string(1));
  }

  // auto_increment
  if (ati.AutoInc() && auto_inc_value != 0) {
    hdr.auto_inc = --auto_inc_value;
  }

  // create version directory
  fs::create_directory(dir / common::COL_VERSION_DIR);

  system::TianmuFile fattr;
  fattr.OpenCreateEmpty(dir / common::COL_VERSION_DIR / common::TX_ID(0).ToString());
  fattr.WriteExact(&hdr, sizeof(hdr));

  // write index
  for (common::PACK_INDEX i = 0; i < no_pack; i++) fattr.WriteExact(&i, sizeof(i));
  fattr.Flush();

  if (no_rows > 0) {
    // all DPNs are null-only
    DPN dpn;
    dpn.reset();
    dpn.used = 1;
    dpn.numOfNulls = 1 << pss;
    dpn.numOfRecords = 1 << pss;
    dpn.xmax = common::MAX_XID;
    dpn.dataAddress = DPN_INVALID_ADDR;

    system::TianmuFile fdn;
    fdn.OpenCreateEmpty(dir / common::COL_DN_FILE);
    for (common::PACK_INDEX i = 0; i < no_pack - 1; i++) fdn.WriteExact(&dpn, sizeof(dpn));

    // the last one
    auto left = no_rows % (1 << pss);
    if (left != 0) {
      dpn.numOfRecords = left;
      dpn.numOfNulls = left;
    }
    fdn.WriteExact(&dpn, sizeof(dpn));
    fdn.Flush();
    fs::resize_file(dir / common::COL_DN_FILE, common::COL_DN_FILE_SIZE);
  }

  // create filter directories
  fs::create_directory(dir / common::COL_FILTER_DIR);
  fs::create_directory(dir / common::COL_FILTER_DIR / common::COL_FILTER_BLOOM_DIR);
  fs::create_directory(dir / common::COL_FILTER_DIR / common::COL_FILTER_CMAP_DIR);
  fs::create_directory(dir / common::COL_FILTER_DIR / common::COL_FILTER_HIST_DIR);
}

void TianmuAttr::LoadVersion(common::TX_ID xid) {
  auto fname = Path() / common::COL_VERSION_DIR / xid.ToString();
  system::TianmuFile fattr;
  fattr.OpenReadOnly(fname);
  fattr.ReadExact(&hdr, sizeof(hdr));

  SetUnique(hdr.unique);
  SetUniqueUpdated(hdr.unique_updated);

  if (hdr.dict_ver != 0) {
    m_dict = ha_tianmu_engine_->cache.GetOrFetchObject<FTree>(FTreeCoordinate(m_tid, m_cid, hdr.dict_ver), this);
  }
  m_idx.resize(hdr.numOfPacks);
  fattr.ReadExact(&m_idx[0], sizeof(common::PACK_INDEX) * hdr.numOfPacks);
}

void TianmuAttr::Truncate() {
  no_change = false;
  hdr = {};
  m_share->Truncate();
  if (ct.Lookup()) {
    hdr.dict_ver = 1;  // starting with 1 because 0 means n/a
    auto dict = std::make_unique<FTree>();
    dict->Init(ct.GetPrecision());
    dict->SaveData(Path() / common::COL_DICT_DIR / std::to_string(1));
  }
  m_idx.clear();
}

size_t TianmuAttr::ComputeNaturalSize() {
  size_t na_size = (Type().NotNull() ? 0 : 1) * NumOfObj() / 8;

  switch (TypeName()) {
    case common::ColumnType::STRING:
    case common::ColumnType::BYTE:
    case common::ColumnType::DATE:
      na_size += Type().GetPrecision() * NumOfObj();
      break;
    case common::ColumnType::TIME:
    case common::ColumnType::YEAR:
    case common::ColumnType::DATETIME:
    case common::ColumnType::TIMESTAMP:
      na_size += Type().GetDisplaySize() * NumOfObj();
      break;
    case common::ColumnType::NUM:
      na_size += (Type().GetPrecision() + (Type().GetScale() ? 1 : 0)) * NumOfObj();
      break;
    case common::ColumnType::BIT:
      na_size += Type().GetPrecision() * NumOfObj();  // one bytes one digit
      break;
    case common::ColumnType::BIGINT:
    case common::ColumnType::REAL:
      na_size += 8 * NumOfObj();
      break;
    case common::ColumnType::FLOAT:
    case common::ColumnType::INT:
      na_size += 4 * NumOfObj();
      break;
    case common::ColumnType::MEDIUMINT:
      na_size += 3 * NumOfObj();
      break;
    case common::ColumnType::SMALLINT:
      na_size += 2 * NumOfObj();
      break;
    case common::ColumnType::BYTEINT:
      na_size += 1 * NumOfObj();
      break;
    case common::ColumnType::VARCHAR:
      na_size += hdr.natural_size;
      break;
    case common::ColumnType::LONGTEXT:
      na_size += hdr.natural_size;
      break;
    case common::ColumnType::VARBYTE:
    case common::ColumnType::BIN:
      na_size += hdr.natural_size;
      break;
    default:
      break;
  }
  return na_size;
}

void TianmuAttr::SaveFilters() {
  if (filter_hist) {
    filter_hist->SaveToFile(m_tx->GetID());
    filter_hist.reset();
  }

  if (filter_cmap) {
    filter_cmap->SaveToFile(m_tx->GetID());
    filter_cmap.reset();
  }

  if (filter_bloom) {
    filter_bloom->SaveToFile(m_tx->GetID());
    filter_bloom.reset();
  }
}

// Save all modified data (pack, filter, dictionary, etc) to disk.
// This is basically the PREPARE phase of COMMIT.
bool TianmuAttr::SaveVersion() {
  ASSERT(m_tx != nullptr, "Attempt to modify table in read-only transaction");

  // save modified pack data
  for (size_t i = 0; i < m_idx.size(); i++) {
    auto &dpn = get_dpn(i);
    if (dpn.IsLocal()) {
      no_change = false;
      RefreshFilter(i);
      auto &dpn(get_dpn(i));
      if (dpn.Trivial() || dpn.synced) {
        // trivial or already saved to disk
        if (auto p = get_pack(i); p != nullptr) {
          p->Unlock();
          ha_tianmu_engine_->cache.DropObject(get_pc(i));
          dpn.SetPackPtr(0);
        }
        continue;
      }

      get_pack(i)->Save();
      get_pack(i)->Unlock();  // now it can be released by MM
      dpn.SetPackPtr(0);
    }
  }

  if (no_change)
    return false;

  // truncated table?
  if (!m_idx.empty()) {
    SaveFilters();

    // save dictionary if modified
    if (m_dict && m_dict->Changed()) {
      m_dict->SaveData(Path() / common::COL_DICT_DIR / std::to_string(hdr.dict_ver));
    }
    hdr.auto_inc = m_share->auto_inc_.load();
    hdr.unique = IsUnique();
    hdr.unique_updated = IsUniqueUpdated();
    hdr.numOfPacks = m_idx.size();
    hdr.compressed_size = std::accumulate(m_idx.begin(), m_idx.end(), size_t(0), [this](size_t sum, auto &pi) {
      auto dpn = m_share->get_dpn_ptr(pi);
      if (dpn->dataAddress != DPN_INVALID_ADDR)
        return sum + dpn->dataLength;
      else
        return sum;
    });
  }

  // save attr transaction version
  auto fname = Path() / common::COL_VERSION_DIR / m_tx->GetID().ToString();
  system::TianmuFile fattr;
  fattr.OpenCreate(fname);
  fattr.WriteExact(&hdr, sizeof(hdr));
  fattr.WriteExact(&m_idx[0], sizeof(decltype(m_idx)::value_type) * hdr.numOfPacks);

  if (tianmu_sysvar_sync_buffers)
    fattr.Flush();

  return true;
}

void TianmuAttr::PostCommit() {
  if (!no_change) {
    for (size_t i = 0; i < m_idx.size(); i++) {
      auto &dpn = get_dpn(i);
      if (dpn.IsLocal()) {
        dpn.SetLocal(false);
        if (dpn.base != common::INVALID_PACK_INDEX)
          m_share->get_dpn_ptr(dpn.base)->xmax = ha_tianmu_engine_->MaxXID();
      }
    }

    ha_tianmu_engine_->DeferRemove(Path() / common::COL_VERSION_DIR / m_version.ToString(), m_tid);
    if (m_share->has_filter_bloom)
      ha_tianmu_engine_->DeferRemove(
          Path() / common::COL_FILTER_DIR / common::COL_FILTER_BLOOM_DIR / m_version.ToString(), m_tid);
    if (m_share->has_filter_cmap)
      ha_tianmu_engine_->DeferRemove(
          Path() / common::COL_FILTER_DIR / common::COL_FILTER_CMAP_DIR / m_version.ToString(), m_tid);
    if (m_share->has_filter_hist)
      ha_tianmu_engine_->DeferRemove(
          Path() / common::COL_FILTER_DIR / common::COL_FILTER_HIST_DIR / m_version.ToString(), m_tid);

    m_version = m_tx->GetID();
  }
  m_tx = nullptr;
}

void TianmuAttr::Rollback() {
  for (size_t i = 0; i < m_idx.size(); i++) {
    auto &dpn(get_dpn(i));
    if (dpn.IsLocal()) {
      ha_tianmu_engine_->cache.DropObject(get_pc(i));
      dpn.reset();
    }
  }
  m_tx = nullptr;
}

void TianmuAttr::LoadPackInfo([[maybe_unused]] Transaction *trans_) {
  if (hdr.dict_ver != 0 && !m_dict) {
    m_dict = ha_tianmu_engine_->cache.GetOrFetchObject<FTree>(FTreeCoordinate(m_tid, m_cid, hdr.dict_ver), this);
  }
}

PackOntologicalStatus TianmuAttr::GetPackOntologicalStatus(int pack_no) {
  LoadPackInfo();
  DPN const *dpn(pack_no >= 0 ? &get_dpn(pack_no) : nullptr);
  if (pack_no < 0 || dpn->NullOnly())
    return PackOntologicalStatus::NULLS_ONLY;
  if (GetPackType() == common::PackType::INT) {
    if (dpn->min_i == dpn->max_i) {
      if (dpn->numOfNulls == 0)
        return PackOntologicalStatus::UNIFORM;
      return PackOntologicalStatus::UNIFORM_AND_NULLS;
    }
  }
  return PackOntologicalStatus::NORMAL;
}

types::BString TianmuAttr::GetValueString(const int64_t obj) {
  if (obj == common::NULL_VALUE_64)
    return types::BString();
  int pack = row2pack(obj);
  int offset = row2offset(obj);

  if (GetPackType() == common::PackType::STR) {
    auto const &dpn(get_dpn(pack));
    if (dpn.Trivial())  // no pack data
      return types::BString();
    DEBUG_ASSERT(get_pack(pack)->IsLocked());
    auto cur_pack = get_packS(pack);

    return cur_pack->GetValueBinary(offset);
  }
  int64_t v = GetValueInt64(obj);
  return DecodeValue_S(v);
}

types::BString TianmuAttr::GetNotNullValueString(const int64_t obj) {
  int pack = row2pack(obj);
  int offset = row2offset(obj);

  if (GetPackType() == common::PackType::STR) {
    auto cur_pack = get_packS(pack);
    ASSERT(cur_pack != nullptr, "Pack ptr is null");
    ASSERT(cur_pack->IsLocked(), "Access unlocked pack");
    return cur_pack->GetValueBinary(offset);
  }
  int64_t v = GetNotNullValueInt64(obj);
  return DecodeValue_S(v);
}

// original 0-level value (text, std::string, date, time etc.)
void TianmuAttr::GetValueBin(int64_t obj, size_t &size, char *val_buf) {
  if (obj == common::NULL_VALUE_64)
    return;
  common::ColumnType a_type = TypeName();
  size = 0;
  DEBUG_ASSERT(NumOfObj() >= static_cast<uint64_t>(obj));
  LoadPackInfo();
  int pack = row2pack(obj);
  int offset = row2offset(obj);
  auto const &dpn(get_dpn(pack));
  if (dpn.NullOnly())
    return;
  if (ATI::IsStringType(a_type)) {
    if (GetPackType() == common::PackType::INT) {
      int64_t res = GetValueInt64(obj);
      if (res == common::NULL_VALUE_64)
        return;
      size = m_dict->ValueSize((int)res);
      std::memcpy(val_buf, m_dict->GetBuffer((int)res), size);
      return;
    } else {  // no dictionary
      if (dpn.Trivial())
        return;
      auto p = get_packS(pack);
      DEBUG_ASSERT(p->IsLocked());
      types::BString v(p->GetValueBinary(offset));
      size = v.size();
      v.CopyTo(val_buf, size);
      return;
    }
  } else if (ATI::IsInteger32Type(a_type)) {
    size = 4;
    int64_t v = GetValueInt64(obj);
    if (v == common::NULL_VALUE_64)
      return;
    *(int *)val_buf = int(v);
    val_buf[4] = 0;
    return;
  } else if (a_type == common::ColumnType::NUM || a_type == common::ColumnType::BIGINT || ATI::IsRealType(a_type) ||
             ATI::IsDateTimeType(a_type) || a_type == common::ColumnType::BIT) {
    size = 8;
    int64_t v = GetValueInt64(obj);
    if (v == common::NULL_VALUE_64)
      return;
    *(int64_t *)(val_buf) = v;
    val_buf[8] = 0;
    return;
  }
  return;
}

types::TianmuValueObject TianmuAttr::GetValue(int64_t obj, bool lookup_to_num) {
  if (obj == common::NULL_VALUE_64)
    return types::TianmuValueObject();
  common::ColumnType a_type = TypeName();
  DEBUG_ASSERT(NumOfObj() >= static_cast<uint64_t>(obj));
  types::TianmuValueObject ret;
  if (!IsNull(obj)) {
    if (ATI::IsTxtType(a_type) && !lookup_to_num)
      ret = GetNotNullValueString(obj);
    else if (ATI::IsBinType(a_type)) {
      auto tmp_size = GetLength(obj);
      types::BString tianmu_s(nullptr, tmp_size, true);
      GetValueBin(obj, tmp_size, tianmu_s.val_);
      tianmu_s.null_ = false;
      ret = tianmu_s;
    } else if (ATI::IsIntegerType(a_type))
      ret = types::TianmuNum(GetNotNullValueInt64(obj), -1, false, a_type);
    else if (a_type == common::ColumnType::TIMESTAMP) {
      // needs to convert UTC/GMT time stored on server to time zone of client
      types::BString s = GetValueString(obj);
      MYSQL_TIME myt;
      MYSQL_TIME_STATUS not_used;
      // convert UTC timestamp given in string into TIME structure
      str_to_datetime(s.GetDataBytesPointer(), s.len_, &myt, TIME_DATETIME_ONLY, &not_used);
      return types::TianmuDateTime(myt, common::ColumnType::TIMESTAMP);
    } else if (ATI::IsDateTimeType(a_type))
      ret = types::TianmuDateTime(this->GetNotNullValueInt64(obj), a_type);
    else if (ATI::IsRealType(a_type))
      ret = types::TianmuNum(this->GetNotNullValueInt64(obj), 0, true, a_type);
    else if (lookup_to_num || a_type == common::ColumnType::NUM || a_type == common::ColumnType::BIT)
      ret = types::TianmuNum((int64_t)GetNotNullValueInt64(obj), Type().GetScale());
  }
  return ret;
}

types::TianmuDataType &TianmuAttr::GetValueData(size_t obj, types::TianmuDataType &value, bool lookup_to_num) {
  if (obj == size_t(common::NULL_VALUE_64) || IsNull(obj))
    value = ValuePrototype(lookup_to_num);
  else {
    common::ColumnType a_type = TypeName();
    DEBUG_ASSERT(NumOfObj() >= static_cast<uint64_t>(obj));
    if (ATI::IsTxtType(a_type) && !lookup_to_num)
      ((types::BString &)value) = GetNotNullValueString(obj);
    else if (ATI::IsBinType(a_type)) {
      auto tmp_size = GetLength(obj);
      ((types::BString &)value) = types::BString(nullptr, tmp_size, true);
      GetValueBin(obj, tmp_size, ((types::BString &)value).val_);
      value.null_ = false;
    } else if (ATI::IsIntegerType(a_type))
      ((types::TianmuNum &)value).Assign(GetNotNullValueInt64(obj), -1, false, a_type);
    else if (ATI::IsDateTimeType(a_type)) {
      ((types::TianmuDateTime &)value) = types::TianmuDateTime(this->GetNotNullValueInt64(obj), a_type);
    } else if (ATI::IsRealType(a_type))
      ((types::TianmuNum &)value).Assign(this->GetNotNullValueInt64(obj), 0, true, a_type);
    else if (ATI::IsBitType(a_type)) {
      ((types::TianmuNum &)value).Assign(this->GetNotNullValueInt64(obj), Type().GetScale(), false, a_type);
    } else
      ((types::TianmuNum &)value).Assign(this->GetNotNullValueInt64(obj), Type().GetScale());
  }
  return value;
}

int64_t TianmuAttr::GetNumOfNulls(int pack) {
  LoadPackInfo();
  if (pack == -1)
    return NumOfNulls();
  return get_dpn(pack).numOfNulls;
}

size_t TianmuAttr::GetActualSize(int pack) {
  if (GetPackOntologicalStatus(pack) == PackOntologicalStatus::NULLS_ONLY)
    return 0;
  if (Type().Lookup() || GetPackType() != common::PackType::STR)
    return Type().GetPrecision();
  return get_dpn(pack).sum_i;
}

int64_t TianmuAttr::GetSum(int pack, bool &nonnegative) {
  LoadPackInfo();
  auto const &dpn(get_dpn(pack));
  if (GetPackOntologicalStatus(pack) == PackOntologicalStatus::NULLS_ONLY ||
      /* dpns.Size() == 0 || */ Type().IsString())
    return common::NULL_VALUE_64;
  if (!Type().IsFloat() &&
      (dpn.min_i < (common::MINUS_INF_64 / (SHORT_MAX + 1)) || dpn.max_i > (common::PLUS_INF_64 / (SHORT_MAX + 1))))
    return common::NULL_VALUE_64;  // conservative overflow test for
                                   // int/decimals
  nonnegative = (dpn.min_i >= 0);
  return dpn.sum_i;
}

int64_t TianmuAttr::GetMinInt64(int pack) {
  LoadPackInfo();
  if (GetPackOntologicalStatus(pack) == PackOntologicalStatus::NULLS_ONLY)
    return common::MINUS_INF_64;
  return get_dpn(pack).min_i;
}

int64_t TianmuAttr::GetMaxInt64(int pack) {
  LoadPackInfo();
  if (GetPackOntologicalStatus(pack) == PackOntologicalStatus::NULLS_ONLY)
    return common::PLUS_INF_64;
  return get_dpn(pack).max_i;
}

types::BString TianmuAttr::GetMaxString(int pack) {
  LoadPackInfo();
  if (GetPackOntologicalStatus(pack) == PackOntologicalStatus::NULLS_ONLY || pack_type != common::PackType::STR)
    return types::BString();
  auto s = get_dpn(pack).max_s;
  size_t max_len = GetActualSize(pack);
  if (max_len > 8)
    max_len = 8;
  int64_t min_len = max_len - 1;
  while (min_len >= 0 && s[min_len] != '\0') min_len--;
  return types::BString(s, min_len >= 0 ? min_len : max_len, true);
}

types::BString TianmuAttr::GetMinString(int pack) {
  LoadPackInfo();
  if (GetPackOntologicalStatus(pack) == PackOntologicalStatus::NULLS_ONLY || pack_type != common::PackType::STR)
    return types::BString();
  auto s = get_dpn(pack).min_s;
  size_t max_len = GetActualSize(pack);
  int64_t min_len = (max_len > 8 ? 8 : max_len);
  while (min_len > 0 && s[min_len - 1] == '\0') min_len--;
  return types::BString(s, min_len, true);
}

// size of original 0-level value (text/binary, not null-terminated)
size_t TianmuAttr::GetLength(int64_t obj) {
  DEBUG_ASSERT(NumOfObj() >= static_cast<uint64_t>(obj));
  LoadPackInfo();
  int pack = row2pack(obj);
  auto const &dpn(get_dpn(pack));
  if (dpn.NullOnly())
    return 0;
  if (GetPackType() != common::PackType::STR)
    return Type().GetDisplaySize();
  return get_packS(pack)->GetValueBinary(row2offset(obj)).size();
}

// original 0-level value for a given 1-level code
types::BString TianmuAttr::DecodeValue_S(int64_t code) {
  if (code == common::NULL_VALUE_64) {
    return types::BString();
  }
  if (Type().Lookup()) {
    DEBUG_ASSERT(GetPackType() == common::PackType::INT);
    return m_dict->GetRealValue((int)code);
  }
  common::ColumnType a_type = TypeName();
  if (ATI::IsIntegerType(a_type)) {
    types::TianmuNum tianmu_n(code, -1, false, a_type);
    types::BString tianmu_bstring = tianmu_n.ToBString();
    tianmu_bstring.MakePersistent();
    return tianmu_bstring;
  } else if (ATI::IsRealType(a_type)) {
    types::TianmuNum tianmu_n(code, -1, true, a_type);
    types::BString tianmu_bstring = tianmu_n.ToBString();
    tianmu_bstring.MakePersistent();
    return tianmu_bstring;
  } else if (a_type == common::ColumnType::NUM || a_type == common::ColumnType::BIT) {
    types::TianmuNum tianmu_n(code, Type().GetScale(), false, a_type);
    types::BString tianmu_bstring = tianmu_n.ToBString();
    tianmu_bstring.MakePersistent();
    return tianmu_bstring;
  } else if (ATI::IsDateTimeType(a_type)) {
    types::TianmuDateTime tianmu_dt(code, a_type);
    if (a_type == common::ColumnType::TIMESTAMP) {
      types::TianmuDateTime::AdjustTimezone(tianmu_dt);
    }
    types::BString tianmu_bstring = tianmu_dt.ToBString();
    tianmu_bstring.MakePersistent();
    return tianmu_bstring;
  }
  return types::BString();
}

// 1-level code value for a given 0-level (text) value
// if new_val, then add to dictionary if not present
int TianmuAttr::EncodeValue_T(const types::BString &tianmu_s, bool new_val, common::ErrorCode *tianmu_err_code) {
  if (tianmu_err_code)
    *tianmu_err_code = common::ErrorCode::SUCCESS;
  if (tianmu_s.IsNull())
    return common::NULL_VALUE_32;
  if (ATI::IsStringType(TypeName())) {
    DEBUG_ASSERT(GetPackType() == common::PackType::INT);
    LoadPackInfo();
    int vs = m_dict->GetEncodedValue(tianmu_s.val_, tianmu_s.len_);
    if (vs < 0) {
      if (!new_val) {
        return common::NULL_VALUE_32;
      }

      ASSERT(m_tx != nullptr, "attempt to update dictionary in readonly transaction");

      // copy on write
      if (!m_dict->Changed()) {
        auto sp = m_dict;
        m_dict = sp->Clone();
        sp->Unlock();
        hdr.dict_ver++;
        ha_tianmu_engine_->cache.PutObject(FTreeCoordinate(m_tid, m_cid, hdr.dict_ver), m_dict);
      }
      vs = m_dict->Add(tianmu_s.val_, tianmu_s.len_);
    }
    return vs;
  }
  char const *val = tianmu_s.val_;
  if (val == 0)
    val = ZERO_LENGTH_STRING;
  if (ATI::IsDateTimeType(TypeName()) || TypeName() == common::ColumnType::BIGINT) {
    ASSERT(0, "Wrong data type!");
  } else {
    types::TianmuNum tianmu_n;
    common::ErrorCode tmp_tianmu_rc = types::TianmuNum::Parse(tianmu_s, tianmu_n, TypeName());
    if (tianmu_err_code)
      *tianmu_err_code = tmp_tianmu_rc;
    return (int)(int64_t)tianmu_n;
  }
  return common::NULL_VALUE_32;
}

// transform a types::TianmuNum value into 1-level code, take into account
// the precision etc. no changes for REAL; rounded=true iff v has greater
// precision than the column and the returned result is rounded down
int64_t TianmuAttr::EncodeValue64(types::TianmuDataType *v, bool &rounded, common::ErrorCode *tianmu_err_code) {
  rounded = false;
  if (tianmu_err_code)
    *tianmu_err_code = common::ErrorCode::SUCCESS;
  if (!v || v->IsNull())
    return common::NULL_VALUE_64;

  if ((Type().Lookup() && v->Type() != common::ColumnType::NUM && v->Type() != common::ColumnType::BIT)) {
    return EncodeValue_T(v->ToBString(), false, tianmu_err_code);
  } else if (ATI::IsDateTimeType(TypeName()) || ATI::IsDateTimeNType(TypeName())) {
    return ((types::TianmuDateTime *)v)->GetInt64();
  }
  ASSERT(GetPackType() == common::PackType::INT, "Pack type must be numeric!");

  int64_t vv = ((types::TianmuNum *)v)->ValueInt();
  int vp = ((types::TianmuNum *)v)->Scale();
  if (ATI::IsRealType(TypeName())) {
    if (((types::TianmuNum *)v)->IsReal())
      return vv;  // already stored as double
    double res = double(vv);
    res /= types::Uint64PowOfTen(vp);
    // for(int i=0;i<vp;i++) res*=10;
    return *(int64_t *)(&res);  // encode
  }
  if (((types::TianmuNum *)v)->IsReal()) {  // v is double
    double vd = *(double *)(&vv);
    vd *= types::Uint64PowOfTen(Type().GetScale());  // translate into int64_t of proper precision
    if (vd > common::PLUS_INF_64)
      return common::PLUS_INF_64;
    if (vd < common::MINUS_INF_64)
      return common::MINUS_INF_64;
    int64_t res = int64_t(vd);
    if (fabs(vd - double(res)) > 0.01)
      rounded = true;  // ignore errors which are 2 digits less than declared
                       // precision
    return res;
  }
  unsigned char dplaces = Type().GetScale();
  while (vp < dplaces) {
    if (vv < common::MINUS_INF_64 / 10)
      return common::MINUS_INF_64;
    if (vv > common::PLUS_INF_64 / 10)
      return common::PLUS_INF_64;
    vv *= 10;
    vp++;
  }
  while (vp > dplaces) {
    if (vv % 10 != 0)
      rounded = true;
    vv /= 10;
    vp--;
  }
  return vv;
}

int64_t TianmuAttr::EncodeValue64(const types::TianmuValueObject &v, bool &rounded,
                                  common::ErrorCode *tianmu_err_code) {
  return EncodeValue64(v.Get(), rounded, tianmu_err_code);
}

size_t TianmuAttr::GetPrefixLength(int pack) {
  LoadPackInfo();

  if (GetPackOntologicalStatus(pack) == PackOntologicalStatus::NULLS_ONLY)
    return 0;

  auto const &dpn(get_dpn(pack));
  size_t dif_pos = 0;
  for (; (dif_pos < sizeof(uint64_t)) && dpn.min_s[dif_pos] && (dpn.min_s[dif_pos] == dpn.max_s[dif_pos]); ++dif_pos)
    ;

  return dif_pos;
}

void TianmuAttr::LockPackForUse(common::PACK_INDEX pn) {
  if (m_idx.empty()) {  // in case table not insert data
    return;
  }

  auto dpn = &get_dpn(pn);
  if (dpn->IsLocal())
    dpn = m_share->get_dpn_ptr(dpn->base);

  if (dpn->Trivial() && !dpn->IsLocal())
    return;

  while (true) {
    if (dpn->IncRef())
      return;

    // either the pack is not loaded yet or other thread is loading it

    uint64_t v = 0;
    if (dpn->CAS(v, loading_flag)) {
      // we win the chance to load data
      std::shared_ptr<Pack> sp;
      try {
        sp = ha_tianmu_engine_->cache.GetOrFetchObject<Pack>(get_pc(pn), this);
      } catch (std::exception &e) {
        dpn->SetPackPtr(0);
        TIANMU_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
        throw e;
      } catch (...) {
        dpn->SetPackPtr(0);
        TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
        throw;
      }

      uint64_t newv = reinterpret_cast<unsigned long>(sp.get()) + tag_one;
      uint64_t expected = loading_flag;
      ASSERT(dpn->CAS(expected, newv),
             "bad loading flag" + std::to_string(newv) + ". " + Path().string() + " index:" + std::to_string(pn));
      return;
    }
    // some one is loading data, wait a while and retry
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
}

void TianmuAttr::UnlockPackFromUse(common::PACK_INDEX pn) {
  if (m_idx.empty()) {  // in case table not insert data
    return;
  }

  if (pn >= m_idx.size()) {
    return;
  }

  auto dpn = &get_dpn(pn);
  if (dpn->IsLocal())
    dpn = m_share->get_dpn_ptr(dpn->base);

  if (dpn->Trivial())
    return;

  auto v = dpn->GetPackPtr();
  unsigned long newv;

  do {
    if (v <= tag_one) {
      TIANMU_LOG(LogCtl_Level::ERROR, "UnlockPackFromUse fail, v [%ld] <= tag_one [%ld]", v, tag_one);
      ASSERT(0,
             "Unexpected lock counter!: " + Path().string() + " index:" + std::to_string(pn) + " " + std::to_string(v));
    }
    newv = v - tag_one;
    if ((v & ~tag_mask) == tag_one)
      newv = 0;
  } while (!dpn->CAS(v, newv));

  if (newv != 0) {
    return;
  }

  auto ap = reinterpret_cast<Pack *>(v & tag_mask);
  ap->Unlock();
}

void TianmuAttr::Collapse() {
  if (m_dict && !m_dict->Changed()) {
    m_dict->Release();
    m_dict.reset();
  }
}

void TianmuAttr::Release() { Collapse(); }

std::shared_ptr<Pack> TianmuAttr::Fetch(const PackCoordinate &pc) {
  auto dpn = m_share->get_dpn_ptr(pc_dp(pc));
  if (GetPackType() == common::PackType::STR)
    return std::make_shared<PackStr>(dpn, pc, m_share);
  return std::make_shared<PackInt>(dpn, pc, m_share);
}

std::shared_ptr<FTree> TianmuAttr::Fetch([[maybe_unused]] const FTreeCoordinate &coord) {
  auto sp = std::make_shared<FTree>();
  sp->LoadData(Path() / common::COL_DICT_DIR / std::to_string(hdr.dict_ver));
  return sp;
}

void TianmuAttr::PreparePackForLoad() {
  if (SizeOfPack() == 0 || get_last_dpn().numOfRecords == (1U << pss)) {
    // just allocate a DPN but do not create dp for now
    auto ret = m_share->alloc_dpn(m_tx->GetID());
    m_idx.push_back(ret);
  } else {
    CopyPackForWrite(SizeOfPack() - 1);
  }
}

void TianmuAttr::LoadData(loader::ValueCache *nvs, Transaction *conn_info) {
  no_change = false;
  if (conn_info)
    current_txn_ = conn_info;

  PreparePackForLoad();
  int pi = SizeOfPack() - 1;
  switch (GetPackType()) {
    case common::PackType::INT:
      LoadDataPackN(pi, nvs);
      break;
    case common::PackType::STR: {
      LoadDataPackS(pi, nvs);
      break;
    }
    default:
      throw common::DatabaseException("Unknown pack type" + Path().string());
      break;
  }

  DPN &dpn = get_dpn(pi);
  Pack *pack = get_pack(pi);
  if (!dpn.Trivial()) {
    pack->Save();
  }
  if (current_txn_->LoadSource() == common::LoadSource::LS_File) {
    if (pack) {
      pack->Unlock();
    }
    ha_tianmu_engine_->cache.DropObject(get_pc(pi));
    dpn.SetPackPtr(0);
  }

  hdr.numOfRecords += nvs->NumOfValues();
  hdr.numOfNulls += (Type().NotNull() ? 0 : nvs->NumOfNulls());
  hdr.natural_size += nvs->SumarizedSize();
  hdr.numOfDeleted += nvs->NumOfDeletes();
#ifndef NDEBUG
  TIANMU_LOG(LogCtl_Level::DEBUG,
             "DELTA INSERT load_data_task tid: %d, cid: %d, pack index: %d, pack size: %d, write batch size: %d", m_tid,
             m_cid, pi, (hdr.numOfRecords - pi * 65536), nvs->NumOfValues());
#endif
}

void TianmuAttr::LoadDataPackN(size_t pi, loader::ValueCache *nvs) {
  std::optional<common::double_int_t> nv;

  if (Type().NotNull()) {
    if (ATI::IsStringType(TypeName()))
      nv.emplace(long(EncodeValue_T(ZERO_LENGTH_STRING, true)));
    else
      nv.emplace(0L);
  }

  auto &dpn = get_dpn(pi);
  auto load_values = nvs->NumOfValues();
  size_t load_nulls = nv.has_value() ? 0 : nvs->NumOfNulls();

  // nulls only
  if (load_nulls == load_values && nvs->NumOfDeletes() == 0 && (dpn.numOfRecords == 0 || dpn.NullOnly())) {
    dpn.numOfRecords += load_values;
    dpn.numOfNulls += load_values;
    return;
  }

  bool is_real_type = ATI::IsRealType(ct.GetTypeName());

  // has non-null data to load
  int64_t load_min;
  int64_t load_max;
  if (!is_real_type) {
    nvs->CalcIntStats(nv);
    load_min = nvs->MinInt();
    load_max = nvs->MaxInt();
    dpn.sum_i += nvs->SumInt();
  } else {
    nvs->CalcRealStats(nv);
    *(double *)&load_min = nvs->MinDouble();
    *(double *)&load_max = nvs->MaxDouble();
    dpn.sum_d += nvs->SumDouble();
  }

  // now dpn->sum has been updated

  // uniform package
  if ((dpn.numOfNulls + load_nulls) == 0 && load_min == load_max && nvs->NumOfDeletes() == 0 &&
      (dpn.numOfRecords == 0 || (dpn.min_i == load_min && dpn.max_i == load_max))) {
    dpn.min_i = load_min;
    dpn.max_i = load_max;
    dpn.numOfRecords += load_values;
  } else {
    // new package (also in case of expanding so-far-uniform package)
    if (dpn.Trivial()) {
      // we need a pack struct for the previous trivial dp
      auto sp = ha_tianmu_engine_->cache.GetOrFetchObject<Pack>(get_pc(pi), this);

      // we don't need any synchronization here because the dpn is local!
      dpn.SetPackPtr(reinterpret_cast<unsigned long>(sp.get()) + tag_one);
    }
    get_packN(pi)->LoadValues(nvs, nv);
  }

  // update global column statistics
  if (nvs->NumOfNulls() != nvs->NumOfValues()) {
    if (NumOfObj() == 0) {
      SetMinInt64(dpn.min_i);
      SetMaxInt64(dpn.max_i);
    } else {
      if (!ATI::IsRealType(TypeName())) {
        if (GetMinInt64() > dpn.min_i)
          SetMinInt64(dpn.min_i);
        if (GetMaxInt64() < dpn.max_i)
          SetMaxInt64(dpn.max_i);
      } else {
        int64_t a_min = GetMinInt64();
        int64_t a_max = GetMaxInt64();
        if (*(double *)(&a_min) > dpn.min_d)
          SetMinInt64(dpn.min_i);
        if (*(double *)(&a_max) < dpn.max_d)
          SetMaxInt64(dpn.max_i);  // 1-level statistics
      }
    }
  }
}

void TianmuAttr::LoadDataPackS(size_t pi, loader::ValueCache *nvs) {
  auto &dpn(get_dpn(pi));

  auto load_nulls = Type().NotNull() ? 0 : nvs->NumOfNulls();
  auto cnt = nvs->NumOfValues();

  // no need to store any values - uniform package
  if (load_nulls == cnt && nvs->NumOfDeletes() == 0 && (dpn.numOfRecords == 0 || dpn.NullOnly())) {
    dpn.numOfRecords += cnt;
    dpn.numOfNulls += cnt;
    return;
  }

  // new package or expanding so-far-null package
  if (dpn.numOfRecords == 0 || dpn.NullOnly()) {
    auto sp = ha_tianmu_engine_->cache.GetOrFetchObject<Pack>(get_pc(pi), this);
    dpn.SetPackPtr(reinterpret_cast<unsigned long>(sp.get()) + tag_one);
  }

  get_packS(pi)->LoadValues(nvs);
}

void TianmuAttr::UpdateData(uint64_t row, Value &old_v, Value &new_v) {
  // rclog << lock << "update data for row " << row << " col " << m_cid <<
  // system::unlock;
  no_change = false;

  auto pn = row2pack(row);
  FunctionExecutor fe([this, pn]() { LockPackForUse(pn); }, [this, pn]() { UnlockPackFromUse(pn); });
  // primary key process
  UpdateIfIndex(nullptr, row, ColId(), old_v, new_v);

  CopyPackForWrite(pn);

  auto &dpn = get_dpn(pn);
  auto dpn_save = dpn;
  if (dpn.Trivial()) {
    // need to create pack struct for previous trivial pack
    ha_tianmu_engine_->cache.GetOrFetchObject<Pack>(get_pc(pn), this);
  }

  if (ct.Lookup() && new_v.HasValue()) {
    auto &str = new_v.GetString();
    int code = m_dict->GetEncodedValue(str.data(), str.size());
    if (code < 0) {
      ASSERT(m_tx != nullptr, "attempt to update dictionary in readonly transaction");
      // copy on write
      if (!m_dict->Changed()) {
        auto sp = m_dict;
        m_dict = sp->Clone();
        sp->Unlock();
        hdr.dict_ver++;
        ha_tianmu_engine_->cache.PutObject(FTreeCoordinate(m_tid, m_cid, hdr.dict_ver), m_dict);
      }
      code = m_dict->Add(str.data(), str.size());
    }
    new_v.SetInt(code);
  }

  get_pack(pn)->UpdateValue(row2offset(row), new_v);
  dpn.synced = false;

  // update global data
  hdr.numOfNulls -= dpn_save.numOfNulls;
  hdr.numOfNulls += dpn.numOfNulls;

  ResetMaxMin(dpn);
}

void TianmuAttr::UpdateBatchData(core::Transaction *tx, const std::unordered_map<uint64_t, Value> &rows) {
  no_change = false;

  // group by pn
  std::unordered_map<common::PACK_INDEX, std::unordered_map<uint64_t, Value>> packs;
  for (const auto &row : rows) {
    auto row_id = row.first;
    auto row_val = row.second;
    auto pn = row2pack(row_id);
    auto pack = packs.find(pn);
    if (pack != packs.end()) {
      pack->second.emplace(row_id, row_val);
    } else {
      packs.emplace(pn, std::unordered_map<uint64_t, Value>{{row_id, row_val}});
    }
  }

  for (const auto &pack : packs) {
    auto pn = pack.first;
    FunctionExecutor fe([this, pn]() { LockPackForUse(pn); }, [this, pn]() { UnlockPackFromUse(pn); });
    CopyPackForWrite(pn);
    auto &dpn = get_dpn(pn);
    auto dpn_save = dpn;
    if (dpn.Trivial()) {
      ha_tianmu_engine_->cache.GetOrFetchObject<Pack>(get_pc(pn), this);
    }

    for (const auto &row : pack.second) {
      uint64_t row_id = row.first;
      Value row_val = row.second;
      if (ct.Lookup() && row_val.HasValue()) {
        auto &str = row_val.GetString();
        int code = m_dict->GetEncodedValue(str.data(), str.size());
        if (code < 0) {
          ASSERT(m_tx != nullptr, "attempt to update dictionary in readonly transaction");
          if (!m_dict->Changed()) {
            auto sp = m_dict;
            m_dict = sp->Clone();
            sp->Unlock();
            hdr.dict_ver++;
            ha_tianmu_engine_->cache.PutObject(FTreeCoordinate(m_tid, m_cid, hdr.dict_ver), m_dict);
          }
          code = m_dict->Add(str.data(), str.size());
        }
        row_val.SetInt(code);
      }
      get_pack(pn)->UpdateValue(row2offset(row_id), row_val);
    }

    dpn.synced = false;
    // update global data
    hdr.numOfNulls -= dpn_save.numOfNulls;
    hdr.numOfNulls += dpn.numOfNulls;
    ResetMaxMin(dpn);
#ifndef NDEBUG
    TIANMU_LOG(LogCtl_Level::DEBUG,
               "DELTA UPDATE batch_update_task tid: %d, cid: %d, pack index: %d, write batch size: %d", m_tid, m_cid,
               pn, pack.second.size());
#endif
  }
}

bool TianmuAttr::IsDelete(int64_t row) {
  if (row == common::NULL_VALUE_64)
    return true;
  DEBUG_ASSERT(hdr.numOfRecords >= static_cast<uint64_t>(row));
  auto pack = row2pack(row);
  const auto &dpn = get_dpn(pack);

  if (dpn.numOfDeleted == 0)
    return false;

  if (dpn.numOfDeleted > 0) {
    FunctionExecutor fe([this, pack]() { LockPackForUse(pack); }, [this, pack]() { UnlockPackFromUse(pack); });
    return get_pack(pack)->IsDeleted(row2offset(row));
  }
  return false;
}

void TianmuAttr::DeleteData(uint64_t row) {
  auto pn = row2pack(row);
  FunctionExecutor fe([this, pn]() { LockPackForUse(pn); }, [this, pn]() { UnlockPackFromUse(pn); });

  // primary key process
  DeleteByPrimaryKey(row, ColId());

  CopyPackForWrite(pn);
  auto &dpn = get_dpn(pn);
  auto dpn_save = dpn;
  if (dpn.Trivial()) {
    // need to create pack struct for previous trivial pack
    ha_tianmu_engine_->cache.GetOrFetchObject<Pack>(get_pc(pn), this);
  }
  get_pack(pn)->DeleteByRow(row2offset(row));

  // update global data
  hdr.numOfNulls -= dpn_save.numOfNulls;
  hdr.numOfNulls += dpn.numOfNulls;
  hdr.numOfDeleted++;
  ResetMaxMin(dpn);
}

void TianmuAttr::DeleteBatchData(core::Transaction *tx, const std::vector<uint64_t> &rows) {
  // group by pn
  std::unordered_map<common::PACK_INDEX, std::vector<uint64_t>> packs;
  //  for (const auto &[row, val] : rows) {
  for (const auto &row_id : rows) {
    auto pn = row2pack(row_id);
    auto pack = packs.find(pn);
    if (pack != packs.end()) {
      pack->second.push_back(row_id);
    } else {
      packs.emplace(pn, std::vector<uint64_t>{row_id});
    }
  }

  for (const auto &pack : packs) {
    auto pn = pack.first;
    FunctionExecutor fe([this, pn]() { LockPackForUse(pn); }, [this, pn]() { UnlockPackFromUse(pn); });
    CopyPackForWrite(pn);
    auto &dpn = get_dpn(pn);
    auto dpn_save = dpn;
    if (dpn.Trivial()) {
      ha_tianmu_engine_->cache.GetOrFetchObject<Pack>(get_pc(pn), this);
    }

    for (const auto &row_id : pack.second) {
      get_pack(pn)->DeleteByRow(row2offset(row_id));
    }
    // update global data
    hdr.numOfNulls -= dpn_save.numOfNulls;
    hdr.numOfNulls += dpn.numOfNulls;
    hdr.numOfDeleted -= dpn_save.numOfDeleted;
    hdr.numOfNulls += dpn.numOfDeleted;
    ResetMaxMin(dpn);
#ifndef NDEBUG
    TIANMU_LOG(LogCtl_Level::DEBUG,
               "DELTA DELETE batch_delete_task tid: %d, cid: %d, pack index: %d, write batch size: %d", m_tid, m_cid,
               pn, pack.second.size());
#endif
  }
}

void TianmuAttr::ResetMaxMin(DPN &dpn) {
  if (GetPackType() == common::PackType::INT) {
    if (dpn.min_i < hdr.min) {
      hdr.min = dpn.min_i;
    } else {
      // re-calculate the min
      hdr.min = std::numeric_limits<int64_t>::max();
      for (uint i = 0; i < m_idx.size(); i++) {
        if (!get_dpn(i).NullOnly())
          hdr.min = std::min(get_dpn(i).min_i, hdr.min);
      }
    }

    if (dpn.max_i > hdr.max) {
      hdr.max = dpn.max_i;
    } else {
      // re-calculate the max
      hdr.max = std::numeric_limits<int64_t>::min();
      for (uint i = 0; i < m_idx.size(); i++) {
        if (!get_dpn(i).NullOnly())
          hdr.max = std::max(get_dpn(i).max_i, hdr.max);
      }
    }
  } else {  // common::PackType::STR
  }
}

void TianmuAttr::CopyPackForWrite(common::PACK_INDEX pi) {
  if (get_dpn(pi).IsLocal())
    return;

  auto &old_dpn(get_dpn(pi));  // save a ref to the old dpn

  auto pos = m_share->alloc_dpn(m_tx->GetID(), &old_dpn);

  // update current view
  m_idx[pi] = pos;
  auto &dpn(get_dpn(pi));

  //  if (dpn.Trivial())
  //      return;

  const PackCoordinate pc_old(m_tid, m_cid, m_share->GetPackIndex(&old_dpn));
  const PackCoordinate pc_new(get_pc(pi));
  std::shared_ptr<Pack> new_pack;
  // if the pack data is already loaded, just clone it to avoid disk IO
  // otherwise, load pack data from disk
  auto pack = ha_tianmu_engine_->cache.GetLockedObject<Pack>(pc_old);
  if (pack) {
    new_pack = pack->Clone(pc_new);
    new_pack->SetDPN(&dpn);  // need to set dpn after clone
    ha_tianmu_engine_->cache.PutObject(pc_new, new_pack);
    pack->Unlock();
  } else {
    new_pack = ha_tianmu_engine_->cache.GetOrFetchObject<Pack>(get_pc(pi), this);
  }
  dpn.SetPackPtr(reinterpret_cast<unsigned long>(new_pack.get()) + tag_one);
}

void TianmuAttr::CompareAndSetCurrentMin(const types::BString &tstmp, types::BString &min, bool set) {
  bool res;
  if (types::RequiresUTFConversions(Type().GetCollation())) {
    res = CollationStrCmp(Type().GetCollation(), tstmp, min) < 0;
  } else
    res = tstmp.CompareWith(min) < 0;

  if (!set || res) {
    min = tstmp;
    min.MakePersistent();
    set = true;
  }
}

void TianmuAttr::CompareAndSetCurrentMax(const types::BString &tstmp, types::BString &max) {
  bool res;
  if (types::RequiresUTFConversions(Type().GetCollation())) {
    res = CollationStrCmp(Type().GetCollation(), tstmp, max) > 0;
  } else
    res = tstmp.CompareWith(max) > 0;

  if (res) {
    max = tstmp;
    max.MakePersistent();
  }
}

types::BString TianmuAttr::MinS(Filter *f) {
  if (f->IsEmpty() || !ATI::IsStringType(TypeName()) || NumOfObj() == 0 || NumOfObj() == NumOfNulls())
    return types::BString();
  types::BString min;
  bool set = false;
  if (f->NumOfBlocks() != SizeOfPack())
    throw common::DatabaseException("Data integrity error, query cannot be evaluated (MinS).");
  else {
    LoadPackInfo();
    FilterOnesIterator it(f, pss);
    while (it.IsValid()) {
      uint b = it.GetCurrPack();
      if (b >= SizeOfPack())
        continue;
      auto const &dpn(get_dpn(b));
      auto p = get_packS(b);
      if (GetPackType() == common::PackType::INT &&
          (GetPackOntologicalStatus(b) == PackOntologicalStatus::UNIFORM ||
           (GetPackOntologicalStatus(b) == PackOntologicalStatus::UNIFORM_AND_NULLS && f->IsFull(b)))) {
        CompareAndSetCurrentMin(DecodeValue_S(dpn.min_i), min, set);
        it.NextPack();
      } else if (!(dpn.NullOnly() || dpn.numOfRecords == 0)) {
        while (it.IsValid() && b == (unsigned int)it.GetCurrPack()) {
          int n = it.GetCurrInPack();
          if (GetPackType() == common::PackType::STR && p->IsNull(n) == 0) {
            CompareAndSetCurrentMin(p->GetValueBinary(n), min, set);
          }
          ++it;
        }
      }
    }
  }
  return min;
}

types::BString TianmuAttr::MaxS(Filter *f) {
  if (f->IsEmpty() || !ATI::IsStringType(TypeName()) || NumOfObj() == 0 || NumOfObj() == NumOfNulls())
    return types::BString();

  types::BString max;
  if (f->NumOfBlocks() != SizeOfPack())
    throw common::DatabaseException("Data integrity error, query cannot be evaluated (MaxS).");
  else {
    LoadPackInfo();
    FilterOnesIterator it(f, pss);
    while (it.IsValid()) {
      int b = it.GetCurrPack();
      if (uint(b) >= SizeOfPack())
        continue;
      auto const &dpn(get_dpn(b));
      auto p = get_packS(b);
      if (GetPackType() == common::PackType::INT &&
          (GetPackOntologicalStatus(b) == PackOntologicalStatus::UNIFORM ||
           (GetPackOntologicalStatus(b) == PackOntologicalStatus::UNIFORM_AND_NULLS && f->IsFull(b)))) {
        CompareAndSetCurrentMax(DecodeValue_S(dpn.min_i), max);
      } else if (!(dpn.NullOnly() || dpn.numOfRecords == 0)) {
        while (it.IsValid() && b == it.GetCurrPack()) {
          int n = it.GetCurrInPack();
          if (GetPackType() == common::PackType::STR && p->IsNull(n) == 0) {
            CompareAndSetCurrentMax(p->GetValueBinary(n), max);
          } else if (GetPackType() == common::PackType::INT && !p->IsNull(n)) {
            CompareAndSetCurrentMax(DecodeValue_S(get_packN(b)->GetValInt(n) + dpn.min_i), max);
          }
          ++it;
        }
      }
    }
  }
  return max;
}

void TianmuAttr::UpdateRSI_Hist(common::PACK_INDEX pi) {
  if (!GetFilter_Hist()) {
    return;
  }

  if (GetPackType() != common::PackType::INT || NumOfObj() == 0)
    return;

  filter_hist->Update(pi, get_dpn(pi), get_packN(pi));
}

void TianmuAttr::UpdateRSI_CMap(common::PACK_INDEX pi) {
  if (GetPackType() != common::PackType::STR || NumOfObj() == 0 || types::RequiresUTFConversions(Type().GetCollation()))
    return;

  if (!GetFilter_CMap())
    return;

  if (GetPackOntologicalStatus(pi) == PackOntologicalStatus::NULLS_ONLY)
    return;
  filter_cmap->Update(pi, get_dpn(pi), get_packS(pi));
}

void TianmuAttr::UpdateRSI_Bloom(common::PACK_INDEX pi) {
  if (!GetFilter_Bloom()) {
    return;
  }

  if (NumOfObj() == 0)
    return;

  if (GetPackOntologicalStatus(pi) == PackOntologicalStatus::NULLS_ONLY)
    return;

  filter_bloom->Update(pi, get_dpn(pi), get_packS(pi));
}

void TianmuAttr::RefreshFilter(common::PACK_INDEX pi) {
  UpdateRSI_Bloom(pi);
  UpdateRSI_CMap(pi);
  UpdateRSI_Hist(pi);
}

Pack *TianmuAttr::get_pack(size_t i) { return reinterpret_cast<Pack *>(get_dpn(i).GetPackPtr() & tag_mask); }

Pack *TianmuAttr::get_pack(size_t i) const { return reinterpret_cast<Pack *>(get_dpn(i).GetPackPtr() & tag_mask); }

std::shared_ptr<RSIndex_Hist> TianmuAttr::GetFilter_Hist() {
  if (!tianmu_sysvar_enable_histogram_cmap_bloom) {
    return nullptr;
  }

  if (!m_share->has_filter_hist)
    return nullptr;

  if (m_tx != nullptr) {
    if (!filter_hist)
      filter_hist = std::make_shared<RSIndex_Hist>(Path() / common::COL_FILTER_DIR, m_version);
    return filter_hist;
  }
  if (!filter_hist)
    filter_hist = std::static_pointer_cast<RSIndex_Hist>(ha_tianmu_engine_->filter_cache.Get(
        FilterCoordinate(m_tid, m_cid, (int)FilterType::HIST, m_version.v1, m_version.v2), filter_creator));
  return filter_hist;
}

std::shared_ptr<RSIndex_CMap> TianmuAttr::GetFilter_CMap() {
  if (!tianmu_sysvar_enable_histogram_cmap_bloom) {
    return nullptr;
  }

  if (!m_share->has_filter_cmap)
    return nullptr;

  if (m_tx != nullptr) {
    if (!filter_cmap)
      filter_cmap = std::make_shared<RSIndex_CMap>(Path() / common::COL_FILTER_DIR, m_version);
    return filter_cmap;
  }
  return std::static_pointer_cast<RSIndex_CMap>(ha_tianmu_engine_->filter_cache.Get(
      FilterCoordinate(m_tid, m_cid, (int)FilterType::CMAP, m_version.v1, m_version.v2), filter_creator));
}

std::shared_ptr<RSIndex_Bloom> TianmuAttr::GetFilter_Bloom() {
  if (!tianmu_sysvar_enable_histogram_cmap_bloom) {
    return nullptr;
  }

  if (!m_share->has_filter_bloom)
    return nullptr;

  if (m_tx != nullptr) {
    if (!filter_bloom)
      filter_bloom = std::make_shared<RSIndex_Bloom>(Path() / common::COL_FILTER_DIR, m_version);
    return filter_bloom;
  }
  return std::static_pointer_cast<RSIndex_Bloom>(ha_tianmu_engine_->filter_cache.Get(
      FilterCoordinate(m_tid, m_cid, (int)FilterType::BLOOM, m_version.v1, m_version.v2), filter_creator));
}

void TianmuAttr::UpdateIfIndex(core::Transaction *tx, uint64_t row, uint64_t col, const Value &old_v,
                               const Value &new_v) {
  if (tx == nullptr) {
    tx = current_txn_;
  }
  auto path = m_share->owner->Path();
  std::shared_ptr<index::TianmuTableIndex> tab = ha_tianmu_engine_->GetTableIndex(path);
  // col is not primary key
  if (!tab)
    return;
  std::vector<uint> keycols = tab->KeyCols();
  if (std::find(keycols.begin(), keycols.end(), col) == keycols.end())
    return;

  if (!new_v.HasValue())
    throw common::Exception("primary key not support null!");

  if (GetPackType() == common::PackType::STR) {
    auto &vnew = new_v.GetString();
    auto &vold = old_v.GetString();
    std::string nkey(vnew.data(), vnew.length());
    std::string okey(vold.data(), vold.length());
    common::ErrorCode returnCode = tab->UpdateIndex(tx, nkey, okey, row);
    if (returnCode == common::ErrorCode::DUPP_KEY || returnCode == common::ErrorCode::FAILED) {
      TIANMU_LOG(LogCtl_Level::DEBUG, "Duplicate entry: %s for primary key", vnew.data());
      throw common::DupKeyException("Duplicate entry: " + vnew + " for primary key");
    }
  } else {  // common::PackType::INT
    int64_t vnew = new_v.GetInt();
    int64_t vold = old_v.GetInt();
    std::string nkey(reinterpret_cast<const char *>(&vnew), sizeof(int64_t));
    std::string okey(reinterpret_cast<const char *>(&vold), sizeof(int64_t));
    common::ErrorCode returnCode = tab->UpdateIndex(tx, nkey, okey, row);
    if (returnCode == common::ErrorCode::DUPP_KEY || returnCode == common::ErrorCode::FAILED) {
      TIANMU_LOG(LogCtl_Level::DEBUG, "Duplicate entry :%" PRId64 " for primary key", vnew);
      throw common::DupKeyException("Duplicate entry: " + std::to_string(vnew) + " for primary key");
    }
  }
}

void TianmuAttr::DeleteByPrimaryKey(uint64_t row, uint64_t col) {
  auto path = m_share->owner->Path();
  std::shared_ptr<index::TianmuTableIndex> tab = ha_tianmu_engine_->GetTableIndex(path);
  // col is not primary key
  if (!tab)
    return;
  std::vector<uint> keycols = tab->KeyCols();
  if (std::find(keycols.begin(), keycols.end(), col) == keycols.end())
    return;

  if (GetPackType() == common::PackType::STR) {
    auto currentValue = GetValueString(row);
    std::vector<std::string> fields{currentValue.ToString()};
    common::ErrorCode returnCode = tab->DeleteIndex(current_txn_, fields, row);
    if (returnCode == common::ErrorCode::FAILED) {
      TIANMU_LOG(LogCtl_Level::DEBUG, "Delete: %s for primary key", currentValue.GetDataBytesPointer());
      throw common::Exception("Delete: " + currentValue.ToString() + " for primary key");
    }
  } else {  // common::PackType::INT
    auto currentValue = GetValueInt64(row);
    std::string currentRowKey(reinterpret_cast<const char *>(&currentValue), sizeof(int64_t));
    std::vector<std::string> fields{currentRowKey};
    common::ErrorCode returnCode = tab->DeleteIndex(current_txn_, fields, row);
    if (returnCode == common::ErrorCode::FAILED) {
      TIANMU_LOG(LogCtl_Level::DEBUG, "Delete: %" PRId64 " for primary key", currentValue);
      throw common::Exception("Delete: " + std::to_string(currentValue) + " for primary key");
    }
  }
}

}  // namespace core
}  // namespace Tianmu
