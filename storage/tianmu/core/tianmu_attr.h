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
#ifndef TIANMU_CORE_TIANMU_ATTR_H_
#define TIANMU_CORE_TIANMU_ATTR_H_
#pragma once

#include <vector>

#include "common/assert.h"
#include "common/common_definitions.h"
#include "core/bin_tools.h"
#include "core/column_share.h"
#include "core/dpn.h"
#include "core/ftree.h"
#include "core/pack.h"
#include "core/physical_column.h"
#include "core/rough_multi_index.h"
#include "core/rsi_bloom.h"
#include "core/rsi_cmap.h"
#include "core/rsi_histogram.h"
#include "core/tianmu_attr_typeinfo.h"
#include "core/tools.h"
#include "loader/value_cache.h"
#include "mm/traceable_object.h"
#include "system/file_system.h"
#include "system/tianmu_system.h"
#include "types/tianmu_data_types.h"
#include "types/tianmu_num.h"
#include "util/fs.h"

namespace Tianmu {
namespace core {

class Transaction;
class Filter;
class MIUpdatingIterator;
class TextStat;

//   Attribute (universal class)

// ENCODING LEVELS
// 0 - text values of attributes
//     nullptr represented as '\0' string or null pointer
// 1 - int encoded:
//		common::CT::INT,common::CT::NUM   - int64_t value,
// common::NULL_VALUE_64 for null, may be also
// treated as int (NULL_VALUE)   decimals: the
// value is shifted by precision, e.g. "583880"=583.88 for DEC(10,3)
// common::CT::TIME, common::CT::DATE, common::CT::YEAR - bitwise 64-bit
// encoding as DATETIME
// common::CT::STRING, common::CT::VARCHAR:
// lookup	- value from the dictionary as int64_t,
// common::NULL_VALUE_64 for null
// non-lookup	- text value as BString
// 2 - locally encoded (to be read from packs):
// string, non-lookup - as level 1;
//	other			- uint64_t, relatively to min value in
// pack, nulls encoded in a separate bit mask.

class PackAllocator {
 public:
  virtual std::shared_ptr<Pack> Fetch(const PackCoordinate &coord) = 0;
  virtual std::shared_ptr<FTree> Fetch(const FTreeCoordinate &coord) = 0;
};

class TianmuAttr final : public mm::TraceableObject, public PhysicalColumn, public PackAllocator {
  friend class TianmuTable;

 public:
  TianmuAttr(Transaction *tx, common::TX_ID xid, int a_num, int t_num, ColumnShare *share);
  TianmuAttr() = delete;
  TianmuAttr(const TianmuAttr &) = delete;
  TianmuAttr &operator=(const TianmuAttr &) = delete;
  ~TianmuAttr() = default;

  static void Create(const fs::path &path, const AttributeTypeInfo &ati, uint8_t pss, size_t no_rows,
                     uint64_t auto_inc_value = 0);

  mm::TO_TYPE TraceableType() const override { return mm::TO_TYPE::TO_TEMPORARY; }
  void UpdateData(uint64_t row, Value &old_v, Value &new_v);
  void UpdateBatchData(core::Transaction *tx, const std::unordered_map<uint64_t, Value> &rows);
  void UpdateIfIndex(core::Transaction *tx, uint64_t row, uint64_t col, const Value &old_v, const Value &new_v);
  void Truncate();
  void DeleteData(uint64_t row);
  void DeleteByPrimaryKey(uint64_t row, uint64_t col);
  void DeleteBatchData(core::Transaction *tx, const std::vector<uint64_t> &rows);
  bool IsDelete(int64_t row);

  void ResetMaxMin(DPN &dpn);

  const types::TianmuDataType &ValuePrototype(bool lookup_to_num) const {
    if ((Type().Lookup() && lookup_to_num) || ATI::IsNumericType(TypeName()))
      return types::TianmuNum::NullValue();
    if (ATI::IsStringType(TypeName()))
      return types::BString::NullValue();
    DEBUG_ASSERT(ATI::IsDateTimeType(TypeName()));
    return types::TianmuDateTime::NullValue();
  }

  int64_t GetValueInt64(int64_t obj) const override {
    if (obj == common::NULL_VALUE_64)
      return common::NULL_VALUE_64;
    DEBUG_ASSERT(hdr.numOfRecords >= static_cast<uint64_t>(obj));
    auto pack = row2pack(obj);
    const auto &dpn = get_dpn(pack);
    auto p = get_packN(pack);
    if (!dpn.Trivial()) {
      DEBUG_ASSERT(pack_type == common::PackType::INT);
      DEBUG_ASSERT(p->IsLocked());  // assuming it is already loaded and locked
      int inpack = row2offset(obj);
      if (p->IsNull(inpack))
        return common::NULL_VALUE_64;
      int64_t res = p->GetValInt(inpack);  // 2-level encoding
      // Natural encoding
      if (ATI::IsRealType(TypeName()))
        return res;
      res += dpn.min_i;
      return res;
    }
    if (dpn.NullOnly())
      return common::NULL_VALUE_64;
    // the only possibility: uniform
    ASSERT(dpn.min_i == dpn.max_i);
    return dpn.min_i;
  }

  // Get value which we already know as not null
  int64_t GetNotNullValueInt64(int64_t obj) const override {
    int pack = row2pack(obj);
    const auto &dpn = get_dpn(pack);
    if (!dpn.Trivial()) {
      DEBUG_ASSERT(get_pack(pack)->IsLocked());

      int64_t res = get_packN(pack)->GetValInt(row2offset(obj));  // 2-level encoding
      // Natural encoding
      if (ATI::IsRealType(TypeName()))
        return res;
      res += dpn.min_i;
      return res;
    }
    // the only possibility: uniform
    return dpn.min_i;
  }

  bool IsNull(int64_t obj) const override {
    if (obj == common::NULL_VALUE_64)
      return true;
    DEBUG_ASSERT(hdr.numOfRecords >= static_cast<uint64_t>(obj));
    auto pack = row2pack(obj);
    const auto &dpn = get_dpn(pack);

    if (Type().NotNull() || dpn.numOfNulls == 0)
      return false;

    if (!dpn.Trivial()) {
      DEBUG_ASSERT(get_pack(pack)->IsLocked());  // assuming the pack is already loaded and locked
      return get_pack(pack)->IsNull(row2offset(obj));
    }

    if (dpn.NullOnly())
      return true;

    if ((pack_type == common::PackType::STR) && !dpn.Trivial()) {
      DEBUG_ASSERT(0);
      return true;
    }
    return true;
  };

  /*! \brief Get a non null-terminated String from a column
   *
   * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and
   * locked
   *
   * Can be used to retrieve binary data
   *
   * \param row identifies a row
   * \param bin_as_hex in case of a binary column, present the value as
   * hexadecimal number \return types::BString object - a string
   * representation of an object from the column
   */
  types::BString GetValueString(const int64_t row);
  types::BString GetNotNullValueString(const int64_t row);

  void GetValueString(int64_t row, types::BString &s) override { s = GetValueString(row); }
  void GetNotNullValueString(int64_t row, types::BString &s) override { s = GetNotNullValueString(row); }
  // The last stage of conserving memory: delete all data, must be reloaded from
  // disk on next use
  void Collapse();

  void LockPackForUse(common::PACK_INDEX pi);
  void UnlockPackFromUse(common::PACK_INDEX pi);

  void CopyPackForWrite(common::PACK_INDEX pi);

  void Release() override;

  int NumOfAttr() const override { return m_cid; }
  phys_col_t ColType() const override { return phys_col_t::kTianmuAttr; }
  common::PackType GetPackType() const { return pack_type; }
  PackOntologicalStatus GetPackOntologicalStatus(int pack_no) override;

  size_t ComputeNaturalSize();  // natural size of data (i.e. without any compression)
  // the compressed size of the attribute (for e.g. calculating compression
  // ratio); may be slightly approximated
  int64_t CompressedSize() const { return hdr.compressed_size; };
  uint32_t ValueOfPackPower() const { return pss; }
  uint64_t NumOfObj() const { return hdr.numOfRecords; }
  uint64_t NumOfDeleted() const { return hdr.numOfDeleted; }
  uint64_t NumOfNulls() const { return hdr.numOfNulls; }
  uint SizeOfPack() const { return m_idx.size(); }
  int64_t GetMinInt64() const { return hdr.min; }
  void SetMinInt64(int64_t a_imin) { hdr.min = a_imin; }
  int64_t GetMaxInt64() const { return hdr.max; }
  void SetMaxInt64(int64_t a_imax) { hdr.max = a_imax; }
  bool GetIfAutoInc() const { return ct.GetAutoInc(); }
  bool GetIfUnsigned() const { return ct.GetUnsigned(); }
  uint64_t AutoIncNext() {
    backup_auto_inc_next_ = m_share->auto_inc_.fetch_add(1);
    if (backup_auto_inc_next_ >= UINT64_MAX)
      return backup_auto_inc_next_;
    uint64_t auto_inc_value = m_share->auto_inc_.load();
    uint64_t max_value{0};
    bool unsigned_flag = GetIfUnsigned();
    switch (TypeName()) {
      case common::ColumnType::BYTEINT:
        max_value = unsigned_flag ? TIANMU_TINYINT_UNSIGNED_MAX : TIANMU_TINYINT_MAX;
        break;
      case common::ColumnType::SMALLINT:
        max_value = unsigned_flag ? TIANMU_SMALLINT_UNSIGNED_MAX : TIANMU_SMALLINT_MAX;
        break;
      case common::ColumnType::MEDIUMINT:
        max_value = unsigned_flag ? TIANMU_MEDIUMINT_UNSIGNED_MAX : TIANMU_MEDIUMINT_MAX;
        break;
      case common::ColumnType::INT:
        max_value = unsigned_flag ? TIANMU_INT_UNSIGNED_MAX : TIANMU_INT_MAX;
        break;
      case common::ColumnType::BIGINT:
        max_value = unsigned_flag ? common::TIANMU_BIGINT_UNSIGNED_MAX : common::TIANMU_BIGINT_MAX;
        break;
      default:
        break;
    }
    return max_value >= auto_inc_value ? auto_inc_value : max_value;  // not exceed max value
  }
  void RollBackIfAutoInc() {
    if (!GetIfAutoInc())
      return;
    m_share->auto_inc_.store(backup_auto_inc_next_);
    return;
  }
  uint64_t GetAutoInc() const { return m_share->auto_inc_.load(); }
  uint64_t GetAutoIncInfo() const { return m_share->auto_inc_.load() + 1; }
  void SetAutoInc(uint64_t v) {
    backup_auto_inc_next_ = m_share->auto_inc_.load();
    m_share->auto_inc_.store(v);
  }
  // Original 0-level value (text, not null-terminated) and its length; binary
  // data types may be displayed as hex
  void GetValueBin(int64_t obj, size_t &size, char *val_buf);

  // size of original 0-level value (text/binary, not null-terminated)
  size_t GetLength(int64_t obj);

  // lookup_to_num=true to return a number instead of string
  // should be removed to get rid of types::TianmuValueObject class
  types::TianmuValueObject GetValue(int64_t obj, bool lookup_to_num = false) override;
  types::TianmuDataType &GetValueData(size_t obj, types::TianmuDataType &value, bool lookup_to_num = false);

  int64_t GetNumOfNulls(int pack) override;
  bool IsRoughNullsOnly() const override { return hdr.numOfRecords == hdr.numOfNulls; }
  size_t GetNumOfValues(int pack) const { return get_dpn(pack).numOfRecords; }
  int64_t GetSum(int pack, bool &nonnegative) override;
  size_t GetActualSize(int pack);
  int64_t GetMinInt64(int pack) override;
  int64_t GetMaxInt64(int pack) override;
  types::BString GetMaxString(int pack) override;
  types::BString GetMinString(int pack) override;
  size_t GetPrefixLength(int pack);
  types::BString DecodeValue_S(int64_t code) override;
  int EncodeValue_S(types::BString &v) override { return EncodeValue_T(v); }
  // 1-level code value for a given 0-level (text) value, if new_val then add to
  // dictionary if not present
  int EncodeValue_T(const types::BString &tianmu_s, bool new_val = false, common::ErrorCode *tianmu_err_code = 0);
  // no changes for REAL; rounded=true iff v has greater precision than the
  // column and the returned result is rounded down
  int64_t EncodeValue64(types::TianmuDataType *v, bool &rounded,
                        common::ErrorCode *tianmu_err_code = 0);  // as above
  int64_t EncodeValue64(const types::TianmuValueObject &v, bool &rounded, common::ErrorCode *tianmu_err_code = 0);

  // Query execution
  void EvaluatePack(MIUpdatingIterator &mit, int dim, Descriptor &desc) override;
  common::ErrorCode EvaluateOnIndex(MIUpdatingIterator &mit, int dim, Descriptor &desc, int64_t limit) override;
  bool TryToMerge(Descriptor &d1,
                  Descriptor &d2) override;  // true, if d2 is no longer needed

  // provide the best upper approximation of number of diff. values (incl.null,
  // if flag set)
  uint64_t ApproxDistinctVals(bool incl_nulls, Filter *f, common::RoughSetValue *rf,
                              bool outer_nulls_possible) override;

  // provide the exact number of diff. non-null values, if possible, or
  // common::NULL_VALUE_64
  uint64_t ExactDistinctVals(Filter *f) override;

  // provide the most probable approximation of number of objects matching the
  // condition
  uint64_t ApproxAnswerSize(Descriptor &d) override;

  // maximal byte string length in column
  size_t MaxStringSize(Filter *f = nullptr) override;
  bool IsDistinct(Filter *f) override;
  // for numerical: best rough approximation of min/max for a given filter (or
  // global min if filter is nullptr) or rough filter
  int64_t RoughMin(Filter *f, common::RoughSetValue *rf = nullptr) override;
  int64_t RoughMax(Filter *f, common::RoughSetValue *rf = nullptr) override;

  // Rough queries and indexes
  // Note that you should release all indexes after using a series of
  // RoughChecks!

  // check whether any value from the pack may meet the condition
  common::RoughSetValue RoughCheck(int pack, Descriptor &d, bool additional_nulls_possible) override;
  // check whether any pair from two packs of two different attr/tables may meet
  // the condition
  common::RoughSetValue RoughCheck(int pack1, int pack2, Descriptor &d) override;
  // check whether any value from the pack may meet the condition "... BETWEEN
  // min AND max"
  common::RoughSetValue RoughCheckBetween(int pack, int64_t min, int64_t max) override;

  // calculate the number of 1's in histograms and other KN stats
  void RoughStats(double &hist_density, int &trivial_packs, double &span);
  void DisplayAttrStats(Filter *f) override;  // filter is for # of objects in packs
  double RoughSelectivity() override;
  void GetTextStat(types::TextStat &s, Filter *f = nullptr) override;

  std::vector<int64_t> GetListOfDistinctValuesInPack(int pack) override;

  void LoadData(loader::ValueCache *nvs, Transaction *conn_info = nullptr);
  void LoadPackInfo(Transaction *trans_ = current_txn_);
  void LoadProcessedData([[maybe_unused]] std::unique_ptr<system::Stream> &s,
                         [[maybe_unused]] size_t no_rows){/* TODO */};
  void PreparePackForLoad();

  bool SaveVersion();  // return true iff there was any change
  void Rollback();
  void PostCommit();

  int Cardinality() const { return m_dict->CountOfUniqueValues(); }
  types::BString GetRealString(int i) { return m_dict->GetRealValue(i); }
  std::shared_ptr<Pack> Fetch(const PackCoordinate &coord) override;
  std::shared_ptr<FTree> Fetch(const FTreeCoordinate &coord) override;
  uint32_t ColId() const { return m_share->col_id; }

 private:
  void LoadVersion(common::TX_ID xid);
  void SaveFilters();
  void RefreshFilter(common::PACK_INDEX pi);
  void UpdateRSI_Hist(common::PACK_INDEX pi);
  void UpdateRSI_CMap(common::PACK_INDEX pi);
  void UpdateRSI_Bloom(common::PACK_INDEX pi);
  void LoadDataPackN(size_t i, loader::ValueCache *nvs);
  void LoadDataPackS(size_t i, loader::ValueCache *nvs);

  void CompareAndSetCurrentMin(const types::BString &tstmp, types::BString &min, bool set);
  void CompareAndSetCurrentMax(const types::BString &tstmp, types::BString &min);

  types::BString MinS(Filter *f);
  types::BString MaxS(Filter *f);

  PackCoordinate get_pc(common::PACK_INDEX pi) const { return PackCoordinate{m_tid, m_cid, int(m_idx[pi])}; }
  DPN &get_dpn(size_t i) {
    ASSERT(i < m_idx.size(), "bad dpn index " + std::to_string(i) + "/" + std::to_string(m_idx.size()));
    return *m_share->get_dpn_ptr(m_idx[i]);
  }
  const DPN &get_dpn(size_t i) const {
    ASSERT(i < m_idx.size(), "bad dpn index " + std::to_string(i) + "/" + std::to_string(m_idx.size()));
    return *m_share->get_dpn_ptr(m_idx[i]);
  }

  Pack *get_pack(size_t i);
  PackInt *get_packN(size_t i) { return reinterpret_cast<PackInt *>(get_pack(i)); }
  PackStr *get_packS(size_t i) { return reinterpret_cast<PackStr *>(get_pack(i)); }
  Pack *get_pack(size_t i) const;
  PackInt *get_packN(size_t i) const { return reinterpret_cast<PackInt *>(get_pack(i)); }
  PackStr *get_packS(size_t i) const { return reinterpret_cast<PackStr *>(get_pack(i)); }
  DPN &get_last_dpn() { return *m_share->get_dpn_ptr(m_idx.back()); }
  const DPN &get_last_dpn() const { return *m_share->get_dpn_ptr(m_idx.back()); }

  void EvaluatePack_IsNoDelete(MIUpdatingIterator &mit, int dim);
  void EvaluatePack_IsNull(MIUpdatingIterator &mit, int dim);
  void EvaluatePack_NotNull(MIUpdatingIterator &mit, int dim);
  void EvaluatePack_Like(MIUpdatingIterator &mit, int dim, Descriptor &d);
  void EvaluatePack_Like_UTF(MIUpdatingIterator &mit, int dim, Descriptor &d);
  void EvaluatePack_InString(MIUpdatingIterator &mit, int dim, Descriptor &d);
  void EvaluatePack_InString_UTF(MIUpdatingIterator &mit, int dim, Descriptor &d);
  void EvaluatePack_InNum(MIUpdatingIterator &mit, int dim, Descriptor &d);
  void EvaluatePack_BetweenString(MIUpdatingIterator &mit, int dim, Descriptor &d);
  void EvaluatePack_BetweenString_UTF(MIUpdatingIterator &mit, int dim, Descriptor &d);
  void EvaluatePack_BetweenInt(MIUpdatingIterator &mit, int dim, Descriptor &d);
  void EvaluatePack_BetweenReal(MIUpdatingIterator &mit, int dim, Descriptor &d);
  void EvaluatePack_AttrAttr(MIUpdatingIterator &mit, int dim, Descriptor &d);
  void EvaluatePack_AttrAttrReal(MIUpdatingIterator &mit, int dim, Descriptor &d);

  common::ErrorCode EvaluateOnIndex_BetweenInt(MIUpdatingIterator &mit, int dim, Descriptor &d, int64_t limit);
  common::ErrorCode EvaluateOnIndex_BetweenString(MIUpdatingIterator &mit, int dim, Descriptor &d, int64_t limit);
  common::ErrorCode EvaluateOnIndex_BetweenString_UTF(MIUpdatingIterator &mit, int dim, Descriptor &d, int64_t limit);
  common::PACK_INDEX row2pack(size_t row) const { return row >> pss; }
  int row2offset(size_t row) const { return row % (1L << pss); }
  const fs::path &Path() const { return m_share->m_path; }
  std::string GetFieldName() const { return m_share->GetFieldName(); }

 private:
  COL_VER_HDR hdr{};
  common::TX_ID m_version;  // the read-from version
  Transaction *m_tx;
  int m_tid;
  int m_cid;
  ColumnShare *m_share;

  std::shared_ptr<FTree> m_dict;
  std::vector<common::PACK_INDEX> m_idx;

  bool no_change = true;
  uint64_t backup_auto_inc_next_{0};

  // local filters for write session
  std::shared_ptr<RSIndex_Hist> filter_hist;
  std::shared_ptr<RSIndex_CMap> filter_cmap;
  std::shared_ptr<RSIndex_Bloom> filter_bloom;

  std::shared_ptr<RSIndex_Hist> GetFilter_Hist();
  std::shared_ptr<RSIndex_CMap> GetFilter_CMap();
  std::shared_ptr<RSIndex_Bloom> GetFilter_Bloom();
  uint8_t pss;
  common::PackType pack_type;
  double rough_selectivity = -1;  // a probability that simple condition "c = 100" needs to open a data
                                  // pack, providing KNs etc.
  std::function<std::shared_ptr<RSIndex>(const FilterCoordinate &co)> filter_creator;
};

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_TIANMU_ATTR_H_
