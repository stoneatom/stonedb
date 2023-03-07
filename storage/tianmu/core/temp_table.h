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
#ifndef TIANMU_CORE_TEMP_TABLE_H_
#define TIANMU_CORE_TEMP_TABLE_H_
#pragma once

#include <vector>

#include "common/common_definitions.h"
#include "core/cq_term.h"
#include "core/descriptor.h"
#include "core/just_a_table.h"
#include "core/mi_updating_iterator.h"
#include "core/multi_index.h"
#include "core/mysql_expression.h"
#include "core/pack_orderer.h"
#include "core/parameterized_filter.h"
#include "core/physical_column.h"
#include "core/sorter_wrapper.h"
#include "exporter/data_exporter.h"
#include "system/large_buffer.h"

namespace Tianmu {
namespace core {

class Descriptor;
class Filter;
class Query;
class TianmuTable;
class ResultSender;
class SortDescriptor;
class Transaction;

// Sepecial Instruction
struct SI {
  std::string separator;  // group_concat separator
  ORDER::enum_order order;
};

//  TempTable - for storing a definition or content of a temporary table (view)

class TempTable : public JustATable {
 public:
  class Attr final : public PhysicalColumn {
   public:
    SI si;
    void *buffer;             // buffer to values of attribute, if materialized
    int64_t no_obj;           // number of objects in the buffer
    uint32_t no_power;        // number of objects in the buffer
    int64_t no_materialized;  // number of objects already set in the buffer
    uint page_size;           // size of one page of buffered values
    char *alias;
    common::ColOperation mode;
    bool distinct;  // distinct modifier for aggregations
    CQTerm term;
    int dim;  // dimension of a column, i.e., index of source table,
    // -1 means there is no corresponding table: constant, count(*)
    uint orig_precision;
    bool not_complete;  // does not contain all the column elements - some
                        // functions cannot be computed

    Attr(CQTerm t, common::ColOperation m, uint32_t power, bool distinct = false, char *alias = nullptr, int dim = -1,
         common::ColumnType type = common::ColumnType::INT, uint scale = 0, uint precision = 10, bool notnull = true,
         DTCollation collation = DTCollation(), SI *si1 = nullptr);
    Attr(const Attr &);
    Attr &operator=(const Attr &);
    int operator==(const Attr &);
    ~Attr();

    bool IsListField() const { return (mode == common::ColOperation::LISTING) && term.vc; };
    bool ShouldOutput() const { return alias && IsListField(); }
    bool NeedFill() const {
      return ((mode == common::ColOperation::LISTING) && term.vc && alias) ||
             !term.vc->IsConst();  // constant value, the buffer is already
                                   // filled in
    }

    enum phys_col_t ColType() const override { return phys_col_t::ATTR; }
    //! Use in cases where actual string length is less than declared, before
    //! materialization of Attr
    void OverrideStringSize(int size) {
      ct.OverrideInternalSize(size);
      ct.SetPrecision(size);
    }
    void SetPrecision(int prec) { ct.SetPrecision(prec); }
    void SetScale(int sc) { ct.SetScale(sc); }
    void CreateBuffer(uint64_t size, Transaction *conn = nullptr, bool not_completed = false);
    void FillValue(const MIIterator &mii, size_t idx);
    size_t FillValues(MIIterator &mii, size_t start, size_t count);
    void SetNewPageSize(uint new_page_size);
    void SetValueString(int64_t obj, const types::BString &val);
    types::TianmuValueObject GetValue(int64_t obj, bool lookup_to_num = false) override;
    void GetValueString(int64_t row, types::BString &s) override { GetValueString(s, row); }
    void GetValueString(types::BString &s, int64_t row);
    void GetNotNullValueString(int64_t row, types::BString &s) override { GetValueString(s, row); }
    uint64_t ApproxDistinctVals(bool incl_nulls, Filter *f, common::RoughSetValue *rf,
                                bool outer_nulls_possible) override;  // provide the best upper
                                                                      // approximation of number of diff.
                                                                      // values (incl. null, if flag set)
    uint64_t ExactDistinctVals(Filter *f [[maybe_unused]]) override { return common::NULL_VALUE_64; }
    bool IsDistinct(Filter *f [[maybe_unused]]) override { return false; }
    size_t MaxStringSize(Filter *f = nullptr) override;  // maximal byte string length in column
    int64_t RoughMin(Filter *f [[maybe_unused]] = nullptr,
                     common::RoughSetValue *rf [[maybe_unused]] = nullptr) override {
      return common::MINUS_INF_64;
    }  // for numerical: best rough approximation of min for a given filter (or
       // global min if filter is nullptr)
    int64_t RoughMax(Filter *f [[maybe_unused]] = nullptr,
                     common::RoughSetValue *rf [[maybe_unused]] = nullptr) override {
      return common::PLUS_INF_64;
    }  // for numerical: best rough approximation of max for a given filter (or
       // global max if filter is nullptr)
    void DisplayAttrStats(Filter *f [[maybe_unused]]) override {}
    bool TryToMerge(Descriptor &d1 [[maybe_unused]], Descriptor &d2 [[maybe_unused]]) override { return false; }
    PackOntologicalStatus GetPackOntologicalStatus(int pack_no [[maybe_unused]]) override {
      return PackOntologicalStatus::NORMAL;
    }  // not implemented properly yet
    void ApplyFilter(MultiIndex &, int64_t offset, int64_t no_obj);
    void DeleteBuffer();

    bool IsNull(int64_t obj) const override;
    void SetNull(int64_t obj);
    void SetMinusInf(int64_t obj);
    void SetPlusInf(int64_t obj);
    int64_t GetValueInt64(int64_t obj) const override;
    int64_t GetNotNullValueInt64(int64_t obj) const override;
    void SetValueInt64(int64_t obj, int64_t val);
    void InvalidateRow(int64_t obj);
    int64_t GetMinInt64(int pack) override;
    int64_t GetMaxInt64(int pack) override;
    types::BString GetMaxString(int pack) override;
    types::BString GetMinString(int pack) override;
    int64_t GetNumOfNulls(int pack) override;
    bool IsRoughNullsOnly() const override { return false; }
    int64_t GetSum(int pack, bool &nonnegative) override;
    int NumOfAttr() const override { return -1; }
    common::RoughSetValue RoughCheck(int pack [[maybe_unused]], Descriptor &d [[maybe_unused]],
                                     bool additional_nulls_possible [[maybe_unused]]) override {
      return common::RoughSetValue::RS_SOME;
    }
    common::RoughSetValue RoughCheck(int pack1 [[maybe_unused]], int pack2 [[maybe_unused]],
                                     Descriptor &d [[maybe_unused]]) override {
      return common::RoughSetValue::RS_SOME;
    }
    // as far as Attr is not pack oriented the function below should not be
    // called
    void EvaluatePack(MIUpdatingIterator &mit [[maybe_unused]], int dim [[maybe_unused]],
                      Descriptor &desc [[maybe_unused]]) override {
      DEBUG_ASSERT(0);
    }
    common::ErrorCode EvaluateOnIndex(MIUpdatingIterator &mit [[maybe_unused]], int dim [[maybe_unused]],
                                      Descriptor &desc [[maybe_unused]], int64_t limit [[maybe_unused]]) override {
      TIANMU_ERROR("To be implemented.");
      return common::ErrorCode::FAILED;
    }
    types::BString DecodeValue_S(int64_t code [[maybe_unused]]) override {
      DEBUG_ASSERT(0);
      return types::BString();
    }  // TianmuAttr only
    int EncodeValue_S(types::BString &v [[maybe_unused]]) override {
      DEBUG_ASSERT(0);
      return -1;
    }  // lookup (physical) only
  };

  struct TableMode {
    bool distinct;
    bool top;
    bool exists;
    int64_t param1, param2;  // e.g., TOP(5), LIMIT(2,5)
    TableMode() : distinct(false), top(false), exists(false), param1(0), param2(-1) {}
  };

 protected:
  TempTable(const TempTable &, bool is_vc_owner);
  TempTable(JustATable *const, int alias, Query *q);

  std::shared_ptr<TempTable> CreateMaterializedCopy(bool translate_order,
                                                    bool in_subq);  // move all buffers to a newly created
                                                                    // TempTable, make this VC-pointing to it;
                                                                    // the new table must be deleted by
                                                                    // DeleteMaterializedCopy()
  void DeleteMaterializedCopy(std::shared_ptr<TempTable> &t_new);   // delete the external table and remove VC
                                                                    // pointers, make this fully materialized
  int mem_scale;
  std::map<PhysicalColumn *, PhysicalColumn *> attr_back_translation;
  //	TempTable* subq_template;

 public:
  virtual ~TempTable();
  void TranslateBackVCs();
  // Query execution (CompiledQuery language implementation)
  void AddConds(Condition *cond, CondType type);
  void AddInnerConds(Condition *cond, std::vector<TabID> &dims);
  void AddLeftConds(Condition *cond, std::vector<TabID> &dims1, std::vector<TabID> &dims2);
  void SetMode(TMParameter mode, int64_t mode_param1 = 0, int64_t mode_param2 = -1);
  void JoinT(JustATable *t, int alias, JoinType jt);
  int AddColumn(CQTerm, common::ColOperation, char *alias, bool distinct, SI si);
  void AddOrder(vcolumn::VirtualColumn *vc, int direction);
  void Union(TempTable *, int);
  void Union(TempTable *, int, ResultSender *sender, int64_t &g_offset, int64_t &g_limit);
  void RoughUnion(TempTable *, ResultSender *sender);

  void ForceFullMaterialize() { force_full_materialize = true; }
  // Maintenance and low-level functions

  bool OrderByAndMaterialize(std::vector<SortDescriptor> &ord, int64_t limit, int64_t offset,
                             ResultSender *sender = nullptr);  // Sort data contained in
                                                               // ParameterizedFilter by using some
                                                               // attributes (usually specified by
                                                               // AddOrder, but in general -
                                                               // arbitrary)
  // just materialize as SELECT *
  void FillMaterializedBuffers(int64_t local_limit, int64_t local_offset, ResultSender *sender, bool pagewise);

  virtual void RoughMaterialize(bool in_subq = false, ResultSender *sender = nullptr, bool lazy = false);
  virtual void Materialize(bool in_subq = false, ResultSender *sender = nullptr, bool lazy = false);

  // just_distinct = 'select distinct' but no 'group by'
  // Set no_obj = no. of groups in result
  void RoughAggregate(ResultSender *sender);
  void RoughAggregateCount(DimensionVector &dims, int64_t &min_val, int64_t &max_val, bool group_by_present);
  void RoughAggregateMinMax(vcolumn::VirtualColumn *vc, int64_t &min_val, int64_t &max_val);
  void RoughAggregateSum(vcolumn::VirtualColumn *vc, int64_t &min_val, int64_t &max_val,
                         std::vector<Attr *> &group_by_attrs, bool nulls_only, bool distinct_present);
  void VerifyAttrsSizes();  // verifies attr[i].field_size basing on the current
                            // multiindex contents

  void SuspendDisplay();
  void ResumeDisplay();
  void LockPackForUse(unsigned attr, unsigned pack_no) override;
  void UnlockPackFromUse(unsigned attr [[maybe_unused]], unsigned pack_no [[maybe_unused]]) override {}
  int64_t NumOfObj() const override { return no_obj; }
  uint32_t Getpackpower() const override { return p_power; }
  int64_t NumOfMaterialized() { return no_materialized; }
  void SetNumOfObj(int64_t n) { no_obj = n; }
  void SetNumOfMaterialized(int64_t n) {
    no_obj = n;
    no_materialized = n;
  }
  TType TableType() const override { return TType::TEMP_TABLE; }  // type of JustATable - TempTable
  uint NumOfAttrs() const override { return (uint)attrs.size(); }
  uint NumOfDisplaybleAttrs() const override { return no_cols; }  // no. of columns with defined alias
  bool IsDisplayAttr(int i) { return attrs[i]->alias != nullptr; }
  int64_t GetTable64(int64_t obj, int attr) override;
  void GetTable_S(types::BString &s, int64_t obj, int attr) override;
  void GetTableString(types::BString &s, int64_t obj, uint attr);
  types::TianmuValueObject GetValueObject(int64_t obj, uint attr);

  uint64_t ApproxAnswerSize(int attr,
                            Descriptor &d);  // provide the most probable
                                             // approximation of number of
                                             // objects matching the condition

  bool IsNull(int64_t obj,
              int attr) override;  // return true if the value of attr. is null

  int64_t RoughMin(int n_a [[maybe_unused]], Filter *f [[maybe_unused]] = nullptr) { return common::MINUS_INF_64; }
  int64_t RoughMax(int n_a [[maybe_unused]], Filter *f [[maybe_unused]] = nullptr) { return common::PLUS_INF_64; }

  uint MaxStringSize(int n_a, Filter *f [[maybe_unused]] = nullptr) override {
    if (n_a < 0)
      return GetFieldSize(-n_a - 1);
    return GetFieldSize(n_a);
  }

  uint NumOfDimensions() { return filter.mind_ ? (uint)filter.mind_->NumOfDimensions() : 1; }
  int GetDimension(TabID alias);
  std::vector<AttributeTypeInfo> GetATIs(bool orig = false) override;
  int GetAttrScale(int a) {
    DEBUG_ASSERT(a >= 0 && (uint)a < NumOfAttrs());
    return attrs[a]->Type().GetScale();
  }

  int GetAttrSize(int a) {
    DEBUG_ASSERT(a >= 0 && (uint)a < NumOfAttrs());
    return attrs[a]->Type().GetDisplaySize();
  }

  uint GetFieldSize(int a) {
    DEBUG_ASSERT(a >= 0 && (uint)a < NumOfAttrs());
    return attrs[a]->Type().GetInternalSize();
  }

  int GetNumOfDigits(int a) {
    DEBUG_ASSERT(a >= 0 && (uint)a < NumOfAttrs());
    return attrs[a]->Type().GetPrecision();
  }

  const ColumnType &GetColumnType(int a) override {
    DEBUG_ASSERT(a >= 0 && (uint)a < NumOfAttrs());
    return attrs[a]->Type();
  }

  PhysicalColumn *GetColumn(int a) override {
    DEBUG_ASSERT(a >= 0 && (uint)a < NumOfAttrs());
    return attrs[a];
  }

  Attr *GetAttrP(uint a) {
    DEBUG_ASSERT(a < (uint)NumOfAttrs());
    return attrs[a];
  }

  Attr *GetDisplayableAttrP(uint i) {
    DEBUG_ASSERT(!displayable_attr.empty());
    return displayable_attr[i];
  }
  void CreateDisplayableAttrP();
  uint GetDisplayableAttrIndex(uint attr);
  MultiIndex *GetMultiIndexP() { return filter.mind_; }
  void ClearMultiIndexP() {
    if (nullptr == filter.mind_) {
      return;
    }

    if (filter_shallow_memory) {
      return;
    }

    delete filter.mind_;
    filter.mind_ = nullptr;
  }
  MultiIndex *GetOutputMultiIndexP() { return &output_mind; }
  ParameterizedFilter *GetFilterP() { return &filter; }
  JustATable *GetTableP(uint dim) {
    DEBUG_ASSERT(dim < tables.size());
    return tables[dim];
  }

  int NumOfTables() const { return int(tables.size()); }
  std::vector<int> &GetAliases() { return aliases; }
  std::vector<JustATable *> &GetTables() { return tables; }
  bool IsMaterialized() { return materialized; }
  void SetAsMaterialized() { materialized = true; }
  // materialized with order by and limits
  bool IsFullyMaterialized() { return materialized && ((order_by.size() == 0 && !mode.top) || !no_obj); }
  uint CalculatePageSize(int64_t no_obj = -1);  // computes number of output records kept in memory
  // based on no_obj and some upper limit CACHE_SIZE (100MB)
  void SetPageSize(int64_t new_page_size);
  void SetOneOutputRecordSize(uint size) { size_of_one_record = size; }
  uint GetOneOutputRecordSize() { return size_of_one_record; }
  int64_t GetPageSize() {
    DEBUG_ASSERT(attrs[0]);
    return attrs[0]->page_size;
  }

  void DisplayRSI();  // display info about all RSI contained in TianmuTables from
                      // this TempTable

  bool HasHavingConditions() { return having_conds.Size() > 0; }
  bool CheckHavingConditions(MIIterator &it) { return having_conds[0].tree->root->CheckCondition(it); }
  void ClearHavingConditions() { having_conds.Clear(); }
  void RemoveFromManagedList(const TianmuTable *rct);
  int AddVirtColumn(vcolumn::VirtualColumn *vc);
  int AddVirtColumn(vcolumn::VirtualColumn *vc, int no);
  uint NumOfVirtColumns() { return uint(virt_cols.size()); }
  void ReserveVirtColumns(int no);
  vcolumn::VirtualColumn *GetVirtualColumn(uint col_num) {
    DEBUG_ASSERT(col_num < virt_cols.size());
    return virt_cols[col_num];
  }
  void SetVCDistinctVals(int dim, int64_t val);  // set dist. vals for all vc of this dimension
  void ResetVCStatistics();
  void ProcessParameters(const MIIterator &mit, const int alias);
  void RoughProcessParameters(const MIIterator &mit, const int alias);
  int DimInDistinctContext();  // return a dimension number if it is used only
                               // in contexts where row repetitions may be
                               // omitted, e.g. distinct
  Condition &GetWhereConds() { return filter.GetConditions(); }
  void MoveVC(int colnum, std::vector<vcolumn::VirtualColumn *> &from, std::vector<vcolumn::VirtualColumn *> &to);
  void MoveVC(vcolumn::VirtualColumn *vc, std::vector<vcolumn::VirtualColumn *> &from,
              std::vector<vcolumn::VirtualColumn *> &to);
  void FillbufferTask(Attr *attr, Transaction *txn, MIIterator *page_start, int64_t start_row, int64_t page_end);
  size_t TaskPutValueInST(MIIterator *it, Transaction *ci, SorterWrapper *st);
  bool HasTempTable() const { return has_temp_table; }

  void MarkCondPush() { can_cond_push_down = true; };
  bool CanCondPushDown() { return can_cond_push_down; };

 protected:
  int64_t no_obj;
  uint32_t p_power;                      // pack power
  uint no_cols;                          // no. of output columns, i.e., with defined alias
  TableMode mode;                        // based on { TM_DISTINCT, TM_TOP, TM_EXISTS }
  std::vector<Attr *> attrs;             // vector of output columns, each column contains
                                         // a buffer with values
  std::vector<Attr *> displayable_attr;  // just a shortcut: some of attrs
  Condition having_conds;
  std::vector<JustATable *> tables;                 // vector of pointers to source tables
  std::vector<int> aliases;                         // vector of aliases of source tables
  std::vector<vcolumn::VirtualColumn *> virt_cols;  // vector of virtual columns defined for TempTable
  std::vector<bool> virt_cols_for_having;           // is a virt column based on output_mind (true) or
                                                    // source mind_ (false)
  std::vector<JoinType> join_types;                 // vector of types of joins, one less than tables
  ParameterizedFilter filter;                       // multidimensional filter, contains multiindex,
                                                    // can be parametrized
  bool filter_shallow_memory = false;               // is filter shallow memory
  MultiIndex output_mind;                           // one dimensional MultiIndex used for operations on
                                                    // output columns of TempTable
  std::vector<SortDescriptor> order_by;             // indexes of order by columns
  bool group_by = false;                            // true if there is at least one grouping column
  bool is_vc_owner = true;                          // true if temptable should dealocate virtual columns
  int no_global_virt_cols;                          // keeps number of virtual columns. In case for_subq
                                                    // == true, all locally created
  // virtual columns will be removed
  bool is_sent;  // true if result of materialization was sent to MySQL

  // some internal functions for low-level query execution
  static const uint CACHE_SIZE = 100000000;  // size of memory cache for data in the materialized TempTable
  // everything else is cached on disk (CachedBuffer)
  bool IsParametrized();  // is the temptable (select) parametrized?
  void SendResult(int64_t local_limit, int64_t local_offset, ResultSender &sender, bool pagewise);

  void ApplyOffset(int64_t limit,
                   int64_t offset);  // apply limit and offset to attr buffers

  bool materialized = false;
  bool has_temp_table = false;

  bool lazy;                       // materialize on demand, page by page
  int64_t no_materialized;         // if lazy - how many are ready
  common::Tribool rough_is_empty;  // rough value specifying if there is
                                   // non-empty result of a query
  bool force_full_materialize = false;
  bool can_cond_push_down = false;  // Conditional push occurs

  // Refactoring: extracted small methods
  bool CanOrderSources();
  bool LimitMayBeAppliedToWhere();
  ColumnType GetUnionType(ColumnType type1, ColumnType type2);
  bool SubqueryInFrom();
  uint size_of_one_record;

 public:
  class RecordIterator;
  virtual RecordIterator begin(Transaction *conn = nullptr);
  virtual RecordIterator end(Transaction *conn = nullptr);

 public:
  Transaction *m_conn;  // external pointer

  void Display(std::ostream &out = std::cout);  // output to console
  static std::shared_ptr<TempTable> Create(const TempTable &, bool in_subq);
  static std::shared_ptr<TempTable> Create(JustATable *const, int alias, Query *q, bool for_subquery = false);
  bool IsSent() { return is_sent; }
  void SetIsSent() { is_sent = true; }
  common::Tribool RoughIsEmpty() { return rough_is_empty; }

 public:
  class Record final {
   public:
    Record(RecordIterator &it) : m_it(it) {}
    Record(Record const &it) = default;
    types::TianmuDataType &operator[](size_t i) const { return *(m_it.dataTypes[i]); }
    RecordIterator &m_it;
  };

  /*! \brief RecordIterator class for _table records.
   */
  class RecordIterator final {
   public:
    TempTable *table;
    uint64_t _currentRNo;
    Transaction *_conn;
    bool is_prepared;

    std::vector<std::unique_ptr<types::TianmuDataType>> dataTypes;

   public:
    RecordIterator();
    RecordIterator(const RecordIterator &riter);
    ~RecordIterator() = default;
    RecordIterator &operator=(const RecordIterator &ri) = delete;

    bool operator==(RecordIterator const &i) const;
    bool operator!=(RecordIterator const &i) const;
    RecordIterator &operator++();
    Record operator*() {
      if (!is_prepared) {
        PrepareValues();
        is_prepared = true;
      }
      return (Record(*this));
    }
    uint64_t currentRowNumber() { return _currentRNo; }
    TempTable *Owner() const { return (table); }

   private:
    RecordIterator(TempTable *, Transaction *, uint64_t = 0);
    void PrepareValues();

   private:
    friend class TempTable;
    friend class Record;
  };
};

class TempTableForSubquery : public TempTable {
 public:
  TempTableForSubquery(JustATable *const t, int alias, Query *q)
      : TempTable(t, alias, q), template_filter(0), is_attr_for_rough(false), rough_materialized(false){};
  ~TempTableForSubquery();

  void CreateTemplateIfNotExists();
  void Materialize(bool in_subq = false, ResultSender *sender = nullptr, bool lazy = false) override;
  void RoughMaterialize(bool in_subq = false, ResultSender *sender = nullptr, bool lazy = false) override;
  void ResetToTemplate(bool rough, bool use_filter_shallow = false);
  void SetAttrsForRough();
  void SetAttrsForExact();

 private:
  ParameterizedFilter *template_filter;
  Condition template_having_conds;
  TableMode template_mode;
  std::vector<SortDescriptor> template_order_by;
  bool is_attr_for_rough;
  bool rough_materialized;
  std::vector<Attr *> attrs_for_exact;
  std::vector<Attr *> attrs_for_rough;
  std::vector<Attr *> template_attrs;
  std::vector<vcolumn::VirtualColumn *> template_virt_cols;
};

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_TEMP_TABLE_H_
