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

#include <dirent.h>
#include <time.h>
#include <fstream>

#include "common/common_definitions.h"
#include "common/exception.h"
#include "core/engine.h"
#include "core/pack_guardian.h"
#include "core/rc_attr.h"
#include "core/table_share.h"
#include "core/transaction.h"
#include "handler/stonedb_handler.h"
#include "loader/load_parser.h"
#include "system/channel.h"
#include "system/file_system.h"
#include "system/net_stream.h"
#include "types/value_parser4txt.h"
#include "util/bitset.h"
#include "util/timer.h"
#include "vc/virtual_column.h"

#include "core/rc_table.h"

namespace stonedb {
namespace core {
uint32_t RCTable::GetTableId(const fs::path &dir) {
  TABLE_META meta;
  system::StoneDBFile f;
  f.OpenReadOnly(dir / common::TABLE_DESC_FILE);
  f.ReadExact(&meta, sizeof(meta));
  return meta.id;
}

void RCTable::CreateNew(const std::shared_ptr<TableOption> &opt) {
  uint32_t tid = rceng->GetNextTableId();
  auto &path(opt->path);
  uint32_t no_attrs = opt->atis.size();

  fs::create_directory(path);

  TABLE_META meta{common::FILE_MAGIC, common::TABLE_DATA_VERSION, tid, opt->pss};

  system::StoneDBFile ftbl;
  ftbl.OpenCreateEmpty(path / common::TABLE_DESC_FILE);
  ftbl.WriteExact(&meta, sizeof(meta));
  ftbl.Flush();
  ftbl.Close();

  auto zero = common::TABLE_VERSION_PREFIX + common::TX_ID(0).ToString();
  std::ofstream ofs(path / zero);
  common::TX_ID zid(0);
  for (size_t idx = 0; idx < no_attrs; idx++) {
    ofs.write(reinterpret_cast<char *>(&zid), sizeof(zid));
  }
  ofs.flush();

  fs::create_symlink(zero, path / common::TABLE_VERSION_FILE);

  auto column_path = path / common::COLUMN_DIR;
  fs::create_directories(column_path);

  for (size_t idx = 0; idx < no_attrs; idx++) {
    auto dir = Engine::GetNextDataDir();
    dir /= std::to_string(tid) + "." + std::to_string(idx);
    if (system::DoesFileExist(dir)) {
      throw common::DatabaseException("Directory " + dir.string() + " already exists!");
    }
    fs::create_directory(dir);
    auto lnk = column_path / std::to_string(idx);
    fs::create_symlink(dir, lnk);

    RCAttr::Create(lnk, opt->atis[idx], opt->pss, 0);
    // STONEDB_LOG(LogCtl_Level::INFO, "Column %zu at %s", idx, dir.c_str());
  }
  STONEDB_LOG(LogCtl_Level::INFO, "Create table %s, ID = %u", opt->path.c_str(), tid);
}

void RCTable::Alter(const std::string &table_path, std::vector<Field *> &new_cols, std::vector<Field *> &old_cols,
                    size_t no_objs) {
  fs::path tmp_dir = table_path + ".tmp";
  fs::path tab_dir = table_path + common::STONEDB_EXT;

  fs::copy(tab_dir, tmp_dir, fs::copy_options::recursive | fs::copy_options::copy_symlinks);

  for (auto &p : fs::directory_iterator(tmp_dir / common::COLUMN_DIR)) fs::remove(p);

  TABLE_META meta;
  system::StoneDBFile f;
  f.OpenReadOnly(tab_dir / common::TABLE_DESC_FILE);
  f.ReadExact(&meta, sizeof(meta));
  meta.id = rceng->GetNextTableId();  // only table id is updated

  system::StoneDBFile tempf;
  tempf.OpenReadWrite(tmp_dir / common::TABLE_DESC_FILE);
  tempf.WriteExact(&meta, sizeof(meta));
  tempf.Flush();

  std::vector<common::TX_ID> old_versions(old_cols.size());
  {
    system::StoneDBFile f;
    f.OpenReadOnly(tab_dir / common::TABLE_VERSION_FILE);
    f.ReadExact(&old_versions[0], old_cols.size() * sizeof(common::TX_ID));
  }

  std::vector<common::TX_ID> new_versions;
  for (size_t i = 0; i < new_cols.size(); i++) {
    size_t j;
    for (j = 0; j < old_cols.size(); j++)
      if (old_cols[j] != nullptr && std::strcmp(new_cols[i]->field_name, old_cols[j]->field_name) == 0) {
        old_cols[j] = nullptr;
        break;
      }

    auto column_dir = tmp_dir / common::COLUMN_DIR / std::to_string(i);
    if (j < old_cols.size()) {  // column exists
      fs::copy_symlink(tab_dir / common::COLUMN_DIR / std::to_string(j), column_dir);
      new_versions.push_back(old_versions[j]);
      continue;
    }
    new_versions.push_back(0);
    // new column
    auto dir = Engine::GetNextDataDir();
    dir /= std::to_string(meta.id) + "." + std::to_string(i);
    fs::create_directory(dir);
    fs::create_symlink(dir, column_dir);
    RCAttr::Create(column_dir, Engine::GetAttrTypeInfo(*new_cols[i]), meta.pss, no_objs);
    STONEDB_LOG(LogCtl_Level::INFO, "Add column %s at %s", new_cols[i]->field_name, dir.c_str());
  }
  {
    system::StoneDBFile f;
    f.OpenCreate(tmp_dir / common::TABLE_VERSION_FILE);
    f.WriteExact(&new_versions[0], new_cols.size() * sizeof(common::TX_ID));
    f.Flush();
  }
  fs::resize_file(tmp_dir / common::TABLE_VERSION_FILE, new_cols.size() * sizeof(common::TX_ID));
  STONEDB_LOG(LogCtl_Level::INFO, "Altered table %s", table_path.c_str());
}

void RCTable::Truncate() {
  for (auto &attr : m_attrs) attr->Truncate();
  m_mem_table->Truncate(m_tx);
}

RCTable::RCTable(std::string const &p, TableShare *s, Transaction *tx) : share(s), m_tx(tx), m_path(p) {
  db_name = m_path.parent_path().filename().string();

  if (!fs::exists(m_path)) {
    throw common::NoTableFolderException(m_path);
  }

  m_attrs.reserve(share->NoCols());
  m_versions.resize(share->NoCols());

  auto sz = sizeof(decltype(m_versions)::value_type) * m_versions.size();
  ASSERT(sz == fs::file_size(fs::canonical(fs::read_symlink(m_path / common::TABLE_VERSION_FILE), m_path)),
         "bad version file size. expected " + std::to_string(sz));

  system::StoneDBFile f;
  f.OpenReadOnly(m_path / common::TABLE_VERSION_FILE);
  f.ReadExact(&m_versions[0], sz);

  for (uint32_t i = 0; i < share->NoCols(); i++) {
    auto &attr = m_attrs.emplace_back(
        std::make_unique<RCAttr>(m_tx, m_versions[i], i, share->TabID(), share->GetColumnShare(i)));
    attr->TrackAccess();
  }

  std::string normalized_path;
  std::string table_name = share->Path();
  if (!index::NormalizeName(table_name, normalized_path)) {
    throw common::Exception("Normalization wrong of table  " + share->Path());
  }
  m_mem_table = kvstore->FindMemTable(normalized_path);
}

void RCTable::LockPackInfoForUse() {
  for (auto &attr : m_attrs) {
    attr->Lock();
    attr->TrackAccess();
  }
}

void RCTable::UnlockPackInfoFromUse() {
  for (auto &attr : m_attrs) attr->Unlock();
}

void RCTable::LockPackForUse(unsigned attr, unsigned pack_no) {
  if (pack_no == 0xFFFFFFFF) return;
  m_attrs[attr]->LockPackForUse(pack_no);
}

void RCTable::UnlockPackFromUse(unsigned attr, unsigned pack_no) {
  if (pack_no == 0xFFFFFFFF) return;
  m_attrs[attr]->UnlockPackFromUse(pack_no);
}

int RCTable::GetID() const { return share->TabID(); }

std::vector<AttrInfo> RCTable::GetAttributesInfo() {
  std::vector<AttrInfo> info(NumOfAttrs());
  Verify();
  for (int j = 0; j < (int)NumOfAttrs(); j++) {
    info[j].type = m_attrs[j]->TypeName();
    info[j].size = m_attrs[j]->Type().GetPrecision();
    info[j].precision = m_attrs[j]->Type().GetScale();
    if (m_attrs[j]->Type().NotNull())
      info[j].no_nulls = true;
    else
      info[j].no_nulls = false;
    info[j].actually_unique = (m_attrs[j]->PhysicalColumn::IsDistinct() == common::RSValue::RS_ALL);
    info[j].uncomp_size = m_attrs[j]->ComputeNaturalSize();
    info[j].comp_size = m_attrs[j]->CompressedSize();
  }
  return info;
}

std::vector<AttributeTypeInfo> RCTable::GetATIs([[maybe_unused]] bool orig) {
  std::vector<AttributeTypeInfo> deas;
  for (uint j = 0; j < NumOfAttrs(); j++) {
    deas.emplace_back(m_attrs[j]->TypeName(), m_attrs[j]->Type().NotNull(), m_attrs[j]->Type().GetPrecision(),
                      m_attrs[j]->Type().GetScale(), false, m_attrs[j]->Type().GetCollation());
  }

  return deas;
}

const ColumnType &RCTable::GetColumnType(int col) { return m_attrs[col]->Type(); }

RCAttr *RCTable::GetAttr(int n_a) {
  m_attrs[n_a]->TrackAccess();
  return m_attrs[n_a].get();
}

// return ture when there is inconsistency
bool RCTable::Verify() {
  bool ok = true;
  uint64_t n_o = 0xFFFFFFFF;
  for (auto &attr : m_attrs) {
    if (n_o == 0xFFFFFFFF) n_o = attr->NumOfObj();
    if (n_o != attr->NumOfObj()) ok = false;
  }
  if (!ok) {
    std::stringstream ss;
    ss << "Error: columns in table " << m_path.string() << " are inconsistent. No. of records for each column:\n";
    for (auto &attr : m_attrs) ss << attr->AttrNo() << " : " << attr->NumOfObj() << std::endl;
    STONEDB_LOG(LogCtl_Level::ERROR, "%s", ss.str().c_str());
  }
  return !ok;
}

void RCTable::CommitVersion() {
  if (Verify()) throw common::DatabaseException("Data integrity is broken in table " + m_path.string());

  utils::result_set<bool> res;
  bool no_except = true;
  for (auto &attr : m_attrs) res.insert(rceng->load_thread_pool.add_task(&RCAttr::SaveVersion, attr.get()));

  std::vector<size_t> changed_columns;
  changed_columns.reserve(m_attrs.size());
  for (size_t i = 0; i < m_attrs.size(); i++) {
    try {
      if (res.get(i)) {
        m_versions[i] = m_tx->GetID();
        changed_columns.emplace_back(i);
      }
    } catch (std::exception &e) {
      no_except = false;
      STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
    } catch (...) {
      no_except = false;
      STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
    }
  }
  if (!no_except) {
    throw common::Exception("Parallel save attribute failed.");
  }

  if (changed_columns.empty()) return;

  auto v = common::TABLE_VERSION_PREFIX + m_tx->GetID().ToString();
  system::StoneDBFile fv;
  fv.OpenCreateEmpty(m_path / v);
  fv.WriteExact(&m_versions[0], m_versions.size() * sizeof(decltype(m_versions)::value_type));

  std::error_code ec;
  fs::create_symlink(v, m_path / common::TABLE_VERSION_FILE_TMP, ec);
  if (ec) {
    if (ec == std::errc::file_exists) {
      STONEDB_LOG(LogCtl_Level::WARN, "delete leftover tmp version file under %s", m_path.c_str());
      fs::remove(m_path / common::TABLE_VERSION_FILE_TMP);
    }
    fs::create_symlink(v, m_path / common::TABLE_VERSION_FILE_TMP);
  }

  // flush data with disk device
  if (stonedb_sysvar_sync_buffers) {
    for (auto &col : changed_columns) {
      fs::path dir = m_path / common::COLUMN_DIR / std::to_string(col);

      if (fs::is_symlink(dir)) dir = fs::canonical(fs::read_symlink(dir), m_path);

      if (!fs::is_directory(dir)) {
        STONEDB_LOG(LogCtl_Level::ERROR, "Not a directory: %s", dir.c_str());
        continue;
      }

      for (auto &it : fs::directory_iterator(dir)) {
        if (fs::is_regular_file(it.path())) {
          system::StoneDBFile fb;
          fb.OpenReadOnly(it.path().string());
          fb.Flush();
        }
      }
    }
    fv.Flush();
    system::FlushDirectoryChanges(m_path);
  }

  auto oldv = fs::canonical(fs::read_symlink(m_path / common::TABLE_VERSION_FILE), m_path);
  auto p = m_path / common::TABLE_VERSION_FILE;

  // the path 'p' will be atomically replaced
  fs::rename(m_path / common::TABLE_VERSION_FILE_TMP, p);

  int fd = ::open(p.c_str(), O_RDWR);
  if (fd < 0) {
    throw std::system_error(errno, std::system_category(), "open() " + p.string());
  }
  int ret = ::fsync(fd);
  if (ret != 0) {
    throw std::system_error(errno, std::system_category(), "fsync() " + p.string());
  }
  ::close(fd);

  // Calling fsync() does not necessarily ensure that the entry in the
  // directory containing the file has also reached disk.  For that an
  // explicit fsync() on a file descriptor for the directory is also needed.
  auto dir = ::opendir(m_path.c_str());
  if (dir == NULL) {
    throw std::system_error(errno, std::system_category(), "opendir() " + p.string());
  }

  std::shared_ptr<void> defer(nullptr, [dir](...) { ::closedir(dir); });

  fd = ::dirfd(dir);
  if (fd < 0) {
    throw std::system_error(errno, std::system_category(), "dirfd() " + m_path.string());
  }
  ret = ::fsync(fd);
  if (ret != 0) {
    throw std::system_error(errno, std::system_category(), "fsync() " + m_path.string());
  }

  fs::remove(oldv);

  // submited this as the current version
  share->CommitWrite(this);
}

void RCTable::PostCommit() {
  for (auto &attr : m_attrs) attr->PostCommit();
}

void RCTable::Rollback([[maybe_unused]] common::TX_ID xid, bool) {
  STONEDB_LOG(LogCtl_Level::INFO, "roll back table %s.%s", db_name.c_str(), m_path.c_str());
  for (auto &attr : m_attrs) attr->Rollback();
}

uint32_t RCTable::Getpackpower() const { return share->PackSizeShift(); }

void RCTable::DisplayRSI() {
  std::stringstream ss;

  ss << "--- RSIndices for " << m_path << " (tab.no. " << share->TabID() << ", "
     << ((NumOfObj() + (1 << Getpackpower()) - 1) >> Getpackpower()) << " packs) ---" << std::endl;
  ss << "Name               Triv.packs\tSpan\tHist.dens." << std::endl;
  ss << "-----------------------------------------------------------" << std::endl;
  for (size_t i = 0; i < m_attrs.size(); i++) {
    double hd = 0;
    double span = 0;
    int trivials = 0;
    m_attrs[i]->RoughStats(hd, trivials, span);
    auto si = std::to_string(i);
    ss << si;

    int display_limit = int(20) - si.length();
    for (int j = 0; j < display_limit; j++) ss << " ";
    rccontrol << trivials << "\t\t";
    if (span != -1)
      ss << int(span * 10000) / 100.0 << "%\t";
    else
      ss << "\t";
    if (hd != -1)
      ss << int(hd * 10000) / 100.0 << "%\t";
    else
      ss << "\t";
    ss << std::endl;
  }
  ss << "-----------------------------------------------------------" << std::endl;
  STONEDB_LOG(LogCtl_Level::DEBUG, "%s", ss.str().c_str());
}

RCTable::Iterator::Iterator(RCTable &table, std::shared_ptr<Filter> filter)
    : table(&table), filter(filter), it(filter.get(), table.Getpackpower()) {}

void RCTable::Iterator::Initialize(const std::vector<bool> &attrs_) {
  int attr_id = 0;
  attrs.clear();
  record.clear();
  values_fetchers.clear();

  for (auto const iter : attrs_) {
    if (iter) {
      RCAttr *attr = table->GetAttr(attr_id);
      attrs.push_back(attr);
      record.emplace_back(attr->ValuePrototype(false).Clone());
      values_fetchers.push_back(
          std::bind(&RCAttr::GetValueData, attr, std::placeholders::_1, std::ref(*record[attr_id]), false));
    } else {
      record.emplace_back(table->GetAttr(attr_id)->ValuePrototype(false).Clone());
    }
    attr_id++;
  }
}

bool RCTable::Iterator::operator==(const Iterator &iter) { return position == iter.position; }

void RCTable::Iterator::operator++(int) {
  ++it;
  int64_t new_pos;
  if (it.IsValid())
    new_pos = *it;
  else
    new_pos = -1;
  UnlockPacks(new_pos);
  position = new_pos;
  current_record_fetched = false;
}

void RCTable::Iterator::MoveToRow(int64_t row_id) {
  UnlockPacks(row_id);
  it.RewindToRow(row_id);
  if (it.IsValid())
    position = row_id;
  else
    position = -1;
  current_record_fetched = false;
}

void RCTable::Iterator::FetchValues() {
  if (!current_record_fetched) {
    LockPacks();
    for (auto &func : values_fetchers) func(position);
    current_record_fetched = true;
  }
}

void RCTable::Iterator::UnlockPacks(int64_t new_row_id) {
  if (position != -1) {
    uint32_t power = table->Getpackpower();
    if (new_row_id == -1 || (position >> power) != (new_row_id >> power)) dp_locks.clear();
  }
}

void RCTable::Iterator::LockPacks() {
  if (dp_locks.empty() && position != -1) {
    uint32_t power = table->Getpackpower();
    for (auto &a : attrs) dp_locks.emplace_back(std::make_unique<DataPackLock>(a, int(position >> power)));
  }
}

RCTable::Iterator RCTable::Iterator::CreateBegin(RCTable &table, std::shared_ptr<Filter> filter,
                                                 const std::vector<bool> &attrs) {
  RCTable::Iterator ret(table, filter);
  ret.Initialize(attrs);
  ret.it.Rewind();
  if (ret.it.IsValid())
    ret.position = *(ret.it);
  else
    ret.position = -1;
  ret.conn = current_tx;
  return ret;
}

RCTable::Iterator RCTable::Iterator::CreateEnd() { return RCTable::Iterator(); }

int64_t RCTable::NumOfObj() { return m_attrs[0]->NumOfObj(); }

void RCTable::GetTable_S(types::BString &s, int64_t obj, int attr) {
  DEBUG_ASSERT(static_cast<size_t>(attr) <= m_attrs.size());
  DEBUG_ASSERT(static_cast<uint64_t>(obj) <= m_attrs[attr]->NumOfObj());
  s = m_attrs[attr]->GetValueString(obj);
}

int64_t RCTable::GetTable64(int64_t obj, int attr) {
  DEBUG_ASSERT(static_cast<size_t>(attr) <= m_attrs.size());
  DEBUG_ASSERT(static_cast<uint64_t>(obj) <= m_attrs[attr]->NumOfObj());
  return m_attrs[attr]->GetValueInt64(obj);
}

bool RCTable::IsNull(int64_t obj, int attr) {
  DEBUG_ASSERT(static_cast<size_t>(attr) <= m_attrs.size());
  DEBUG_ASSERT(static_cast<uint64_t>(obj) <= m_attrs[attr]->NumOfObj());
  return (m_attrs[attr]->IsNull(obj) ? true : false);
}

types::RCValueObject RCTable::GetValue(int64_t obj, int attr, [[maybe_unused]] Transaction *conn) {
  DEBUG_ASSERT(static_cast<size_t>(attr) <= m_attrs.size());
  DEBUG_ASSERT(static_cast<uint64_t>(obj) <= m_attrs[attr]->NumOfObj());
  return m_attrs[attr]->GetValue(obj, false);
}

uint RCTable::MaxStringSize(int n_a, Filter *f) {
  DEBUG_ASSERT(n_a >= 0 && static_cast<size_t>(n_a) <= m_attrs.size());
  if (NumOfObj() == 0) return 1;
  return m_attrs[n_a]->MaxStringSize(f);
}

void RCTable::FillRowByRowid(TABLE *table, int64_t obj) {
  int col_id = 0;
  assert((int64_t)obj <= NumOfObj());
  for (Field **field = table->field; *field; field++, col_id++) {
    LockPackForUse(col_id, obj >> m_attrs[col_id]->NumOfPackpower());
    std::shared_ptr<void> defer(nullptr,
                                [this, col_id, obj](...) { UnlockPackFromUse(col_id, obj >> Getpackpower()); });
    std::unique_ptr<types::RCDataType> value(m_attrs[col_id]->ValuePrototype(false).Clone());
    m_attrs[col_id]->GetValueData(obj, *value);
    if (bitmap_is_set(table->read_set, col_id)) {
      Engine::ConvertToField(*field, *value, nullptr);
    } else {
      std::memset((*field)->ptr, 0, 2);
      (*field)->set_null();
    }
  }
}

void RCTable::LoadDataInfile(system::IOParameters &iop) {
  if (iop.LoadDelayed() && GetID() != iop.TableID()) {
    throw common::SDBError(common::ErrorCode::DATA_ERROR, "Invalid table ID(" + std::to_string(GetID()) + "/" +
                                                              std::to_string(iop.TableID()) + "): " + m_path.string());
  }

  FunctionExecutor fe(std::bind(&RCTable::LockPackInfoForUse, this), std::bind(&RCTable::UnlockPackInfoFromUse, this));

  if (iop.LoadDelayed()) {
    no_loaded_rows = stonedb_sysvar_enable_rowstore ? MergeMemTable(iop) : ProcessDelayed(iop);
  } else {
    no_loaded_rows = ProceedNormal(iop);
  }
}

void RCTable::Field2VC(Field *f, loader::ValueCache &vc, size_t col) {
  if (f->is_null()) {
    vc.ExpectedNull(true);
    return;
  }

  switch (f->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONGLONG: {
      int64_t v = f->val_int();
      if (m_attrs[col]->GetIfAutoInc() && v == 0)
        // Value of auto inc column was not assigned by user
        *reinterpret_cast<int64_t *>(vc.Prepare(sizeof(int64_t))) = m_attrs[col]->AutoIncNext();
      else
        *reinterpret_cast<int64_t *>(vc.Prepare(sizeof(int64_t))) = v;
      vc.ExpectedSize(sizeof(int64_t));
      if (m_attrs[col]->GetIfAutoInc()) {
        // inc counter should be set to value of user assigned
        if (static_cast<uint64_t>(v) > m_attrs[col]->GetAutoInc()) {
          m_attrs[col]->SetAutoInc(v);
        }
      }
    } break;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE: {
      double v = f->val_real();
      *reinterpret_cast<int64_t *>(vc.Prepare(sizeof(int64_t))) = *reinterpret_cast<int64_t *>(&v);
      vc.ExpectedSize(sizeof(int64_t));
    } break;
    case MYSQL_TYPE_NEWDECIMAL: {
      auto dec_f = dynamic_cast<Field_new_decimal *>(f);
      *reinterpret_cast<int64_t *>(vc.Prepare(sizeof(int64_t))) =
          std::lround(dec_f->val_real() * types::PowOfTen(dec_f->dec));
      vc.ExpectedSize(sizeof(int64_t));
    } break;
    case MYSQL_TYPE_TIMESTAMP: {
      MYSQL_TIME my_time;
      std::memset(&my_time, 0, sizeof(my_time));
      f->get_time(&my_time);
      // convert to UTC
      if (!common::IsTimeStampZero(my_time)) {
        my_bool myb;
        my_time_t secs_utc = current_tx->Thd()->variables.time_zone->TIME_to_gmt_sec(&my_time, &myb);
        common::GMTSec2GMTTime(&my_time, secs_utc);
      }
      types::DT dt = {};
      dt.year = my_time.year;
      dt.month = my_time.month;
      dt.day = my_time.day;
      dt.hour = my_time.hour;
      dt.minute = my_time.minute;
      dt.second = my_time.second;

      *reinterpret_cast<int64_t *>(vc.Prepare(sizeof(int64_t))) = dt.val;
      vc.ExpectedSize(sizeof(int64_t));
    } break;
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_DATETIME2: {
      MYSQL_TIME my_time;
      std::memset(&my_time, 0, sizeof(my_time));
      f->get_time(&my_time);
      types::DT dt = {};
      dt.year = my_time.year;
      dt.month = my_time.month;
      dt.day = my_time.day;
      dt.hour = my_time.hour;
      dt.minute = my_time.minute;
      dt.second = my_time.second;

      *reinterpret_cast<int64_t *>(vc.Prepare(sizeof(int64_t))) = dt.val;
      vc.ExpectedSize(sizeof(int64_t));
    } break;
    case MYSQL_TYPE_YEAR:  // what the hell?
    {
      types::DT dt = {};
      dt.year = f->val_int();
      *reinterpret_cast<int64_t *>(vc.Prepare(sizeof(int64_t))) = dt.val;
      vc.ExpectedSize(sizeof(int64_t));
    } break;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING: {
      String buf;
      f->val_str(&buf);
      if (m_attrs[col]->Type().IsLookup()) {
        types::BString s(buf.length() == 0 ? "" : buf.ptr(), buf.length());
        int64_t *buf = reinterpret_cast<int64_t *>(vc.Prepare(sizeof(int64_t)));
        *buf = m_attrs[col]->EncodeValue_T(s, true);
        vc.ExpectedSize(sizeof(int64_t));
      } else {
        auto ptr = vc.Prepare(buf.length());
        std::memcpy(ptr, buf.ptr(), buf.length());
        vc.ExpectedSize(buf.length());
      }
    } break;
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_NULL:
    case MYSQL_TYPE_BIT:
    default:
      throw common::Exception("unsupported mysql type " + std::to_string(f->type()));
      break;
  }
}

int RCTable::Insert(TABLE *table) {
  FunctionExecutor fe(std::bind(&RCTable::LockPackInfoForUse, this), std::bind(&RCTable::UnlockPackInfoFromUse, this));

  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
  std::shared_ptr<void> defer(nullptr,
                              [org_bitmap, table](...) { dbug_tmp_restore_column_map(table->read_set, org_bitmap); });

  std::vector<loader::ValueCache> vcs;
  vcs.reserve(NumOfAttrs());
  for (uint i = 0; i < NumOfAttrs(); i++) {
    vcs.emplace_back(1, 128);
    Field2VC(table->field[i], vcs[i], i);
    vcs[i].Commit();
  }

  std::shared_ptr<index::RCTableIndex> tab = rceng->GetTableIndex(share->Path());
  if (tab) {
    std::vector<std::string_view> fields;
    std::vector<uint> cols = tab->KeyCols();
    for (auto &col : cols) {
      fields.emplace_back(vcs[col].GetDataBytesPointer(0), vcs[col].Size(0));
    }

    if (tab->InsertIndex(current_tx, fields, NumOfObj()) == common::ErrorCode::DUPP_KEY) {
      STONEDB_LOG(LogCtl_Level::INFO, "Insert duplicate key on row %d", NumOfObj() - 1);
      return HA_ERR_FOUND_DUPP_KEY;
    }
  }
  for (uint i = 0; i < NumOfAttrs(); i++) {
    m_attrs[i]->LoadData(&vcs[i]);
  }
  return 0;
}

void RCTable::UpdateItem(uint64_t row, uint64_t col, Value &v) { m_attrs[col]->UpdateData(row, v); }

uint64_t RCTable::ProceedNormal(system::IOParameters &iop) {
  std::unique_ptr<system::Stream> fs;
  if (iop.LocalLoad()) {
    fs.reset(new system::NetStream(iop));
  } else {
    fs.reset(new system::StoneDBFile());
    fs->OpenReadOnly(iop.Path());
  }

  loader::LoadParser parser(m_attrs, iop, share->PackSize(), fs);

  uint to_prepare;
  uint no_of_rows_returned;
  utils::Timer timer;
  do {
    to_prepare = share->PackSize() - (m_attrs[0]->NumOfObj() % share->PackSize());
    std::vector<loader::ValueCache> value_buffers;
    no_of_rows_returned = parser.GetPackrow(to_prepare, value_buffers);
    no_dup_rows += parser.GetDuprow();
    if (parser.GetNoRow() > 0) {
      utils::result_set<void> res;
      for (uint att = 0; att < m_attrs.size(); ++att) {
        res.insert(
            rceng->load_thread_pool.add_task(&RCAttr::LoadData, m_attrs[att].get(), &value_buffers[att], current_tx));
      }
      res.get_all();
    }
  } while (no_of_rows_returned == to_prepare);

  auto no_loaded_rows = parser.GetNoRow();

  if (no_loaded_rows > 0 && mysql_bin_log.is_open())
    if (binlog_load_query_log_event(iop) != 0) {
      STONEDB_LOG(LogCtl_Level::ERROR, "Write load binlog fail!");
      throw common::FormatException("Write load binlog fail!");
    }
  timer.Print(__PRETTY_FUNCTION__);

  no_rejected_rows = parser.GetNoRejectedRows();

  if (parser.ThresholdExceeded(no_loaded_rows + no_rejected_rows))
    throw common::FormatException("Rejected rows threshold exceeded. " + std::to_string(no_rejected_rows) + " out of " +
                                  std::to_string(no_loaded_rows + no_rejected_rows) + " rows rejected.");

  if (no_loaded_rows == 0 && no_rejected_rows == 0 && parser.GetDuprow() == 0) throw common::FormatException(-1, -1);

  return no_loaded_rows;
}

int RCTable::binlog_load_query_log_event(system::IOParameters &iop) {
  char *load_data_query, *end, *fname_start, *fname_end, *p = NULL;
  size_t pl = 0;
  List<Item> fv;
  Item *item;
  String *str;
  String pfield, pfields, string_buf;
  int n;
  LOAD_FILE_INFO *lf_info;
  std::string db_name, tab_name;
  lf_info = (LOAD_FILE_INFO *)iop.GetLogInfo();
  THD *thd = lf_info->thd;
  sql_exchange *ex = thd->lex->exchange;
  TABLE *table = thd->lex->select_lex.table_list.first->table;
  if (ex == nullptr || table == nullptr) return -1;
  auto pa = fs::path(iop.GetTableName());
  std::tie(db_name, tab_name) = std::make_tuple(pa.parent_path().filename().native(), pa.filename().native());
  append_identifier(thd, &string_buf, tab_name.c_str(), tab_name.length());
  Load_log_event lle(thd, ex, db_name.c_str(), string_buf.c_ptr_safe(), fv, FALSE, DUP_ERROR, FALSE, TRUE);
  if (thd->lex->local_file) lle.set_fname_outside_temp_buf(ex->file_name, std::strlen(ex->file_name));

  if (!thd->lex->field_list.elements) {
    Field **field;
    for (field = table->field; *field; field++) thd->lex->field_list.push_back(new Item_field(*field));
    // bitmap_set_all(table->write_set);
    if (setup_fields(thd, Ref_ptr_array(), thd->lex->update_list, MARK_COLUMNS_WRITE, 0, 0) ||
        setup_fields(thd, Ref_ptr_array(), thd->lex->value_list, MARK_COLUMNS_READ, 0, 0))
      return -1;
  } else {
    if (setup_fields(thd, Ref_ptr_array(), thd->lex->field_list, MARK_COLUMNS_WRITE, 0, 0) ||
        setup_fields(thd, Ref_ptr_array(), thd->lex->update_list, MARK_COLUMNS_WRITE, 0, 0))
      return -1;

    if (setup_fields(thd, Ref_ptr_array(), thd->lex->value_list, MARK_COLUMNS_READ, 0, 0)) return -1;
  }

  if (!thd->lex->field_list.is_empty()) {
    List_iterator<Item> li(thd->lex->field_list);

    pfields.append(" (");
    n = 0;

    while ((item = li++)) {
      if (n++) pfields.append(", ");
      if (item->type() == Item::FIELD_ITEM)
        append_identifier(thd, &pfields, item->item_name.ptr(), std::strlen(item->item_name.ptr()));
      else
        item->print(&pfields, QT_ORDINARY);
    }
    pfields.append(")");
  }

  if (!thd->lex->update_list.is_empty()) {
    List_iterator<Item> lu(thd->lex->update_list);
    List_iterator<String> ls(thd->lex->load_set_str_list);

    pfields.append(" SET ");
    n = 0;

    while ((item = lu++)) {
      str = ls++;
      if (n++) pfields.append(", ");
      append_identifier(thd, &pfields, item->item_name.ptr(), std::strlen(item->item_name.ptr()));

      str->copy();
      pfields.append(str->ptr());
      str->free();
    }
    thd->lex->load_set_str_list.empty();
  }

  p = pfields.c_ptr_safe();
  pl = std::strlen(p);

  if (!(load_data_query = (char *)thd->alloc(lle.get_query_buffer_length() + 1 + pl))) return -1;

  lle.print_query(FALSE, ex->cs ? ex->cs->csname : NULL, load_data_query, &end, &fname_start, &fname_end);

  std::strcpy(end, p);
  end += pl;

  Execute_load_query_log_event e(thd, load_data_query, end - load_data_query,
                                 static_cast<uint>(fname_start - load_data_query - 1),
                                 static_cast<uint>(fname_end - load_data_query), LOAD_DUP_ERROR, TRUE, FALSE, FALSE, 0);
  return mysql_bin_log.write_event(&e);
}

size_t RCTable::max_row_length(std::vector<loader::ValueCache> &vcs, uint row, uint delimiter) {
  size_t row_len = 0;
  for (uint att = 0; att < m_attrs.size(); ++att) {
    switch (m_attrs[att]->TypeName()) {
      case common::CT::VARCHAR:
      case common::CT::VARBYTE:
      case common::CT::BIN:
      case common::CT::LONGTEXT:
      case common::CT::BYTE:
      case common::CT::STRING: {
        row_len += (2 * vcs[att].Size(row));  // real data len
        row_len += delimiter;
      } break;
      default:
        row_len += 255;  // max Display Width(M) ; number(255), datetime(19)
        row_len += delimiter;

        break;
    }
  }
  return row_len;
}

int RCTable::binlog_insert2load_log_event(system::IOParameters &iop) {
  char *load_data_query, *p = NULL;
  size_t pl = 0;
  List<Item> fv;

  String pfield, pfields, string_buf;
  LOAD_FILE_INFO *lf_info;
  std::string db_name, tab_name;
  uint32 fname_start_pos = 0;
  uint32 fname_end_pos = 0;

  lf_info = (LOAD_FILE_INFO *)iop.GetLogInfo();
  THD *thd = lf_info->thd;

  auto pa = fs::path(iop.GetTableName());
  std::tie(db_name, tab_name) = std::make_tuple(pa.parent_path().filename().native(), pa.filename().native());
  pfields.append("LOAD DATA ");
  fname_start_pos = pfields.length();
  pfields.append("LOCAL INFILE 'localfile' INTO");

  fname_end_pos = pfields.length();

  pfields.append(" TABLE ");
  append_identifier(thd, &pfields, tab_name.c_str(), tab_name.length());
  pfields.append(
      " FIELDS TERMINATED BY '\001' ENCLOSED BY '`' ESCAPED BY '\\\\' LINES "
      "TERMINATED BY '\002'");

  /* TODO: m_attrs[att]->Name() is not available now
     int  n = 0;
      pfields.append(" (");
      for (uint att = 0; att < m_attrs.size(); ++att) {
          if (n++)
              pfields.append(", ");
          append_identifier(thd, &pfields, m_attrs[att]->Name().c_str(),
     std::strlen(m_attrs[att]->Name().c_str()));
      }
      pfields.append(")");
      */

  p = pfields.c_ptr_safe();
  pl = std::strlen(p);
  std::unique_ptr<char[]> buf = std::unique_ptr<char[]>(new char[pl + 1]);
  load_data_query = buf.get();

  std::memcpy(load_data_query, p, pl);
  Execute_load_query_log_event e(thd, load_data_query, pl, static_cast<uint>(fname_start_pos - 1),
                                 static_cast<uint>(fname_end_pos), LOAD_DUP_ERROR, TRUE, FALSE, FALSE, 0);
  return mysql_bin_log.write_event(&e);
}

int RCTable::binlog_insert2load_block(std::vector<loader::ValueCache> &vcs, uint load_obj, system::IOParameters &iop) {
  uint block_len, max_event_size;
  uchar *buffer = nullptr;
  size_t buf_sz = 16_MB;
  static char FIELDS_DELIMITER = '\001';
  static char LINES_DELIMITER = '\002';
  static char ENCLOSED_CHAR = '`';
  static char ESCAPE_CHAR = '\\';
  static std::string ENCLOSE("``");

  LOAD_FILE_INFO *lf_info = (LOAD_FILE_INFO *)iop.GetLogInfo();
  if (lf_info == nullptr || lf_info->thd->is_current_stmt_binlog_format_row()) return 0;
  max_event_size = lf_info->thd->variables.max_allowed_packet;

  std::unique_ptr<uchar[]> block_buf = std::unique_ptr<uchar[]>(new uchar[buf_sz]);
  // write load binlog
  auto ptr = block_buf.get();
  uint cols = m_attrs.size();
  for (uint i = 0; i < load_obj; i++) {
    size_t length = max_row_length(vcs, i, sizeof(FIELDS_DELIMITER));
    length += sizeof(LINES_DELIMITER);
    auto used = ptr - block_buf.get();
    if (buf_sz - used < length) {
      while (buf_sz - used < length) {
        buf_sz *= 2;
        if (buf_sz > 2_GB) throw common::Exception("Binlog Insert2load buffer larger than 2G");
      }
      std::unique_ptr<uchar[]> old_buf = std::move(block_buf);
      block_buf.reset(new uchar[buf_sz]);
      std::memcpy(block_buf.get(), old_buf.get(), used);
      ptr = block_buf.get() + used;
    }
    for (uint att = 0; att < m_attrs.size(); ++att) {
      if (vcs[att].IsNull(i)) {
        std::memcpy(ptr, "NULL", 4);
        ptr += 4;
        if (att < cols - 1) {
          *ptr = FIELDS_DELIMITER;
          ptr += sizeof(FIELDS_DELIMITER);
        }
        continue;
      }
      switch (m_attrs[att]->TypeName()) {
        case common::CT::NUM:
        case common::CT::REAL:
        case common::CT::FLOAT:
        case common::CT::BYTEINT:
        case common::CT::SMALLINT:
        case common::CT::INT:
        case common::CT::MEDIUMINT:
        case common::CT::BIGINT: {
          types::BString s;
          int64_t v = *(int64_t *)(vcs[att].GetDataBytesPointer(i));
          if (v == common::NULL_VALUE_64)
            s = types::BString();
          else {
            types::RCNum rcd(v, m_attrs[att]->Type().GetScale(), m_attrs[att]->Type().IsFloat(),
                             m_attrs[att]->TypeName());
            s = rcd.ToBString();
          }
          std::memcpy(ptr, s.GetDataBytesPointer(), s.size());
          ptr += s.size();
          if (att < cols - 1) {
            *ptr = FIELDS_DELIMITER;
            ptr += sizeof(FIELDS_DELIMITER);
          }
        } break;
        case common::CT::TIMESTAMP: {
          types::BString s;
          int64_t v = *(int64_t *)(vcs[att].GetDataBytesPointer(i));
          if (v == common::NULL_VALUE_64) {
            s = types::RCDateTime().ToBString();
          } else {
            types::RCDateTime rcdt(v, m_attrs[att]->TypeName());
            types::RCDateTime::AdjustTimezone(rcdt);
            s = rcdt.ToBString();
          }
          std::memcpy(ptr, s.GetDataBytesPointer(), s.size());
          ptr += s.size();
          if (att < cols - 1) {
            *ptr = FIELDS_DELIMITER;
            ptr += sizeof(FIELDS_DELIMITER);
          }
        } break;
        case common::CT::YEAR:
        case common::CT::TIME:
        case common::CT::DATETIME:
        case common::CT::DATE: {
          types::BString s;
          int64_t v = *(int64_t *)(vcs[att].GetDataBytesPointer(i));
          if (v == common::NULL_VALUE_64) {
            s = types::RCDateTime().ToBString();
          } else {
            types::RCDateTime rcdt(v, m_attrs[att]->TypeName());
            s = rcdt.ToBString();
          }
          std::memcpy(ptr, s.GetDataBytesPointer(), s.size());
          ptr += s.size();
          if (att < cols - 1) {
            *ptr = FIELDS_DELIMITER;
            ptr += sizeof(FIELDS_DELIMITER);
          }
        } break;
        case common::CT::VARCHAR:
        case common::CT::VARBYTE:
        case common::CT::BIN:
        case common::CT::LONGTEXT:
        case common::CT::BYTE:
        case common::CT::STRING: {
          const char *v = vcs[att].GetDataBytesPointer(i);
          uint size = vcs[att].Size(i);
          bool null = vcs[att].IsNull(i);
          if (!null && size == 0) {
            std::memcpy(ptr, ENCLOSE.c_str(), ENCLOSE.length());
            ptr += ENCLOSE.length();
          } else {
            types::BString s;
            if (m_attrs[att]->Type().IsLookup()) {
              s = m_attrs[att]->DecodeValue_S(*(int64_t *)v);
              v = s.GetDataBytesPointer();
              size = s.size();
            }
            const char *c_ptr = v;
            for (; c_ptr - v < size; c_ptr++) {
              if (*c_ptr == FIELDS_DELIMITER || *c_ptr == LINES_DELIMITER || *c_ptr == ESCAPE_CHAR ||
                  *c_ptr == ENCLOSED_CHAR) {
                *ptr = ESCAPE_CHAR;
                ptr++;
                *ptr = *c_ptr;
                ptr++;
              } else {
                *ptr = *c_ptr;
                ptr++;
              }
            }
          }
          if (att < cols - 1) {
            *ptr = FIELDS_DELIMITER;
            ptr += sizeof(FIELDS_DELIMITER);
          }
        } break;
        case common::CT::DATETIME_N:
        case common::CT::TIMESTAMP_N:
        case common::CT::TIME_N:
        case common::CT::UNK:
        default:
          throw common::Exception("Unsupported StoneDB Type " +
                                  std::to_string(static_cast<unsigned char>(m_attrs[att]->TypeName())));
          break;
      }
    }
    *ptr = LINES_DELIMITER;
    ptr += sizeof(LINES_DELIMITER);
  }
  buffer = block_buf.get();
  for (block_len = (uint)(ptr - block_buf.get()); block_len > 0;
       buffer += std::min(block_len, max_event_size), block_len -= std::min(block_len, max_event_size)) {
    if (lf_info->wrote_create_file) {
      Append_block_log_event a(lf_info->thd, lf_info->thd->db, buffer, std::min(block_len, max_event_size),
                               lf_info->log_delayed);
      if (mysql_bin_log.write_event(&a)) return -1;
    } else {
      Begin_load_query_log_event b(lf_info->thd, lf_info->thd->db, buffer, std::min(block_len, max_event_size),
                                   lf_info->log_delayed);
      if (mysql_bin_log.write_event(&b)) return -1;
      lf_info->wrote_create_file = 1;
    }
  }

  return 0;
}

class DelayedInsertParser final {
 public:
  DelayedInsertParser(std::vector<std::unique_ptr<RCAttr>> &attrs, std::vector<std::unique_ptr<char[]>> *vec,
                      uint packsize, std::shared_ptr<index::RCTableIndex> index)
      : pack_size(packsize), attrs(attrs), vec(vec), index_table(index) {}

  uint GetRows(uint no_of_rows, std::vector<loader::ValueCache> &value_buffers) {
    int64_t start_row = attrs[0]->NumOfObj();

    value_buffers.reserve(attrs.size());
    for (size_t i = 0; i < attrs.size(); i++) {
      value_buffers.emplace_back(pack_size, pack_size * 128);
    }

    uint no_of_rows_returned;
    for (no_of_rows_returned = 0; no_of_rows_returned < no_of_rows; no_of_rows_returned++) {
      if (processed == vec->size()) {
        // no more to parse
        return no_of_rows_returned;
      }

      auto ptr = (*vec)[processed].get();
      // int  tid = *(int32_t *)ptr;
      ptr += sizeof(int32_t);
      std::string path(ptr);
      ptr += path.length() + 1;
      utils::BitSet null_mask(attrs.size(), ptr);
      ptr += null_mask.data_size();
      for (uint i = 0; i < attrs.size(); i++) {
        auto &vc = value_buffers[i];
        if (null_mask[i]) {
          vc.ExpectedNull(true);
          continue;
        }
        auto &attr(attrs[i]);
        switch (attr->GetPackType()) {
          case common::PackType::STR: {
            uint32_t len = *(uint32_t *)ptr;
            ptr += sizeof(uint32_t);
            auto buf = vc.Prepare(len);
            if (buf == nullptr) {
              throw std::bad_alloc();
            }
            std::memcpy(buf, ptr, len);
            vc.ExpectedSize(len);
            ptr += len;
          } break;
          case common::PackType::INT: {
            if (attr->Type().IsLookup()) {
              uint32_t len = *(uint32_t *)ptr;
              ptr += sizeof(uint32_t);
              types::BString s(len == 0 ? "" : ptr, len);
              int64_t *buf = reinterpret_cast<int64_t *>(vc.Prepare(sizeof(int64_t)));
              *buf = attr->EncodeValue_T(s, true);
              vc.ExpectedSize(sizeof(int64_t));
              ptr += len;
            } else {
              int64_t *buf = reinterpret_cast<int64_t *>(vc.Prepare(sizeof(int64_t)));
              *buf = *(int64_t *)ptr;

              if (attr->GetIfAutoInc()) {
                if (*buf == 0)  // Value of auto inc column was not assigned by user
                  *buf = attr->AutoIncNext();

                if (static_cast<uint64_t>(*buf) > attr->GetAutoInc()) attr->SetAutoInc(*buf);
              }
              vc.ExpectedSize(sizeof(int64_t));
              ptr += sizeof(int64_t);
            }
          } break;
          default:
            break;
        }
      }
      for (auto &vc : value_buffers) {
        vc.Commit();
      }

      processed++;
      // insert index into kvstore
      if (InsertIndex(value_buffers, start_row) != common::ErrorCode::SUCCESS) {
        for (auto &vc : value_buffers) {
          vc.Rollback();
        }
      }
    }
    return no_of_rows_returned;
  }

  common::ErrorCode InsertIndex(std::vector<loader::ValueCache> &vcs, int64_t start_row) {
    if (!index_table) return common::ErrorCode::SUCCESS;

    size_t row_idx = vcs[0].NumOfValues() - 1;
    std::vector<uint> cols = index_table->KeyCols();
    std::vector<std::string_view> fields;
    for (auto &col : cols) {
      fields.emplace_back(vcs[col].GetDataBytesPointer(row_idx), vcs[col].Size(row_idx));
    }

    if (index_table->InsertIndex(current_tx, fields, start_row + row_idx) == common::ErrorCode::DUPP_KEY) {
      STONEDB_LOG(LogCtl_Level::DEBUG, "Delay insert discard this row for duplicate key");
      return common::ErrorCode::DUPP_KEY;
    }
    return common::ErrorCode::SUCCESS;
  }

 private:
  uint processed = 0;
  uint pack_size;
  std::vector<std::unique_ptr<RCAttr>> &attrs;
  std::vector<std::unique_ptr<char[]>> *vec;
  std::shared_ptr<index::RCTableIndex> index_table;
};

uint64_t RCTable::ProcessDelayed(system::IOParameters &iop) {
  std::string str(basename(const_cast<char *>(iop.Path())));
  auto vec = reinterpret_cast<std::vector<std::unique_ptr<char[]>> *>(std::stol(str));

  DelayedInsertParser parser(m_attrs, vec, share->PackSize(), rceng->GetTableIndex(share->Path()));
  long no_loaded_rows = 0;

  uint to_prepare, no_of_rows_returned;
  do {
    to_prepare = share->PackSize() - (int)(m_attrs[0]->NumOfObj() % share->PackSize());
    std::vector<loader::ValueCache> vcs;
    no_of_rows_returned = parser.GetRows(to_prepare, vcs);
    size_t real_loaded_rows = vcs[0].NumOfValues();
    no_dup_rows += (no_of_rows_returned - real_loaded_rows);
    if (real_loaded_rows > 0) {
      utils::result_set<void> res;
      for (uint att = 0; att < m_attrs.size(); ++att) {
        res.insert(rceng->load_thread_pool.add_task(&RCAttr::LoadData, m_attrs[att].get(), &vcs[att], current_tx));
      }
      res.get_all();
      no_loaded_rows += real_loaded_rows;
      if (real_loaded_rows > 0 && mysql_bin_log.is_open()) binlog_insert2load_block(vcs, real_loaded_rows, iop);
    }
  } while (no_of_rows_returned == to_prepare);

  if (no_loaded_rows > 0 && mysql_bin_log.is_open())
    if (binlog_insert2load_log_event(iop) != 0) {
      STONEDB_LOG(LogCtl_Level::ERROR, "Write insert to load binlog fail!");
      throw common::FormatException("Write insert to load binlog fail!");
    }

  return no_loaded_rows;
}

void RCTable::InsertMemRow(std::unique_ptr<char[]> buf, uint32_t size) {
  return m_mem_table->InsertRow(std::move(buf), size);
}

int RCTable::MergeMemTable(system::IOParameters &iop) {
  ASSERT(m_tx, "Transaction not generated.");
  ASSERT(m_mem_table, "memory table not exist");
  auto index_table = rceng->GetTableIndex(share->Path());

  struct timespec t1, t2, t3;
  clock_gettime(CLOCK_REALTIME, &t1);
  std::vector<std::unique_ptr<char[]>> vec;
  {
    uchar entry_key[32];
    size_t key_pos = 0;
    auto cf_handle = m_mem_table->GetCFHandle();
    uint32_t mem_id = m_mem_table->GetMemID();
    index::be_store_index(entry_key + key_pos, mem_id);
    key_pos += sizeof(uint32_t);
    index::be_store_byte(entry_key + key_pos, static_cast<uchar>(RCMemTable::RecordType::kInsert));
    key_pos += sizeof(uchar);
    rocksdb::Slice entry_slice((char *)entry_key, key_pos);

    uchar upper_key[32];
    size_t upper_pos = 0;
    index::be_store_index(upper_key + upper_pos, mem_id);
    upper_pos += sizeof(uint32_t);
    uchar upkey = static_cast<int>(RCMemTable::RecordType::kInsert) + 1;
    index::be_store_byte(upper_key + upper_pos, upkey);
    upper_pos += sizeof(uchar);
    rocksdb::Slice upper_slice((char *)upper_key, upper_pos);

    m_mem_table->next_load_id_ = m_mem_table->next_insert_id_.load();
    rocksdb::ReadOptions ropts;
    ropts.iterate_upper_bound = &upper_slice;
    std::unique_ptr<rocksdb::Iterator> iter(m_tx->KVTrans().GetDataIterator(ropts, cf_handle));
    iter->Seek(entry_slice);

    while (iter->Valid()) {
      auto value = iter->value();
      std::unique_ptr<char[]> buf(new char[value.size()]);
      std::memcpy(buf.get(), value.data(), value.size());
      vec.emplace_back(std::move(buf));
      m_tx->KVTrans().SingleDeleteData(cf_handle, iter->key());
      m_mem_table->stat.read_cnt++;
      m_mem_table->stat.read_bytes += value.size();

      iter->Next();
      if (vec.size() >= static_cast<std::size_t>(stonedb_sysvar_insert_max_buffered)) break;
    }

    if (iter->Valid() && iter->key().starts_with(entry_slice)) {
      m_mem_table->next_load_id_ = index::be_to_uint64((uchar *)iter->key().data() + key_pos);
    }
  }
  if (vec.empty()) return 0;
  clock_gettime(CLOCK_REALTIME, &t2);

  DelayedInsertParser parser(m_attrs, &vec, share->PackSize(), index_table);
  long no_loaded_rows = 0;

  uint to_prepare, no_of_rows_returned;
  do {
    to_prepare = share->PackSize() - (int)(m_attrs[0]->NumOfObj() % share->PackSize());
    std::vector<loader::ValueCache> vcs;
    no_of_rows_returned = parser.GetRows(to_prepare, vcs);
    size_t real_loaded_rows = vcs[0].NumOfValues();
    no_dup_rows += (no_of_rows_returned - real_loaded_rows);
    if (real_loaded_rows > 0) {
      utils::result_set<void> res;
      for (uint att = 0; att < m_attrs.size(); ++att) {
        res.insert(rceng->load_thread_pool.add_task(&RCAttr::LoadData, m_attrs[att].get(), &vcs[att], m_tx));
      }
      res.get_all();
      no_loaded_rows += real_loaded_rows;
      if (real_loaded_rows > 0 && mysql_bin_log.is_open()) binlog_insert2load_block(vcs, real_loaded_rows, iop);
    }
  } while (no_of_rows_returned == to_prepare);
  clock_gettime(CLOCK_REALTIME, &t3);

  if (no_loaded_rows > 0 && mysql_bin_log.is_open())
    if (binlog_insert2load_log_event(iop) != 0) {
      STONEDB_LOG(LogCtl_Level::ERROR, "Write insert to load binlog fail!");
      throw common::FormatException("Write insert to load binlog fail!");
    }

  if (t2.tv_sec - t1.tv_sec > 15) {
    STONEDB_LOG(LogCtl_Level::WARN, "Latency of rowstore %s larger than 15s, compact manually.", share->Path().c_str());
    kvstore->GetRdb()->CompactRange(rocksdb::CompactRangeOptions(), m_mem_table->GetCFHandle(), nullptr, nullptr);
  }

  if ((t3.tv_sec - t2.tv_sec > 15) && index_table) {
    STONEDB_LOG(LogCtl_Level::WARN, "Latency of index table %s larger than 15s, compact manually.",
                share->Path().c_str());
    kvstore->GetRdb()->CompactRange(rocksdb::CompactRangeOptions(), index_table->m_rdbkey->get_cf(), nullptr, nullptr);
  }

  return no_loaded_rows;
}
}  // namespace core
}  // namespace stonedb
