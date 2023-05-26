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

#include "engine.h"

#include <sys/sysinfo.h>
#include <boost/algorithm/string.hpp>
#include <set>
#include <tuple>

#include "common/common_definitions.h"
#include "common/data_format.h"
#include "common/exception.h"
#include "common/mysql_gate.h"
#include "core/rc_mem_table.h"
#include "core/table_share.h"
#include "core/task_executor.h"
#include "core/temp_table.h"
#include "core/tools.h"
#include "core/transaction.h"
#include "mm/initializer.h"
#include "mysql/thread_pool_priv.h"
#include "mysqld_thd_manager.h"
#include "system/file_out.h"
#include "system/res_manager.h"
#include "system/stdout.h"
#include "thr_lock.h"
#include "util/bitset.h"
#include "util/fs.h"
#include "util/thread_pool.h"

#include "engine.ic"  // optimize_for_tianmu

namespace Tianmu {

namespace handler {
extern void resolve_async_join_settings(const std::string &settings);
}

namespace core {

using Tianmu::handler::QueryRouteTo;

#ifdef PROFILE_LOCK_WAITING
LockProfiler lock_profiler;
#endif
constexpr auto BUFFER_FILE = "TIANMU_INSERT_BUFFER";

namespace {
// It should be immutable after RCEngine initialization
std::vector<fs::path> tianmu_data_dirs;
std::mutex v_mtx;
const char *TIANMU_DATA_DIR = "tianmu_data";
}  // namespace

static void signal_handler([[maybe_unused]] int sig, siginfo_t *si, [[maybe_unused]] void *uc) {
  THD *thd = static_cast<THD *>(si->si_value.sival_ptr);
  thd->awake(THD::KILL_QUERY);
}

static int setup_sig_handler() {
  struct sigaction sa;
  sa.sa_flags = SA_SIGINFO;
  sa.sa_sigaction = signal_handler;
  sigemptyset(&sa.sa_mask);

  if (sigaction(SIGRTMIN, &sa, nullptr) == -1) {
    TIANMU_LOG(LogCtl_Level::INFO, "Failed to set up signal handler. error =%d[%s]", errno, std::strerror(errno));
    return 1;
  }
  return 0;
}

static int has_pack(const LEX_STRING &comment) {
  int ret = common::DFT_PSS;
  std::string str(comment.str, comment.length);
  boost::to_upper(str);
  std::string val;
  auto pos = str.find("PACK");
  if (pos == std::string::npos)
    return ret;
  size_t val_pos = str.find(':', pos);
  if (val_pos == std::string::npos)
    return ret;
  size_t term_pos = str.find(';', val_pos);
  if (term_pos == std::string::npos) {
    val = str.substr(val_pos + 1);
  } else {
    val = str.substr(val_pos + 1, term_pos - val_pos - 1);
  }
  boost::trim(val);
  ret = atoi(val.c_str());
  if (ret > common::DFT_PSS || ret <= 0)
    ret = common::DFT_PSS;
  return ret;
}

static std::string has_mem_name(const LEX_STRING &comment) {
  std::string name = "";
  std::string str(comment.str, comment.length);
  boost::to_upper(str);
  auto pos = str.find("ROWSTORE");
  if (pos == std::string::npos)
    return name;
  size_t val_pos = str.find(':', pos);
  if (val_pos == std::string::npos)
    return name;
  size_t term_pos = str.find(';', val_pos);
  if (term_pos == std::string::npos) {
    name = str.substr(val_pos + 1);
  } else {
    name = str.substr(val_pos + 1, term_pos - val_pos - 1);
  }
  boost::trim(name);

  return name;
}

static auto GetNames(const std::string &table_path) {
  auto p = fs::path(table_path);
  return std::make_tuple(p.parent_path().filename().native(), p.filename().native());
}

bool Engine::TrxCmp::operator()(Transaction *l, Transaction *r) const { return l->GetID() < r->GetID(); }

Engine::Engine()
    : delay_insert_thread_pool(
          "bg_loader",
          tianmu_sysvar_bg_load_threads
              ? tianmu_sysvar_bg_load_threads
              : ((std::thread::hardware_concurrency() / 2) ? (std::thread::hardware_concurrency() / 2) : 1)),
      load_thread_pool("loader",
                       tianmu_sysvar_load_threads ? tianmu_sysvar_load_threads : std::thread::hardware_concurrency()),
      query_thread_pool(
          "query", tianmu_sysvar_query_threads ? tianmu_sysvar_query_threads : std::thread::hardware_concurrency()),
      insert_buffer(BUFFER_FILE, tianmu_sysvar_insert_buffer_size) {
  tianmu_data_dir = mysql_real_data_home;
}

fs::path Engine::GetNextDataDir() {
  std::scoped_lock guard(v_mtx);

  if (tianmu_data_dirs.empty()) {
    // fall back to use MySQL data directory
    auto p = ha_rcengine_->tianmu_data_dir / TIANMU_DATA_DIR;
    if (!fs::is_directory(p))
      fs::create_directory(p);
    return p;
  }

  auto sz = tianmu_data_dirs.size();
  if (sz == 1) {
    // we have no choice
    return tianmu_data_dirs[0];
  }

  if (tianmu_sysvar_dist_policy == 1) {
    return tianmu_data_dirs[std::rand() % sz];
  } else if (tianmu_sysvar_dist_policy == 2) {
    auto v = tianmu_data_dirs;
    v.erase(std::remove_if(v.begin(), v.end(),
                           [](const fs::path &s) {
                             auto si = fs::space(s);
                             auto usage = 100 - ((si.available * 100) / si.capacity);
                             if (usage > static_cast<size_t>(tianmu_sysvar_disk_usage_threshold)) {
                               TIANMU_LOG(LogCtl_Level::WARN, "disk %s usage %lu%%", s.native().c_str(), usage);
                               return true;
                             }
                             return false;
                           }),
            v.end());
    if (v.size() == 0) {
      TIANMU_LOG(LogCtl_Level::ERROR, "all data directories usage exceed %d%%", tianmu_sysvar_disk_usage_threshold);
      throw common::Exception("Disk usage exceeds defined threshold");
    }
    return v[std::rand() % v.size()];
  }
  // round-robin
  static int idx = 0;
  return tianmu_data_dirs[idx++ % sz];
}

int Engine::Init(uint engine_slot) {
  m_slot = engine_slot;
  ConfigureRCControl();
  if (tianmu_sysvar_controlquerylog > 0) {
    rc_querylog_.setOn();
  } else {
    rc_querylog_.setOff();
  }
  std::srand(unsigned(time(nullptr)));

  if (tianmu_sysvar_servermainheapsize == 0) {
    long pages = sysconf(_SC_PHYS_PAGES);
    long pagesize = sysconf(_SC_PAGESIZE);
    if (pagesize > 0 && pages > 0) {
      tianmu_sysvar_servermainheapsize = pages * pagesize / 1_MB / 2;
    } else {
      tianmu_sysvar_servermainheapsize = 10000;
    }
  }
  size_t main_size = size_t(tianmu_sysvar_servermainheapsize) << 20;

  std::string hugefiledir = tianmu_sysvar_hugefiledir;
  int hugefilesize = 0;  // unused
  if (hugefiledir.empty())
    mm::MemoryManagerInitializer::Instance(0, main_size);
  else
    mm::MemoryManagerInitializer::Instance(0, main_size, hugefiledir, hugefilesize);

  the_filter_block_owner = new TheFilterBlockOwner();

  std::string cachefolder_path = tianmu_sysvar_cachefolder;
  boost::trim(cachefolder_path);
  boost::trim_if(cachefolder_path, boost::is_any_of("\""));
  if (SetUpCacheFolder(cachefolder_path) != 0)
    return 1;
  system::ClearDirectory(cachefolder_path);

  m_resourceManager = new system::ResourceManager();

#ifdef FUNCTIONS_EXECUTION_TIMES
  fet = new FunctionsExecutionTimes();
#endif
  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu engine started. ");
  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu data directories:");
  TIANMU_LOG(LogCtl_Level::INFO, "  {");
  if (tianmu_data_dirs.empty())
    TIANMU_LOG(LogCtl_Level::INFO, "    default");
  else
    for (auto &dir : tianmu_data_dirs) {
      auto si = fs::space(dir);
      TIANMU_LOG(LogCtl_Level::INFO, "    %s  capacity/available: %ld/%ld", dir.native().c_str(), si.capacity,
                 si.available);
    }
  TIANMU_LOG(LogCtl_Level::INFO, "  }");

  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu thread pool for background load, size = %ld", delay_insert_thread_pool.size());
  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu thread pool for load, size = %ld", load_thread_pool.size());
  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu thread pool for query, size = %ld", query_thread_pool.size());

  m_monitor_thread = std::thread([this] {
    struct job {
      long interval;
      std::function<void()> func;
    } jobs[] = {
        {60, [this]() { this->LogStat(); }},
        {60 * 5,
         []() {
           TIANMU_LOG(
               LogCtl_Level::INFO,
               "Memory: release [%llu %llu %llu %llu] %lu, total "
               "%lu. (un)freeable %lu/%lu, total alloc/free "
               "%lu/%lu",
               mm::TraceableObject::Instance()->getReleaseCount1(), mm::TraceableObject::Instance()->getReleaseCount2(),
               mm::TraceableObject::Instance()->getReleaseCount3(), mm::TraceableObject::Instance()->getReleaseCount4(),
               mm::TraceableObject::Instance()->getReleaseCount(), mm::TraceableObject::Instance()->getReleaseTotal(),
               mm::TraceableObject::GetFreeableSize(), mm::TraceableObject::GetUnFreeableSize(),
               mm::TraceableObject::Instance()->getAllocSize(), mm::TraceableObject::Instance()->getFreeSize());
         }},
    };

    int counter = 0;
    const long loop_interval = 60;

    while (!exiting) {
      counter++;
      std::unique_lock<std::mutex> lk(cv_mtx);
      if (cv.wait_for(lk, std::chrono::seconds(loop_interval)) == std::cv_status::timeout) {
        if (!tianmu_sysvar_qps_log)
          continue;
        for (auto &j : jobs) {
          if (counter % (j.interval / loop_interval) == 0)
            j.func();
        }
      }
    }
    TIANMU_LOG(LogCtl_Level::INFO, "Tianmu monitor thread exiting...");
  });

  m_load_thread = std::thread([this] { ProcessDelayedInsert(); });
  m_merge_thread = std::thread([this] { ProcessDelayedMerge(); });

  m_purge_thread = std::thread([this] {
    do {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      std::unique_lock<std::mutex> lk(cv_mtx);
      if (cv.wait_for(lk, std::chrono::seconds(3)) == std::cv_status::timeout)
        HandleDeferredJobs();
    } while (!exiting);
    TIANMU_LOG(LogCtl_Level::INFO, "Tianmu file purge thread exiting...");
  });

  if (setup_sig_handler()) {
    return -1;
  }

  if (tianmu_sysvar_start_async > 0)
    ResetTaskExecutor(tianmu_sysvar_start_async);
  handler::resolve_async_join_settings(tianmu_sysvar_async_join);

  return 0;
}

void Engine::HandleDeferredJobs() {
  std::scoped_lock lk(gc_tasks_mtx);
  auto sz = gc_tasks.size();
  while (sz-- > 0 && !exiting) {
    auto &t = gc_tasks.front();
    if (t.xmin < min_xid) {
      std::error_code ec;
      fs::remove(t.file, ec);
      // Ignore ENOENT since files might be deleted by 'drop table'.
      if (ec && ec != std::errc::no_such_file_or_directory) {
        TIANMU_LOG(LogCtl_Level::ERROR, "Failed to remove file %s Error:%s", t.file.string().c_str(),
                   ec.message().c_str());
      }
    } else {
      gc_tasks.emplace_back(t);
    }
    gc_tasks.pop_front();
  }
}

void Engine::DeferRemove(const fs::path &file, int32_t cookie) {
  std::scoped_lock lk(gc_tasks_mtx);
  if (fs::exists(file))
    gc_tasks.emplace_back(purge_task{file, MaxXID(), cookie});
}

Engine::~Engine() {
  {
    std::scoped_lock lk(cv_mtx);
    std::scoped_lock lk_merge(cv_merge_mtx);
    exiting = true;
    TIANMU_LOG(LogCtl_Level::INFO, "Tianmu engine shutting down.");
  }
  cv.notify_all();
  cv_merge.notify_all();
  m_load_thread.join();
  m_merge_thread.join();
  m_purge_thread.join();
  m_monitor_thread.join();

  cache.ReleaseAll();
  table_share_map.clear();
  mem_table_map.clear();
  m_table_keys.clear();

  delete m_resourceManager;
  m_resourceManager = nullptr;

  delete the_filter_block_owner;
  the_filter_block_owner = nullptr;

  try {
    mm::MemoryManagerInitializer::EnsureNoLeakedTraceableObject();
  } catch (common::AssertException &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Memory leak! %s", e.what());
  } catch (...) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Unkown exception caught");
  }
  if (rc_control_.isOn())
    mm::MemoryManagerInitializer::deinit(true);
  else
    mm::MemoryManagerInitializer::deinit(false);

  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu engine destroyed.");
}

void Engine::EncodeRecord(const std::string &table_path, int table_id, Field **field, size_t col, size_t blobs,
                          std::unique_ptr<char[]> &buf, uint32_t &size, THD *thd) {
  size = blobs > 0 ? 4_MB : 128_KB;
  buf.reset(new char[size]);
  utils::BitSet null_mask(col);

  // layout:
  //     table_id
  //     table_path
  //     null_mask
  //     fileds...
  char *ptr = buf.get();
  *(int32_t *)ptr = table_id;  // table id
  ptr += sizeof(int32_t);
  int32_t path_len = table_path.size();
  std::memcpy(ptr, table_path.c_str(), path_len);
  ptr += path_len;
  *ptr++ = 0;  // end with NUL

  auto null_offset = ptr - buf.get();
  ptr += null_mask.data_size();

  for (uint i = 0; i < col; i++) {
    Field *f = field[i];

    size_t length;
    if (f->is_flag_set(BLOB_FLAG))  // stonedb8
      length = dynamic_cast<Field_blob *>(f)->get_length();
    else
      length = f->row_pack_length();

    length += 8;

    size_t used = ptr - buf.get();
    if (size - used < length) {
      while (size - used < length) {
        size *= 2;
        if (size > utils::MappedCircularBuffer::MAX_BUF_SIZE)
          throw common::Exception(table_path + " INSERT data exceeds max buffer size " +
                                  std::to_string(utils::MappedCircularBuffer::MAX_BUF_SIZE));
      }

      std::unique_ptr<char[]> old_buf = std::move(buf);
      buf.reset(new char[size]);
      std::memcpy(buf.get(), old_buf.get(), used);
      ptr = buf.get() + used;
    }

    if (f->is_null()) {
      null_mask.set(i);
      continue;
    }

    switch (f->type()) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONGLONG: {
        int64_t v = f->val_int();
        common::PushWarningIfOutOfRange(thd, std::string(f->field_name), v, f->type(), f->is_flag_set(UNSIGNED_FLAG));
        *reinterpret_cast<int64_t *>(ptr) = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE: {
        double v = f->val_real();
        *(int64_t *)ptr = *reinterpret_cast<int64_t *>(&v);
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_NEWDECIMAL: {
        auto dec_f = dynamic_cast<Field_new_decimal *>(f);
        *(int64_t *)ptr = std::lround(dec_f->val_real() * types::PowOfTen(dec_f->dec));
        ptr += sizeof(int64_t);
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
        types::DT dt(my_time);
        *(int64_t *)ptr = dt.val;
        ptr += sizeof(int64_t);
      } break;

      case MYSQL_TYPE_TIMESTAMP: {
        MYSQL_TIME my_time;
        std::memset(&my_time, 0, sizeof(my_time));
        f->get_time(&my_time);
        auto saved = my_time.second_part;
        // convert to UTC
        if (!common::IsTimeStampZero(my_time)) {
          bool myb;
          my_time_t secs_utc = current_thd->variables.time_zone->TIME_to_gmt_sec(&my_time, &myb);
          common::GMTSec2GMTTime(&my_time, secs_utc);
        }
        my_time.second_part = saved;
        types::DT dt(my_time);
        *(int64_t *)ptr = dt.val;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_YEAR:  // what the hell?
      {
        types::DT dt = {};
        dt.year = f->val_int();
        *(int64_t *)ptr = dt.val;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_STRING: {
        String str;
        f->val_str(&str);
        *(uint32_t *)ptr = str.length();
        ptr += sizeof(uint32_t);
        std::memcpy(ptr, str.ptr(), str.length());
        ptr += str.length();
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
    ASSERT(ptr <= buf.get() + size, "Buffer overflow");
  }

  std::memcpy(buf.get() + null_offset, null_mask.data(), null_mask.data_size());
  size = ptr - buf.get();
}

std::unique_ptr<char[]> Engine::GetRecord(size_t &len) {
  auto rec = insert_buffer.Read();
  len = rec.second;
  return std::move(rec.first);
}

uint32_t Engine::GetNextTableId() {
  static std::mutex seq_mtx;

  std::scoped_lock lk(seq_mtx);
  fs::path p = tianmu_data_dir / "tianmu.tid";
  if (!fs::exists(p)) {
    TIANMU_LOG(LogCtl_Level::INFO, "Creating table id file");
    std::ofstream seq_file(p.string());
    if (seq_file)
      seq_file << 0;
    if (!seq_file) {
      throw common::FileException("Failed to write to table id file");
    }
  }

  uint32_t seq;
  std::fstream seq_file(p.string());
  if (seq_file)
    seq_file >> seq;
  if (!seq_file) {
    throw common::FileException("Failed to read from table id file");
  }
  seq++;
  seq_file.seekg(0);
  seq_file << seq;
  if (!seq_file) {
    throw common::FileException("Failed to write to table id file");
  }

  return seq;
}

std::shared_ptr<TableOption> Engine::GetTableOption(const std::string &table, TABLE *form,
                                                    [[maybe_unused]] dd::Table *table_def) {
  auto opt = std::make_shared<TableOption>();

  int power = has_pack(form->s->comment);
  if (power < 5 || power > 16) {
    TIANMU_LOG(LogCtl_Level::ERROR, "create table comment: pack size shift(%d) should be >=5 and <= 16", power);
    throw common::SyntaxException("Unexpected data pack size.");
  }

  opt->pss = power;

  for (uint i = 0; i < form->s->fields; ++i) {
    Field *f = form->field[i];
    opt->atis.push_back(Engine::GetAttrTypeInfo(*f));
  }

  opt->path = table + common::TIANMU_EXT;
  opt->name = form->s->table_name.str;
  return opt;
}

void Engine::CreateTable(const std::string &table, TABLE *form, [[maybe_unused]] dd::Table *table_def) {
  RCTable::CreateNew(GetTableOption(table, form, table_def));
}

AttributeTypeInfo Engine::GetAttrTypeInfo(const Field &field) {
  bool auto_inc = field.is_flag_set(AUTO_INCREMENT_FLAG);
  if (auto_inc && field.part_of_key.to_ulonglong() == 0) {
    throw common::AutoIncException("AUTO_INCREMENT can be only declared on primary key column!");
  }

  bool notnull = (field.null_bit == 0);
  auto str = boost::to_upper_copy(std::string(field.comment.str, field.comment.length));
  bool filter = (str.find("FILTER") != std::string::npos);

  auto fmt = common::PackFmt::DEFAULT;
  if (str.find("LOOKUP") != std::string::npos) {
    if (field.type() == MYSQL_TYPE_STRING || field.type() == MYSQL_TYPE_VARCHAR)
      fmt = common::PackFmt::LOOKUP;
    else {
      std::string s =
          "LOOKUP can only be declared on CHAR/VARCHAR column. Ignored on "
          "column ";
      s = s + (field.table ? std::string(field.table->s->table_name.str) + "." : "") + field.field_name;
      push_warning(current_thd, Sql_condition::SL_WARNING, WARN_OPTION_IGNORED, s.c_str());
    }
  } else if (str.find("NOCOMPRESS") != std::string::npos)
    fmt = common::PackFmt::NOCOMPRESS;
  else if (str.find("PPM1") != std::string::npos)
    fmt = common::PackFmt::PPM1;
  else if (str.find("PPM2") != std::string::npos)
    fmt = common::PackFmt::PPM2;
  else if (str.find("RANGECODE") != std::string::npos)
    fmt = common::PackFmt::RANGECODE;
  else if (str.find("LZ4") != std::string::npos)
    fmt = common::PackFmt::LZ4;
  else if (str.find("ZLIB") != std::string::npos)
    fmt = common::PackFmt::ZLIB;

  switch (field.type()) {
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:
      return AttributeTypeInfo(Engine::GetCorrespondingType(field), notnull, (ushort)field.field_length, 0, auto_inc,
                               DTCollation(), fmt, filter);
    case MYSQL_TYPE_TIME:
      return AttributeTypeInfo(common::ColumnType::TIME, notnull, 0, 0, false, DTCollation(), fmt, filter);
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR: {
      if (field.field_length > FIELD_MAXLENGTH)
        throw common::UnsupportedDataTypeException("Length of STRING or VARCHAR exceeds 65535 bytes.");
      // Trie column only supports String/VARCHAR column and it
      // doesn't work for case-insensitive collations.
      if (str.find("TRIE") != std::string::npos)
        fmt = common::PackFmt::TRIE;
      if (const Field_str *fstr = dynamic_cast<const Field_string *>(&field)) {
        DTCollation coll(fstr->charset(), fstr->derivation());
        if (fmt == common::PackFmt::TRIE && types::IsCaseInsensitive(coll)) {
          TIANMU_LOG(LogCtl_Level::ERROR, "TRIE can not work with case-insensitive collation: %s!",
                     coll.collation->m_coll_name);  // stonedb8
          throw common::UnsupportedDataTypeException();
        }
        if (fstr->charset() != &my_charset_bin)
          return AttributeTypeInfo(common::ColumnType::STRING, notnull, field.field_length, 0, auto_inc, coll, fmt,
                                   filter);
        return AttributeTypeInfo(common::ColumnType::BYTE, notnull, field.field_length, 0, auto_inc, coll, fmt, filter);
      } else if (const Field_str *fvstr = dynamic_cast<const Field_varstring *>(&field)) {
        DTCollation coll(fvstr->charset(), fvstr->derivation());
        if (fmt == common::PackFmt::TRIE && types::IsCaseInsensitive(coll))
          throw common::UnsupportedDataTypeException();
        if (fvstr->charset() != &my_charset_bin)
          return AttributeTypeInfo(common::ColumnType::VARCHAR, notnull, field.field_length, 0, auto_inc, coll, fmt,
                                   filter);
        return AttributeTypeInfo(common::ColumnType::VARBYTE, notnull, field.field_length, 0, auto_inc, coll, fmt,
                                 filter);
      }
      throw common::UnsupportedDataTypeException();
    }
    case MYSQL_TYPE_NEWDECIMAL: {
      const Field_new_decimal *fnd = ((const Field_new_decimal *)&field);
      if (/*fnd->precision > 0 && */ fnd->precision <= 18 /*&& fnd->dec >= 0*/
          && fnd->dec <= fnd->precision)
        return AttributeTypeInfo(common::ColumnType::NUM, notnull, fnd->precision, fnd->dec);
      throw common::UnsupportedDataTypeException("Precision must be less than or equal to 18.");
    }
    case MYSQL_TYPE_BLOB:
      if (const Field_str *fstr = dynamic_cast<const Field_str *>(&field)) {
        if (const Field_blob *fblo = dynamic_cast<const Field_blob *>(fstr)) {
          if (fblo->charset() != &my_charset_bin) {  // TINYTEXT, MEDIUMTEXT, TEXT, LONGTEXT
            common::ColumnType t = common::ColumnType::VARCHAR;
            if (field.field_length > FIELD_MAXLENGTH) {
              t = common::ColumnType::LONGTEXT;
            }
            return AttributeTypeInfo(t, notnull, field.field_length, 0, auto_inc, DTCollation(), fmt, filter);
          }
          switch (field.field_length) {
            case 255:
            case FIELD_MAXLENGTH:
              // TINYBLOB, BLOB
              return AttributeTypeInfo(common::ColumnType::VARBYTE, notnull, field.field_length, 0, auto_inc,
                                       DTCollation(), fmt, filter);
            case 16777215:
            case 4294967295:
              // MEDIUMBLOB, LONGBLOB
              return AttributeTypeInfo(common::ColumnType::BIN, notnull, field.field_length, 0, auto_inc, DTCollation(),
                                       fmt, filter);
            default:
              throw common::UnsupportedDataTypeException();
          }
        }
      }
      [[fallthrough]];
    default:
      throw common::UnsupportedDataTypeException(std::string("Unsupported data type[") +
                                                 common::get_enum_field_types_name(field.type()) + std::string("]"));
  }
  throw;
}

void Engine::CommitTx(THD *thd, bool all) {
  TIANMU_LOG(LogCtl_Level::INFO, "commit txn");

  // fix bug: issue759, in file sql_trunction.cc and function end_transaction(), will call CommitTx() twice
  // skip for first time, called in trans_commit_stmt(), thd->server_status = SERVER_STATUS_IN_TRANS and all = false
  // then called in trans_commit_implicit(), thd->server_status will be set "~SERVER_STATUS_IN_TRANS", and all = true
  // tianmu truncate table in 5.7 is ok, 8.0 has this problem.
  // Same problem with alter table ,see: https://github.com/stoneatom/stonedb/issues/1670
  // Same problem with alter table ,see: https://github.com/stoneatom/stonedb/issues/1715
  if ((thd->lex->sql_command == SQLCOM_TRUNCATE || thd->lex->sql_command == SQLCOM_ALTER_TABLE ||
       thd->lex->sql_command == SQLCOM_CREATE_TABLE) &&
      (thd->server_status & SERVER_STATUS_IN_TRANS)) {
    return;
  }
  if (all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT)) {
    GetTx(thd)->Commit(thd);
  }
  ClearTx(thd);
}

void Engine::Rollback(THD *thd, bool all, bool force_error_message) {
  force_error_message = force_error_message || (!all && thd_test_options(thd, OPTION_NOT_AUTOCOMMIT));
  TIANMU_LOG(LogCtl_Level::ERROR, "Roll back query '%s'", thd->query().str);
  if (current_txn_) {
    GetTx(thd)->Rollback(thd, force_error_message);
    ClearTx(thd);
  }
  thd->transaction_rollback_request = false;
}

int Engine::DeleteTable(const char *table, [[maybe_unused]] const dd::Table *table_def, [[maybe_unused]] THD *thd) {
  {
    std::unique_lock<std::shared_mutex> mem_guard(mem_table_mutex);
    RCMemTable::DropMemTable(table);
    mem_table_map.erase(table);
  }

  if (DeleteTableIndex(table, thd)) {
    TIANMU_LOG(LogCtl_Level::ERROR, "DeleteTable  failed");
    return 1;
  }

  UnRegisterTable(table);
  std::string p = table;
  p += common::TIANMU_EXT;
  auto id = RCTable::GetTableId(p);
  cache.ReleaseTable(id);
  filter_cache.RemoveIf([id](const FilterCoordinate &c) { return c[0] == int(id); });

  {
    std::scoped_lock lk(gc_tasks_mtx);
    gc_tasks.remove_if([id](const purge_task &t) -> bool { return static_cast<uint32_t>(t.cookie) == id; });
  }
  system::DeleteDirectory(p);
  TIANMU_LOG(LogCtl_Level::INFO, "Drop table %s, ID = %u", table, id);

  return 0;
}

void Engine::TruncateTable(const std::string &table_path, [[maybe_unused]] dd::Table *table_def,
                           [[maybe_unused]] THD *thd) {
  auto indextab = GetTableIndex(table_path);
  if (indextab != nullptr) {
    indextab->TruncateIndexTable();
  }
  auto tab = current_txn_->GetTableByPath(table_path);
  tab->Truncate();
  auto id = tab->GetID();
  cache.ReleaseTable(id);
  filter_cache.RemoveIf([id](const FilterCoordinate &c) { return c[0] == int(id); });
  TIANMU_LOG(LogCtl_Level::INFO, "Truncated table %s, ID = %u", table_path.c_str(), id);
}

void Engine::GetTableIterator(const std::string &table_path, RCTable::Iterator &iter_begin, RCTable::Iterator &iter_end,
                              std::shared_ptr<RCTable> &table, const std::vector<bool> &attrs, THD *thd) {
  table = GetTx(thd)->GetTableByPath(table_path);
  iter_begin = table->Begin(attrs);
  iter_end = table->End();
}

std::shared_ptr<RCTable> Engine::GetTableRD(const std::string &table_path) {
  return getTableShare(table_path)->GetSnapshot();
}

std::vector<AttrInfo> Engine::GetTableAttributesInfo(const std::string &table_path,
                                                     [[maybe_unused]] TABLE_SHARE *table_share) {
  std::scoped_lock guard(global_mutex_);

  std::shared_ptr<RCTable> tab;
  if (current_txn_ != nullptr)
    tab = current_txn_->GetTableByPath(table_path);
  else
    tab = GetTableRD(table_path);
  // rccontrol << "Table " << table_path << " (" << tab->GetID() << ") accessed
  // by MySQL engine." << endl;
  return tab->GetAttributesInfo();
}

void Engine::UpdateAndStoreColumnComment(TABLE *table, int field_id, Field *source_field, int source_field_id,
                                         CHARSET_INFO *cs) {
  // stonedb8 start convert orig_table to table, MySQL 8.0 don't have orig_table, idea from
  // ha_innodb.cc:create_table_def
  if (source_field->table->s->db_type() == tianmu_hton) {  // do not use table (cont. default values)
    char buf_size[256] = {0};
    char buf_ratio[256] = {0};
    uint buf_size_count = 0;
    uint buf_ratio_count = 0;
    int64_t sum_c = 0, sum_u = 0;

    std::vector<AttrInfo> attr_info = GetTableAttributesInfo(source_field->table->s->path.str, source_field->table->s);
    // stonedb8 end

    bool is_unique = false;
    if (source_field_id < (int)attr_info.size()) {
      is_unique = attr_info[source_field_id].actually_unique;
      sum_c += attr_info[source_field_id].comp_size;
      sum_u += attr_info[source_field_id].uncomp_size;
    }

    double d_comp = int(sum_c / 104857.6) / 10.0;  // 1 MB = 2^20 bytes
    if (d_comp < 0.1)
      d_comp = 0.1;
    double ratio = (sum_c > 0 ? sum_u / double(sum_c) : 0);
    if (ratio > 1000)
      ratio = 999.99;

    buf_size_count = std::snprintf(buf_size, 256, "Size[MB]: %.1f", d_comp);
    if (is_unique)
      buf_ratio_count = std::snprintf(buf_ratio, 256, "; Ratio: %.2f; unique", ratio);
    else
      buf_ratio_count = std::snprintf(buf_ratio, 256, "; Ratio: %.2f", ratio);

    uint len = uint(source_field->comment.length) + buf_size_count + buf_ratio_count + 3;
    //  char* full_comment = new char(len);  !!!! DO NOT USE NEW
    //  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11
    char *full_comment = (char *)my_malloc(0, len, MYF(0));
    for (uint i = 0; i < len; i++) full_comment[i] = ' ';
    full_comment[len - 1] = 0;

    char *pos = full_comment + source_field->comment.length;
    if (source_field->comment.length) {
      std::memcpy(full_comment, source_field->comment.str, source_field->comment.length);
      *pos++ = ';';
      *pos++ = ' ';
    }

    // std::memcpy_s(pos, buf_size_count, buf_size, buf_size_count);
    std::memcpy(pos, buf_size, buf_size_count);
    pos += buf_size_count;
    // std::memcpy_s(pos, buf_ratio_count, buf_ratio, buf_ratio_count);
    std::memcpy(pos, buf_ratio, buf_ratio_count);
    pos[buf_ratio_count] = 0;

    table->field[field_id]->store(full_comment, len - 1, cs);
    //  delete [] full_comment; !!!! DO NOT USE DELETE
    //  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    my_free(full_comment);
    //  table->field[field_id]->store("abcde", 5, cs);
  } else {
    table->field[field_id]->store(source_field->comment.str, uint(source_field->comment.length), cs);
  }
}

void Engine::AddTx(Transaction *tx) {
  std::scoped_lock guard(m_txs_mtx);
  m_txs.insert(tx);
  min_xid = (*m_txs.begin())->GetID();
  max_xid = (*m_txs.rbegin())->GetID();
}

void Engine::RemoveTx(Transaction *tx) {
  std::scoped_lock guard(m_txs_mtx);
  m_txs.erase(tx);
  if (!m_txs.empty()) {
    min_xid = (*m_txs.begin())->GetID();
    max_xid = (*m_txs.rbegin())->GetID();
  } else {
    min_xid = UINT64_MAX;
    max_xid = UINT64_MAX;
  }
  delete tx;
}

Transaction *Engine::CreateTx(THD *thd) {
  // the transaction should be created by owner THD
  ASSERT(thd->get_ha_data(tianmu_hton->slot)->ha_ptr == nullptr, "Nested transaction is not supported!");  // stonedb8
  ASSERT(current_txn_ == nullptr, "Previous transaction is not finished!");

  current_txn_ = new Transaction(thd);
  thd->get_ha_data(tianmu_hton->slot)->ha_ptr = current_txn_;  // stonedb8

  AddTx(current_txn_);

  return current_txn_;
}

Transaction *Engine::GetTx(THD *thd) {
  if (thd->get_ha_data(tianmu_hton->slot)->ha_ptr == nullptr)
    return CreateTx(thd);                                                          // stonedb8
  return static_cast<Transaction *>(thd->get_ha_data(tianmu_hton->slot)->ha_ptr);  // stonedb8
}

void Engine::ClearTx(THD *thd) {
  ASSERT(current_txn_ == (Transaction *)thd->get_ha_data(tianmu_hton->slot)->ha_ptr, "Bad transaction");  // stonedb8

  if (current_txn_ == nullptr)
    return;

  RemoveTx(current_txn_);
  current_txn_ = nullptr;
  thd->get_ha_data(tianmu_hton->slot)->ha_ptr = nullptr;  // stonedb8
}

int Engine::SetUpCacheFolder(const std::string &cachefolder_path) {
  if (!fs::exists(cachefolder_path)) {
    TIANMU_LOG(LogCtl_Level::INFO, "Cachefolder %s does not exist. Trying to create it.", cachefolder_path.c_str());
    std::error_code ec;
    fs::create_directories(cachefolder_path, ec);
    if (ec) {
      sql_print_error("Tianmu: Can not create folder %s.", cachefolder_path.c_str());
      TIANMU_LOG(LogCtl_Level::ERROR, "DatabaseException: %s", ec.message().c_str());
      return 1;
    }
  }

  if (!system::IsReadWriteAllowed(cachefolder_path)) {
    sql_print_error("Tianmu: Can not access cache folder %s.", cachefolder_path.c_str());
    TIANMU_LOG(LogCtl_Level::ERROR, "Can not access cache folder %s", cachefolder_path.c_str());
    return 1;
  }
  return 0;
}

std::string get_parameter_name(enum tianmu_var_name vn) {
  DEBUG_ASSERT(static_cast<int>(vn) >= 0 &&
               static_cast<int>(vn) <= static_cast<int>(tianmu_var_name::TIANMU_VAR_LIMIT));
  return tianmu_var_name_strings[static_cast<int>(vn)];
}

int get_parameter(THD *thd, enum tianmu_var_name vn, double &value) {
  std::string var_data = get_parameter_name(vn);
  // stonedb8 start
  bool null_val;

  const auto it = thd->user_vars.find(var_data);
  if (it == thd->user_vars.end())
    return 1;
  value = it->second->val_real(&null_val);
  // stonedb8 end
  if (null_val)
    return 2;
  return 0;
}

int get_parameter(THD *thd, enum tianmu_var_name vn, [[maybe_unused]] int64_t &value) {
  std::string var_data = get_parameter_name(vn);
  // stonedb8 start
  bool null_val;

  const auto it = thd->user_vars.find(var_data);
  if (it == thd->user_vars.end())
    return 1;
  it->second->val_int(&null_val);
  // stonedb8 end
  if (null_val)
    return 2;
  return 0;
}

int get_parameter(THD *thd, enum tianmu_var_name vn, std::string &value) {
  // stonedb8 start
  bool null_val;
  std::string var_data = get_parameter_name(vn);
  String str;

  const auto it = thd->user_vars.find(var_data);
  if (it == thd->user_vars.end())
    return 1;
  it->second->val_str(&null_val, &str, DECIMAL_NOT_SPECIFIED);
  // stonedb8 end
  if (null_val)
    return 2;
  value = std::string(str.ptr());

  return 0;
}

int get_parameter(THD *thd, enum tianmu_var_name vn, longlong &result, std::string &s_result) {
  // stonedb8 start
  std::string var_data = get_parameter_name(vn);
  const auto entry = thd->user_vars.find(var_data);

  if (entry == thd->user_vars.end())
    return 1;

  if (entry->second->type() == DECIMAL_RESULT) {
    switch (vn) {
      case tianmu_var_name::TIANMU_ABORT_ON_THRESHOLD: {
        double dv;
        bool null_value;
        my_decimal v;

        entry->second->val_decimal(&null_value, &v);
        my_decimal2double(E_DEC_FATAL_ERROR, &v, &dv);
        result = *(longlong *)&dv;
        break;
      }
      default:
        result = -1;
        break;
    }
    return 0;
  } else if (entry->second->type() == INT_RESULT) {
    switch (vn) {
      case tianmu_var_name::TIANMU_THROTTLE:
      case tianmu_var_name::TIANMU_TIANMUEXPRESSIONS:
      case tianmu_var_name::TIANMU_PARALLEL_AGGR:
      case tianmu_var_name::TIANMU_ABORT_ON_COUNT:
        bool null_value;
        result = entry->second->val_int(&null_value);
        break;
      default:
        result = -1;
        break;
    }
    return 0;
  } else if (entry->second->type() == STRING_RESULT) {
    result = -1;
    bool null_value;
    String str;

    entry->second->val_str(&null_value, &str, DECIMAL_NOT_SPECIFIED);
    var_data = std::string(str.ptr());

    if (vn == tianmu_var_name::TIANMU_DATAFORMAT || vn == tianmu_var_name::TIANMU_REJECT_FILE_PATH) {
      s_result = var_data;
    } else if (vn == tianmu_var_name::TIANMU_PIPEMODE) {
      boost::to_upper(var_data);
      if (var_data == "SERVER")
        result = 1;
      if (var_data == "CLIENT")
        result = 0;
    } else if (vn == tianmu_var_name::TIANMU_NULL) {
      s_result = var_data;
    }
    return 0;
  } else {
    result = 0;
    return 2;
  }
  // stonedb8 end
}

int Engine::RenameTable([[maybe_unused]] Transaction *trans_, const std::string &from, const std::string &to,
                        [[maybe_unused]] const dd::Table *from_table_def, [[maybe_unused]] dd::Table *to_table_def,
                        [[maybe_unused]] THD *thd) {
  UnRegisterTable(from);
  auto id = RCTable::GetTableId(from + common::TIANMU_EXT);
  cache.ReleaseTable(id);
  filter_cache.RemoveIf([id](const FilterCoordinate &c) { return c[0] == int(id); });
  system::RenameFile(tianmu_data_dir / (from + common::TIANMU_EXT), tianmu_data_dir / (to + common::TIANMU_EXT));
  RenameRdbTable(from, to);
  DBUG_EXECUTE_IF("delete tianmu primary key", DeleteTableIndex(from, thd););
  if (DeleteTableIndex(from, thd)) {
    TIANMU_LOG(LogCtl_Level::ERROR, "delete tianmu primary key failed.");
    return 1;
  }
  UnregisterMemTable(from, to);
  TIANMU_LOG(LogCtl_Level::INFO, "Rename table %s to %s", from.c_str(), to.c_str());

  return 0;
}

void Engine::PrepareAlterTable(const std::string &table_path, std::vector<Field *> &new_cols,
                               std::vector<Field *> &old_cols, [[maybe_unused]] THD *thd) {
  // make sure all deffered jobs are done in case some file gets recreated.
  HandleDeferredJobs();

  auto tab = current_txn_->GetTableByPath(table_path);
  RCTable::Alter(table_path, new_cols, old_cols, tab->NumOfObj());
  cache.ReleaseTable(tab->GetID());
  UnRegisterTable(table_path);
}

static void HandleDelayedLoad(int table_id, std::vector<std::unique_ptr<char[]>> &vec) {
  std::string addr = std::to_string(reinterpret_cast<long>(&vec));

  std::string table_path(vec[0].get() + sizeof(int32_t));
  std::string db_name, tab_name;

  std::tie(db_name, tab_name) = GetNames(table_path);
  std::string load_data_query = "LOAD DELAYED INSERT DATA INTO TABLE " + db_name + "." + tab_name;

  LEX_CSTRING dbname, tabname, loadquery;
  dbname.str = const_cast<char *>(db_name.c_str());
  dbname.length = db_name.length();
  loadquery.str = const_cast<char *>(load_data_query.c_str());
  loadquery.length = load_data_query.length();

  // strconvert: support special characters in table name
  char t_tbname[MAX_TABLE_NAME_LEN + 1] = {0};
  tabname.length = filename_to_tablename(const_cast<char *>(tab_name.c_str()), t_tbname, sizeof(t_tbname));
  tabname.str = t_tbname;
  // END

  // TIANMU UPGRADE
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();  // global thread manager

  THD *thd = new THD;
  thd->thread_stack = (char *)&thd;
  my_thread_init();
  thd->store_globals();
  thd->m_security_ctx->skip_grants();

  /* Add thread to THD list so that's it's visible in 'show processlist' */
  thd->set_new_thread_id();
  thd->set_time();  // stonedb8
  thd_manager->add_thd(thd);
  mysql_reset_thd_for_next_command(thd);
  thd->init_query_mem_roots();  // stonedb8
  thd->set_db(dbname);
  // Forge LOAD DATA INFILE query which will be used in SHOW PROCESS LIST
  thd->set_query(loadquery);
  thd->set_query_id(next_query_id());
  thd->update_charset();  // for the charset change to take effect
  // Usually lex_start() is called by mysql_parse(), but we need
  // it here as the present method does not call mysql_parse().
  lex_start(thd);
  TABLE_LIST tl(thd->strmake(thd->db().str, thd->db().length), thd->db().length, tabname.str, tabname.length,
                tabname.str, TL_WRITE_CONCURRENT_INSERT);  // stonedb8
  tl.updating = 1;

  // the table will be opened in mysql_load
  thd->lex->sql_command = SQLCOM_LOAD;
  sql_exchange ex("buffered_insert", 0, FILETYPE_MEM);
  ex.file_name = const_cast<char *>(addr.c_str());
  ex.skip_lines = table_id;  // this is ugly...
  thd->lex->query_block->context.resolve_in_table_list_only(&tl);
  if (open_temporary_tables(thd, &tl)) {
    // error/////
  }
  List<Item> tmp_list;  // dummy
  // stonedb8 start
  /*
  if (mysql_load(thd, &ex, &tl, tmp_list, tmp_list, tmp_list, DUP_ERROR, false)) {
      thd->is_slave_error = 1;
  }
  */
  thd->lex->query_tables = &tl;
  LEX_STRING lex_str = {const_cast<char *>(ex.file_name), addr.size()};

  thd->lex->m_sql_cmd = new (thd->mem_root)
      Sql_cmd_load_table(ex.filetype, false, lex_str, On_duplicate::ERROR, nullptr, nullptr, nullptr, nullptr, ex.field,
                         ex.line, ex.skip_lines, nullptr, nullptr, nullptr, nullptr);

  if (thd->lex->m_sql_cmd->execute(thd)) {
    thd->is_slave_error = 1;
  }

  // stonedb8 end

  thd->set_catalog({0, 1});  // TIANMU UPGRADE
  thd->set_db({nullptr, 0}); /* will free the current database */
  thd->reset_query();
  thd->get_stmt_da()->set_overwrite_status(true);
  thd->is_error() ? trans_rollback_stmt(thd) : trans_commit_stmt(thd);
  thd->get_stmt_da()->set_overwrite_status(false);
  close_thread_tables(thd);
  if (thd->transaction_rollback_request) {
    trans_rollback_implicit(thd);
    thd->mdl_context.release_transactional_locks();
  } else if (!thd->in_multi_stmt_transaction_mode())
    thd->mdl_context.release_transactional_locks();
  else
    thd->mdl_context.release_statement_locks();

  thd->mem_root->ClearForReuse();  // stonedb8
  if (thd->is_fatal_error()) {
    TIANMU_LOG(LogCtl_Level::ERROR, "LOAD DATA failed on table '%s'", tab_name.c_str());
  }
  thd->release_resources();
  thd_manager->remove_thd(thd);
  delete thd;
  my_thread_end();
}

void DistributeLoad(std::unordered_map<int, std::vector<std::unique_ptr<char[]>>> &tm) {
  utils::result_set<void> res;
  for (auto &it : tm) {
    res.insert(ha_rcengine_->delay_insert_thread_pool.add_task(HandleDelayedLoad, it.first, std::ref(it.second)));
  }
  res.get_all();
  tm.clear();
}

void Engine::ProcessDelayedInsert() {
  mysql_mutex_lock(&LOCK_server_started);
  while (!mysqld_server_started) {
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&COND_server_started, &LOCK_server_started, &abstime);
    if (exiting) {
      mysql_mutex_unlock(&LOCK_server_started);
      return;
    }
  }
  mysql_mutex_unlock(&LOCK_server_started);

  std::unordered_map<int, std::vector<std::unique_ptr<char[]>>> tm;
  int buffer_recordnum = 0;
  int sleep_cnt = 0;
  while (!exiting) {
    if (tianmu_sysvar_enable_rowstore) {
      std::unique_lock<std::mutex> lk(cv_mtx);
      cv.wait_for(lk, std::chrono::milliseconds(tianmu_sysvar_insert_wait_ms));
      continue;
    }

    size_t len;
    auto rec = GetRecord(len);
    if (len == 0) {
      if (tm.empty()) {
        std::unique_lock<std::mutex> lk(cv_mtx);
        cv.wait_for(lk, std::chrono::milliseconds(tianmu_sysvar_insert_wait_ms));
      } else {
        if (buffer_recordnum >= tianmu_sysvar_insert_numthreshold || sleep_cnt > tianmu_sysvar_insert_cntthreshold) {
          DistributeLoad(tm);
          buffer_recordnum = 0;
          sleep_cnt = 0;
        } else {
          std::unique_lock<std::mutex> lk(cv_mtx);
          cv.wait_for(lk, std::chrono::milliseconds(tianmu_sysvar_insert_wait_ms));
          sleep_cnt++;
        }
      }
      continue;
    }

    buffer_recordnum++;
    auto table_id = *(int32_t *)rec.get();
    tm[table_id].emplace_back(std::move(rec));
    if (tm[table_id].size() > static_cast<std::size_t>(tianmu_sysvar_insert_max_buffered)) {
      // in case the ingress rate is too high
      DistributeLoad(tm);
      buffer_recordnum = 0;
    }
  }
  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu load thread exiting...");
}

void Engine::ProcessDelayedMerge() {
  mysql_mutex_lock(&LOCK_server_started);
  while (!mysqld_server_started) {
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&COND_server_started, &LOCK_server_started, &abstime);
    if (exiting) {
      mysql_mutex_unlock(&LOCK_server_started);
      return;
    }
  }
  mysql_mutex_unlock(&LOCK_server_started);

  std::map<std::string, int> sleep_cnts;
  while (!exiting) {
    if (!tianmu_sysvar_enable_rowstore) {
      std::unique_lock<std::mutex> lk(cv_merge_mtx);
      cv_merge.wait_for(lk, std::chrono::milliseconds(tianmu_sysvar_insert_wait_ms));
      continue;
    }

    std::unordered_map<int, std::vector<std::unique_ptr<char[]>>> tm;
    {
      try {
        std::scoped_lock guard(mem_table_mutex);
        for (auto &[name, mem_table] : mem_table_map) {
          int64_t record_count = mem_table->CountRecords();
          if (record_count >= tianmu_sysvar_insert_numthreshold ||
              (sleep_cnts.count(name) && sleep_cnts[name] > tianmu_sysvar_insert_cntthreshold)) {
            auto share = ha_rcengine_->getTableShare(name);
            auto table_id = share->TabID();
            utils::BitSet null_mask(share->NumOfCols());
            std::unique_ptr<char[]> buf(new char[sizeof(uint32_t) + name.size() + 1 + null_mask.data_size()]);
            char *ptr = buf.get();
            *(int32_t *)ptr = table_id;  // table id
            ptr += sizeof(int32_t);
            std::memcpy(ptr, name.c_str(), name.size());
            ptr += name.size();
            *ptr++ = 0;  // end with NUL
            std::memcpy(ptr, null_mask.data(), null_mask.data_size());
            tm[table_id].emplace_back(std::move(buf));
            sleep_cnts[name] = 0;
          } else if (record_count > 0) {
            if (sleep_cnts.count(name))
              sleep_cnts[name]++;
            else
              sleep_cnts[name] = 0;
          }
        }
      } catch (common::Exception &e) {
        TIANMU_LOG(LogCtl_Level::ERROR, "delayed merge failed. %s %s", e.what(), e.trace().c_str());
        std::unique_lock<std::mutex> lk(cv_merge_mtx);
        cv_merge.wait_for(lk, std::chrono::milliseconds(tianmu_sysvar_insert_wait_ms));
        continue;
      } catch (...) {
        TIANMU_LOG(LogCtl_Level::ERROR, "delayed merge failed.");
        std::unique_lock<std::mutex> lk(cv_merge_mtx);
        cv_merge.wait_for(lk, std::chrono::milliseconds(tianmu_sysvar_insert_wait_ms));
        continue;
      }
    }
    if (!tm.empty())
      DistributeLoad(tm);
    else {
      std::unique_lock<std::mutex> lk(cv_merge_mtx);
      cv_merge.wait_for(lk, std::chrono::milliseconds(tianmu_sysvar_insert_wait_ms));
    }
  }
  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu merge thread exiting...");
}

void Engine::LogStat() {
  static long last_sample_time = 0;
  static query_id_t saved_query_id = 0;
  static TianmuStat saved;

  if (last_sample_time == 0) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    last_sample_time = t.tv_sec;
    saved_query_id = atomic_global_query_id;  // stonedb8
    saved = tianmu_stat;
    return;
  }

  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  long sample_time = t.tv_sec;
  long diff = sample_time - last_sample_time;
  if (diff == 0) {
    TIANMU_LOG(LogCtl_Level::ERROR, "LogStat() called too frequently. last sample time %ld ", last_sample_time);
    return;
  }
  query_id_t query_id = atomic_global_query_id;  // stonedb8
  long queries = query_id - saved_query_id;

  {
    static ulong saved_com_stat[SQLCOM_END];

    // commands we are interested in
    static const enum_sql_command cmds[] = {
        SQLCOM_SELECT,
        // SQLCOM_CREATE_TABLE,
        // SQLCOM_ALTER_TABLE,
        SQLCOM_UPDATE,
        SQLCOM_INSERT,
        // SQLCOM_INSERT_SELECT,
        // SQLCOM_DELETE,
        // SQLCOM_TRUNCATE,
        // SQLCOM_DROP_TABLE,
        SQLCOM_LOAD,
    };

    System_status_var sv;  // stonedb8
    mysql_mutex_lock(&LOCK_status);
    calc_sum_of_all_status(&sv);
    mysql_mutex_unlock(&LOCK_status);
    std::string msg("Command: ");
    for (auto &c : cmds) {
      auto delta = sv.com_stat[c] - saved_com_stat[c];
      saved_com_stat[c] = sv.com_stat[c];
      msg =
          msg + sql_statement_names[c].str + " " + std::to_string(delta) + "/" + std::to_string(sv.com_stat[c]) + ", ";
    }
    msg = msg + "queries " + std::to_string(queries) + "/" + std::to_string(atomic_global_query_id);  // stonedb8
    TIANMU_LOG(LogCtl_Level::INFO, "%s", msg.c_str());
  }

  TIANMU_LOG(LogCtl_Level::INFO,
             "Select: %lu/%lu, Loaded: %lu/%lu(%lu/%lu), dup: %lu/%lu, insert: "
             "%lu/%lu, failed insert: %lu/%lu, update: "
             "%lu/%lu",
             tianmu_stat.select - saved.select, tianmu_stat.select, tianmu_stat.loaded - saved.loaded,
             tianmu_stat.loaded, tianmu_stat.load_cnt - saved.load_cnt, tianmu_stat.load_cnt,
             tianmu_stat.loaded_dup - saved.loaded_dup, tianmu_stat.loaded_dup,
             tianmu_stat.delayinsert - saved.delayinsert, tianmu_stat.delayinsert,
             tianmu_stat.failed_delayinsert - saved.failed_delayinsert, tianmu_stat.failed_delayinsert,
             tianmu_stat.update - saved.update, tianmu_stat.update);

  if (tianmu_stat.loaded == saved.loaded && tianmu_stat.delayinsert > saved.delayinsert) {
    TIANMU_LOG(LogCtl_Level::ERROR, "No data loaded from insert buffer");
  }

  // update with last minute statistics
  IPM = tianmu_stat.delayinsert - saved.delayinsert;
  IT = tianmu_stat.delayinsert;
  QPM = tianmu_stat.select - saved.select;
  QT = tianmu_stat.select;
  LPM = tianmu_stat.loaded - saved.loaded;
  LT = tianmu_stat.loaded;
  LDPM = tianmu_stat.loaded_dup - saved.loaded_dup;
  LDT = tianmu_stat.loaded_dup;
  UPM = tianmu_stat.update - saved.update;
  UT = tianmu_stat.update;

  last_sample_time = sample_time;
  saved_query_id = query_id;
  saved = tianmu_stat;
}

void Engine::InsertDelayed(const std::string &table_path, int table_id, TABLE *table) {
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
  std::shared_ptr<void> defer(nullptr,
                              [table, org_bitmap](...) { dbug_tmp_restore_column_map(table->read_set, org_bitmap); });

  uint32_t buf_sz = 0;
  std::unique_ptr<char[]> buf;
  EncodeRecord(table_path, table_id, table->field, table->s->fields, table->s->blob_fields, buf, buf_sz, table->in_use);

  int failed = 0;
  while (true) {
    try {
      insert_buffer.Write(utils::MappedCircularBuffer::TAG::INSERT_RECORD, buf.get(), buf_sz);
    } catch (std::length_error &e) {
      if (failed++ >= tianmu_sysvar_insert_wait_time / 50) {
        TIANMU_LOG(LogCtl_Level::ERROR, "insert buffer is out of space");
        throw e;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      continue;
    } catch (...) {
      throw;
    }
    break;
  }
}

void Engine::InsertMemRow(const std::string &table_path, std::shared_ptr<TableShare> &share, TABLE *table) {
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
  std::shared_ptr<void> defer(nullptr,
                              [table, org_bitmap](...) { dbug_tmp_restore_column_map(table->read_set, org_bitmap); });

  uint32_t buf_sz = 0;
  std::unique_ptr<char[]> buf;
  EncodeRecord(table_path, share->TabID(), table->field, table->s->fields, table->s->blob_fields, buf, buf_sz,
               table->in_use);
  auto rctable = share->GetSnapshot();
  rctable->InsertMemRow(std::move(buf), buf_sz);
}

int Engine::InsertRow(const std::string &table_path, [[maybe_unused]] Transaction *trans_, TABLE *table,
                      std::shared_ptr<TableShare> &share) {
  int ret = 0;
  try {
    if (tianmu_sysvar_insert_delayed && table->s->tmp_table == NO_TMP_TABLE) {
      if (tianmu_sysvar_enable_rowstore) {
        InsertMemRow(table_path, share, table);
      } else {
        InsertDelayed(table_path, share->TabID(), table);
      }
      tianmu_stat.delayinsert++;
    } else {
      current_txn_->SetLoadSource(common::LoadSource::LS_Direct);
      auto rct = current_txn_->GetTableByPath(table_path);
      ret = rct->Insert(table);
    }
    return ret;
  } catch (common::OutOfRangeException &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "inserting data out of range. %s %s", e.what(), e.trace().c_str());
    // in strict sql_mode, we should return error no.
    // TODO: in the future, we'll support insert success with warning in strict sql_mode, currently the data is
    // not write into data file, refs crashed issue: https://github.com/stoneatom/stonedb/issues/1716
    if (trans_->Thd()->is_strict_mode()) {
      ret = 1;
    }
  } catch (common::Exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "delayed inserting failed. %s %s", e.what(), e.trace().c_str());
  } catch (std::exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "delayed inserting failed. %s", e.what());
  } catch (...) {
    TIANMU_LOG(LogCtl_Level::ERROR, "delayed inserting failed.");
  }

  if (tianmu_sysvar_insert_delayed) {
    tianmu_stat.failed_delayinsert++;
    ret = 1;
  }

  return ret;
}

common::TianmuError Engine::RunLoader(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg) {
  common::TianmuError tianmu_e;
  TABLE *table;
  int transactional_table = 0;

  table = table_list->table;
  transactional_table = table->file->has_transactions();

  try {
    std::unique_ptr<system::IOParameters> iop;

    auto tianmu_error = Engine::GetIOParams(iop, *thd, *ex, table, arg);

    if (tianmu_error != common::ErrorCode::SUCCESS)
      throw tianmu_error;

    std::string table_path = GetTablePath(table);

    table->copy_blobs = 0;
    thd->num_truncated_fields = 0L;  // stonedb8

    auto tab = current_txn_->GetTableByPath(table_path);

    tab->LoadDataInfile(*iop);

    if (current_txn_->Killed()) {
      thd->send_kill_message();
      throw common::TianmuError(common::ErrorCode::KILLED);
    }

    // We must invalidate the table in query cache before binlog writing and
    // ha_autocommit_...
    // query_cache.invalidate(thd, table_list, 0);  // stonedb8 TODO: query_cache is deleted by MySQL8

    COPY_INFO::Statistics stats;
    stats.records = ha_rows(tab->NoRecordsLoaded());
    tianmu_stat.loaded += tab->NoRecordsLoaded();
    tianmu_stat.loaded_dup += tab->NoRecordsDuped();
    tianmu_stat.load_cnt++;

    char name[FN_REFLEN];
    snprintf(name, sizeof(name), ER(ER_LOAD_INFO), (long)stats.records,
             0,  // deleted
             0,  // skipped
             (long)thd->get_stmt_da()->current_statement_cond_count());

    /* ok to client */
    my_ok(thd, stats.records, 0L, name);
  } catch (common::Exception &e) {
    tianmu_e = common::TianmuError(common::ErrorCode::UNKNOWN_ERROR, "Tianmu internal error");
  } catch (common::TianmuError &e) {
    tianmu_e = e;
  }

  // if (thd->transaction.stmt.cannot_safely_rollback())
  //    thd->transaction.all.mark_modified_non_trans_table();

  if (transactional_table) {
    if (tianmu_e == common::ErrorCode::SUCCESS)
      trans_commit_stmt(thd);
    else
      trans_rollback_stmt(thd);
  }

  if (thd->lock) {
    mysql_unlock_tables(thd, thd->lock);
    thd->lock = 0;
  }
  // thd->abort_on_warning = 0;
  return tianmu_e;
}

// stonedb8 Query_block
QueryRouteTo Engine::RouteTo(THD *thd, TABLE_LIST *table_list, Query_block *selects_list,
                             bool &in_case_of_failure_can_go_to_mysql, bool with_insert) {
  in_case_of_failure_can_go_to_mysql = true;

  if (!table_list)
    return QueryRouteTo::TO_MYSQL;

  if (with_insert)
    table_list = table_list->next_global ? table_list->next_global : *(table_list->prev_global);  // we skip one

  bool has_TianmuTable = false;
  for (TABLE_LIST *tl = table_list; tl; tl = tl->next_global) {  // we go through tables
    if (!tl->is_view_or_derived() && !tl->is_view()) {
      // In this list we have all views, derived tables and their sources,
      // so anyway we walk through all the source tables
      // even though we seem to reject the control of views
      if (!(has_TianmuTable = IsTianmuTable(tl->table)))
        return QueryRouteTo::TO_MYSQL;
    }
  }

  if (!has_TianmuTable)  // No Tianmu table is involved. Return to MySQL.
    return QueryRouteTo::TO_MYSQL;

  // then we check the parameter of file format.
  // if it is MYSQL_format AND we write to a file, it is a MYSQL route.
  int is_dump;
  const char *file = GetFilename(selects_list, is_dump);

  if (file) {  // it writes to a file
    longlong param = 0;
    std::string s_res;

    if (get_parameter(thd, tianmu_var_name::TIANMU_DATAFORMAT, param, s_res))
      return QueryRouteTo::TO_TIANMU;

    if (boost::iequals(boost::trim_copy(s_res), "MYSQL"))
      return QueryRouteTo::TO_MYSQL;

    common::DataFormatPtr df = common::DataFormat::GetDataFormat(s_res);
    if (!df) {  // parameter is UNKNOWN VALUE

      my_message(ER_SYNTAX_ERROR, "Histgore specific error: Unknown value of TIANMU_DATAFORMAT parameter", MYF(0));
      return QueryRouteTo::TO_TIANMU;

    } else if (!df->CanExport()) {
      my_message(
          ER_SYNTAX_ERROR,
          (std::string("Tianmu specific error: Export in '") + df->GetName() + ("' format is not supported.")).c_str(),
          MYF(0));
      return QueryRouteTo::TO_TIANMU;

    } else
      in_case_of_failure_can_go_to_mysql = false;  // in case of failure
                                                   // it cannot go to MYSQL - it writes to a file,
                                                   // but the file format is not MYSQL
                                                   // param not set - we assume it is (deprecated: MYSQL)
                                                   // common::EDF::TRI_UNKNOWN
  }                                                // if(file)

  return QueryRouteTo::TO_TIANMU;
}

bool Engine::IsTianmuTable(TABLE *table) {
  return table && table->s->db_type() == tianmu_hton;  // table->db_type is always nullptr
}

const char *Engine::GetFilename(Query_block *selects_list, int &is_dumpfile) {  // stonedb8
  // if the function returns a filename <> nullptr
  // additionally is_dumpfile indicates whether it was 'select into OUTFILE' or
  // maybe 'select into DUMPFILE' if the function returns nullptr it was a regular
  // 'select' don't look into is_dumpfile in this case

  if (selects_list->parent_lex->result == nullptr) {
    return nullptr;
  }

  auto exchange = static_cast<Query_result_to_file *>(selects_list->parent_lex->result)->get_sql_exchange();

  if (exchange) {
    is_dumpfile = exchange->dumpfile;
    return exchange->file_name;
  }

  return nullptr;
}

std::unique_ptr<system::IOParameters> Engine::CreateIOParameters(const std::string &path, void *arg) {
  if (path.empty())
    return std::unique_ptr<system::IOParameters>(new system::IOParameters());

  std::string data_dir;
  std::string data_path;

  if (fs::path(path).is_absolute()) {
    data_dir = "";
    data_path = path;
  } else {
    data_dir = ha_rcengine_->tianmu_data_dir;
    std::string db_name, tab_name;
    std::tie(db_name, tab_name) = GetNames(path);
    data_path += db_name;
    data_path += '/';
    data_path += tab_name;
  }

  std::unique_ptr<system::IOParameters> iop(new system::IOParameters(data_dir, data_path));
  char fname[FN_REFLEN + sizeof("Index.xml")];
  get_charsets_dir(fname);
  iop->SetCharsetsDir(std::string(fname));

  iop->SetATIs(current_txn_->GetTableByPath(path)->GetATIs());
  iop->SetLogInfo(arg);
  return iop;
}

std::unique_ptr<system::IOParameters> Engine::CreateIOParameters([[maybe_unused]] THD *thd, TABLE *table, void *arg) {
  if (table == nullptr)
    return CreateIOParameters("", arg);

  return CreateIOParameters(GetTablePath(table), arg);
}

std::string Engine::GetTablePath(TABLE *table) { return table->s->normalized_path.str; }

//  compute time zone difference between client and server
void Engine::ComputeTimeZoneDiffInMinutes(THD *thd, short &sign, short &minutes) {
  if (strcasecmp(thd->variables.time_zone->get_name()->ptr(), "System") == 0) {
    // System time zone
    sign = 1;
    minutes = common::NULL_VALUE_SH;
    return;
  }
  MYSQL_TIME client_zone, utc;
  utc.year = 1970;
  utc.month = 1;
  utc.day = 1;
  utc.hour = 0;
  utc.minute = 0;
  utc.second = 0;
  utc.second_part = 0;
  utc.neg = 0;
  utc.time_type = MYSQL_TIMESTAMP_DATETIME;
  thd->variables.time_zone->gmt_sec_to_TIME(&client_zone, (my_time_t)0);  // check if client time zone is set
  longlong secs;
  long msecs;
  sign = 1;
  minutes = 0;
  if (calc_time_diff(utc, client_zone, 1, &secs, &msecs))
    sign = -1;  // stonedb8
  minutes = (short)(secs / 60);
}

common::TianmuError Engine::GetRejectFileIOParameters(THD &thd, std::unique_ptr<system::IOParameters> &io_params) {
  std::string reject_file;
  int64_t abort_on_count = 0;
  double abort_on_threshold = 0;

  get_parameter(&thd, tianmu_var_name::TIANMU_REJECT_FILE_PATH, reject_file);
  if (get_parameter(&thd, tianmu_var_name::TIANMU_REJECT_FILE_PATH, reject_file) == 2)
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER, "Wrong value of TIANMU_LOAD_REJECT_FILE parameter.");

  if (get_parameter(&thd, tianmu_var_name::TIANMU_ABORT_ON_COUNT, abort_on_count) == 2)
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER, "Wrong value of TIANMU_ABORT_ON_COUNT parameter.");

  if (get_parameter(&thd, tianmu_var_name::TIANMU_ABORT_ON_THRESHOLD, abort_on_threshold) == 2)
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER,
                               "Wrong value of TIANMU_ABORT_ON_THRESHOLD parameter.");

  if (abort_on_count != 0 && abort_on_threshold != 0)
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER,
                               "TIANMU_ABORT_ON_COUNT and TIANMU_ABORT_ON_THRESHOLD "
                               "parameters are mutualy exclusive.");

  if (!(abort_on_threshold >= 0.0 && abort_on_threshold < 1.0))
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER,
                               "TIANMU_ABORT_ON_THRESHOLD parameter value must be in range (0,1).");

  if ((abort_on_count != 0 || abort_on_threshold != 0) && reject_file.empty())
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER,
                               "TIANMU_ABORT_ON_COUNT or TIANMU_ABORT_ON_THRESHOLD can by only specified with "
                               "TIANMU_REJECT_FILE_PATH parameter.");

  if (!reject_file.empty() && fs::exists(reject_file))
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER,
                               "Can not create the reject file, the file already exists.");

  io_params->SetRejectFile(reject_file, abort_on_count, abort_on_threshold);
  return common::ErrorCode::SUCCESS;
}

common::TianmuError Engine::GetIOParams(std::unique_ptr<system::IOParameters> &io_params, THD &thd, sql_exchange &ex,
                                        TABLE *table, void *arg, bool for_exporter) {
  const CHARSET_INFO *cs = ex.cs;
  // stonedb8 start
  auto cmd = down_cast<Sql_cmd_load_table *>(thd.lex->m_sql_cmd);
  bool local_load = for_exporter ? false : cmd->m_is_local_file;
  uint value_list_elements = cmd->m_opt_set_exprs.size();
  // stonedb8 end
  int io_mode = -1;
  char name[FN_REFLEN] = {0};
  char *tdb = nullptr;
  if (table) {
    tdb = table->s->db.str ? const_cast<char *>(table->s->db.str) : const_cast<char *>(thd.db().str);
  } else
    tdb = const_cast<char *>(thd.db().str);

  io_params = CreateIOParameters(&thd, table, arg);
  short sign, minutes;
  ComputeTimeZoneDiffInMinutes(&thd, sign, minutes);
  io_params->SetTimeZone(sign, minutes);

  if (ex.filetype == FILETYPE_MEM) {
    io_params->load_delayed_insert_ = true;
  }
#ifdef DONT_ALLOW_FULL_LOAD_DATA_PATTIANMU
  ex->file_name += dirname_length(ex->file_name);
#endif

  longlong param = 0;
  std::string s_res;
  if (common::DataFormat::GetNoFormats() > 1) {
    if (!get_parameter(&thd, tianmu_var_name::TIANMU_DATAFORMAT, param, s_res)) {
      common::DataFormatPtr df = common::DataFormat::GetDataFormat(s_res);
      if (!df)
        return common::TianmuError(common::ErrorCode::WRONG_PARAMETER, "Unknown value of TIANMU_DATAFORMAT parameter.");
      else
        io_mode = df->GetId();
    } else
      io_mode = common::DataFormat::GetDataFormat(0)->GetId();
  } else
    io_mode = common::DataFormat::GetDataFormat(0)->GetId();

  if (!get_parameter(&thd, tianmu_var_name::TIANMU_NULL, param, s_res))
    io_params->SetNullsStr(s_res);

  if (io_params->LoadDelayed()) {
    std::strcpy(name, ex.file_name);
  } else {
    if (!dirname_length(ex.file_name)) {
      strxnmov(name, FN_REFLEN - 1, mysql_real_data_home, tdb, NullS);
      (void)fn_format(name, ex.file_name, name, "", MY_RELATIVE_PATH | MY_UNPACK_FILENAME);
    } else {
      if (!local_load)
        fn_format(name, ex.file_name, mysql_real_data_home, "", MY_RELATIVE_PATH | MY_UNPACK_FILENAME);
      else
        std::strcpy(name, ex.file_name);
    }
  }
  io_params->SetOutput(io_mode, name);

  if (ex.field.escaped->length() > 1)
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER, "Multicharacter escape std::string not supported.");

  if (ex.field.enclosed->length() > 1 &&
      (ex.field.enclosed->length() != 4 || strcasecmp(ex.field.enclosed->ptr(), "nullptr") != 0))
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER, "Multicharacter enclose std::string not supported.");

  if (!for_exporter) {
    common::TianmuError tianmu_err = GetRejectFileIOParameters(thd, io_params);
    if (tianmu_err.GetErrorCode() != common::ErrorCode::SUCCESS)
      return tianmu_err;
  }

  if (tianmu_sysvar_usemysqlimportexportdefaults) {
    io_params->SetEscapeCharacter(*ex.field.escaped->ptr());
    io_params->SetDelimiter(ex.field.field_term->ptr());
    io_params->SetLineTerminator(ex.line.line_term->ptr());
    if (ex.field.enclosed->length() == 4 && strcasecmp(ex.field.enclosed->ptr(), "nullptr") == 0)
      io_params->SetParameter(system::Parameter::STRING_QUALIFIER, '\0');
    else
      io_params->SetParameter(system::Parameter::STRING_QUALIFIER, *ex.field.enclosed->ptr());

  } else {
    if (ex.field.escaped->alloced_length() != 0)
      io_params->SetEscapeCharacter(*ex.field.escaped->ptr());

    if (ex.field.field_term->alloced_length() != 0)
      io_params->SetDelimiter(ex.field.field_term->ptr());

    if (ex.line.line_term->alloced_length() != 0)
      io_params->SetLineTerminator(ex.line.line_term->ptr());

    if (ex.field.enclosed->length()) {
      if (ex.field.enclosed->length() == 4 && strcasecmp(ex.field.enclosed->ptr(), "nullptr") == 0)
        io_params->SetParameter(system::Parameter::STRING_QUALIFIER, '\0');
      else
        io_params->SetParameter(system::Parameter::STRING_QUALIFIER, *ex.field.enclosed->ptr());
    }
  }

  if (io_params->EscapeCharacter() != 0 &&
      io_params->Delimiter().find(io_params->EscapeCharacter()) != std::string::npos)
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER,
                               "Field terminator containing the escape character not supported.");

  if (io_params->EscapeCharacter() != 0 && io_params->StringQualifier() != 0 &&
      io_params->EscapeCharacter() == io_params->StringQualifier())
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER,
                               "The same enclose and escape characters not supported.");

  bool unsupported_syntax = false;
  if (cs != 0)
    io_params->SetParameter(system::Parameter::CHARSET_INFO_NUMBER, (int)(cs->number));
  else if (!for_exporter)
    io_params->SetParameter(system::Parameter::CHARSET_INFO_NUMBER,
                            (int)(thd.variables.collation_database->number));  // default charset

  if (ex.skip_lines != 0) {
    unsupported_syntax = true;
    io_params->SetParameter(system::Parameter::SKIP_LINES, (int64_t)ex.skip_lines);
  }

  if (ex.line.line_start != 0 && ex.line.line_start->length() != 0) {
    unsupported_syntax = true;
    io_params->SetParameter(system::Parameter::LINE_STARTER, std::string(ex.line.line_start->ptr()));
  }

  if (local_load && ((thd.lex)->sql_command == SQLCOM_LOAD)) {
    io_params->SetParameter(system::Parameter::LOCAL_LOAD, (int)local_load);
  }

  if (value_list_elements != 0) {
    unsupported_syntax = true;
    io_params->SetParameter(system::Parameter::VALUE_LIST_ELEMENTS, (int64_t)value_list_elements);
  }

  if (ex.field.opt_enclosed) {
    // unsupported_syntax = true;
    io_params->SetParameter(system::Parameter::OPTIONALLY_ENCLOSED, 1);
  }

  if (unsupported_syntax)
    push_warning(&thd, Sql_condition::SL_NOTE, ER_UNKNOWN_ERROR,
                 "Query contains syntax that is unsupported and will be ignored.");
  return common::ErrorCode::SUCCESS;
}

void Engine::UnRegisterTable(const std::string &table_path) {
  std::scoped_lock guard(table_share_mutex);
  table_share_map.erase(table_path);
}

std::shared_ptr<TableShare> Engine::getTableShare(const std::string &name) {
  std::scoped_lock guard(table_share_mutex);
  auto it = table_share_map.find(name);
  if (it == table_share_map.end()) {
    throw common::Exception("Failed to find table share for " + name);
  }
  return it->second;
}

std::shared_ptr<TableShare> Engine::GetTableShare(const TABLE_SHARE *table_share) {
  const std::string name(table_share->normalized_path.str);
  std::scoped_lock guard(table_share_mutex);

  try {
    auto it = table_share_map.find(name);
    if (it == table_share_map.end()) {
      auto share = std::make_shared<TableShare>(name + common::TIANMU_EXT, table_share);
      table_share_map[name] = share;
      return share;
    }
    return it->second;
  } catch (common::Exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to create table share: %s", e.what());
  } catch (std::exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to create table share: %s", e.what());
  } catch (...) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to create table share");
  }
  return nullptr;
}

void Engine::AddTableIndex(const std::string &table_path, TABLE *table, [[maybe_unused]] THD *thd) {
  std::unique_lock<std::shared_mutex> guard(tables_keys_mutex);
  if (!index::RCTableIndex::FindIndexTable(table_path)) {
    index::RCTableIndex::CreateIndexTable(table_path, table);
  }
  auto iter = m_table_keys.find(table_path);
  if (iter == m_table_keys.end()) {
    std::shared_ptr<index::RCTableIndex> tab = std::make_shared<index::RCTableIndex>(table_path, table);
    m_table_keys[table_path] = tab;
  }
}

bool Engine::DeleteTableIndex(const std::string &table_path, [[maybe_unused]] THD *thd) {
  if (table_path.empty() || thd == nullptr) {
    return true;
  }

  std::unique_lock<std::shared_mutex> index_guard(tables_keys_mutex);
  if (index::RCTableIndex::FindIndexTable(table_path)) {
    index::RCTableIndex::DropIndexTable(table_path);
  }
  if (m_table_keys.find(table_path) != m_table_keys.end()) {
    m_table_keys.erase(table_path);
  }

  return false;
}

std::shared_ptr<index::RCTableIndex> Engine::GetTableIndex(const std::string &table_path) {
  std::shared_lock<std::shared_mutex> guard(tables_keys_mutex);
  auto iter = m_table_keys.find(table_path);
  if (iter != m_table_keys.end()) {
    return iter->second;
  }
  return nullptr;
}

void Engine::RenameRdbTable(const std::string &from, const std::string &to) {
  std::unique_lock<std::shared_mutex> guard(tables_keys_mutex);
  auto tab = m_table_keys[from];
  if (tab) {
    tab->RenameIndexTable(from, to);
    tab->RefreshIndexTable(to);
    m_table_keys.erase(from);
    m_table_keys[to] = tab;
  }
}

void Engine::AddMemTable(TABLE *form, std::shared_ptr<TableShare> share) {
  std::scoped_lock guard(mem_table_mutex);
  try {
    auto table_path = share->Path();
    if (mem_table_map.find(table_path) == mem_table_map.end()) {
      mem_table_map[table_path] = RCMemTable::CreateMemTable(share, has_mem_name(form->s->comment));
      return;
    }
    return;
  } catch (common::Exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to create memory table: %s / %s", e.what(), e.trace().c_str());
  } catch (std::exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to create memory table: %s", e.what());
  } catch (...) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to create memory table");
  }
  return;
}

void Engine::UnregisterMemTable(const std::string &from, const std::string &to) {
  std::scoped_lock guard(mem_table_mutex);
  auto iter = mem_table_map.find(from);
  if (iter != mem_table_map.end()) {
    auto tb_mem = iter->second;
    tb_mem->Rename(to);
    mem_table_map.erase(iter);
  }
}

void Engine::ResetTaskExecutor(int percent) {
  if (percent > 0) {
    if (task_executor) {
      task_executor->Exit();
      task_executor.reset();
    }

    unsigned cpus = std::max(std::thread::hardware_concurrency(), 1u);
    unsigned threads = (cpus * percent + 99) / 100;
    task_executor = std::make_unique<TaskExecutor>();
    std::future<void> ready_future = task_executor->Init(threads);
    ready_future.get();
  } else {
    if (task_executor) {
      task_executor->Exit();
      task_executor.reset();
    }
  }
}

// notice: rowstore stat compute insert/read roughly
// as it costs much to get storage size in rocks
std::string Engine::RowStoreStat() {
  uint64_t write_cnt = 0;
  uint64_t write_bytes = 0;
  uint64_t read_cnt = 0;
  uint64_t read_bytes = 0;
  uint64_t delta_cnt = 0;
  uint64_t delta_bytes = 0;
  std::scoped_lock guard(mem_table_mutex);
  for (auto &iter : mem_table_map) {
    auto mem_table = iter.second;
    write_cnt += mem_table->stat.write_cnt.load();
    write_bytes += mem_table->stat.write_bytes.load();
    read_cnt += mem_table->stat.read_cnt;
    read_bytes += mem_table->stat.read_bytes;
    if (mem_table->stat.write_cnt > mem_table->stat.read_cnt)
      delta_cnt += (mem_table->stat.write_cnt - mem_table->stat.read_cnt);
    if (mem_table->stat.write_bytes > mem_table->stat.read_bytes)
      delta_bytes += (mem_table->stat.write_bytes - mem_table->stat.read_bytes);
  }

  return "w:" + std::to_string(write_cnt) + "/" + std::to_string(write_bytes) + ", r:" + std::to_string(read_cnt) +
         "/" + std::to_string(read_bytes) + " delta: " + std::to_string(delta_cnt) + "/" + std::to_string(delta_bytes);
}

/*
Handles a single query If an error appears during query preparation/optimization query structures are cleaned up
and the function returns information about the error through res'. If the query can not be compiled by Tianmu engine
QueryRouteTo::TO_MYSQL is returned and MySQL engine continues query execution.
*/
QueryRouteTo Engine::Handle_Query(THD *thd, Query_expression *qe, Query_result *&result, ulong setup_tables_done_option,
                                  int &res, int &optimize_after_tianmu, int &tianmu_free_join, bool with_insert) {
  utils::KillTimer timer(thd, tianmu_sysvar_max_execution_time);

  LEX *lex{thd->lex};
  if (qe->is_simple())
    lex = qe->first_query_block()->parent_lex;

  bool in_case_of_failure_can_go_to_mysql{true};

  optimize_after_tianmu = false;
  tianmu_free_join = 0;

  Query_expression *unit = nullptr;
  Query_block *select_lex = nullptr;
  Query_result_export *se = nullptr;

  if (tianmu_sysvar_pushdown)
    thd->variables.optimizer_switch |= OPTIMIZER_SWITCH_ENGINE_CONDITION_PUSHDOWN;

  if (RouteTo(thd, lex->query_tables, lex->query_block, in_case_of_failure_can_go_to_mysql, with_insert) ==
      QueryRouteTo::TO_MYSQL) {
    return QueryRouteTo::TO_MYSQL;
  }

  if (with_insert)
    result->create_table_for_query_block(thd);  // used for CTAS

  if (lock_tables(thd, thd->lex->query_tables, thd->lex->table_count, 0)) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to lock tables for query '%s'", thd->query().str);
    return QueryRouteTo::TO_TIANMU;
  }

  tianmu_stat.select++;

  // at this point all tables are in RCBase engine, so we can proceed with the
  // query and we know that if the result goes to the file, the TIANMU_DATAFORMAT is
  // one of TIANMU formats
  QueryRouteTo route = QueryRouteTo::TO_TIANMU;

  Query_block *save_current_select = lex->current_query_block();
  List<Query_expression> derived_optimized;  // collection to remember derived tables that are optimized

  if (lex->unit->derived_table) {  // Derived tables are processed completely in the function open_and_lock_tables(...).
    res = false;                   // To avoid execution of derived tables in open_and_lock_tables(...) the function
                                   // mysql_derived_filling(..)
    int free_join = false;  // optimizing and executing derived tables is passed over, then optimization of derived
                            // tables must go here.
    lex->thd->derived_tables_processing = true;

    for (Query_block *sl = lex->all_query_blocks_list; sl; sl = sl->next_select_in_list())  // for all selects
      for (TABLE_LIST *tl = sl->get_table_list(); tl; tl = tl->next_local)                  // for all tables
        if (tl->table &&
            tl->is_view_or_derived()) {  // data source (view or FROM subselect), then do derived table optim.
          Query_block *first_select =
              tl->derived_query_expression()->first_query_block();  // first query block of derived query.

          if (first_select->next_query_block() &&
              first_select->next_query_block()->linkage == UNION_TYPE) {  //?? only if union
            if (lex->is_explain() ||
                tl->derived_query_expression()->item) {  //??called for explain OR there is subselect(?)
              route = QueryRouteTo::TO_MYSQL;
              goto ret_derived;
            }

            // start to process dervied table, not already executed (not materialized?) OR not cacheable (meaning not
            // yet in cache, i.e. not
            // materialized it seems to boil down to NOT MATERIALIZED(?)
            if (!tl->derived_query_expression()->is_executed() || tl->derived_query_expression()->uncacheable) {
              res = tl->derived_query_expression()->optimize_for_tianmu(thd);
              if (!res)
                derived_optimized.push_back(tl->derived_query_expression());
            }
          } else {                                                         //??not union
            tl->derived_query_expression()->set_limit(thd, first_select);  // stonedb8
            if (tl->derived_query_expression()->select_limit_cnt == HA_POS_ERROR)
              first_select->remove_base_options(OPTION_FOUND_ROWS);
            lex->set_current_query_block(first_select);

            int optimize_derived_after_tianmu = false;
            res = optimize_select(
                thd, ulong(first_select->active_options() | thd->variables.option_bits | SELECT_NO_UNLOCK),
                (Query_result *)tl->derived_result, first_select, optimize_derived_after_tianmu, free_join);

            if (optimize_derived_after_tianmu)
              derived_optimized.push_back(tl->derived_query_expression());
          }

          lex->set_current_query_block(save_current_select);

          if (!res && free_join)  // no error &
            route = QueryRouteTo::TO_MYSQL;
          if (res || route == QueryRouteTo::TO_MYSQL)
            goto ret_derived;
        }

    lex->thd->derived_tables_processing = false;
  }  // if (lex->unit->derived_table)

  // prepare, optimize and execute the main query
  se = dynamic_cast<Query_result_export *>(result);
  if (se != nullptr)
    result = new exporter::select_tianmu_export(se);

  select_lex = lex->query_block;
  unit = lex->unit;
  if (select_lex->next_query_block()) {  // it is union
    if (!unit->is_prepared())
      res = unit->prepare(thd, result, nullptr, (ulong)(SELECT_NO_UNLOCK | setup_tables_done_option), 0);
    // similar to mysql_union(...) from sql_union.cpp
    if (!res) {
      if (lex->is_explain() || unit->item)  // explain or sth was already computed - go to mysql
        route = QueryRouteTo::TO_MYSQL;
      else {
        int old_executed = unit->is_executed();
        res = unit->optimize_for_tianmu(thd);  //====exec()
        optimize_after_tianmu = true;

        if (!res) {
          try {
            route = ha_rcengine_->Execute(thd, thd->lex, result, unit);
            if (route == QueryRouteTo::TO_MYSQL) {
              if (in_case_of_failure_can_go_to_mysql)
                old_executed ? unit->set_executed() : unit->reset_executed();
              else {
                const char *err_msg =
                    "Error: Query syntax not implemented in Tianmu, can "
                    "export only to MySQL format (set TIANMU_DATAFORMAT to 'MYSQL').";
                TIANMU_LOG(LogCtl_Level::ERROR, "%s", err_msg);
                my_message(ER_SYNTAX_ERROR, err_msg, MYF(0));
                throw ReturnMeToMySQLWithError();
              }
            }
          } catch (ReturnMeToMySQLWithError &) {
            route = QueryRouteTo::TO_TIANMU;
            res = true;
          }
        }

      }  // else
    }    // if (select_lex->next_query_block())

    if (res || route == QueryRouteTo::TO_TIANMU) {
      unit->cleanup(thd, 0);
      optimize_after_tianmu = false;
    }

  } else {                                            // not a union
    unit->set_limit(thd, unit->global_parameters());  // the fragment of original  // stonedb8
                                                      // handle_select(...)
    //(until the first part of optimization)
    // used for non-union select

    //'options' of mysql_select will be set in JOIN, as far as JOIN for
    // every PS/SP execution new, we will not need reset this flag if
    // setup_tables_done_option changed for next rexecution

    int err;
    err = optimize_select(thd,
                          ulong(select_lex->active_options() | thd->variables.option_bits | setup_tables_done_option),
                          result, select_lex, optimize_after_tianmu, tianmu_free_join);

    // start tianm processing
    if (!err) {
      try {
        route = Execute(thd, lex, result);
        if (route == QueryRouteTo::TO_MYSQL && !in_case_of_failure_can_go_to_mysql) {
          TIANMU_LOG(LogCtl_Level::ERROR,
                     "Error: Query syntax not implemented in Tianmu, can export "
                     "only to MySQL format (set TIANMU_DATAFORMAT to 'MYSQL').");
          my_message(ER_SYNTAX_ERROR,
                     "Query syntax not implemented in Tianmu, can export only "
                     "to MySQL "
                     "format (set TIANMU_DATAFORMAT to 'MYSQL').",
                     MYF(0));
          throw ReturnMeToMySQLWithError();
        }
      } catch (ReturnMeToMySQLWithError &) {
        route = QueryRouteTo::TO_TIANMU;
        err = true;
      }
    }

    if (tianmu_free_join) {  // there was a join created in an upper function
                             // so an upper function will do the cleanup
      if (err || route == QueryRouteTo::TO_TIANMU) {
        thd->set_proc_info("end");
        select_lex->cleanup(thd, 0);

        optimize_after_tianmu = false;
        tianmu_free_join = 0;
      }
      res = (err || thd->is_error());
    } else
      res = select_lex->join->error;

  }  // end of not a union

  if (select_lex->join && Query::IsLOJ(select_lex->join_list))
    optimize_after_tianmu = 2;  // optimize partially (part=4), since part of LOJ
                                // optimization was already done
  res |= (int)thd->is_error();  // the ending of original handle_select(...) */
  if (unlikely(res)) {
    // If we had a another error reported earlier then this will be ignored //

    // stonedb8 start
    // result->send_error(ER_UNKNOWN_ERROR, ER(ER_UNKNOWN_ERROR));
    my_message(ER_UNKNOWN_ERROR, ER(ER_UNKNOWN_ERROR), MYF(0));
    result->abort_result_set(thd);
    // stonedb8 end
  }

  if (se != nullptr) {
    // free the tianmu export object,
    // restore the original mysql export object
    // and prepare if it is expected to be prepared
    if (!select_lex->next_query_block() && select_lex->join != nullptr && select_lex->query_result() == result) {
      select_lex->set_query_result(se);
      if (((exporter::select_tianmu_export *)result)->IsPrepared())
        se->prepare(thd, *(select_lex->join->fields), unit);  // stonedb8
    }

    delete result;
    result = se;
  }
ret_derived:
  // if the query is redirected to MySQL engine optimization of derived tables must be completed and derived tables must
  // be filled
  lex->thd->derived_tables_processing = false;

  if (route == QueryRouteTo::TO_MYSQL) {
    for (Query_block *sl = lex->all_query_blocks_list; sl; sl = sl->next_select_in_list()) {
      for (TABLE_LIST *tl = sl->get_table_list(); tl; tl = tl->next_local) {
        if (tl->table && tl->is_derived()) {  // is dervied table and opened, set to proccessed.
          lex->thd->derived_tables_processing = true;
          tl->derived_query_expression()->optimize_after_tianmu(thd);
        }
      }  // for TABLE_LIST
    }    // for Query_block

    lex->set_current_query_block(save_current_select);
  }

  return route;
}

QueryRouteTo Engine::Execute(THD *thd, LEX *lex, Query_result *result_output, Query_expression *unit_for_union) {
  DEBUG_ASSERT(thd->lex == lex);
  Query_block *selects_list = lex->query_block;
  Query_block *last_distinct = nullptr;
  if (unit_for_union != nullptr)
    last_distinct = unit_for_union->union_distinct;

  int is_dumpfile = 0;
  const char *export_file_name = GetFilename(selects_list, is_dumpfile);
  if (is_dumpfile) {
    push_warning(thd, Sql_condition::SL_NOTE, ER_UNKNOWN_ERROR,
                 "Dumpfile not implemented in Tianmu, executed by MySQL engine.");
    return QueryRouteTo::TO_MYSQL;
  }

  Query query(current_txn_);
  CompiledQuery cqu;

  if (result_output->start_execution(thd))
    return QueryRouteTo::TO_MYSQL;

  current_txn_->ResetDisplay();  // switch display on
  query.SetRoughQuery(selects_list->active_options() & SELECT_ROUGHLY);

  try {
    if (query.Compile(&cqu, selects_list, last_distinct) == QueryRouteTo::TO_MYSQL) {
      push_warning(thd, Sql_condition::SL_NOTE, ER_UNKNOWN_ERROR,
                   "Query syntax not implemented in Tianmu, executed by MySQL engine.");
      return QueryRouteTo::TO_MYSQL;
    }
  } catch (common::Exception const &x) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Query Compile Error: %s", x.what());
    my_message(ER_UNKNOWN_ERROR, (std::string("Tianmu compile specific error: ") + x.what()).c_str(), MYF(0));
    throw ReturnMeToMySQLWithError();
  }

  std::unique_ptr<ResultSender> sender;

  FunctionExecutor lock_and_unlock_pack_info(std::bind(&Query::LockPackInfoForUse, &query),
                                             std::bind(&Query::UnlockPackInfoFromUse, &query));

  try {
    std::shared_ptr<RCTable> rct;
    if (lex->sql_command == SQLCOM_INSERT_SELECT &&
        Engine::IsTianmuTable(((Query_tables_list *)lex)->query_tables->table)) {
      std::string table_path = Engine::GetTablePath(((Query_tables_list *)lex)->query_tables->table);
      rct = current_txn_->GetTableByPathIfExists(table_path);
    }

    if ((unit_for_union != nullptr) && (lex->sql_command != SQLCOM_CREATE_TABLE)) {      // for exclude CTAS
      int res = result_output->prepare(thd, unit_for_union->item_list, unit_for_union);  // stonedb8 add thd
      if (res) {
        TIANMU_LOG(LogCtl_Level::ERROR, "Error: Unsupported UNION");
        my_message(ER_UNKNOWN_ERROR, "Tianmu: unsupported UNION", MYF(0));
        throw ReturnMeToMySQLWithError();
      }
      // stonedb8 start
      if (export_file_name)
        sender.reset(new ResultExportSender(thd, result_output, unit_for_union->item_list));
      else
        sender.reset(new ResultSender(thd, result_output, unit_for_union->item_list));
    } else {
      if (export_file_name)
        // in 5.7, selects_list->item_list, in 8.0, we use selects_list->get_fields_list()?
        sender.reset(new ResultExportSender(thd, result_output, *selects_list->get_fields_list()));
      else
        sender.reset(new ResultSender(thd, result_output, *selects_list->get_fields_list()));
      // stonedb8 end
    }

    TempTable *result = query.Preexecute(cqu, sender.get());
    ASSERT(result != nullptr, "Query execution returned no result object");
    if (query.IsRoughQuery())
      result->RoughMaterialize(false, sender.get());
    else
      result->Materialize(false, sender.get());

    sender->Finalize(result);

    if (rct) {
      // in this case if this is an insert to RCTable from select based on the
      // same table RCTable object for this table can't be deleted in TempTable
      // destructor It will be deleted in RefreshTables method that will be
      // called on commit
      result->RemoveFromManagedList(rct.get());
      query.RemoveFromManagedList(rct);
      rct.reset();
    }
    sender.reset();
  } catch (...) {
    bool with_error = false;
    if (sender) {
      if (sender->SentRows() > 0) {
        sender->CleanUp();
        with_error = true;
      } else
        sender->CleanUp();
    }
    return (handle_exceptions(thd, current_txn_, with_error));
  }

  return QueryRouteTo::TO_TIANMU;
}

}  // namespace core
}  // namespace Tianmu
