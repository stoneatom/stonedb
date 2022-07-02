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

#include "common/mysql_gate.h"

#include "common/data_format.h"
#include "common/exception.h"
#include "core/rc_mem_table.h"
#include "core/table_share.h"
#include "core/task_executor.h"
#include "core/temp_table.h"
#include "core/tools.h"
#include "core/transaction.h"
#include "mm/initializer.h"
#include "system/file_out.h"
#include "system/res_manager.h"
#include "system/stdout.h"
#include "util/bitset.h"
#include "util/fs.h"
#include "util/thread_pool.h"

namespace stonedb {
namespace dbhandler {
extern void resolve_async_join_settings(const std::string &settings);
}
namespace core {
#ifdef PROFILE_LOCK_WAITING
LockProfiler lock_profiler;
#endif
constexpr auto BUFFER_FILE = "STONEDB_INSERT_BUFFER";

namespace {
// It should be immutable after RCEngine initialization
std::vector<fs::path> stonedb_data_dirs;
std::mutex v_mtx;
const char *STONEDB_DATA_DIR = "stonedb_data";
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

  if (sigaction(SIGRTMIN, &sa, NULL) == -1) {
    STONEDB_LOG(LogCtl_Level::INFO, "Failed to set up signal handler. error =%d[%s]", errno, std::strerror(errno));
    return 1;
  }
  return 0;
}

fs::path Engine::GetNextDataDir() {
  std::scoped_lock guard(v_mtx);

  if (stonedb_data_dirs.empty()) {
    // fall back to use MySQL data directory
    auto p = rceng->stonedb_data_dir / STONEDB_DATA_DIR;
    if (!fs::is_directory(p)) fs::create_directory(p);
    return p;
  }

  auto sz = stonedb_data_dirs.size();
  if (sz == 1) {
    // we have no choice
    return stonedb_data_dirs[0];
  }

  if (stonedb_sysvar_dist_policy == 1) {
    return stonedb_data_dirs[std::rand() % sz];
  } else if (stonedb_sysvar_dist_policy == 2) {
    auto v = stonedb_data_dirs;
    v.erase(std::remove_if(v.begin(), v.end(),
                           [](const fs::path &s) {
                             auto si = fs::space(s);
                             auto usage = 100 - ((si.available * 100) / si.capacity);
                             if (usage > static_cast<size_t>(stonedb_sysvar_disk_usage_threshold)) {
                               STONEDB_LOG(LogCtl_Level::WARN, "disk %s usage %d%%", s.native().c_str(), usage);
                               return true;
                             }
                             return false;
                           }),
            v.end());
    if (v.size() == 0) {
      STONEDB_LOG(LogCtl_Level::ERROR, "all data directories usage exceed %d%%", stonedb_sysvar_disk_usage_threshold);
      throw common::Exception("Disk usage exceeds defined threshold");
    }
    return v[std::rand() % v.size()];
  }
  // round-robin
  static int idx = 0;
  return stonedb_data_dirs[idx++ % sz];
}

static int has_pack(const LEX_STRING &comment) {
  int ret = common::DFT_PSS;
  std::string str(comment.str, comment.length);
  boost::to_upper(str);
  std::string val;
  auto pos = str.find("PACK");
  if (pos == std::string::npos) return ret;
  size_t val_pos = str.find(':', pos);
  if (val_pos == std::string::npos) return ret;
  size_t term_pos = str.find(';', val_pos);
  if (term_pos == std::string::npos) {
    val = str.substr(val_pos + 1);
  } else {
    val = str.substr(val_pos + 1, term_pos - val_pos - 1);
  }
  boost::trim(val);
  ret = atoi(val.c_str());
  if (ret > common::DFT_PSS || ret <= 0) ret = common::DFT_PSS;
  return ret;
}

static std::string has_mem_name(const LEX_STRING &comment) {
  std::string name = "";
  std::string str(comment.str, comment.length);
  boost::to_upper(str);
  auto pos = str.find("ROWSTORE");
  if (pos == std::string::npos) return name;
  size_t val_pos = str.find(':', pos);
  if (val_pos == std::string::npos) return name;
  size_t term_pos = str.find(';', val_pos);
  if (term_pos == std::string::npos) {
    name = str.substr(val_pos + 1);
  } else {
    name = str.substr(val_pos + 1, term_pos - val_pos - 1);
  }
  boost::trim(name);

  return name;
}

bool parameter_equals(THD *thd, enum sdb_var_name vn, longlong value) {
  longlong param = 0;
  std::string s_res;

  if (!get_parameter(thd, vn, param, s_res))
    if (param == value) return true;

  return false;
}

static auto GetNames(const std::string &table_path) {
  auto p = fs::path(table_path);
  return std::make_tuple(p.parent_path().filename().native(), p.filename().native());
}

bool Engine::TrxCmp::operator()(Transaction *l, Transaction *r) const { return l->GetID() < r->GetID(); }

Engine::Engine()
    : delay_insert_thread_pool(
          "bg_loader",
          stonedb_sysvar_bg_load_threads
              ? stonedb_sysvar_bg_load_threads
              : ((std::thread::hardware_concurrency() / 2) ? (std::thread::hardware_concurrency() / 2) : 1)),
      load_thread_pool("loader",
                       stonedb_sysvar_load_threads ? stonedb_sysvar_load_threads : std::thread::hardware_concurrency()),
      query_thread_pool(
          "query", stonedb_sysvar_query_threads ? stonedb_sysvar_query_threads : std::thread::hardware_concurrency()),
      insert_buffer(BUFFER_FILE, stonedb_sysvar_insert_buffer_size) {
  stonedb_data_dir = mysql_real_data_home;
}

int Engine::Init(uint engine_slot) {
  m_slot = engine_slot;
  ConfigureRCControl();
  if (stonedb_sysvar_controlquerylog > 0) {
    rcquerylog.setOn();
  } else {
    rcquerylog.setOff();
  }
  std::srand(unsigned(time(NULL)));

  if (stonedb_sysvar_servermainheapsize == 0) {
    long pages = sysconf(_SC_PHYS_PAGES);
    long pagesize = sysconf(_SC_PAGESIZE);
    if (pagesize > 0 && pages > 0) {
      stonedb_sysvar_servermainheapsize = pages * pagesize / 1_MB / 2;
    } else {
      stonedb_sysvar_servermainheapsize = 10000;
    }
  }
  size_t main_size = size_t(stonedb_sysvar_servermainheapsize) << 20;

  std::string hugefiledir = stonedb_sysvar_hugefiledir;
  int hugefilesize = 0;  // unused
  if (hugefiledir.empty())
    mm::MemoryManagerInitializer::Instance(0, main_size);
  else
    mm::MemoryManagerInitializer::Instance(0, main_size, hugefiledir, hugefilesize);

  the_filter_block_owner = new TheFilterBlockOwner();

  std::string cachefolder_path = stonedb_sysvar_cachefolder;
  boost::trim(cachefolder_path);
  boost::trim_if(cachefolder_path, boost::is_any_of("\""));
  if (SetUpCacheFolder(cachefolder_path) != 0) return 1;
  system::ClearDirectory(cachefolder_path);

  m_resourceManager = new system::ResourceManager();

#ifdef FUNCTIONS_EXECUTION_TIMES
  fet = new FunctionsExecutionTimes();
#endif
  STONEDB_LOG(LogCtl_Level::INFO, "StoneDB engine started. ");
  STONEDB_LOG(LogCtl_Level::INFO, "StoneDB data directories:");
  STONEDB_LOG(LogCtl_Level::INFO, "  {");
  if (stonedb_data_dirs.empty())
    STONEDB_LOG(LogCtl_Level::INFO, "    default");
  else
    for (auto &dir : stonedb_data_dirs) {
      auto si = fs::space(dir);
      STONEDB_LOG(LogCtl_Level::INFO, "    %s  capacity/available: %ld/%ld", dir.native().c_str(), si.capacity,
                  si.available);
    }
  STONEDB_LOG(LogCtl_Level::INFO, "  }");

  STONEDB_LOG(LogCtl_Level::INFO, "StoneDB thread pool for background load, size = %ld",
              delay_insert_thread_pool.size());
  STONEDB_LOG(LogCtl_Level::INFO, "StoneDB thread pool for load, size = %ld", load_thread_pool.size());
  STONEDB_LOG(LogCtl_Level::INFO, "StoneDB thread pool for query, size = %ld", query_thread_pool.size());

  m_monitor_thread = std::thread([this] {
    struct job {
      long interval;
      std::function<void()> func;
    } jobs[] = {
        {60, [this]() { this->LogStat(); }},
        {60 * 5,
         []() {
           STONEDB_LOG(
               LogCtl_Level::INFO,
               "Memory: release [%llu %llu %llu %llu] %llu, total "
               "%llu. (un)freeable %lu/%lu, total alloc/free "
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
        if (!stonedb_sysvar_qps_log) continue;
        for (auto &j : jobs) {
          if (counter % (j.interval / loop_interval) == 0) j.func();
        }
      }
    }
    STONEDB_LOG(LogCtl_Level::INFO, "StoneDB monitor thread exiting...");
  });

  m_load_thread = std::thread([this] { ProcessDelayedInsert(); });
  m_merge_thread = std::thread([this] { ProcessDelayedMerge(); });

  m_purge_thread = std::thread([this] {
    do {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      std::unique_lock<std::mutex> lk(cv_mtx);
      if (cv.wait_for(lk, std::chrono::seconds(3)) == std::cv_status::timeout) HandleDeferredJobs();
    } while (!exiting);
    STONEDB_LOG(LogCtl_Level::INFO, "StoneDB file purge thread exiting...");
  });

  if (setup_sig_handler()) {
    return -1;
  }

  if (stonedb_sysvar_start_async > 0) ResetTaskExecutor(stonedb_sysvar_start_async);
  dbhandler::resolve_async_join_settings(stonedb_sysvar_async_join);

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
        STONEDB_LOG(LogCtl_Level::ERROR, "Failed to remove file %s Error:%s", t.file.string().c_str(), ec.message());
      }
    } else {
      gc_tasks.emplace_back(t);
    }
    gc_tasks.pop_front();
  }
}

void Engine::DeferRemove(const fs::path &file, int32_t cookie) {
  std::scoped_lock lk(gc_tasks_mtx);
  if (fs::exists(file)) gc_tasks.emplace_back(purge_task{file, MaxXID(), cookie});
}

Engine::~Engine() {
  {
    std::scoped_lock lk(cv_mtx);
    std::scoped_lock lk_merge(cv_merge_mtx);
    exiting = true;
    STONEDB_LOG(LogCtl_Level::INFO, "StoneDB engine shutting down.");
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
  delete the_filter_block_owner;

  try {
    mm::MemoryManagerInitializer::EnsureNoLeakedTraceableObject();
  } catch (common::AssertException &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Memory leak! %s", e.what());
  } catch (...) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Unkown exception caught");
  }
  if (rccontrol.isOn())
    mm::MemoryManagerInitializer::deinit(true);
  else
    mm::MemoryManagerInitializer::deinit(false);

  STONEDB_LOG(LogCtl_Level::INFO, "StoneDB engine destroyed.");
}

void Engine::EncodeRecord(const std::string &table_path, int tid, Field **field, size_t col, size_t blobs,
                          std::unique_ptr<char[]> &buf, uint32_t &size) {
  size = blobs > 0 ? 4_MB : 128_KB;
  buf.reset(new char[size]);
  utils::BitSet null_mask(col);

  // layout:
  //     table_id
  //     table_path
  //     null_mask
  //     fileds...
  char *ptr = buf.get();
  *(int32_t *)ptr = tid;  // table id
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
    if (f->flags & BLOB_FLAG)
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
      case MYSQL_TYPE_TINY: {
        int64_t v = f->val_int();
        if (v > SDB_TINYINT_MAX)
          v = SDB_TINYINT_MAX;
        else if (v < SDB_TINYINT_MIN)
          v = SDB_TINYINT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_SHORT: {
        int64_t v = f->val_int();
        if (v > SDB_SMALLINT_MAX)
          v = SDB_SMALLINT_MAX;
        else if (v < SDB_SMALLINT_MIN)
          v = SDB_SMALLINT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_LONG: {
        int64_t v = f->val_int();
        if (v > std::numeric_limits<int>::max())
          v = std::numeric_limits<int>::max();
        else if (v < SDB_INT_MIN)
          v = SDB_INT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_INT24: {
        int64_t v = f->val_int();
        if (v > SDB_MEDIUMINT_MAX)
          v = SDB_MEDIUMINT_MAX;
        else if (v < SDB_MEDIUMINT_MIN)
          v = SDB_MEDIUMINT_MIN;
        *(int64_t *)ptr = v;
        ptr += sizeof(int64_t);
      } break;
      case MYSQL_TYPE_LONGLONG: {
        int64_t v = f->val_int();
        if (v > common::SDB_BIGINT_MAX)
          v = common::SDB_BIGINT_MAX;
        else if (v < common::SDB_BIGINT_MIN)
          v = common::SDB_BIGINT_MIN;
        *(int64_t *)ptr = v;
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
          my_bool myb;
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
  fs::path p = stonedb_data_dir / "stonedb.tid";
  if (!fs::exists(p)) {
    STONEDB_LOG(LogCtl_Level::INFO, "Creating table id file");
    std::ofstream seq_file(p.string());
    if (seq_file) seq_file << 0;
    if (!seq_file) {
      throw common::FileException("Failed to write to table id file");
    }
  }

  uint32_t seq;
  std::fstream seq_file(p.string());
  if (seq_file) seq_file >> seq;
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

std::shared_ptr<TableOption> Engine::GetTableOption(const std::string &table, TABLE *form) {
  auto opt = std::make_shared<TableOption>();

  int power = has_pack(form->s->comment);
  if (power < 5 || power > 16) {
    STONEDB_LOG(LogCtl_Level::ERROR, "create table comment: pack size shift(%d) should be >=5 and <= 16");
    throw common::SyntaxException("Unexpected data pack size.");
  }

  opt->pss = power;

  for (uint i = 0; i < form->s->fields; ++i) {
    Field *f = form->field[i];
    opt->atis.push_back(Engine::GetAttrTypeInfo(*f));
  }

  opt->path = table + common::STONEDB_EXT;
  opt->name = form->s->table_name.str;
  return opt;
}

void Engine::CreateTable(const std::string &table, TABLE *form) { RCTable::CreateNew(GetTableOption(table, form)); }

AttributeTypeInfo Engine::GetAttrTypeInfo(const Field &field) {
  bool auto_inc = field.flags & AUTO_INCREMENT_FLAG;
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
      push_warning(current_thd, Sql_condition::WARN_LEVEL_WARN, WARN_OPTION_IGNORED, s.c_str());
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
      if (field.flags & UNSIGNED_FLAG)
        throw common::UnsupportedDataTypeException("UNSIGNED data types are not supported.");
      [[fallthrough]];
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:
      return AttributeTypeInfo(Engine::GetCorrespondingType(field), notnull, (ushort)field.field_length, 0, auto_inc,
                               DTCollation(), fmt, filter);
    case MYSQL_TYPE_TIME:
      return AttributeTypeInfo(common::CT::TIME, notnull, 0, 0, false, DTCollation(), fmt, filter);
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR: {
      if (field.field_length > FIELD_MAXLENGTH)
        throw common::UnsupportedDataTypeException("Length of STRING or VARCHAR exceeds 65535 bytes.");
      // Trie column only supports String/VARCHAR column and it
      // doesn't work for case-insensitive collations.
      if (str.find("TRIE") != std::string::npos) fmt = common::PackFmt::TRIE;
      if (const Field_str *fstr = dynamic_cast<const Field_string *>(&field)) {
        DTCollation coll(fstr->charset(), fstr->derivation());
        if (fmt == common::PackFmt::TRIE && types::IsCaseInsensitive(coll)) {
          STONEDB_LOG(LogCtl_Level::ERROR, "TRIE can not work with case-insensitive collation: %s!",
                      coll.collation->name);
          throw common::UnsupportedDataTypeException();
        }
        if (fstr->charset() != &my_charset_bin)
          return AttributeTypeInfo(common::CT::STRING, notnull, field.field_length, 0, auto_inc, coll, fmt, filter);
        return AttributeTypeInfo(common::CT::BYTE, notnull, field.field_length, 0, auto_inc, coll, fmt, filter);
      } else if (const Field_str *fvstr = dynamic_cast<const Field_varstring *>(&field)) {
        DTCollation coll(fvstr->charset(), fvstr->derivation());
        if (fmt == common::PackFmt::TRIE && types::IsCaseInsensitive(coll))
          throw common::UnsupportedDataTypeException();
        if (fvstr->charset() != &my_charset_bin)
          return AttributeTypeInfo(common::CT::VARCHAR, notnull, field.field_length, 0, auto_inc, coll, fmt, filter);
        return AttributeTypeInfo(common::CT::VARBYTE, notnull, field.field_length, 0, auto_inc, coll, fmt, filter);
      }
      throw common::UnsupportedDataTypeException();
    }
    case MYSQL_TYPE_NEWDECIMAL: {
      if (field.flags & UNSIGNED_FLAG)
        throw common::UnsupportedDataTypeException("UNSIGNED data types are not supported.");
      const Field_new_decimal *fnd = ((const Field_new_decimal *)&field);
      if (/*fnd->precision > 0 && */ fnd->precision <= 18 /*&& fnd->dec >= 0*/
          && fnd->dec <= fnd->precision)
        return AttributeTypeInfo(common::CT::NUM, notnull, fnd->precision, fnd->dec);
      throw common::UnsupportedDataTypeException("Precision must be less than or equal to 18.");
    }
    case MYSQL_TYPE_BLOB:
      if (const Field_str *fstr = dynamic_cast<const Field_str *>(&field)) {
        if (const Field_blob *fblo = dynamic_cast<const Field_blob *>(fstr)) {
          if (fblo->charset() != &my_charset_bin) {  // TINYTEXT, MEDIUMTEXT, TEXT, LONGTEXT
            common::CT t = common::CT::VARCHAR;
            if (field.field_length > FIELD_MAXLENGTH) {
              t = common::CT::LONGTEXT;
            }
            return AttributeTypeInfo(t, notnull, field.field_length, 0, auto_inc, DTCollation(), fmt, filter);
          }
          switch (field.field_length) {
            case 255:
            case FIELD_MAXLENGTH:
              // TINYBLOB, BLOB
              return AttributeTypeInfo(common::CT::VARBYTE, notnull, field.field_length, 0, auto_inc, DTCollation(),
                                       fmt, filter);
            case 16777215:
            case 4294967295:
              // MEDIUMBLOB, LONGBLOB
              return AttributeTypeInfo(common::CT::BIN, notnull, field.field_length, 0, auto_inc, DTCollation(), fmt,
                                       filter);
            default:
              throw common::UnsupportedDataTypeException();
          }
        }
      }
      [[fallthrough]];
    default:
      throw common::UnsupportedDataTypeException("Unsupported data type.");
  }
  throw;
}

void Engine::CommitTx(THD *thd, bool all) {
  if (all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT)) {
    GetTx(thd)->Commit(thd);
    thd->server_status &= ~SERVER_STATUS_IN_TRANS;
  }
  ClearTx(thd);
}

void Engine::Rollback(THD *thd, bool all, bool force_error_message) {
  force_error_message = force_error_message || (!all && thd_test_options(thd, OPTION_NOT_AUTOCOMMIT));
  STONEDB_LOG(LogCtl_Level::ERROR, "Roll back query '%s'", thd_query_string(thd)->str);
  if (current_tx) {
    GetTx(thd)->Rollback(thd, force_error_message);
    ClearTx(thd);
  }
  thd->server_status &= ~SERVER_STATUS_IN_TRANS;
  thd->transaction_rollback_request = false;
}

void Engine::DeleteTable(const char *table, [[maybe_unused]] THD *thd) {
  {
    std::unique_lock<std::shared_mutex> index_guard(tables_keys_mutex);
    index::RCTableIndex::DropIndexTable(table);
    m_table_keys.erase(table);
  }
  {
    std::unique_lock<std::shared_mutex> mem_guard(mem_table_mutex);
    RCMemTable::DropMemTable(table);
    mem_table_map.erase(table);
  }

  UnRegisterTable(table);
  std::string p = table;
  p += common::STONEDB_EXT;
  auto id = RCTable::GetTableId(p);
  cache.ReleaseTable(id);
  filter_cache.RemoveIf([id](const FilterCoordinate &c) { return c[0] == int(id); });

  {
    std::scoped_lock lk(gc_tasks_mtx);
    gc_tasks.remove_if([id](const purge_task &t) -> bool { return static_cast<uint32_t>(t.cookie) == id; });
  }
  system::DeleteDirectory(p);
  STONEDB_LOG(LogCtl_Level::INFO, "Drop table %s, ID = %u", table, id);
}

void Engine::TruncateTable(const std::string &table_path, [[maybe_unused]] THD *thd) {
  auto indextab = GetTableIndex(table_path);
  if (indextab != nullptr) {
    indextab->TruncateIndexTable();
  }
  auto tab = current_tx->GetTableByPath(table_path);
  tab->Truncate();
  auto id = tab->GetID();
  cache.ReleaseTable(id);
  filter_cache.RemoveIf([id](const FilterCoordinate &c) { return c[0] == int(id); });
  STONEDB_LOG(LogCtl_Level::INFO, "Truncated table %s, ID = %u", table_path.c_str(), id);
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
  std::scoped_lock guard(global_mutex);

  std::shared_ptr<RCTable> tab;
  if (current_tx != nullptr)
    tab = current_tx->GetTableByPath(table_path);
  else
    tab = GetTableRD(table_path);
  // rccontrol << "Table " << table_path << " (" << tab->GetID() << ") accessed
  // by MySQL engine." << endl;
  return tab->GetAttributesInfo();
}

void Engine::UpdateAndStoreColumnComment(TABLE *table, int field_id, Field *source_field, int source_field_id,
                                         CHARSET_INFO *cs) {
  if (source_field->orig_table->s->db_type() == rcbase_hton) {  // do not use table (cont. default values)
    char buf_size[256] = {0};
    char buf_ratio[256] = {0};
    uint buf_size_count = 0;
    uint buf_ratio_count = 0;
    int64_t sum_c = 0, sum_u = 0;

    std::vector<AttrInfo> attr_info =
        GetTableAttributesInfo(source_field->orig_table->s->path.str, source_field->orig_table->s);

    bool is_unique = false;
    if (source_field_id < (int)attr_info.size()) {
      is_unique = attr_info[source_field_id].actually_unique;
      sum_c += attr_info[source_field_id].comp_size;
      sum_u += attr_info[source_field_id].uncomp_size;
    }

    double d_comp = int(sum_c / 104857.6) / 10.0;  // 1 MB = 2^20 bytes
    if (d_comp < 0.1) d_comp = 0.1;
    double ratio = (sum_c > 0 ? sum_u / double(sum_c) : 0);
    if (ratio > 1000) ratio = 999.99;

    buf_size_count = std::snprintf(buf_size, 256, "Size[MB]: %.1f", d_comp);
    if (is_unique)
      buf_ratio_count = std::snprintf(buf_ratio, 256, "; Ratio: %.2f; unique", ratio);
    else
      buf_ratio_count = std::snprintf(buf_ratio, 256, "; Ratio: %.2f", ratio);

    uint len = uint(source_field->comment.length) + buf_size_count + buf_ratio_count + 3;
    //  char* full_comment = new char(len);  !!!! DO NOT USE NEW
    //  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11
    char *full_comment = (char *)my_malloc(len, MYF(0));
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
  ASSERT(thd->ha_data[m_slot].ha_ptr == NULL, "Nested transaction is not supported!");
  ASSERT(current_tx == NULL, "Previous transaction is not finished!");

  current_tx = new Transaction(thd);
  thd->ha_data[m_slot].ha_ptr = current_tx;

  AddTx(current_tx);

  return current_tx;
}

Transaction *Engine::GetTx(THD *thd) {
  if (thd->ha_data[m_slot].ha_ptr == nullptr) return CreateTx(thd);
  return static_cast<Transaction *>(thd->ha_data[m_slot].ha_ptr);
}

void Engine::ClearTx(THD *thd) {
  ASSERT(current_tx == (Transaction *)thd->ha_data[m_slot].ha_ptr, "Bad transaction");

  if (current_tx == nullptr) return;

  RemoveTx(current_tx);
  current_tx = nullptr;
  thd->ha_data[m_slot].ha_ptr = NULL;
}

int Engine::SetUpCacheFolder(const std::string &cachefolder_path) {
  if (!fs::exists(cachefolder_path)) {
    STONEDB_LOG(LogCtl_Level::INFO, "Cachefolder %s does not exist. Trying to create it.", cachefolder_path.c_str());
    std::error_code ec;
    fs::create_directories(cachefolder_path, ec);
    if (ec) {
      sql_print_error("StoneDB: Can not create folder %s.", cachefolder_path.c_str());
      STONEDB_LOG(LogCtl_Level::ERROR, "DatabaseException: %s", ec.message().c_str());
      return 1;
    }
  }

  if (!system::IsReadWriteAllowed(cachefolder_path)) {
    sql_print_error("StoneDB: Can not access cache folder %s.", cachefolder_path.c_str());
    STONEDB_LOG(LogCtl_Level::ERROR, "Can not access cache folder %s", cachefolder_path.c_str());
    return 1;
  }
  return 0;
}

std::string get_parameter_name(enum sdb_var_name vn) {
  DEBUG_ASSERT(static_cast<int>(vn) >= 0 && static_cast<int>(vn) <= static_cast<int>(sdb_var_name::SDB_VAR_LIMIT));
  return sdb_var_name_strings[static_cast<int>(vn)];
}

int get_parameter(THD *thd, enum sdb_var_name vn, double &value) {
  std::string var_data = get_parameter_name(vn);
  user_var_entry *m_entry;
  my_bool null_val;

  m_entry = (user_var_entry *)my_hash_search(&thd->user_vars, (uchar *)var_data.c_str(), (uint)var_data.size());
  if (!m_entry) return 1;
  value = m_entry->val_real(&null_val);
  if (null_val) return 2;
  return 0;
}

int get_parameter(THD *thd, enum sdb_var_name vn, int64_t &value) {
  std::string var_data = get_parameter_name(vn);
  user_var_entry *m_entry;
  my_bool null_val;

  m_entry = (user_var_entry *)my_hash_search(&thd->user_vars, (uchar *)var_data.c_str(), (uint)var_data.size());

  if (!m_entry) return 1;
  value = m_entry->val_int(&null_val);
  if (null_val) return 2;
  return 0;
}

int get_parameter(THD *thd, enum sdb_var_name vn, std::string &value) {
  my_bool null_val;
  std::string var_data = get_parameter_name(vn);
  user_var_entry *m_entry;
  String str;

  m_entry = (user_var_entry *)my_hash_search(&thd->user_vars, (uchar *)var_data.c_str(), (uint)var_data.size());
  if (!m_entry) return 1;

  m_entry->val_str(&null_val, &str, NOT_FIXED_DEC);

  if (null_val) return 2;
  value = std::string(str.ptr());

  return 0;
}

int get_parameter(THD *thd, enum sdb_var_name vn, longlong &result, std::string &s_result) {
  user_var_entry *m_entry;
  std::string var_data = get_parameter_name(vn);

  m_entry = (user_var_entry *)my_hash_search(&thd->user_vars, (uchar *)var_data.c_str(), (uint)var_data.size());
  if (!m_entry) return 1;

  if (m_entry->type() == DECIMAL_RESULT) {
    switch (vn) {
      case sdb_var_name::SDB_ABORT_ON_THRESHOLD: {
        double dv;
        my_bool null_value;
        my_decimal v;

        m_entry->val_decimal(&null_value, &v);
        my_decimal2double(E_DEC_FATAL_ERROR, &v, &dv);
        result = *(longlong *)&dv;
        break;
      }
      default:
        result = -1;
        break;
    }
    return 0;
  } else if (m_entry->type() == INT_RESULT) {
    switch (vn) {
      case sdb_var_name::SDB_THROTTLE:
      case sdb_var_name::SDB_STONEDBEXPRESSIONS:
      case sdb_var_name::SDB_PARALLEL_AGGR:
      case sdb_var_name::SDB_ABORT_ON_COUNT:
        my_bool null_value;
        result = m_entry->val_int(&null_value);
        break;
      default:
        result = -1;
        break;
    }
    return 0;
  } else if (m_entry->type() == STRING_RESULT) {
    result = -1;
    my_bool null_value;
    String str;

    m_entry->val_str(&null_value, &str, NOT_FIXED_DEC);
    var_data = std::string(str.ptr());

    if (vn == sdb_var_name::SDB_DATAFORMAT || vn == sdb_var_name::SDB_REJECT_FILE_PATH) {
      s_result = var_data;
    } else if (vn == sdb_var_name::SDB_PIPEMODE) {
      boost::to_upper(var_data);
      if (var_data == "SERVER") result = 1;
      if (var_data == "CLIENT") result = 0;
    } else if (vn == sdb_var_name::SDB_NULL) {
      s_result = var_data;
    }
    return 0;
  } else {
    result = 0;
    return 2;
  }
}

void Engine::RenameTable([[maybe_unused]] Transaction *trans, const std::string &from, const std::string &to,
                         [[maybe_unused]] THD *thd) {
  UnRegisterTable(from);
  auto id = RCTable::GetTableId(from + common::STONEDB_EXT);
  cache.ReleaseTable(id);
  filter_cache.RemoveIf([id](const FilterCoordinate &c) { return c[0] == int(id); });
  system::RenameFile(stonedb_data_dir / (from + common::STONEDB_EXT), stonedb_data_dir / (to + common::STONEDB_EXT));
  RenameRdbTable(from, to);
  UnregisterMemTable(from, to);
  STONEDB_LOG(LogCtl_Level::INFO, "Rename table %s to %s", from.c_str(), to.c_str());
}

void Engine::PrepareAlterTable(const std::string &table_path, std::vector<Field *> &new_cols,
                               std::vector<Field *> &old_cols, [[maybe_unused]] THD *thd) {
  // make sure all deffered jobs are done in case some file gets recreated.
  HandleDeferredJobs();

  auto tab = current_tx->GetTableByPath(table_path);
  RCTable::Alter(table_path, new_cols, old_cols, tab->NumOfObj());
  cache.ReleaseTable(tab->GetID());
  UnRegisterTable(table_path);
}

static void HandleDelayedLoad(int tid, std::vector<std::unique_ptr<char[]>> &vec) {
  std::string addr = std::to_string(reinterpret_cast<long>(&vec));

  std::string table_path(vec[0].get() + sizeof(int32_t));
  std::string db_name, tab_name;

  std::tie(db_name, tab_name) = GetNames(table_path);
  std::string load_data_query = "LOAD DELAYED INSERT DATA INTO TABLE " + db_name + "." + tab_name;

  my_thread_init();
  THD *thd = new THD;
  thd->thread_stack = (char *)&thd;
  thd->security_ctx->skip_grants();
  init_thr_lock();
  thd->store_globals();

  /* Add thread to THD list so that's it's visible in 'show processlist' */
  mysql_mutex_lock(&LOCK_thread_count);
  thd->thread_id = thd->variables.pseudo_thread_id = thread_id++;
  thd->set_current_time();
  add_global_thread(thd);
  mysql_mutex_unlock(&LOCK_thread_count);

  mysql_reset_thd_for_next_command(thd);

  thd->init_for_queries();
  thd->set_db(db_name.c_str(), db_name.length());
  // Forge LOAD DATA INFILE query which will be used in SHOW PROCESS LIST
  thd->set_query_and_id(const_cast<char *>(load_data_query.c_str()), load_data_query.length(), thd->charset(),
                        next_query_id());
  thd->update_charset();  // for the charset change to take effect

  // Usually lex_start() is called by mysql_parse(), but we need
  // it here as the present method does not call mysql_parse().
  lex_start(thd);
  TABLE_LIST tl;
  tl.init_one_table(thd->strmake(thd->db, thd->db_length), thd->db_length, tab_name.c_str(), tab_name.length(),
                    tab_name.c_str(), TL_WRITE_CONCURRENT_INSERT);
  tl.updating = 1;
  // the table will be opened in mysql_load
  thd->lex->local_file = false;
  thd->lex->sql_command = SQLCOM_LOAD;
  sql_exchange ex("buffered_insert", 0, FILETYPE_MEM);
  ex.file_name = const_cast<char *>(addr.c_str());
  ex.skip_lines = tid;  // this is ugly...
  thd->lex->select_lex.context.resolve_in_table_list_only(&tl);
  if (open_temporary_tables(thd, &tl)) {
    // error
  }
  List<Item> tmp_list;  // dummy
  if (mysql_load(thd, &ex, &tl, tmp_list, tmp_list, tmp_list, DUP_ERROR, false, false)) {
    thd->is_slave_error = 1;
  }

  thd->catalog = 0;
  thd->set_db(NULL, 0); /* will free the current database */
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

  free_root(thd->mem_root, MYF(MY_KEEP_PREALLOC));
  if (thd->is_fatal_error) {
    STONEDB_LOG(LogCtl_Level::ERROR, "LOAD DATA failed on table '%s'", tab_name.c_str());
  }
  thd->release_resources();
  remove_global_thread(thd);
  delete thd;
  my_thread_end();
}

void DistributeLoad(std::unordered_map<int, std::vector<std::unique_ptr<char[]>>> &tm) {
  utils::result_set<void> res;
  for (auto &it : tm) {
    res.insert(rceng->delay_insert_thread_pool.add_task(HandleDelayedLoad, it.first, std::ref(it.second)));
  }
  res.get_all();
  tm.clear();
}

void Engine::ProcessDelayedInsert() {
  mysql_mutex_lock(&LOCK_server_started);
  while (!mysqld_server_started) {
    struct timespec abstime;
    set_timespec(abstime, 1);
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
    if (stonedb_sysvar_enable_rowstore) {
      std::unique_lock<std::mutex> lk(cv_mtx);
      cv.wait_for(lk, std::chrono::milliseconds(stonedb_sysvar_insert_wait_ms));
      continue;
    }

    size_t len;
    auto rec = GetRecord(len);
    if (len == 0) {
      if (tm.empty()) {
        std::unique_lock<std::mutex> lk(cv_mtx);
        cv.wait_for(lk, std::chrono::milliseconds(stonedb_sysvar_insert_wait_ms));
      } else {
        if (buffer_recordnum >= stonedb_sysvar_insert_numthreshold || sleep_cnt > stonedb_sysvar_insert_cntthreshold) {
          DistributeLoad(tm);
          buffer_recordnum = 0;
          sleep_cnt = 0;
        } else {
          std::unique_lock<std::mutex> lk(cv_mtx);
          cv.wait_for(lk, std::chrono::milliseconds(stonedb_sysvar_insert_wait_ms));
          sleep_cnt++;
        }
      }
      continue;
    }

    buffer_recordnum++;
    auto tid = *(int32_t *)rec.get();
    tm[tid].emplace_back(std::move(rec));
    if (tm[tid].size() > static_cast<std::size_t>(stonedb_sysvar_insert_max_buffered)) {
      // in case the ingress rate is too high
      DistributeLoad(tm);
      buffer_recordnum = 0;
    }
  }
  STONEDB_LOG(LogCtl_Level::INFO, "StoneDB load thread exiting...");
}

void Engine::ProcessDelayedMerge() {
  mysql_mutex_lock(&LOCK_server_started);
  while (!mysqld_server_started) {
    struct timespec abstime;
    set_timespec(abstime, 1);
    mysql_cond_timedwait(&COND_server_started, &LOCK_server_started, &abstime);
    if (exiting) {
      mysql_mutex_unlock(&LOCK_server_started);
      return;
    }
  }
  mysql_mutex_unlock(&LOCK_server_started);

  std::map<std::string, int> sleep_cnts;
  while (!exiting) {
    if (!stonedb_sysvar_enable_rowstore) {
      std::unique_lock<std::mutex> lk(cv_merge_mtx);
      cv_merge.wait_for(lk, std::chrono::milliseconds(stonedb_sysvar_insert_wait_ms));
      continue;
    }

    std::unordered_map<int, std::vector<std::unique_ptr<char[]>>> tm;
    {
      try {
        std::scoped_lock guard(mem_table_mutex);
        for (auto &[name, mem_table] : mem_table_map) {
          int64_t record_count = mem_table->CountRecords();
          if (record_count >= stonedb_sysvar_insert_numthreshold ||
              (sleep_cnts.count(name) && sleep_cnts[name] > stonedb_sysvar_insert_cntthreshold)) {
            auto share = rceng->getTableShare(name);
            auto tid = share->TabID();
            utils::BitSet null_mask(share->NoCols());
            std::unique_ptr<char[]> buf(new char[sizeof(uint32_t) + name.size() + 1 + null_mask.data_size()]);
            char *ptr = buf.get();
            *(int32_t *)ptr = tid;  // table id
            ptr += sizeof(int32_t);
            std::memcpy(ptr, name.c_str(), name.size());
            ptr += name.size();
            *ptr++ = 0;  // end with NUL
            std::memcpy(ptr, null_mask.data(), null_mask.data_size());
            tm[tid].emplace_back(std::move(buf));
            sleep_cnts[name] = 0;
          } else if (record_count > 0) {
            if (sleep_cnts.count(name))
              sleep_cnts[name]++;
            else
              sleep_cnts[name] = 0;
          }
        }
      } catch (common::Exception &e) {
        STONEDB_LOG(LogCtl_Level::ERROR, "delayed merge failed. %s %s", e.what(), e.trace().c_str());
        std::unique_lock<std::mutex> lk(cv_merge_mtx);
        cv_merge.wait_for(lk, std::chrono::milliseconds(stonedb_sysvar_insert_wait_ms));
        continue;
      } catch (...) {
        STONEDB_LOG(LogCtl_Level::ERROR, "delayed merge failed.");
        std::unique_lock<std::mutex> lk(cv_merge_mtx);
        cv_merge.wait_for(lk, std::chrono::milliseconds(stonedb_sysvar_insert_wait_ms));
        continue;
      }
    }
    if (!tm.empty())
      DistributeLoad(tm);
    else {
      std::unique_lock<std::mutex> lk(cv_merge_mtx);
      cv_merge.wait_for(lk, std::chrono::milliseconds(stonedb_sysvar_insert_wait_ms));
    }
  }
  STONEDB_LOG(LogCtl_Level::INFO, "StoneDB merge thread exiting...");
}

void Engine::LogStat() {
  static long last_sample_time = 0;
  static query_id_t saved_query_id = 0;
  static StonedbStat saved;

  if (last_sample_time == 0) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    last_sample_time = t.tv_sec;
    saved_query_id = global_query_id;
    saved = stonedb_stat;
    return;
  }

  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  long sample_time = t.tv_sec;
  long diff = sample_time - last_sample_time;
  if (diff == 0) {
    STONEDB_LOG(LogCtl_Level::ERROR, "LogStat() called too frequently. last sample time %ld ", last_sample_time);
    return;
  }
  query_id_t query_id = global_query_id;
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

    STATUS_VAR sv;
    calc_sum_of_all_status(&sv);

    std::string msg("Command: ");
    for (auto &c : cmds) {
      auto delta = sv.com_stat[c] - saved_com_stat[c];
      saved_com_stat[c] = sv.com_stat[c];
      msg =
          msg + sql_statement_names[c].str + " " + std::to_string(delta) + "/" + std::to_string(sv.com_stat[c]) + ", ";
    }
    msg = msg + "queries " + std::to_string(queries) + "/" + std::to_string(global_query_id);
    STONEDB_LOG(LogCtl_Level::INFO, msg.c_str());
  }

  STONEDB_LOG(LogCtl_Level::INFO,
              "Select: %lu/%lu, Loaded: %lu/%lu(%lu/%lu), dup: %lu/%lu, insert: "
              "%lu/%lu, failed insert: %lu/%lu, update: "
              "%lu/%lu",
              stonedb_stat.select - saved.select, stonedb_stat.select, stonedb_stat.loaded - saved.loaded,
              stonedb_stat.loaded, stonedb_stat.load_cnt - saved.load_cnt, stonedb_stat.load_cnt,
              stonedb_stat.loaded_dup - saved.loaded_dup, stonedb_stat.loaded_dup,
              stonedb_stat.delayinsert - saved.delayinsert, stonedb_stat.delayinsert,
              stonedb_stat.failed_delayinsert - saved.failed_delayinsert, stonedb_stat.failed_delayinsert,
              stonedb_stat.update - saved.update, stonedb_stat.update);

  if (stonedb_stat.loaded == saved.loaded && stonedb_stat.delayinsert > saved.delayinsert) {
    STONEDB_LOG(LogCtl_Level::ERROR, "No data loaded from insert buffer");
  }

  // update with last minute statistics
  IPM = stonedb_stat.delayinsert - saved.delayinsert;
  IT = stonedb_stat.delayinsert;
  QPM = stonedb_stat.select - saved.select;
  QT = stonedb_stat.select;
  LPM = stonedb_stat.loaded - saved.loaded;
  LT = stonedb_stat.loaded;
  LDPM = stonedb_stat.loaded_dup - saved.loaded_dup;
  LDT = stonedb_stat.loaded_dup;
  UPM = stonedb_stat.update - saved.update;
  UT = stonedb_stat.update;

  last_sample_time = sample_time;
  saved_query_id = query_id;
  saved = stonedb_stat;
}

void Engine::InsertDelayed(const std::string &table_path, int tid, TABLE *table) {
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
  std::shared_ptr<void> defer(nullptr,
                              [table, org_bitmap](...) { dbug_tmp_restore_column_map(table->read_set, org_bitmap); });

  uint32_t buf_sz = 0;
  std::unique_ptr<char[]> buf;
  EncodeRecord(table_path, tid, table->field, table->s->fields, table->s->blob_fields, buf, buf_sz);

  int failed = 0;
  while (true) {
    try {
      insert_buffer.Write(utils::MappedCircularBuffer::TAG::INSERT_RECORD, buf.get(), buf_sz);
    } catch (std::length_error &e) {
      if (failed++ >= stonedb_sysvar_insert_wait_time / 50) {
        STONEDB_LOG(LogCtl_Level::ERROR, "insert buffer is out of space");
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
  EncodeRecord(table_path, share->TabID(), table->field, table->s->fields, table->s->blob_fields, buf, buf_sz);
  auto rctable = share->GetSnapshot();
  rctable->InsertMemRow(std::move(buf), buf_sz);
}

int Engine::InsertRow(const std::string &table_path, [[maybe_unused]] Transaction *trans, TABLE *table,
                      std::shared_ptr<TableShare> &share) {
  int ret = 0;
  try {
    if (stonedb_sysvar_insert_delayed) {
      if (stonedb_sysvar_enable_rowstore) {
        InsertMemRow(table_path, share, table);
      } else {
        InsertDelayed(table_path, share->TabID(), table);
      }
      stonedb_stat.delayinsert++;
    } else {
      auto rct = current_tx->GetTableByPath(table_path);
      ret = rct->Insert(table);
    }
    return ret;
  } catch (common::Exception &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "delayed inserting failed. %s %s", e.what(), e.trace().c_str());
  } catch (std::exception &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "delayed inserting failed. %s", e.what());
  } catch (...) {
    STONEDB_LOG(LogCtl_Level::ERROR, "delayed inserting failed.");
  }

  if (stonedb_sysvar_insert_delayed) {
    stonedb_stat.failed_delayinsert++;
    ret = 1;
  }

  return ret;
}

common::SDBError Engine::RunLoader(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg) {
  common::SDBError sdbe;
  TABLE *table;
  int transactional_table = 0;

  table = table_list->table;
  transactional_table = table->file->has_transactions();

  try {
    std::unique_ptr<system::IOParameters> iop;

    auto sdberror = Engine::GetIOP(iop, *thd, *ex, table, arg);

    if (sdberror != common::ErrorCode::SUCCESS) throw sdberror;

    std::string table_path = GetTablePath(table);

    table->copy_blobs = 0;
    thd->cuted_fields = 0L;

    auto tab = current_tx->GetTableByPath(table_path);

    tab->LoadDataInfile(*iop);

    if (current_tx->Killed()) {
      thd->send_kill_message();
      throw common::SDBError(common::ErrorCode::KILLED);
    }

    // We must invalidate the table in query cache before binlog writing and
    // ha_autocommit_...
    query_cache_invalidate3(thd, table_list, 0);

    COPY_INFO::Statistics stats;
    stats.records = ha_rows(tab->NoRecordsLoaded());
    stonedb_stat.loaded += tab->NoRecordsLoaded();
    stonedb_stat.loaded_dup += tab->NoRecordsDuped();
    stonedb_stat.load_cnt++;

    char name[FN_REFLEN];
    my_snprintf(name, sizeof(name), ER(ER_LOAD_INFO), (long)stats.records,
                0,  // deleted
                0,  // skipped
                (long)thd->get_stmt_da()->current_statement_warn_count());

    /* ok to client */
    my_ok(thd, stats.records, 0L, name);
  } catch (common::Exception &e) {
    sdbe = common::SDBError(common::ErrorCode::UNKNOWN_ERROR, "StoneDB internal error");
  } catch (common::SDBError &e) {
    sdbe = e;
  }

  if (thd->transaction.stmt.cannot_safely_rollback()) thd->transaction.all.mark_modified_non_trans_table();

  if (transactional_table) {
    if (sdbe == common::ErrorCode::SUCCESS)
      trans_commit_stmt(thd);
    else
      trans_rollback_stmt(thd);
  }

  if (thd->lock) {
    mysql_unlock_tables(thd, thd->lock);
    thd->lock = 0;
  }
  thd->abort_on_warning = 0;
  return sdbe;
}

bool Engine::IsSDBRoute(THD *thd, TABLE_LIST *table_list, SELECT_LEX *selects_list,
                        int &in_case_of_failure_can_go_to_mysql, int with_insert) {
  in_case_of_failure_can_go_to_mysql = true;

  if (!table_list) return false;
  if (with_insert) table_list = table_list->next_global;  // we skip one

  if (!table_list) return false;

  bool has_SDBTable = false;
  for (TABLE_LIST *tl = table_list; tl; tl = tl->next_global) {  // SZN:we go through tables
    if (!tl->derived && !tl->view) {
      // In this list we have all views, derived tables and their
      // sources, so anyway we walk through all the source tables
      // even though we seem to reject the control of views
      if (!IsSDBTable(tl->table))
        return false;
      else
        has_SDBTable = true;
    }
  }
  if (!has_SDBTable)  // No StoneDB table is involved. Return to MySQL.
    return false;

  // then we check the parameter of file format.
  // if it is MYSQL_format AND we write to a file, it is a MYSQL route.
  int is_dump;
  const char *file = GetFilename(selects_list, is_dump);

  if (file) {  // it writes to a file
    longlong param = 0;
    std::string s_res;
    if (!get_parameter(thd, sdb_var_name::SDB_DATAFORMAT, param, s_res)) {
      if (boost::iequals(boost::trim_copy(s_res), "MYSQL")) return false;

      common::DataFormatPtr df = common::DataFormat::GetDataFormat(s_res);
      if (!df) {  // parameter is UNKNOWN VALUE
        my_message(ER_SYNTAX_ERROR, "Histgore specific error: Unknown value of SDB_DATAFORMAT parameter", MYF(0));
        return true;
      } else if (!df->CanExport()) {
        my_message(ER_SYNTAX_ERROR,
                   (std::string("StoneDB specific error: Export in '") + df->GetName() + ("' format is not supported."))
                       .c_str(),
                   MYF(0));
        return true;
      } else
        in_case_of_failure_can_go_to_mysql = false;  // in case of failure
                                                     // it cannot go to MYSQL - it writes to a file,
                                                     // but the file format is not MYSQL
    } else                                           // param not set - we assume it is (deprecated: MYSQL)
                                                     // common::EDF::TRI_UNKNOWN
      return true;
  }

  return true;
}

bool Engine::IsSDBTable(TABLE *table) {
  return table && table->s->db_type() == rcbase_hton;  // table->db_type is always NULL
}

const char *Engine::GetFilename(SELECT_LEX *selects_list, int &is_dumpfile) {
  // if the function returns a filename <> NULL
  // additionally is_dumpfile indicates whether it was 'select into OUTFILE' or
  // maybe 'select into DUMPFILE' if the function returns NULL it was a regular
  // 'select' don't look into is_dumpfile in this case
  if (selects_list->parent_lex->exchange) {
    is_dumpfile = selects_list->parent_lex->exchange->dumpfile;
    return selects_list->parent_lex->exchange->file_name;
  }
  return 0;
}

std::unique_ptr<system::IOParameters> Engine::CreateIOParameters(const std::string &path, void *arg) {
  if (path.empty()) return std::unique_ptr<system::IOParameters>(new system::IOParameters());

  std::string data_dir;
  std::string data_path;

  if (fs::path(path).is_absolute()) {
    data_dir = "";
    data_path = path;
  } else {
    data_dir = rceng->stonedb_data_dir;
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

  iop->SetATIs(current_tx->GetTableByPath(path)->GetATIs());
  iop->SetLogInfo(arg);
  return iop;
}

std::unique_ptr<system::IOParameters> Engine::CreateIOParameters([[maybe_unused]] THD *thd, TABLE *table, void *arg) {
  if (table == NULL) return CreateIOParameters("", arg);

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
  if (calc_time_diff(&utc, &client_zone, 1, &secs, &msecs)) sign = -1;
  minutes = (short)(secs / 60);
}

common::SDBError Engine::GetRejectFileIOParameters(THD &thd, std::unique_ptr<system::IOParameters> &io_params) {
  std::string reject_file;
  int64_t abort_on_count = 0;
  double abort_on_threshold = 0;

  get_parameter(&thd, sdb_var_name::SDB_REJECT_FILE_PATH, reject_file);
  if (get_parameter(&thd, sdb_var_name::SDB_REJECT_FILE_PATH, reject_file) == 2)
    return common::SDBError(common::ErrorCode::WRONG_PARAMETER, "Wrong value of STONEDB_LOAD_REJECT_FILE parameter.");

  if (get_parameter(&thd, sdb_var_name::SDB_ABORT_ON_COUNT, abort_on_count) == 2)
    return common::SDBError(common::ErrorCode::WRONG_PARAMETER, "Wrong value of SDB_ABORT_ON_COUNT parameter.");

  if (get_parameter(&thd, sdb_var_name::SDB_ABORT_ON_THRESHOLD, abort_on_threshold) == 2)
    return common::SDBError(common::ErrorCode::WRONG_PARAMETER, "Wrong value of SDB_ABORT_ON_THRESHOLD parameter.");

  if (abort_on_count != 0 && abort_on_threshold != 0)
    return common::SDBError(common::ErrorCode::WRONG_PARAMETER,
                            "SDB_ABORT_ON_COUNT and SDB_ABORT_ON_THRESHOLD "
                            "parameters are mutualy exclusive.");

  if (!(abort_on_threshold >= 0.0 && abort_on_threshold < 1.0))
    return common::SDBError(common::ErrorCode::WRONG_PARAMETER,
                            "SDB_ABORT_ON_THRESHOLD parameter value must be in range (0,1).");

  if ((abort_on_count != 0 || abort_on_threshold != 0) && reject_file.empty())
    return common::SDBError(common::ErrorCode::WRONG_PARAMETER,
                            "SDB_ABORT_ON_COUNT or SDB_ABORT_ON_THRESHOLD can by only specified with "
                            "SDB_REJECT_FILE_PATH parameter.");

  if (!reject_file.empty() && fs::exists(reject_file))
    return common::SDBError(common::ErrorCode::WRONG_PARAMETER,
                            "Can not create the reject file, the file already exists.");

  io_params->SetRejectFile(reject_file, abort_on_count, abort_on_threshold);
  return common::ErrorCode::SUCCESS;
}

common::SDBError Engine::GetIOP(std::unique_ptr<system::IOParameters> &io_params, THD &thd, sql_exchange &ex,
                                TABLE *table, void *arg, bool for_exporter) {
  const CHARSET_INFO *cs = ex.cs;
  bool local_load = for_exporter ? false : (bool)(thd.lex)->local_file;
  uint value_list_elements = (thd.lex)->value_list.elements;
  // thr_lock_type lock_option = (thd.lex)->lock_option;

  int io_mode = -1;
  char name[FN_REFLEN];
  char *tdb = 0;
  if (table) {
    tdb = table->s->db.str ? table->s->db.str : thd.db;
  } else
    tdb = thd.db;

  io_params = CreateIOParameters(&thd, table, arg);
  short sign, minutes;
  ComputeTimeZoneDiffInMinutes(&thd, sign, minutes);
  io_params->SetTimeZone(sign, minutes);

  if (ex.filetype == FILETYPE_MEM) {
    io_params->load_delayed_insert = true;
  }
#ifdef DONT_ALLOW_FULL_LOAD_DATA_PATSTONEDB
  ex->file_name += dirname_length(ex->file_name);
#endif

  longlong param = 0;
  std::string s_res;
  if (common::DataFormat::GetNoFormats() > 1) {
    if (!get_parameter(&thd, sdb_var_name::SDB_DATAFORMAT, param, s_res)) {
      common::DataFormatPtr df = common::DataFormat::GetDataFormat(s_res);
      if (!df)
        return common::SDBError(common::ErrorCode::WRONG_PARAMETER, "Unknown value of SDB_DATAFORMAT parameter.");
      else
        io_mode = df->GetId();
    } else
      io_mode = common::DataFormat::GetDataFormat(0)->GetId();
  } else
    io_mode = common::DataFormat::GetDataFormat(0)->GetId();

  if (!get_parameter(&thd, sdb_var_name::SDB_NULL, param, s_res)) io_params->SetNullsStr(s_res);

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

  if (ex.escaped->length() > 1)
    return common::SDBError(common::ErrorCode::WRONG_PARAMETER, "Multicharacter escape std::string not supported.");

  if (ex.enclosed->length() > 1 && (ex.enclosed->length() != 4 || strcasecmp(ex.enclosed->ptr(), "NULL") != 0))
    return common::SDBError(common::ErrorCode::WRONG_PARAMETER, "Multicharacter enclose std::string not supported.");

  if (!for_exporter) {
    common::SDBError sdberr = GetRejectFileIOParameters(thd, io_params);
    if (sdberr.GetErrorCode() != common::ErrorCode::SUCCESS) return sdberr;
  }

  if (stonedb_sysvar_usemysqlimportexportdefaults) {
    io_params->SetEscapeCharacter(*ex.escaped->ptr());
    io_params->SetDelimiter(ex.field_term->ptr());
    io_params->SetLineTerminator(ex.line_term->ptr());
    if (ex.enclosed->length() == 4 && strcasecmp(ex.enclosed->ptr(), "NULL") == 0)
      io_params->SetParameter(system::Parameter::STRING_QUALIFIER, '\0');
    else
      io_params->SetParameter(system::Parameter::STRING_QUALIFIER, *ex.enclosed->ptr());

  } else {
    if (ex.escaped->alloced_length() != 0) io_params->SetEscapeCharacter(*ex.escaped->ptr());

    if (ex.field_term->alloced_length() != 0) io_params->SetDelimiter(ex.field_term->ptr());

    if (ex.line_term->alloced_length() != 0) io_params->SetLineTerminator(ex.line_term->ptr());

    if (ex.enclosed->length()) {
      if (ex.enclosed->length() == 4 && strcasecmp(ex.enclosed->ptr(), "NULL") == 0)
        io_params->SetParameter(system::Parameter::STRING_QUALIFIER, '\0');
      else
        io_params->SetParameter(system::Parameter::STRING_QUALIFIER, *ex.enclosed->ptr());
    }
  }

  if (io_params->EscapeCharacter() != 0 &&
      io_params->Delimiter().find(io_params->EscapeCharacter()) != std::string::npos)
    return common::SDBError(common::ErrorCode::WRONG_PARAMETER,
                            "Field terminator containing the escape character not supported.");

  if (io_params->EscapeCharacter() != 0 && io_params->StringQualifier() != 0 &&
      io_params->EscapeCharacter() == io_params->StringQualifier())
    return common::SDBError(common::ErrorCode::WRONG_PARAMETER,
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

  if (ex.line_start != 0 && ex.line_start->length() != 0) {
    unsupported_syntax = true;
    io_params->SetParameter(system::Parameter::LINE_STARTER, std::string(ex.line_start->ptr()));
  }

  if (local_load && ((thd.lex)->sql_command == SQLCOM_LOAD)) {
    io_params->SetParameter(system::Parameter::LOCAL_LOAD, (int)local_load);
  }

  if (value_list_elements != 0) {
    unsupported_syntax = true;
    io_params->SetParameter(system::Parameter::VALUE_LIST_ELEMENTS, (int64_t)value_list_elements);
  }

  if (ex.opt_enclosed) {
    // unsupported_syntax = true;
    io_params->SetParameter(system::Parameter::OPTIONALLY_ENCLOSED, 1);
  }

  if (unsupported_syntax)
    push_warning(&thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
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
      auto share = std::make_shared<TableShare>(name + common::STONEDB_EXT, table_share);
      table_share_map[name] = share;
      return share;
    }
    return it->second;
  } catch (common::Exception &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Failed to create table share: %s", e.what());
  } catch (std::exception &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Failed to create table share: %s", e.what());
  } catch (...) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Failed to create table share");
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
    if (tab->Enable())
      m_table_keys[table_path] = tab;
    else
      tab.reset();
  }
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
    STONEDB_LOG(LogCtl_Level::ERROR, "Failed to create memory table: %s / %s", e.what(), e.trace().c_str());
  } catch (std::exception &e) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Failed to create memory table: %s", e.what());
  } catch (...) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Failed to create memory table");
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

}  // namespace core
}  // namespace stonedb

int stonedb_push_data_dir(const char *dir) {
  using namespace stonedb;
  using namespace core;
  try {
    auto p = fs::path(dir);
    if (!fs::is_directory(p)) {
      STONEDB_LOG(LogCtl_Level::ERROR, "Path %s is not a directory or cannot be accessed.", dir);
      return 1;
    }

    if (fs::space(p).available < 1_GB) {
      STONEDB_LOG(LogCtl_Level::ERROR, "StoneDB requires data directory has at least 1G available space!");
      return 1;
    }
    p /= fs::path(STONEDB_DATA_DIR);
    auto result = std::find(std::begin(stonedb_data_dirs), std::end(stonedb_data_dirs), p);
    if (result != std::end(stonedb_data_dirs)) {
      STONEDB_LOG(LogCtl_Level::WARN, "Path %s specified multiple times as data directory.", dir);
    } else {
      fs::create_directory(p);
      stonedb_data_dirs.emplace_back(p);
    }
  } catch (fs::filesystem_error &err) {
    STONEDB_LOG(LogCtl_Level::ERROR, "Filesystem error %s", err.what());
    return 1;
  }

  return 0;
}
