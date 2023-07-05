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
#include "core/delta_table.h"
#include "core/table_share.h"
#include "core/temp_table.h"
#include "core/transaction.h"
#include "executor/task_executor.h"
#include "mm/initializer.h"
#include "mm/memory_statistics.h"
#include "mysql/thread_pool_priv.h"
#include "mysqld_thd_manager.h"
#include "system/file_out.h"
#include "system/res_manager.h"
#include "system/stdout.h"
#include "thr_lock.h"
#include "util/bitset.h"
#include "util/fs.h"
#include "util/thread_pool.h"
#include "util/tools.h"

namespace Tianmu {
namespace DBHandler {
extern void resolve_async_join_settings(const std::string &settings);
}
namespace core {
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

fs::path Engine::GetNextDataDir() {
  std::scoped_lock guard(v_mtx);
  core::Engine *eng = reinterpret_cast<core::Engine *>(tianmu_hton->data);

  if (tianmu_data_dirs.empty()) {
    // fall back to use MySQL data directory
    auto p = eng->tianmu_data_dir / TIANMU_DATA_DIR;
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
                               TIANMU_LOG(LogCtl_Level::WARN, "disk %s usage %d%%", s.native().c_str(), usage);
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

bool parameter_equals(THD *thd, enum tianmu_param_name vn, longlong value) {
  longlong param = 0;
  std::string s_res;

  if (!get_parameter(thd, vn, param, s_res))
    if (param == value)
      return true;

  return false;
}

static auto GetNames(const std::string &table_path) {
  auto p = fs::path(table_path);
  return std::make_tuple(p.parent_path().filename().native(), p.filename().native());
}

bool Engine::TrxCmp::operator()(Transaction *l, Transaction *r) const { return l->GetID() < r->GetID(); }

Engine::Engine()
    : bg_load_thread_pool("bg_loader", tianmu_sysvar_bg_load_threads ? tianmu_sysvar_bg_load_threads
                                                                     : ((std::thread::hardware_concurrency() / 2)
                                                                            ? (std::thread::hardware_concurrency() / 2)
                                                                            : 1)),
      load_thread_pool("loader",
                       tianmu_sysvar_load_threads ? tianmu_sysvar_load_threads : std::thread::hardware_concurrency()),
      query_thread_pool(
          "query", tianmu_sysvar_query_threads ? tianmu_sysvar_query_threads : std::thread::hardware_concurrency()),
      delete_or_update_thread_pool("delete_or_update", tianmu_sysvar_delete_or_update_threads
                                                           ? tianmu_sysvar_delete_or_update_threads
                                                           : std::thread::hardware_concurrency()),
      insert_buffer(BUFFER_FILE, tianmu_sysvar_insert_buffer_size) {
  tianmu_data_dir = mysql_real_data_home;
  store_ = nullptr;
}

int Engine::Init(uint engine_slot) {
  m_slot = engine_slot;
  ConfigureRCControl();

  if (tianmu_sysvar_controlquerylog > 0) {
    tianmu_querylog_.setOn();
  } else {
    tianmu_querylog_.setOff();
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
  int hugefilesize = tianmu_sysvar_hugefilesize;  // MB
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

  // init the tianmu key-value store, aka, rocksdb engine.
  store_ = new index::KVStore();
  store_->Init();

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

  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu thread pool for background load, size = %ld", bg_load_thread_pool.size());
  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu thread pool for load, size = %ld", load_thread_pool.size());
  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu thread pool for query, size = %ld", query_thread_pool.size());

  m_monitor_thread = std::thread([this] {
    TIANMU_LOG(LogCtl_Level::INFO, "Tianmu monitor thread start...");
    struct job {
      long interval;
      std::function<void()> func;
    } jobs[] = {
        {tianmu_sysvar_log_loop_interval, [this]() { this->LogStat(); }},
        {tianmu_sysvar_log_loop_interval,
         [this]() {
           for (auto &delta : m_table_deltas) {
             TIANMU_LOG(LogCtl_Level::INFO,
                        "table name: %s, delta table id: %d delta_size: %ld, current load id: %ld, merge id: %ld, "
                        "current row_id: %ld",
                        delta.second->FullName().c_str(), delta.second->GetDeltaTableID(),
                        delta.second->load_id.load() - delta.second->merge_id.load(), delta.second->load_id.load(),
                        delta.second->merge_id.load(), delta.second->row_id.load());
           }
         }},
        {tianmu_sysvar_log_loop_interval * 5,
         []() {
           TIANMU_LOG(
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

    while (!exiting) {
      counter++;
      std::unique_lock<std::mutex> lk(cv_mtx);
      if (cv.wait_for(lk, std::chrono::seconds(tianmu_sysvar_log_loop_interval)) == std::cv_status::timeout) {
        if (!tianmu_sysvar_qps_log)
          continue;
        for (auto &j : jobs) {
          if (counter % (j.interval / tianmu_sysvar_log_loop_interval) == 0)
            j.func();
        }
      }
    }
    TIANMU_LOG(LogCtl_Level::INFO, "Tianmu monitor thread exiting...");
  });

  m_load_thread = std::thread([this] { ProcessInsertBufferMerge(); });
  m_merge_thread = std::thread([this] { ProcessDeltaStoreMerge(); });
  m_purge_thread = std::thread([this] {
    TIANMU_LOG(LogCtl_Level::INFO, "Tianmu file purge thread start...");
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
  DBHandler::resolve_async_join_settings(tianmu_sysvar_async_join);

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
        TIANMU_LOG(LogCtl_Level::ERROR, "Failed to remove file %s Error:%s", t.file.string().c_str(), ec.message());
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
  m_table_deltas.clear();
  m_table_keys.clear();

  if (m_resourceManager) {
    delete m_resourceManager;
    m_resourceManager = nullptr;
  }

  if (store_) {
    delete store_;
    store_ = nullptr;
  }

  if (the_filter_block_owner) {
    delete the_filter_block_owner;
    the_filter_block_owner = nullptr;
  }

  try {
    mm::MemoryManagerInitializer::EnsureNoLeakedTraceableObject();
  } catch (common::AssertException &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Memory leak! %s", e.what());
  } catch (...) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Unkown exception caught");
  }

  mm::MemoryManagerInitializer::deinit(tianmu_control_.isOn() ? true : false);

  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu engine destroyed.");
}

char *Engine::FiledToStr(char *ptr, Field *field, DeltaRecordHead *deltaRecord, int col_num, THD *thd) {
  switch (field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONGLONG: {
      int64_t v = field->val_int();
      common::PushWarningIfOutOfRange(thd, std::string(field->field_name), v, field->type(),
                                      field->flags & UNSIGNED_FLAG);
      *reinterpret_cast<int64_t *>(ptr) = v;
      deltaRecord->field_len_[col_num] = sizeof(int64_t);
      ptr += sizeof(int64_t);
    } break;
    case MYSQL_TYPE_BIT: {
      int64_t v = field->val_int();
      // open it when support M = 64, now all value parsed is < 0.
      if (v > common::TIANMU_BIGINT_MAX)  // v > bigint max when uint64_t is supported
        v = common::TIANMU_BIGINT_MAX;    // TODO(fix with bit prec)
      *reinterpret_cast<int64_t *>(ptr) = v;
      deltaRecord->field_len_[col_num] = sizeof(int64_t);
      ptr += sizeof(int64_t);
    } break;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE: {
      double v = field->val_real();
      *reinterpret_cast<int64_t *>(ptr) = *reinterpret_cast<int64_t *>(&v);
      deltaRecord->field_len_[col_num] = sizeof(int64_t);
      ptr += sizeof(int64_t);
    } break;
    case MYSQL_TYPE_NEWDECIMAL: {
      auto dec_f = dynamic_cast<Field_new_decimal *>(field);
      *(int64_t *)ptr = std::lround(dec_f->val_real() * types::PowOfTen(dec_f->dec));
      deltaRecord->field_len_[col_num] = sizeof(int64_t);
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
      field->get_time(&my_time);
      types::DT dt(my_time);
      *(int64_t *)ptr = dt.val;
      deltaRecord->field_len_[col_num] = sizeof(int64_t);
      ptr += sizeof(int64_t);
    } break;

    case MYSQL_TYPE_TIMESTAMP: {
      MYSQL_TIME my_time;
      std::memset(&my_time, 0, sizeof(my_time));
      field->get_time(&my_time);
      auto saved = my_time.second_part;
      // convert to UTC
      if (!common::IsTimeStampZero(my_time)) {
        my_bool myb;
        my_time_t secs_utc = thd->variables.time_zone->TIME_to_gmt_sec(&my_time, &myb);
        common::GMTSec2GMTTime(&my_time, secs_utc);
      }
      my_time.second_part = saved;
      types::DT dt(my_time);
      *(int64_t *)ptr = dt.val;
      deltaRecord->field_len_[col_num] = sizeof(int64_t);
      ptr += sizeof(int64_t);
    } break;
    case MYSQL_TYPE_YEAR:  // what the hell?
    {
      types::DT dt = {};
      dt.year = field->val_int();
      *(int64_t *)ptr = dt.val;
      deltaRecord->field_len_[col_num] = sizeof(int64_t);
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
      field->val_str(&str);
      std::memcpy(ptr, str.ptr(), str.length());
      deltaRecord->field_len_[col_num] = str.length();
      ptr += str.length();
    } break;
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_NULL:
    default:
      throw common::Exception("unsupported mysql type " + std::to_string(field->type()));
      break;
  }
  return ptr;
}

const char *Engine::StrToFiled(const char *ptr, Field *field, DeltaRecordHead *deltaRecord, int col_num) {
  switch (field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_LONGLONG: {
      int64_t v = *(int64_t *)ptr;
      ptr += sizeof(int64_t);
      field->store(v, true);
      break;
    }
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_DECIMAL: {
      double v = *(double *)ptr;
      ptr += sizeof(double);
      field->store(v);
      break;
    }
    case MYSQL_TYPE_NEWDECIMAL: {
      my_decimal md;
      double v = *(double *)ptr;
      ptr += sizeof(double);
      double2decimal(v, &md);
      auto dec_field = static_cast<Field_new_decimal *>(field);
      decimal_round(&md, &md, dec_field->decimals(), HALF_UP);
      decimal2bin(&md, (uchar *)field->ptr, dec_field->precision, dec_field->decimals());
    }
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2: {
      uint64_t v = *(uint64_t *)ptr;
      ptr += sizeof(uint64_t);
      types::DT dt;
      dt.val = v;
      MYSQL_TIME my_time = {};
      dt.Store(&my_time, MYSQL_TIMESTAMP_TIME);
      field->store_time(&my_time);
    } break;
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_NEWDATE: {
      uint64_t v = *(uint64_t *)ptr;
      ptr += sizeof(uint64_t);
      types::DT dt;
      dt.val = v;
      MYSQL_TIME my_time = {};
      dt.Store(&my_time, MYSQL_TIMESTAMP_DATE);
      field->store_time(&my_time);
    } break;
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2: {
      uint64_t v = *(uint64_t *)ptr;
      ptr += sizeof(uint64_t);
      types::DT dt;
      dt.val = v;
      MYSQL_TIME my_time = {};
      dt.Store(&my_time, MYSQL_TIMESTAMP_DATETIME);
      field->store_time(&my_time);
    } break;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB: {
      uint32_t str_len = deltaRecord->field_len_[col_num];
      auto buf = std::make_unique<char[]>(str_len);
      std::memcpy(buf.get(), ptr, str_len);
      ptr += str_len;
      field->store(buf.get(), str_len, field->charset());
    } break;
    default:
      // MYSQL_TYPE_BIT
      throw common::Exception("Unsupported field type for INSERT " + std::to_string(field->type()));
  }
  return ptr;
}

void Engine::EncodeInsertRecord(const std::string &table_path, Field **field, size_t col, size_t blobs,
                                std::unique_ptr<char[]> &buf, uint32_t &size, THD *thd) {
  size = blobs > 0 ? 4_MB : 128_KB;
  buf.reset(new char[size]);
  char *ptr = buf.get();
  DeltaRecordHeadForInsert deltaRecord(DELTA_RECORD_NORMAL, col);
  ptr = deltaRecord.recordEncode(ptr);

  for (uint i = 0; i < col; i++) {
    Field *f = field[i];
    {  // resize
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
    }

    if (f->is_null()) {
      deltaRecord.null_mask_.set(i);
      deltaRecord.field_len_[i] = 0;
      continue;
    }
    ptr = FiledToStr(ptr, f, &deltaRecord, i, thd);
    ASSERT(ptr <= buf.get() + size, "Buffer overflow");
  }

  std::memcpy(buf.get() + deltaRecord.null_offset_, deltaRecord.null_mask_.data(), deltaRecord.null_mask_.data_size());
  size = ptr - buf.get();
}

bool Engine::DecodeInsertRecordToField(const char *ptr, Field **fields) {
  DeltaRecordHeadForInsert deltaRecord;
  ptr = deltaRecord.recordDecode(ptr);

  if (deltaRecord.is_deleted_ == DELTA_RECORD_DELETE) {
    return false;
  }
  for (uint i = 0; i < deltaRecord.field_count_; i++) {
    auto field = fields[i];
    if (deltaRecord.null_mask_[i]) {
      field->set_null();
      continue;
    }
    field->set_notnull();
    ptr = StrToFiled(ptr, field, &deltaRecord, i);
  }
  return true;
}

void Engine::EncodeUpdateRecord(const std::string &table_path, std::unordered_map<uint, Field *> update_fields,
                                size_t field_count, size_t blobs, std::unique_ptr<char[]> &buf, uint32_t &buf_size,
                                THD *thd) {
  buf_size = blobs > 0 ? 4_MB : 128_KB;
  buf.reset(new char[buf_size]);
  char *ptr = buf.get();
  DeltaRecordHeadForUpdate deltaRecord(field_count);
  ptr = deltaRecord.recordEncode(ptr);

  // fields...
  for (uint i = 0; i < field_count; i++) {
    // field not update
    if (update_fields.find(i) == update_fields.end()) {
      deltaRecord.field_len_[i] = 0;
      continue;
    }
    deltaRecord.update_mask_.set(i);

    Field *f = update_fields[i];
    if (f == nullptr || f->is_null()) {
      deltaRecord.null_mask_.set(i);
      deltaRecord.field_len_[i] = 0;
      continue;
    }
    {  // resize
      size_t length;
      if (f->flags & BLOB_FLAG)
        length = dynamic_cast<Field_blob *>(f)->get_length();
      else
        length = f->row_pack_length();
      length += 8;
      size_t used = ptr - buf.get();
      if (buf_size - used < length) {
        while (buf_size - used < length) {
          buf_size *= 2;
          if (buf_size > utils::MappedCircularBuffer::MAX_BUF_SIZE)
            throw common::Exception(table_path + " Update data exceeds max buffer size " +
                                    std::to_string(utils::MappedCircularBuffer::MAX_BUF_SIZE));
        }
        std::unique_ptr<char[]> old_buf = std::move(buf);
        buf.reset(new char[buf_size]);
        std::memcpy(buf.get(), old_buf.get(), used);
        ptr = buf.get() + used;
      }
    }
    ptr = FiledToStr(ptr, f, &deltaRecord, i, thd);
    ASSERT(ptr <= buf.get() + buf_size, "Buffer overflow");
  }
  std::memcpy(buf.get() + deltaRecord.update_offset_, deltaRecord.update_mask_.data(),
              deltaRecord.update_mask_.data_size());
  std::memcpy(buf.get() + deltaRecord.null_offset_, deltaRecord.null_mask_.data(), deltaRecord.null_mask_.data_size());
  buf_size = ptr - buf.get();
}

void Engine::DecodeUpdateRecordToField(const char *ptr, Field **fields) {
  DeltaRecordHeadForUpdate deltaRecord;
  ptr = deltaRecord.recordDecode(ptr);
  for (uint i = 0; i < deltaRecord.field_count_; i++) {
    if (deltaRecord.update_mask_[i]) {
      auto field = fields[i];
      if (deltaRecord.null_mask_[i]) {
        field->set_null();
        continue;
      }
      field->set_notnull();
      ptr = StrToFiled(ptr, field, &deltaRecord, i);
    }
  }
}

void Engine::EncodeDeleteRecord(std::unique_ptr<char[]> &buf, uint32_t &buf_size) {
  buf_size = sizeof(RecordType) + sizeof(uint32_t);
  buf.reset(new char[buf_size]);
  char *ptr = buf.get();
  DeltaRecordHeadForDelete deltaRecord;
  ptr = deltaRecord.recordEncode(ptr);
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
    if (seq_file) {
      seq_file << 0;
      seq_file.flush();  // sync to disk mandatory.
    }

    if (!seq_file) {
      throw common::FileException("Failed to write to table id file");
    }

    return 0;  // fast return if it's a new-created tianmu.tid file.
  }

  uint32_t seq{0};
  std::fstream seq_file(p.string());
  if (seq_file)
    seq_file >> seq;
  if (!seq_file) {
    throw common::FileException("Failed to read from table id file");
  }
  seq++;
  seq_file.seekg(0);
  seq_file << seq;
  seq_file.flush();  // sync to disk mandatory.
  if (!seq_file) {
    throw common::FileException("Failed to write to table id file");
  }

  return seq;
}

std::shared_ptr<TableOption> Engine::GetTableOption(const std::string &table, TABLE *form,
                                                    HA_CREATE_INFO *create_info) {
  auto opt = std::make_shared<TableOption>();

  int power = has_pack(form->s->comment);
  if (power < 5 || power > 16) {
    TIANMU_LOG(LogCtl_Level::ERROR, "create table comment: pack size shift(%d) should be >=5 and <= 16");
    throw common::SyntaxException("Unexpected data pack size.");
  }

  opt->pss = power;

  for (uint i = 0; i < form->s->fields; ++i) {
    Field *f = form->field[i];
    opt->atis.push_back(Engine::GetAttrTypeInfo(*f));
  }

  opt->path = table + common::TIANMU_EXT;
  opt->name = form->s->table_name.str;
  opt->create_info = create_info;
  return opt;
}

void Engine::CreateTable(const std::string &table, TABLE *form, HA_CREATE_INFO *create_info) {
  TianmuTable::CreateNew(GetTableOption(table, form, create_info));
}

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
                               DTCollation(), fmt, filter, std::string(), field.flags & UNSIGNED_FLAG);
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
                     coll.collation->name);
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
    case MYSQL_TYPE_BIT: {
      const Field_bit_as_char *f_bit = ((const Field_bit_as_char *)&field);
      if (/*f_bit->field_length > 0 && */ f_bit->field_length <= common::TIANMU_BIT_MAX_PREC)
        return AttributeTypeInfo(common::ColumnType::BIT, notnull, f_bit->field_length);
      throw common::UnsupportedDataTypeException(
          "The bit(M) type, M must be less than or equal to 63 in tianmu engine.");
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
  /*
  Currently, the tianmu engine does not support manual transactions,
  and there is a problem with the current determination of automatic commit.
  After auto commit is turned off, tianmu will not commit the transaction.
  Therefore, it is modified to the default automatic commit statement level transaction.
  In the future, this logic will be modified after manual transactions are supported.

  if (all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT)) {
    GetTx(thd)->Commit(thd);
  }
  */
  GetTx(thd)->Commit(thd);
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

int Engine::DeleteTable(const char *table, [[maybe_unused]] THD *thd) {
  DBUG_ENTER("Engine::DeleteTable");

  {
    std::unique_lock<std::shared_mutex> mem_guard(mem_table_mutex);
    DeltaTable::DropDeltaTable(table);
    m_table_deltas.erase(table);
  }

  if (DeleteTableIndex(table, thd)) {
    TIANMU_LOG(LogCtl_Level::ERROR, "DeleteTable  failed");
    DBUG_RETURN(1);
  }
  DBUG_EXECUTE_IF("TIANMU_DELETE_TABLE_AFTER_INDEX", { DBUG_RETURN(1); });

  UnRegisterTable(table);
  std::string p = table;
  p += common::TIANMU_EXT;
  auto id = TianmuTable::GetTableId(p);
  cache.ReleaseTable(id);
  filter_cache.RemoveIf([id](const FilterCoordinate &c) { return c[0] == int(id); });

  {
    std::scoped_lock lk(gc_tasks_mtx);
    gc_tasks.remove_if([id](const purge_task &t) -> bool { return static_cast<uint32_t>(t.cookie) == id; });
  }
  system::DeleteDirectory(p);
  TIANMU_LOG(LogCtl_Level::INFO, "Drop table %s, ID = %u", table, id);

  DBUG_RETURN(0);
}

void Engine::TruncateTable(const std::string &table_path, [[maybe_unused]] THD *thd) {
  DBUG_ENTER("Engine::TruncateTable");

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

  DBUG_VOID_RETURN;
}

std::shared_ptr<TianmuTable> Engine::GetTableRD(const std::string &table_path) {
  return getTableShare(table_path)->GetSnapshot();
}

std::vector<AttrInfo> Engine::GetTableAttributesInfo(const std::string &table_path,
                                                     [[maybe_unused]] TABLE_SHARE *table_share) {
  std::scoped_lock guard(global_mutex_);

  std::shared_ptr<TianmuTable> tab;
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
  if (source_field->orig_table->s->db_type() == tianmu_hton) {  // do not use table (cont. default values)
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
  ASSERT(thd->ha_data[m_slot].ha_ptr == nullptr, "Nested transaction is not supported!");
  ASSERT(current_txn_ == nullptr, "Previous transaction is not finished!");

  current_txn_ = new Transaction(thd);
  thd->ha_data[m_slot].ha_ptr = current_txn_;

  AddTx(current_txn_);

  return current_txn_;
}

Transaction *Engine::GetTx(THD *thd) {
  if (thd->ha_data[m_slot].ha_ptr == nullptr)
    return CreateTx(thd);
  return static_cast<Transaction *>(thd->ha_data[m_slot].ha_ptr);
}

void Engine::ClearTx(THD *thd) {
  ASSERT(current_txn_ == (Transaction *)thd->ha_data[m_slot].ha_ptr, "Bad transaction");

  if (current_txn_ == nullptr)
    return;

  RemoveTx(current_txn_);
  current_txn_ = nullptr;
  thd->ha_data[m_slot].ha_ptr = nullptr;
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

std::string get_parameter_name(enum tianmu_param_name vn) {
  DEBUG_ASSERT(static_cast<int>(vn) >= 0 &&
               static_cast<int>(vn) <= static_cast<int>(tianmu_param_name::TIANMU_VAR_LIMIT));
  return tianmu_var_name_strings[static_cast<int>(vn)];
}

int get_parameter(THD *thd, enum tianmu_param_name vn, double &value) {
  std::string var_data = get_parameter_name(vn);
  user_var_entry *m_entry;
  my_bool null_val;

  m_entry = (user_var_entry *)my_hash_search(&thd->user_vars, (uchar *)var_data.c_str(), (uint)var_data.size());
  if (!m_entry)
    return 1;
  value = m_entry->val_real(&null_val);
  if (null_val)
    return 2;
  return 0;
}

int get_parameter(THD *thd, enum tianmu_param_name vn, int64_t &value) {
  std::string var_data = get_parameter_name(vn);
  user_var_entry *m_entry;
  my_bool null_val;

  m_entry = (user_var_entry *)my_hash_search(&thd->user_vars, (uchar *)var_data.c_str(), (uint)var_data.size());

  if (!m_entry)
    return 1;
  value = m_entry->val_int(&null_val);
  if (null_val)
    return 2;
  return 0;
}

int get_parameter(THD *thd, enum tianmu_param_name vn, std::string &value) {
  my_bool null_val;
  std::string var_data = get_parameter_name(vn);
  user_var_entry *m_entry;
  String str;

  m_entry = (user_var_entry *)my_hash_search(&thd->user_vars, (uchar *)var_data.c_str(), (uint)var_data.size());
  if (!m_entry)
    return 1;

  m_entry->val_str(&null_val, &str, NOT_FIXED_DEC);

  if (null_val)
    return 2;
  value = std::string(str.ptr());

  return 0;
}

int get_parameter(THD *thd, enum tianmu_param_name vn, longlong &result, std::string &s_result) {
  user_var_entry *m_entry;
  std::string var_data = get_parameter_name(vn);

  m_entry = (user_var_entry *)my_hash_search(&thd->user_vars, (uchar *)var_data.c_str(), (uint)var_data.size());
  if (!m_entry)
    return 1;

  if (m_entry->type() == DECIMAL_RESULT) {
    switch (vn) {
      case tianmu_param_name::TIANMU_ABORT_ON_THRESHOLD: {
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
      case tianmu_param_name::TIANMU_THROTTLE:
      case tianmu_param_name::TIANMU_TIANMUEXPRESSIONS:
      case tianmu_param_name::TIANMU_PARALLEL_AGGR:
      case tianmu_param_name::TIANMU_ABORT_ON_COUNT:
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

    if (vn == tianmu_param_name::TIANMU_DATAFORMAT || vn == tianmu_param_name::TIANMU_REJECT_FILE_PATH) {
      s_result = var_data;
    } else if (vn == tianmu_param_name::TIANMU_PIPEMODE) {
      boost::to_upper(var_data);
      if (var_data == "SERVER")
        result = 1;
      if (var_data == "CLIENT")
        result = 0;
    } else if (vn == tianmu_param_name::TIANMU_NULL) {
      s_result = var_data;
    }
    return 0;
  } else {
    result = 0;
    return 2;
  }
}

int Engine::RenameTable([[maybe_unused]] Transaction *trans_, const std::string &from, const std::string &to,
                        [[maybe_unused]] THD *thd) {
  UnRegisterTable(from);
  auto id = TianmuTable::GetTableId(from + common::TIANMU_EXT);
  cache.ReleaseTable(id);
  filter_cache.RemoveIf([id](const FilterCoordinate &c) { return c[0] == int(id); });
  system::RenameFile(tianmu_data_dir / (from + common::TIANMU_EXT), tianmu_data_dir / (to + common::TIANMU_EXT));
  RenameRdbTable(from, to);
  DBUG_EXECUTE_IF("delete tianmu primary key", DeleteTableIndex(from, thd););
  if (DeleteTableIndex(from, thd)) {
    TIANMU_LOG(LogCtl_Level::ERROR, "delete tianmu primary key failed.");
    return 1;
  }
  UnregisterDeltaTable(from, to);
  TIANMU_LOG(LogCtl_Level::INFO, "Rename table %s to %s", from.c_str(), to.c_str());
  return 0;
}

void Engine::PrepareAlterTable(const std::string &table_path, std::vector<Field *> &new_cols,
                               std::vector<Field *> &old_cols, [[maybe_unused]] THD *thd) {
  // make sure all deffered jobs are done in case some file gets recreated.
  HandleDeferredJobs();

  auto tab = current_txn_->GetTableByPath(table_path);
  TianmuTable::Alter(table_path, new_cols, old_cols, tab->NumOfObj());
  cache.ReleaseTable(tab->GetID());
  UnRegisterTable(table_path);
}

static void HandleDelayedLoad(uint32_t table_id, std::vector<std::unique_ptr<char[]>> &vec) {
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

  // fix issue 362: bug for insert into a table which table name contains special characters
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
  thd->set_current_time();
  thd_manager->add_thd(thd);
  mysql_reset_thd_for_next_command(thd);
  thd->init_for_queries();
  thd->set_db(dbname);
  // Forge LOAD DATA INFILE query which will be used in SHOW PROCESS LIST
  thd->set_query(loadquery);
  thd->set_query_id(next_query_id());
  thd->update_charset();  // for the charset change to take effect
  // Usually lex_start() is called by mysql_parse(), but we need
  // it here as the present method does not call mysql_parse().
  lex_start(thd);
  TABLE_LIST tl;
  tl.init_one_table(thd->strmake(thd->db().str, thd->db().length), thd->db().length, tabname.str, tabname.length,
                    tabname.str, TL_WRITE_CONCURRENT_INSERT);  // TIANMU UPGRADE
  tl.updating = 1;

  // the table will be opened in mysql_load
  thd->lex->sql_command = SQLCOM_LOAD;
  sql_exchange ex("buffered_insert", 0, FILETYPE_MEM);
  ex.file_name = const_cast<char *>(addr.c_str());
  ex.skip_lines = 0;
  ex.tab_id = table_id;
  thd->lex->select_lex->context.resolve_in_table_list_only(&tl);
  if (open_temporary_tables(thd, &tl)) {
    // error/////
  }
  List<Item> tmp_list;  // dummy
  if (mysql_load(thd, &ex, &tl, tmp_list, tmp_list, tmp_list, DUP_ERROR, false)) {
    thd->is_slave_error = 1;
  }

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

  free_root(thd->mem_root, MYF(MY_KEEP_PREALLOC));
  if (thd->is_fatal_error) {
    TIANMU_LOG(LogCtl_Level::ERROR, "LOAD DATA failed on table '%s'", tab_name.c_str());
  }

  thd->release_resources();
  thd_manager->remove_thd(thd);
  delete thd;

  my_thread_end();
}

void DistributeLoad(std::unordered_map<uint32_t, std::vector<std::unique_ptr<char[]>>> &tm) {
  utils::result_set<void> res;
  core::Engine *eng = reinterpret_cast<core::Engine *>(tianmu_hton->data);

  for (auto &it : tm) {
    res.insert(eng->bg_load_thread_pool.add_task(HandleDelayedLoad, it.first, std::ref(it.second)));
  }

  res.get_all();
  tm.clear();
}

void Engine::ProcessInsertBufferMerge() {
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

  std::unordered_map<uint32_t, std::vector<std::unique_ptr<char[]>>> tm;
  uint32_t buffer_recordnum = 0;
  uint32_t sleep_cnt = 0;
  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu merge insert_buffer thread start...");
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
    auto table_id = *(uint32_t *)rec.get();
    tm[table_id].emplace_back(std::move(rec));
    if (tm[table_id].size() > static_cast<std::size_t>(tianmu_sysvar_insert_max_buffered)) {
      // in case the ingress rate is too high
      DistributeLoad(tm);
      buffer_recordnum = 0;
    }
  }
  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu load thread exiting...");
}

void Engine::ProcessDeltaStoreMerge() {
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

  std::map<std::string, uint> sleep_cnts;
  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu merge delta store thread start...");
  core::Engine *eng = reinterpret_cast<core::Engine *>(tianmu_hton->data);

  while (!exiting) {
    if (!tianmu_sysvar_enable_rowstore) {
      std::unique_lock<std::mutex> lk(cv_merge_mtx);
      cv_merge.wait_for(lk, std::chrono::milliseconds(tianmu_sysvar_insert_wait_ms));
      continue;
    }

    std::unordered_map<uint32_t, std::vector<std::unique_ptr<char[]>>> need_merge_table;
    {
      try {
        std::scoped_lock guard(mem_table_mutex);
        for (auto &[name, delta_table] : m_table_deltas) {
          uint64_t record_count = delta_table->CountRecords();
          if ((record_count >= tianmu_sysvar_insert_numthreshold ||
               (sleep_cnts.count(name) && sleep_cnts[name] > tianmu_sysvar_insert_cntthreshold))) {
            auto share = eng->getTableShare(name);
            auto table_id = share->TabID();

            utils::BitSet null_mask(share->NumOfCols());

            std::unique_ptr<char[]> buf(new char[sizeof(uint32_t) + name.size() + 1 + null_mask.data_size()]);
            char *ptr = buf.get();
            *(uint32_t *)ptr = table_id;  // table id
            ptr += sizeof(uint32_t);

            std::memcpy(ptr, name.c_str(), name.size());
            ptr += name.size();

            *ptr++ = 0;  // end with NUL

            std::memcpy(ptr, null_mask.data(), null_mask.data_size());
            need_merge_table[table_id].emplace_back(std::move(buf));
            sleep_cnts[name] = 0;
          } else if (record_count > 0) {
            if (sleep_cnts.count(name))
              sleep_cnts[name]++;
            else
              sleep_cnts[name] = 0;
          }
        }
      } catch (common::Exception &e) {
        TIANMU_LOG(LogCtl_Level::ERROR, "Tianmu merge delta store  failed. %s %s", e.what(), e.trace().c_str());
        std::unique_lock<std::mutex> lk(cv_merge_mtx);
        cv_merge.wait_for(lk, std::chrono::milliseconds(tianmu_sysvar_insert_wait_ms));
        continue;
      } catch (...) {
        TIANMU_LOG(LogCtl_Level::ERROR, "Tianmu merge delta store failed.");
        std::unique_lock<std::mutex> lk(cv_merge_mtx);
        cv_merge.wait_for(lk, std::chrono::milliseconds(tianmu_sysvar_insert_wait_ms));
        continue;
      }
    }

    if (!need_merge_table.empty())
      DistributeLoad(need_merge_table);
    else {
      std::unique_lock<std::mutex> lk(cv_merge_mtx);
      cv_merge.wait_for(lk, std::chrono::milliseconds(tianmu_sysvar_insert_wait_ms));
    }
  }

  TIANMU_LOG(LogCtl_Level::INFO, "Tianmu merge delta store thread exiting...");
}

void Engine::LogStat() {
  static long last_sample_time = 0;
  static query_id_t saved_query_id = 0;
  static TianmuStat saved;

  if (last_sample_time == 0) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    last_sample_time = t.tv_sec;
    saved_query_id = global_query_id;
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
  query_id_t query_id = global_query_id;
  long queries = query_id - saved_query_id;

  {
    static ulong saved_com_stat[SQLCOM_END];

    // commands we are interested in
    static const enum_sql_command cmds[] = {
        SQLCOM_SELECT,        SQLCOM_CREATE_TABLE, SQLCOM_ALTER_TABLE, SQLCOM_UPDATE,     SQLCOM_INSERT,
        SQLCOM_INSERT_SELECT, SQLCOM_DELETE,       SQLCOM_TRUNCATE,    SQLCOM_DROP_TABLE, SQLCOM_LOAD,
    };

    STATUS_VAR sv;
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
    msg = msg + "queries " + std::to_string(queries) + "/" + std::to_string(global_query_id);
    TIANMU_LOG(LogCtl_Level::INFO, msg.c_str());
  }

  {
    const auto &&mem_info = MemoryStatisticsOS::Instance()->GetMemInfo();

    uint64_t mem_available = mem_info.mem_available;
    uint64_t swap_used = mem_info.swap_used;
    int64_t mem_available_chg = mem_available - m_mem_available_;
    int64_t swap_used_chg = swap_used - m_swap_used_;
    m_mem_available_ = mem_available;
    m_swap_used_ = swap_used;

    TIANMU_LOG(LogCtl_Level::INFO, "mem_available_chg: %ld swap_used_chg: %ld", mem_available_chg, swap_used_chg);

    memory_statistics_record("HEATBEAT", "UPDATE");
  }

  TIANMU_LOG(LogCtl_Level::DEBUG,
             "Select: %lu/%lu, "
             "Loaded: %lu/%lu(%lu/%lu), "
             "dup: %lu/%lu, "
             "delta insert: %lu/%lu, "
             "failed delta insert: %lu/%lu, "
             "delta update: %lu/%lu, "
             "failed delta update: %lu/%lu, "
             "delta delete: %lu/%lu, "
             "failed delta delete: %lu/%lu, "
             "update: %lu/%lu",
             tianmu_stat.select - saved.select, tianmu_stat.select, tianmu_stat.loaded - saved.loaded,
             tianmu_stat.loaded, tianmu_stat.load_cnt - saved.load_cnt, tianmu_stat.load_cnt,
             tianmu_stat.loaded_dup - saved.loaded_dup, tianmu_stat.loaded_dup,
             tianmu_stat.delta_insert - saved.delta_insert, tianmu_stat.delta_insert,
             tianmu_stat.failed_delta_insert - saved.failed_delta_insert, tianmu_stat.failed_delta_insert,
             tianmu_stat.delta_update - saved.delta_update, tianmu_stat.delta_update,
             tianmu_stat.failed_delta_update - saved.failed_delta_update, tianmu_stat.failed_delta_update,
             tianmu_stat.delta_insert - saved.delta_insert, tianmu_stat.delta_insert,
             tianmu_stat.failed_delta_delete - saved.failed_delta_delete, tianmu_stat.failed_delta_delete,
             tianmu_stat.update - saved.update, tianmu_stat.update);

  if (tianmu_stat.loaded == saved.loaded && tianmu_stat.delta_insert > saved.delta_insert) {
    TIANMU_LOG(LogCtl_Level::ERROR, "No data loaded from delta store");  // why this log? need add update delete?
  }

  // update with last minute statistics
  IPM = tianmu_stat.delta_insert - saved.delta_insert;
  IT = tianmu_stat.delta_insert;
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

void Engine::InsertDelayed(const std::string &table_path, TABLE *table) {
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
  std::shared_ptr<void> defer(nullptr,
                              [table, org_bitmap](...) { dbug_tmp_restore_column_map(table->read_set, org_bitmap); });

  uint32_t buf_sz = 0;
  std::unique_ptr<char[]> buf;
  EncodeInsertRecord(table_path, table->field, table->s->fields, table->s->blob_fields, buf, buf_sz, table->in_use);

  unsigned int failed = 0;
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

int Engine::InsertToDelta(const std::string &table_path, std::shared_ptr<TableShare> &share, TABLE *table) {
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
  std::shared_ptr<void> defer(nullptr,
                              [table, org_bitmap](...) { dbug_tmp_restore_column_map(table->read_set, org_bitmap); });
  auto tm_table = share->GetSnapshot();
  uint64_t row_id = tm_table->NextRowId();
  // Insert primary key first
  int ret = tm_table->InsertIndexForDelta(table, row_id);
  if (ret != 0)
    return ret;
  // check & encode
  uint32_t buf_sz = 0;
  std::unique_ptr<char[]> buf;
  EncodeInsertRecord(table_path, table->field, table->s->fields, table->s->blob_fields, buf, buf_sz, table->in_use);
  // insert to delta
  tm_table->InsertToDelta(row_id, std::move(buf), buf_sz);
  return ret;
}

void Engine::UpdateToDelta(const std::string &table_path, std::shared_ptr<TableShare> &share, TABLE *table,
                           uint64_t row_id, const uchar *old_data, uchar *new_data) {
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
  std::shared_ptr<void> defer(nullptr,
                              [table, org_bitmap](...) { dbug_tmp_restore_column_map(table->read_set, org_bitmap); });
  auto tm_table = share->GetSnapshot();
  auto path = share->Path();
  std::shared_ptr<index::TianmuTableIndex> tab = GetTableIndex(path);
  std::unordered_map<uint, Field *> update_fields;
  for (uint col_id = 0; col_id < table->s->fields; col_id++) {
    bool col_is_index = false;
    if (!bitmap_is_set(table->write_set, col_id)) {
      continue;
    }
    auto field = table->field[col_id];
    // Determine whether the column is a primary key
    if (tab) {
      std::vector<uint> keycols = tab->KeyCols();
      if (std::find(keycols.begin(), keycols.end(), col_id) != keycols.end()) {
        col_is_index = true;
      }
    }

    if (field->real_maybe_null()) {
      if (field->is_null_in_record(old_data) && field->is_null_in_record(new_data)) {
        continue;
      }
      if (field->is_null_in_record(new_data)) {
        update_fields.emplace(col_id, nullptr);  // set to null
        if (col_is_index) {
          tm_table->UpdateIndexForDelta(table, row_id, col_id);
        }
        continue;
      }
    }
    auto o_ptr = field->ptr - table->record[0] + old_data;
    auto n_ptr = field->ptr - table->record[0] + new_data;
    if (field->is_null_in_record(old_data) || std::memcmp(o_ptr, n_ptr, field->pack_length()) != 0) {
      update_fields.emplace(col_id, field);
      if (col_is_index) {
        tm_table->UpdateIndexForDelta(table, row_id, col_id);
      }
    }
  }

  uint32_t buf_sz = 0;
  std::unique_ptr<char[]> buf;
  EncodeUpdateRecord(table_path, update_fields, table->s->fields, table->s->blob_fields, buf, buf_sz, table->in_use);

  tm_table->UpdateToDelta(row_id, std::move(buf), buf_sz);
}

void Engine::DeleteToDelta(std::shared_ptr<TableShare> &share, TABLE *table, uint64_t row_id) {
  auto tm_table = share->GetSnapshot();
  tm_table->DeleteIndexForDelta(table, row_id);

  uint32_t buf_sz = 0;
  std::unique_ptr<char[]> buf;
  EncodeDeleteRecord(buf, buf_sz);
  tm_table->DeleteToDelta(row_id, std::move(buf), buf_sz);
}

int Engine::InsertRow(const std::string &table_path, [[maybe_unused]] Transaction *trans_, TABLE *table,
                      std::shared_ptr<TableShare> &share) {
  int ret = 0;
  try {
    if (tianmu_sysvar_insert_delayed && table->s->tmp_table == NO_TMP_TABLE) {
      if (tianmu_sysvar_enable_rowstore) {
        ret = InsertToDelta(table_path, share, table);
      } else {
        InsertDelayed(table_path, table);
      }
      tianmu_stat.delta_insert++;
    } else {
      current_txn_->SetLoadSource(common::LoadSource::LS_Direct);
      auto rct = current_txn_->GetTableByPath(table_path);
      ret = rct->Insert(table);
    }
    return ret;
  } catch (common::DupKeyException &e) {
    ret = HA_ERR_FOUND_DUPP_KEY;
    TIANMU_LOG(LogCtl_Level::ERROR, "delayed inserting failed: %s", e.what());
  } catch (common::Exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "delayed inserting failed. %s %s", e.what(), e.trace().c_str());
  } catch (std::exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "delayed inserting failed. %s", e.what());
  } catch (...) {
    TIANMU_LOG(LogCtl_Level::ERROR, "delayed inserting failed.");
  }

  if (tianmu_sysvar_insert_delayed) {
    tianmu_stat.failed_delta_insert++;
    ret = 1;
  }

  return ret;
}

int Engine::UpdateRow(const std::string &table_path, TABLE *table, std::shared_ptr<TableShare> &share, uint64_t row_id,
                      const uchar *old_data, uchar *new_data) {
  // DBUG_ENTER("Engine::UpdateRow");
  int ret = 0;
  if (tianmu_sysvar_insert_delayed && table->s->tmp_table == NO_TMP_TABLE && tianmu_sysvar_enable_rowstore) {
    UpdateToDelta(table_path, share, table, row_id, old_data, new_data);
    tianmu_stat.delta_update++;
  } else {
    auto tm_table = current_txn_->GetTableByPath(table_path);
    ret = tm_table->Update(table, row_id, old_data, new_data);
  }
  return ret;
  // DBUG_RETURN(ret);
}

int Engine::DeleteRow(const std::string &table_path, TABLE *table, std::shared_ptr<TableShare> &share,
                      uint64_t row_id) {
  DBUG_ENTER("Engine::DeleteRow");
  int ret = 0;
  if (tianmu_sysvar_insert_delayed && table->s->tmp_table == NO_TMP_TABLE && tianmu_sysvar_enable_rowstore) {
    DeleteToDelta(share, table, row_id);
    tianmu_stat.delta_delete++;
  } else {
    auto tm_table = current_txn_->GetTableByPath(table_path);
    ret = tm_table->Delete(table, row_id);
  }

  DBUG_RETURN(ret);
}

common::TianmuError Engine::RunLoader(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, void *arg) {
  common::TianmuError tianmu_e;
  TABLE *table;
  int transactional_table = 0;

  table = table_list->table;
  transactional_table = table->file->has_transactions();

  try {
    std::unique_ptr<system::IOParameters> iop;

    auto tianmu_error = Engine::GetIOP(iop, *thd, *ex, table, arg);

    if (tianmu_error != common::ErrorCode::SUCCESS)
      throw tianmu_error;

    std::string table_path = GetTablePath(table);

    table->copy_blobs = 0;
    thd->cuted_fields = 0L;

    auto tab = current_txn_->GetTableByPath(table_path);

    // Maybe not good to put this code here,just for temp.
    bitmap_set_all(table->read_set);
    bitmap_set_all(table->write_set);
    thd->count_cuted_fields = CHECK_FIELD_WARN;
    tab->LoadDataInfile(*iop);
    thd->count_cuted_fields = CHECK_FIELD_IGNORE;
    if (current_txn_->Killed()) {
      thd->send_kill_message();
      throw common::TianmuError(common::ErrorCode::KILLED);
    }

    // We must invalidate the table in query cache before binlog writing and
    // ha_autocommit_...
    query_cache.invalidate(thd, table_list, 0);

    COPY_INFO::Statistics stats;
    stats.records = ha_rows(tab->NoRecordsLoaded());
    tianmu_stat.loaded += tab->NoRecordsLoaded();
    tianmu_stat.loaded_dup += tab->NoRecordsDuped();
    tianmu_stat.load_cnt++;

    char name[FN_REFLEN];
    my_snprintf(name, sizeof(name), ER(ER_LOAD_INFO), (long)stats.records,
                0,  // deleted
                0,  // skipped
                (long)thd->get_stmt_da()->current_statement_cond_count());

    /* ok to client */
    my_ok(thd, stats.records, 0L, name);
  } catch (common::FormatException &e) {
    tianmu_e = common::TianmuError(common::ErrorCode::DATA_ERROR, e.what());
  } catch (common::NetStreamException &e) {
    tianmu_e = common::TianmuError(common::ErrorCode::FAILED, e.what());
  } catch (common::Exception &e) {
    tianmu_e = common::TianmuError(common::ErrorCode::UNKNOWN_ERROR, std::string("Tianmu internal error:") + e.what());
  } catch (common::TianmuError &e) {
    tianmu_e = e;
  }

  // if (thd->transaction.stmt.cannot_safely_rollback())
  //     thd->transaction.all.mark_modified_non_trans_table();

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

bool Engine::IsTIANMURoute(THD *thd, TABLE_LIST *table_list, SELECT_LEX *selects_list,
                           int &in_case_of_failure_can_go_to_mysql, int with_insert) {
  in_case_of_failure_can_go_to_mysql = true;

  if (!table_list)
    return false;
  if (with_insert)
    table_list = table_list->next_global ? table_list->next_global : *(table_list->prev_global);  // we skip one

  if (!table_list)
    return false;

  bool has_TIANMUTable = false;
  for (TABLE_LIST *tl = table_list; tl; tl = tl->next_global) {  // SZN:we go through tables
    if (!tl->is_view_or_derived() && !tl->is_view()) {
      // In this list we have all views, derived tables and their
      // sources, so anyway we walk through all the source tables
      // even though we seem to reject the control of views
      if (!IsTianmuTable(tl->table))
        return false;
      else
        has_TIANMUTable = true;
    }
  }
  if (!has_TIANMUTable)  // No Tianmu table is involved. Return to MySQL.
    return false;

  // then we check the parameter of file format.
  // if it is MYSQL_format AND we write to a file, it is a MYSQL route.
  int is_dump;
  const char *file = GetFilename(selects_list, is_dump);

  if (file) {  // it writes to a file
    longlong param = 0;
    std::string s_res;
    if (!get_parameter(thd, tianmu_param_name::TIANMU_DATAFORMAT, param, s_res)) {
      if (boost::iequals(boost::trim_copy(s_res), "MYSQL"))
        return false;

      common::DataFormatPtr df = common::DataFormat::GetDataFormat(s_res);
      if (!df) {  // parameter is UNKNOWN VALUE
        my_message(ER_SYNTAX_ERROR, "Histgore specific error: Unknown value of TIANMU_DATAFORMAT parameter", MYF(0));
        return true;
      } else if (!df->CanExport()) {
        my_message(ER_SYNTAX_ERROR,
                   (std::string("Tianmu specific error: Export in '") + df->GetName() + ("' format is not supported."))
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

bool Engine::IsTianmuTable(TABLE *table) {
  return table && table->s->db_type() == tianmu_hton;  // table->db_type is always nullptr
}

const char *Engine::GetFilename(SELECT_LEX *selects_list, int &is_dumpfile) {
  // if the function returns a filename <> nullptr
  // additionally is_dumpfile indicates whether it was 'select into OUTFILE' or
  // maybe 'select into DUMPFILE' if the function returns nullptr it was a regular
  // 'select' don't look into is_dumpfile in this case
  if (selects_list->parent_lex->exchange) {
    is_dumpfile = selects_list->parent_lex->exchange->dumpfile;
    return selects_list->parent_lex->exchange->file_name;
  }
  return 0;
}

std::unique_ptr<system::IOParameters> Engine::CreateIOParameters(const std::string &path, void *arg) {
  if (path.empty())
    return std::unique_ptr<system::IOParameters>(new system::IOParameters());

  std::string data_dir;
  std::string data_path;
  core::Engine *eng = reinterpret_cast<core::Engine *>(tianmu_hton->data);

  if (fs::path(path).is_absolute()) {
    data_dir = "";
    data_path = path;
  } else {
    data_dir = eng->tianmu_data_dir;
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

  if (calc_time_diff(&utc, &client_zone, 1, &secs, &msecs))
    sign = -1;

  minutes = (short)(secs / 60);
}

common::TianmuError Engine::GetRejectFileIOParameters(THD &thd, std::unique_ptr<system::IOParameters> &io_params) {
  std::string reject_file;
  int64_t abort_on_count = 0;
  double abort_on_threshold = 0;

  get_parameter(&thd, tianmu_param_name::TIANMU_REJECT_FILE_PATH, reject_file);
  if (get_parameter(&thd, tianmu_param_name::TIANMU_REJECT_FILE_PATH, reject_file) == 2)
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER, "Wrong value of TIANMU_LOAD_REJECT_FILE parameter.");

  if (get_parameter(&thd, tianmu_param_name::TIANMU_ABORT_ON_COUNT, abort_on_count) == 2)
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER, "Wrong value of TIANMU_ABORT_ON_COUNT parameter.");

  if (get_parameter(&thd, tianmu_param_name::TIANMU_ABORT_ON_THRESHOLD, abort_on_threshold) == 2)
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

common::TianmuError Engine::GetIOP(std::unique_ptr<system::IOParameters> &io_params, THD &thd, sql_exchange &ex,
                                   TABLE *table, void *arg, bool for_exporter) {
  const CHARSET_INFO *cs = ex.cs;
  bool local_load = for_exporter ? false : (bool)(thd.lex)->local_file;
  uint value_list_elements = (thd.lex)->load_value_list.elements;
  // thr_lock_type lock_option = (thd.lex)->lock_option;

  int io_mode = -1;
  char name[FN_REFLEN];
  char *tdb = 0;
  if (table) {
    tdb = table->s->db.str ? table->s->db.str : (char *)thd.db().str;
  } else
    tdb = (char *)thd.db().str;

  io_params = CreateIOParameters(&thd, table, arg);

  short sign, minutes;
  ComputeTimeZoneDiffInMinutes(&thd, sign, minutes);
  io_params->SetTimeZone(sign, minutes);
  io_params->SetTHD(&thd);
  io_params->SetTable(table);
  if (ex.filetype == FILETYPE_MEM) {
    io_params->load_delayed_insert_ = true;
  }
#ifdef DONT_ALLOW_FULL_LOAD_DATA_PATTIANMU
  ex->file_name += dirname_length(ex->file_name);
#endif

  longlong param = 0;
  std::string s_res;
  if (common::DataFormat::GetNoFormats() > 1) {
    if (!get_parameter(&thd, tianmu_param_name::TIANMU_DATAFORMAT, param, s_res)) {
      common::DataFormatPtr df = common::DataFormat::GetDataFormat(s_res);
      if (!df)
        return common::TianmuError(common::ErrorCode::WRONG_PARAMETER, "Unknown value of TIANMU_DATAFORMAT parameter.");
      else
        io_mode = df->GetId();
    } else
      io_mode = common::DataFormat::GetDataFormat(0)->GetId();
  } else
    io_mode = common::DataFormat::GetDataFormat(0)->GetId();

  if (!get_parameter(&thd, tianmu_param_name::TIANMU_NULL, param, s_res))
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

    if (ex.field.field_term->ptr() && strlen(ex.field.field_term->ptr()))
      io_params->SetDelimiter(ex.field.field_term->ptr());

    if (ex.line.line_term->ptr() && strlen(ex.line.line_term->ptr()))
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
#if 0
  if (io_params->EscapeCharacter() != 0 && io_params->StringQualifier() != 0 &&
      io_params->EscapeCharacter() == io_params->StringQualifier())
    return common::TianmuError(common::ErrorCode::WRONG_PARAMETER,
                               "The same enclose and escape characters not supported.");
#endif
  bool unsupported_syntax = false;
  if (cs != 0)
    io_params->SetParameter(system::Parameter::CHARSET_INFO_NUMBER, (int)(cs->number));
  else if (!for_exporter)
    io_params->SetParameter(system::Parameter::CHARSET_INFO_NUMBER,
                            (int)(thd.variables.collation_database->number));  // default charset

  if (ex.skip_lines != 0) {
    io_params->SetParameter(system::Parameter::SKIP_LINES, (int64_t)ex.skip_lines);
  }

  if (ex.tab_id != 0) {
    io_params->SetParameter(system::Parameter::TABLE_ID, static_cast<int64_t>(ex.tab_id));
  }

  if (ex.line.line_start != 0 && ex.line.line_start->length() != 0) {
    unsupported_syntax = true;
    io_params->SetParameter(system::Parameter::LINE_STARTER, std::string(ex.line.line_start->ptr()));
  }

  if (local_load && ((thd.lex)->sql_command == SQLCOM_LOAD)) {
    io_params->SetParameter(system::Parameter::LOCAL_LOAD, (int)local_load);
  }

  if (value_list_elements != 0) {
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
  } catch (common::DatabaseException &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to create table share: %s", e.getExceptionMsg());
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
  if (!index::TianmuTableIndex::FindIndexTable(table_path)) {
    index::TianmuTableIndex::CreateIndexTable(table_path, table);
  }
  auto iter = m_table_keys.find(table_path);
  if (iter == m_table_keys.end()) {
    std::shared_ptr<index::TianmuTableIndex> tab = std::make_shared<index::TianmuTableIndex>(table_path, table);
    m_table_keys[table_path] = tab;
  }
}

bool Engine::DeleteTableIndex(const std::string &table_path, [[maybe_unused]] THD *thd) {
  if (table_path.empty() || thd == nullptr) {
    return true;
  }

  std::unique_lock<std::shared_mutex> index_guard(tables_keys_mutex);
  if (index::TianmuTableIndex::FindIndexTable(table_path)) {
    index::TianmuTableIndex::DropIndexTable(table_path);
  }

  if (m_table_keys.find(table_path) != m_table_keys.end()) {
    m_table_keys.erase(table_path);
  }

  return false;
}

std::shared_ptr<index::TianmuTableIndex> Engine::GetTableIndex(const std::string &table_path) {
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

void Engine::AddTableDelta(TABLE *form, std::shared_ptr<TableShare> share) {
  std::scoped_lock guard(mem_table_mutex);
  try {
    auto table_path = share->Path();
    if (m_table_deltas.find(table_path) == m_table_deltas.end()) {
      m_table_deltas[table_path] = DeltaTable::CreateDeltaTable(share, has_mem_name(form->s->comment));
      uint64 base_row_num = share->GetSnapshot()->NumOfObj();
      m_table_deltas[table_path]->Init(base_row_num);
      return;
    }
    return;
  } catch (common::Exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to create delta table: %s / %s", e.what(), e.trace().c_str());
  } catch (std::exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to create delta table: %s", e.what());
  } catch (...) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to create delta table");
  }
  return;
}

void Engine::UnregisterDeltaTable(const std::string &from, const std::string &to) {
  std::scoped_lock guard(mem_table_mutex);
  auto iter = m_table_deltas.find(from);
  if (iter != m_table_deltas.end()) {
    auto delta_table = iter->second;
    delta_table->Rename(to);
    m_table_deltas.erase(iter);
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
std::string Engine::DeltaStoreStat() {
  uint64_t write_cnt = 0;
  uint64_t write_bytes = 0;
  uint64_t read_cnt = 0;
  uint64_t read_bytes = 0;
  uint64_t delta_cnt = 0;
  uint64_t delta_bytes = 0;
  std::scoped_lock guard(mem_table_mutex);
  for (auto &iter : m_table_deltas) {
    auto delta_table = iter.second;
    write_cnt += delta_table->stat.write_cnt.load();
    write_bytes += delta_table->stat.write_bytes.load();
    read_cnt += delta_table->stat.read_cnt;
    read_bytes += delta_table->stat.read_bytes;
    if (delta_table->stat.write_cnt > delta_table->stat.read_cnt)
      delta_cnt += (delta_table->stat.write_cnt - delta_table->stat.read_cnt);
    if (delta_table->stat.write_bytes > delta_table->stat.read_bytes)
      delta_bytes += (delta_table->stat.write_bytes - delta_table->stat.read_bytes);
  }

  return "w:" + std::to_string(write_cnt) + "/" + std::to_string(write_bytes) + ", r:" + std::to_string(read_cnt) +
         "/" + std::to_string(read_bytes) + " delta: " + std::to_string(delta_cnt) + "/" + std::to_string(delta_bytes);
}


/**
 * generate delta sync info on show engine query
 * @param [out] buf  output stream
 * @param filter_set table name filter, empty set means no filter
 * @return number of queried delta tables
 */
size_t Engine::GetDeltaSyncStats(std::ostringstream &buf, std::unordered_set<std::string> &filter_set) {
  std::vector<std::shared_ptr<DeltaTable>> table_list;
  for(const auto& delta:m_table_deltas){
    if(filter_set.empty() || filter_set.find(delta.second->FullName())!=filter_set.end()){
      table_list.push_back(delta.second);
    }
  }
  buf << table_list.size() << " table(s) matched in the delta sync-stat query:" << std::endl;
  for(auto& table: table_list){
    buf << "table name: " << table->FullName()
        << ", delta table id: " << table->GetDeltaTableID()
        << ", current load id: " << table->load_id.load()
        << ", merge id: " << table->merge_id.load()
        <<", current row_id: " << table->row_id.load() << std::endl;
  }
  return table_list.size();
}

  }  // namespace core
}  // namespace Tianmu

int tianmu_push_data_dir(const char *dir) {
  using namespace Tianmu;
  using namespace core;
  try {
    auto p = fs::path(dir);
    if (!fs::is_directory(p)) {
      TIANMU_LOG(LogCtl_Level::ERROR, "Path %s is not a directory or cannot be accessed.", dir);
      return 1;
    }

    if (fs::space(p).available < 1_GB) {
      TIANMU_LOG(LogCtl_Level::ERROR, "Tianmu requires data directory has at least 1G available space!");
      return 1;
    }
    p /= fs::path(TIANMU_DATA_DIR);
    auto result = std::find(std::begin(tianmu_data_dirs), std::end(tianmu_data_dirs), p);
    if (result != std::end(tianmu_data_dirs)) {
      TIANMU_LOG(LogCtl_Level::WARN, "Path %s specified multiple times as data directory.", dir);
    } else {
      fs::create_directory(p);
      tianmu_data_dirs.emplace_back(p);
    }
  } catch (fs::filesystem_error &err) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Filesystem error %s", err.what());
    return 1;
  }

  return 0;
}
