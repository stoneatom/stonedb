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
#include <arpa/inet.h>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "core/transaction.h"
#include "handler/tianmu_handler.h"
#include "mm/initializer.h"
#include "system/file_out.h"
#include "binlog.h"

handlerton *rcbase_hton;

struct st_mysql_sys_var {
  MYSQL_PLUGIN_VAR_HEADER;
};

namespace Tianmu {
namespace dbhandler {

/*
 If frm_error() is called then we will use this to to find out what file
 extentions exist for the storage engine. This is also used by the default
 rename_table and delete_table method in handler.cc.
 */
my_bool tianmu_bootstrap = 0;

char *strmov_str(char *dst, const char *src)
{
  while ((*dst++ = *src++)) ;
  return dst-1;
}

static int rcbase_done_func([[maybe_unused]] void *p) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  if (rceng) {
    delete rceng;
    rceng = nullptr;
  }
  if (kvstore) {
    delete kvstore;
    kvstore = nullptr;
  }

  DBUG_RETURN(0);
}

handler *rcbase_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root) {
  return new (mem_root) TianmuHandler(hton, table);
}

int rcbase_panic_func([[maybe_unused]] handlerton *hton, enum ha_panic_function flag) {
  if (tianmu_bootstrap) return 0;

  if (flag == HA_PANIC_CLOSE) {
    delete rceng;
    rceng = nullptr;
    delete kvstore;
    kvstore = nullptr;
  }
  return 0;
}

int rcbase_rollback([[maybe_unused]] handlerton *hton, THD *thd, bool all) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = 1;
  try {
    rceng->Rollback(thd, all);
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception error is caught: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

int rcbase_close_connection(handlerton *hton, THD *thd) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = 1;
  try {
    rcbase_rollback(hton, thd, true);
    rceng->ClearTx(thd);
    ret = 0;
  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception error is caught: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

int rcbase_commit([[maybe_unused]] handlerton *hton, THD *thd, bool all) {
  DBUG_ENTER(__PRETTY_FUNCTION__);

  int ret = 1;
  std::string error_message;

  if (!(thd->no_errors != 0 || thd->killed || thd->transaction_rollback_request)) {
    try {
      rceng->CommitTx(thd, all);
      ret = 0;
    } catch (std::exception &e) {
      error_message = std::string("Error: ") + e.what();
    } catch (...) {
      error_message = std::string(__func__) + " An unknown system exception error caught.";
    }
  }

  if (ret) {
    try {
      rceng->Rollback(thd, all, true);
      if (!error_message.empty()) {
        TIANMU_LOG(LogCtl_Level::ERROR, "%s", error_message.c_str());
        my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), error_message.c_str(), MYF(0));
      }
    } catch (std::exception &e) {
      if (!error_message.empty()) {
        TIANMU_LOG(LogCtl_Level::ERROR, "%s", error_message.c_str());
        my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), error_message.c_str(), MYF(0));
      }
      my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR),
                 (std::string("Failed to rollback transaction. Error: ") + e.what()).c_str(), MYF(0));
      TIANMU_LOG(LogCtl_Level::ERROR, "Failed to rollback transaction. Error: %s.", e.what());
    } catch (...) {
      if (!error_message.empty()) {
        TIANMU_LOG(LogCtl_Level::ERROR, "%s", error_message.c_str());
        my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), error_message.c_str(), MYF(0));
      }
      my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "Failed to rollback transaction. Unknown error.",
                 MYF(0));
      TIANMU_LOG(LogCtl_Level::ERROR, "Failed to rollback transaction. Unknown error.");
    }
  }

  DBUG_RETURN(ret);
}

bool rcbase_show_status([[maybe_unused]] handlerton *hton, THD *thd, stat_print_fn *pprint, enum ha_stat_type stat) {
  const static char *hton_name = "TIANMU";

  std::ostringstream buf(std::ostringstream::out);

  buf << std::endl;

  if (stat == HA_ENGINE_STATUS) {
    mm::TraceableObject::Instance()->HeapHistogram(buf);
    return pprint(thd, hton_name, uint(std::strlen(hton_name)), "Heap Histograms", uint(std::strlen("Heap Histograms")),
                  buf.str().c_str(), uint(buf.str().length()));
  }
  return false;
}

extern my_bool tianmu_bootstrap;

static int init_variables() {
  opt_binlog_order_commits = false;
  return 0;
}

int rcbase_init_func(void *p) {
  DBUG_ENTER(__PRETTY_FUNCTION__);
  rcbase_hton = (handlerton *)p;

  if (init_variables()) {
    DBUG_RETURN(1);
  }

  rcbase_hton->state = SHOW_OPTION_YES;
  rcbase_hton->db_type = DB_TYPE_TIANMU;
  rcbase_hton->create = rcbase_create_handler;
  rcbase_hton->flags = HTON_NO_FLAGS;
  rcbase_hton->panic = rcbase_panic_func;
  rcbase_hton->close_connection = rcbase_close_connection;
  rcbase_hton->commit = rcbase_commit;
  rcbase_hton->rollback = rcbase_rollback;
  rcbase_hton->show_status = rcbase_show_status;

  // When mysqld runs as bootstrap mode, we do not need to initialize
  // memmanager.
  if (tianmu_bootstrap) DBUG_RETURN(0);

  int ret = 1;
  rceng = NULL;

  try {
    std::string log_file = mysql_home_ptr;
    log_setup(log_file + "/log/tianmu.log");
    rccontrol.addOutput(new system::FileOut(log_file + "/log/trace.log"));
    rcquerylog.addOutput(new system::FileOut(log_file + "/log/query.log"));
    struct hostent *hent = NULL;
    hent = gethostbyname(glob_hostname);
    if (hent) strmov_str(glob_hostip, inet_ntoa(*(struct in_addr *)(hent->h_addr_list[0])));
    my_snprintf(glob_serverInfo, sizeof(glob_serverInfo), "\tServerIp:%s\tServerHostName:%s\tServerPort:%d",
                glob_hostip, glob_hostname, mysqld_port);
    kvstore = new index::KVStore();
    kvstore->Init();
    rceng = new core::Engine();
    ret = rceng->Init(total_ha);
    {
      TIANMU_LOG(LogCtl_Level::INFO,
                  "\n"
                  "------------------------------------------------------------"
                  "----------------------------------"
                  "-------------\n"
                  "    ######  ########  #######  ##     ## ######## ######   "
                  "######   \n"
                  "   ##    ##    ##    ##     ## ####   ## ##       ##    ## "
                  "##    ## \n"
                  "   ##          ##    ##     ## ## ##  ## ##       ##    ## "
                  "##    ## \n"
                  "    ######     ##    ##     ## ##  ## ## ######   ##    ## "
                  "######## \n"
                  "         ##    ##    ##     ## ##   #### ##       ##    ## "
                  "##    ## \n"
                  "   ##    ##    ##    ##     ## ##    ### ##       ##    ## "
                  "##    ## \n"
                  "    ######     ##     #######  ##     ## ######## ######   "
                  "######   \n"
                  "------------------------------------------------------------"
                  "----------------------------------"
                  "-------------\n");
    }

  } catch (std::exception &e) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), e.what(), MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An exception error is caught: %s.", e.what());
  } catch (...) {
    my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), "An unknown system exception error caught.", MYF(0));
    TIANMU_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
  }

  DBUG_RETURN(ret);
}

struct st_mysql_storage_engine tianmu_storage_engine = {MYSQL_HANDLERTON_INTERFACE_VERSION};

int get_DelayedBufferUsage_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_CHAR;
  var->value = buff;
  std::string str = rceng->DelayedBufferStat();
  std::memcpy(buff, str.c_str(), str.length() + 1);
  return 0;
}

int get_RowStoreUsage_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_CHAR;
  var->value = buff;
  std::string str = rceng->RowStoreStat();
  std::memcpy(buff, str.c_str(), str.length() + 1);
  return 0;
}

int get_InsertPerMinute_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = rceng->GetIPM();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_QueryPerMinute_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = rceng->GetQPM();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_LoadPerMinute_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = rceng->GetLPM();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_Freeable_StatusVar([[maybe_unused]] MYSQL_THD thd, struct st_mysql_show_var *outvar, char *tmp) {
  *((int64_t *)tmp) = mm::TraceableObject::GetFreeableSize();
  outvar->value = tmp;
  outvar->type = SHOW_LONGLONG;
  return 0;
}

int get_UnFreeable_StatusVar([[maybe_unused]] MYSQL_THD thd, struct st_mysql_show_var *outvar, char *tmp) {
  *((int64_t *)tmp) = mm::TraceableObject::GetUnFreeableSize();
  outvar->value = tmp;
  outvar->type = SHOW_LONGLONG;
  return 0;
}

int get_MemoryScale_StatusVar([[maybe_unused]] MYSQL_THD thd, struct st_mysql_show_var *outvar, char *tmp) {
  *((int64_t *)tmp) = mm::TraceableObject::MemorySettingsScale();
  outvar->value = tmp;
  outvar->type = SHOW_LONGLONG;
  return 0;
}

int get_InsertTotal_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = rceng->GetIT();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_QueryTotal_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = rceng->GetQT();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_LoadTotal_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = rceng->GetLT();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_LoadDupTotal_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = rceng->GetLDT();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_LoadDupPerMinute_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = rceng->GetLDPM();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_UpdateTotal_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = rceng->GetUT();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

int get_UpdatePerMinute_StatusVar([[maybe_unused]] MYSQL_THD thd, SHOW_VAR *outvar, char *tmp) {
  *((int64_t *)tmp) = rceng->GetUPM();
  outvar->value = tmp;
  outvar->type = SHOW_LONG;
  return 0;
}

char masteslave_info[8192];

SHOW_VAR tianmu_masterslave_dump[] = {{"info", masteslave_info, SHOW_CHAR, SHOW_SCOPE_UNDEF}, {NullS, NullS, SHOW_LONG, SHOW_SCOPE_UNDEF}};

//  showtype
//  =====================
//  SHOW_UNDEF, SHOW_BOOL, SHOW_INT, SHOW_LONG,
//  SHOW_LONGLONG, SHOW_CHAR, SHOW_CHAR_PTR,
//  SHOW_ARRAY, SHOW_FUNC, SHOW_DOUBLE

int tianmu_throw_error_func([[maybe_unused]] MYSQL_THD thd, [[maybe_unused]] struct st_mysql_sys_var *var,
                             [[maybe_unused]] void *save, struct st_mysql_value *value) {
  int buffer_length = 512;
  char buff[512] = {0};

  DEBUG_ASSERT(value->value_type(value) == MYSQL_VALUE_TYPE_STRING);

  my_message(static_cast<int>(common::ErrorCode::UNKNOWN_ERROR), value->val_str(value, buff, &buffer_length), MYF(0));
  return -1;
}

static void update_func_str([[maybe_unused]] THD *thd, struct st_mysql_sys_var *var, void *tgt, const void *save) {
  char *old = *(char **)tgt;
  *(char **)tgt = *(char **)save;
  if (var->flags & PLUGIN_VAR_MEMALLOC) {
    *(char **)tgt = my_strdup(PSI_NOT_INSTRUMENTED, *(char **)save, MYF(0));
    my_free(old);
  }
}

void refresh_sys_table_func([[maybe_unused]] MYSQL_THD thd, [[maybe_unused]] struct st_mysql_sys_var *var, void *tgt,
                            const void *save) {
  *(my_bool *)tgt = *(my_bool *)save ? TRUE : FALSE;
}

void debug_update(MYSQL_THD thd, struct st_mysql_sys_var *var, void *var_ptr, const void *save);
void trace_update(MYSQL_THD thd, struct st_mysql_sys_var *var, void *var_ptr, const void *save);
void controlquerylog_update(MYSQL_THD thd, struct st_mysql_sys_var *var, void *var_ptr, const void *save);
void start_async_update(MYSQL_THD thd, struct st_mysql_sys_var *var, void *var_ptr, const void *save);
extern void async_join_update(MYSQL_THD thd, struct st_mysql_sys_var *var, void *var_ptr, const void *save);

#define STATUS_FUNCTION(name, showtype, member)                                                             \
  int get_##name##_StatusVar([[maybe_unused]] MYSQL_THD thd, struct st_mysql_show_var *outvar, char *tmp) { \
    *((int64_t *)tmp) = rceng->cache.member();                                                              \
    outvar->value = tmp;                                                                                    \
    outvar->type = showtype;                                                                                \
    return 0;                                                                                               \
  }

#define MM_STATUS_FUNCTION(name, showtype, member)                                                          \
  int get_##name##_StatusVar([[maybe_unused]] MYSQL_THD thd, struct st_mysql_show_var *outvar, char *tmp) { \
    *((int64_t *)tmp) = mm::TraceableObject::Instance()->member();                                          \
    outvar->value = tmp;                                                                                    \
    outvar->type = showtype;                                                                                \
    return 0;                                                                                               \
  }

#define STATUS_MEMBER(name, label) \
  { "Tianmu_" #label, (char *)get_##name##_StatusVar, SHOW_FUNC, SHOW_SCOPE_UNDEF}

STATUS_FUNCTION(gdchits, SHOW_LONGLONG, getCacheHits)
STATUS_FUNCTION(gdcmisses, SHOW_LONGLONG, getCacheMisses)
STATUS_FUNCTION(gdcreleased, SHOW_LONGLONG, getReleased)
STATUS_FUNCTION(gdcreadwait, SHOW_LONGLONG, getReadWait)
STATUS_FUNCTION(gdcfalsewakeup, SHOW_LONGLONG, getFalseWakeup)
STATUS_FUNCTION(gdcreadwaitinprogress, SHOW_LONGLONG, getReadWaitInProgress)
STATUS_FUNCTION(gdcpackloads, SHOW_LONGLONG, getPackLoads)
STATUS_FUNCTION(gdcloaderrors, SHOW_LONGLONG, getLoadErrors)
STATUS_FUNCTION(gdcredecompress, SHOW_LONGLONG, getReDecompress)

MM_STATUS_FUNCTION(mmallocblocks, SHOW_LONGLONG, getAllocBlocks)
MM_STATUS_FUNCTION(mmallocobjs, SHOW_LONGLONG, getAllocObjs)
MM_STATUS_FUNCTION(mmallocsize, SHOW_LONGLONG, getAllocSize)
MM_STATUS_FUNCTION(mmallocpack, SHOW_LONGLONG, getAllocPack)
MM_STATUS_FUNCTION(mmalloctemp, SHOW_LONGLONG, getAllocTemp)
MM_STATUS_FUNCTION(mmalloctempsize, SHOW_LONGLONG, getAllocTempSize)
MM_STATUS_FUNCTION(mmallocpacksize, SHOW_LONGLONG, getAllocPackSize)
MM_STATUS_FUNCTION(mmfreeblocks, SHOW_LONGLONG, getFreeBlocks)
MM_STATUS_FUNCTION(mmfreepacks, SHOW_LONGLONG, getFreePacks)
MM_STATUS_FUNCTION(mmfreetemp, SHOW_LONGLONG, getFreeTemp)
MM_STATUS_FUNCTION(mmfreepacksize, SHOW_LONGLONG, getFreePackSize)
MM_STATUS_FUNCTION(mmfreetempsize, SHOW_LONGLONG, getFreeTempSize)
MM_STATUS_FUNCTION(mmfreesize, SHOW_LONGLONG, getFreeSize)
MM_STATUS_FUNCTION(mmrelease1, SHOW_LONGLONG, getReleaseCount1)
MM_STATUS_FUNCTION(mmrelease2, SHOW_LONGLONG, getReleaseCount2)
MM_STATUS_FUNCTION(mmrelease3, SHOW_LONGLONG, getReleaseCount3)
MM_STATUS_FUNCTION(mmrelease4, SHOW_LONGLONG, getReleaseCount4)
MM_STATUS_FUNCTION(mmreloaded, SHOW_LONGLONG, getReloaded)
MM_STATUS_FUNCTION(mmreleasecount, SHOW_LONGLONG, getReleaseCount)
MM_STATUS_FUNCTION(mmreleasetotal, SHOW_LONGLONG, getReleaseTotal)

static struct st_mysql_show_var statusvars[] = {
    STATUS_MEMBER(gdchits, gdc_hits),
    STATUS_MEMBER(gdcmisses, gdc_misses),
    STATUS_MEMBER(gdcreleased, gdc_released),
    STATUS_MEMBER(gdcreadwait, gdc_readwait),
    STATUS_MEMBER(gdcfalsewakeup, gdc_false_wakeup),
    STATUS_MEMBER(gdcreadwaitinprogress, gdc_read_wait_in_progress),
    STATUS_MEMBER(gdcpackloads, gdc_pack_loads),
    STATUS_MEMBER(gdcloaderrors, gdc_load_errors),
    STATUS_MEMBER(gdcredecompress, gdc_redecompress),
    STATUS_MEMBER(mmrelease1, mm_release1),
    STATUS_MEMBER(mmrelease2, mm_release2),
    STATUS_MEMBER(mmrelease3, mm_release3),
    STATUS_MEMBER(mmrelease4, mm_release4),
    STATUS_MEMBER(mmallocblocks, mm_alloc_blocs),
    STATUS_MEMBER(mmallocobjs, mm_alloc_objs),
    STATUS_MEMBER(mmallocsize, mm_alloc_size),
    STATUS_MEMBER(mmallocpack, mm_alloc_packs),
    STATUS_MEMBER(mmalloctemp, mm_alloc_temp),
    STATUS_MEMBER(mmalloctempsize, mm_alloc_temp_size),
    STATUS_MEMBER(mmallocpacksize, mm_alloc_pack_size),
    STATUS_MEMBER(mmfreeblocks, mm_free_blocks),
    STATUS_MEMBER(mmfreepacks, mm_free_packs),
    STATUS_MEMBER(mmfreetemp, mm_free_temp),
    STATUS_MEMBER(mmfreepacksize, mm_free_pack_size),
    STATUS_MEMBER(mmfreetempsize, mm_free_temp_size),
    STATUS_MEMBER(mmfreesize, mm_free_size),
    STATUS_MEMBER(mmreloaded, mm_reloaded),
    STATUS_MEMBER(mmreleasecount, mm_release_count),
    STATUS_MEMBER(mmreleasetotal, mm_release_total),
    STATUS_MEMBER(DelayedBufferUsage, delay_buffer_usage),
    STATUS_MEMBER(RowStoreUsage, row_store_usage),
    STATUS_MEMBER(Freeable, mm_freeable),
    STATUS_MEMBER(InsertPerMinute, insert_per_minute),
    STATUS_MEMBER(LoadPerMinute, load_per_minute),
    STATUS_MEMBER(QueryPerMinute, query_per_minute),
    STATUS_MEMBER(MemoryScale, mm_scale),
    STATUS_MEMBER(UnFreeable, mm_unfreeable),
    STATUS_MEMBER(InsertTotal, insert_total),
    STATUS_MEMBER(LoadTotal, load_total),
    STATUS_MEMBER(QueryTotal, query_total),
    STATUS_MEMBER(LoadDupPerMinute, load_dup_per_minute),
    STATUS_MEMBER(LoadDupTotal, load_dup_total),
    STATUS_MEMBER(UpdatePerMinute, update_per_minute),
    STATUS_MEMBER(UpdateTotal, update_total),
    {0, 0, SHOW_UNDEF, SHOW_SCOPE_UNDEF},
};

static MYSQL_SYSVAR_BOOL(refresh_sys_tianmu, tianmu_sysvar_refresh_sys_table, PLUGIN_VAR_BOOL, "-", NULL,
                         refresh_sys_table_func, TRUE);
static MYSQL_THDVAR_STR(trigger_error, PLUGIN_VAR_STR | PLUGIN_VAR_THDLOCAL, "-", tianmu_throw_error_func,
                        update_func_str, NULL);

static MYSQL_SYSVAR_INT(ini_allowmysqlquerypath, tianmu_sysvar_allowmysqlquerypath, PLUGIN_VAR_READONLY, "-", NULL,
                        NULL, 0, 0, 1, 0);
static MYSQL_SYSVAR_STR(ini_cachefolder, tianmu_sysvar_cachefolder, PLUGIN_VAR_READONLY, "-", NULL, NULL, "cache");
static MYSQL_SYSVAR_INT(ini_knlevel, tianmu_sysvar_knlevel, PLUGIN_VAR_READONLY, "-", NULL, NULL, 99, 0, 99, 0);
static MYSQL_SYSVAR_BOOL(ini_pushdown, tianmu_sysvar_pushdown, PLUGIN_VAR_READONLY, "-", NULL, NULL, TRUE);
static MYSQL_SYSVAR_INT(ini_servermainheapsize, tianmu_sysvar_servermainheapsize, PLUGIN_VAR_READONLY, "-", NULL, NULL,
                        0, 0, 1000000, 0);
static MYSQL_SYSVAR_BOOL(ini_usemysqlimportexportdefaults, tianmu_sysvar_usemysqlimportexportdefaults,
                         PLUGIN_VAR_READONLY, "-", NULL, NULL, FALSE);
static MYSQL_SYSVAR_INT(ini_threadpoolsize, tianmu_sysvar_threadpoolsize, PLUGIN_VAR_READONLY, "-", NULL, NULL, 1, 0,
                        1000000, 0);
static MYSQL_SYSVAR_INT(ini_cachesizethreshold, tianmu_sysvar_cachesizethreshold, PLUGIN_VAR_INT, "-", NULL, NULL, 4,
                        0, 1024, 0);
static MYSQL_SYSVAR_INT(ini_cachereleasethreshold, tianmu_sysvar_cachereleasethreshold, PLUGIN_VAR_INT, "-", NULL,
                        NULL, 100, 0, 100000, 0);
static MYSQL_SYSVAR_BOOL(insert_delayed, tianmu_sysvar_insert_delayed, PLUGIN_VAR_READONLY, "-", NULL, NULL, TRUE);
static MYSQL_SYSVAR_INT(insert_cntthreshold, tianmu_sysvar_insert_cntthreshold, PLUGIN_VAR_READONLY, "-", NULL, NULL,
                        2, 0, 1000, 0);
static MYSQL_SYSVAR_INT(insert_numthreshold, tianmu_sysvar_insert_numthreshold, PLUGIN_VAR_READONLY, "-", NULL, NULL,
                        10000, 0, 100000, 0);
static MYSQL_SYSVAR_INT(insert_wait_ms, tianmu_sysvar_insert_wait_ms, PLUGIN_VAR_READONLY, "-", NULL, NULL, 100, 10,
                        10000, 0);
static MYSQL_SYSVAR_INT(insert_wait_time, tianmu_sysvar_insert_wait_time, PLUGIN_VAR_INT, "-", NULL, NULL, 1000, 0,
                        600000, 0);
static MYSQL_SYSVAR_INT(insert_max_buffered, tianmu_sysvar_insert_max_buffered, PLUGIN_VAR_READONLY, "-", NULL, NULL,
                        65536, 0, 10000000, 0);
static MYSQL_SYSVAR_BOOL(compensation_start, tianmu_sysvar_compensation_start, PLUGIN_VAR_BOOL, "-", NULL, NULL,
                         FALSE);
static MYSQL_SYSVAR_STR(hugefiledir, tianmu_sysvar_hugefiledir, PLUGIN_VAR_READONLY, "-", NULL, NULL, "");
static MYSQL_SYSVAR_INT(cachinglevel, tianmu_sysvar_cachinglevel, PLUGIN_VAR_READONLY, "-", NULL, NULL, 1, 0, 512, 0);
static MYSQL_SYSVAR_STR(mm_policy, tianmu_sysvar_mm_policy, PLUGIN_VAR_READONLY, "-", NULL, NULL, "");
static MYSQL_SYSVAR_INT(mm_hardlimit, tianmu_sysvar_mm_hardlimit, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 1, 0);
static MYSQL_SYSVAR_STR(mm_releasepolicy, tianmu_sysvar_mm_releasepolicy, PLUGIN_VAR_READONLY, "-", NULL, NULL, "2q");
static MYSQL_SYSVAR_INT(mm_largetempratio, tianmu_sysvar_mm_largetempratio, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0,
                        99, 0);
static MYSQL_SYSVAR_INT(mm_largetemppool_threshold, tianmu_sysvar_mm_large_threshold, PLUGIN_VAR_INT,
                        "size threshold in MB for using large temp thread pool", NULL, NULL, 16, 0, 10240, 0);
static MYSQL_SYSVAR_INT(sync_buffers, tianmu_sysvar_sync_buffers, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 1, 0);

static MYSQL_SYSVAR_INT(query_threads, tianmu_sysvar_query_threads, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 100,
                        0);
static MYSQL_SYSVAR_INT(load_threads, tianmu_sysvar_load_threads, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 100, 0);
static MYSQL_SYSVAR_INT(bg_load_threads, tianmu_sysvar_bg_load_threads, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0,
                        100, 0);
static MYSQL_SYSVAR_INT(insert_buffer_size, tianmu_sysvar_insert_buffer_size, PLUGIN_VAR_READONLY, "-", NULL, NULL,
                        512, 512, 10000, 0);

static MYSQL_THDVAR_INT(session_debug_level, PLUGIN_VAR_INT, "session debug level", NULL, debug_update, 3, 0, 5, 0);
static MYSQL_THDVAR_INT(control_trace, PLUGIN_VAR_OPCMDARG, "ini controltrace", NULL, trace_update, 0, 0, 100, 0);
static MYSQL_SYSVAR_INT(global_debug_level, tianmu_sysvar_global_debug_level, PLUGIN_VAR_INT, "global debug level",
                        NULL, NULL, 4, 0, 5, 0);

static MYSQL_SYSVAR_INT(distinct_cache_size, tianmu_sysvar_distcache_size, PLUGIN_VAR_INT,
                        "Upper byte limit for GroupDistinctCache buffer", NULL, NULL, 64, 64, 256, 0);
static MYSQL_SYSVAR_BOOL(filterevaluation_speedup, tianmu_sysvar_filterevaluation_speedup, PLUGIN_VAR_BOOL, "-", NULL,
                         NULL, TRUE);
static MYSQL_SYSVAR_BOOL(groupby_speedup, tianmu_sysvar_groupby_speedup, PLUGIN_VAR_BOOL, "-", NULL, NULL, TRUE);
static MYSQL_SYSVAR_BOOL(orderby_speedup, tianmu_sysvar_orderby_speedup, PLUGIN_VAR_BOOL, "-", NULL, NULL, FALSE);
static MYSQL_SYSVAR_INT(join_parallel, tianmu_sysvar_join_parallel, PLUGIN_VAR_INT,
                        "join matching parallel: 0-Disabled, 1-Auto, N-specify count", NULL, NULL, 1, 0, 1000, 0);
static MYSQL_SYSVAR_INT(join_splitrows, tianmu_sysvar_join_splitrows, PLUGIN_VAR_INT,
                        "join split rows:0-Disabled, 1-Auto, N-specify count", NULL, NULL, 0, 0, 1000, 0);
static MYSQL_SYSVAR_BOOL(minmax_speedup, tianmu_sysvar_minmax_speedup, PLUGIN_VAR_BOOL, "-", NULL, NULL, TRUE);
static MYSQL_SYSVAR_UINT(index_cache_size, tianmu_sysvar_index_cache_size, PLUGIN_VAR_READONLY,
                         "Index cache size in MB", NULL, NULL, 0, 0, 65536, 0);
static MYSQL_SYSVAR_BOOL(index_search, tianmu_sysvar_index_search, PLUGIN_VAR_BOOL, "-", NULL, NULL, TRUE);
static MYSQL_SYSVAR_BOOL(enable_rowstore, tianmu_sysvar_enable_rowstore, PLUGIN_VAR_BOOL, "-", NULL, NULL, TRUE);
static MYSQL_SYSVAR_BOOL(parallel_filloutput, tianmu_sysvar_parallel_filloutput, PLUGIN_VAR_BOOL, "-", NULL, NULL,
                         TRUE);
static MYSQL_SYSVAR_BOOL(parallel_mapjoin, tianmu_sysvar_parallel_mapjoin, PLUGIN_VAR_BOOL, "-", NULL, NULL, FALSE);

static MYSQL_SYSVAR_INT(max_execution_time, tianmu_sysvar_max_execution_time, PLUGIN_VAR_INT,
                        "max query execution time in seconds", NULL, NULL, 0, 0, 10000, 0);
static MYSQL_SYSVAR_INT(ini_controlquerylog, tianmu_sysvar_controlquerylog, PLUGIN_VAR_INT, "global controlquerylog",
                        NULL, controlquerylog_update, 1, 0, 100, 0);

static const char *dist_policy_names[] = {"round-robin", "random", "space", 0};
static TYPELIB policy_typelib_t = {array_elements(dist_policy_names) - 1, "dist_policy_names", dist_policy_names, 0};
static MYSQL_SYSVAR_ENUM(data_distribution_policy, tianmu_sysvar_dist_policy, PLUGIN_VAR_RQCMDARG,
                         "Specifies the policy to distribute column data among multiple data "
                         "directories."
                         "Possible values are round-robin(default), random, and space",
                         NULL, NULL, 2, &policy_typelib_t);

static MYSQL_SYSVAR_INT(disk_usage_threshold, tianmu_sysvar_disk_usage_threshold, PLUGIN_VAR_INT,
                        "Specifies the disk usage threshold for data diretories.", NULL, NULL, 85, 10, 99, 0);

static MYSQL_SYSVAR_UINT(lookup_max_size, tianmu_sysvar_lookup_max_size, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Lookup dictionary max size", NULL, NULL, 100000, 1000, 1000000, 0);

static MYSQL_SYSVAR_BOOL(qps_log, tianmu_sysvar_qps_log, PLUGIN_VAR_BOOL, "-", NULL, NULL, TRUE);

static MYSQL_SYSVAR_BOOL(force_hashjoin, tianmu_sysvar_force_hashjoin, PLUGIN_VAR_BOOL, "-", NULL, NULL, FALSE);
static MYSQL_SYSVAR_INT(start_async, tianmu_sysvar_start_async, PLUGIN_VAR_INT,
                        "Enable async, specifies async threads x/100 * cpus", NULL, start_async_update, 0, 0, 100, 0);
static MYSQL_SYSVAR_STR(async_join, tianmu_sysvar_async_join, PLUGIN_VAR_STR,
                        "Set async join params: packStep;traverseCount;matchCount", NULL, async_join_update, "1;0;0;0");
static MYSQL_SYSVAR_BOOL(join_disable_switch_side, tianmu_sysvar_join_disable_switch_side, PLUGIN_VAR_BOOL, "-", NULL,
                         NULL, FALSE);
static MYSQL_SYSVAR_BOOL(enable_histogram_cmap_bloom, tianmu_sysvar_enable_histogram_cmap_bloom, PLUGIN_VAR_BOOL, "-",
                         NULL, NULL, FALSE);

static MYSQL_SYSVAR_UINT(result_sender_rows, tianmu_sysvar_result_sender_rows, PLUGIN_VAR_UNSIGNED,
                         "The number of rows to load at a time when processing "
                         "queries like select xxx from yyya",
                         NULL, NULL, 65536, 1024, 131072, 0);

void debug_update(MYSQL_THD thd, [[maybe_unused]] struct st_mysql_sys_var *var, void *var_ptr, const void *save) {
  if (rceng) {
    auto cur_conn = rceng->GetTx(thd);
    // set debug_level for connection level
    cur_conn->SetDebugLevel(*((int *)save));
  }
  *((int *)var_ptr) = *((int *)save);
}

void trace_update(MYSQL_THD thd, [[maybe_unused]] struct st_mysql_sys_var *var, void *var_ptr, const void *save) {
  *((int *)var_ptr) = *((int *)save);
  // get global mysql_sysvar_control_trace
  tianmu_sysvar_controltrace = THDVAR(nullptr, control_trace);
  if (rceng) {
    core::Transaction *cur_conn = rceng->GetTx(thd);
    cur_conn->SetSessionTrace(*((int *)save));
    ConfigureRCControl();
  }
}

void controlquerylog_update([[maybe_unused]] MYSQL_THD thd, [[maybe_unused]] struct st_mysql_sys_var *var,
                            void *var_ptr, const void *save) {
  *((int *)var_ptr) = *((int *)save);
  int control = *((int *)var_ptr);
  if (rceng) {
    if (control)
      rcquerylog.setOn();
    else
      rcquerylog.setOff();
  }
}

void start_async_update([[maybe_unused]] MYSQL_THD thd, [[maybe_unused]] struct st_mysql_sys_var *var, void *var_ptr,
                        const void *save) {
  int percent = *((int *)save);
  *((int *)var_ptr) = percent;
  if (rceng) {
    rceng->ResetTaskExecutor(percent);
  }
}

void resolve_async_join_settings(const std::string &settings) {
  std::vector<std::string> splits_vec;
  boost::split(splits_vec, settings, boost::is_any_of(";"));
  if (splits_vec.size() >= 4) {
    try {
      tianmu_sysvar_async_join_setting.pack_per_step = boost::lexical_cast<int>(splits_vec[0]);
      tianmu_sysvar_async_join_setting.rows_per_step = boost::lexical_cast<int>(splits_vec[1]);
      tianmu_sysvar_async_join_setting.traverse_slices = boost::lexical_cast<int>(splits_vec[2]);
      tianmu_sysvar_async_join_setting.match_slices = boost::lexical_cast<int>(splits_vec[3]);
    } catch (...) {
      TIANMU_LOG(LogCtl_Level::ERROR, "Failed resolve async join settings");
    }
  }
}

void async_join_update([[maybe_unused]] MYSQL_THD thd, [[maybe_unused]] struct st_mysql_sys_var *var,
                       [[maybe_unused]] void *var_ptr, const void *save) {
  const char *str = *static_cast<const char *const *>(save);
  std::string settings(str);
  resolve_async_join_settings(settings);
}

static struct st_mysql_sys_var *tianmu_showvars[] = {MYSQL_SYSVAR(bg_load_threads),
                                                  MYSQL_SYSVAR(cachinglevel),
                                                  MYSQL_SYSVAR(compensation_start),
                                                  MYSQL_SYSVAR(control_trace),
                                                  MYSQL_SYSVAR(data_distribution_policy),
                                                  MYSQL_SYSVAR(disk_usage_threshold),
                                                  MYSQL_SYSVAR(distinct_cache_size),
                                                  MYSQL_SYSVAR(filterevaluation_speedup),
                                                  MYSQL_SYSVAR(global_debug_level),
                                                  MYSQL_SYSVAR(groupby_speedup),
                                                  MYSQL_SYSVAR(hugefiledir),
                                                  MYSQL_SYSVAR(index_cache_size),
                                                  MYSQL_SYSVAR(index_search),
                                                  MYSQL_SYSVAR(enable_rowstore),
                                                  MYSQL_SYSVAR(ini_allowmysqlquerypath),
                                                  MYSQL_SYSVAR(ini_cachefolder),
                                                  MYSQL_SYSVAR(ini_cachereleasethreshold),
                                                  MYSQL_SYSVAR(ini_cachesizethreshold),
                                                  MYSQL_SYSVAR(ini_controlquerylog),
                                                  MYSQL_SYSVAR(ini_knlevel),
                                                  MYSQL_SYSVAR(ini_pushdown),
                                                  MYSQL_SYSVAR(ini_servermainheapsize),
                                                  MYSQL_SYSVAR(ini_threadpoolsize),
                                                  MYSQL_SYSVAR(ini_usemysqlimportexportdefaults),
                                                  MYSQL_SYSVAR(insert_buffer_size),
                                                  MYSQL_SYSVAR(insert_cntthreshold),
                                                  MYSQL_SYSVAR(insert_delayed),
                                                  MYSQL_SYSVAR(insert_max_buffered),
                                                  MYSQL_SYSVAR(insert_numthreshold),
                                                  MYSQL_SYSVAR(insert_wait_ms),
                                                  MYSQL_SYSVAR(insert_wait_time),
                                                  MYSQL_SYSVAR(join_disable_switch_side),
                                                  MYSQL_SYSVAR(enable_histogram_cmap_bloom),
                                                  MYSQL_SYSVAR(join_parallel),
                                                  MYSQL_SYSVAR(join_splitrows),
                                                  MYSQL_SYSVAR(load_threads),
                                                  MYSQL_SYSVAR(lookup_max_size),
                                                  MYSQL_SYSVAR(max_execution_time),
                                                  MYSQL_SYSVAR(minmax_speedup),
                                                  MYSQL_SYSVAR(mm_hardlimit),
                                                  MYSQL_SYSVAR(mm_largetempratio),
                                                  MYSQL_SYSVAR(mm_largetemppool_threshold),
                                                  MYSQL_SYSVAR(mm_policy),
                                                  MYSQL_SYSVAR(mm_releasepolicy),
                                                  MYSQL_SYSVAR(orderby_speedup),
                                                  MYSQL_SYSVAR(parallel_filloutput),
                                                  MYSQL_SYSVAR(parallel_mapjoin),
                                                  MYSQL_SYSVAR(qps_log),
                                                  MYSQL_SYSVAR(query_threads),
                                                  MYSQL_SYSVAR(refresh_sys_tianmu),
                                                  MYSQL_SYSVAR(session_debug_level),
                                                  MYSQL_SYSVAR(sync_buffers),
                                                  MYSQL_SYSVAR(trigger_error),
                                                  MYSQL_SYSVAR(async_join),
                                                  MYSQL_SYSVAR(force_hashjoin),
                                                  MYSQL_SYSVAR(start_async),
                                                  MYSQL_SYSVAR(result_sender_rows),
                                                  NULL};
}  // namespace dbhandler
}  // namespace Tianmu

mysql_declare_plugin(tianmu){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &Tianmu::dbhandler::tianmu_storage_engine,
    "TIANMU",
    "StoneAtom Group Holding Limited",
    "Tianmu storage engine",
    PLUGIN_LICENSE_GPL,
    Tianmu::dbhandler::rcbase_init_func, /* Plugin Init */
    Tianmu::dbhandler::rcbase_done_func, /* Plugin Deinit */
    0x0001 /* 0.1 */,
    Tianmu::dbhandler::statusvars,   /* status variables  */
    Tianmu::dbhandler::tianmu_showvars, /* system variables  */
    NULL,                             /* config options    */
    0                                 /* flags for plugin */
} mysql_declare_plugin_end;
