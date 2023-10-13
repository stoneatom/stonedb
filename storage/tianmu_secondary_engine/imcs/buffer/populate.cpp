/* Copyright (c) 2018, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  Populate the changes from this buffer to tiles in IMCS.

  Created 2/2/2023 Hom.LEE
 */

#include <vector>
#include <atomic>
#include <string>

#include "populate.h"

#include "../common/error.h"
#include "../imcs_base.h"
#include "log0sys.h"
#include "mtr0types.h"
#include "fil0fil.h"
#include "mtr0log.h"
#include "parse_redo.h"
#include "log0recv.h"
#include "os0thread-create.h"
#include "log0write.h"
#include "log0log.h"
#include "mysql.h"

namespace Tianmu {
namespace IMCS {

std::chrono::milliseconds get_rapid_repopulate_every() {
  return std::chrono::milliseconds(RAPID_REPOPULATE_EVERY);
}

ulint parse_single_redo(mlog_id_t *type, byte *ptr, byte *end_ptr,
                        space_id_t *space_id, page_no_t *page_no, byte **body,
                        lsn_t start_lsn, bool apply_to_rapid) {
  byte *new_ptr;

  *body = nullptr;

  if (ptr == end_ptr) {
    return 0;
  }

  switch (*ptr) {
    case MLOG_MULTI_REC_END:
    case MLOG_DUMMY_RECORD:
      *page_no = FIL_NULL;
      *space_id = SPACE_UNKNOWN;
      *type = static_cast<mlog_id_t>(*ptr);
      return 1;
    case MLOG_MULTI_REC_END | MLOG_SINGLE_REC_FLAG:
    case MLOG_DUMMY_RECORD | MLOG_SINGLE_REC_FLAG:
      // corrupt log
      assert(0);
    case MLOG_TABLE_DYNAMIC_META:
    case MLOG_TABLE_DYNAMIC_META | MLOG_SINGLE_REC_FLAG:
      table_id_t id;
      uint64_t version;

      *page_no = FIL_NULL;
      *space_id = SPACE_UNKNOWN;

      new_ptr =
          mlog_parse_initial_dict_log_record(ptr, end_ptr, type, &id, &version);

      if (new_ptr != nullptr) {
        new_ptr = recv_sys->metadata_recover->parseMetadataLog(
            id, version, new_ptr, end_ptr);
      }

      return new_ptr == nullptr ? 0 : new_ptr - ptr;
  }

  new_ptr =
      mlog_parse_initial_log_record(ptr, end_ptr, type, space_id, page_no);
  *body = new_ptr;
  if (new_ptr == nullptr) {
    return 0;
  }

  assert(*type != 0);

  new_ptr = parse_log_body(*type, new_ptr, end_ptr, *space_id, *page_no,
                           new_ptr - ptr, start_lsn, apply_to_rapid);

  if (new_ptr == nullptr) {
    return 0;
  }

  return new_ptr - ptr;
}

int handle_single_rec(byte *ptr, byte *end_ptr, size_t *len, lsn_t start_lsn) {
  mlog_id_t type;
  byte *body;
  page_no_t page_no;
  space_id_t space_id;

  *len = parse_single_redo(&type, ptr, end_ptr, &space_id, &page_no, &body,
                           start_lsn, true);
  if (*len == 0) {
    return RET_NOT_FOUND_COMPLETE_MTR;
  }
  return RET_SUCCESS;
}

int handle_multi_rec(byte *ptr, byte *end_ptr, size_t *len, lsn_t start_lsn) {
  ulint n_recs = 0;
  ulint total_len = 0;

  byte *orig_ptr = ptr;

  // just check whether exist a complete mtr
  for (;;) {
    mlog_id_t type = MLOG_BIGGEST_TYPE;
    byte *body;
    page_no_t page_no = 0;
    space_id_t space_id = 0;

    ulint len = parse_single_redo(&type, ptr, end_ptr, &space_id, &page_no,
                                  &body, start_lsn, false);

    if (len == 0) {
      return RET_NOT_FOUND_COMPLETE_MTR;
    }

    total_len += len;
    ++n_recs;

    ptr += len;

    if (type == MLOG_MULTI_REC_END) {
      break;
    }
  }

  ptr = orig_ptr;

  for (uint32 i = 0; i < n_recs; ++i) {
    mlog_id_t type = MLOG_BIGGEST_TYPE;
    byte *body;
    page_no_t page_no = 0;
    space_id_t space_id = 0;
    ulint len = parse_single_redo(&type, ptr, end_ptr, &space_id, &page_no,
                                  &body, start_lsn, true);
    ptr += len;
  }

  *len = total_len;
  assert(ptr - orig_ptr == total_len);
  return RET_SUCCESS;
}

// handle single mtr
lsn_t parse_redo_and_apply(log_t *log_ptr, lsn_t start_lsn, lsn_t target_lsn) {
  int ret = RET_SUCCESS;

  size_t start_offset = start_lsn % log_ptr->buf_size;
  size_t end_offset = target_lsn % log_ptr->buf_size;

  size_t size;
  if (start_offset < end_offset) {
    size = end_offset - start_offset;
  } else {
    size = log_ptr->buf_size - start_offset + end_offset;
  }

  byte *buf = new byte[size];

  size_t end_buf_offset = 0;
  size_t cur_offset;
  lsn_t cur_lsn = start_lsn;

  std::vector<std::pair<size_t, lsn_t>> seg;

  if (start_offset >= end_offset) {
    cur_offset = start_offset;
    size_t sz = log_ptr->buf_size;

    while (cur_offset < sz) {
      size_t cur_off_in_block = cur_offset % OS_FILE_LOG_BLOCK_SIZE;
      size_t bytes_in_block = OS_FILE_LOG_BLOCK_SIZE - cur_off_in_block;
      if (cur_offset + bytes_in_block > sz) {
        bytes_in_block += sz - cur_offset;
      }
      size_t to_off_in_block = cur_off_in_block + bytes_in_block;
      if (to_off_in_block < LOG_BLOCK_HDR_SIZE) {
        cur_offset += bytes_in_block;
        continue;
      }
      if (cur_off_in_block >= OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
        cur_offset += bytes_in_block;
        continue;
      }

      size_t fix_start_offset = cur_offset;
      size_t fix_lsn = cur_lsn;
      size_t fix_len = bytes_in_block;
      if (cur_off_in_block < LOG_BLOCK_HDR_SIZE) {
        fix_start_offset += LOG_BLOCK_HDR_SIZE - cur_off_in_block;
        fix_lsn += LOG_BLOCK_HDR_SIZE - cur_off_in_block;
        fix_len -= LOG_BLOCK_HDR_SIZE - cur_off_in_block;
      }
      if (to_off_in_block >= OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
        fix_len -=
            to_off_in_block - (OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) + 1;
      }

      memcpy(buf + end_buf_offset, log_ptr->buf + fix_start_offset, fix_len);

      seg.emplace_back(end_buf_offset, fix_lsn);

      cur_offset += bytes_in_block;
      cur_lsn += bytes_in_block;
      end_buf_offset += fix_len;
    }
    cur_offset = 0;
  } else {
    cur_offset = start_offset;
  }
  while (cur_offset < end_offset) {
    size_t cur_off_in_block = cur_offset % OS_FILE_LOG_BLOCK_SIZE;
    size_t bytes_in_block = OS_FILE_LOG_BLOCK_SIZE - cur_off_in_block;
    if (cur_offset + bytes_in_block >= end_offset) {
      bytes_in_block = end_offset - cur_offset;
    }
    size_t to_off_in_block = cur_off_in_block + bytes_in_block;
    if (to_off_in_block < LOG_BLOCK_HDR_SIZE) {
      cur_offset += bytes_in_block;
      continue;
    }
    if (cur_off_in_block >= OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
      cur_offset += bytes_in_block;
      continue;
    }

    size_t fix_start_offset = cur_offset;
    size_t fix_lsn = cur_lsn;
    size_t fix_len = bytes_in_block;
    if (cur_off_in_block < LOG_BLOCK_HDR_SIZE) {
      fix_start_offset += LOG_BLOCK_HDR_SIZE - cur_off_in_block;
      fix_lsn += LOG_BLOCK_HDR_SIZE - cur_off_in_block;
      fix_len -= LOG_BLOCK_HDR_SIZE - cur_off_in_block;
    }
    if (to_off_in_block >= OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
      fix_len -=
          to_off_in_block - (OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE);
    }

    memcpy(buf + end_buf_offset, log_ptr->buf + fix_start_offset, fix_len);

    seg.emplace_back(end_buf_offset, fix_lsn);

    cur_offset += bytes_in_block;
    cur_lsn += bytes_in_block;
    end_buf_offset += fix_len;
  }

  byte *ptr = buf;
  assert(end_buf_offset <= size);
  byte *end_ptr = buf + end_buf_offset;

  size_t offset = 0;

  while (ptr < end_ptr) {
    bool single_rec;
    switch (*ptr) {
      case MLOG_DUMMY_RECORD:
        single_rec = true;
        break;
      default:
        single_rec = !!(*ptr & MLOG_SINGLE_REC_FLAG);
    }
    size_t len = 0;
    if (single_rec) {
      if (RET_FAIL(handle_single_rec(ptr, end_ptr, &len, start_lsn))) {
        assert(ret == RET_NOT_FOUND_COMPLETE_MTR);
        break;
      }
    } else {
      if (RET_FAIL(handle_multi_rec(ptr, end_ptr, &len, start_lsn))) {
        assert(ret == RET_NOT_FOUND_COMPLETE_MTR);
        break;
      }
    }

    // ============== debug ===============
    auto it = upper_bound(seg.begin(), seg.end(), offset,
                          [](size_t lhs, const std::pair<size_t, lsn_t> &rhs) {
                            return lhs < rhs.first;
                          });
    it--;
    lsn_t left = it->second + (offset - it->first);
    // ============== debug ===============
    offset += len;
    ptr += len;
    // ============== debug ===============
    it = upper_bound(seg.begin(), seg.end(), offset,
                     [](size_t lhs, const std::pair<size_t, lsn_t> &rhs) {
                       return lhs < rhs.first;
                     });
    it--;
    lsn_t right = it->second + (offset - it->first);
    std::string msg =
        "rapid-mtr: " + std::to_string(left) + " " + std::to_string(right);
    LogErr(SYSTEM_LEVEL, ER_CONDITIONAL_DEBUG, msg.c_str());
    // ============== debug ===============
  }

  auto it = upper_bound(seg.begin(), seg.end(), offset,
                        [](size_t lhs, const std::pair<size_t, lsn_t> &rhs) {
                          return lhs < rhs.first;
                        });
  assert(it != seg.begin());
  it--;
  lsn_t next_lsn = it->second + (offset - it->first);

  delete[] buf;
  return next_lsn;
}

IB_thread log_rapid_thread;

mysql_pfs_key_t log_rapid_thread_key;

void start_backgroud_threads(log_t &log) {
  log_rapid_thread =
      os_thread_create(log_rapid_thread_key, 0, repopulate, &log);
  log_rapid_thread.start();
}

static void log_allocate_rapid_events(log_t &log) {
  const size_t n = INNODB_LOG_EVENTS_DEFAULT;

  log.rapid_events_size = n;

  // TODO: use arena allocator
  log.rapid_events = new os_event_t[n];
  for (size_t i = 0; i < log.rapid_events_size; ++i) {
    log.rapid_events[i] = os_event_create();
  }
}

void repopulate(log_t *log_ptr) {
  assert(log_ptr != nullptr);

  // adapt for redo log parse
  recv_sys_init();

  // initialize rapid lsn
  log_ptr->rapid_lsn = log_ptr->recent_written.tail();

  log_allocate_rapid_events(*log_ptr);
  log_ptr->enable_rapid_lsn = true;

  for (;;) {
    lsn_t target_lsn =
        log_ptr->flushed_to_disk_lsn.load(std::memory_order_relaxed);
    lsn_t rapid_lsn = log_ptr->rapid_lsn.load();

    while (rapid_lsn >= target_lsn) {
      log_write_up_to(*log_ptr, rapid_lsn, true);
      target_lsn = log_ptr->flushed_to_disk_lsn.load(std::memory_order_relaxed);
    }

    lsn_t next_lsn = parse_redo_and_apply(log_ptr, rapid_lsn, target_lsn);

    std::string msg = "rapid: " + std::to_string(rapid_lsn) + " " +
                      std::to_string(target_lsn) + " " +
                      std::to_string(next_lsn);
    LogErr(SYSTEM_LEVEL, ER_CONDITIONAL_DEBUG, msg.c_str());

    if (next_lsn > rapid_lsn) {
      lsn_t lsn = rapid_lsn;
      const lsn_t notified_up_to_lsn =
          ut_uint64_align_up(next_lsn, OS_FILE_LOG_BLOCK_SIZE);
      while (lsn <= notified_up_to_lsn) {
        const size_t slot = compute_rapid_event_slot(lsn);
        lsn += OS_FILE_LOG_BLOCK_SIZE;
        os_event_set(log_ptr->rapid_events[slot]);
      }
    }

    if (!log_ptr->rapid_lsn.compare_exchange_strong(rapid_lsn, next_lsn)) {
      // only one thread can update the lsn
      assert(0);
    }
  }
}

size_t compute_rapid_event_slot(lsn_t lsn) {
  return ((lsn - 1) / OS_FILE_LOG_BLOCK_SIZE) & (INNODB_LOG_EVENTS_DEFAULT - 1);
}

}  // namespace IMCS
}  // namespace Tianmu
