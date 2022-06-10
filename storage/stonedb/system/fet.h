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
#ifndef STONEDB_SYSTEM_FET_H_
#define STONEDB_SYSTEM_FET_H_
#pragma once

#include <map>

#include "core/tools.h"
#include "system/rc_system.h"

namespace stonedb {
namespace system {
// #define FUNCTIONS_EXECUTION_TIMES

#ifdef FUNCTIONS_EXECUTION_TIMES
class FunctionsExecutionTimes;
extern FunctionsExecutionTimes *fet;
using LoadedDataPackCounter = std::set<core::PackCoordinate>;
extern LoadedDataPackCounter count_distinct_dp_loads;
extern LoadedDataPackCounter count_distinct_dp_decompressions;
extern uint64_t NoBytesReadByDPs;
extern uint64_t SizeOfUncompressedDP;

void NotifyDataPackLoad(const core::PackCoordinate &coord);

void NotifyDataPackDecompression(const core::PackCoordinate &coord);

class FunctionsExecutionTimes {
  struct Entry {
    uint64_t call;  // no. of calls
    uint64_t time;  // total time of calls
    Entry(uint64_t t) {
      call = 1;
      time = t;
    }
    Entry() {}
  };

  struct SortEntry {
    SortEntry(const std::string &s, const Entry &e) : n(s), e(e){};
    std::string n;
    Entry e;
  };

  class SortEntryLess {
   public:
    bool operator()(const SortEntry &e1, const SortEntry &e2) { return e1.e.time < e2.e.time; }
  };

 public:
  FunctionsExecutionTimes() {
    times = new std::map<std::string, Entry>;
    flush = false;
    InstallFlushSignal();
  }
  ~FunctionsExecutionTimes() {
    times->clear();
    delete times;
  }

  void Add(const std::string &function_name, uint64_t time) {
    std::scoped_lock guard(fet_mutex);

    if (flush) {
      Print();
      Clear();
      flush = false;
    }

    if (times->find(function_name) != times->end()) {
      Entry &entry = (*times)[function_name];
      entry.time += time;
      entry.call++;
    } else {
      // times.insert(function_name);
      Entry entry(time);
      times->insert(std::make_pair(function_name, entry));
    }
  }

  void PrintToRcdev() {
    std::scoped_lock guard(fet_mutex);
    Print();
  }

  // order flushing; it will be executed on the next Add()
  void Flush() { flush = true; }
  static void FlushFET(int signum);  // flush global object 'fet'
  static void InstallFlushSignal();

 private:
  void Clear() { times->clear(); }
  void Print() {
    STONEDB_LOG(INFO,
                "***** Function Execution Times "
                "************************************************************");
    std::priority_queue<SortEntry, std::vector<SortEntry>, SortEntryLess> sq;
    char str[512] = {0};
    for (auto &iter : *times) sq.push(SortEntry(iter.first, iter.second));

    while (!sq.empty()) {
      std::sprintf(str, "%-60s \t%6llu \t%8.2fs", sq.top().n.c_str(), sq.top().e.call, sq.top().e.time / 1000000.0);
      STONEDB_LOG(INFO, "%s", str);
      sq.pop();
    }

    STONEDB_LOG(INFO, "Number of distinct Data Packs loads: %u", (uint)count_distinct_dp_loads.size());
    STONEDB_LOG(INFO, "Number of distinct Data Packs decompressions: %u",
                (uint)count_distinct_dp_decompressions.size());
    STONEDB_LOG(INFO, "Size of DP read from disk: %fMB", ((double)NoBytesReadByDPs / 1_MB));
    STONEDB_LOG(INFO, "Size of uncompressed DPs: %fMB", ((double)SizeOfUncompressedDP / 1_MB));
    STONEDB_LOG(INFO,
                "**************************************************************"
                "***********"
                "******************");
  }

  std::map<std::string, Entry> *times;
  std::mutex fet_mutex;
  bool flush;
};

class FETOperator {
 public:
  FETOperator(const std::string &identyfier) : id(identyfier) { gettimeofday(&start_time, NULL); }
  ~FETOperator() {
    struct timeval t2;
    uint64_t sec, usec;
    sec = usec = 0;
    gettimeofday(&t2, NULL);

    sec += (t2.tv_sec - start_time.tv_sec);
    if (t2.tv_usec < start_time.tv_usec) {
      sec--;
      t2.tv_usec += 1000000l;
    };

    usec += (t2.tv_usec - start_time.tv_usec);
    if (usec >= 1000000l) {
      usec -= 1000000l;
      sec++;
    };
    usec += sec * 1000000;
    fet->Add(id, usec);
  }

 private:
  std::string id;
  struct timeval start_time, t2;
};

inline void FunctionsExecutionTimes::FlushFET(int signum) { fet->Flush(); }

inline void FunctionsExecutionTimes::InstallFlushSignal() {
  // install FlushFET as a signal handler for SIGUSR1
  //	void* ret = (void*)signal(16, FunctionsExecutionTimes::FlushFET);
  signal(16, FunctionsExecutionTimes::FlushFET);
}

// #ifdef FUNCTIONS_EXECUTION_TIMES
// #define _FET(name,str)		FETOperator name(str)
// #else
// #define _FET(name,str)
// #endif

#define MEASURE_FET(X) FETOperator feto(X)

#else
#define MEASURE_FET(X)
#endif
}  // namespace system
}  // namespace stonedb

#endif  // STONEDB_SYSTEM_FET_H_
