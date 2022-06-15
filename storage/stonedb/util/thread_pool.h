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
#ifndef STONEDB_UTIL_THREAD_POOL_H_
#define STONEDB_UTIL_THREAD_POOL_H_
#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

#include "common/exception.h"
#include "util/log_ctl.h"

namespace stonedb {
namespace utils {
class thread_pool final {
 public:
  thread_pool(const std::string &name, size_t sz = std::thread::hardware_concurrency()) : name(name) {
    for (size_t i = 0; i < sz; ++i)
      workers.emplace_back([this, i]() {
        tp_owner = this;

        // thread name is restricted to 16 characters, including the terminating
        // null byte ('\0').
        std::string suffix = "[" + std::to_string(i) + "]";
        std::string tname = this->name.substr(0, 15 - suffix.size()) + suffix;
        if (::pthread_setname_np(pthread_self(), tname.c_str()) != 0)
          throw std::system_error(errno, std::system_category(), "failed to set thread name");

        while (true) {
          std::function<void()> task;
          {
            std::unique_lock<std::mutex> lock(queue_mutex);
            condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });
            if (stop && tasks.empty()) return;
            task = std::move(tasks.front());
            tasks.pop();
          }
          task();
        }
      });
  }

  ~thread_pool() {
    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      stop = true;
    }
    condition.notify_all();
    for (auto &worker : workers) worker.join();
  }

  size_t size() const { return workers.size(); }

  // check if the calling thread is a worker of this thread pool
  bool is_owner() const { return tp_owner == this; }

  template <class F, class... Args>
  auto add_task(F &&f, Args &&...args) -> std::future<typename std::result_of<F(Args...)>::type> {
    if (tp_owner == this) throw std::logic_error("add task in worker thread");

    auto task = std::make_shared<std::packaged_task<typename std::result_of<F(Args...)>::type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    auto res = task->get_future();
    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      // don't allow enqueuing if we are stopping
      if (stop) throw std::runtime_error("add task on stopped thread_pool");
      tasks.emplace([task]() { (*task)(); });
    }
    condition.notify_one();
    return res;
  }

 private:
  thread_local static thread_pool *tp_owner;
  std::vector<std::thread> workers;
  std::queue<std::function<void()>> tasks;

  std::string name;
  std::mutex queue_mutex;
  std::condition_variable condition;
  bool stop = false;
};

// shortcut for waiting for a group of task to finish
template <typename T>
class result_set final {
  using FT = typename std::future<T>;

 public:
  result_set() {}
  void insert(FT &&v) { results.push_back(std::forward<FT>(v)); }
  size_t size() const { return results.size(); }
  T get(size_t i) { return results[i].get(); }
  void get_all() {
    for (auto &&result : results) result.get();
  }
  void get_all_with_except() {
    bool no_except = true;
    for (auto &&result : results) try {
        result.get();
      } catch (std::exception &e) {
        no_except = false;
        STONEDB_LOG(LogCtl_Level::ERROR, "An exception is caught: %s", e.what());
      } catch (...) {
        no_except = false;
        STONEDB_LOG(LogCtl_Level::ERROR, "An unknown system exception error caught.");
      }
    if (!no_except) {
      throw common::Exception("Parallel worker run failed.");
    }
  }

 private:
  std::vector<FT> results;
};
}  // namespace utils
}  // namespace stonedb

#endif  // STONEDB_UTIL_THREAD_POOL_H_
