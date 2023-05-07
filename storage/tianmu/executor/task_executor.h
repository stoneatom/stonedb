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
#ifndef TIANMU_CORE_TASK_EXECUTOR_H_
#define TIANMU_CORE_TASK_EXECUTOR_H_
#pragma once

#include <future>
#include <memory>
#include <thread>

#include "base/core/reactor.h"

namespace Tianmu {
namespace core {

class AppTemplate;

class TaskExecutor {
 public:
  class Task {
   public:
    virtual ~Task() = default;
    virtual void Run() = 0;
  };

  template <typename Func>
  class FuncTask : public Task {
   public:
    FuncTask(Func &&func) : _func(std::move(func)) {}

    ~FuncTask() override = default;

    void Run() override {
      _func().then([promise = std::move(_promise)]() mutable { promise.set_value(); });
    }

   private:
    std::future<void> get_future() { return _promise.get_future(); }

    friend class TaskExecutor;
    Func _func;
    std::promise<void> _promise;
  };

  TaskExecutor();
  ~TaskExecutor();

  std::future<void> Init(unsigned threads);

  template <typename Func>
  std::future<void> Execute(Func &&func) {
    auto task = std::make_unique<FuncTask<Func>>(std::move(func));
    auto ret = task->get_future();
    AddTask(std::move(task));
    return ret;
  }

  void Exit();

 private:
  void AddTask(std::unique_ptr<Task> task);

  std::thread _main_thread;
  std::unique_ptr<AppTemplate> _app_template;
};

#ifndef DISABLE_USED_FOR_TIANMU
TaskExecutor *GetTaskExecutor();

template <typename Func>
inline std::future<void> ScheduleAsyncTask(Func &&func) {
  return GetTaskExecutor()->Execute(std::move(func));
}
#endif

}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_TASK_EXECUTOR_H_
