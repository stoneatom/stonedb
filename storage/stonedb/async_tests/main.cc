/*
copyright info
*/
#include <iostream>

#include "base/core/app_template.h"
#include "base/core/distributed.h"
#include "base/core/reactor.h"
#include "base/core/sleep.h"
#include "base/core/thread.h"

#include "async_tests/task_executor.h"

using namespace std::chrono_literals;
using namespace stonedb;
namespace stdexp = std::experimental;

class TaskRunner {
  class MyTask : public stonedb::base::task {
   public:
    MyTask(int count, uint64_t *result) {
      _count = count;
      _result = result;
    }

    ~MyTask() noexcept override {}

    void run() noexcept override {
      stonedb::base::print("Established task on cpu %3d\n", stonedb::base::engine().cpu_id());
      for (int index = 0; index < _count; ++index) {
        for (int jndex = 0; jndex < index; ++jndex) {
          *_result += jndex;
        }
      }
    }

   private:
    int _count;
    uint64_t *_result;
  };

 public:
  explicit TaskRunner(int count) : _count(count) {
    _count = count;
    _result = 0;
  }

  stonedb::base::future<> Run() {
    return stonedb::base::sleep(std::chrono::milliseconds(1)).then([this]() {
      stonedb::base::print("Established task on cpu %3d\n", stonedb::base::engine().cpu_id());
      uint64_t count = 0;
      for (int index = 0; index < _count; ++index) {
        for (int jndex = 0; jndex < index; ++jndex) {
          _result += jndex;
          count++;
        }
      }
      stonedb::base::print("%ld\n", count);
      return stonedb::base::make_ready_future();
    });
  }

  stonedb::base::future<uint64_t> get_result() {
    stonedb::base::print("Requests on cpu %2d: %ld\n", stonedb::base::engine().cpu_id(), _result);
    return stonedb::base::make_ready_future<uint64_t>(_result);
  }

  stonedb::base::future<> stop() { return stonedb::base::make_ready_future(); }

 private:
  int _count;
  uint64_t _result;
};

uint64_t test_pool(int count, bool print_result = true) {
  uint64_t result = 0;
  for (int index = 0; index < count; ++index) {
    for (int jndex = 0; jndex < index; ++jndex) {
      result += jndex;
    }
  }
  if (print_result) stonedb::base::print("Total requests: %u\n", result);
  return result;
}

stonedb::base::future<bool> RecurseFuture(int *count) {
  if (*count == 0) {
    return stonedb::base::make_ready_future<bool>(true);
  }

  stonedb::base::print("Count:%d\n", (*count)--);
  return stonedb::base::sleep(1ms).then([] { return false; });
  // return sleep(1ms).then([count]{ return RecurseFuture(count); });
}

stonedb::base::future<stdexp::optional<bool>> Recurse(std::shared_ptr<int> counter, unsigned thd) {
  if (*counter == 0) {
    return stonedb::base::make_ready_future<stdexp::optional<bool>>(true);
  }

  int count = (*counter)--;
  return stonedb::base::smp::submit_to(thd,
                                       [count, thd] {
                                         // std::this_thread::sleep_for(1000ms);
                                         stonedb::base::print("Calling on : %d thread counter: %d\n", thd, count);
                                         test_pool(50000, false);
                                       })
      .then([thd]() {
        stonedb::base::print("Called completed on : %d thread \n", thd);
        return stdexp::optional<bool>(stdexp::nullopt);
      });
}

void TestPostTask(std::shared_ptr<core::TaskExecutor> task_executor) {
  std::chrono::duration<double> total_elapsed =
      stonedb::base::steady_clock_type::now() - stonedb::base::steady_clock_type::now();
  std::chrono::duration<double> max_elapsed = total_elapsed;
  for (int index = 0; index < 100000; ++index) {
    auto started = stonedb::base::steady_clock_type::now();
    task_executor
        ->Execute([started, &total_elapsed, &max_elapsed]() {
          auto elapsed = stonedb::base::steady_clock_type::now() - started;
          if (elapsed > max_elapsed) max_elapsed = elapsed;
          total_elapsed += elapsed;
          return stonedb::base::make_ready_future<>();
        })
        .wait();
  }

  auto ave_secs = static_cast<double>(total_elapsed.count() / 100000.0);
  auto max_secs = static_cast<double>(max_elapsed.count());
  stonedb::base::print("Ave post task time: %fs  max post time:%fs\n", ave_secs, max_secs);
}

void TestSubmit(std::shared_ptr<core::TaskExecutor> task_executor) {
  auto ready_future = task_executor->Execute([]() {
    std::shared_ptr<int> counter(new int(10));
    stonedb::base::future<> parallel_future =
        stonedb::base::parallel_for_each(boost::irange<unsigned>(0, stonedb::base::smp::count), [counter](unsigned c) {
          return stonedb::base::repeat_until_value(std::bind(&Recurse, counter, c)).then([](bool) {
            stonedb::base::print("Repeat completed\n");
          });
        });
    return parallel_future.then([]() { stonedb::base::print("Parallel completed\n"); });
  });

  ready_future.wait();
}

void TestSubmitRandom(std::shared_ptr<core::TaskExecutor> task_executor) {
  auto ready_future = task_executor->Execute([]() {
    for (auto t : boost::irange(1u, stonedb::base::smp::count - 1)) {
      stonedb::base::smp::submit_to(t, [] { test_pool(100000); });
    }
    // std::this_thread::sleep_for(10s);
    // stonedb::base::print("10s completed\n");
    return stonedb::base::sleep(3s).then([]() {
      stonedb::base::print("3s completed\n");
      stonedb::base::smp::submit([] { test_pool(100); });
    });
  });

  ready_future.wait();
}

int main(int argc, char **argv) {
  auto started = stonedb::base::steady_clock_type::now();
  std::shared_ptr<core::TaskExecutor> task_executor(new core::TaskExecutor);
  std::future<void> ready_future = task_executor->Init(std::thread::hardware_concurrency());
  ready_future.wait();
  auto elapsed = stonedb::base::steady_clock_type::now() - started;
  auto secs = static_cast<double>(elapsed.count() / 1000000000.0);
  stonedb::base::print("Init time: %f\n", secs);
  std::this_thread::sleep_for(2s);
  TestPostTask(task_executor);
  TestSubmit(task_executor);

  TestSubmitRandom(task_executor);

  task_executor->Exit();
  return 0;
}

#if 0
int main(int argc, char** argv) {
    auto thd = std::thread([]() {
        stonedb::base::app_template app(stonedb::base::create_default_options(3));
        app.run_deprecated([] {
            ::pthread_setname_np(::pthread_self(), "reactor-main");
            return smp::submit_to(1, []{ return test_pool(1000000); })
                      .then([](uint64_t val){ return 0; });
        });
    });
    thd.join();
    return 0;

    app_template app;
    app.run_deprecated([] {

      // std::function<future<>(int)> functor;
      // functor = [&functor](int count){
      //   if (count == 0) {
      //     return make_ready_future<>();
      //   }
      //   print("Count:%d\n", count--);
      //   return sleep(1ms).then(functor);
      // };
      //int* count = new int(10);
      // std::shared_ptr<int> counter(new int(10000));
      // // future<bool> ready_future = RecurseFuture(&count);
      // // ready_future.then([&ready_future, &count](bool to_exit) {
      // //     if (to_exit) {
      // //       print("Print completed\n");
      // //     } else {
      // //       ready_future = RecurseFuture(&count);
      // //     }
      // // });

      // // ready_future = RecurseFuture(&count);

      // return repeat_until_value(std::bind(&Recurse, counter)).then([](bool completed) {
      //   print("Print completed\n");
      //   return 0;
      // });
      // bool completed = false;
      // while (!completed) {
      //   ready_future.then([&completed, &count](bool to_exit) {
      //      completed = to_exit;
      //      if (completed) {
      //         print("Print completed\n");
      //      } else {
      //         RecurseFuture(&count);
      //      }
      //   });
      // }

      return smp::submit_to(1, []{ return test_pool(1000000); })
              .then([](uint64_t val){ return 0; });


      //return make_ready_future<int>(0);

      //auto ready_future = make_ready_future<int>(0);
      // std::cout << "Hello waiter" << std::endl;
      // auto start = std::chrono::high_resolution_clock::now();
      // std::this_thread::sleep_for(2s);
      // auto end = std::chrono::high_resolution_clock::now();
      // std::chrono::duration<double, std::milli> elapsed = end-start;
      // std::cout << "Waited " << elapsed.count() << " ms\n";
      // auto task_runners = new distributed<TaskRunner>;
      // auto started = steady_clock_type::now();
      // return task_runners->start(200000).then([task_runners] {
      //           return task_runners->invoke_on_all(&TaskRunner::Run);
      //        }).then([task_runners] {
      //           return task_runners->map_reduce(adder<uint64_t>(), &TaskRunner::get_result);
      //        }).then([task_runners, started](uint64_t result) {
      //           auto finished = steady_clock_type::now();
      //           auto elapsed = finished - started;
      //           auto secs = static_cast<double>(elapsed.count() / 1000000000.0);
      //           print("Total cpus: %u\n", smp::count);
      //           print("Total requests: %u\n", result);
      //           print("Total time: %f\n", secs);
      //           print("Requests/sec: %f\n", static_cast<double>(result) / secs);
      //           print("==========     done     ============\n");
      //           return task_runners->stop().then([task_runners] {
      //             delete task_runners;
      //             return make_ready_future<int>(0);
      //           });
      //         });

      // stonedb::base::async([] {
      //   auto t1 = new stonedb::base::thread([] {
      //     int repeat = 0;
      //     while (repeat < 5) {
      //       for (int index = 0; index < 5; ++index) {
      //         print("Excute from t1 %d:%d\n", repeat, index);
      //       }
      //       repeat++;
      //       stonedb::base::thread::yield();
      //     }
      //   });

      //   auto t2 = new stonedb::base::thread([] {
      //     int repeat = 0;
      //     while (repeat < 5) {
      //       print("Excute from t2 %d\n", repeat);
      //       repeat++;
      //       stonedb::base::thread::yield();
      //     }
      //   });

      //   print("Excute from tmain\n");

      //   when_all(t1->join(), t2->join()).discard_result().then([t1, t2] {
      //      delete t1;
      //      delete t2;
      //   }).get();
      // });

      //return ready_future.then([](int code) { engine().exit(code); return code; });
    });

    //return 0;
}
#endif
