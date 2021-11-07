#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "gtest_util.h"
#include "thread_pool.h"


namespace thr {
namespace internal {

void SleepFor(double seconds) {
  std::this_thread::sleep_for(
      std::chrono::nanoseconds(static_cast<int64_t>(seconds * 1e9)));
}

template <typename T>
static void task_add(T x, T y, T* out) {
  *out = x + y;
}

template <typename T>
struct task_slow_add {
  void operator()(T x, T y, T* out) {
    SleepFor(seconds_);
    *out = x + y;
  }

  const double seconds_;
};

typedef std::function<void(int, int, int*)> AddTaskFunc;

template <typename T>
static T add(T x, T y) {
  return x + y;
}

template <typename T>
static T slow_add(double seconds, T x, T y) {
  SleepFor(seconds);
  return x + y;
}

template <typename T>
static T inplace_add(T& x, T y) {
  return x += y;
}

class AddTester {
 public:
  explicit AddTester(int nadds, arrow::StopToken stop_token = arrow::StopToken::Unstoppable())
      : nadds_(nadds), stop_token_(stop_token), xs_(nadds), ys_(nadds), outs_(nadds, -1) {
    int x = 0, y = 0;
    std::generate(xs_.begin(), xs_.end(), [&] {
      ++x;
      return x;
    });
    std::generate(ys_.begin(), ys_.end(), [&] {
      y += 10;
      return y;
    });
  }

  AddTester(AddTester&&) = default;

  void SpawnTasks(ThreadPool* pool, AddTaskFunc add_func) {
    for (int i = 0; i < nadds_; ++i) {
      ASSERT_OK(pool->Spawn([=] { add_func(xs_[i], ys_[i], &outs_[i]); }, stop_token_));
    }
  }

  void CheckResults() {
    for (int i = 0; i < nadds_; ++i) {
      ASSERT_EQ(outs_[i], (i + 1) * 11);
    }
  }

  void CheckNotAllComputed() {
    for (int i = 0; i < nadds_; ++i) {
      if (outs_[i] == -1) {
        return;
      }
    }
    ASSERT_TRUE(0) << "all values were computed";
  }

 private:

  int nadds_;
  arrow::StopToken stop_token_;
  std::vector<int> xs_;
  std::vector<int> ys_;
  std::vector<int> outs_;
};

class TestThreadPool : public ::testing::Test {
 public:
  void TearDown() override {
    fflush(stdout);
    fflush(stderr);
  }

  std::shared_ptr<ThreadPool> MakeThreadPool() { return MakeThreadPool(4); }

  std::shared_ptr<ThreadPool> MakeThreadPool(int threads) {
    return *ThreadPool::Make(threads);
  }

  void DoSpawnAdds(ThreadPool* pool, int nadds, AddTaskFunc add_func,
                   arrow::StopToken stop_token = arrow::StopToken::Unstoppable(),
                   arrow::StopSource* stop_source = nullptr) {
    AddTester add_tester(nadds, stop_token);
    add_tester.SpawnTasks(pool, add_func);
    if (stop_source) {
      stop_source->RequestStop();
    }
    ASSERT_OK(pool->Shutdown());
    if (stop_source) {
      add_tester.CheckNotAllComputed();
    } else {
      add_tester.CheckResults();
    }
  }

  void SpawnAdds(ThreadPool* pool, int nadds, AddTaskFunc add_func,
                 arrow::StopToken stop_token = arrow::StopToken::Unstoppable()) {
    DoSpawnAdds(pool, nadds, std::move(add_func), std::move(stop_token));
  }

  void SpawnAddsAndCancel(ThreadPool* pool, int nadds, AddTaskFunc add_func,
                          arrow::StopSource* stop_source) {
    DoSpawnAdds(pool, nadds, std::move(add_func), stop_source->token(), stop_source);
  }

  void DoSpawnAddsThreaded(ThreadPool* pool, int nthreads, int nadds,
                           AddTaskFunc add_func,
                           arrow::StopToken stop_token = arrow::StopToken::Unstoppable(),
                           arrow::StopSource* stop_source = nullptr) {
    // Same as SpawnAdds, but do the task spawning from multiple threads
    std::vector<AddTester> add_testers;
    std::vector<std::thread> threads;
    for (int i = 0; i < nthreads; ++i) {
      add_testers.emplace_back(nadds, stop_token);
    }
    for (auto& add_tester : add_testers) {
      threads.emplace_back([&] { add_tester.SpawnTasks(pool, add_func); });
    }
    if (stop_source) {
      stop_source->RequestStop();
    }
    for (auto& thread : threads) {
      thread.join();
    }
    ASSERT_OK(pool->Shutdown());
    for (auto& add_tester : add_testers) {
      if (stop_source) {
        add_tester.CheckNotAllComputed();
      } else {
        add_tester.CheckResults();
      }
    }
  }

  void SpawnAddsThreaded(ThreadPool* pool, int nthreads, int nadds, AddTaskFunc add_func,
                         arrow::StopToken stop_token = arrow::StopToken::Unstoppable()) {
    DoSpawnAddsThreaded(pool, nthreads, nadds, std::move(add_func),
                        std::move(stop_token));
  }

  void SpawnAddsThreadedAndCancel(ThreadPool* pool, int nthreads, int nadds,
                                  AddTaskFunc add_func, arrow::StopSource* stop_source) {
    DoSpawnAddsThreaded(pool, nthreads, nadds, std::move(add_func), stop_source->token(),
                        stop_source);
  }
};

TEST_F(TestThreadPool, Submit) {
  auto pool = this->MakeThreadPool(3);
  {
    ASSERT_OK_AND_ASSIGN(arrow::Future<int> fut, pool->Submit(add<int>, 4, 5));
    arrow::Result<int> res = fut.result();
    ASSERT_OK_AND_EQ(9, res);
  }
  {
    ASSERT_OK_AND_ASSIGN(arrow::Future<std::string> fut,
                         pool->Submit(add<std::string>, "foo", "bar"));
    ASSERT_OK_AND_EQ("foobar", fut.result());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto fut, pool->Submit(slow_add<int>, /*seconds=*/0.01, 4, 5));
    ASSERT_OK_AND_EQ(9, fut.result());
  }
}

} //namespace internal
} //namespace th