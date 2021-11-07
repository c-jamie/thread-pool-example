
#pragma once

#include <cstdint>
#include <memory>
#include <queue>
#include <type_traits>
#include <utility>
#include "logging.h"

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/cancel.h"
#include "arrow/util/functional.h"
#include "arrow/util/future.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"
#include "arrow/util/cancel.h"

namespace thr {

/// \brief Get the capacity of the global thread pool
///
/// Return the number of worker threads in the thread pool to which
/// Arrow dispatches various CPU-bound tasks.  This is an ideal number,
/// not necessarily the exact number of threads at a given point in time.
///
/// You can change this number using SetCpuThreadPoolCapacity().
int GetCpuThreadPoolCapacity();

/// \brief Set the capacity of the global thread pool
///
/// Set the number of worker threads int the thread pool to which
/// Arrow dispatches various CPU-bound tasks.
///
/// The current number is returned by GetCpuThreadPoolCapacity().
arrow::Status SetCpuThreadPoolCapacity(int threads);

namespace internal {

// Hints about a task that may be used by an Executor.
// They are ignored by the provided ThreadPool implementation.
struct TaskHints {
  // The lower, the more urgent
  int32_t priority = 0;
  // The IO transfer size in bytes
  int64_t io_size = -1;
  // The approximate CPU cost in number of instructions
  int64_t cpu_cost = -1;
  // An application-specific ID
  int64_t external_id = -1;
};

class Executor {
 public:
  using StopCallback = arrow::internal::FnOnce<void(const arrow::Status&)>;

  virtual ~Executor();

  // Spawn a fire-and-forget task.
  template <typename Function>
  arrow::Status Spawn(Function&& func) {
    return SpawnReal(TaskHints{}, std::forward<Function>(func), arrow::StopToken::Unstoppable(),
                     StopCallback{});
  }
  template <typename Function>
  arrow::Status Spawn(Function&& func, arrow::StopToken stop_token) {
    return SpawnReal(TaskHints{}, std::forward<Function>(func), std::move(stop_token),
                     StopCallback{});
  }
  template <typename Function>
  arrow::Status Spawn(TaskHints hints, Function&& func) {
    return SpawnReal(hints, std::forward<Function>(func), arrow::StopToken::Unstoppable(),
                     StopCallback{});
  }
  template <typename Function>
  arrow::Status Spawn(TaskHints hints, Function&& func, arrow::StopToken stop_token) {
    return SpawnReal(hints, std::forward<Function>(func), std::move(stop_token),
                     StopCallback{});
  }
  template <typename Function>
  arrow::Status Spawn(TaskHints hints, Function&& func, arrow::StopToken stop_token,
               StopCallback stop_callback) {
    return SpawnReal(hints, std::forward<Function>(func), std::move(stop_token),
                     std::move(stop_callback));
  }

  // Submit a callable and arguments for execution.  Return a future that
  // will return the callable's result value once.
  // The callable's arguments are copied before execution.
  template <typename Function, typename... Args,
            typename FutureType = typename arrow::detail::ContinueFuture::ForSignature<
                Function && (Args && ...)>>
  arrow::Result<FutureType> Submit(TaskHints hints, arrow::StopToken stop_token, Function&& func,
                            Args&&... args) {
    using ValueType = typename FutureType::ValueType;

    auto future = FutureType::Make();
    auto task = std::bind(arrow::detail::ContinueFuture{}, future,
                          std::forward<Function>(func), std::forward<Args>(args)...);
    struct {
      arrow::WeakFuture<ValueType> weak_fut;

      void operator()(const arrow::Status& st) {
        auto fut = weak_fut.get();
        if (fut.is_valid()) {
          fut.MarkFinished(st);
        }
      }
    } stop_callback{arrow::WeakFuture<ValueType>(future)};
    auto out = SpawnReal(hints, std::move(task), std::move(stop_token),std::move(stop_callback));
    return future;
  }

  template <typename Function, typename... Args,
            typename FutureType = typename ::arrow::detail::ContinueFuture::ForSignature<
                Function && (Args && ...)>>
 arrow::Result<FutureType> Submit(Function&& func, Args&&... args) {
    return Submit(TaskHints{}, arrow::StopToken::Unstoppable(), std::forward<Function>(func),
                  std::forward<Args>(args)...);
  }
  // Return the level of parallelism (the number of tasks that may be executed
  // concurrently).  This may be an approximate number.
  virtual int GetCapacity() = 0;

  // Return true if the thread from which this function is called is owned by this
  // Executor. Returns false if this Executor does not support this property.
  virtual bool OwnsThisThread() { return false; }

 protected:

  Executor() = default;

  // Subclassing API
  virtual arrow::Status SpawnReal(TaskHints hints, arrow::internal::FnOnce<void()> task, arrow::StopToken,
                           StopCallback&&) = 0;
};

class ThreadPool : public Executor {
  public:

  static arrow::Result<std::shared_ptr<ThreadPool>> Make(int threads);

  static arrow::Result<std::shared_ptr<ThreadPool>> MakeEternal(int threads);

  ~ThreadPool() override;

  int GetCapacity() override;

  bool OwnsThisThread() override;

  int GetNumTasks();

  arrow::Status SetCapacity(int threads);

  static int DefaultCapacity();

  arrow::Status Shutdown(bool wait = true);

  void WaitForIdle();

  struct State;

protected:

  friend ThreadPool* GetCpuThreadPool();

  ThreadPool();

  arrow::Status SpawnReal(TaskHints hints, arrow::internal::FnOnce<void()> task, arrow::StopToken,
                  StopCallback&&) override;

  void CollectFinishedWorkersUnlocked();

  void LaunchWorkersUnlocked(int threads);

  int GetActualCapacity();

  void ProtectAgainstFork();

  static std::shared_ptr<ThreadPool> MakeCpuThreadPool();

  std::shared_ptr<State> sp_state_;
  State* state_;
  bool shutdown_on_destroy_; 

};

ThreadPool* GetCpuThreadPool();

} // internal

} // thr
