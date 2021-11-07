#include "thread_pool.h"
#include <algorithm>
#include <condition_variable>
#include <deque>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace thr {
namespace internal {

Executor::~Executor() = default;

struct Task {
  arrow::internal::FnOnce<void()> callable;
  arrow::StopToken stop_token;
  Executor::StopCallback stop_callback;
};

struct ThreadPool::State {
	State() = default;
	std::mutex mutex_;
	std::condition_variable cv_;
	std::condition_variable cv_shutdown_;
	std::condition_variable cv_idle_;

	std::list<std::thread> workers_;
	std::vector<std::thread> finished_workers_;
	std::deque<Task> pending_tasks_;
	int desired_capacity_ = 0;
	int tasks_queued_or_running_ = 0;
	bool please_shutdown_ = false;
	bool quick_shutdown_ = false;
};

// The worker loop is an independent function so that it can keep running
// after the ThreadPool is destroyed.
static void WorkerLoop(std::shared_ptr<ThreadPool::State> state,
                       std::list<std::thread>::iterator it) {
	ARROW_LOG(INFO) << "WorkerLoop";
  std::unique_lock<std::mutex> lock(state->mutex_);

  // Since we hold the lock, `it` now points to the correct thread object
  // (LaunchWorkersUnlocked has exited)
  DCHECK_EQ(std::this_thread::get_id(), it->get_id());

  // If too many threads, we should secede from the pool
  const auto should_secede = [&]() -> bool {
    return state->workers_.size() > static_cast<size_t>(state->desired_capacity_);
  };

  while (true) {
    // By the time this thread is started, some tasks may have been pushed
    // or shutdown could even have been requested.  So we only wait on the
    // condition variable at the end of the loop.

    // Execute pending tasks if any
    while (!state->pending_tasks_.empty() && !state->quick_shutdown_) {
      // We check this opportunistically at each loop iteration since
      // it releases the lock below.
      ARROW_LOG(INFO) << "executing task";
      if (should_secede()) {
        break;
      }

      DCHECK_GE(state->tasks_queued_or_running_, 0);
      {
        Task task = std::move(state->pending_tasks_.front());
        state->pending_tasks_.pop_front();
        arrow::StopToken* stop_token = &task.stop_token;
        lock.unlock();
        if (!stop_token->IsStopRequested()) {
          std::move(task.callable)();
        } else {
          if (task.stop_callback) {
            std::move(task.stop_callback)(stop_token->Poll());
          }
        }
        ARROW_UNUSED(std::move(task));  // release resources before waiting for lock
        lock.lock();
      }
      if (ARROW_PREDICT_FALSE(--state->tasks_queued_or_running_ == 0)) {
        state->cv_idle_.notify_all();
      }
    }
    // Now either the queue is empty *or* a quick shutdown was requested
    if (state->please_shutdown_ || should_secede()) {
      break;
    }
    // Wait for next wakeup
    state->cv_.wait(lock);
  }
  DCHECK_GE(state->tasks_queued_or_running_, 0);

  // We're done.  Move our thread object to the trashcan of finished
  // workers.  This has two motivations:
  // 1) the thread object doesn't get destroyed before this function finishes
  //    (but we could call thread::detach() instead)
  // 2) we can explicitly join() the trashcan threads to make sure all OS threads
  //    are exited before the ThreadPool is destroyed.  Otherwise subtle
  //    timing conditions can lead to false positives with Valgrind.
  DCHECK_EQ(std::this_thread::get_id(), it->get_id());
  state->finished_workers_.push_back(std::move(*it));
  state->workers_.erase(it);
  if (state->please_shutdown_) {
    // Notify the function waiting in Shutdown().
    state->cv_shutdown_.notify_one();
  }
}


void ThreadPool::WaitForIdle() {
	ARROW_LOG(INFO) << "ThreadPool::WaitForIdle";
	std::unique_lock<std::mutex> lk(state_ -> mutex_);
	state_ -> cv_idle_.wait(lk, [this] {return state_ ->  tasks_queued_or_running_ == 0;});
}

ThreadPool::ThreadPool()
	: sp_state_(std::make_shared<ThreadPool::State>()),
		state_(sp_state_.get()),
		shutdown_on_destroy_(true) {
    ARROW_LOG(INFO) << "ThreadPool";

}

ThreadPool::~ThreadPool() {
  ARROW_LOG(INFO) << "~ThreadPool";
	if (shutdown_on_destroy_) {
		Shutdown(false);
	}
}

void ThreadPool::ProtectAgainstFork() {

}

arrow::Status ThreadPool::SetCapacity(int threads) {
	ProtectAgainstFork();
  ARROW_LOG(INFO) << "ThreadPool::SetCapacity";
	std::unique_lock<std::mutex> lock(state_ -> mutex_);
	if (state_ -> please_shutdown_) {
		return arrow::Status::Invalid("op forbidden after shutdown");
	}

	if (threads < 0) {
		return arrow::Status::Invalid("cap must be < 0");
	}

	CollectFinishedWorkersUnlocked();

	state_ -> desired_capacity_ = threads;

	const int required = std::min(static_cast<int>(state_ -> pending_tasks_.size()),
								threads - static_cast<int>(state_ -> workers_.size()));
	if (required > 0) {
		LaunchWorkersUnlocked(required);
	} else if (required < 0 ) {
		state_ -> cv_.notify_all();
	}
	return arrow::Status::OK();
}

int ThreadPool::GetCapacity() {
  ARROW_LOG(INFO) << "ThreadPool::GetCapacity";
  ProtectAgainstFork();
  std::unique_lock<std::mutex> lock(state_->mutex_);
  return state_->desired_capacity_;
}

int ThreadPool::GetNumTasks() {
//   ProtectAgainstFork();
  ARROW_LOG(INFO) << "ThreadPool::GetNumTasks";
  std::unique_lock<std::mutex> lock(state_->mutex_);
  return state_->tasks_queued_or_running_;
}

int ThreadPool::GetActualCapacity() {
  ARROW_LOG(INFO) << "ThreadPool::GetAcutalCapacity";
  ProtectAgainstFork();
  std::unique_lock<std::mutex> lock(state_->mutex_);
  return static_cast<int>(state_->workers_.size());
}

arrow::Status ThreadPool::Shutdown(bool wait) {
  ARROW_LOG(INFO) << "ThreadPool::Shutdown";
	ProtectAgainstFork();
	std::unique_lock<std::mutex> lock(state_ -> mutex_);

	if (state_ -> please_shutdown_) {
		return arrow::Status::Invalid("Shutdown() already called");
	}
	state_ -> please_shutdown_ = true;
	state_ -> quick_shutdown_ = !wait;
	state_ -> cv_.notify_all();
	state_ -> cv_shutdown_.wait(lock, [this] {return state_ ->  workers_.empty();});
	if (!state_ -> quick_shutdown_) {
		DCHECK_EQ(state_ -> pending_tasks_.size(), 0);
	} else {
		state_ ->  pending_tasks_.clear();
	}
	CollectFinishedWorkersUnlocked();
	return arrow::Status::OK();
}

void ThreadPool::CollectFinishedWorkersUnlocked() {
  ARROW_LOG(INFO) << "ThreadPool::CollectFinishedWorkersUnlocked";
	for (auto& thread : state_ -> finished_workers_) {
		thread.join();
	}
	state_ -> finished_workers_.clear();

}

thread_local ThreadPool* current_thread_pool_ = nullptr;

bool ThreadPool::OwnsThisThread() {return current_thread_pool_ == this;}

void ThreadPool::LaunchWorkersUnlocked(int threads) {
  ARROW_LOG(INFO) << "ThreadPool::LaunchWorkersUnlocked";
	std::shared_ptr<State> state = sp_state_;
	for (int i = 0; i < threads; i++) {
    ARROW_LOG(INFO) << "launching thread: " << i;
		state_ -> workers_.emplace_back();
		auto it = --(state_ -> workers_.end());
		*it = std::thread([this, state, it] {
			current_thread_pool_ = this;
			WorkerLoop(state, it);
		});
	}
}

arrow::Status ThreadPool::SpawnReal(TaskHints hints, arrow::internal::FnOnce<void()> task, arrow::StopToken stop_token,
													StopCallback&& stop_callback) {

  ARROW_LOG(INFO) << "ThreadPool::SpawnReal";
	{
		ProtectAgainstFork();
		std::lock_guard<std::mutex> lock(state_->mutex_);
		if (state_ -> please_shutdown_) {
			return arrow::Status::Invalid("op forbid dur shutdown");
		}

		CollectFinishedWorkersUnlocked();
		state_ -> tasks_queued_or_running_ ++;
		if (static_cast<int>(state_->workers_.size()) < state_ -> tasks_queued_or_running_ &&
			state_-> desired_capacity_ > static_cast<int>(state_ -> workers_.size())) {
			LaunchWorkersUnlocked(1);
		}
		state_ -> pending_tasks_.push_back(
			{std::move(task), std::move(stop_token), std::move(stop_callback)});
	}
	state_ -> cv_.notify_one();
	return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<ThreadPool>> ThreadPool::Make(int threads) {
  ARROW_LOG(INFO) << "ThreadPool::Make";
	auto pool = std::shared_ptr<ThreadPool>(new ThreadPool());
	pool -> SetCapacity(threads);
	return pool;
}

arrow::Result<std::shared_ptr<ThreadPool>> ThreadPool::MakeEternal(int threads) {
  ARROW_ASSIGN_OR_RAISE(auto pool, Make(threads));
  // On Windows, the ThreadPool destructor may be called after non-main threads
  // have been killed by the OS, and hang in a condition variable.
  // On Unix, we want to avoid leak reports by Valgrind.
#ifdef _WIN32
  pool->shutdown_on_destroy_ = false;
#endif
  return pool;
}

static int ParseOMPEnvVar(const char* name) {
  // OMP_NUM_THREADS is a comma-separated list of positive integers.
  // We are only interested in the first (top-level) number.
  return 4;
}


int ThreadPool::DefaultCapacity() {
  int capacity, limit;
  capacity = ParseOMPEnvVar("OMP_NUM_THREADS");
  if (capacity == 0) {
    capacity = std::thread::hardware_concurrency();
  }
  limit = ParseOMPEnvVar("OMP_THREAD_LIMIT");
  if (limit > 0) {
    capacity = std::min(limit, capacity);
  }
  if (capacity == 0) {
    ARROW_LOG(WARNING) << "Failed to determine the number of available threads, "
                          "using a hardcoded arbitrary value";
    capacity = 4;
  }
  return capacity;
}

std::shared_ptr<ThreadPool> ThreadPool::MakeCpuThreadPool() {
  ARROW_LOG(INFO) << "ThreadPool::MakeCpuThreadPool";
	auto maybe_pool = ThreadPool::MakeEternal(ThreadPool::DefaultCapacity());
	if (!maybe_pool.ok()) {
		maybe_pool.status().Abort("failed to create global cpu tp");
	}
	return *std::move(maybe_pool);
}

ThreadPool* GetCpuThreadPool() {
  ARROW_LOG(INFO) << "GetCpuThreadPool";
	static std::shared_ptr<ThreadPool> singelton = ThreadPool::MakeCpuThreadPool();
	return singelton.get();
}

int GetCpuThreadPoolCapacity() {return internal::GetCpuThreadPool() -> GetCapacity();}


} //namespace internal

arrow::Status SetCpuThreadPoolCapacity(int threads) {
  ARROW_LOG(INFO) << "SetCpuThreadPool";
	return internal::GetCpuThreadPool() -> SetCapacity(threads);
}

} // namespace th