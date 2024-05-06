#pragma once

#include <atomic>
#include <concepts>
#include <functional>
#include <thread>
#include <vector>
#include "common/concurrency/future.h"
#include "common/macros.h"
#include "concurrentqueue/concurrentqueue.h"

namespace xodb::common {

template <typename T>
concept Callable = requires(T t) {
  { t() } -> std::convertible_to<void>;
};

class WorkerPool {
  using Task = std::function<void(void)>;

 public:
  explicit WorkerPool(size_t num_worker) : num_workers_(num_worker) {
    worker_pool_.reserve(num_worker);

    for (size_t i = 0; i < num_workers_; i++) {
      worker_pool_.emplace_back([&, i] { DispatchTasks(i); });
    }
  }

  DISALLOW_COPY_AND_MOVE(WorkerPool);

  ~WorkerPool() { Terminate(); }

  void Terminate() {
    if (!finished_.load(std::memory_order_relaxed)) {
      finished_ = true;

      for (auto &thread : worker_pool_) {
        if (thread.joinable()) {
          thread.join();
        }
      }

      // remove all remaining tasks from shared task queue
      Task task;
      while (task_queue_.try_dequeue(task)) {
      }
    }
  }

  template <typename T, typename R>
    requires Callable<T>
  std::shared_ptr<Future<R>> SubmitTask(T t) {
    auto promise = Future<R>::CreatePromise();
    std::shared_ptr<Future<R>> future(promise.get_future());
    auto task = [t = std::move(t), promise = std::move(promise)] {
      R r = t();
      promise.set_value(r);
    };

    task_queue_.enqueue(std::move(task));

    return future;
  }

 private:
  void DispatchTasks([[maybe_unused]] size_t worker_id) {
    Task task;
    while (!finished_.load(std::memory_order_relaxed)) {
      if (task_queue_.try_dequeue(task)) {
        task();
      }
    }
  }

  std::vector<std::thread> worker_pool_;
  std::atomic<bool> finished_{false};
  size_t num_workers_{0};
  moodycamel::ConcurrentQueue<Task> task_queue_;
};

}  // namespace xodb::common
