/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

class ThreadPool final {
 public:
  ThreadPool();
  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  void operator=(const ThreadPool&) = delete;

  template <typename F, typename... Args>
  auto ExecuteTask(F&& f, Args&&... args) -> std::future<typename std::invoke_result<F(Args...)>::type>;

  void JoinAll();
  void SetMaxIdleThread(unsigned int m);

 private:
  void _CreateWorker();
  void _WorkerRoutine();
  void _MonitorRoutine();

  std::thread monitor_;
  std::atomic<unsigned> maxIdleThread_;
  std::atomic<unsigned> pendingStopSignal_;

  static thread_local bool working_;
  std::deque<std::thread> workers_;

  std::mutex mutex_;
  std::condition_variable cond_;
  unsigned waiters_;
  bool shutdown_;
  std::deque<std::function<void()> > tasks_;

  static const int kMaxThreads = 256;
};

template <typename F, typename... Args>
auto ThreadPool::ExecuteTask(F&& f, Args&&... args) -> std::future<typename std::invoke_result<F(Args...)>::type> {
  using resultType = typename std::invoke_result<F(Args...)>::type;

  auto task =
      std::make_shared<std::packaged_task<resultType()> >(std::bind(std::forward<F>(f), std::forward<Args>(args)...));

  {
    std::unique_lock<std::mutex> guard(mutex_);
    if (shutdown_) {
      return std::future<resultType>();
    }

    tasks_.emplace_back([=]() { (*task)(); });
    if (waiters_ == 0) {
      _CreateWorker();
    }

    cond_.notify_one();
  }

  return task->get_future();
}
