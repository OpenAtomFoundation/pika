// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/bg_thread.h"
#include <sys/time.h>
#include <cstdlib>
#include <mutex>

#include "pstd/include/pstd_mutex.h"
#include "pstd/include/xdebug.h"

namespace net {

void BGThread::Schedule(void (*function)(void*), void* arg) {
  std::unique_lock lock(mu_);

  wsignal_.wait(lock, [this]() { return queue_.size() < full_ || should_stop(); });

  if (!should_stop()) {
    queue_.emplace(function, arg);
    rsignal_.notify_one();
  }
}

void BGThread::QueueSize(int* pri_size, int* qu_size) {
  std::lock_guard lock(mu_);
  *pri_size = static_cast<int32_t>(timer_queue_.size());
  *qu_size = static_cast<int32_t>(queue_.size());
}

void BGThread::QueueClear() {
  std::lock_guard lock(mu_);
  std::queue<BGItem>().swap(queue_);
  std::priority_queue<TimerItem>().swap(timer_queue_);
  wsignal_.notify_one();
}

void BGThread::SwallowReadyTasks() {
  // it's safe to swallow all the remain tasks in ready and timer queue,
  // while the schedule function would stop to add any tasks.
  mu_.lock();
  while (!queue_.empty()) {
    auto [function, arg] = queue_.front();
    queue_.pop();
    mu_.unlock();
    (*function)(arg);
    mu_.lock();
  }
  mu_.unlock();

  auto now = std::chrono::system_clock::now();
  uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
  mu_.lock();

  while (!timer_queue_.empty()) {
    auto [exec_time, function, arg] = timer_queue_.top();
    if (unow < exec_time) {
      break;
    }
    timer_queue_.pop();
    // Don't lock while doing task
    mu_.unlock();
    (*function)(arg);
    mu_.lock();
  }
  mu_.unlock();
}

void* BGThread::ThreadMain() {
  while (!should_stop()) {
    std::unique_lock lock(mu_);

    rsignal_.wait(lock, [this]() { return !queue_.empty() || !timer_queue_.empty() || should_stop(); });

    if (should_stop()) {
      break;
    }

    if (!timer_queue_.empty()) {
      auto now = std::chrono::system_clock::now();
      uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
      auto [exec_time, function, arg] = timer_queue_.top();
      if (unow >= exec_time) {
        timer_queue_.pop();
        lock.unlock();
        (*function)(arg);
        continue;
      } else if (queue_.empty() && !should_stop()) {
        rsignal_.wait_for(lock, std::chrono::microseconds(exec_time - unow));

        lock.unlock();
        continue;
      }
    }

    if (!queue_.empty()) {
      auto [function, arg] = queue_.front();
      queue_.pop();
      wsignal_.notify_one();
      lock.unlock();
      (*function)(arg);
    }
  }
  // swalloc all the remain tasks in ready and timer queue
  SwallowReadyTasks();
  return nullptr;
}

/*
 * timeout is in millisecond
 */
void BGThread::DelaySchedule(uint64_t timeout, void (*function)(void*), void* arg) {
  auto now = std::chrono::system_clock::now();
  uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
  uint64_t exec_time = unow + timeout * 1000;

  std::lock_guard lock(mu_);
  if (!should_stop()) {
    timer_queue_.emplace(exec_time, function, arg);
    rsignal_.notify_one();
  }
}

}  // namespace net
