// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/bg_thread.h"
#include <sys/time.h>

#include "pstd/include/pstd_mutex.h"
#include "pstd/include/xdebug.h"

namespace net {

void BGThread::Schedule(void (*function)(void*), void* arg) {
  mu_.Lock();
  while (queue_.size() >= full_ && !should_stop()) {
    wsignal_.Wait();
  }
  if (!should_stop()) {
    queue_.push(BGItem(function, arg));
    rsignal_.Signal();
  }
  mu_.Unlock();
}

void BGThread::QueueSize(int* pri_size, int* qu_size) {
  pstd::MutexLock l(&mu_);
  *pri_size = timer_queue_.size();
  *qu_size = queue_.size();
}

void BGThread::QueueClear() {
  pstd::MutexLock l(&mu_);
  std::queue<BGItem>().swap(queue_);
  std::priority_queue<TimerItem>().swap(timer_queue_);
  wsignal_.Signal();
}

void BGThread::SwallowReadyTasks() {
  // it's safe to swallow all the remain tasks in ready and timer queue,
  // while the schedule function would stop to add any tasks.
  mu_.Lock();
  while (!queue_.empty()) {
    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop();
    mu_.Unlock();
    (*function)(arg);
    mu_.Lock();
  }
  mu_.Unlock();

  struct timeval now;
  gettimeofday(&now, NULL);
  uint64_t unow = now.tv_sec * 1000000 + now.tv_usec;
  mu_.Lock();
  while(!timer_queue_.empty()) {
    TimerItem top_item = timer_queue_.top();
    if (unow / 1000 < top_item.exec_time / 1000) {
      break;
    }
    void (*function)(void*) = top_item.function;
    void* arg = top_item.arg;
    timer_queue_.pop();
    // Don't lock while doing task
    mu_.Unlock();
    (*function)(arg);
    mu_.Lock();
  }
  mu_.Unlock();
}

void *BGThread::ThreadMain() {
  while (!should_stop()) {
    mu_.Lock();
    while (queue_.empty() && timer_queue_.empty() && !should_stop()) {
      rsignal_.Wait();
    }
    if (should_stop()) {
      mu_.Unlock();
      break;
    }
    if (!timer_queue_.empty()) {
      struct timeval now;
      gettimeofday(&now, NULL);

      TimerItem timer_item = timer_queue_.top();
      uint64_t unow = now.tv_sec * 1000000 + now.tv_usec;
      if (unow / 1000 >= timer_item.exec_time / 1000) {
        void (*function)(void*) = timer_item.function;
        void* arg = timer_item.arg;
        timer_queue_.pop();
        mu_.Unlock();
        (*function)(arg);
        continue;
      } else if (queue_.empty() && !should_stop()) {
        rsignal_.TimedWait(
            static_cast<uint32_t>((timer_item.exec_time - unow) / 1000));
        mu_.Unlock();
        continue;
      }
    }
    if (!queue_.empty()) {
      void (*function)(void*) = queue_.front().function;
      void* arg = queue_.front().arg;
      queue_.pop();
      wsignal_.Signal();
      mu_.Unlock();
      (*function)(arg);
    }
  }
  // swalloc all the remain tasks in ready and timer queue
  SwallowReadyTasks();
  return NULL;
}

/*
 * timeout is in millisecond
 */
void BGThread::DelaySchedule(
    uint64_t timeout, void (*function)(void *), void* arg) {
  /*
   * pthread_cond_timedwait api use absolute API
   * so we need gettimeofday + timeout
   */
  struct timeval now;
  gettimeofday(&now, NULL);
  uint64_t exec_time;
  exec_time = now.tv_sec * 1000000 + timeout * 1000 + now.tv_usec;

  mu_.Lock();
  if (!should_stop()) {
    timer_queue_.push(TimerItem(exec_time, function, arg));
    rsignal_.Signal();
  }
  mu_.Unlock();
}

}  // namespace net
