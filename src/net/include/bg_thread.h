// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_BG_THREAD_H_
#define NET_INCLUDE_BG_THREAD_H_

#include <atomic>
#include <queue>

#include "net/include/net_thread.h"

#include "pstd/include/pstd_mutex.h"

namespace net {

struct TimerItem {
  uint64_t exec_time;
  void (*function)(void*);
  void* arg;
  TimerItem(uint64_t _exec_time, void (*_function)(void*), void* _arg)
      : exec_time(_exec_time), function(_function), arg(_arg) {}
  bool operator<(const TimerItem& item) const { return exec_time > item.exec_time; }
};

class BGThread final : public Thread {
 public:
  explicit BGThread(int full = 100000) :  full_(full) {}

  ~BGThread() override {
    // call virtual in destructor, BGThread must be final
    StopThread();
  }

  int StopThread() override {
    should_stop_ = true;
    rsignal_.notify_one();
    wsignal_.notify_one();
    return Thread::StopThread();
  }

  void Schedule(void (*function)(void*), void* arg);

  /*
   * timeout is in millionsecond
   */
  void DelaySchedule(uint64_t timeout, void (*function)(void*), void* arg);

  void QueueSize(int* pri_size, int* qu_size);
  void QueueClear();
  void SwallowReadyTasks();

 private:
  struct BGItem {
    void (*function)(void*);
    void* arg;
    BGItem(void (*_function)(void*), void* _arg) : function(_function), arg(_arg) {}
  };

  std::queue<BGItem> queue_;
  std::priority_queue<TimerItem> timer_queue_;

  size_t full_;
  pstd::Mutex mu_;
  pstd::CondVar rsignal_;
  pstd::CondVar wsignal_;
  void* ThreadMain() override;
};

}  // namespace net
#endif  // NET_INCLUDE_BG_THREAD_H_
