// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/bg_thread.h"
#include <iostream>
#include <string>
#include "pstd/include/pstd_mutex.h"
#include "unistd.h"

using namespace std;

static pstd::Mutex print_lock;

void task(void* arg) {
  std::unique_ptr<int32_t> int_arg(static_cast<int32_t*>(arg));
  {
    std::lock_guard l(print_lock);
    std::cout << " task : " << *int_arg << std::endl;
  }
  sleep(1);
}

struct TimerItem {
  uint64_t exec_time;
  void (*function)(void*);
  void* arg;
  TimerItem(uint64_t _exec_time, void (*_function)(void*), void* _arg)
      : exec_time(_exec_time), function(_function), arg(_arg) {}
  bool operator<(const TimerItem& item) const { return exec_time > item.exec_time; }
};

int32_t main() {
  net::BGThread t, t2(5);
  t.StartThread();
  t2.StartThread();
  int32_t qsize = 0, pqsize = 0;

  std::cout << "Normal BGTask... " << std::endl;
  for (int32_t i = 0; i < 10; i++) {
    int32_t* pi = new int32_t(i);
    t.Schedule(task, (void*)pi);
    t.QueueSize(&pqsize, &qsize);
    std::lock_guard l(print_lock);
    std::cout << " current queue size:" << qsize << ", " << pqsize << std::endl;
  }
  std::cout << std::endl << std::endl;

  while (qsize > 0) {
    t.QueueSize(&pqsize, &qsize);
    sleep(1);
  }

  qsize = pqsize = 0;
  std::cout << "Limit queue BGTask... " << std::endl;
  for (int32_t i = 0; i < 10; i++) {
    int32_t* pi = new int32_t(i);
    t2.Schedule(task, (void*)pi);
    t2.QueueSize(&pqsize, &qsize);
    std::lock_guard l(print_lock);
    std::cout << " current queue size:" << qsize << ", " << pqsize << std::endl;
  }
  std::cout << std::endl << std::endl;

  while (qsize > 0) {
    t2.QueueSize(&pqsize, &qsize);
    sleep(1);
  }

  std::cout << "TimerItem Struct... " << std::endl;
  std::priority_queue<TimerItem> pq;
  pq.push(TimerItem(1, task, nullptr));
  pq.push(TimerItem(5, task, nullptr));
  pq.push(TimerItem(3, task, nullptr));
  pq.push(TimerItem(2, task, nullptr));
  pq.push(TimerItem(4, task, nullptr));

  while (!pq.empty()) {
    printf("%ld\n", pq.top().exec_time);
    pq.pop();
  }
  std::cout << std::endl << std::endl;

  std::cout << "Restart BGThread" << std::endl;
  t.StopThread();
  t.StartThread();
  std::cout << "Time BGTask... " << std::endl;
  for (int32_t i = 0; i < 10; i++) {
    int32_t* pi = new int32_t(i);
    t.DelaySchedule(i * 1000, task, (void*)pi);
    t.QueueSize(&pqsize, &qsize);
    std::lock_guard l(print_lock);
    std::cout << " current queue size:" << qsize << ", " << pqsize << std::endl;
  }
  sleep(3);
  std::cout << "QueueClear..." << std::endl;
  t.QueueClear();
  sleep(10);

  return 0;
}
