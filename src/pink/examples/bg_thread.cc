// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "unistd.h"
#include <string>
#include <iostream>
#include "pink/include/bg_thread.h"
#include "slash/include/slash_mutex.h"

using namespace std;

static slash::Mutex print_lock;

void task(void *arg) {
  {
  slash::MutexLock l(&print_lock);
  std::cout << " task : " << *((int *)arg) << std::endl;
  }
  sleep(1);
  delete (int*)arg;
}

struct TimerItem {
  uint64_t exec_time;
  void (*function)(void *);
  void* arg;
  TimerItem(uint64_t _exec_time, void (*_function)(void*), void* _arg) :
    exec_time(_exec_time),
    function(_function),
    arg(_arg) {}
  bool operator < (const TimerItem& item) const {
    return exec_time > item.exec_time;
  }
};

int main() {
  pink::BGThread t, t2(5);
  t.StartThread();
  t2.StartThread();
  int qsize = 0, pqsize = 0;

  std::cout << "Normal BGTask... " << std::endl;
  for (int i = 0; i < 10; i++) {
    int *pi = new int(i);
    t.Schedule(task, (void*)pi);
    t.QueueSize(&pqsize, &qsize);
    slash::MutexLock l(&print_lock);
    std::cout << " current queue size:" << qsize << ", " << pqsize << std::endl;
  }
  std::cout << std::endl << std::endl;

  while (qsize > 0) {
    t.QueueSize(&pqsize, &qsize);
    sleep(1);
  }


  qsize = pqsize = 0;
  std::cout << "Limit queue BGTask... " << std::endl;
  for (int i = 0; i < 10; i++) {
    int *pi = new int(i);
    t2.Schedule(task, (void*)pi);
    t2.QueueSize(&pqsize, &qsize);
    slash::MutexLock l(&print_lock);
    std::cout << " current queue size:" << qsize << ", " << pqsize << std::endl;
  }
  std::cout << std::endl << std::endl;

  while (qsize > 0) {
    t2.QueueSize(&pqsize, &qsize);
    sleep(1);
  }
  
  std::cout << "TimerItem Struct... " << std::endl;
  std::priority_queue<TimerItem> pq;
  pq.push(TimerItem(1, task, NULL));
  pq.push(TimerItem(5, task, NULL));
  pq.push(TimerItem(3, task, NULL));
  pq.push(TimerItem(2, task, NULL));
  pq.push(TimerItem(4, task, NULL));

  while (!pq.empty()) {
    printf("%ld\n", pq.top().exec_time);
    pq.pop();
  }
  std::cout << std::endl << std::endl;

  std::cout << "Restart BGThread" << std::endl;
  t.StopThread();
  t.StartThread();
  std::cout << "Time BGTask... " << std::endl;
  for (int i = 0; i < 10; i++) {
    int *pi = new int(i);
    t.DelaySchedule(i * 1000, task, (void*)pi);
    t.QueueSize(&pqsize, &qsize);
    slash::MutexLock l(&print_lock);
    std::cout << " current queue size:" << qsize << ", " << pqsize << std::endl;
  }
  sleep(3);
  std::cout << "QueueClear..." << std::endl;
  t.QueueClear();
  sleep(10);

  return 0;
}
