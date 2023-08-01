// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "unistd.h"

#include <pthread.h>
#include <sys/time.h>
#include <iostream>
#include <string>

#include "net/include/thread_pool.h"
#include "pstd/include/pstd_mutex.h"

using namespace std;

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

static pstd::Mutex print_lock;

void task(void* arg) {
  std::unique_ptr<int32_t> int_arg(static_cast<int32_t*>(arg));
  {
    std::lock_guard l(print_lock);
    std::cout << " task : " << *int_arg << " time(micros) " << NowMicros() << "   thread id: " << pthread_self()
              << std::endl;
  }
  sleep(1);
}

int32_t main() {
  // 10 threads
  net::ThreadPool t(10, 1000), t2(10, 5);
  t.start_thread_pool();
  t2.start_thread_pool();
  size_t qsize = 0, pqsize = 0;

  std::cout << "Test Normal Task... " << std::endl;
  for (int32_t i = 0; i < 10; i++) {
    int32_t* pi = new int32_t(i);
    t.Schedule(task, (void*)pi);
    t.cur_queue_size(&qsize);
    t.cur_time_queue_size(&pqsize);
    std::lock_guard l(print_lock);
    std::cout << " current queue size:" << qsize << ", " << pqsize << std::endl;
  }

  while (qsize > 0) {
    t.cur_queue_size(&qsize);
    sleep(1);
  }

  std::cout << std::endl << std::endl << std::endl;

  qsize = pqsize = 0;
  std::cout << "Test Time Task" << std::endl;
  t.stop_thread_pool();
  t.start_thread_pool();
  for (int32_t i = 0; i < 10; i++) {
    int32_t* pi = new int32_t(i);
    t.DelaySchedule(i * 1000, task, (void*)pi);
    t.cur_queue_size(&qsize);
    t.cur_time_queue_size(&pqsize);
    std::lock_guard l(print_lock);
    std::cout << "Schedule task " << i << " time(micros) " << NowMicros() << " for " << i * 1000 * 1000 << " micros "
              << std::endl;
  }
  while (pqsize > 0) {
    t.cur_time_queue_size(&pqsize);
    sleep(1);
  }
  std::cout << std::endl << std::endl;

  qsize = pqsize = 0;
  t.stop_thread_pool();
  t.start_thread_pool();
  std::cout << "Test Drop Task... " << std::endl;
  for (int32_t i = 0; i < 10; i++) {
    int32_t* pi = new int32_t(i);
    t.DelaySchedule(i * 1000, task, (void*)pi);
    t.cur_queue_size(&qsize);
    t.cur_time_queue_size(&pqsize);
    std::lock_guard l(print_lock);
    std::cout << " current queue size:" << qsize << ", " << pqsize << std::endl;
  }
  sleep(3);
  std::cout << "QueueClear..." << std::endl;
  t.stop_thread_pool();
  sleep(10);

  return 0;
}
